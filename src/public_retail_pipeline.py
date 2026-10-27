
from pathlib import Path
from shutil import copy2
from time import perf_counter
import json

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


ROOT = Path(__file__).resolve().parents[1]

SOURCE = ROOT / "data/public_source/online_retail_100k.csv"
LAKE = ROOT / "data/lake"

BRONZE = LAKE / "bronze/public_retail"
SILVER = LAKE / "silver/public_retail"
GOLD = LAKE / "gold/public_retail"
QUARANTINE = LAKE / "quarantine/public_retail"
BENCHMARK = ROOT / "data/benchmark/public_retail_metrics.json"

SOURCE_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def write_parquet(df, path):
    df.write.mode("overwrite").parquet(str(path))


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(
            f"CSV tidak ditemukan: {SOURCE}\n"
            "Jalankan src/prepare_public_data.py terlebih dahulu."
        )

    for path in [BRONZE, SILVER, GOLD, QUARANTINE]:
        path.mkdir(parents=True, exist_ok=True)

    BENCHMARK.parent.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("PublicRetailETL-100K")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    timings = {}
    start_total = perf_counter()

    try:
        # --------------------------------------------------
        # BRONZE: arsip CSV asli, tanpa transformasi isi
        # --------------------------------------------------
        t0 = perf_counter()

        bronze_csv = BRONZE / "online_retail_100k.csv"
        copy2(SOURCE, bronze_csv)

        raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(bronze_csv))
            .select(*SOURCE_COLUMNS)
            .withColumn("_source_row_id", F.monotonically_increasing_id())
            .cache()
        )

        raw_count = raw.count()
        timings["bronze_seconds"] = round(perf_counter() - t0, 3)

        # --------------------------------------------------
        # TRANSFORM: casting dan normalisasi
        # --------------------------------------------------
        t0 = perf_counter()

        typed = (
            raw
            .withColumn(
                "order_id",
                F.trim(F.col("InvoiceNo"))
            )
            .withColumn(
                "product_id",
                F.trim(F.col("StockCode"))
            )
            .withColumn(
                "product_name",
                F.trim(F.col("Description"))
            )
            .withColumn(
                "quantity",
                F.col("Quantity").cast("int")
            )
            .withColumn(
                "order_ts",
                F.to_timestamp(F.col("InvoiceDate"))
            )
            .withColumn(
                "unit_price",
                F.col("UnitPrice").cast("decimal(12,2)")
            )
            .withColumn(
                "customer_id",
                F.trim(F.col("CustomerID"))
            )
            .withColumn(
                "country",
                F.trim(F.col("Country"))
            )
        )

        # Pembatalan dikenali dari InvoiceNo yang diawali C.
        is_cancellation = F.upper(
            F.coalesce(F.col("order_id"), F.lit(""))
        ).startswith("C")

        cancellations = typed.filter(is_cancellation)
        non_cancelled = typed.filter(~is_cancellation)

        # Invalid non-cancellation records.
        valid_condition = (
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("product_id").isNotNull()
            & (F.col("product_id") != "")
            & F.col("order_ts").isNotNull()
            & F.col("quantity").isNotNull()
            & (F.col("quantity") > 0)
            & F.col("unit_price").isNotNull()
            & (F.col("unit_price") > 0)
        )

        invalid = non_cancelled.filter(~valid_condition)
        eligible = non_cancelled.filter(valid_condition)

        # Hanya duplikat identik pada baris yang memenuhi valid_condition
        # yang dikeluarkan dari Silver.
        dedup_window = (
            Window
            .partitionBy(*SOURCE_COLUMNS)
            .orderBy("_source_row_id")
        )

        ranked = eligible.withColumn(
            "_duplicate_rank",
            F.row_number().over(dedup_window)
        )

        duplicate_rows = ranked.filter(
            F.col("_duplicate_rank") > 1
        )

        unique_rows = ranked.filter(
            F.col("_duplicate_rank") == 1
        )

        silver = (
            unique_rows
            .select(
                "order_id",
                "product_id",
                "product_name",
                "quantity",
                "order_ts",
                "unit_price",
                "customer_id",
                "country",
            )
            .withColumn(
                "order_date",
                F.to_date("order_ts")
            )
            .withColumn(
                "total_amount",
                (
                    F.col("quantity") * F.col("unit_price")
                ).cast("decimal(18,2)")
            )
            .cache()
        )

        # Materialisasi agar benchmark dan validasi konsisten.
        silver_count = silver.count()
        invalid_count = invalid.count()
        cancellation_count = cancellations.count()
        duplicate_count = duplicate_rows.count()

        timings["transform_seconds"] = round(
            perf_counter() - t0, 3
        )

        # --------------------------------------------------
        # QUARANTINE: simpan tiap kategori secara terpisah
        # --------------------------------------------------
        t0 = perf_counter()

        write_parquet(
            invalid.drop("_source_row_id"),
            QUARANTINE / "invalid_records"
        )
        write_parquet(
            cancellations.drop("_source_row_id"),
            QUARANTINE / "cancellations"
        )
        write_parquet(
            duplicate_rows.drop("_source_row_id", "_duplicate_rank"),
            QUARANTINE / "duplicate_records"
        )

        timings["quarantine_write_seconds"] = round(
            perf_counter() - t0, 3
        )

        # --------------------------------------------------
        # SILVER: data penjualan bersih dalam format Parquet
        # --------------------------------------------------
        t0 = perf_counter()

        write_parquet(silver, SILVER)

        timings["silver_write_seconds"] = round(
            perf_counter() - t0, 3
        )

        # --------------------------------------------------
        # GOLD: agregasi untuk analisis bisnis
        # --------------------------------------------------
        t0 = perf_counter()

        daily_sales = (
            silver.groupBy("order_date")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.countDistinct("order_id").alias(
                    "invoice_count"
                ),
                F.sum("quantity").alias("units_sold"),
            )
        )

        product_sales = (
            silver.groupBy("product_id", "product_name")
            .agg(
                F.sum("quantity").alias("units_sold"),
                F.sum("total_amount").alias("total_revenue"),
                F.countDistinct("order_id").alias(
                    "invoice_count"
                ),
            )
        )

        country_sales = (
            silver.groupBy("country")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.countDistinct("order_id").alias(
                    "invoice_count"
                ),
            )
        )

        write_parquet(daily_sales, GOLD / "daily_sales")
        write_parquet(product_sales, GOLD / "product_sales")
        write_parquet(country_sales, GOLD / "country_sales")

        # Memaksa penyelesaian agregasi untuk mengukur waktunya.
        daily_count = daily_sales.count()
        product_count = product_sales.count()
        country_count = country_sales.count()

        timings["gold_seconds"] = round(
            perf_counter() - t0, 3
        )

        # --------------------------------------------------
        # QUALITY CHECKS: rekonsiliasi dan integritas
        # --------------------------------------------------
        t0 = perf_counter()

        # Semua baris harus masuk tepat satu kategori:
        # cancellation, invalid, duplicate, atau silver.
        accounted_rows = (
            cancellation_count
            + invalid_count
            + duplicate_count
            + silver_count
        )

        invalid_silver_count = silver.filter(
            F.col("order_id").isNull()
            | (F.col("order_id") == "")
            | F.col("order_ts").isNull()
            | F.col("quantity").isNull()
            | (F.col("quantity") <= 0)
            | F.col("unit_price").isNull()
            | (F.col("unit_price") <= 0)
            | (F.col("total_amount") <= 0)
        ).count()

        null_order_ids = silver.filter(
            F.col("order_id").isNull()
        ).count()

        distinct_order_dates = silver.select(
            "order_date"
        ).distinct().count()

        silver_revenue = silver.agg(
            F.sum("total_amount").alias("revenue")
        ).first()["revenue"]

        quality_checks = {
            "source_has_100k_rows": raw_count == 100_000,
            "row_reconciliation_passed": (
                raw_count == accounted_rows
            ),
            "no_invalid_rows_in_silver": (
                invalid_silver_count == 0
            ),
            "no_null_order_ids_in_silver": (
                null_order_ids == 0
            ),
            "silver_count_matches_output": (
                silver_count == silver.count()
            ),
        }

        timings["quality_checks_seconds"] = round(
            perf_counter() - t0, 3
        )
        timings["total_seconds"] = round(
            perf_counter() - start_total, 3
        )

        metrics = {
            "dataset": "UCI Online Retail",
            "target_rows": 100_000,
            "raw_rows": raw_count,
            "cancellation_rows": cancellation_count,
            "invalid_rows": invalid_count,
            "duplicate_rows_removed": duplicate_count,
            "silver_rows": silver_count,
            "rows_accounted_for": accounted_rows,
            "silver_active_days": distinct_order_dates,
            "silver_revenue": (
                str(silver_revenue)
                if silver_revenue is not None
                else None
            ),
            "gold_daily_rows": daily_count,
            "gold_product_rows": product_count,
            "gold_country_rows": country_count,
            "quality_checks": quality_checks,
            "all_quality_checks_passed": all(
                quality_checks.values()
            ),
            "timings_seconds": timings,
        }

        with open(BENCHMARK, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)

        print("\n=== PUBLIC RETAIL ETL REPORT ===")
        print(json.dumps(metrics, indent=2, ensure_ascii=False))

        if not all(quality_checks.values()):
            raise RuntimeError(
                "Satu atau lebih quality checks gagal. "
                "Periksa laporan benchmark."
            )

        print("\nPipeline selesai. Semua quality checks lulus.")

    finally:
        raw.unpersist() if "raw" in locals() else None
        if "silver" in locals():
            silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()