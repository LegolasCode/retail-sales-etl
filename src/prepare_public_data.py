from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "public_source" / "Online Retail.xlsx"
OUTPUT = ROOT / "data" / "public_source" / "online_retail_100k.csv"

TARGET_ROWS = 100_000

def main():
    if not SOURCE.exists():
        raise FileNotFoundError(
            f"File sumber tidak ditemukan: {SOURCE}\n"
            "Periksa nama file hasil ekstraksi UCI."
        )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    # Membaca hanya 100.000 baris pertama dari workbook.
    # File Excel asli tetap dipertahankan.
    df = pd.read_excel(SOURCE, nrows=TARGET_ROWS)

    if len(df) != TARGET_ROWS:
        raise ValueError(
            f"Target {TARGET_ROWS:,} baris, "
            f"tetapi hanya ditemukan {len(df):,} baris."
        )

    expected_columns = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Kolom sumber tidak lengkap: {sorted(missing)}")

    df.to_csv(OUTPUT, index=False)

    print("=== PUBLIC DATA PREPARATION ===")
    print(f"Source: {SOURCE.name}")
    print(f"Output: {OUTPUT}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"File size: {OUTPUT.stat().st_size / (1024**2):.2f} MB")
    print("Preparation completed.")

if __name__ == "__main__":
    main()
