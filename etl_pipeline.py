import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import mysql.connector

# ==========================================
# CONFIGURATION
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),  
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

required_keys = ["user", "password", "database"]
missing = [
    key for key in required_keys
    if not DB_CONFIG.get(key)
]

if missing:
    raise RuntimeError(
        f"Konfigurasi database belum lengkap: {', '.join(missing)}"
    )

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

INPUT_FILE = PROJECT_ROOT / "data" / "raw_sales.csv"

# ==========================================
# 1. EXTRACT
# ==========================================
# Tahap Extract: Membaca file data mentah (raw data) dari file CSV ke dalam bentuk DataFrame Pandas.
print("=== EXTRACT ===")

df = pd.read_csv(INPUT_FILE)

print(f"Raw records: {len(df)}")


# ==========================================
# 2. TRANSFORM
# ==========================================
# Tahap Transform: Membersihkan, menstandardisasi, dan memproses data sebelum dimasukkan ke database.
print("\n=== TRANSFORM ===")

# Menghapus baris data yang duplikat persis secara keseluruhan
before = len(df)
df = df.drop_duplicates()

print(
    f"Duplicate removed: "
    f"{before - len(df)}"
)

# Menstandardisasi kolom teks (mengisi nilai kosong dengan "Unknown" dan menghapus spasi berlebih)
text_columns = [
    "customer_name",
    "customer_city",
    "product_name",
    "category",
    "payment_method",
    "status"
]

for column in text_columns:

    df[column] = (
        df[column]
        .fillna("Unknown")
        .astype(str)
        .str.strip()
    )


# Standardisasi penulisan metode pembayaran agar seragam
df["payment_method"] = (
    df["payment_method"]
    .str.lower()
    .replace({
        "cash": "Cash",
        "transfer": "Transfer",
        "e-wallet": "E-Wallet"
    })
)

# Standardisasi penulisan status pesanan
df["status"] = (
    df["status"]
    .str.lower()
    .replace({
        "completed": "Completed",
        "cancelled": "Cancelled"
    })
)

# Konversi kolom tanggal ke format datetime (jika ada format invalid, ubah menjadi NaT)
df["order_date"] = pd.to_datetime(
    df["order_date"],
    errors="coerce"
)

# Konversi kolom numerik (quantity dan unit_price) menjadi angka
df["quantity"] = pd.to_numeric(
    df["quantity"],
    errors="coerce"
)

df["unit_price"] = pd.to_numeric(
    df["unit_price"],
    errors="coerce"
)

# Menghapus baris dengan data invalid (tanggal kosong, quantity <= 0, atau unit_price <= 0)
before = len(df)

df = df[
    (df["order_date"].notna()) &
    (df["quantity"] > 0) &
    (df["unit_price"] > 0)
].copy()

print(
    f"Invalid records removed: "
    f"{before - len(df)}"
)

# Mengisi kembali kota pelanggan yang kosong dengan "Unknown"
df["customer_city"] = (
    df["customer_city"]
    .replace("", "Unknown")
)

# Menghitung total nilai penjualan (quantity dikali unit_price)
df["total_amount"] = (
    df["quantity"] *
    df["unit_price"]
)


print(f"Clean records: {len(df)}")



# ==========================================
# 3. CONNECT TO MYSQL
# ==========================================
print("\n=== LOAD ===")

connection = get_db_connection()
cursor = connection.cursor()

try:
    # Mulai satu transaksi untuk seluruh proses load.
    connection.start_transaction()

    # ==========================================
    # 4. FULL REFRESH: HAPUS DATA LAMA
    # ==========================================
    # Urutan mengikuti relasi foreign key.
    cursor.execute("DELETE FROM sales")
    cursor.execute("DELETE FROM products")
    cursor.execute("DELETE FROM customers")

    # ==========================================
    # 5. LOAD CUSTOMERS
    # ==========================================
    customers = (
        df[["customer_name", "customer_city"]]
        .drop_duplicates()
    )

    customer_query = """
        INSERT INTO customers (customer_name, city)
        VALUES (%s, %s)
    """

    customer_rows = [
        (row.customer_name, row.customer_city)
        for row in customers.itertuples(index=False)
    ]

    cursor.executemany(customer_query, customer_rows)

    # ==========================================
    # 6. LOAD PRODUCTS
    # ==========================================
    products = (
        df[["product_name", "category", "unit_price"]]
        .drop_duplicates()
    )

    product_query = """
        INSERT INTO products (product_name, category, unit_price)
        VALUES (%s, %s, %s)
    """

    product_rows = [
        (row.product_name, row.category, float(row.unit_price))
        for row in products.itertuples(index=False)
    ]

    cursor.executemany(product_query, product_rows)

    # ==========================================
    # 7. BUILD ID MAPPINGS
    # ==========================================
    cursor.execute("""
        SELECT customer_id, customer_name, city
        FROM customers
    """)

    customer_map = {
        (name, city): customer_id
        for customer_id, name, city in cursor.fetchall()
    }

    cursor.execute("""
        SELECT product_id, product_name, category, unit_price
        FROM products
    """)

    product_map = {
        (name, category, float(price)): product_id
        for product_id, name, category, price in cursor.fetchall()
    }

    # ==========================================
    # 8. LOAD SALES
    # ==========================================
    sales_query = """
        INSERT INTO sales (
            order_id, order_date, customer_id, product_id,
            quantity, unit_price, total_amount,
            payment_method, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    sales_rows = []

    for row in df.itertuples(index=False):
        customer_id = customer_map[
            (row.customer_name, row.customer_city)
        ]

        product_id = product_map[
            (row.product_name, row.category, float(row.unit_price))
        ]

        sales_rows.append((
            row.order_id,
            row.order_date.date(),
            customer_id,
            product_id,
            int(row.quantity),
            float(row.unit_price),
            float(row.total_amount),
            row.payment_method,
            row.status
        ))

    cursor.executemany(sales_query, sales_rows)

    # ==========================================
    # 9. DATA QUALITY CHECK
    # ==========================================
    cursor.execute("SELECT COUNT(*) FROM sales")
    total_records = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM sales
        WHERE quantity <= 0
           OR unit_price <= 0
           OR total_amount <= 0
    """)

    invalid_records = cursor.fetchone()[0]

    # Pastikan hasil load sesuai dengan data hasil transformasi.
    if total_records != len(df):
        raise ValueError(
            f"Jumlah record tidak sesuai: "
            f"expected={len(df)}, actual={total_records}"
        )

    if invalid_records > 0:
        raise ValueError(
            f"Ditemukan {invalid_records} record penjualan invalid"
        )

    # Commit hanya setelah semua tahap dan validasi berhasil.
    connection.commit()

    print(f"Customers loaded: {len(customers)}")
    print(f"Products loaded: {len(products)}")
    print(f"Sales loaded: {total_records}")
    print(f"Invalid sales records: {invalid_records}")

except Exception:
    connection.rollback()
    print("Load gagal. Perubahan dalam transaksi dibatalkan.")
    raise

finally:
    cursor.close()
    connection.close()

print("\nETL pipeline selesai.")