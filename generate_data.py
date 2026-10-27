import csv
import random
import os
from datetime import datetime, timedelta

# Mengatur seed random agar data yang dihasilkan selalu konsisten dan sama setiap kali program dijalankan
random.seed(42)

# Daftar master data untuk pelanggan (nama dan kota asal)
customers = [
    ("Andi", "Padang"),
    ("Budi", "Pekanbaru"),
    ("Citra", "Padang"),
    ("Dina", "Bukittinggi"),
    ("Eka", "Pekanbaru"),
    ("Fajar", "Padang"),
    ("Gita", "Payakumbuh"),
    ("Hadi", "Padang"),
]

# Daftar master data produk (nama produk, kategori, dan harga satuan)
products = [
    ("Laptop Stand", "Accessories", 150000),
    ("Wireless Mouse", "Accessories", 85000),
    ("Keyboard", "Accessories", 175000),
    ("USB Hub", "Accessories", 75000),
    ("Webcam", "Electronics", 350000),
    ("Headset", "Electronics", 250000),
    ("Monitor", "Electronics", 1800000),
    ("SSD 512GB", "Storage", 750000),
]

# Variasi metode pembayaran (sengaja ada inkonsistensi huruf kapital)
payment_methods = [
    "Cash",
    "Transfer",
    "E-Wallet",
    "cash",
    "TRANSFER",
]

# Variasi status pesanan (sengaja ada inkonsistensi huruf kapital)
statuses = [
    "Completed",
    "completed",
    "Cancelled",
]

# Membuat folder "data" jika belum ada untuk menyimpan file CSV output
os.makedirs("data", exist_ok=True)

# List untuk menampung seluruh baris data transaksi
rows = []

# Menentukan tanggal awal transaksi (1 Januari 2026)
start_date = datetime(2026, 1, 1)

# Melakukan perulangan sebanyak 500 kali untuk menghasilkan data penjualan
for i in range(1, 501):

    customer = random.choice(customers)
    product = random.choice(products)

    # Menghasilkan tanggal acak dalam rentang 180 hari dari tanggal awal
    order_date = start_date + timedelta(
        days=random.randint(0, 180)
    )
    
    quantity = random.randint(1, 5)

    # Injeksi anomali: Membuat quantity bernilai negatif pada baris tertentu (kelipatan 97)
    if i % 97 == 0:
        quantity = -1

    customer_name = customer[0]
    customer_city = customer[1]

    # Injeksi anomali: Mengosongkan nama kota pada baris tertentu (kelipatan 83)
    if i % 83 == 0:
        customer_city = ""

    payment = random.choice(payment_methods)
    status = random.choice(statuses)

    # Menyusun satu baris data transaksi
    row = [
        f"ORD{i:05d}",
        order_date.strftime("%Y-%m-%d"),
        customer_name,
        customer_city,
        product[0],
        product[1],
        quantity,
        product[2],
        payment,
        status
    ]

    rows.append(row)


# Menambahkan baris duplikat secara manual untuk simulasi data ganda
rows.append(rows[10])
rows.append(rows[25])
rows.append(rows[100])

# Menambahkan satu baris data dengan format tanggal yang tidak valid
rows.append([
    "ORD99999",
    "2026-99-99",
    "Unknown",
    "Padang",
    "Mouse",
    "Accessories",
    1,
    85000,
    "Cash",
    "Completed"
])

file_path = "data/raw_sales.csv"

# Membuka file dalam mode tulis (write) dengan encoding UTF-8
with open(file_path, "w", newline="", encoding="utf-8") as f:

    writer = csv.writer(f)

    # Menuliskan header kolom CSV
    writer.writerow([
        "order_id",
        "order_date",
        "customer_name",
        "customer_city",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "payment_method",
        "status"
    ])

    writer.writerows(rows)

# Menampilkan informasi ringkasan proses ke terminal
print(f"Dataset berhasil dibuat: {file_path}")
print(f"Jumlah raw records: {len(rows)}")