USE retail_dw;


-- =====================================================
-- 1. TOTAL REVENUE
-- =====================================================

SELECT
    SUM(total_amount) AS total_revenue
FROM sales
WHERE status = 'Completed';


-- =====================================================
-- 2. REVENUE PER BULAN
-- =====================================================

SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    SUM(total_amount) AS revenue
FROM sales
WHERE status = 'Completed'
GROUP BY
    DATE_FORMAT(order_date, '%Y-%m')
ORDER BY
    month;


-- =====================================================
-- 3. PRODUK PALING BANYAK TERJUAL
-- =====================================================

SELECT
    p.product_name,
    SUM(s.quantity) AS total_quantity
FROM sales s
JOIN products p
    ON s.product_id = p.product_id
WHERE s.status = 'Completed'
GROUP BY
    p.product_name
ORDER BY
    total_quantity DESC;


-- =====================================================
-- 4. REVENUE BERDASARKAN KATEGORI
-- =====================================================

SELECT
    p.category,
    SUM(s.total_amount) AS revenue
FROM sales s
JOIN products p
    ON s.product_id = p.product_id
WHERE s.status = 'Completed'
GROUP BY
    p.category
ORDER BY
    revenue DESC;

-- =====================================================
-- 5. CUSTOMER DENGAN TRANSAKSI / SPENDING TERBESAR
-- =====================================================

SELECT
    c.customer_name,
    c.city,
    COUNT(s.order_id) AS total_transactions,
    SUM(s.total_amount) AS total_spending
FROM sales s
JOIN customers c
    ON s.customer_id = c.customer_id
WHERE s.status = 'Completed'
GROUP BY
    c.customer_id,
    c.customer_name,
    c.city
ORDER BY
    total_spending DESC;

-- =====================================================
-- 6. REVENUE BERDASARKAN KOTA
-- =====================================================

SELECT
    c.city,
    SUM(s.total_amount) AS revenue
FROM sales s
JOIN customers c
    ON s.customer_id = c.customer_id
WHERE s.status = 'Completed'
GROUP BY
    c.city
ORDER BY
    revenue DESC;

-- =====================================================
-- 7. ANALISIS METODE PEMBAYARAN
-- =====================================================

SELECT
    payment_method,
    COUNT(*) AS total_transactions,
    SUM(total_amount) AS revenue
FROM sales
WHERE status = 'Completed'
GROUP BY
    payment_method
ORDER BY
    revenue DESC;

-- =====================================================
-- 8. DATA QUALITY CHECK
-- =====================================================

SELECT
    COUNT(*) AS total_records,

    SUM(
        CASE
            WHEN quantity <= 0
            THEN 1
            ELSE 0
        END
    ) AS invalid_quantity,

    SUM(
        CASE
            WHEN unit_price <= 0
            THEN 1
            ELSE 0
        END
    ) AS invalid_price,

    SUM(
        CASE
            WHEN total_amount <= 0
            THEN 1
            ELSE 0
        END
    ) AS invalid_total_amount

FROM sales;