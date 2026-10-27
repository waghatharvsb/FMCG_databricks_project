-- 1. Orders with no customer
SELECT COUNT(*) AS null_customer_rows
FROM fmcg.gold.fact_orders
WHERE customer_code IS NULL;

-- 2. Orders whose product is missing from dim_products
SELECT COUNT(*) AS unmatched_product_rows
FROM fmcg.gold.fact_orders f
LEFT JOIN fmcg.gold.dim_products p ON f.product_code = p.product_code
WHERE p.product_code IS NULL;

-- 3. Orders whose customer is missing from dim_customers
SELECT COUNT(*) AS unmatched_customer_rows
FROM fmcg.gold.fact_orders f
LEFT JOIN fmcg.gold.dim_customers c ON f.customer_code = c.customer_code
WHERE c.customer_code IS NULL;

-- 4. Duplicate orders (same date, product and customer more than once)
SELECT date, product_code, customer_code, COUNT(*) AS n
FROM fmcg.gold.fact_orders
GROUP BY date, product_code, customer_code
HAVING COUNT(*) > 1
ORDER BY n DESC
LIMIT 50;

-- 5. Non-positive quantities
SELECT COUNT(*) AS bad_quantity_rows
FROM fmcg.gold.fact_orders
WHERE sold_quantity IS NULL OR sold_quantity <= 0;