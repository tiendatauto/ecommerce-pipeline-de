-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Customer Value View

-- COMMAND ----------

CREATE OR REPLACE VIEW ecommerce_incr.gold.v_customer_value
AS
WITH customer_sales AS (
    SELECT
        customer_id,
        COUNT(DISTINCT cart_id) AS carts,
        COUNT(DISTINCT product_id) AS distinct_products,
        SUM(quantity) AS units_purchased,
        ROUND(SUM(line_net_amount), 2) AS net_revenue,
        ROUND(SUM(line_discount_amount), 2) AS discount_amount,
        ROUND(SUM(line_net_amount) / NULLIF(COUNT(DISTINCT cart_id), 0), 2) AS avg_order_value
    FROM ecommerce_incr.gold.fact_sales
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.customer_name,
    c.gender,
    c.age_group,
    c.city,
    c.state,
    c.country,
    COALESCE(s.carts, 0) AS carts,
    COALESCE(s.distinct_products, 0) AS distinct_products,
    COALESCE(s.units_purchased, 0) AS units_purchased,
    COALESCE(s.net_revenue, 0) AS net_revenue,
    COALESCE(s.discount_amount, 0) AS discount_amount,
    COALESCE(s.avg_order_value, 0) AS avg_order_value,
    CASE
        WHEN COALESCE(s.net_revenue, 0) >= 3000 THEN 'VIP'
        WHEN COALESCE(s.net_revenue, 0) >= 1000 THEN 'GROWTH'
        WHEN COALESCE(s.net_revenue, 0) > 0 THEN 'STANDARD'
        ELSE 'NO_PURCHASE'
    END AS customer_value_segment
FROM ecommerce_incr.gold.dim_customers c
LEFT JOIN customer_sales s
  ON c.customer_id = s.customer_id;

-- COMMAND ----------

SELECT * FROM ecommerce_incr.gold.v_customer_value ORDER BY net_revenue DESC;
