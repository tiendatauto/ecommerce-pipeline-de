-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Category Revenue View

-- COMMAND ----------

CREATE OR REPLACE VIEW ecommerce_incr.gold.v_category_revenue
AS
SELECT
    category_slug,
    category_name,
    COUNT(DISTINCT cart_id) AS carts,
    COUNT(DISTINCT customer_id) AS customers,
    COUNT(DISTINCT product_id) AS products_sold,
    SUM(quantity) AS units_sold,
    ROUND(SUM(line_gross_amount), 2) AS gross_revenue,
    ROUND(SUM(line_discount_amount), 2) AS discount_amount,
    ROUND(SUM(line_net_amount), 2) AS net_revenue,
    ROUND(SUM(gross_margin_proxy), 2) AS gross_margin_proxy,
    ROUND(SUM(line_net_amount) / NULLIF(COUNT(DISTINCT cart_id), 0), 2) AS revenue_per_cart,
    ROUND(SUM(line_discount_amount) / NULLIF(SUM(line_gross_amount), 0) * 100, 2) AS effective_discount_pct
FROM ecommerce_incr.gold.fact_sales
GROUP BY
    category_slug,
    category_name;

-- COMMAND ----------

SELECT * FROM ecommerce_incr.gold.v_category_revenue ORDER BY net_revenue DESC;
