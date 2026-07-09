-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Inventory Risk View

-- COMMAND ----------

CREATE OR REPLACE VIEW ecommerce_incr.gold.v_inventory_risk
AS
WITH product_sales AS (
    SELECT
        product_id,
        SUM(quantity) AS units_sold,
        ROUND(SUM(line_net_amount), 2) AS net_revenue
    FROM ecommerce_incr.gold.fact_sales
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.category_slug,
    p.category_name,
    p.brand_name,
    p.stock_quantity,
    p.inventory_risk,
    r.priority AS replenishment_priority,
    r.action AS recommended_action,
    COALESCE(s.units_sold, 0) AS units_sold,
    COALESCE(s.net_revenue, 0) AS net_revenue,
    p.rating,
    p.availability_status
FROM ecommerce_incr.gold.dim_products p
LEFT JOIN product_sales s
  ON p.product_id = s.product_id
LEFT JOIN ecommerce_incr.gold.ref_inventory_status r
  ON p.inventory_risk = r.inventory_risk;

-- COMMAND ----------

SELECT * FROM ecommerce_incr.gold.v_inventory_risk ORDER BY replenishment_priority, net_revenue DESC;
