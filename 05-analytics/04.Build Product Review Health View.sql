-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Product Review Health View

-- COMMAND ----------

CREATE OR REPLACE VIEW ecommerce_incr.gold.v_product_review_health
AS
SELECT
    p.product_id,
    p.product_name,
    p.category_slug,
    p.category_name,
    p.brand_name,
    COUNT(r.review_id) AS review_count,
    ROUND(AVG(r.review_rating), 2) AS avg_review_rating,
    COUNT_IF(r.sentiment_label = 'POSITIVE') AS positive_reviews,
    COUNT_IF(r.sentiment_label = 'NEUTRAL') AS neutral_reviews,
    COUNT_IF(r.sentiment_label = 'NEGATIVE') AS negative_reviews,
    CASE
        WHEN COUNT(r.reviewer_email) = 0 THEN 'NO_REVIEWS'
        WHEN AVG(r.review_rating) >= 4 THEN 'HEALTHY'
        WHEN AVG(r.review_rating) >= 3 THEN 'WATCH'
        ELSE 'QUALITY_RISK'
    END AS review_health
FROM ecommerce_incr.gold.dim_products p
LEFT JOIN ecommerce_incr.gold.fact_product_reviews r
  ON p.product_id = r.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category_slug,
    p.category_name,
    p.brand_name;

-- COMMAND ----------

SELECT * FROM ecommerce_incr.gold.v_product_review_health ORDER BY avg_review_rating ASC NULLS FIRST;
