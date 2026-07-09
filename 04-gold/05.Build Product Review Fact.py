# Databricks notebook source
# MAGIC %md
# MAGIC # Build Product Review Fact

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_product_reviews"

# COMMAND ----------

reviews_df = spark.table(f"{catalog_name}.{silver_schema}.product_reviews").filter(F.col("batch_id") == v_batch_id)
products_df = spark.table(f"{catalog_name}.{gold_schema}.dim_products")

# COMMAND ----------

fact_product_reviews_df = (
    reviews_df.alias("r")
    .join(products_df.alias("p"), F.col("r.product_id") == F.col("p.product_id"), "left")
    .select(
        F.sha2(F.concat_ws("|", F.col("r.product_id"), F.col("r.reviewer_email"), F.col("r.review_timestamp")), 256).alias("review_id"),
        F.col("r.product_id"),
        F.coalesce(F.col("p.product_name"), F.col("r.product_name")).alias("product_name"),
        F.coalesce(F.col("p.category_slug"), F.col("r.category_slug")).alias("category_slug"),
        F.col("p.category_name"),
        F.col("p.brand_name"),
        F.col("r.review_rating"),
        F.col("r.sentiment_label"),
        F.col("r.review_timestamp"),
        F.col("r.reviewer_name"),
        F.col("r.reviewer_email"),
    )
)

# COMMAND ----------

write_to_gold(
    input_df=fact_product_reviews_df,
    target_table=target_table,
    merge_condition="t.review_id = s.review_id",
    columns_to_update=[
        "product_id",
        "product_name",
        "category_slug",
        "category_name",
        "brand_name",
        "review_rating",
        "sentiment_label",
        "review_timestamp",
        "reviewer_name",
        "reviewer_email",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
