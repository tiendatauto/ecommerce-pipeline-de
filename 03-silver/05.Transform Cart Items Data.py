# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Cart Items Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.cart_items"
silver_table = f"{catalog_name}.{silver_schema}.cart_items"

# COMMAND ----------

cart_items_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

cart_items_final_df = (
    cart_items_df.select(
        F.col("cartId").cast("int").alias("cart_id"),
        F.col("userId").cast("int").alias("customer_id"),
        F.col("productId").cast("int").alias("product_id"),
        F.col("title").alias("product_name"),
        F.col("price").cast("double").alias("unit_price"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("lineTotal").cast("double").alias("line_gross_amount"),
        F.col("discountPercentage").cast("double").alias("line_discount_pct"),
        F.col("lineDiscountedTotal").cast("double").alias("line_net_amount"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("cart_id").isNotNull() & F.col("product_id").isNotNull())
    .dropDuplicates(["cart_id", "product_id"])
    .withColumn("line_discount_amount", F.col("line_gross_amount") - F.col("line_net_amount"))
)

# COMMAND ----------

write_to_silver(
    input_df=cart_items_final_df,
    target_table=silver_table,
    merge_condition="t.cart_id = s.cart_id AND t.product_id = s.product_id",
    columns_to_update=[
        "customer_id",
        "product_name",
        "unit_price",
        "quantity",
        "line_gross_amount",
        "line_discount_pct",
        "line_net_amount",
        "line_discount_amount",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
