# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Carts Data

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.carts"
silver_table = f"{catalog_name}.{silver_schema}.carts"

# COMMAND ----------

carts_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

carts_final_df = (
    carts_df.select(
        F.col("id").cast("int").alias("cart_id"),
        F.col("userId").cast("int").alias("customer_id"),
        F.col("total").cast("double").alias("cart_gross_amount"),
        F.col("discountedTotal").cast("double").alias("cart_net_amount"),
        F.col("totalProducts").cast("int").alias("total_products"),
        F.col("totalQuantity").cast("int").alias("total_quantity"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("cart_id").isNotNull())
    .dropDuplicates(["cart_id"])
    .withColumn("cart_discount_amount", F.col("cart_gross_amount") - F.col("cart_net_amount"))
    .withColumn(
        "cart_discount_pct",
        F.when(F.col("cart_gross_amount") > 0, F.round(F.col("cart_discount_amount") / F.col("cart_gross_amount") * 100, 2))
         .otherwise(F.lit(0.0))
    )
)

# COMMAND ----------

write_to_silver(
    input_df=carts_final_df,
    target_table=silver_table,
    merge_condition="t.cart_id = s.cart_id",
    columns_to_update=[
        "customer_id",
        "cart_gross_amount",
        "cart_net_amount",
        "total_products",
        "total_quantity",
        "cart_discount_amount",
        "cart_discount_pct",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
