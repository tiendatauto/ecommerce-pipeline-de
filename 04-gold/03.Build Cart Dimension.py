# Databricks notebook source
# MAGIC %md
# MAGIC # Build Cart Dimension

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

target_table = f"{catalog_name}.{gold_schema}.dim_carts"

# COMMAND ----------

carts_df = spark.table(f"{catalog_name}.{silver_schema}.carts").filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

dim_carts_df = carts_df.select(
    "cart_id",
    "customer_id",
    "cart_gross_amount",
    "cart_net_amount",
    "cart_discount_amount",
    "cart_discount_pct",
    "total_products",
    "total_quantity",
)

# COMMAND ----------

write_to_gold(
    input_df=dim_carts_df,
    target_table=target_table,
    merge_condition="t.cart_id = s.cart_id",
    columns_to_update=[
        "customer_id",
        "cart_gross_amount",
        "cart_net_amount",
        "cart_discount_amount",
        "cart_discount_pct",
        "total_products",
        "total_quantity",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
