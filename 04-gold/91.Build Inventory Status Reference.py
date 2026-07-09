# Databricks notebook source
# MAGIC %md
# MAGIC # Build Inventory Status Reference

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.ref_inventory_status"

# COMMAND ----------

from pyspark.sql import Row

inventory_status_rows = [
    Row(inventory_risk="LOW_STOCK", min_stock=0, max_stock=10, priority=1, action="Reorder immediately"),
    Row(inventory_risk="WATCH", min_stock=11, max_stock=50, priority=2, action="Monitor demand and supplier lead time"),
    Row(inventory_risk="HEALTHY", min_stock=51, max_stock=None, priority=3, action="No immediate replenishment action"),
]

ref_inventory_status_df = spark.createDataFrame(inventory_status_rows)

# COMMAND ----------

(
    ref_inventory_status_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))
