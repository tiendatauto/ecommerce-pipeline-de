# Databricks notebook source
# MAGIC %md
# MAGIC # Identify Next E-Commerce API Batch

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

from pyspark.sql import functions as F

active_batch = None
if spark.catalog.tableExists(control_table):
    active_rows = (
        spark.table(control_table)
             .filter(F.col("status") == "in_progress")
             .orderBy(F.col("created_timestamp").desc())
             .select("batch_id")
             .limit(1)
             .collect()
    )
    active_batch = active_rows[0].batch_id if active_rows else None

if active_batch:
    next_batch = active_batch
    has_batch = "true"
else:
    next_batch = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss') AS batch_id").first().batch_id
    has_batch = "true"

print(f"Next e-commerce API batch to process: {next_batch}")

dbutils.jobs.taskValues.set(key="p_batch_id", value=next_batch)
dbutils.jobs.taskValues.set(key="has_batch", value=has_batch)
