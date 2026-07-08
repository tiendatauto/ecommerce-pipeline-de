# Databricks notebook source
# MAGIC %md
# MAGIC # Build Result Points Reference

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.ref_result_points"

# COMMAND ----------

from pyspark.sql import Row

result_points_rows = [
    Row(result_code="W", result_name="Win", standing_points=3),
    Row(result_code="D", result_name="Draw", standing_points=1),
    Row(result_code="L", result_name="Loss", standing_points=0),
    Row(result_code="SCHEDULED", result_name="Scheduled", standing_points=0),
]

ref_result_points_df = spark.createDataFrame(result_points_rows)

# COMMAND ----------

(
    ref_result_points_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))
