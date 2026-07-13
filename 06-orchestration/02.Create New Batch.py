# Databricks notebook source
# MAGIC %md
# MAGIC # Create New Batch

# COMMAND ----------

# MAGIC %md
# MAGIC Mark the generated e-commerce API batch as in progress.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

if v_batch_id:
    in_progress_df = (
        spark.createDataFrame(
            [Row(batch_id=v_batch_id, status="in_progress")]
        )
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )

    (
        in_progress_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(control_table)
    )

    print(f"Marked batch {v_batch_id} as in_progress")
else:
    raise Exception("batch_id is missing")  
