# Databricks notebook source
# MAGIC %md
# MAGIC # Complete Batch

# COMMAND ----------

# MAGIC %md
# MAGIC Mark the e-commerce API batch as completed after all medallion tasks finish.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

from datetime import datetime
v_batch_id = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

if v_batch_id:
    delta_table = DeltaTable.forName(spark, control_table)

    source_df = (
        spark.createDataFrame([(v_batch_id,)], ["batch_id"])
            .withColumn("status", F.lit("completed"))
            .withColumn("updated_timestamp", F.current_timestamp())
    )

    (
        delta_table.alias("t")
            .merge(
                source_df.alias("s"),
                "t.batch_id = s.batch_id AND t.status = 'in_progress'"
            )
            .whenMatchedUpdate(set={
                "status": "s.status",
                "updated_timestamp": "s.updated_timestamp"
            })
            .execute()
    )

    print(f"Marked batch {v_batch_id} as completed")
else:
    raise Exception("batch_id is missing")  
