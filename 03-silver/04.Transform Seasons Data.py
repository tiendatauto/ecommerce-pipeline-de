# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Seasons Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.seasons"
silver_table = f"{catalog_name}.{silver_schema}.seasons"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

seasons_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

seasons_final_df = (
    seasons_df.select(
        col_or_null(seasons_df, "strSeason", "season"),
        col_or_null(seasons_df, "requested_league_id", "league_id"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("season").isNotNull() & F.col("league_id").isNotNull())
    .dropDuplicates(["league_id", "season"])
)

# COMMAND ----------

write_to_silver(
    input_df=seasons_final_df,
    target_table=silver_table,
    merge_condition="t.league_id = s.league_id AND t.season = s.season",
    columns_to_update=[
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
