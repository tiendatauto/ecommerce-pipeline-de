# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Sports Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sports"
silver_table = f"{catalog_name}.{silver_schema}.sports"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

sports_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

sports_final_df = (
    sports_df.select(
        col_or_null(sports_df, "idSport", "sport_id"),
        col_or_null(sports_df, "strSport", "sport_name"),
        col_or_null(sports_df, "strFormat", "sport_format"),
        col_or_null(sports_df, "strSportThumb", "sport_thumb_url"),
        col_or_null(sports_df, "strSportIconGreen", "sport_icon_url"),
        col_or_null(sports_df, "strSportDescription", "sport_description"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("sport_name").isNotNull())
    .dropDuplicates(["sport_name"])
    .withColumn("sport_name", F.initcap("sport_name"))
)

# COMMAND ----------

write_to_silver(
    input_df=sports_final_df,
    target_table=silver_table,
    merge_condition="t.sport_name = s.sport_name",
    columns_to_update=[
        "sport_id",
        "sport_format",
        "sport_thumb_url",
        "sport_icon_url",
        "sport_description",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
