# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Leagues Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.leagues"
silver_table = f"{catalog_name}.{silver_schema}.leagues"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

leagues_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

leagues_final_df = (
    leagues_df.select(
        col_or_null(leagues_df, "idLeague", "league_id"),
        col_or_null(leagues_df, "strLeague", "league_name"),
        col_or_null(leagues_df, "strSport", "sport_name"),
        col_or_null(leagues_df, "strLeagueAlternate", "league_alternate_name"),
        col_or_null(leagues_df, "strCountry", "country"),
        col_or_null(leagues_df, "strCurrentSeason", "current_season"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("league_id").isNotNull())
    .dropDuplicates(["league_id"])
    .withColumn("league_name", F.initcap("league_name"))
    .withColumn("sport_name", F.initcap("sport_name"))
    .withColumn("country", F.initcap("country"))
)

# COMMAND ----------

write_to_silver(
    input_df=leagues_final_df,
    target_table=silver_table,
    merge_condition="t.league_id = s.league_id",
    columns_to_update=[
        "league_name",
        "sport_name",
        "league_alternate_name",
        "country",
        "current_season",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
