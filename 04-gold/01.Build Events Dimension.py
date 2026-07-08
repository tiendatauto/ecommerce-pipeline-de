# Databricks notebook source
# MAGIC %md
# MAGIC # Build Events Dimension

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

target_table = f"{catalog_name}.{gold_schema}.dim_events"

# COMMAND ----------

events_df = spark.table(f"{catalog_name}.{silver_schema}.events").filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

dim_events_df = events_df.select(
    "event_id",
    "league_id",
    "league_name",
    "sport_name",
    "season",
    "round",
    "event_name",
    "event_date",
    "event_timestamp",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "venue_name",
    "country",
    "event_status",
    "is_played",
)

# COMMAND ----------

write_to_gold(
    input_df=dim_events_df,
    target_table=target_table,
    merge_condition="t.event_id = s.event_id",
    columns_to_update=[
        "league_id",
        "league_name",
        "sport_name",
        "season",
        "round",
        "event_name",
        "event_date",
        "event_timestamp",
        "home_team_id",
        "home_team_name",
        "away_team_id",
        "away_team_name",
        "venue_name",
        "country",
        "event_status",
        "is_played",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
