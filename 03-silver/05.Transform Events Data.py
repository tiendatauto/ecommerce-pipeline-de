# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Events Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.events"
silver_table = f"{catalog_name}.{silver_schema}.events"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

events_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

events_selected_df = events_df.select(
    col_or_null(events_df, "idEvent", "event_id"),
    col_or_null(events_df, "idLeague", "league_id"),
    col_or_null(events_df, "strLeague", "league_name"),
    col_or_null(events_df, "strSport", "sport_name"),
    col_or_null(events_df, "strSeason", "season"),
    col_or_null(events_df, "intRound", "round", "int"),
    col_or_null(events_df, "strEvent", "event_name"),
    col_or_null(events_df, "dateEvent", "event_date", "date"),
    col_or_null(events_df, "strTimestamp", "event_timestamp", "timestamp"),
    col_or_null(events_df, "idHomeTeam", "home_team_id"),
    col_or_null(events_df, "strHomeTeam", "home_team_name"),
    col_or_null(events_df, "idAwayTeam", "away_team_id"),
    col_or_null(events_df, "strAwayTeam", "away_team_name"),
    col_or_null(events_df, "intHomeScore", "home_score", "int"),
    col_or_null(events_df, "intAwayScore", "away_score", "int"),
    col_or_null(events_df, "strStatus", "event_status"),
    col_or_null(events_df, "strVenue", "venue_name"),
    col_or_null(events_df, "strCountry", "country"),
    col_or_null(events_df, "requested_league_id"),
    col_or_null(events_df, "requested_season"),
    "ingestion_timestamp",
    "source_api",
    "batch_id",
)

# COMMAND ----------

events_final_df = (
    events_selected_df
    .filter(F.col("event_id").isNotNull())
    .dropDuplicates(["event_id"])
    .withColumn("league_name", F.initcap("league_name"))
    .withColumn("sport_name", F.initcap("sport_name"))
    .withColumn("home_team_name", F.initcap("home_team_name"))
    .withColumn("away_team_name", F.initcap("away_team_name"))
    .withColumn("event_name", F.initcap("event_name"))
    .withColumn(
        "is_played",
        F.col("home_score").isNotNull() & F.col("away_score").isNotNull()
    )
)

# COMMAND ----------

write_to_silver(
    input_df=events_final_df,
    target_table=silver_table,
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
        "home_score",
        "away_score",
        "event_status",
        "venue_name",
        "country",
        "requested_league_id",
        "requested_season",
        "is_played",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
