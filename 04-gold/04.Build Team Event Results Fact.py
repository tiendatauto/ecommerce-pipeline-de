# Databricks notebook source
# MAGIC %md
# MAGIC # Build Team Event Results Fact

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

target_table = f"{catalog_name}.{gold_schema}.fact_team_event_results"

# COMMAND ----------

events_df = (
    spark.table(f"{catalog_name}.{silver_schema}.events")
         .filter(F.col("batch_id") == v_batch_id)
         .filter(F.col("home_team_id").isNotNull() & F.col("away_team_id").isNotNull())
)

# COMMAND ----------

home_results_df = events_df.select(
    "event_id",
    "league_id",
    "league_name",
    "sport_name",
    "season",
    "event_date",
    F.col("home_team_id").alias("team_id"),
    F.col("home_team_name").alias("team_name"),
    F.col("away_team_id").alias("opponent_team_id"),
    F.col("away_team_name").alias("opponent_team_name"),
    F.lit("HOME").alias("venue_side"),
    F.col("home_score").alias("goals_for"),
    F.col("away_score").alias("goals_against"),
    "is_played",
)

away_results_df = events_df.select(
    "event_id",
    "league_id",
    "league_name",
    "sport_name",
    "season",
    "event_date",
    F.col("away_team_id").alias("team_id"),
    F.col("away_team_name").alias("team_name"),
    F.col("home_team_id").alias("opponent_team_id"),
    F.col("home_team_name").alias("opponent_team_name"),
    F.lit("AWAY").alias("venue_side"),
    F.col("away_score").alias("goals_for"),
    F.col("home_score").alias("goals_against"),
    "is_played",
)

# COMMAND ----------

fact_team_event_results_df = (
    home_results_df.unionByName(away_results_df)
    .withColumn(
        "result_code",
        F.when(~F.col("is_played"), F.lit("SCHEDULED"))
         .when(F.col("goals_for") > F.col("goals_against"), F.lit("W"))
         .when(F.col("goals_for") == F.col("goals_against"), F.lit("D"))
         .otherwise(F.lit("L"))
    )
    .withColumn(
        "standing_points",
        F.when(F.col("result_code") == "W", F.lit(3))
         .when(F.col("result_code") == "D", F.lit(1))
         .otherwise(F.lit(0))
    )
    .withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
    .withColumn("is_win", F.col("result_code") == "W")
    .withColumn("is_draw", F.col("result_code") == "D")
    .withColumn("is_loss", F.col("result_code") == "L")
)

# COMMAND ----------

write_to_gold(
    input_df=fact_team_event_results_df,
    target_table=target_table,
    merge_condition="t.event_id = s.event_id AND t.team_id = s.team_id",
    columns_to_update=[
        "league_id",
        "league_name",
        "sport_name",
        "season",
        "event_date",
        "team_name",
        "opponent_team_id",
        "opponent_team_name",
        "venue_side",
        "goals_for",
        "goals_against",
        "is_played",
        "result_code",
        "standing_points",
        "goal_difference",
        "is_win",
        "is_draw",
        "is_loss",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
