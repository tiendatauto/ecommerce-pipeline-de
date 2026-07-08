# Databricks notebook source
# MAGIC %md
# MAGIC # Build Players Dimension

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

target_table = f"{catalog_name}.{gold_schema}.dim_players"

# COMMAND ----------

players_df = spark.table(f"{catalog_name}.{silver_schema}.players").filter(F.col("batch_id") == v_batch_id)
teams_df = spark.table(f"{catalog_name}.{gold_schema}.dim_teams")

# COMMAND ----------

dim_players_df = (
    players_df.alias("p")
    .join(teams_df.alias("t"), F.col("p.team_id") == F.col("t.team_id"), "left")
    .select(
        F.col("p.player_id"),
        F.col("p.player_name"),
        F.coalesce(F.col("p.team_id"), F.col("p.requested_team_id")).alias("team_id"),
        F.coalesce(F.col("p.team_name"), F.col("t.team_name")).alias("team_name"),
        F.col("t.league_id"),
        F.col("t.league_name"),
        F.col("p.nationality"),
        F.col("p.position_name"),
        F.col("p.birth_date"),
        F.col("p.gender"),
        F.col("p.player_thumb_url"),
    )
)

# COMMAND ----------

write_to_gold(
    input_df=dim_players_df,
    target_table=target_table,
    merge_condition="t.player_id = s.player_id",
    columns_to_update=[
        "player_name",
        "team_id",
        "team_name",
        "league_id",
        "league_name",
        "nationality",
        "position_name",
        "birth_date",
        "gender",
        "player_thumb_url",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
