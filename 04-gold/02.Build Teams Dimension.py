# Databricks notebook source
# MAGIC %md
# MAGIC # Build Teams Dimension

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

target_table = f"{catalog_name}.{gold_schema}.dim_teams"

# COMMAND ----------

teams_df = spark.table(f"{catalog_name}.{silver_schema}.teams").filter(F.col("batch_id") == v_batch_id)
leagues_df = spark.table(f"{catalog_name}.{silver_schema}.leagues")

# COMMAND ----------

dim_teams_df = (
    teams_df.alias("t")
    .join(leagues_df.alias("l"), F.col("t.league_id") == F.col("l.league_id"), "left")
    .select(
        F.col("t.team_id"),
        F.col("t.team_name"),
        F.col("t.alternate_name"),
        F.coalesce(F.col("t.league_id"), F.col("t.requested_league_id")).alias("league_id"),
        F.coalesce(F.col("t.league_name"), F.col("l.league_name")).alias("league_name"),
        F.coalesce(F.col("t.sport_name"), F.col("l.sport_name")).alias("sport_name"),
        F.coalesce(F.col("t.country"), F.col("l.country")).alias("country"),
        F.col("t.stadium_name"),
        F.col("t.stadium_location"),
        F.col("t.stadium_capacity"),
        F.col("t.formed_year"),
        F.col("t.team_badge_url"),
        F.col("t.website_url"),
    )
)

# COMMAND ----------

write_to_gold(
    input_df=dim_teams_df,
    target_table=target_table,
    merge_condition="t.team_id = s.team_id",
    columns_to_update=[
        "team_name",
        "alternate_name",
        "league_id",
        "league_name",
        "sport_name",
        "country",
        "stadium_name",
        "stadium_location",
        "stadium_capacity",
        "formed_year",
        "team_badge_url",
        "website_url",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
