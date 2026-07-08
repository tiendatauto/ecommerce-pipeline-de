# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Teams Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.teams"
silver_table = f"{catalog_name}.{silver_schema}.teams"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

teams_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

teams_final_df = (
    teams_df.select(
        col_or_null(teams_df, "idTeam", "team_id"),
        col_or_null(teams_df, "strTeam", "team_name"),
        col_or_null(teams_df, "strAlternate", "alternate_name"),
        col_or_null(teams_df, "idLeague", "league_id"),
        col_or_null(teams_df, "strLeague", "league_name"),
        col_or_null(teams_df, "strSport", "sport_name"),
        col_or_null(teams_df, "strCountry", "country"),
        col_or_null(teams_df, "strStadium", "stadium_name"),
        col_or_null(teams_df, "strStadiumLocation", "stadium_location"),
        col_or_null(teams_df, "intStadiumCapacity", "stadium_capacity", "int"),
        col_or_null(teams_df, "intFormedYear", "formed_year", "int"),
        col_or_null(teams_df, "strTeamBadge", "team_badge_url"),
        col_or_null(teams_df, "strWebsite", "website_url"),
        col_or_null(teams_df, "requested_league_id"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("team_id").isNotNull())
    .dropDuplicates(["team_id"])
    .withColumn("team_name", F.initcap("team_name"))
    .withColumn("league_name", F.initcap("league_name"))
    .withColumn("sport_name", F.initcap("sport_name"))
    .withColumn("country", F.initcap("country"))
)

# COMMAND ----------

write_to_silver(
    input_df=teams_final_df,
    target_table=silver_table,
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
        "requested_league_id",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
