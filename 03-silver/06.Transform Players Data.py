# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Players Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.players"
silver_table = f"{catalog_name}.{silver_schema}.players"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

players_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

players_final_df = (
    players_df.select(
        col_or_null(players_df, "idPlayer", "player_id"),
        col_or_null(players_df, "strPlayer", "player_name"),
        col_or_null(players_df, "idTeam", "team_id"),
        col_or_null(players_df, "strTeam", "team_name"),
        col_or_null(players_df, "strNationality", "nationality"),
        col_or_null(players_df, "strPosition", "position_name"),
        col_or_null(players_df, "dateBorn", "birth_date", "date"),
        col_or_null(players_df, "strGender", "gender"),
        col_or_null(players_df, "strThumb", "player_thumb_url"),
        col_or_null(players_df, "requested_team_id"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("player_id").isNotNull())
    .dropDuplicates(["player_id"])
    .withColumn("player_name", F.initcap("player_name"))
    .withColumn("team_name", F.initcap("team_name"))
    .withColumn("nationality", F.initcap("nationality"))
)

# COMMAND ----------

write_to_silver(
    input_df=players_final_df,
    target_table=silver_table,
    merge_condition="t.player_id = s.player_id",
    columns_to_update=[
        "player_name",
        "team_id",
        "team_name",
        "nationality",
        "position_name",
        "birth_date",
        "gender",
        "player_thumb_url",
        "requested_team_id",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
