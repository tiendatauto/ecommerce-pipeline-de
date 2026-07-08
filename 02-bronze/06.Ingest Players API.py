# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Team Players From TheSportsDB API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
dbutils.widgets.text("p_max_teams", "10")
v_batch_id = dbutils.widgets.get("p_batch_id")
v_max_teams = int(dbutils.widgets.get("p_max_teams"))

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

teams_table = f"{catalog_name}.{bronze_schema}.teams"
table_name = f"{catalog_name}.{bronze_schema}.players"

# COMMAND ----------

team_ids = [
    row.idTeam
    for row in (
        spark.table(teams_table)
             .filter(F.col("batch_id") == v_batch_id)
             .select("idTeam")
             .where(F.col("idTeam").isNotNull())
             .dropDuplicates()
             .limit(v_max_teams)
             .collect()
    )
]

# COMMAND ----------

players_df = None
for team_id in team_ids:
    team_players_df = (
        fetch_endpoint_to_df(
            endpoint="lookup_all_players.php",
            dataset_name="players",
            params={"id": team_id}
        )
        .withColumn("requested_team_id", F.lit(team_id))
    )
    players_df = team_players_df if players_df is None else players_df.unionByName(team_players_df, allowMissingColumns=True)

# COMMAND ----------

display(players_df)

# COMMAND ----------

write_to_bronze(
    input_df=players_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
