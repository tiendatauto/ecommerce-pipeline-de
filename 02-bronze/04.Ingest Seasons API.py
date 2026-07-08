# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest League Seasons From TheSportsDB API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
dbutils.widgets.text("p_league_ids", ",".join(sportsdb_default_league_ids) if "sportsdb_default_league_ids" in globals() else "4328")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

v_league_ids = [
    league_id.strip()
    for league_id in dbutils.widgets.get("p_league_ids").split(",")
    if league_id.strip()
]
table_name = f"{catalog_name}.{bronze_schema}.seasons"

# COMMAND ----------

seasons_df = None
for league_id in v_league_ids:
    league_seasons_df = (
        fetch_endpoint_to_df(
            endpoint="search_all_seasons.php",
            dataset_name="seasons",
            params={"id": league_id}
        )
        .withColumn("requested_league_id", F.lit(league_id))
    )
    seasons_df = league_seasons_df if seasons_df is None else seasons_df.unionByName(league_seasons_df, allowMissingColumns=True)

# COMMAND ----------

display(seasons_df)

# COMMAND ----------

write_to_bronze(
    input_df=seasons_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
