# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest League Events From TheSportsDB API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
dbutils.widgets.text("p_league_ids", ",".join(sportsdb_default_league_ids) if "sportsdb_default_league_ids" in globals() else "4328")
dbutils.widgets.text("p_season", sportsdb_default_season if "sportsdb_default_season" in globals() else "2025-2026")
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
v_season = dbutils.widgets.get("p_season")
table_name = f"{catalog_name}.{bronze_schema}.events"

# COMMAND ----------

events_df = None
for league_id in v_league_ids:
    league_events_df = (
        fetch_endpoint_to_df(
            endpoint="eventsseason.php",
            dataset_name="events",
            params={"id": league_id, "s": v_season}
        )
        .withColumn("requested_league_id", F.lit(league_id))
        .withColumn("requested_season", F.lit(v_season))
    )
    events_df = league_events_df if events_df is None else events_df.unionByName(league_events_df, allowMissingColumns=True)

# COMMAND ----------

display(events_df)

# COMMAND ----------

write_to_bronze(
    input_df=events_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
