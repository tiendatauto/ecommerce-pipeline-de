# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Leagues From TheSportsDB API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

table_name = f"{catalog_name}.{bronze_schema}.leagues"

# COMMAND ----------

leagues_df = fetch_endpoint_to_df(
    endpoint="all_leagues.php",
    dataset_name="leagues"
)

# COMMAND ----------

display(leagues_df)

# COMMAND ----------

write_to_bronze(
    input_df=leagues_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
