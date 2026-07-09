# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Users From DummyJSON API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

table_name = f"{catalog_name}.{bronze_schema}.users"

# COMMAND ----------

users_df = fetch_endpoint_to_df(
    endpoint="users",
    dataset_name="users",
    params={"limit": dummyjson_default_limit}
)

# COMMAND ----------

display(users_df)

# COMMAND ----------

write_to_bronze(
    input_df=users_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
