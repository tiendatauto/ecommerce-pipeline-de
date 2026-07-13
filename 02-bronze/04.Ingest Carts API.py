# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Carts From DummyJSON API

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

table_name = f"{catalog_name}.{bronze_schema}.carts"

# COMMAND ----------

carts_df = fetch_endpoint_to_df(
    endpoint="carts",
    dataset_name="carts",
    params={"limit": dummyjson_default_limit}
)

# COMMAND ----------

display(carts_df)

# COMMAND ----------

write_to_bronze(
    input_df=carts_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
