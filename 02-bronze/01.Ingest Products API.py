# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Products From DummyJSON API

# COMMAND ----------

from datetime import datetime

v_batch_id = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

table_name = f"{catalog_name}.{bronze_schema}.products"

# COMMAND ----------

products_df = fetch_endpoint_to_df(
    endpoint="products",
    dataset_name="products",
    params={"limit": dummyjson_default_limit}
)

# COMMAND ----------

display(products_df)

# COMMAND ----------

write_to_bronze(
    input_df=products_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
