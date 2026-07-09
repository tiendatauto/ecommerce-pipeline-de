# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Product Categories From DummyJSON API

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

table_name = f"{catalog_name}.{bronze_schema}.product_categories"

# COMMAND ----------

categories_df = fetch_endpoint_to_df(
    endpoint="products/categories",
    dataset_name="product_categories"
)

# COMMAND ----------

display(categories_df)

# COMMAND ----------

write_to_bronze(
    input_df=categories_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
