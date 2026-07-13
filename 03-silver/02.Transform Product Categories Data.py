# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Product Categories Data

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.product_categories"
silver_table = f"{catalog_name}.{silver_schema}.product_categories"

# COMMAND ----------

categories_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

categories_final_df = (
    categories_df.select(
        F.col("slug").alias("category_slug"),
        F.col("name").alias("category_name"),
        F.col("url").alias("category_url"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("category_slug").isNotNull())
    .dropDuplicates(["category_slug"])
    .withColumn("category_slug", F.lower("category_slug"))
    .withColumn("category_name", F.initcap("category_name"))
)

# COMMAND ----------

write_to_silver(
    input_df=categories_final_df,
    target_table=silver_table,
    merge_condition="t.category_slug = s.category_slug",
    columns_to_update=[
        "category_name",
        "category_url",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
