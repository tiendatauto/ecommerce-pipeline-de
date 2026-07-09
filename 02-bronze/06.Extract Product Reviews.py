# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Product Reviews From Product API Payload

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

products_table = f"{catalog_name}.{bronze_schema}.products"
table_name = f"{catalog_name}.{bronze_schema}.product_reviews"

# COMMAND ----------

product_reviews_df = (
    spark.table(products_table)
         .filter(F.col("batch_id") == v_batch_id)
         .withColumn("review", F.explode_outer("reviews"))
         .select(
             F.col("id").alias("productId"),
             F.col("title").alias("productTitle"),
             F.col("category"),
             F.col("review.rating").alias("reviewRating"),
             F.col("review.comment").alias("reviewComment"),
             F.col("review.date").alias("reviewDate"),
             F.col("review.reviewerName").alias("reviewerName"),
             F.col("review.reviewerEmail").alias("reviewerEmail"),
             "ingestion_timestamp",
             "source_api",
             "dataset_name",
         )
)

# COMMAND ----------

display(product_reviews_df)

# COMMAND ----------

write_to_bronze(
    input_df=product_reviews_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
