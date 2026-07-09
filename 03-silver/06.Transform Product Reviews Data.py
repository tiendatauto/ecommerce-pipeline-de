# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Product Reviews Data

# COMMAND ----------

from datetime import datetime
v_batch_id = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.product_reviews"
silver_table = f"{catalog_name}.{silver_schema}.product_reviews"

# COMMAND ----------

reviews_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

reviews_final_df = (
    reviews_df.select(
        F.col("productId").cast("int").alias("product_id"),
        F.col("productTitle").alias("product_name"),
        F.lower("category").alias("category_slug"),
        F.col("reviewRating").cast("int").alias("review_rating"),
        F.col("reviewComment").alias("review_comment"),
        F.col("reviewDate").cast("timestamp").alias("review_timestamp"),
        F.col("reviewerName").alias("reviewer_name"),
        F.col("reviewerEmail").alias("reviewer_email"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("product_id").isNotNull() & F.col("reviewer_email").isNotNull())
    .dropDuplicates(["product_id", "reviewer_email", "review_timestamp"])
    .withColumn(
        "sentiment_label",
        F.when(F.col("review_rating") >= 4, F.lit("POSITIVE"))
         .when(F.col("review_rating") == 3, F.lit("NEUTRAL"))
         .otherwise(F.lit("NEGATIVE"))
    )
)

# COMMAND ----------

write_to_silver(
    input_df=reviews_final_df,
    target_table=silver_table,
    merge_condition="t.product_id = s.product_id AND t.reviewer_email = s.reviewer_email AND t.review_timestamp = s.review_timestamp",
    columns_to_update=[
        "product_name",
        "category_slug",
        "review_rating",
        "review_comment",
        "reviewer_name",
        "sentiment_label",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
