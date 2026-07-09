# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Products Data

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.products"
silver_table = f"{catalog_name}.{silver_schema}.products"

# COMMAND ----------

products_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

products_final_df = (
    products_df.select(
        F.col("id").cast("int").alias("product_id"),
        F.col("title").alias("product_name"),
        F.col("description").alias("product_description"),
        F.col("category").alias("category_slug"),
        F.col("brand").alias("brand_name"),
        F.col("sku"),
        F.col("price").cast("double").alias("list_price"),
        F.col("discountPercentage").cast("double").alias("discount_pct"),
        F.round(F.col("price") * (1 - F.col("discountPercentage") / 100), 2).alias("net_unit_price"),
        F.col("rating").cast("double").alias("rating"),
        F.col("stock").cast("int").alias("stock_quantity"),
        F.col("availabilityStatus").alias("availability_status"),
        F.col("minimumOrderQuantity").cast("int").alias("minimum_order_quantity"),
        F.col("warrantyInformation").alias("warranty_information"),
        F.col("shippingInformation").alias("shipping_information"),
        F.col("returnPolicy").alias("return_policy"),
        F.col("weight").cast("double").alias("weight"),
        F.col("dimensions.width").cast("double").alias("width"),
        F.col("dimensions.height").cast("double").alias("height"),
        F.col("dimensions.depth").cast("double").alias("depth"),
        F.col("thumbnail").alias("thumbnail_url"),
        F.col("tags"),
        F.col("ingestion_timestamp"),
        F.col("source_api"),
        F.col("batch_id"),
    )
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
    .withColumn("product_name", F.initcap("product_name"))
    .withColumn("category_slug", F.lower("category_slug"))
    .withColumn(
        "inventory_risk",
        F.when(F.col("stock_quantity") <= 10, F.lit("LOW_STOCK"))
         .when(F.col("stock_quantity") <= 50, F.lit("WATCH"))
         .otherwise(F.lit("HEALTHY"))
    )
)

# COMMAND ----------

write_to_silver(
    input_df=products_final_df,
    target_table=silver_table,
    merge_condition="t.product_id = s.product_id",
    columns_to_update=[
        "product_name",
        "product_description",
        "category_slug",
        "brand_name",
        "sku",
        "list_price",
        "discount_pct",
        "net_unit_price",
        "rating",
        "stock_quantity",
        "availability_status",
        "minimum_order_quantity",
        "warranty_information",
        "shipping_information",
        "return_policy",
        "weight",
        "width",
        "height",
        "depth",
        "thumbnail_url",
        "tags",
        "inventory_risk",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
