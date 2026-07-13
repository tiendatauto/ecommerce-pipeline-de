# Databricks notebook source
# MAGIC %md
# MAGIC # Build Product Dimension

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_products"

# COMMAND ----------

products_df = spark.table(f"{catalog_name}.{silver_schema}.products").filter(F.col("batch_id") == v_batch_id)
categories_df = spark.table(f"{catalog_name}.{silver_schema}.product_categories")

# COMMAND ----------

dim_products_df = (
    products_df.alias("p")
    .join(categories_df.alias("c"), F.col("p.category_slug") == F.col("c.category_slug"), "left")
    .select(
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.product_description"),
        F.col("p.category_slug"),
        F.coalesce(F.col("c.category_name"), F.initcap(F.regexp_replace(F.col("p.category_slug"), "-", " "))).alias("category_name"),
        F.col("p.brand_name"),
        F.col("p.sku"),
        F.col("p.list_price"),
        F.col("p.discount_pct"),
        F.col("p.net_unit_price"),
        F.col("p.rating"),
        F.col("p.stock_quantity"),
        F.col("p.availability_status"),
        F.col("p.inventory_risk"),
        F.col("p.minimum_order_quantity"),
        F.col("p.warranty_information"),
        F.col("p.shipping_information"),
        F.col("p.return_policy"),
        F.col("p.weight"),
        F.col("p.width"),
        F.col("p.height"),
        F.col("p.depth"),
        F.col("p.thumbnail_url"),
    )
)

# COMMAND ----------

write_to_gold(
    input_df=dim_products_df,
    target_table=target_table,
    merge_condition="t.product_id = s.product_id",
    columns_to_update=[
        "product_name",
        "product_description",
        "category_slug",
        "category_name",
        "brand_name",
        "sku",
        "list_price",
        "discount_pct",
        "net_unit_price",
        "rating",
        "stock_quantity",
        "availability_status",
        "inventory_risk",
        "minimum_order_quantity",
        "warranty_information",
        "shipping_information",
        "return_policy",
        "weight",
        "width",
        "height",
        "depth",
        "thumbnail_url",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
