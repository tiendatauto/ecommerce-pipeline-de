# Databricks notebook source
# MAGIC %md
# MAGIC # Build Sales Fact

# COMMAND ----------

from datetime import datetime
v_batch_id = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_sales"

# COMMAND ----------

cart_items_df = spark.table(f"{catalog_name}.{silver_schema}.cart_items").filter(F.col("batch_id") == v_batch_id)
products_df = spark.table(f"{catalog_name}.{gold_schema}.dim_products")
carts_df = spark.table(f"{catalog_name}.{gold_schema}.dim_carts")

# COMMAND ----------

fact_sales_df = (
    cart_items_df.alias("ci")
    .join(products_df.alias("p"), F.col("ci.product_id") == F.col("p.product_id"), "left")
    .join(carts_df.alias("c"), F.col("ci.cart_id") == F.col("c.cart_id"), "left")
    .select(
        F.concat_ws("-", F.col("ci.cart_id"), F.col("ci.product_id")).alias("sales_line_id"),
        F.col("ci.cart_id"),
        F.coalesce(F.col("c.customer_id"), F.col("ci.customer_id")).alias("customer_id"),
        F.col("ci.product_id"),
        F.coalesce(F.col("p.product_name"), F.col("ci.product_name")).alias("product_name"),
        F.col("p.category_slug"),
        F.col("p.category_name"),
        F.col("p.brand_name"),
        F.col("ci.unit_price"),
        F.col("ci.quantity"),
        F.col("ci.line_gross_amount"),
        F.col("ci.line_discount_pct"),
        F.col("ci.line_discount_amount"),
        F.col("ci.line_net_amount"),
        F.col("p.stock_quantity"),
        F.col("p.inventory_risk"),
        F.current_date().alias("snapshot_date"),
    )
    .withColumn(
        "gross_margin_proxy",
        F.round(F.col("line_net_amount") * 0.35, 2)
    )
)

# COMMAND ----------

write_to_gold(
    input_df=fact_sales_df,
    target_table=target_table,
    merge_condition="t.sales_line_id = s.sales_line_id",
    columns_to_update=[
        "cart_id",
        "customer_id",
        "product_id",
        "product_name",
        "category_slug",
        "category_name",
        "brand_name",
        "unit_price",
        "quantity",
        "line_gross_amount",
        "line_discount_pct",
        "line_discount_amount",
        "line_net_amount",
        "stock_quantity",
        "inventory_risk",
        "snapshot_date",
        "gross_margin_proxy",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
