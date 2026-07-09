# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Cart Items From Cart API Payload

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

carts_table = f"{catalog_name}.{bronze_schema}.carts"
table_name = f"{catalog_name}.{bronze_schema}.cart_items"

# COMMAND ----------

cart_items_df = (
    spark.table(carts_table)
         .filter(F.col("batch_id") == v_batch_id)
         .withColumn("cart_product", F.explode_outer("products"))
         .select(
             F.col("id").alias("cartId"),
             F.col("userId"),
             F.col("total").alias("cartTotal"),
             F.col("discountedTotal").alias("cartDiscountedTotal"),
             F.col("totalProducts"),
             F.col("totalQuantity"),
             F.col("cart_product.id").alias("productId"),
             F.col("cart_product.title").alias("title"),
             F.col("cart_product.price").alias("price"),
             F.col("cart_product.quantity").alias("quantity"),
             F.col("cart_product.total").alias("lineTotal"),
             F.col("cart_product.discountPercentage").alias("discountPercentage"),
             F.col("cart_product.discountedTotal").alias("lineDiscountedTotal"),
             "ingestion_timestamp",
             "source_api",
             "dataset_name",
         )
)

# COMMAND ----------

display(cart_items_df)

# COMMAND ----------

write_to_bronze(
    input_df=cart_items_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))
