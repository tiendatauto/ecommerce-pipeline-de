# Databricks notebook source
# MAGIC %md
# MAGIC # Build Customer Dimension

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

target_table = f"{catalog_name}.{gold_schema}.dim_customers"

# COMMAND ----------

customers_df = spark.table(f"{catalog_name}.{silver_schema}.customers").filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

dim_customers_df = customers_df.select(
    "customer_id",
    "customer_name",
    "email",
    "phone",
    "gender",
    "age",
    "age_group",
    "birth_date",
    "city",
    "state",
    "country",
    "postal_code",
    "company_name",
    "company_department",
    "job_title",
)

# COMMAND ----------

write_to_gold(
    input_df=dim_customers_df,
    target_table=target_table,
    merge_condition="t.customer_id = s.customer_id",
    columns_to_update=[
        "customer_name",
        "email",
        "phone",
        "gender",
        "age",
        "age_group",
        "birth_date",
        "city",
        "state",
        "country",
        "postal_code",
        "company_name",
        "company_department",
        "job_title",
    ],
)

# COMMAND ----------

display(spark.table(target_table))
