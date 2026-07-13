# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Customers Data

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

bronze_table = f"{catalog_name}.{bronze_schema}.users"
silver_table = f"{catalog_name}.{silver_schema}.customers"

# COMMAND ----------

users_df = spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)

# COMMAND ----------

customers_final_df = (
    users_df.select(
        F.col("id").cast("int").alias("customer_id"),
        F.concat_ws(" ", F.col("firstName"), F.col("lastName")).alias("customer_name"),
        F.col("email"),
        F.col("phone"),
        F.col("gender"),
        F.col("age").cast("int").alias("age"),
        F.col("birthDate").cast("date").alias("birth_date"),
        F.col("address.city").alias("city"),
        F.col("address.state").alias("state"),
        F.col("address.country").alias("country"),
        F.col("address.postalCode").alias("postal_code"),
        F.col("company.name").alias("company_name"),
        F.col("company.department").alias("company_department"),
        F.col("company.title").alias("job_title"),
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    )
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .withColumn("customer_name", F.initcap("customer_name"))
    .withColumn(
        "age_group",
        F.when(F.col("age") < 25, F.lit("18-24"))
         .when(F.col("age") < 35, F.lit("25-34"))
         .when(F.col("age") < 45, F.lit("35-44"))
         .when(F.col("age") < 55, F.lit("45-54"))
         .otherwise(F.lit("55+"))
    )
)

# COMMAND ----------

write_to_silver(
    input_df=customers_final_df,
    target_table=silver_table,
    merge_condition="t.customer_id = s.customer_id",
    columns_to_update=[
        "customer_name",
        "email",
        "phone",
        "gender",
        "age",
        "birth_date",
        "city",
        "state",
        "country",
        "postal_code",
        "company_name",
        "company_department",
        "job_title",
        "age_group",
        "ingestion_timestamp",
        "source_api",
        "batch_id",
    ],
)

# COMMAND ----------

display(spark.table(silver_table))
