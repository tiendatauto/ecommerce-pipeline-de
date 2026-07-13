# Databricks notebook source
# MAGIC %md
# MAGIC # Run Bronze Ingestion Pipeline
# MAGIC
# MAGIC Single workflow entry point for all source ingestion and Bronze extraction.

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

ingestion_notebooks = [
    "./01.Ingest Products API",
    "./02.Ingest Product Categories API",
    "./03.Ingest Users API",
    "./04.Ingest Carts API",
    # These two notebooks depend on the products and carts Bronze tables above.
    "./05.Extract Cart Items",
    "./06.Extract Product Reviews",
]

for notebook_path in ingestion_notebooks:
    print(f"Running {notebook_path} for batch {v_batch_id}")
    dbutils.notebook.run(
        notebook_path,
        0,
        {"p_batch_id": v_batch_id},
    )

print(f"Bronze ingestion pipeline completed for batch {v_batch_id}")
