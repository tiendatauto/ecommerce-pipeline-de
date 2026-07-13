# Databricks notebook source
# MAGIC %md
# MAGIC # Run Gold Pipeline
# MAGIC
# MAGIC Single workflow entry point for all Silver-to-Gold processing.

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

# Dimensions must be available before the fact notebooks join to them.
gold_notebooks = [
    "./01.Build Product Dimension",
    "./02.Build Customer Dimension",
    "./03.Build Cart Dimension",
    "./04.Build Sales Fact",
    "./05.Build Product Review Fact",
]

for notebook_path in gold_notebooks:
    print(f"Running {notebook_path} for batch {v_batch_id}")
    dbutils.notebook.run(
        notebook_path,
        0,
        {"p_batch_id": v_batch_id},
    )

print(f"Gold pipeline completed for batch {v_batch_id}")
