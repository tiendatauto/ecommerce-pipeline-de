# Databricks notebook source
# MAGIC %md
# MAGIC # Run Silver Transformation Pipeline
# MAGIC
# MAGIC Single workflow entry point for all Bronze-to-Silver transformations.

# COMMAND ----------

# MAGIC %run ../00-common/06.batch-helpers

# COMMAND ----------

v_batch_id = get_batch_id()

# COMMAND ----------

transformation_notebooks = [
    "./01.Transform Products Data",
    "./02.Transform Product Categories Data",
    "./03.Transform Customers Data",
    "./04.Transform Carts Data",
    "./05.Transform Cart Items Data",
    "./06.Transform Product Reviews Data",
]

for notebook_path in transformation_notebooks:
    print(f"Running {notebook_path} for batch {v_batch_id}")
    dbutils.notebook.run(
        notebook_path,
        0,
        {"p_batch_id": v_batch_id},
    )

print(f"Silver transformation pipeline completed for batch {v_batch_id}")
