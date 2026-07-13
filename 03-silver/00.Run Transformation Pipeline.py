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

from concurrent.futures import ThreadPoolExecutor


def run_notebook(notebook_path):
    print(f"Running {notebook_path} for batch {v_batch_id}")
    return dbutils.notebook.run(
        notebook_path,
        0,
        {"p_batch_id": v_batch_id},
    )


with ThreadPoolExecutor(max_workers=6) as executor:
    products_future = executor.submit(run_notebook, "./01.Transform Products Data")
    categories_future = executor.submit(run_notebook, "./02.Transform Product Categories Data")
    customers_future = executor.submit(run_notebook, "./03.Transform Customers Data")
    carts_future = executor.submit(run_notebook, "./04.Transform Carts Data")
    cart_items_future = executor.submit(run_notebook, "./05.Transform Cart Items Data")
    product_reviews_future = executor.submit(run_notebook, "./06.Transform Product Reviews Data")

    products_future.result()
    categories_future.result()
    customers_future.result()
    carts_future.result()
    cart_items_future.result()
    product_reviews_future.result()

print(f"Silver transformation pipeline completed for batch {v_batch_id}")
