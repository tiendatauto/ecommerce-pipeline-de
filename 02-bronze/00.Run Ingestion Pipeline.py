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

from concurrent.futures import ThreadPoolExecutor


def run_notebook(notebook_path):
    print(f"Running {notebook_path} for batch {v_batch_id}")
    return dbutils.notebook.run(
        notebook_path,
        0,
        {"p_batch_id": v_batch_id},
    )


def run_notebook_after(notebook_path, upstream_future):
    upstream_future.result()
    return run_notebook(notebook_path)


with ThreadPoolExecutor(max_workers=6) as executor:
    products_future = executor.submit(run_notebook, "./01.Ingest Products API")
    categories_future = executor.submit(run_notebook, "./02.Ingest Product Categories API")
    users_future = executor.submit(run_notebook, "./03.Ingest Users API")
    carts_future = executor.submit(run_notebook, "./04.Ingest Carts API")

    cart_items_future = executor.submit(
        run_notebook_after,
        "./05.Extract Cart Items",
        carts_future,
    )
    product_reviews_future = executor.submit(
        run_notebook_after,
        "./06.Extract Product Reviews",
        products_future,
    )

    products_future.result()
    categories_future.result()
    users_future.result()
    carts_future.result()
    cart_items_future.result()
    product_reviews_future.result()

print(f"Bronze ingestion pipeline completed for batch {v_batch_id}")
