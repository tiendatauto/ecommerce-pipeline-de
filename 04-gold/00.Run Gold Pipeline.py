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

from concurrent.futures import ThreadPoolExecutor


def run_notebook(notebook_path):
    print(f"Running {notebook_path} for batch {v_batch_id}")
    return dbutils.notebook.run(
        notebook_path,
        0,
        {"p_batch_id": v_batch_id},
    )


def run_notebook_after(notebook_path, first_future, second_future=None):
    first_future.result()
    if second_future is not None:
        second_future.result()
    return run_notebook(notebook_path)


with ThreadPoolExecutor(max_workers=5) as executor:
    products_dimension_future = executor.submit(
        run_notebook,
        "./01.Build Product Dimension",
    )
    customers_dimension_future = executor.submit(
        run_notebook,
        "./02.Build Customer Dimension",
    )
    carts_dimension_future = executor.submit(
        run_notebook,
        "./03.Build Cart Dimension",
    )

    sales_fact_future = executor.submit(
        run_notebook_after,
        "./04.Build Sales Fact",
        products_dimension_future,
        carts_dimension_future,
    )
    product_reviews_fact_future = executor.submit(
        run_notebook_after,
        "./05.Build Product Review Fact",
        products_dimension_future,
    )

    products_dimension_future.result()
    customers_dimension_future.result()
    carts_dimension_future.result()
    sales_fact_future.result()
    product_reviews_fact_future.result()

print(f"Gold pipeline completed for batch {v_batch_id}")
