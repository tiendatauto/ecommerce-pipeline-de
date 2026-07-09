# Databricks notebook source
# Helper functions for DummyJSON API ingestion

import time
import urllib.parse

import requests
from pyspark.sql import functions as F
from pyspark.sql import types as T


# COMMAND ----------

# MAGIC %run ./05.api-schema-helpers

# COMMAND ----------


def fetch_dummyjson_json(endpoint, params=None):
    encoded_endpoint = "/".join(
        urllib.parse.quote(str(part).strip("/"))
        for part in endpoint.strip("/").split("/")
    )
    url = f"{dummyjson_api_base_url}/{encoded_endpoint}"
    response = requests.get(url, params=params, timeout=60)

    if response.status_code == 429:
        time.sleep(65)
        response = requests.get(url, params=params, timeout=60)

    response.raise_for_status()
    return response.json(), response.url


def extract_records(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ["products", "carts", "users", "categories"]:
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [payload]
    return []


def api_records_to_df(records, dataset_name, source_url):
    if records:
        schema = infer_records_schema(records)
        normalized_records = [normalize_record(record, schema) for record in records]
        df = spark.createDataFrame(normalized_records, schema)
    else:
        df = spark.createDataFrame([], T.StructType([]))

    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_api", F.lit(source_url))
          .withColumn("dataset_name", F.lit(dataset_name))
    )


def fetch_endpoint_to_df(endpoint, dataset_name, params=None):
    payload, source_url = fetch_dummyjson_json(endpoint, params=params)
    return api_records_to_df(extract_records(payload), dataset_name, source_url)


# COMMAND ----------


def write_to_bronze(
    input_df,
    target_table,
    batch_id
):
    final_df = input_df.withColumn("batch_id", F.lit(batch_id))
    (
        final_df
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("batch_id")
            .option("replaceWhere", f"batch_id = '{batch_id}'")
            .saveAsTable(target_table)
    )
