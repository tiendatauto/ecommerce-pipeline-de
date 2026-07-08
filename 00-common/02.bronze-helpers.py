# Databricks notebook source
# Helper functions for TheSportsDB API ingestion

import json
import time
import urllib.parse

import requests
from pyspark.sql import functions as F


def fetch_sportsdb_json(endpoint, params=None):
    encoded_endpoint = "/".join(
        urllib.parse.quote(str(part).strip("/").replace(" ", "_"))
        for part in endpoint.strip("/").split("/")
    )
    url = f"{sportsdb_api_base_url}/{encoded_endpoint}"
    response = requests.get(
        url,
        params=params,
        timeout=60,
    )

    if response.status_code == 429:
        time.sleep(65)
        response = requests.get(
            url,
            params=params,
            timeout=60,
        )

    response.raise_for_status()
    return response.json(), response.url


def extract_records(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list):
                return value
        return [payload]
    return []


def api_records_to_df(records, dataset_name, source_url):
    json_rows = [json.dumps(record) for record in records]
    if json_rows:
        df = spark.read.json(sc.parallelize(json_rows))
    else:
        df = spark.read.json(sc.parallelize(["{}"])).limit(0)

    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_api", F.lit(source_url))
          .withColumn("dataset_name", F.lit(dataset_name))
    )


def fetch_endpoint_to_df(endpoint, dataset_name, params=None):
    payload, source_url = fetch_sportsdb_json(endpoint, params=params)
    return api_records_to_df(extract_records(payload), dataset_name, source_url)


# COMMAND ----------


def add_ingestion_metadata(df):
    if "_metadata" in df.columns:
        return (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("source_file", F.col("_metadata.file_path"))
        )
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_file", F.lit(None).cast("string"))
    )


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
