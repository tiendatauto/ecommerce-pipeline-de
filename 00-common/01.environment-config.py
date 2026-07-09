# Databricks notebook source
# Unity Catalog Object Names
catalog_name = 'ecommerce_incr'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
control_schema = 'control'

# COMMAND ----------

landing_folder_path = '/Volumes/ecommerce_incr/landing/files'

# DummyJSON API configuration for e-commerce datasets.
dummyjson_api_base_url = "https://dummyjson.com"
dummyjson_default_limit = 0
