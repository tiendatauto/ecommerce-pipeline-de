# Databricks notebook source
# Unity Catalog Object Names
catalog_name = 'sportsdb_incr'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'
control_schema = 'control'

# COMMAND ----------

landing_folder_path = '/Volumes/sportsdb_incr/landing/files'

# TheSportsDB v1 API configuration. Public free key: 123.
sportsdb_api_key = "123"
sportsdb_api_base_url = f"https://www.thesportsdb.com/api/v1/json/{sportsdb_api_key}"
sportsdb_default_league_ids = ["4328"]  # English Premier League
sportsdb_default_season = "2025-2026"
