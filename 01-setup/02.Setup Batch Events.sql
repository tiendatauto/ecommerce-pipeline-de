-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup Batch Control

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sportsdb_incr.control
    MANAGED LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/control';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sportsdb_incr.control.batch_control
(
    batch_id STRING,
    status STRING,
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
);

-- COMMAND ----------

SELECT * FROM sportsdb_incr.control.batch_control;
