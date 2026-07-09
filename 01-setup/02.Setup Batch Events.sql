-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup Batch Control

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ecommerce_incr.control
    MANAGED LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/control';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ecommerce_incr.control.batch_control
(
    batch_id STRING,
    status STRING,
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP
);

-- COMMAND ----------

SELECT * FROM ecommerce_incr.control.batch_control;
