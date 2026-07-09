-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set Up DummyJSON E-Commerce Project Environment

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_sport_tdl_ecommerce_incr
URL 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for DummyJSON e-commerce incremental project';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ecommerce_incr
MANAGED LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/'
COMMENT 'Main catalog for DummyJSON e-commerce medallion project';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ecommerce_incr.landing;
CREATE SCHEMA IF NOT EXISTS ecommerce_incr.bronze
    MANAGED LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS ecommerce_incr.silver
    MANAGED LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS ecommerce_incr.gold
    MANAGED LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/gold';
CREATE SCHEMA IF NOT EXISTS ecommerce_incr.control
    MANAGED LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/control';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS ecommerce_incr.landing.files
LOCATION 'abfss://ecommerce-incr@databricksporttdl.dfs.core.windows.net/landing';

-- COMMAND ----------

SHOW SCHEMAS IN ecommerce_incr;
