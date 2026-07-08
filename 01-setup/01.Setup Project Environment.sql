-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set Up TheSportsDB Project Environment

-- COMMAND ----------

-- Update storage credential and ADLS container names for your Azure workspace.
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_sportsdb_incr
URL 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-sport-sc`)
COMMENT 'External location for TheSportsDB incremental project';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS sportsdb_incr
MANAGED LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/'
COMMENT 'Main catalog for TheSportsDB medallion project';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS sportsdb_incr.landing;
CREATE SCHEMA IF NOT EXISTS sportsdb_incr.bronze
    MANAGED LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS sportsdb_incr.silver
    MANAGED LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS sportsdb_incr.gold
    MANAGED LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/gold';
CREATE SCHEMA IF NOT EXISTS sportsdb_incr.control
    MANAGED LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/control';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS sportsdb_incr.landing.files
LOCATION 'abfss://sportsdb-incr@databricksporttdl.dfs.core.windows.net/landing';

-- COMMAND ----------

SHOW SCHEMAS IN sportsdb_incr;
