-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Fixture Coverage View

-- COMMAND ----------

CREATE OR REPLACE VIEW sportsdb_incr.gold.v_fixture_coverage
AS
SELECT
    league_id,
    league_name,
    sport_name,
    season,
    COUNT(*) AS total_events,
    COUNT_IF(is_played) AS played_events,
    COUNT_IF(NOT is_played) AS scheduled_events,
    ROUND(COUNT_IF(is_played) / COUNT(*) * 100, 2) AS played_event_pct,
    MIN(event_date) AS first_event_date,
    MAX(event_date) AS last_event_date
FROM sportsdb_incr.gold.dim_events
GROUP BY
    league_id,
    league_name,
    sport_name,
    season;

-- COMMAND ----------

SELECT * FROM sportsdb_incr.gold.v_fixture_coverage;
