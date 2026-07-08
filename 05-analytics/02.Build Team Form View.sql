-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Team Form View

-- COMMAND ----------

CREATE OR REPLACE VIEW sportsdb_incr.gold.v_team_form
AS
WITH played_results AS (
    SELECT
        league_id,
        league_name,
        season,
        team_id,
        team_name,
        event_id,
        event_date,
        opponent_team_id,
        opponent_team_name,
        venue_side,
        goals_for,
        goals_against,
        result_code,
        standing_points,
        ROW_NUMBER() OVER (
            PARTITION BY league_id, season, team_id
            ORDER BY event_date DESC, event_id DESC
        ) AS recency_rank
    FROM sportsdb_incr.gold.fact_team_event_results
    WHERE is_played
),
last_five AS (
    SELECT *
    FROM played_results
    WHERE recency_rank <= 5
)
SELECT
    league_id,
    league_name,
    season,
    team_id,
    team_name,
    COUNT(*) AS matches_in_form_window,
    SUM(standing_points) AS form_points,
    COUNT_IF(result_code = 'W') AS form_wins,
    COUNT_IF(result_code = 'D') AS form_draws,
    COUNT_IF(result_code = 'L') AS form_losses,
    SUM(goals_for) AS form_goals_for,
    SUM(goals_against) AS form_goals_against,
    ARRAY_JOIN(
        TRANSFORM(
            SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT('rank', recency_rank, 'result', result_code))),
            result_item -> result_item.result
        ),
        ''
    ) AS latest_results
FROM last_five
GROUP BY
    league_id,
    league_name,
    season,
    team_id,
    team_name;

-- COMMAND ----------

SELECT * FROM sportsdb_incr.gold.v_team_form;
