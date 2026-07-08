-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Team Standings View

-- COMMAND ----------

CREATE OR REPLACE VIEW sportsdb_incr.gold.v_team_standings
AS
WITH team_results AS (
    SELECT
        league_id,
        league_name,
        sport_name,
        season,
        team_id,
        team_name,
        COUNT_IF(is_played) AS matches_played,
        SUM(standing_points) AS total_points,
        SUM(goals_for) AS goals_for,
        SUM(goals_against) AS goals_against,
        SUM(goal_difference) AS goal_difference,
        COUNT_IF(is_win) AS wins,
        COUNT_IF(is_draw) AS draws,
        COUNT_IF(is_loss) AS losses,
        COUNT_IF(venue_side = 'HOME' AND is_win) AS home_wins,
        COUNT_IF(venue_side = 'AWAY' AND is_win) AS away_wins
    FROM sportsdb_incr.gold.fact_team_event_results
    WHERE is_played
    GROUP BY
        league_id,
        league_name,
        sport_name,
        season,
        team_id,
        team_name
)
SELECT
    league_id,
    league_name,
    sport_name,
    season,
    RANK() OVER (
        PARTITION BY league_id, season
        ORDER BY total_points DESC, goal_difference DESC, goals_for DESC, wins DESC
    ) AS standing,
    team_id,
    team_name,
    matches_played,
    total_points,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_difference,
    home_wins,
    away_wins
FROM team_results;

-- COMMAND ----------

SELECT * FROM sportsdb_incr.gold.v_team_standings;
