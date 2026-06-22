{{
    config(
        materialized='table',
        tags=['mart', 'constructor', 'season']
    )
}}

WITH constructor_results AS (
    SELECT
        r.season_year,
        r.race_key,
        rr.constructor_key,
        c.constructor_name,
        MAX(CASE WHEN rr.finish_position = 1 THEN 1 ELSE 0 END) as is_win,
        SUM(rr.points_scored) as race_points
    FROM {{ ref('fct_race_result') }} rr
    JOIN {{ ref('dim_race') }} r ON rr.race_key = r.race_key
    JOIN {{ ref('dim_constructor') }} c ON rr.constructor_key = c.constructor_key
    GROUP BY 
        r.season_year,
        r.race_key,
        rr.constructor_key,
        c.constructor_name
)

SELECT
    constructor_key,
    constructor_name,
    season_year,
    SUM(is_win) as season_wins,
    SUM(race_points) as season_points
FROM constructor_results
GROUP BY
    constructor_key,
    constructor_name,
    season_year
