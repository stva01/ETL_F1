{{
  config(
    materialized='table',
    tags=['mart', 'gold']
  )
}}

/*
  mart_season_summary
  -------------------
  One row per F1 season (~76 rows).
  Grain: 1 row per season_year.

  Dashboard tabs: Overview (races per decade), Seasons (stat cards),
                  Records (champions table).

  Sources:
    dim_season              → champion names, era_label
    fct_race_result         → per-season race analytics
*/

with season_race_metrics as (
    select
        season_year,
        count(distinct race_key)                                  as total_races,
        count(distinct case when is_winner      then driver_key end) as unique_winners,
        count(distinct case when is_pole        then driver_key end) as unique_pole_sitters,
        sum(case when is_dnf                    then 1 else 0 end)   as total_dnfs
    from {{ ref('fct_race_result') }}
    group by season_year
),

-- Champion stats for that season (joins to their per-season performance)
champion_season as (
    select
        ds.season_year,
        ds.driver_champion_name,
        ds.constructor_champion_name,
        ds.champion_points,
        ds.champion_wins
    from {{ ref('dim_season') }} ds
),

-- Closest championship gap: points difference between position 1 and 2
championship_gap as (
    select
        year        as season_year,
        max(case when championship_position = 1 then championship_points end)
        - max(case when championship_position = 2 then championship_points end)
            as closest_championship_gap
    from {{ ref('int_standings_driver_eoy') }}
    group by year
)

select
    srm.season_year,
    srm.total_races,
    cs.driver_champion_name      as driver_champion,
    cs.constructor_champion_name as constructor_champion,
    cs.champion_wins,
    cs.champion_points,
    srm.unique_winners,
    srm.unique_pole_sitters,
    srm.total_dnfs,
    cg.closest_championship_gap,
    ds.era_label,
    current_timestamp            as last_updated_at
from season_race_metrics srm
left join champion_season cs
    on srm.season_year = cs.season_year
left join championship_gap cg
    on srm.season_year = cg.season_year
left join {{ ref('dim_season') }} ds
    on srm.season_year = ds.season_year
