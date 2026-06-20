{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

/*
  dim_season
  ----------
  One row per F1 season (~76 rows).
  Grain: 1 row per season_year.

  Season completeness rule:
    Champion columns are NULL if no championship_position = 1 exists
    in the EOY standings for that year (i.e. season still in progress).
*/

with season_races as (
    select
        year             as season_year,
        count(*)         as total_races
    from {{ ref('int_race_bridge') }}
    group by year
),

driver_champion as (
    select
        ds.year          as season_year,
        db.full_name     as driver_champion_name,
        ds.championship_points as champion_points,
        ds.season_wins   as champion_wins
    from {{ ref('int_standings_driver_eoy') }} ds
    inner join {{ ref('int_driver_bridge') }} db
        on ds.driver_id = db.driver_id
    where ds.championship_position = 1
),

constructor_champion as (
    select
        cs.year          as season_year,
        cb.constructor_name as constructor_champion_name
    from {{ ref('int_standings_constructor_eoy') }} cs
    inner join {{ ref('int_constructor_bridge') }} cb
        on cs.constructor_id = cb.constructor_id
    where cs.championship_position = 1
)

select
    sr.season_year,
    sr.total_races,
    dc.driver_champion_name,
    cc.constructor_champion_name,
    dc.champion_points,
    dc.champion_wins,
    case
        when sr.season_year between 1950 and 1957 then 'Front-engine'
        when sr.season_year between 1958 and 1965 then 'Rear-engine Revolution'
        when sr.season_year between 1966 and 1976 then '3-Litre Era'
        when sr.season_year between 1977 and 1982 then 'Ground Effect'
        when sr.season_year between 1983 and 1988 then 'Turbo Era'
        when sr.season_year between 1989 and 1994 then 'Naturally Aspirated'
        when sr.season_year between 1995 and 2005 then 'V10 Era'
        when sr.season_year between 2006 and 2013 then 'V8 Era'
        when sr.season_year between 2014 and 2025 then 'Turbo Hybrid V6'
        else '2026 Regulations'
    end                  as era_label,
    current_timestamp    as dw_created_at
from season_races sr
left join driver_champion dc
    on sr.season_year = dc.season_year
left join constructor_champion cc
    on sr.season_year = cc.season_year
