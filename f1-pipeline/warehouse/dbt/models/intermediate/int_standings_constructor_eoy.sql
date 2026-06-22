{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing'},
    tags=['intermediate', 'standings', 'constructor']
  )
}}

/*
  int_standings_constructor_eoy
  -----------------------------
  One row per constructor per season — the FINAL championship standing
  after the last race of each year.

  Sources:
    - stg_kaggle_constructor_standings (1958–2024)
    - stg_jolpica_constructor_standings (2025+)
*/

with kaggle_ranked as (
    select
        cast(constructor_id as integer) as constructor_id,
        cast(race_id as integer)        as race_id,
        cast(year as integer)           as year,
        cast(position as integer)       as position,
        cast(points as double)          as points,
        cast(wins as integer)           as wins,
        row_number() over (
            partition by cast(year as integer), cast(constructor_id as integer)
            order by cast(race_id as integer) desc
        ) as rn
    from {{ ref('stg_kaggle_constructor_standings') }}
),

kaggle_eoy as (
    select
        constructor_id,
        year,
        position  as championship_position,
        points    as championship_points,
        wins      as season_wins
    from kaggle_ranked
    where rn = 1
),

jolpica_ranked as (
    select
        cast(season_year as integer)   as season_year,
        cast(round as integer)         as round,
        cast(constructor_id as varchar) as constructor_id,
        cast(position as integer)      as position,
        cast(points as double)         as points,
        cast(wins as integer)          as wins,
        row_number() over (
            partition by cast(season_year as integer), cast(constructor_id as varchar)
            order by cast(round as integer) desc
        ) as rn
    from {{ ref('stg_jolpica_constructor_standings') }}
),

jolpica_eoy as (
    select
        cb.constructor_id,
        ranked.season_year       as year,
        ranked.position          as championship_position,
        ranked.points            as championship_points,
        ranked.wins              as season_wins
    from jolpica_ranked ranked
    inner join {{ ref('int_constructor_bridge') }} cb
        on ranked.constructor_id = cb.jolpica_constructor_id
    where ranked.rn = 1
),

unified as (
    select cast(constructor_id as bigint) as constructor_id, year, championship_position, championship_points, season_wins,
           'kaggle' as source_system
    from kaggle_eoy

    union all

    select cast(constructor_id as bigint) as constructor_id, year, championship_position, championship_points, season_wins,
           'jolpica' as source_system
    from jolpica_eoy
)

select
    constructor_id,
    year,
    championship_position,
    championship_points,
    season_wins,
    source_system,
    current_timestamp as _dbt_loaded_at
from unified
