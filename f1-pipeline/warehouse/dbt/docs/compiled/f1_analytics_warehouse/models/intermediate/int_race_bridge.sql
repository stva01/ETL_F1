

/*
  int_race_bridge
  ---------------
  One row per race, all-time (1950–2026+).
  Maps Jolpica (season_year, round) → Kaggle race_id and extends
  the race calendar with 2025+ races that only exist in Jolpica.

  Kaggle races:    1950–2024 (have circuit_id, gp_name, race_date)
  Jolpica races:   2025+     (season_year + round only)

  Synthetic race_id for Jolpica-only races:
    100000 + (season_year * 100) + round
    e.g. 2025 round 1 → 102501
*/

with kaggle_races as (
    select
        race_id,
        year,
        round,
        circuit_id,
        gp_name,
        race_date,
        'kaggle' as data_source
    from "f1_analytics"."main_staging"."stg_kaggle_races"
),

-- All distinct Jolpica seasons/rounds
jolpica_races as (
    select distinct
        season_year as year,
        round
    from "f1_analytics"."main_staging"."stg_jolpica_results"
),

-- Identify Jolpica races NOT already in Kaggle (i.e., 2025+)
jolpica_only as (
    select
        jr.year,
        jr.round,
        -- Synthetic race_id: 100000 + year*100 + round
        (100000 + jr.year * 100 + jr.round) as race_id,
        null::integer                         as circuit_id,
        'Round ' || cast(jr.round as varchar) || ' (' || cast(jr.year as varchar) || ')' as gp_name,
        null::date                            as race_date,
        'jolpica'                             as data_source
    from jolpica_races jr
    left join kaggle_races kr
        on jr.year = kr.year
        and jr.round = kr.round
    where kr.race_id is null
),

unified as (
    select race_id, year, round, circuit_id, gp_name, race_date, data_source
    from kaggle_races

    union all

    select race_id, year, round, circuit_id, gp_name, race_date, data_source
    from jolpica_only
)

select
    race_id,
    year,
    round,
    circuit_id,
    gp_name,
    race_date,
    data_source,
    current_timestamp as _dbt_loaded_at
from unified