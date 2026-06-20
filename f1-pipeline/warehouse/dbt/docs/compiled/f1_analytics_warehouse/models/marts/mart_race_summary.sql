

/*
  mart_race_summary
  -----------------
  One row per race (~1,120 rows).
  Grain: 1 row per race_key.

  Dashboard tabs: Seasons (race results table).

  Sources:
    dim_race          → race context (name, date, circuit)
    fct_race_result   → winner, starters, finishers, DNFs
    fct_qualifying    → pole sitter
*/

with race_winner as (
    select
        race_key,
        driver_key,
        constructor_key
    from "f1_analytics"."main_marts"."fct_race_result"
    where is_winner = true
    -- Guard: take only one winner per race (in case of a data anomaly)
    qualify row_number() over (partition by race_key order by result_id) = 1
),

pole_sitter as (
    select
        race_key,
        driver_key
    from "f1_analytics"."main_marts"."fct_qualifying"
    where is_pole_position = true
    qualify row_number() over (partition by race_key order by qualify_id) = 1
),

fastest_lap_holder as (
    select
        race_key,
        driver_key
    from "f1_analytics"."main_marts"."fct_race_result"
    where is_fastest_lap = true
    qualify row_number() over (partition by race_key order by result_id) = 1
),

race_counts as (
    select
        race_key,
        count(*)                               as total_starters,
        count(case when not is_dnf then 1 end) as total_finishers,
        sum(case when is_dnf then 1 else 0 end) as total_dnfs
    from "f1_analytics"."main_marts"."fct_race_result"
    group by race_key
)

select
    dr.race_key,
    dr.season_year,
    dr.round_number,
    dr.race_name,
    dc.circuit_name,
    dr.race_date,

    -- Winner
    dw.full_name                as winner_name,
    dcon.constructor_name       as winner_constructor,

    -- Pole sitter
    dp.full_name                as pole_sitter_name,

    -- Fastest lap
    df.full_name                as fastest_lap_driver,

    -- Field stats
    coalesce(rc.total_starters, 0)   as total_starters,
    coalesce(rc.total_finishers, 0)  as total_finishers,
    coalesce(rc.total_dnfs, 0)       as total_dnfs,

    current_timestamp           as last_updated_at
from "f1_analytics"."main_marts"."dim_race" dr

-- Circuit name
left join "f1_analytics"."main_marts"."dim_circuit" dc
    on dr.circuit_key = dc.circuit_key

-- Race winner driver name + constructor
left join race_winner rw on dr.race_key = rw.race_key
left join "f1_analytics"."main_marts"."dim_driver" dw on rw.driver_key = dw.driver_key
left join "f1_analytics"."main_marts"."dim_constructor" dcon on rw.constructor_key = dcon.constructor_key

-- Pole sitter
left join pole_sitter ps on dr.race_key = ps.race_key
left join "f1_analytics"."main_marts"."dim_driver" dp on ps.driver_key = dp.driver_key

-- Fastest lap driver
left join fastest_lap_holder flh on dr.race_key = flh.race_key
left join "f1_analytics"."main_marts"."dim_driver" df on flh.driver_key = df.driver_key

-- Race counts
left join race_counts rc on dr.race_key = rc.race_key