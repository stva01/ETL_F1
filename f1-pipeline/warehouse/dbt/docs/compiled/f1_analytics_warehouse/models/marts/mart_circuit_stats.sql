

/*
  mart_circuit_stats
  ------------------
  One row per circuit (~77 rows).
  Grain: 1 row per circuit_key.

  Dashboard tabs: Circuits (races hosted bar chart, circuit cards,
                  fastest laps table).

  Sources:
    dim_circuit         → circuit identity (name, country, lat/lng)
    fct_race_result     → race stats per circuit
    dim_race            → links race_key → circuit_key
*/

with circuit_race_metrics as (
    select
        dr.circuit_key,
        count(distinct fct.race_key)                              as total_races_held,
        min(fct.season_year)                                      as first_race_year,
        max(fct.season_year)                                      as last_race_year
    from "f1_analytics"."main_marts"."fct_race_result" fct
    join "f1_analytics"."main_marts"."dim_race" dr on fct.race_key = dr.race_key
    where dr.circuit_key is not null
    group by dr.circuit_key
),

-- Driver with most wins at each circuit
driver_circuit_wins as (
    select
        dr.circuit_key,
        fct.driver_key,
        sum(case when fct.is_winner then 1 else 0 end) as wins,
        row_number() over (
            partition by dr.circuit_key
            order by sum(case when fct.is_winner then 1 else 0 end) desc
        ) as rn
    from "f1_analytics"."main_marts"."fct_race_result" fct
    join "f1_analytics"."main_marts"."dim_race" dr on fct.race_key = dr.race_key
    where dr.circuit_key is not null and fct.driver_key is not null
    group by dr.circuit_key, fct.driver_key
),

top_driver_circuit as (
    select
        dw.circuit_key,
        d.full_name  as most_wins_driver,
        dw.wins      as most_wins_driver_count
    from driver_circuit_wins dw
    join "f1_analytics"."main_marts"."dim_driver" d on dw.driver_key = d.driver_key
    where dw.rn = 1
),

-- Constructor with most wins at each circuit
constructor_circuit_wins as (
    select
        dr.circuit_key,
        fct.constructor_key,
        sum(case when fct.is_winner then 1 else 0 end) as wins,
        row_number() over (
            partition by dr.circuit_key
            order by sum(case when fct.is_winner then 1 else 0 end) desc
        ) as rn
    from "f1_analytics"."main_marts"."fct_race_result" fct
    join "f1_analytics"."main_marts"."dim_race" dr on fct.race_key = dr.race_key
    where dr.circuit_key is not null and fct.constructor_key is not null
    group by dr.circuit_key, fct.constructor_key
),

top_constructor_circuit as (
    select
        cw.circuit_key,
        dc.constructor_name  as most_wins_constructor,
        cw.wins              as most_wins_constructor_count
    from constructor_circuit_wins cw
    join "f1_analytics"."main_marts"."dim_constructor" dc on cw.constructor_key = dc.constructor_key
    where cw.rn = 1
)

select
    c.circuit_key,
    c.circuit_name,
    c.country,
    c.location_city,
    c.latitude,
    c.longitude,
    c.altitude_metres,

    coalesce(crm.total_races_held, 0)   as total_races_held,
    crm.first_race_year,
    crm.last_race_year,

    tdc.most_wins_driver,
    coalesce(tdc.most_wins_driver_count, 0) as most_wins_driver_count,

    tcc.most_wins_constructor,
    coalesce(tcc.most_wins_constructor_count, 0) as most_wins_constructor_count,

    current_timestamp                   as last_updated_at
from "f1_analytics"."main_marts"."dim_circuit" c
left join circuit_race_metrics crm  on c.circuit_key = crm.circuit_key
left join top_driver_circuit tdc    on c.circuit_key = tdc.circuit_key
left join top_constructor_circuit tcc on c.circuit_key = tcc.circuit_key