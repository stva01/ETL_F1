

/*
  mart_driver_season
  ------------------
  One row per driver × season (~20K rows).
  Grain: 1 row per (driver_key, season_year).

  Dashboard tabs: Seasons (standings, race results).

  Sources:
    fct_race_result             → per-season race stats
    fct_qualifying              → per-season pole stats
    int_standings_driver_eoy    → final championship position
    dim_driver                  → driver name
*/

with season_race_stats as (
    select
        driver_key,
        season_year,
        count(*)                                           as races_entered,
        sum(case when is_winner      then 1 else 0 end)   as season_wins,
        sum(case when is_podium      then 1 else 0 end)   as season_podiums,
        sum(case when is_fastest_lap then 1 else 0 end)   as season_fastest_laps,
        sum(case when is_dnf         then 1 else 0 end)   as dnf_count,
        sum(coalesce(points_scored, 0))                   as season_points,
        avg(case when finish_position is not null
                 then finish_position::decimal end)        as avg_finishing_pos,
        avg(case when grid_position is not null
                 then grid_position::decimal end)          as avg_grid_pos,
        min(finish_position)                               as best_result
    from "f1_analytics"."main_marts"."fct_race_result"
    where driver_key is not null
    group by driver_key, season_year
),

season_pole_stats as (
    select
        driver_key,
        season_year,
        sum(case when is_pole_position then 1 else 0 end) as season_poles
    from "f1_analytics"."main_marts"."fct_qualifying"
    where driver_key is not null
    group by driver_key, season_year
),

-- Primary constructor = most frequent constructor for that driver that season
primary_constructor as (
    select
        driver_key,
        season_year,
        constructor_key,
        count(*) as race_count,
        row_number() over (
            partition by driver_key, season_year
            order by count(*) desc
        ) as rn
    from "f1_analytics"."main_marts"."fct_race_result"
    where driver_key is not null and constructor_key is not null
    group by driver_key, season_year, constructor_key
),

primary_constructor_name as (
    select
        pc.driver_key,
        pc.season_year,
        dc.constructor_name
    from primary_constructor pc
    join "f1_analytics"."main_marts"."dim_constructor" dc on pc.constructor_key = dc.constructor_key
    where pc.rn = 1
),

eoy_standings as (
    select
        driver_id           as driver_key,
        year                as season_year,
        championship_position as final_championship_pos,
        case when championship_position = 1 then true else false end as is_champion
    from "f1_analytics"."main_intermediate"."int_standings_driver_eoy"
)

select
    srs.driver_key,
    srs.season_year,
    d.full_name,
    pcn.constructor_name,
    srs.races_entered,
    srs.season_wins,
    srs.season_podiums,
    coalesce(sps.season_poles, 0)               as season_poles,
    srs.season_fastest_laps,
    srs.season_points,
    eos.final_championship_pos,
    coalesce(eos.is_champion, false)            as is_champion,
    round(coalesce(srs.avg_finishing_pos, 0), 2) as avg_finishing_pos,
    round(coalesce(srs.avg_grid_pos, 0), 2)     as avg_grid_pos,
    srs.best_result,
    srs.dnf_count,
    current_timestamp                           as last_updated_at
from season_race_stats srs
left join season_pole_stats sps
    on srs.driver_key = sps.driver_key
    and srs.season_year = sps.season_year
left join eoy_standings eos
    on srs.driver_key = eos.driver_key
    and srs.season_year = eos.season_year
left join primary_constructor_name pcn
    on srs.driver_key = pcn.driver_key
    and srs.season_year = pcn.season_year
left join "f1_analytics"."main_marts"."dim_driver" d
    on srs.driver_key = d.driver_key