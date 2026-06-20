

/*
  mart_driver_career
  ------------------
  One row per driver, all-time (~860 rows).
  Grain: 1 row per driver_key.

  Pre-aggregated career stats for dashboard use:
    Overview (hero stats), Drivers (leaderboards), Compare, Records tabs.

  Sources:
    fct_race_result       → race stats (wins, podiums, points, DNFs)
    fct_qualifying        → pole stats
    int_standings_driver_eoy → championship titles
    dim_driver            → identity fields
*/

with race_stats as (
    select
        driver_key,
        count(*)                                           as total_race_starts,
        sum(case when is_winner      then 1 else 0 end)   as total_wins,
        sum(case when is_podium      then 1 else 0 end)   as total_podiums,
        sum(case when is_fastest_lap then 1 else 0 end)   as total_fastest_laps,
        sum(case when is_dnf         then 1 else 0 end)   as dnf_count,
        sum(coalesce(points_scored, 0))                   as total_points,
        avg(case when finish_position is not null
                 then finish_position::decimal end)        as avg_finish_position,
        avg(case when grid_position is not null
                 then grid_position::decimal end)          as avg_grid_position,
        count(distinct constructor_key)                    as teams_raced_for,
        min(season_year)                                   as debut_year_check,
        max(season_year)                                   as final_year_check
    from "f1_analytics"."main_marts"."fct_race_result"
    where driver_key is not null
    group by driver_key
),

qual_stats as (
    select
        driver_key,
        sum(case when is_pole_position then 1 else 0 end) as total_poles
    from "f1_analytics"."main_marts"."fct_qualifying"
    where driver_key is not null
    group by driver_key
),

-- Championship titles = distinct seasons where driver finished position 1
-- Use COUNT(DISTINCT year) — guards against duplicate rows from Kaggle+Jolpica UNION
title_stats as (
    select
        driver_id   as driver_key,
        count(distinct year) as championship_titles
    from "f1_analytics"."main_intermediate"."int_standings_driver_eoy"
    where championship_position = 1
      and driver_id is not null
    group by driver_id
),

-- Best-ever season championship position (separate to include non-champions too)
best_champ_pos as (
    select
        driver_id   as driver_key,
        min(championship_position) as best_championship_position
    from "f1_analytics"."main_intermediate"."int_standings_driver_eoy"
    where driver_id is not null
    group by driver_id
)

select
    d.driver_key,
    d.full_name,
    d.nationality,
    d.date_of_birth,
    d.is_active,
    d.debut_year,
    d.final_year,
    d.career_span_years,

    -- Race aggregates
    coalesce(rs.total_race_starts, 0)   as total_race_starts,
    coalesce(rs.total_wins, 0)          as total_wins,
    coalesce(rs.total_podiums, 0)       as total_podiums,
    coalesce(qs.total_poles, 0)         as total_poles,
    coalesce(rs.total_fastest_laps, 0)  as total_fastest_laps,
    coalesce(rs.total_points, 0)        as total_points,
    coalesce(ts.championship_titles, 0) as championship_titles,

    -- Rates (avoid divide-by-zero)
    case
        when coalesce(rs.total_race_starts, 0) > 0
        then round(rs.total_wins::decimal / rs.total_race_starts * 100, 2)
        else 0
    end                                 as win_rate_pct,
    case
        when coalesce(rs.total_race_starts, 0) > 0
        then round(rs.total_podiums::decimal / rs.total_race_starts * 100, 2)
        else 0
    end                                 as podium_rate_pct,

    round(coalesce(rs.avg_grid_position, 0), 2)   as avg_grid_position,
    round(coalesce(rs.avg_finish_position, 0), 2) as avg_finish_position,
    coalesce(rs.dnf_count, 0)           as dnf_count,
    coalesce(rs.teams_raced_for, 0)     as teams_raced_for,
    bcp.best_championship_position,

    current_timestamp                   as last_updated_at
from "f1_analytics"."main_marts"."dim_driver" d
left join race_stats rs     on d.driver_key = rs.driver_key
left join qual_stats qs     on d.driver_key = qs.driver_key
left join title_stats ts    on d.driver_key = ts.driver_key
left join best_champ_pos bcp on d.driver_key = bcp.driver_key