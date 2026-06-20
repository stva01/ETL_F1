

/*
  mart_constructor_career
  -----------------------
  One row per constructor, all-time (~210 rows).
  Grain: 1 row per constructor_key.

  Dashboard tabs: Constructors (wins chart, titles chart, win share pie).

  Sources:
    fct_race_result                 → race stats (wins, podiums, entries)
    fct_qualifying                  → pole stats
    int_standings_constructor_eoy   → constructor championship titles
    dim_constructor                 → identity fields
*/

with race_stats as (
    select
        constructor_key,
        count(*)                                           as total_race_entries,
        sum(case when is_winner  then 1 else 0 end)       as total_wins,
        sum(case when is_podium  then 1 else 0 end)       as total_podiums,
        sum(coalesce(points_scored, 0))                   as total_points
    from "f1_analytics"."main_marts"."fct_race_result"
    where constructor_key is not null
    group by constructor_key
),

pole_stats as (
    select
        constructor_key,
        sum(case when is_pole_position then 1 else 0 end) as total_poles
    from "f1_analytics"."main_marts"."fct_qualifying"
    where constructor_key is not null
    group by constructor_key
),

title_stats as (
    select
        constructor_id  as constructor_key,
        count(distinct year) as constructor_titles
    from "f1_analytics"."main_intermediate"."int_standings_constructor_eoy"
    where championship_position = 1
    group by constructor_id
),

-- Most successful driver for each constructor (by wins)
driver_wins as (
    select
        constructor_key,
        driver_key,
        sum(case when is_winner then 1 else 0 end) as driver_wins,
        row_number() over (
            partition by constructor_key
            order by sum(case when is_winner then 1 else 0 end) desc
        ) as rn
    from "f1_analytics"."main_marts"."fct_race_result"
    where constructor_key is not null and driver_key is not null
    group by constructor_key, driver_key
),

top_driver as (
    select
        dw.constructor_key,
        d.full_name as most_successful_driver
    from driver_wins dw
    join "f1_analytics"."main_marts"."dim_driver" d on dw.driver_key = d.driver_key
    where dw.rn = 1
)

select
    dc.constructor_key,
    dc.constructor_name,
    dc.nationality,
    dc.is_active,
    dc.debut_year,
    dc.final_year,
    dc.seasons_competed,

    coalesce(rs.total_race_entries, 0)   as total_race_entries,
    coalesce(rs.total_wins, 0)           as total_wins,
    coalesce(rs.total_podiums, 0)        as total_podiums,
    coalesce(ps.total_poles, 0)          as total_poles,
    coalesce(rs.total_points, 0)         as total_points,
    coalesce(ts.constructor_titles, 0)   as constructor_titles,

    case
        when coalesce(rs.total_race_entries, 0) > 0
        then round(rs.total_wins::decimal / rs.total_race_entries * 100, 2)
        else 0
    end                                  as win_rate_pct,

    td.most_successful_driver,
    current_timestamp                    as last_updated_at
from "f1_analytics"."main_marts"."dim_constructor" dc
left join race_stats rs   on dc.constructor_key = rs.constructor_key
left join pole_stats ps   on dc.constructor_key = ps.constructor_key
left join title_stats ts  on dc.constructor_key = ts.constructor_key
left join top_driver td   on dc.constructor_key = td.constructor_key