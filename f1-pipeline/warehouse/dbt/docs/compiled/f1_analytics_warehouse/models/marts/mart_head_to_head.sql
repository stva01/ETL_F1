

/*
  mart_head_to_head
  -----------------
  One row per (driver_a, driver_b, season) — teammate pairs only.
  Grain: 1 row per (driver_a_key, driver_b_key, season_year).

  CRITICAL: driver_a_key < driver_b_key enforced to prevent duplicate pairs.
  ~10 pairs/season × 76 seasons ≈ 1,100–1,500 rows total.

  Dashboard tabs: Compare (head-to-head stats).

  Sources:
    fct_race_result   → self-join on (race_key, constructor_key) to find teammates
    dim_driver        → driver names
    dim_constructor   → team name
*/

with teammate_races as (
    -- Self-join: two drivers who drove for the same team in the same race
    select
        a.race_key,
        a.season_year,
        a.constructor_key,

        -- Canonical pair ordering: lower key is always "a"
        least(a.driver_key, b.driver_key)    as driver_a_key,
        greatest(a.driver_key, b.driver_key) as driver_b_key,

        -- Driver A stats (lower key)
        case when a.driver_key < b.driver_key then a.finish_position end as a_finish,
        case when a.driver_key < b.driver_key then a.is_winner      end as a_win,
        case when a.driver_key < b.driver_key then a.points_scored  end as a_points,

        -- Driver B stats (higher key)
        case when b.driver_key > a.driver_key then b.finish_position end as b_finish,
        case when b.driver_key > a.driver_key then b.is_winner      end as b_win,
        case when b.driver_key > a.driver_key then b.points_scored  end as b_points

    from "f1_analytics"."main_marts"."fct_race_result" a
    -- Join only to the teammate (same race, same team, different driver)
    join "f1_analytics"."main_marts"."fct_race_result" b
        on  a.race_key       = b.race_key
        and a.constructor_key = b.constructor_key
        and a.driver_key     < b.driver_key   -- guarantees exactly one pair per race
    where a.driver_key is not null
      and b.driver_key is not null
      and a.constructor_key is not null
),

season_h2h as (
    select
        driver_a_key,
        driver_b_key,
        season_year,
        constructor_key,

        count(*)                                                       as races_as_teammates,

        -- Better finish = lower numeric finish position
        sum(case when a_finish < b_finish and a_finish is not null
                      and b_finish is not null then 1 else 0 end)     as driver_a_better_finishes,
        sum(case when b_finish < a_finish and a_finish is not null
                      and b_finish is not null then 1 else 0 end)     as driver_b_better_finishes,

        sum(case when a_win = true then 1 else 0 end)                 as driver_a_wins,
        sum(case when b_win = true then 1 else 0 end)                 as driver_b_wins,

        avg(case when a_finish is not null then a_finish::decimal end) as driver_a_avg_position,
        avg(case when b_finish is not null then b_finish::decimal end) as driver_b_avg_position,

        sum(coalesce(a_points, 0))                                    as driver_a_points,
        sum(coalesce(b_points, 0))                                    as driver_b_points
    from teammate_races
    group by driver_a_key, driver_b_key, season_year, constructor_key
)

select
    sh.driver_a_key,
    sh.driver_b_key,
    sh.season_year,
    dc.constructor_name,
    sh.races_as_teammates,

    da.full_name                                       as driver_a_name,
    db.full_name                                       as driver_b_name,

    sh.driver_a_better_finishes,
    sh.driver_b_better_finishes,
    sh.driver_a_wins,
    sh.driver_b_wins,
    round(coalesce(sh.driver_a_avg_position, 0), 2)   as driver_a_avg_position,
    round(coalesce(sh.driver_b_avg_position, 0), 2)   as driver_b_avg_position,
    sh.driver_a_points,
    sh.driver_b_points,

    current_timestamp                                  as last_updated_at
from season_h2h sh
left join "f1_analytics"."main_marts"."dim_driver" da
    on sh.driver_a_key = da.driver_key
left join "f1_analytics"."main_marts"."dim_driver" db
    on sh.driver_b_key = db.driver_key
left join "f1_analytics"."main_marts"."dim_constructor" dc
    on sh.constructor_key = dc.constructor_key