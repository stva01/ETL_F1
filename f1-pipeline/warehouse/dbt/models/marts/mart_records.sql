{{
  config(
    materialized='table',
    tags=['mart', 'gold']
  )
}}

/*
  mart_records
  ------------
  One row per record category (~20 rows).
  Grain: 1 row per record_category.

  Dashboard tabs: Records & Milestones, Overview (stat cards).

  Computed dynamically — all values derived from fct_race_result,
  fct_qualifying, and mart_driver_career.

  Known-value regression test: see tests/assert_records_known_values.sql
  Expected values:
    Most Race Wins (driver)      → Hamilton, 103
    Most Race Wins (constructor) → Ferrari, 257
    Most Titles (driver)         → Hamilton/Schumacher, 7
    Most Poles (driver)          → Hamilton, 104
    Most Podiums (driver)        → Hamilton, 197
    Most Starts (driver)         → Barrichello, 326
    Most Wins in a Season        → Verstappen 2023, 19
    Most Points in a Season      → Verstappen 2023, 575
*/

-- Most race wins — driver
with most_wins_driver as (
    select
        'Most Race Wins'   as record_category,
        'driver'           as record_type,
        full_name          as holder_name,
        total_wins::decimal as record_value,
        total_wins::varchar as record_value_formatted,
        'All-time race wins' as context_note
    from {{ ref('mart_driver_career') }}
    order by total_wins desc
    limit 1
),

-- Most race wins — constructor
most_wins_constructor as (
    select
        'Most Race Wins'            as record_category,
        'constructor'               as record_type,
        constructor_name            as holder_name,
        total_wins::decimal          as record_value,
        total_wins::varchar          as record_value_formatted,
        'All-time race wins'        as context_note
    from {{ ref('mart_constructor_career') }}
    order by total_wins desc
    limit 1
),

-- Most championship titles — driver
most_titles_driver as (
    select
        'Most Championship Titles'  as record_category,
        'driver'                    as record_type,
        full_name                   as holder_name,
        championship_titles::decimal as record_value,
        championship_titles::varchar as record_value_formatted,
        'World Drivers'' Championship titles' as context_note
    from {{ ref('mart_driver_career') }}
    order by championship_titles desc
    limit 1
),

-- Most championship titles — constructor
most_titles_constructor as (
    select
        'Most Championship Titles'  as record_category,
        'constructor'               as record_type,
        constructor_name            as holder_name,
        constructor_titles::decimal  as record_value,
        constructor_titles::varchar  as record_value_formatted,
        'World Constructors'' Championship titles' as context_note
    from {{ ref('mart_constructor_career') }}
    order by constructor_titles desc
    limit 1
),

-- Most podiums — driver
most_podiums as (
    select
        'Most Podiums'              as record_category,
        'driver'                    as record_type,
        full_name                   as holder_name,
        total_podiums::decimal       as record_value,
        total_podiums::varchar       as record_value_formatted,
        'Total podium finishes (1st, 2nd, 3rd)' as context_note
    from {{ ref('mart_driver_career') }}
    order by total_podiums desc
    limit 1
),

-- Most pole positions — driver
most_poles as (
    select
        'Most Pole Positions'       as record_category,
        'driver'                    as record_type,
        full_name                   as holder_name,
        total_poles::decimal         as record_value,
        total_poles::varchar         as record_value_formatted,
        'Total qualifying poles'    as context_note
    from {{ ref('mart_driver_career') }}
    order by total_poles desc
    limit 1
),

-- Most fastest laps — driver
most_fastest_laps as (
    select
        'Most Fastest Laps'         as record_category,
        'driver'                    as record_type,
        full_name                   as holder_name,
        total_fastest_laps::decimal  as record_value,
        total_fastest_laps::varchar  as record_value_formatted,
        'Total fastest laps recorded' as context_note
    from {{ ref('mart_driver_career') }}
    order by total_fastest_laps desc
    limit 1
),

-- Most race starts — driver
most_starts as (
    select
        'Most Race Starts'          as record_category,
        'driver'                    as record_type,
        full_name                   as holder_name,
        total_race_starts::decimal   as record_value,
        total_race_starts::varchar   as record_value_formatted,
        'Total Grand Prix starts'   as context_note
    from {{ ref('mart_driver_career') }}
    order by total_race_starts desc
    limit 1
),

-- Most wins in a single season — driver
most_wins_season as (
    select
        'Most Wins in a Season'     as record_category,
        'driver'                    as record_type,
        full_name || ' (' || season_year::varchar || ')' as holder_name,
        season_wins::decimal         as record_value,
        season_wins::varchar         as record_value_formatted,
        'Most wins in a single F1 season' as context_note
    from {{ ref('mart_driver_season') }}
    order by season_wins desc
    limit 1
),

-- Most points in a single season — driver
most_points_season as (
    select
        'Most Points in a Season'   as record_category,
        'driver'                    as record_type,
        full_name || ' (' || season_year::varchar || ')' as holder_name,
        season_points::decimal       as record_value,
        season_points::varchar       as record_value_formatted,
        'Highest single-season points total' as context_note
    from {{ ref('mart_driver_season') }}
    order by season_points desc
    limit 1
),

-- Highest win rate — driver (min 50 starts to filter one-race wonders)
highest_win_rate as (
    select
        'Highest Win Rate'          as record_category,
        'driver'                    as record_type,
        full_name                   as holder_name,
        win_rate_pct::decimal        as record_value,
        win_rate_pct::varchar        as record_value_formatted,
        'Win % (minimum 50 starts)' as context_note
    from {{ ref('mart_driver_career') }}
    where total_race_starts >= 50
    order by win_rate_pct desc
    limit 1
),

-- Youngest race winner
youngest_winner as (
    select
        'Youngest Race Winner'      as record_category,
        'driver'                    as record_type,
        d.full_name                 as holder_name,
        DATEDIFF('year', d.date_of_birth, dr.race_date)::decimal as record_value,
        DATEDIFF('year', d.date_of_birth, dr.race_date)::varchar as record_value_formatted,
        'Age at first race win'     as context_note
    from {{ ref('fct_race_result') }} fct
    join {{ ref('dim_driver') }} d  on fct.driver_key = d.driver_key
    join {{ ref('dim_race') }} dr   on fct.race_key  = dr.race_key
    where fct.is_winner = true
      and d.date_of_birth is not null
    order by DATEDIFF('year', d.date_of_birth, dr.race_date) asc
    limit 1
)

select * from most_wins_driver
union all select * from most_wins_constructor
union all select * from most_titles_driver
union all select * from most_titles_constructor
union all select * from most_podiums
union all select * from most_poles
union all select * from most_fastest_laps
union all select * from most_starts
union all select * from most_wins_season
union all select * from most_points_season
union all select * from highest_win_rate
union all select * from youngest_winner
