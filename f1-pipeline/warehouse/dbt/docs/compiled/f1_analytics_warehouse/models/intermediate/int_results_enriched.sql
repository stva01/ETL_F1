

/*
  int_results_enriched
  --------------------
  The "fully joined" result row — one row per race result, all-time,
  with human-readable names attached from all dimension tables.

  This is the PRIMARY input that mart models aggregate from.
  Its column set covers every metric in the design doc.

  Joins:
    int_results_unified         — base (race result facts)
    int_driver_bridge           — driver_name, nationality, driver_code
    int_constructor_bridge      — constructor_name
    int_race_bridge             — gp_name, race_date, circuit_id
    stg_kaggle_status           — status_description (for Kaggle DNF codes)
    int_qualifying_unified      — qualifying_position (to compute pole flags)
    stg_kaggle_circuits         — circuit_name, country, location
*/

with results as (
    select * from "f1_analytics"."main_intermediate"."int_results_unified"
),

drivers as (
    select
        driver_id,
        driver_ref,
        driver_code,
        full_name              as driver_name,
        nationality            as driver_nationality,
        date_of_birth,
        current_team,
        team_colour,
        is_active_2025,
        has_openf1_telemetry
    from "f1_analytics"."main_intermediate"."int_driver_bridge"
),

constructors as (
    select
        constructor_id,
        constructor_ref,
        constructor_name,
        constructor_nationality,
        openf1_team_colour
    from "f1_analytics"."main_intermediate"."int_constructor_bridge"
),

races as (
    select
        race_id,
        year,
        round,
        circuit_id,
        gp_name,
        race_date,
        data_source            as race_data_source
    from "f1_analytics"."main_intermediate"."int_race_bridge"
),

status_lookup as (
    select
        status_id,
        status_description
    from "f1_analytics"."main_staging"."stg_kaggle_status"
),

circuits as (
    select
        circuit_id,
        circuit_name,
        location               as circuit_location,
        country                as circuit_country
    from "f1_analytics"."main_staging"."stg_kaggle_circuits"
),

-- Take one qualifying result per (race_id, driver_id) — the qualifying position
qualifying as (
    select
        race_id,
        driver_id,
        qualifying_position
    from "f1_analytics"."main_intermediate"."int_qualifying_unified"
),

enriched as (
    select
        -- Core keys
        r.result_id,
        r.race_id,
        r.driver_id,
        r.constructor_id,
        r.year,
        r.source_system,

        -- Race context
        rc.round,
        rc.gp_name,
        rc.race_date,
        rc.circuit_id,
        ci.circuit_name,
        ci.circuit_location,
        ci.circuit_country,

        -- Driver info
        d.driver_ref,
        d.driver_code,
        d.driver_name,
        d.driver_nationality,
        d.date_of_birth,
        d.current_team        as driver_current_team,
        d.team_colour         as driver_team_colour,
        d.is_active_2025,

        -- Constructor info
        c.constructor_ref,
        c.constructor_name,
        c.constructor_nationality,
        c.openf1_team_colour  as constructor_team_colour,

        -- Race result facts
        r.grid_position,
        r.finishing_position,
        r.finishing_position_text,
        r.points_scored,
        r.laps_completed,
        r.race_time_ms,
        r.fastest_lap_rank,
        r.fastest_lap_speed_kmh,

        -- Status (Kaggle: resolved from status_id; Jolpica: direct string)
        coalesce(
            sl.status_description,
            r.race_status
        )                     as race_status,

        -- Qualifying info
        q.qualifying_position,

        -- Derived boolean flags (used heavily in mart aggregations)
        case when r.finishing_position = 1 then true else false end
            as is_winner,
        case when r.finishing_position <= 3 then true else false end
            as is_podium,
        case when q.qualifying_position = 1 then true else false end
            as is_pole,
        case when r.fastest_lap_rank = 1 then true else false end
            as is_fastest_lap,
        case when r.finishing_position is null
              and r.finishing_position_text not in ('R', 'D', 'E', 'W', 'F', 'N')
             then false
             when r.finishing_position is null then true
             else false
        end                   as is_dnf,

        current_timestamp     as _dbt_loaded_at
    from results r
    left join drivers d
        on r.driver_id = d.driver_id
    left join constructors c
        on r.constructor_id = c.constructor_id
    left join races rc
        on r.race_id = rc.race_id
    left join circuits ci
        on rc.circuit_id = ci.circuit_id
    left join status_lookup sl
        on r.status_id = sl.status_id
    left join qualifying q
        on r.race_id = q.race_id
        and r.driver_id = q.driver_id
)

select * from enriched