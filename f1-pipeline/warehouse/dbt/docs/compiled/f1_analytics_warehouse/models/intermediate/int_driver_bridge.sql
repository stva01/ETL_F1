

/*
  int_driver_bridge
  -----------------
  One row per driver, all-time. Maps all three ID systems:
    - Kaggle:  driver_id (integer, master key)
    - Jolpica: driver_id (string = Kaggle driver_ref, e.g. "hamilton")
    - OpenF1:  driver_number (integer, e.g. 44)

  Join logic:
    Kaggle drivers  (master)
      LEFT JOIN OpenF1 via driver_number = Kaggle number
      LEFT JOIN Jolpica distinct driver_id list via driver_ref match
*/

with kaggle_drivers as (
    select
        driver_id,
        driver_ref,
        driver_code,
        first_name,
        last_name,
        full_name,
        date_of_birth,
        nationality,
        driver_number as kaggle_driver_number
    from "f1_analytics"."main_staging"."stg_kaggle_drivers"
),

-- Deduplicate OpenF1 drivers: take the latest-year record per driver_number
-- to get current team and colour
openf1_latest as (
    select
        driver_number,
        driver_full_name   as openf1_full_name,
        driver_code        as openf1_driver_code,
        country_code       as openf1_country_code,
        team_name          as current_team,
        team_colour        as team_colour,
        year               as openf1_year
    from (
        select
            *,
            row_number() over (
                partition by driver_number
                order by year desc
            ) as rn
        from "f1_analytics"."main_staging"."stg_openf1_drivers"
    ) ranked
    where rn = 1
),

-- Distinct Jolpica driver strings (just to confirm which refs exist in current data)
jolpica_refs as (
    select distinct
        driver_id         as jolpica_driver_id
    from "f1_analytics"."main_staging"."stg_jolpica_results"
),

bridged as (
    select
        -- Canonical integer key (Kaggle master)
        kd.driver_id,
        kd.driver_ref,

        -- Best driver_code: prefer OpenF1 3-letter acronym (more up-to-date)
        coalesce(of1.openf1_driver_code, kd.driver_code) as driver_code,

        -- Name info from Kaggle (authoritative for history)
        kd.first_name,
        kd.last_name,
        kd.full_name,
        kd.date_of_birth,
        kd.nationality,

        -- Driver number
        kd.kaggle_driver_number                          as driver_number,

        -- Jolpica link (string key used in 2025+ data)
        jr.jolpica_driver_id,

        -- OpenF1 link
        of1.driver_number                                as openf1_driver_number,
        of1.current_team,
        of1.team_colour,
        of1.openf1_year                                  as openf1_data_year,

        -- Is this driver active in 2025+ data?
        case when jr.jolpica_driver_id is not null then true else false end
            as is_active_2025,
        case when of1.driver_number is not null then true else false end
            as has_openf1_telemetry,

        current_timestamp as _dbt_loaded_at
    from kaggle_drivers kd
    -- OpenF1: join on driver number (most reliable numeric link)
    left join openf1_latest of1
        on kd.kaggle_driver_number = of1.driver_number
    -- Jolpica: join on driver_ref = jolpica driver_id string
    left join jolpica_refs jr
        on kd.driver_ref = jr.jolpica_driver_id
)

select * from bridged