
  
    
    

    create  table
      "f1_analytics"."main_marts"."dim_driver__dbt_tmp"
  
    as (
      

/*
  dim_driver
  ----------
  One row per driver, all-time (~860 rows).
  Grain: 1 row per canonical driver_id.

  Sources:
    - int_driver_bridge  → identity fields
    - int_results_enriched → debut/final year derivation
*/

with driver_identity as (
    select
        driver_id,
        driver_ref,
        driver_code,
        full_name,
        date_of_birth,
        nationality,
        driver_number     as permanent_number,
        is_active_2025    as is_active
    from "f1_analytics"."main_intermediate"."int_driver_bridge"
),

career_span as (
    select
        driver_id,
        min(year)         as debut_year,
        max(year)         as final_year
    from "f1_analytics"."main_intermediate"."int_results_enriched"
    where driver_id is not null
    group by driver_id
)

select
    di.driver_id          as driver_key,
    di.driver_ref,
    di.driver_code,
    di.full_name,
    di.date_of_birth,
    di.nationality,
    di.permanent_number,
    di.is_active,
    cs.debut_year,
    cs.final_year,
    coalesce(cs.final_year - cs.debut_year + 1, 0) as career_span_years,
    current_timestamp     as dw_created_at
from driver_identity di
left join career_span cs
    on di.driver_id = cs.driver_id
    );
  
  