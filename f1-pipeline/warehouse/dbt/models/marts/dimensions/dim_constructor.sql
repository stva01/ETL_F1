{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

/*
  dim_constructor
  ---------------
  One row per constructor, all-time (~210 rows).
  Grain: 1 row per canonical constructor_id.
*/

with constructor_identity as (
    select
        constructor_id,
        constructor_ref,
        constructor_name,
        constructor_nationality   as nationality
    from {{ ref('int_constructor_bridge') }}
),

career_span as (
    select
        constructor_id,
        min(year)                 as debut_year,
        max(year)                 as final_year,
        count(distinct year)      as seasons_competed
    from {{ ref('int_results_enriched') }}
    where constructor_id is not null
    group by constructor_id
)

select
    ci.constructor_id     as constructor_key,
    ci.constructor_ref,
    ci.constructor_name,
    ci.nationality,
    cs.debut_year,
    cs.final_year,
    cs.seasons_competed,
    -- ponytail: active = raced in last 2 years, good enough
    coalesce(cs.final_year >= extract(year from current_date) - 2, false) as is_active,
    current_timestamp     as dw_created_at
from constructor_identity ci
left join career_span cs
    on ci.constructor_id = cs.constructor_id
