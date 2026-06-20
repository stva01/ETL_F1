{{
  config(
    materialized='incremental',
    unique_key='qualify_id',
    tags=['fact', 'gold', 'incremental']
  )
}}

/*
  fct_qualifying
  --------------
  One row per driver × qualifying session (~11K rows).
  Grain: 1 row per qualify_id.
  Source: int_qualifying_unified
*/

select
    q.qualify_id,
    q.race_id             as race_key,
    q.driver_id           as driver_key,
    q.constructor_id      as constructor_key,
    q.year                as season_year,

    -- Qualifying measures
    q.qualifying_position,
    q.q1_text,
    q.q2_text,
    q.q3_text,
    q.q1_ms,
    q.q2_ms,
    q.q3_ms,

    -- Derived flags
    case when q.qualifying_position = 1 then true else false end as is_pole_position,
    case when q.q2_text is not null then true else false end      as reached_q2,
    case when q.q3_text is not null then true else false end      as reached_q3,

    -- Metadata
    q.source_system,
    current_timestamp     as dw_inserted_at
from {{ ref('int_qualifying_unified') }} q
{% if is_incremental() %}
where q._dbt_loaded_at > (select max(dw_inserted_at) from {{ this }})
{% endif %}
