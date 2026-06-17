{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing'},
    tags=['intermediate', 'unified', 'qualifying']
  )
}}

/*
  int_qualifying_unified
  ----------------------
  One row per qualifying result, all-time (1950–2026+).
  UNIONs Kaggle (1950–2024) + Jolpica (2025+).

  Note: Kaggle stores Q times as text strings (e.g. "1:23.456").
        Jolpica stores both text and milliseconds.
        Both text and ms versions are preserved here.
*/

with kaggle_qualifying as (
    select
        qualify_id,
        race_id,
        driver_id,
        constructor_id,
        year,
        qualifying_position,
        q1_text,
        q2_text,
        q3_text,
        null::bigint as q1_ms,
        null::bigint as q2_ms,
        null::bigint as q3_ms,
        source_system
    from {{ ref('stg_kaggle_qualifying') }}
),

jolpica_qualifying as (
    select
        jq.season_year                   as year,
        jq.round,
        jq.driver_id                     as jolpica_driver_id,
        jq.constructor_id                as jolpica_constructor_id,
        jq.qualifying_position,
        jq.q1_time                       as q1_text,
        jq.q2_time                       as q2_text,
        jq.q3_time                       as q3_text,
        jq.q1_ms,
        jq.q2_ms,
        jq.q3_ms,
        jq.source_system,

        -- Resolve integer keys
        db.driver_id                     as driver_id,
        cb.constructor_id                as constructor_id,
        rb.race_id                       as race_id
    from {{ ref('stg_jolpica_qualifying') }} jq
    left join {{ ref('int_driver_bridge') }} db
        on jq.driver_id = db.jolpica_driver_id
    left join {{ ref('int_constructor_bridge') }} cb
        on jq.constructor_id = cb.jolpica_constructor_id
    left join {{ ref('int_race_bridge') }} rb
        on jq.season_year = rb.year
        and jq.round = rb.round
),

kaggle_final as (
    select
        qualify_id,
        race_id,
        driver_id,
        constructor_id,
        year,
        qualifying_position,
        q1_text,
        q2_text,
        q3_text,
        q1_ms,
        q2_ms,
        q3_ms,
        source_system
    from kaggle_qualifying
),

jolpica_final as (
    select
        -- Synthetic qualify_id to avoid collision with Kaggle range
        (2000000 + race_id * 100 + coalesce(qualifying_position, 99))::integer as qualify_id,
        race_id,
        driver_id,
        constructor_id,
        year,
        qualifying_position,
        q1_text,
        q2_text,
        q3_text,
        q1_ms,
        q2_ms,
        q3_ms,
        source_system
    from jolpica_qualifying
),

unified as (
    select * from kaggle_final
    union all
    select * from jolpica_final
)

select
    qualify_id,
    race_id,
    driver_id,
    constructor_id,
    year,
    qualifying_position,
    q1_text,
    q2_text,
    q3_text,
    q1_ms,
    q2_ms,
    q3_ms,
    source_system,
    current_timestamp as _dbt_loaded_at
from unified
