{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

/*
  dim_race
  --------
  One row per Grand Prix event (~1,120 rows).
  Grain: 1 row per race_id.

  Sources:
    - int_race_bridge → race identity (Kaggle + Jolpica unified)
    - stg_kaggle_races → circuit_id + race_date (Kaggle only, Jolpica races lack circuit FK)
*/

with race_bridge as (
    select
        race_id,
        year,
        round,
        gp_name,
        race_date,
        data_source
    from {{ ref('int_race_bridge') }}
),

-- Get circuit_id from Kaggle races (Jolpica-only races won't have one)
kaggle_circuit as (
    select
        race_id,
        circuit_id
    from {{ ref('stg_kaggle_races') }}
)

select
    rb.race_id            as race_key,
    rb.year               as season_year,
    rb.round              as round_number,
    kc.circuit_id         as circuit_key,
    rb.gp_name            as race_name,
    rb.race_date,
    rb.data_source,
    current_timestamp     as dw_created_at
from race_bridge rb
left join kaggle_circuit kc
    on rb.race_id = kc.race_id
