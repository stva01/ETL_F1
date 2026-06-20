{{
  config(
    materialized='table',
    tags=['dimension', 'gold']
  )
}}

/*
  dim_circuit
  -----------
  One row per circuit (~77 rows).
  Grain: 1 row per circuit_id.
*/

select
    circuit_id            as circuit_key,
    circuit_ref,
    circuit_name,
    location              as location_city,
    country,
    latitude,
    longitude,
    altitude_meters       as altitude_metres,
    current_timestamp     as dw_created_at
from {{ ref('stg_kaggle_circuits') }}
