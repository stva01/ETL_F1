{{
  config(
    materialized='table',
    unique_key=['season_year', 'round', 'driver_id'],
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'jolpica'},
    tags=['staging', 'jolpica', 'event'],
    partition_by='season_year'
  )
}}

with source as (
    select * from {{ source('processed_jolpica', 'driver_standings') }}
),

renamed as (
    select
        cast(season as integer)    as season_year,
        cast(round as integer)     as round,
        cast(driver_id as varchar) as driver_id,
        cast(position as integer)  as position,
        cast(points as double)     as points,
        cast(wins as integer)      as wins,

        now() as _dbt_loaded_at,
        'jolpica' as source_system
    from source
    where season is not null
      and round is not null
      and driver_id is not null
)

select * from renamed