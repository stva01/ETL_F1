{{
  config(
    materialized='table',
    unique_key=['season_year', 'round', 'constructor_id'],
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'jolpica'},
    tags=['staging', 'jolpica', 'standings'],
    partition_by='season_year'
  )
}}

with source as (
    select * from {{ source('processed_jolpica', 'constructor_standings') }}
),

renamed as (
    select
        cast(season as integer)          as season_year,
        cast(round as integer)           as round,
        cast(constructor_id as varchar)  as constructor_id,
        cast(constructor_name as varchar) as constructor_name,
        cast(position as integer)        as position,
        cast(position_text as varchar)   as position_text,
        cast(points as double)           as points,
        cast(wins as integer)            as wins,

        now() as _dbt_loaded_at,
        'jolpica' as source_system
    from source
    where season is not null
      and round is not null
      and constructor_id is not null
)

select * from renamed
