{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing'},
    tags=['staging', 'openf1']
  )
}}

with source as (
    select * from {{ source('s3_processed', 'openf1_drivers') }}
),

cleaned as (
    select
        -- Partition Column
        cast(year as integer) as season_year,
        
        -- Event Context
        cast(meeting_key as integer) as meeting_key,
        cast(session_key as integer) as session_key,
        
        -- Driver Identity
        cast(driver_number as integer) as driver_number,
        cast(full_name as varchar) as driver_full_name,
        cast(name_acronym as varchar) as driver_acronym,
        
        -- Team Information
        cast(team_name as varchar) as constructor_name,
        
        -- Handle missing hex colors safely
        coalesce(cast(team_colour as varchar), 'FFFFFF') as team_hex_color,
        
        -- Lineage Metadata
        current_timestamp() as _dbt_loaded_at,
        'openf1' as source_system
        
    from source
    
    -- Data Quality Filters: Drop corrupted telemetry packets
    where 
        year is not null 
        and meeting_key is not null
        and driver_number is not null
),

-- OpenF1 API frequently broadcasts duplicate driver payloads per session
deduplicated as (
    select distinct * from cleaned
)

select * from deduplicated