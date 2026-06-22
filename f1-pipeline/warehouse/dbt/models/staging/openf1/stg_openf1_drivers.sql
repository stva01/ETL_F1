with source as (
    select * from {{ s3_source('processed_openf1', 'drivers', 'openf1/drivers/*/*.parquet') }}
),

renamed as (
    select
        cast(year as integer) as year,
        cast(driver_number as integer) as driver_number,
        cast(full_name as varchar) as driver_full_name,
        cast(name_acronym as varchar) as driver_code,
        cast(country_code as varchar) as country_code,
        cast(team_name as varchar) as team_name,
        cast(team_colour as varchar) as team_colour,
        cast(broadcast_name as varchar) as broadcast_name,
        
        CURRENT_TIMESTAMP as _dbt_loaded_at,
        'openf1' as source_system
    from source
    where year is not null
      and driver_number is not null
)

select * from renamed