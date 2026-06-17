{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing', 'source': 'kaggle', 'critical': true},
    tags=['staging', 'kaggle', 'dimension', 'bridge']
  )
}}

with source as (
    select * from {{ source('s3_kaggle', 'drivers') }}
),

renamed as (
    select
        cast(driverId as integer) as driver_id,
        cast(driverRef as varchar) as driver_ref,
        cast(code as varchar) as driver_code,
        cast(forename as varchar) as first_name,
        cast(surname as varchar) as last_name,
        concat(forename, ' ', surname) as full_name,
        cast(dob as date) as date_of_birth,
        cast(nationality as varchar) as nationality,
        cast(NULLIF(number, '\N') as integer) as driver_number,
        cast(url as varchar) as wikipedia_url,
        now() as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where driverId is not null
)

select * from renamed