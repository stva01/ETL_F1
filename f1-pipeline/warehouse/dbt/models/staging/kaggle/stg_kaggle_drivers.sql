with source as (
    select * from {{ s3_source('s3_kaggle', 'drivers', 'kaggle/drivers/*.parquet') }}
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
        cast(nullif(cast(number as varchar), '\N') as integer) as driver_number,
        cast(url as varchar) as wikipedia_url,
        {{ dbt.current_timestamp() }} as _dbt_loaded_at,
        'kaggle' as source_system
    from source
    where driverId is not null
)

select * from renamed