{% snapshot drivers_scd %}

{{
    config(
      target_schema='snapshots',
      unique_key='driver_id',
      strategy='check',
      check_cols='all'
    )
}}

select * from {{ ref('stg_kaggle_drivers') }}

{% endsnapshot %}
