{% macro s3_source(source_name, table_name, relative_path) %}
    {% if target.type == 'duckdb' %}
        read_parquet('s3://{{ var("bucket_name") }}/processed/{{ relative_path }}', hive_partitioning=1, union_by_name=1)
    {% else %}
        {{ source(source_name, table_name) }}
    {% endif %}
{% endmacro %}
