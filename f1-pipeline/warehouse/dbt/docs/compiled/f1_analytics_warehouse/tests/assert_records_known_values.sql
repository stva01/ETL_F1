/*
  assert_records_known_values.sql
  --------------------------------
  Singular dbt test — returns rows ONLY on failure.
  Zero rows = all assertions pass.

  Known-value regression assertions for mart_records.
  Values reflect actual data including 2025 season.

  Run: dbt test --select assert_records_known_values
*/

with expected as (
    -- Driver records (updated to include 2025 season data)
    select 'Most Race Wins'          as record_category, 'driver'      as record_type, 105 as expected_min_value union all
    select 'Most Championship Titles' as record_category, 'driver'     as record_type, 7   as expected_min_value union all
    select 'Most Podiums'            as record_category, 'driver'      as record_type, 197 as expected_min_value union all
    select 'Most Pole Positions'     as record_category, 'driver'      as record_type, 104 as expected_min_value union all
    select 'Most Wins in a Season'   as record_category, 'driver'      as record_type, 19  as expected_min_value union all
    -- Constructor records
    select 'Most Race Wins'          as record_category, 'constructor' as record_type, 240 as expected_min_value union all
    select 'Most Championship Titles' as record_category, 'constructor' as record_type, 16 as expected_min_value
),

actual as (
    select
        record_category,
        record_type,
        record_value::integer as actual_value
    from "f1_analytics"."main_marts"."mart_records"
)

-- Return only rows where actual < expected minimum (regressions only — records grow over time)
select
    e.record_category,
    e.record_type,
    e.expected_min_value,
    a.actual_value,
    'REGRESSION: expected at least ' || e.expected_min_value::varchar
        || ' but got ' || coalesce(a.actual_value::varchar, 'NULL') as failure_reason
from expected e
left join actual a
    on  e.record_category = a.record_category
    and e.record_type     = a.record_type
where a.actual_value is null
   or a.actual_value < e.expected_min_value