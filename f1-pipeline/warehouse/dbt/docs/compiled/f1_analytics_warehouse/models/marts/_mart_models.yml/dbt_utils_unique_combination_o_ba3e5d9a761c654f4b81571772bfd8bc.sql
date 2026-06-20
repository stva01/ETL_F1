





with validation_errors as (

    select
        record_category, record_type
    from "f1_analytics"."main_marts"."mart_records"
    group by record_category, record_type
    having count(*) > 1

)

select *
from validation_errors


