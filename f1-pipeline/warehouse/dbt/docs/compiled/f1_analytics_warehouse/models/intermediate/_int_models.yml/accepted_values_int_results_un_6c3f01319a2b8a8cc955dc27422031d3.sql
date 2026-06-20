
    
    

with all_values as (

    select
        source_system as value_field,
        count(*) as n_records

    from "f1_analytics"."main_intermediate"."int_results_unified"
    group by source_system

)

select *
from all_values
where value_field not in (
    'kaggle','jolpica'
)


