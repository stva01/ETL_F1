
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status_category as value_field,
        count(*) as n_records

    from "f1_analytics"."main_marts"."dim_race_status"
    group by status_category

)

select *
from all_values
where value_field not in (
    'Finished','DNF','DNS','DSQ'
)



  
  
      
    ) dbt_internal_test