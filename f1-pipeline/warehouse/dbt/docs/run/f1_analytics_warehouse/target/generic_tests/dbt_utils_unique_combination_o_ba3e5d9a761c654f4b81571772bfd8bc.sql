
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        record_category, record_type
    from "f1_analytics"."main_marts"."mart_records"
    group by record_category, record_type
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test