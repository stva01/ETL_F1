
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select record_value
from "f1_analytics"."main_marts"."mart_records"
where record_value is null



  
  
      
    ) dbt_internal_test