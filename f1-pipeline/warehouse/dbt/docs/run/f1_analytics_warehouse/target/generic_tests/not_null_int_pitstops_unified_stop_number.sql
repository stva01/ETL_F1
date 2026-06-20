
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stop_number
from "f1_analytics"."main_intermediate"."int_pitstops_unified"
where stop_number is null



  
  
      
    ) dbt_internal_test