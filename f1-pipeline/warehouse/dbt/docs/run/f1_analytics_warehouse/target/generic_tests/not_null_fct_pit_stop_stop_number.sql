
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stop_number
from "f1_analytics"."main_marts"."fct_pit_stop"
where stop_number is null



  
  
      
    ) dbt_internal_test