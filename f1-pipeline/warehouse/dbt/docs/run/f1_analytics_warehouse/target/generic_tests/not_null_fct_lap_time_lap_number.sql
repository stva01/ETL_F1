
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lap_number
from "f1_analytics"."main_marts"."fct_lap_time"
where lap_number is null



  
  
      
    ) dbt_internal_test