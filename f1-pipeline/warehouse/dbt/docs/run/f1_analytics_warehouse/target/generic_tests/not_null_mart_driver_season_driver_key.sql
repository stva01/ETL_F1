
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_key
from "f1_analytics"."main_marts"."mart_driver_season"
where driver_key is null



  
  
      
    ) dbt_internal_test