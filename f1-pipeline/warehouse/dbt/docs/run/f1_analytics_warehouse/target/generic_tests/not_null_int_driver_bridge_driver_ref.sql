
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_ref
from "f1_analytics"."main_intermediate"."int_driver_bridge"
where driver_ref is null



  
  
      
    ) dbt_internal_test