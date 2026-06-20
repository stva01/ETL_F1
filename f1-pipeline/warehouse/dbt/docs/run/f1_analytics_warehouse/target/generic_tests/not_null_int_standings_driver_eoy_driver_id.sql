
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_id
from "f1_analytics"."main_intermediate"."int_standings_driver_eoy"
where driver_id is null



  
  
      
    ) dbt_internal_test