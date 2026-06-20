
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_number
from "f1_analytics"."main_staging"."stg_openf1_drivers"
where driver_number is null



  
  
      
    ) dbt_internal_test