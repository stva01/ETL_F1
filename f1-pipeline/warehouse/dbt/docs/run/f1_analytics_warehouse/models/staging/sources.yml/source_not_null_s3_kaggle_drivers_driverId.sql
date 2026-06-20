
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driverId
from "f1_analytics"."processed_kaggle"."drivers"
where driverId is null



  
  
      
    ) dbt_internal_test