
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select circuitId
from "f1_analytics"."processed_kaggle"."circuits"
where circuitId is null



  
  
      
    ) dbt_internal_test