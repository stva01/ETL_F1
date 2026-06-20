
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructorId
from "f1_analytics"."processed_kaggle"."constructors"
where constructorId is null



  
  
      
    ) dbt_internal_test