
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "f1_analytics"."processed_kaggle"."seasons"
where year is null



  
  
      
    ) dbt_internal_test