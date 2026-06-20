
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select raceId
from "f1_analytics"."processed_kaggle"."races"
where raceId is null



  
  
      
    ) dbt_internal_test