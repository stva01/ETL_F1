
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructor_id
from "f1_analytics"."main_staging"."stg_kaggle_qualifying"
where constructor_id is null



  
  
      
    ) dbt_internal_test