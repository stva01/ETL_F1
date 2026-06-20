
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "f1_analytics"."main_staging"."stg_kaggle_driver_standings"
where year is null



  
  
      
    ) dbt_internal_test