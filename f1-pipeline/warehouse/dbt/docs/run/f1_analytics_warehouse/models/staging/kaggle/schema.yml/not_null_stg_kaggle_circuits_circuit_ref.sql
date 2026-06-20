
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select circuit_ref
from "f1_analytics"."main_staging"."stg_kaggle_circuits"
where circuit_ref is null



  
  
      
    ) dbt_internal_test