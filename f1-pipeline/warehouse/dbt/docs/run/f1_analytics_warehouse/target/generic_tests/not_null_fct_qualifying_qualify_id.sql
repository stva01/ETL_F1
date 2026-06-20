
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select qualify_id
from "f1_analytics"."main_marts"."fct_qualifying"
where qualify_id is null



  
  
      
    ) dbt_internal_test