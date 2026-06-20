
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select race_key
from "f1_analytics"."main_marts"."fct_race_result"
where race_key is null



  
  
      
    ) dbt_internal_test