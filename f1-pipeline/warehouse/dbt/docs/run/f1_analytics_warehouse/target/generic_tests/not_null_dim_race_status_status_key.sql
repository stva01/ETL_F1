
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status_key
from "f1_analytics"."main_marts"."dim_race_status"
where status_key is null



  
  
      
    ) dbt_internal_test