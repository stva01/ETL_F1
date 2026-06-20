
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select driver_a_key
from "f1_analytics"."main_marts"."mart_head_to_head"
where driver_a_key is null



  
  
      
    ) dbt_internal_test