
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructor_id
from "f1_analytics"."main_intermediate"."int_constructor_bridge"
where constructor_id is null



  
  
      
    ) dbt_internal_test