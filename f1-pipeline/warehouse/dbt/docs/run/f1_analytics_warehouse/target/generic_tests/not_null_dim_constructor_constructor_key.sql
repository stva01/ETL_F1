
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructor_key
from "f1_analytics"."main_marts"."dim_constructor"
where constructor_key is null



  
  
      
    ) dbt_internal_test