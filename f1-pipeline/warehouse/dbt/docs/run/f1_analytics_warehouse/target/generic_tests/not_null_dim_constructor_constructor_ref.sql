
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructor_ref
from "f1_analytics"."main_marts"."dim_constructor"
where constructor_ref is null



  
  
      
    ) dbt_internal_test