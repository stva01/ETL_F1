
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructor_name
from "f1_analytics"."main_marts"."dim_constructor"
where constructor_name is null



  
  
      
    ) dbt_internal_test