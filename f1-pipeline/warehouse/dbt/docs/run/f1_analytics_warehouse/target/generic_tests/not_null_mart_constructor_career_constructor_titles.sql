
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select constructor_titles
from "f1_analytics"."main_marts"."mart_constructor_career"
where constructor_titles is null



  
  
      
    ) dbt_internal_test