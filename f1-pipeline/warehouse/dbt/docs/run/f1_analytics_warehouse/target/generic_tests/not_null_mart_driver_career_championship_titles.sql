
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select championship_titles
from "f1_analytics"."main_marts"."mart_driver_career"
where championship_titles is null



  
  
      
    ) dbt_internal_test