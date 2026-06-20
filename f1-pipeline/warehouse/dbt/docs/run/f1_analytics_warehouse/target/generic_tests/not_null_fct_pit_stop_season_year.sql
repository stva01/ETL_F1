
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season_year
from "f1_analytics"."main_marts"."fct_pit_stop"
where season_year is null



  
  
      
    ) dbt_internal_test