
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select season_year
from "f1_analytics"."main_marts"."mart_season_summary"
where season_year is null



  
  
      
    ) dbt_internal_test