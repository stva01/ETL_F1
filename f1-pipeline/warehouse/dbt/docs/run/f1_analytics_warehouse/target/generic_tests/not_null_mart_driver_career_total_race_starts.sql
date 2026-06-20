
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_race_starts
from "f1_analytics"."main_marts"."mart_driver_career"
where total_race_starts is null



  
  
      
    ) dbt_internal_test