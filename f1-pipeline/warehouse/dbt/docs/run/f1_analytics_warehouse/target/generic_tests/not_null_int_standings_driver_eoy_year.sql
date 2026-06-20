
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "f1_analytics"."main_intermediate"."int_standings_driver_eoy"
where year is null



  
  
      
    ) dbt_internal_test