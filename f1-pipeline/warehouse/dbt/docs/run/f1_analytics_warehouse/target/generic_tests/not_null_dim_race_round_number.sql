
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select round_number
from "f1_analytics"."main_marts"."dim_race"
where round_number is null



  
  
      
    ) dbt_internal_test