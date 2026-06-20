
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select era_label
from "f1_analytics"."main_marts"."dim_season"
where era_label is null



  
  
      
    ) dbt_internal_test