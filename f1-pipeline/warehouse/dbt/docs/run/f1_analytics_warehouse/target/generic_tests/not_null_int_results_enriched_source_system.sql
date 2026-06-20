
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_system
from "f1_analytics"."main_intermediate"."int_results_enriched"
where source_system is null



  
  
      
    ) dbt_internal_test