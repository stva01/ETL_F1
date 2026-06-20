
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select result_id
from "f1_analytics"."main_intermediate"."int_results_enriched"
where result_id is null



  
  
      
    ) dbt_internal_test