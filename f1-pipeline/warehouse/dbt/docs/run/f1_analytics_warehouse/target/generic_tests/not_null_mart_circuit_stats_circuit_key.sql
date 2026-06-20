
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select circuit_key
from "f1_analytics"."main_marts"."mart_circuit_stats"
where circuit_key is null



  
  
      
    ) dbt_internal_test