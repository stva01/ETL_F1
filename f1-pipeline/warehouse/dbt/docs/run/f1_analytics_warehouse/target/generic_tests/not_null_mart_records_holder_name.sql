
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select holder_name
from "f1_analytics"."main_marts"."mart_records"
where holder_name is null



  
  
      
    ) dbt_internal_test