
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select races_as_teammates
from "f1_analytics"."main_marts"."mart_head_to_head"
where races_as_teammates is null



  
  
      
    ) dbt_internal_test