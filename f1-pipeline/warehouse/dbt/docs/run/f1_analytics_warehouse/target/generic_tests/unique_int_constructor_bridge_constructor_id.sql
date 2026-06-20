
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    constructor_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_intermediate"."int_constructor_bridge"
where constructor_id is not null
group by constructor_id
having count(*) > 1



  
  
      
    ) dbt_internal_test