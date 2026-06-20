
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    constructor_key as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."mart_constructor_career"
where constructor_key is not null
group by constructor_key
having count(*) > 1



  
  
      
    ) dbt_internal_test