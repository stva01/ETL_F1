
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    record_category as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."mart_records"
where record_category is not null
group by record_category
having count(*) > 1



  
  
      
    ) dbt_internal_test