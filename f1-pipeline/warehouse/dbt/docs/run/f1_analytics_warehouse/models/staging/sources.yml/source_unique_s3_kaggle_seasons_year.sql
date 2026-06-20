
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    year as unique_field,
    count(*) as n_records

from "f1_analytics"."processed_kaggle"."seasons"
where year is not null
group by year
having count(*) > 1



  
  
      
    ) dbt_internal_test