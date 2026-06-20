
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    constructorId as unique_field,
    count(*) as n_records

from "f1_analytics"."processed_kaggle"."constructors"
where constructorId is not null
group by constructorId
having count(*) > 1



  
  
      
    ) dbt_internal_test