
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    circuitId as unique_field,
    count(*) as n_records

from "f1_analytics"."processed_kaggle"."circuits"
where circuitId is not null
group by circuitId
having count(*) > 1



  
  
      
    ) dbt_internal_test