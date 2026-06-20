
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    raceId as unique_field,
    count(*) as n_records

from "f1_analytics"."processed_kaggle"."races"
where raceId is not null
group by raceId
having count(*) > 1



  
  
      
    ) dbt_internal_test