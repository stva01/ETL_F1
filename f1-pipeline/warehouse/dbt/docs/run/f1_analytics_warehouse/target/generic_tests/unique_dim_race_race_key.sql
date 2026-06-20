
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    race_key as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."dim_race"
where race_key is not null
group by race_key
having count(*) > 1



  
  
      
    ) dbt_internal_test