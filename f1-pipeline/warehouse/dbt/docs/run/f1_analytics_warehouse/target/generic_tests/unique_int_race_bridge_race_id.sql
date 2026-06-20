
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    race_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_intermediate"."int_race_bridge"
where race_id is not null
group by race_id
having count(*) > 1



  
  
      
    ) dbt_internal_test