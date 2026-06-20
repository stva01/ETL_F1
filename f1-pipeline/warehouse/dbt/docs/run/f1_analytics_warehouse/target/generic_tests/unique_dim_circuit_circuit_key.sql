
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    circuit_key as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."dim_circuit"
where circuit_key is not null
group by circuit_key
having count(*) > 1



  
  
      
    ) dbt_internal_test