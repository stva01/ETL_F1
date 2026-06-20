
    
    

select
    circuit_key as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."dim_circuit"
where circuit_key is not null
group by circuit_key
having count(*) > 1


