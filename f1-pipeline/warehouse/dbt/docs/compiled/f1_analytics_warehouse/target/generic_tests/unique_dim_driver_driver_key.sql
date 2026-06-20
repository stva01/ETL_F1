
    
    

select
    driver_key as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."dim_driver"
where driver_key is not null
group by driver_key
having count(*) > 1


