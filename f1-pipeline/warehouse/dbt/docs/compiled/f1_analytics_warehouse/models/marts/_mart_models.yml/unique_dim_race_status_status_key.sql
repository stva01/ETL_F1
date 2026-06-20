
    
    

select
    status_key as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."dim_race_status"
where status_key is not null
group by status_key
having count(*) > 1


