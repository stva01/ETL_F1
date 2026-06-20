
    
    

select
    result_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."fct_race_result"
where result_id is not null
group by result_id
having count(*) > 1


