
    
    

select
    year as unique_field,
    count(*) as n_records

from "f1_analytics"."processed_kaggle"."seasons"
where year is not null
group by year
having count(*) > 1


