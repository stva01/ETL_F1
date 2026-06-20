
    
    

select
    raceId as unique_field,
    count(*) as n_records

from "f1_analytics"."processed_kaggle"."races"
where raceId is not null
group by raceId
having count(*) > 1


