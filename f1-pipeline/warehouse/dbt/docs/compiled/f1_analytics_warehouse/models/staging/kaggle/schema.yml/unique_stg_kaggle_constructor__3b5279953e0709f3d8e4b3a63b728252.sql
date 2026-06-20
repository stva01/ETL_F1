
    
    

select
    constructor_standings_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_staging"."stg_kaggle_constructor_standings"
where constructor_standings_id is not null
group by constructor_standings_id
having count(*) > 1


