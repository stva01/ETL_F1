
    
    

select
    constructor_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_staging"."stg_kaggle_constructors"
where constructor_id is not null
group by constructor_id
having count(*) > 1


