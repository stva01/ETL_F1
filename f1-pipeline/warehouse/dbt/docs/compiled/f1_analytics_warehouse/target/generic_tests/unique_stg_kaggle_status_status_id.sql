
    
    

select
    status_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_staging"."stg_kaggle_status"
where status_id is not null
group by status_id
having count(*) > 1


