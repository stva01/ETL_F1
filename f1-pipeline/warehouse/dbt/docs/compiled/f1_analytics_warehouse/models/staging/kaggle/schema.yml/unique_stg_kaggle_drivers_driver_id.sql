
    
    

select
    driver_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_staging"."stg_kaggle_drivers"
where driver_id is not null
group by driver_id
having count(*) > 1


