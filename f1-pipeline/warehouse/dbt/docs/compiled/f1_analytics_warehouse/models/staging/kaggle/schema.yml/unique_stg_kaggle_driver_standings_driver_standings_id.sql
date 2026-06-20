
    
    

select
    driver_standings_id as unique_field,
    count(*) as n_records

from "f1_analytics"."main_staging"."stg_kaggle_driver_standings"
where driver_standings_id is not null
group by driver_standings_id
having count(*) > 1


