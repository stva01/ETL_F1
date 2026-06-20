
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    season_year as unique_field,
    count(*) as n_records

from "f1_analytics"."main_staging"."stg_kaggle_seasons"
where season_year is not null
group by season_year
having count(*) > 1



  
  
      
    ) dbt_internal_test