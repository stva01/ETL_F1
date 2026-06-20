
    
    

select
    season_year as unique_field,
    count(*) as n_records

from "f1_analytics"."main_marts"."dim_season"
where season_year is not null
group by season_year
having count(*) > 1


