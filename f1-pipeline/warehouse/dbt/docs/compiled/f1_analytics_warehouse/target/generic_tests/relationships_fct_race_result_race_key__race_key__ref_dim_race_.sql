
    
    

with child as (
    select race_key as from_field
    from "f1_analytics"."main_marts"."fct_race_result"
    where race_key is not null
),

parent as (
    select race_key as to_field
    from "f1_analytics"."main_marts"."dim_race"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


