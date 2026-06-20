
  
    
    

    create  table
      "f1_analytics"."main_marts"."dim_race_status__dbt_tmp"
  
    as (
      

/*
  dim_race_status
  ---------------
  One row per status code (~140 rows).
  Grain: 1 row per status_id.

  Categories:
    Finished — completed race (including lapped)
    DNF      — did not finish (mechanical, accident, other)
    DNS      — did not start / withdrew
    DSQ      — disqualified
*/

select
    status_id             as status_key,
    status_description    as status_label,

    case
        when status_description = 'Finished'           then 'Finished'
        when status_description like '+%Lap%'           then 'Finished'
        when status_description = 'Disqualified'        then 'DSQ'
        when status_description like 'Did not%'         then 'DNS'
        when status_description = 'Not classified'      then 'DNS'
        when status_description = 'Withdrew'            then 'DNS'
        when status_description = 'Excluded'            then 'DSQ'
        else 'DNF'
    end                   as status_category,

    case
        when status_description = 'Finished'            then true
        when status_description like '+%Lap%'            then true
        else false
    end                   as is_classified,

    case
        when status_description in (
            'Engine', 'Gearbox', 'Transmission', 'Hydraulics',
            'Electrical', 'Radiator', 'Suspension', 'Brakes',
            'Clutch', 'Throttle', 'Steering', 'Turbo', 'Exhaust',
            'Oil leak', 'Fuel leak', 'Overheating', 'Mechanical',
            'Differential', 'ERS', 'Power Unit', 'Battery',
            'Fuel system', 'Fuel pressure', 'Water leak', 'Wheel',
            'Wheel nut', 'Driveshaft', 'Tyre', 'Puncture',
            'Technical', 'Oil pressure', 'Water pressure',
            'Alternator', 'Drivetrain', 'Fuel pipe', 'Oil line',
            'Halfshaft', 'Crankshaft', 'Engine fire'
        ) then true
        else false
    end                   as is_mechanical,

    case
        when status_description in (
            'Accident', 'Collision', 'Collision damage',
            'Spun off', 'Fatal accident'
        ) then true
        else false
    end                   as is_accident,

    current_timestamp     as dw_created_at
from "f1_analytics"."main_staging"."stg_kaggle_status"
    );
  
  