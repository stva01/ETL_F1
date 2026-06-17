{{
  config(
    materialized='table',
    meta={'owner': 'data-eng', 'domain': 'f1_racing'},
    tags=['intermediate', 'bridge', 'identity']
  )
}}

/*
  int_constructor_bridge
  ----------------------
  One row per constructor, all-time. Maps:
    - Kaggle:  constructor_id (integer, master key)
    - Jolpica: constructor_id (string = Kaggle constructor_ref, e.g. "red_bull")
    - OpenF1:  team_name (string, e.g. "Red Bull Racing")
*/

with kaggle_constructors as (
    select
        constructor_id,
        constructor_ref,
        constructor_name,
        nationality     as constructor_nationality
    from {{ ref('stg_kaggle_constructors') }}
),

-- Distinct Jolpica constructor strings from results (2025+ coverage)
jolpica_refs as (
    select distinct
        constructor_id as jolpica_constructor_id
    from {{ ref('stg_jolpica_results') }}
),

-- Distinct OpenF1 team names with their latest colour
openf1_teams as (
    select
        team_name         as openf1_team_name,
        team_colour       as openf1_team_colour,
        -- Normalise to lowercase for fuzzy matching
        lower(trim(team_name)) as team_name_lower
    from (
        select
            team_name,
            team_colour,
            row_number() over (
                partition by lower(trim(team_name))
                order by year desc
            ) as rn
        from {{ ref('stg_openf1_drivers') }}
        where team_name is not null
    ) ranked
    where rn = 1
),

joined as (
    select
        kc.constructor_id,
        kc.constructor_ref,
        kc.constructor_name,
        kc.constructor_nationality,

        -- Jolpica string key (direct ref match)
        jr.jolpica_constructor_id,

        -- OpenF1 team name (fuzzy-matched on constructor_ref, only for refs >= 4 chars)
        ot.openf1_team_name,
        ot.openf1_team_colour,

        -- Activity flags
        case when jr.jolpica_constructor_id is not null then true else false end
            as is_active_2025,
        case when ot.openf1_team_name is not null then true else false end
            as has_openf1_data,

        -- Rank to deduplicate: if a constructor_ref matches multiple team names,
        -- prefer the shortest team_name (most specific match)
        row_number() over (
            partition by kc.constructor_id
            order by
                case when ot.openf1_team_name is not null then length(ot.openf1_team_name) else 9999 end
        ) as rn
    from kaggle_constructors kc
    -- Jolpica: constructor_ref = jolpica constructor_id
    left join jolpica_refs jr
        on kc.constructor_ref = jr.jolpica_constructor_id
    -- OpenF1: fuzzy match — only attempt for refs with >= 4 chars to avoid false matches
    left join openf1_teams ot
        on length(kc.constructor_ref) >= 4
        and (
            ot.team_name_lower like '%' || replace(kc.constructor_ref, '_', ' ') || '%'
            or ot.team_name_lower like '%' || replace(kc.constructor_ref, '_', '') || '%'
        )
),

bridged as (
    select
        constructor_id,
        constructor_ref,
        constructor_name,
        constructor_nationality,
        jolpica_constructor_id,
        openf1_team_name,
        openf1_team_colour,
        is_active_2025,
        has_openf1_data
    from joined
    where rn = 1
)

select
    constructor_id,
    constructor_ref,
    constructor_name,
    constructor_nationality,
    jolpica_constructor_id,
    openf1_team_name,
    openf1_team_colour,
    is_active_2025,
    has_openf1_data,
    current_timestamp as _dbt_loaded_at
from bridged
