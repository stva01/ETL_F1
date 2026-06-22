-- Gold Layer Status Check
-- Run via: dbt compile then run directly against DuckDB

-- 1. Row counts for every mart model
SELECT 'dim_circuit'          AS model, COUNT(*) AS rows FROM main_marts.dim_circuit
UNION ALL SELECT 'dim_constructor',     COUNT(*) FROM main_marts.dim_constructor
UNION ALL SELECT 'dim_driver',          COUNT(*) FROM main_marts.dim_driver
UNION ALL SELECT 'dim_race',            COUNT(*) FROM main_marts.dim_race
UNION ALL SELECT 'dim_race_status',     COUNT(*) FROM main_marts.dim_race_status
UNION ALL SELECT 'dim_season',          COUNT(*) FROM main_marts.dim_season
UNION ALL SELECT 'fct_race_result',     COUNT(*) FROM main_marts.fct_race_result
UNION ALL SELECT 'fct_qualifying',      COUNT(*) FROM main_marts.fct_qualifying
UNION ALL SELECT 'fct_pit_stop',        COUNT(*) FROM main_marts.fct_pit_stop
UNION ALL SELECT 'fct_lap_time',        COUNT(*) FROM main_marts.fct_lap_time
UNION ALL SELECT 'mart_driver_career',  COUNT(*) FROM main_marts.mart_driver_career
UNION ALL SELECT 'mart_driver_season',  COUNT(*) FROM main_marts.mart_driver_season
UNION ALL SELECT 'mart_constructor_career', COUNT(*) FROM main_marts.mart_constructor_career
UNION ALL SELECT 'mart_season_summary', COUNT(*) FROM main_marts.mart_season_summary
UNION ALL SELECT 'mart_circuit_stats',  COUNT(*) FROM main_marts.mart_circuit_stats
UNION ALL SELECT 'mart_race_summary',   COUNT(*) FROM main_marts.mart_race_summary
UNION ALL SELECT 'mart_head_to_head',   COUNT(*) FROM main_marts.mart_head_to_head
UNION ALL SELECT 'mart_records',        COUNT(*) FROM main_marts.mart_records
ORDER BY 1;
