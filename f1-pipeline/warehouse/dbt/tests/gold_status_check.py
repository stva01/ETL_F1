import duckdb
conn = duckdb.connect('f1_analytics.duckdb')

print('=== ROW COUNTS ===')
counts = conn.execute("""
SELECT 'dim_circuit'               AS model, COUNT(*) AS rows FROM main_marts.dim_circuit
UNION ALL SELECT 'dim_constructor',           COUNT(*) FROM main_marts.dim_constructor
UNION ALL SELECT 'dim_driver',                COUNT(*) FROM main_marts.dim_driver
UNION ALL SELECT 'dim_race',                  COUNT(*) FROM main_marts.dim_race
UNION ALL SELECT 'dim_race_status',           COUNT(*) FROM main_marts.dim_race_status
UNION ALL SELECT 'dim_season',                COUNT(*) FROM main_marts.dim_season
UNION ALL SELECT 'fct_race_result',           COUNT(*) FROM main_marts.fct_race_result
UNION ALL SELECT 'fct_qualifying',            COUNT(*) FROM main_marts.fct_qualifying
UNION ALL SELECT 'fct_pit_stop',              COUNT(*) FROM main_marts.fct_pit_stop
UNION ALL SELECT 'fct_lap_time',              COUNT(*) FROM main_marts.fct_lap_time
UNION ALL SELECT 'mart_driver_career',        COUNT(*) FROM main_marts.mart_driver_career
UNION ALL SELECT 'mart_driver_season',        COUNT(*) FROM main_marts.mart_driver_season
UNION ALL SELECT 'mart_constructor_career',   COUNT(*) FROM main_marts.mart_constructor_career
UNION ALL SELECT 'mart_season_summary',       COUNT(*) FROM main_marts.mart_season_summary
UNION ALL SELECT 'mart_circuit_stats',        COUNT(*) FROM main_marts.mart_circuit_stats
UNION ALL SELECT 'mart_race_summary',         COUNT(*) FROM main_marts.mart_race_summary
UNION ALL SELECT 'mart_head_to_head',         COUNT(*) FROM main_marts.mart_head_to_head
UNION ALL SELECT 'mart_records',              COUNT(*) FROM main_marts.mart_records
ORDER BY 1
""").fetchall()
for r in counts:
    print(f'  {r[0]:<30} {r[1]:>8}')

print()
print('=== SPOT CHECKS ===')

hamilton = conn.execute("SELECT full_name, total_wins, championship_titles, total_poles, total_podiums FROM main_marts.mart_driver_career WHERE full_name = 'Lewis Hamilton'").fetchone()
print(f'Hamilton wins={hamilton[1]}, titles={hamilton[2]}, poles={hamilton[3]}, podiums={hamilton[4]}')

schu = conn.execute("SELECT full_name, total_wins, championship_titles FROM main_marts.mart_driver_career WHERE full_name = 'Michael Schumacher'").fetchone()
print(f'Schumacher wins={schu[1]}, titles={schu[2]}')

ferrari = conn.execute("SELECT constructor_name, total_wins, constructor_titles FROM main_marts.mart_constructor_career WHERE constructor_name = 'Ferrari'").fetchone()
print(f'Ferrari wins={ferrari[1]}, titles={ferrari[2]}')

s2023 = conn.execute("SELECT driver_champion, constructor_champion, champion_wins, champion_points FROM main_marts.mart_season_summary WHERE season_year = 2023").fetchone()
print(f'2023: WDC={s2023[0]}, WCC={s2023[1]}, wins={s2023[2]}, pts={s2023[3]}')

current = conn.execute("SELECT season_year, driver_champion_name FROM main_marts.dim_season WHERE season_year >= 2025 ORDER BY season_year DESC LIMIT 1").fetchone()
print(f'In-progress season champion (expect NULL): year={current[0]}, champion={current[1]}')

null_race = conn.execute('SELECT COUNT(*) FROM main_marts.fct_race_result WHERE race_key IS NULL').fetchone()[0]
null_driver = conn.execute('SELECT COUNT(*) FROM main_marts.fct_race_result WHERE driver_key IS NULL').fetchone()[0]
dupe_results = conn.execute('SELECT COUNT(*) FROM (SELECT result_id, COUNT(*) FROM main_marts.fct_race_result GROUP BY 1 HAVING COUNT(*) > 1)').fetchone()[0]
print(f'fct_race_result: NULL race_key={null_race}, NULL driver_key={null_driver}, dupe result_ids={dupe_results}')

monza = conn.execute("SELECT circuit_name, total_races_held FROM main_marts.mart_circuit_stats WHERE circuit_name ILIKE '%monza%'").fetchone()
print(f'Monza: {monza}')

h2h_null = conn.execute('SELECT COUNT(*) FROM main_marts.mart_head_to_head WHERE races_as_teammates = 0').fetchone()[0]
print(f'mart_head_to_head rows with 0 teammate races: {h2h_null}')

print()
print('=== MART_RECORDS ===')
records = conn.execute("SELECT record_category, record_type, holder_name, record_value FROM main_marts.mart_records ORDER BY 1,2").fetchall()
for r in records:
    print(f'  {str(r[0]):<35} {str(r[1]):<15} {str(r[2]):<30} {r[3]}')

conn.close()
