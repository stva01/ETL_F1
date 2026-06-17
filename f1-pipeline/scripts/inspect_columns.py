# inspect_columns.py
import duckdb

DB_PATH = "./warehouse/dbt/f1_analytics.duckdb"
conn = duckdb.connect(DB_PATH)

# Check all tables we care about
tables = [
    ("processed_jolpica", "qualifying"),
    ("processed_jolpica", "results"),
    ("processed_jolpica", "pitstops"),
    ("processed_jolpica", "driver_standings"),
    ("processed_jolpica", "constructor_standings"),
    ("processed_openf1", "drivers"),
    ("processed_openf1", "laps"),
    ("processed_openf1", "pit"),
    ("processed_openf1", "stints")
]

print("=" * 80)
print("ACTUAL COLUMNS IN EACH TABLE")
print("=" * 80)

for schema, table in tables:
    try:
        columns = conn.execute(f"DESCRIBE {schema}.{table}").fetchall()
        print(f"\n📊 {schema}.{table}:")
        for col in columns:
            print(f"   - {col[0]} ({col[1]})")
    except Exception as e:
        print(f"\n❌ {schema}.{table}: {e}")

# Specifically check for raceId vs race_id (common mismatch)
print("\n" + "=" * 80)
print("CHECKING FOR COMMON COLUMN NAME PATTERNS")
print("=" * 80)

for schema, table in tables:
    try:
        columns = conn.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
              AND table_name = '{table}'
              AND column_name ILIKE '%race%'
        """).fetchall()
        
        if columns:
            print(f"\n{schema}.{table} race-related columns: {[c[0] for c in columns]}")
    except:
        pass

conn.close()