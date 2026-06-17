"""
Read schemas from the existing DuckDB file that dbt already populated.
Also check if any of the new tables are already loaded as views/schemas.
"""
import duckdb

con = duckdb.connect("warehouse/dbt/f1_analytics.duckdb", read_only=True)

print("=== ALL SCHEMAS IN DUCKDB ===")
schemas = con.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name").fetchall()
for s in schemas:
    print(f"  {s[0]}")

print("\n=== ALL TABLES BY SCHEMA ===")
tables = con.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    ORDER BY table_schema, table_name
""").fetchall()
for t in tables:
    print(f"  {t[0]}.{t[1]}")

# Check if processed_jolpica schema has any of the new tables accessible
print("\n=== PROCESSED_JOLPICA VIEWS/TABLES ===")
try:
    jol_tables = con.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'processed_jolpica'
    """).fetchall()
    for t in jol_tables:
        print(f"  {t[0]}")
        try:
            cols = con.execute(f"DESCRIBE SELECT * FROM processed_jolpica.{t[0]} LIMIT 0").fetchall()
            for c in cols:
                print(f"    {c[0]:<35} {c[1]}")
        except Exception as e:
            print(f"    Error describing: {e}")
except Exception as e:
    print(f"  Error: {e}")

print("\n=== PROCESSED_OPENF1 VIEWS/TABLES ===")
try:
    of1_tables = con.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'processed_openf1'
    """).fetchall()
    for t in of1_tables:
        print(f"  {t[0]}")
        try:
            cols = con.execute(f"DESCRIBE SELECT * FROM processed_openf1.{t[0]} LIMIT 0").fetchall()
            for c in cols:
                print(f"    {c[0]:<35} {c[1]}")
        except Exception as e:
            print(f"    Error describing: {e}")
except Exception as e:
    print(f"  Error: {e}")

con.close()
