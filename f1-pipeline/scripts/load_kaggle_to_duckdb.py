# Updated script with schema introspection and safe loading
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from config.config import Config

DB_PATH = "./warehouse/dbt/f1_analytics.duckdb"
print(f"🔄 Connecting to database at: {DB_PATH}")
conn = duckdb.connect(DB_PATH)

conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
conn.execute(f"""
    SET s3_region='ap-south-1';
    SET s3_access_key_id='{Config.AWS_ACCESS_KEY}';
    SET s3_secret_access_key='{Config.AWS_SECRET_KEY}';
""")

BUCKET = "f1-pipeline-processed-layer-034381339055"

TABLES_TO_LOAD = [
    # --- JOLPICA ---
    {"schema": "processed_jolpica", "name": "results"},
    {"schema": "processed_jolpica", "name": "qualifying"},
    {"schema": "processed_jolpica", "name": "pitstops"},
    {"schema": "processed_jolpica", "name": "driver_standings"},
    {"schema": "processed_jolpica", "name": "constructor_standings"},
    {"schema": "processed_jolpica", "name": "sprint"},

    # --- OPENF1 ---
    {"schema": "processed_openf1", "name": "drivers"},
    {"schema": "processed_openf1", "name": "laps"},
    {"schema": "processed_openf1", "name": "pit"},
    {"schema": "processed_openf1", "name": "stints"}
]

# Ensure schemas exist
conn.execute("CREATE SCHEMA IF NOT EXISTS processed_jolpica")
conn.execute("CREATE SCHEMA IF NOT EXISTS processed_openf1")
conn.execute("CREATE SCHEMA IF NOT EXISTS processed_kaggle")

print(f"🚀 Loading tables from S3 Bucket: {BUCKET}")

for table in TABLES_TO_LOAD:
    schema = table["schema"]
    name = table["name"]
    source_dir = schema.split('_')[1]  # 'processed_jolpica' -> 'jolpica'
    
    parquet_path = f"s3://{BUCKET}/processed/{source_dir}/{name}/**/*.parquet"

    try:
        print(f"  Loading {schema}.{name}...", end=" ")
        
        # CRITICAL FIX: Add union_by_name=True to handle schema mismatches
        # Also add hive_partitioning to read partitioned data correctly
        conn.execute(f"DROP TABLE IF EXISTS {schema}.{name}")
        conn.execute(f"""
            CREATE TABLE {schema}.{name} AS
            SELECT *
            FROM read_parquet('{parquet_path}', 
                              hive_partitioning=true, 
                              union_by_name=true)
        """)
        
        # Verify columns and count
        columns = conn.execute(f"DESCRIBE {schema}.{name}").fetchall()
        col_names = [col[0] for col in columns]
        
        count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{name}").fetchone()[0]
        print(f"✅ ({count:,} rows, {len(col_names)} columns)")
        print(f"     Columns: {', '.join(col_names[:8])}{'...' if len(col_names) > 8 else ''}")
        
    except Exception as e:
        print(f"❌ Failed: {e}")

# Detailed verification of all tables
print("\n📊 VERIFICATION - All Tables:")
tables_to_check = [
    ("processed_jolpica", "qualifying"),
    ("processed_jolpica", "results"),
    ("processed_jolpica", "pitstops"),
    ("processed_jolpica", "driver_standings"),
    ("processed_jolpica", "constructor_standings"),
    ("processed_jolpica", "sprint"),
    ("processed_openf1", "drivers"),
    ("processed_openf1", "laps"),
    ("processed_openf1", "pit"),
    ("processed_openf1", "stints")
]

for schema, table_name in tables_to_check:
    try:
        exists = conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        """).fetchone()[0]
        
        if exists > 0:
            count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            print(f"  ✅ {schema}.{table_name}: {count:,} rows")
        else:
            print(f"  ❌ {schema}.{table_name}: MISSING")
    except Exception as e:
        print(f"  ⚠️ {schema}.{table_name}: Error - {e}")

conn.close()
print("\n✅ Load complete! Ready for column inspection.")