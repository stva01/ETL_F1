"""
DuckDB Dashboard Readiness Audit Script
Checks all data required for the F1 Dashboard is loaded correctly.
"""
import duckdb
import os
import sys

# Force UTF-8 output so emoji render correctly on Windows PowerShell
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'warehouse', 'dbt', 'f1_analytics.duckdb')

def hr(char='=', width=70):
    print(char * width)

def section(title):
    print()
    hr()
    print(f"  {title}")
    hr()

def run():
    print(f"\nConnecting to: {os.path.abspath(DB_PATH)}")
    db = duckdb.connect(DB_PATH, read_only=True)

    # ── 1. Schemas ──────────────────────────────────────────────────────────
    section("1. SCHEMAS PRESENT")
    schemas = db.execute(
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
    ).fetchall()
    for s in schemas:
        print(f"  • {s[0]}")

    # ── 2. Tables + row counts ───────────────────────────────────────────────
    section("2. ALL TABLES + ROW COUNTS")
    tables = db.execute(
        """SELECT table_schema, table_name
           FROM information_schema.tables
           WHERE table_schema NOT IN ('information_schema','pg_catalog','main')
           ORDER BY table_schema, table_name"""
    ).fetchall()

    schema_totals = {}
    for schema, tbl in tables:
        try:
            cnt = db.execute(f'SELECT COUNT(*) FROM "{schema}"."{tbl}"').fetchone()[0]
            print(f"  {schema}.{tbl}: {cnt:>10,} rows")
            schema_totals[schema] = schema_totals.get(schema, 0) + cnt
        except Exception as e:
            print(f"  {schema}.{tbl}: ERROR — {e}")

    print()
    for schema, total in schema_totals.items():
        print(f"  TOTAL in {schema}: {total:,} rows")

    # ── 3. Dashboard required tables checklist ───────────────────────────────
    section("3. DASHBOARD REQUIRED-TABLE CHECKLIST")
    required = {
        "processed_kaggle": [
            "circuits", "constructors", "drivers", "seasons",
            "races", "results", "qualifying", "lap_times",
            "pit_stops", "driver_standings", "constructor_standings", "status"
        ],
        "processed_jolpica": [
            "results", "qualifying", "pitstops",
            "driver_standings", "constructor_standings", "sprint"
        ],
        "processed_openf1": [
            "drivers", "laps", "pit", "stints"
        ]
    }

    all_ok = True
    for schema, tbls in required.items():
        print(f"\n  [{schema}]")
        for tbl in tbls:
            try:
                cnt = db.execute(f'SELECT COUNT(*) FROM "{schema}"."{tbl}"').fetchone()[0]
                flag = "✅" if cnt > 0 else "⚠️  EMPTY"
                print(f"    {flag}  {tbl}: {cnt:,} rows")
                if cnt == 0:
                    all_ok = False
            except Exception:
                print(f"    ❌  {tbl}: MISSING")
                all_ok = False

    # ── 4. Key column spot-checks ────────────────────────────────────────────
    section("4. KEY COLUMN SPOT-CHECKS (Kaggle)")
    spot = [
        ("processed_kaggle", "drivers",
         "SELECT COUNT(*) as total, COUNT(DISTINCT nationality) as nationalities, MIN(dob) as oldest FROM drivers"),
        ("processed_kaggle", "races",
         "SELECT MIN(year) as first_year, MAX(year) as last_year, COUNT(*) as total FROM races"),
        ("processed_kaggle", "results",
         "SELECT COUNT(*) as total, SUM(CASE WHEN position IS NULL THEN 1 ELSE 0 END) as dnf_nulls, COUNT(DISTINCT raceId) as races FROM results"),
        ("processed_kaggle", "qualifying",
         "SELECT COUNT(*) as total, COUNT(DISTINCT raceId) as races_with_quali FROM qualifying"),
        ("processed_kaggle", "driver_standings",
         "SELECT COUNT(*) as total, COUNT(DISTINCT driverId) as drivers FROM driver_standings"),
        ("processed_kaggle", "constructor_standings",
         "SELECT COUNT(*) as total, COUNT(DISTINCT constructorId) as constructors FROM constructor_standings"),
    ]
    for schema, tbl, sql in spot:
        try:
            full_sql = f'SELECT * FROM ({sql}) t'
            row = db.execute(full_sql).fetchone()
            cols = [d[0] for d in db.execute(full_sql).description]
            result_str = ", ".join(f"{c}={v}" for c, v in zip(cols, row))
            print(f"  {schema}.{tbl}: {result_str}")
        except Exception as e:
            print(f"  {schema}.{tbl}: ERROR — {e}")

    # ── 5. Season coverage check ─────────────────────────────────────────────
    section("5. SEASON COVERAGE (processed_kaggle.races)")
    try:
        rows = db.execute(
            """SELECT year, COUNT(*) as races
               FROM processed_kaggle.races
               GROUP BY year ORDER BY year"""
        ).fetchall()
        first = rows[0]
        last = rows[-1]
        print(f"  First season: {first[0]} ({first[1]} races)")
        print(f"  Last season:  {last[0]} ({last[1]} races)")
        print(f"  Total seasons covered: {len(rows)}")
        # Check for gaps
        years = [r[0] for r in rows]
        gaps = [years[i] for i in range(1, len(years)) if years[i] - years[i-1] > 1]
        if gaps:
            print(f"  ⚠️  Year gaps detected before: {gaps}")
        else:
            print(f"  ✅  No year gaps detected")
    except Exception as e:
        print(f"  ERROR — {e}")

    # ── 6. Jolpica recent data check ─────────────────────────────────────────
    section("6. JOLPICA 2025 SEASON DATA")
    try:
        rows = db.execute(
            """SELECT season, COUNT(DISTINCT round) as rounds, COUNT(*) as result_rows
               FROM processed_jolpica.results
               GROUP BY season ORDER BY season"""
        ).fetchall()
        if rows:
            for r in rows:
                print(f"  Season {r[0]}: {r[1]} rounds, {r[2]} result rows")
        else:
            print("  ⚠️  No Jolpica results data found!")
    except Exception as e:
        print(f"  ERROR — {e}")

    # ── 7. OpenF1 data check ─────────────────────────────────────────────────
    section("7. OPENF1 TELEMETRY DATA")
    for tbl in ["drivers", "laps", "pit", "stints"]:
        try:
            rows = db.execute(
                f"""SELECT year, COUNT(*) as rows
                    FROM processed_openf1.{tbl}
                    GROUP BY year ORDER BY year"""
            ).fetchall()
            if rows:
                summary = ", ".join(f"{r[0]}:{r[1]}" for r in rows)
                print(f"  {tbl}: {summary}")
            else:
                print(f"  {tbl}: ⚠️  EMPTY")
        except Exception as e:
            print(f"  {tbl}: ERROR — {e}")

    # ── 8. Dashboard join health ─────────────────────────────────────────────
    section("8. JOIN HEALTH — races ↔ results ↔ drivers ↔ constructors")
    try:
        row = db.execute(
            """SELECT
                COUNT(DISTINCT r.raceId)      AS races,
                COUNT(DISTINCT res.resultId)  AS results,
                COUNT(DISTINCT d.driverId)    AS drivers,
                COUNT(DISTINCT c.constructorId) AS constructors
               FROM processed_kaggle.races r
               LEFT JOIN processed_kaggle.results res ON r.raceId = res.raceId
               LEFT JOIN processed_kaggle.drivers d   ON res.driverId = d.driverId
               LEFT JOIN processed_kaggle.constructors c ON res.constructorId = c.constructorId"""
        ).fetchone()
        print(f"  Races:        {row[0]:,}")
        print(f"  Results:      {row[1]:,}")
        print(f"  Drivers:      {row[2]:,}")
        print(f"  Constructors: {row[3]:,}")
    except Exception as e:
        print(f"  ERROR — {e}")

    # ── 9. Avg drivers per race (sanity) ────────────────────────────────────
    section("9. AVG DRIVERS PER RACE (expected ~18-20)")
    try:
        row = db.execute(
            """SELECT
                COUNT(*) / NULLIF(COUNT(DISTINCT raceId), 0) AS avg_drivers_per_race,
                MIN(raceId) AS min_race, MAX(raceId) AS max_race
               FROM processed_kaggle.results"""
        ).fetchone()
        print(f"  Avg drivers/race: {row[0]:.1f}")
        print(f"  Race ID range: {row[1]} → {row[2]}")
    except Exception as e:
        print(f"  ERROR — {e}")

    # ── 10. Summary ──────────────────────────────────────────────────────────
    section("10. SUMMARY")
    if all_ok:
        print("  ✅  All required tables present and non-empty")
    else:
        print("  ⚠️  Some tables are MISSING or EMPTY — see checklist above")

    db.close()
    print()

if __name__ == "__main__":
    run()
