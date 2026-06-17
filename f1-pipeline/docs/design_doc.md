# F1 World Championship Dashboard — Data Design Document

**Dashboard Type:** Historical F1 Analytics (1950–2024)  
**Current Pipeline Stage:** Staging Layer  
**Purpose:** Specification for data requirements across Bronze → Silver → Gold layers  
**Last Updated:** 2026

---

## 1. Overview

This document catalogs **all data entities, attributes, and calculations** required to populate the F1 World Championship Dashboard. Use this as the single source of truth for:
- Bronze layer: Raw ingestion validation
- Silver layer: Transformation scope
- Gold layer: Final dimensional model

---

## 2. Core Data Entities

### 2.1 DRIVERS
**Description:** Historical F1 driver records (1950–present)

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| driver_id | STRING (PK) | Kaggle/OpenF1 | Unique identifier (e.g., "hamilton", "verstappen") |
| name | STRING | Kaggle/OpenF1 | Full name |
| nationality | STRING (3-char code) | Kaggle/OpenF1 | ISO country code (e.g., "GBR", "NED") |
| championships_won | INT | Calculated | Count of world championship titles |
| race_wins | INT | Calculated | Aggregate from race_results |
| pole_positions | INT | Calculated | Count from qualifying results |
| podium_finishes | INT | Calculated | Count of P1/P2/P3 finishes |
| fastest_laps | INT | Calculated | Count from race_results |
| race_starts | INT | Calculated | Total entries across seasons |
| win_rate_percent | FLOAT | Calculated | (race_wins / race_starts) × 100 |
| first_season | INT | Kaggle | Year of debut |
| last_season | INT | Kaggle | Year of last race (NULL if active) |
| teams_list | ARRAY[STRING] | Calculated | Unique constructors driven for |
| current_team | STRING | OpenF1 | Latest team (if active) |

**Cardinality:** ~1,000 drivers  
**Grain:** One row per driver, all-time aggregates  
**Updates:** Append (new drivers); overwrite (season aggregate fields)

---

### 2.2 CONSTRUCTORS (Teams)
**Description:** F1 constructor/team records

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| constructor_id | STRING (PK) | Kaggle/OpenF1 | Unique identifier (e.g., "mercedes", "red_bull") |
| name | STRING | Kaggle/OpenF1 | Full constructor name |
| nationality | STRING (3-char) | Kaggle | Home country |
| championships_won | INT | Calculated | Count of constructor titles |
| race_wins | INT | Calculated | Aggregate from constructor_race_results |
| podium_finishes | INT | Calculated | Aggregate podium count |
| debut_season | INT | Kaggle | Year of first entry |
| last_season_active | INT | OpenF1 | Latest year (NULL if active) |
| founding_year | INT | Kaggle | Historical metadata |

**Cardinality:** ~250 constructors  
**Grain:** One row per constructor, all-time aggregates  
**Updates:** Append + overwrite aggregates post-season

---

### 2.3 CIRCUITS (Tracks)
**Description:** F1 race circuit/venue records

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| circuit_id | STRING (PK) | Kaggle/OpenF1 | Unique identifier (e.g., "silverstone", "monaco") |
| name | STRING | Kaggle | Circuit name |
| country | STRING (3-char) | Kaggle | ISO country code |
| location_city | STRING | Kaggle | City/region |
| lap_distance_km | FLOAT | Kaggle | Lap length |
| total_laps_gp | INT | Kaggle | Standard race laps (varies by circuit) |
| first_race_year | INT | Kaggle | Debut year |
| races_held | INT | Calculated | Count of races at this circuit |
| wins_by_driver | MAP[STRING, INT] | Calculated | {driver_id: win_count} |
| fastest_laps_by_driver | MAP[STRING, INT] | Calculated | {driver_id: fl_count} |
| pole_count_by_driver | MAP[STRING, INT] | Calculated | {driver_id: pole_count} |

**Cardinality:** ~80 circuits  
**Grain:** One row per circuit  
**Updates:** Rarely updated; mainly aggregate recalc

---

### 2.4 RACES
**Description:** Individual race/event records

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| race_id | STRING (PK) | Kaggle/OpenF1 | Unique identifier (e.g., "2024_bahrain") |
| year | INT | Kaggle | Season year |
| round | INT | Kaggle | 1–24 (varies by year) |
| grand_prix_name | STRING | Kaggle/OpenF1 | Official name (e.g., "Bahrain Grand Prix") |
| circuit_id | STRING (FK) | Kaggle | Foreign key to circuits |
| date | DATE | Kaggle | Race date |
| winner_driver_id | STRING (FK) | OpenF1 | 1st place finisher |
| pole_position_driver_id | STRING (FK) | OpenF1 | Qualifying leader |
| fastest_lap_driver_id | STRING (FK) | OpenF1 | Fastest lap holder |
| winner_constructor_id | STRING (FK) | OpenF1 | Winning team |
| laps_completed | INT | OpenF1 | Race distance (may be red-flagged) |
| weather | STRING | OpenF1 | Conditions (e.g., "dry", "wet", "mixed") |
| status | STRING | OpenF1 | (e.g., "finished", "crashed", "dnf") |

**Cardinality:** ~1,100 races (1950–2024)  
**Grain:** One row per race  
**Updates:** Append post-event; occasionally overwrite (stewarding updates)

---

### 2.5 RACE_RESULTS
**Description:** Detailed finishing positions and statistics for each race

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| result_id | STRING (PK) | OpenF1/Kaggle | race_id + driver_id |
| race_id | STRING (FK) | OpenF1 | Foreign key to races |
| driver_id | STRING (FK) | OpenF1 | Foreign key to drivers |
| constructor_id | STRING (FK) | OpenF1 | Team for this race |
| finish_position | INT | OpenF1 | 1–20 (NULL if DNF) |
| points_awarded | INT | OpenF1 | Championship points |
| grid_position | INT | OpenF1 | Starting position |
| laps_completed | INT | OpenF1 | Laps finished |
| time_gap_to_leader | STRING | OpenF1 | Delta or DNF reason |
| status | STRING | OpenF1 | (e.g., "finished", "dnf", "disqualified") |
| fastest_lap_flag | BOOLEAN | OpenF1 | Did this driver set fastest lap? |
| pit_stops | INT | OpenF1 | Number of pit stop visits |

**Cardinality:** ~22,000 rows (avg 20 drivers × 1,100 races)  
**Grain:** One row per driver per race  
**Updates:** Append (new races); overwrite (stewards' decisions)

---

### 2.6 DRIVER_SEASON_STATS
**Description:** Aggregated driver performance per season (for season analysis section)

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| driver_season_id | STRING (PK) | Calculated | driver_id + "_" + year |
| year | INT | Kaggle | Season year |
| driver_id | STRING (FK) | Kaggle | Driver identifier |
| constructor_id | STRING (FK) | Kaggle | Team (drivers can switch mid-season; use majority) |
| championship_position | INT | OpenF1 | Final standing (1–25) |
| championship_points | INT | OpenF1 | Total season points |
| races_entered | INT | Calculated | Count of race entries |
| races_completed | INT | Calculated | Count of finishes (not DNF) |
| wins | INT | Calculated | Race victories |
| poles | INT | Calculated | Pole positions |
| fastest_laps | INT | Calculated | Fastest lap awards |
| podiums | INT | Calculated | P1+P2+P3 finishes |
| point_finishes | INT | Calculated | Races in top-10 scoring |

**Cardinality:** ~2,000 rows (avg 20 drivers × 100 seasons)  
**Grain:** One row per driver per season  
**Updates:** Append (new season); overwrite (final standings post-Abu Dhabi)

---

### 2.7 CONSTRUCTOR_SEASON_STATS
**Description:** Aggregated team performance per season

| Attribute | Type | Source | Notes |
|-----------|------|--------|-------|
| constructor_season_id | STRING (PK) | Calculated | constructor_id + "_" + year |
| year | INT | Kaggle | Season year |
| constructor_id | STRING (FK) | Kaggle | Constructor identifier |
| championship_position | INT | OpenF1 | Final standing (1–10) |
| championship_points | INT | OpenF1 | Total season constructor points |
| races_entered | INT | Calculated | Count of car entries |
| wins | INT | Calculated | Race victories |
| podiums | INT | Calculated | Podium finishes (all drivers combined) |
| poles | INT | Calculated | Pole positions (both driver lineups) |
| fastest_laps | INT | Calculated | Fastest lap awards |
| drivers_list | ARRAY[STRING] | Calculated | driver_ids who drove for team that year |

**Cardinality:** ~250 rows (avg 10 teams × 25 seasons)  
**Grain:** One row per constructor per season  
**Updates:** Append (new season); overwrite post-season

---

## 3. Calculated Fields & Metrics

### 3.1 Driver-Level Calculations

| Metric | Formula | Layer | Notes |
|--------|---------|-------|-------|
| `race_wins` | COUNT(DISTINCT race_id) WHERE finish_position = 1 | Silver | From race_results |
| `podium_finishes` | COUNT(DISTINCT race_id) WHERE finish_position ≤ 3 | Silver | All P1/P2/P3 |
| `pole_positions` | COUNT(DISTINCT race_id) WHERE grid_position = 1 | Silver | Qualifying leader |
| `fastest_laps` | SUM(fastest_lap_flag) | Silver | From race_results |
| `race_starts` | COUNT(DISTINCT race_id) | Silver | Every entry (DNF counts) |
| `win_rate_percent` | (race_wins / race_starts) × 100 | Gold | Final dimension |
| `championships_won` | COUNT(DISTINCT year) WHERE championship_position = 1 | Gold | World titles |
| `average_finish_position` | AVG(finish_position) WHERE finish_position IS NOT NULL | Silver | Excludes DNFs |
| `dnf_count` | COUNT(*) WHERE status = "dnf" | Silver | Retirement rate metric |
| `current_team` | race_results.constructor_id WHERE year = MAX(year) | Silver | Latest affiliation |

### 3.2 Constructor-Level Calculations

| Metric | Formula | Layer | Notes |
|--------|---------|-------|-------|
| `race_wins` | COUNT(DISTINCT race_id) FROM race_results WHERE finish_position = 1 | Silver | Team victories |
| `podium_finishes` | COUNT(*) FROM race_results WHERE finish_position ≤ 3 | Silver | All driver podiums |
| `pole_positions` | COUNT(DISTINCT race_id) WHERE grid_position = 1 | Silver | Both drivers' poles |
| `fastest_laps` | SUM(fastest_lap_flag) FROM race_results | Silver | Both drivers combined |
| `championships_won` | COUNT(DISTINCT year) WHERE championship_position = 1 | Gold | Constructor titles |

### 3.3 Circuit-Level Calculations

| Metric | Formula | Layer | Notes |
|--------|---------|-------|-------|
| `races_held` | COUNT(DISTINCT race_id) | Silver | Total races at circuit |
| `wins_by_driver` | {driver_id: COUNT(*)} WHERE finish_position = 1 | Silver | Top 5 drivers |
| `fastest_laps_by_driver` | {driver_id: SUM(fastest_lap_flag)} | Silver | Top 3 drivers |
| `pole_count_by_driver` | {driver_id: COUNT(*)} WHERE grid_position = 1 | Silver | Top 3 drivers |
| `win_distribution` | SUM(CASE finish_position WHEN 1 THEN 1 ELSE 0) by year | Silver | Wins per year at circuit |

---

## 4. Dashboard Sections & Data Mapping

### 4.1 Overview Section

**Charts:**
1. **Historical Wins Timeline** → `races.year`, `COUNT(races)` WHERE winner_driver_id IS NOT NULL
2. **Championships Timeline** → `drivers.championships_won` aggregated by decade
3. **Win Distribution (Pie)** → Top 20 drivers by `race_wins`
4. **Races Per Decade** → `races.year` binned into decades

**Required Tables:** `drivers`, `races`, `race_results`

---

### 4.2 Driver Section

**Charts:**
1. **Driver Stats Table** → All columns from DRIVERS
2. **Performance by Decade** → `driver_season_stats` grouped by FLOOR(year / 10) × 10
3. **Pole Position Trends** → `driver_season_stats.poles` by year (line chart)
4. **Podium Trends** → `driver_season_stats.podiums` by year (area chart)
5. **Wins Comparison** → Top 10 drivers: name vs. `race_wins`

**Required Tables:** `drivers`, `driver_season_stats`, `race_results`

---

### 4.3 Constructor Section

**Charts:**
1. **Constructor Championships** → `constructors.championships_won` sorted DESC
2. **Win Distribution by Team** → Top 15 constructors by `race_wins`
3. **Team Wins Over Time** → `constructor_season_stats.wins` by year (stacked area)
4. **Points Progression** → `constructor_season_stats.championship_points` by year

**Required Tables:** `constructors`, `constructor_season_stats`, `race_results`

---

### 4.4 Circuit Section

**Charts:**
1. **Circuit Stats Table** → All CIRCUITS columns + calculated fields
2. **Wins by Circuit (Horizontal Bar)** → `circuits.races_held` sorted DESC
3. **Fastest Laps Leader** → Top driver by `fastest_laps_by_driver` per circuit
4. **Most Dominant Drivers by Track** → Top 3 drivers per circuit (modal finish position)

**Required Tables:** `circuits`, `races`, `race_results`

---

### 4.5 Records Section

**Charts:**
1. **All-Time Leaders** → Drivers ranked by `championships_won`, `race_wins`, `pole_positions`, `podiums`, `fastest_laps`
2. **Team Leaders** → Constructors ranked by `championships_won`, `race_wins`
3. **Circuit Records** → Circuits with most races held + most wins by single driver

**Required Tables:** `drivers`, `constructors`, `circuits`, `race_results`

---

### 4.6 Season Analysis Section (Interactive Selector)

**User Picks Year:** Filters all below to single season

**Charts:**
1. **Driver Standings** → All drivers from `driver_season_stats` WHERE year = selected_year, sorted by championship_points DESC
2. **Constructor Standings** → All constructors from `constructor_season_stats` WHERE year = selected_year, sorted by championship_points DESC
3. **Race Results Table** → All races from `races` WHERE year = selected_year, columns: round, grand_prix_name, winner_driver_id, winner_constructor_id, pole_position_driver_id, fastest_lap_driver_id

**Required Tables:** `driver_season_stats`, `constructor_season_stats`, `races`, `race_results`

---

### 4.7 Compare Section (Driver Head-to-Head)

**User Picks Two Drivers:** Compares career + season data side-by-side

**Displays:**
1. **Career Stats Comparison** → Columns: championships_won, race_wins, pole_positions, podiums, fastest_laps, race_starts, win_rate_percent
2. **Radar Chart** → Normalized axes: Titles, Wins, Poles, Podiums, Experience (race_starts), Fastest Laps
3. **Season Wins Line Chart** → `driver_season_stats.wins` by year (both drivers overlaid)
4. **Stats Bar Chart** → Wins, Titles, Poles, Podiums, Fast Laps (grouped by metric)

**Required Tables:** `drivers`, `driver_season_stats`, `race_results`

---

## 5. Data Flow: Layer Mapping

### Bronze → Silver (Staging)

**Ingestion Points:**
- **Kaggle Dataset** → Raw CSV files (drivers, constructors, circuits, races, results)
- **Jolpica API** → Latest season standings, real-time results
- **OpenF1 API** → Detailed race data, telemetry, qualifying, weather

**Transformations at Staging (Silver):**
1. Deduplicate records by source (priority: OpenF1 > Jolpica > Kaggle for recency)
2. Standardize driver_id, constructor_id (merge aliases: "lewis_hamilton" = "hamilton")
3. Validate referential integrity (race_id → circuit_id, driver_id exists, etc.)
4. Cast types (dates, integers, nulls)
5. Enrich nationality with ISO codes
6. Calculate race-level metrics: points awarded per grid position (varies by era)

**Output: Silver Tables**
- `stg_drivers` (clean + deduplicated)
- `stg_constructors` (clean + deduplicated)
- `stg_circuits` (clean + deduplicated)
- `stg_races` (clean + deduplicated)
- `stg_race_results` (clean + deduplicated)

---

### Silver → Gold (Warehouse)

**Aggregation & Dimensional Tables:**
1. Build `dim_drivers` ← aggregate `stg_race_results` by driver_id
2. Build `dim_constructors` ← aggregate `stg_race_results` by constructor_id
3. Build `dim_circuits` ← aggregate `stg_races` by circuit_id
4. Build `fct_driver_season_stats` ← aggregate `stg_race_results` by (year, driver_id)
5. Build `fct_constructor_season_stats` ← aggregate `stg_race_results` by (year, constructor_id)
6. Build `fct_races` ← copy from `stg_races` (fact table)
7. Build `fct_race_results` ← copy from `stg_race_results` (fact table)

**Denormalized Aggregations for Dashboard Speed:**
- `agg_driver_decade_stats` ← `fct_driver_season_stats` grouped by decade
- `agg_circuit_wins_by_driver` ← ranked wins per circuit per driver
- `agg_constructor_timeline` ← rolling aggregates by season

---

## 6. Data Quality Checkpoints

### At Each Layer

| Layer | Checkpoint | Rule | Owner |
|-------|-----------|------|-------|
| Bronze | Row counts | races: 1,100–1,150; drivers: 900–1,050 | Ingestion script |
| Bronze | Referential integrity | race_id → circuit_id all exist | Validation SQL |
| Silver | Null rates | race_results.finish_position: <5% NULL (DNFs expected) | dbt test |
| Silver | Duplicate keys | No duplicate (race_id, driver_id) pairs | dbt unique test |
| Gold | Aggregation counts | SUM(driver_wins) = COUNT(*) races WHERE finish_position = 1 | Reconciliation query |
| Gold | Staleness | Latest race in data ≤ 7 days old | Airflow alert |

---

## 7. Refresh Schedule

| Table | Frequency | Trigger | SLA |
|-------|-----------|---------|-----|
| `stg_races`, `stg_race_results` | Daily | New race (schedule-based post-race window) | +2 hours post-race |
| `stg_drivers`, `stg_constructors` | Weekly | Roster changes, career milestones | Sunday 00:00 UTC |
| `dim_*`, `fct_*` (Silver aggregates) | Daily | After Bronze updates | +3 hours post-race |
| `agg_*` (Gold denormalized) | Daily | After fact tables | +4 hours post-race |
| Dashboard cache | On-demand | User refresh or daily 06:00 UTC | Real-time |

---

## 8. Data Dictionary Reference

### Grain Examples

| Table | Grain | Example Row |
|-------|-------|------------|
| `dim_drivers` | One per all-time driver | Hamilton: 334 wins, 103 poles, 7 titles |
| `fct_driver_season_stats` | One per driver per season | Hamilton 2020: 11 wins, 7 poles, 413 pts |
| `fct_races` | One per race event | 2024 Bahrain: Verstappen winner, Monaco pole |
| `fct_race_results` | One per driver per race | Hamilton 2024 Bahrain: P2, 18 pts, grid P1 |
| `dim_circuits` | One per circuit | Silverstone: 58 races, Senna 5 wins |

---

## 9. Staging Layer Deliverables (Your Current Focus)

**You need to ensure Bronze → Silver produces:**

1. ✅ **stg_drivers** — Deduplicated, validated driver master
2. ✅ **stg_constructors** — Deduplicated, validated team master
3. ✅ **stg_circuits** — Deduplicated, validated venue master
4. ✅ **stg_races** — Deduplicated, enriched race calendar
5. ✅ **stg_race_results** — Deduplicated, harmonized result records

**Each must have:**
- Surrogate keys (if missing in source)
- Source tracking (`_source`, `_loaded_at`)
- Data quality flags (`_validation_status`)
- Referential integrity (all FKs resolvable)
- Type consistency (dates, numbers, codes)

---

## 10. Questions for Your Agents/Team

Use this checklist to validate your staging layer:

- [ ] Do we have all driver IDs consistent across Kaggle + OpenF1 + Jolpica?
- [ ] Are race_ids stable and deduplicated (one row per race)?
- [ ] Do all race_results have valid foreign keys (race_id, driver_id, constructor_id)?
- [ ] Are NULL values intentional (e.g., DNF, no fastest lap yet)?
- [ ] Is championship_position calculated correctly for each season?
- [ ] Do we handle mid-season driver transfers (same driver, different team)?
- [ ] Are historical championship points rules applied (pre-1975 vs modern era)?
- [ ] Can we join race → circuit → driver cleanly across all three sources?

---

## 11. Example Query: Verify Staging Completeness

```sql
-- Validate stg_races → stg_race_results cardinality
SELECT 
  r.year,
  COUNT(DISTINCT r.race_id) as race_count,
  COUNT(DISTINCT CONCAT(rr.race_id, '_', rr.driver_id)) as result_rows,
  APPROX_QUANTILES(result_rows / race_count, 2)[OFFSET(1)] as avg_drivers_per_race
FROM stg_races r
LEFT JOIN stg_race_results rr ON r.race_id = rr.race_id
GROUP BY r.year
ORDER BY r.year DESC;
-- Expected: 20 drivers/race on average (varies 18–24)
```

---

**Version:** 1.0  
**Created:** 2026  
**For Questions:** Refer to dashboard UI logic in mockdashboard.html JS sections
