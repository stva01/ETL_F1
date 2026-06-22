import { queryDuckDB } from "@/lib/duckdb";
import CompareClient from "./CompareClient";

export const revalidate = 0;

export interface DriverStat {
  driver_key: number;
  full_name: string;
  nationality: string | null;
  debut_year: number;
  final_year: number;
  total_race_starts: number;
  total_wins: number;
  total_podiums: number;
  total_poles: number;
  total_points: number;
  championship_titles: number;
  win_rate_pct: number;
}

export default async function ComparePage() {
  // Fetch top 100 drivers by race starts to show in the dropdown
  const drivers = await queryDuckDB<DriverStat>(`
    SELECT 
      CAST(driver_key AS INTEGER) as driver_key,
      CAST(full_name AS VARCHAR) as full_name,
      CAST(nationality AS VARCHAR) as nationality,
      CAST(debut_year AS INTEGER) as debut_year,
      CAST(final_year AS INTEGER) as final_year,
      CAST(total_race_starts AS INTEGER) as total_race_starts,
      CAST(total_wins AS INTEGER) as total_wins,
      CAST(total_podiums AS INTEGER) as total_podiums,
      CAST(total_poles AS INTEGER) as total_poles,
      CAST(total_points AS DOUBLE) as total_points,
      CAST(championship_titles AS INTEGER) as championship_titles,
      CAST(win_rate_pct AS DOUBLE) as win_rate_pct
    FROM main_marts.mart_driver_career
    ORDER BY total_race_starts DESC
    LIMIT 100
  `);

  const driverKeys = drivers.map(d => d.driver_key).join(',');
  const trendData = await queryDuckDB<{ driver_key: number, season_year: number, season_wins: number }>(`
    SELECT CAST(driver_key AS INTEGER) as driver_key,
           CAST(season_year AS INTEGER) as season_year,
           CAST(season_wins AS INTEGER) as season_wins
    FROM main_marts.mart_driver_season
    WHERE driver_key IN (${driverKeys})
  `);

  return (
    <main className="section active" id="compare">
      <div className="sec-head">
          <h2>Driver <span>Comparison</span></h2>
          <div className="sec-head-line"></div>
      </div>
      <CompareClient drivers={drivers} trendData={trendData} />
    </main>
  );
}
