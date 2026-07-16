import { querySnowflake } from "@/lib/snowflake";
import DriversCharts from "./DriversCharts";
import { getFlagEmoji } from "@/lib/flags";

export const revalidate = 0;

export default async function DriversPage() {
  const [
    winsData,
    polesData,
    pointsData,
    podiumsData,
    winRateData,
    leaderboardData
  ] = await Promise.all([
    querySnowflake<{ label: string, value: number }>(`
      SELECT full_name as label, CAST(total_wins AS INTEGER) as value 
      FROM MARTS.mart_driver_career 
      ORDER BY total_wins DESC NULLS LAST LIMIT 15
    `),
    querySnowflake<{ label: string, value: number }>(`
      SELECT full_name as label, CAST(total_poles AS INTEGER) as value 
      FROM MARTS.mart_driver_career 
      ORDER BY total_poles DESC NULLS LAST LIMIT 15
    `),
    querySnowflake<{ label: string, value: number }>(`
      SELECT full_name as label, CAST(total_points AS INTEGER) as value 
      FROM MARTS.mart_driver_career 
      ORDER BY total_points DESC NULLS LAST LIMIT 15
    `),
    querySnowflake<{ label: string, value: number }>(`
      SELECT full_name as label, CAST(total_podiums AS INTEGER) as value 
      FROM MARTS.mart_driver_career 
      ORDER BY total_podiums DESC NULLS LAST LIMIT 15
    `),
    querySnowflake<{ full_name: string, nationality: string, win_rate_pct: number, total_wins: number, total_race_starts: number }>(`
      SELECT full_name, CAST(nationality AS VARCHAR) as nationality, win_rate_pct, 
             CAST(total_wins AS INTEGER) as total_wins, CAST(total_race_starts AS INTEGER) as total_race_starts
      FROM MARTS.mart_driver_career 
      WHERE total_race_starts >= 50 AND win_rate_pct IS NOT NULL
      ORDER BY win_rate_pct DESC NULLS LAST LIMIT 10
    `),
    querySnowflake<{ full_name: string, nationality: string, total_wins: number, total_podiums: number, championship_titles: number }>(`
      SELECT full_name, CAST(nationality AS VARCHAR) as nationality,
             CAST(total_wins AS INTEGER) as total_wins, 
             CAST(total_podiums AS INTEGER) as total_podiums, 
             CAST(championship_titles AS INTEGER) as championship_titles 
      FROM MARTS.mart_driver_career 
      ORDER BY total_wins DESC NULLS LAST LIMIT 10
    `)
  ]);

  const top3Names = leaderboardData.slice(0, 3).map(d => `'${d.full_name}'`).join(',');
  const trendData = await querySnowflake<{ full_name: string, season_year: number, season_poles: number, season_podiums: number }>(`
    SELECT full_name, CAST(season_year AS INTEGER) as season_year, 
           CAST(season_poles AS INTEGER) as season_poles, 
           CAST(season_podiums AS INTEGER) as season_podiums
    FROM MARTS.mart_driver_season
    WHERE full_name IN (${top3Names})
    ORDER BY season_year ASC
  `);

  const d1 = leaderboardData[0];
  const d2 = leaderboardData[1];
  const d3 = leaderboardData[2];

  return (
    <main className="section active" id="drivers">
        <div className="sec-head">
            <h2>All-Time <span>Driver</span> Records</h2>
            <div className="sec-head-line"></div>
            <span className="badge badge-red">1950 — 2024</span>
        </div>

        <DriversCharts 
          winsData={winsData} 
          polesData={polesData} 
          pointsData={pointsData} 
          podiumsData={podiumsData} 
          trendData={trendData}
          top3Drivers={leaderboardData.slice(0, 3).map(d => d.full_name)}
        />

        <div className="grid-2" style={{ marginTop: '20px' }}>
            <div className="card">
                <div className="card-title">All-Time Leaderboard — Race Wins</div>
                
                <div className="podium">
                  <div className="podium-block">
                    <div className="podium-name">{d2.full_name}</div>
                    <div className="podium-bar">2</div>
                    <div className="podium-name mono" style={{fontSize: '11px'}}>{d2.total_wins} Wins</div>
                  </div>
                  <div className="podium-block">
                    <div className="podium-name">{d1.full_name}</div>
                    <div className="podium-bar">1</div>
                    <div className="podium-name mono" style={{fontSize: '11px', color:'var(--gold)'}}>{d1.total_wins} Wins</div>
                  </div>
                  <div className="podium-block">
                    <div className="podium-name">{d3.full_name}</div>
                    <div className="podium-bar">3</div>
                    <div className="podium-name mono" style={{fontSize: '11px'}}>{d3.total_wins} Wins</div>
                  </div>
                </div>

                <div className="leaderboard">
                  {leaderboardData.map((d, i) => {
                    let posClass = "";
                    if (i === 0) posClass = "gold";
                    else if (i === 1) posClass = "silver";
                    else if (i === 2) posClass = "bronze";

                    return (
                      <div className="lb-row" key={i}>
                        <div className={`lb-pos ${posClass}`}>P{i + 1}</div>
                        <div className="flag">{getFlagEmoji(d.nationality)}</div>
                        <div className="lb-name">{d.full_name}</div>
                        <div className="lb-extra mono">{d.total_wins} Wins</div>
                        <div className="lb-extra mono">{d.championship_titles} Titles</div>
                      </div>
                    );
                  })}
                </div>
            </div>

            <div className="card">
                <div className="card-title">Highest Win Rate (Min. 50 Starts)</div>
                <div className="leaderboard">
                    {winRateData.map((d, i) => (
                      <div className="lb-row" key={i}>
                          <div className="lb-pos" style={{color: i < 3 ? 'var(--text)' : 'var(--text-faint)'}}>{i + 1}</div>
                          <div className="flag">{getFlagEmoji(d.nationality)}</div>
                          <div className="lb-name" style={{ flex: '0 0 140px' }}>{d.full_name}</div>
                          <div className="lb-bar-wrap">
                              <div className="lb-bar">
                                  <div className="lb-bar-fill" style={{ width: `${d.win_rate_pct}%` }}></div>
                              </div>
                              <div className="lb-val">{d.win_rate_pct.toFixed(1)}%</div>
                          </div>
                      </div>
                    ))}
                </div>
            </div>
        </div>
    </main>
  );
}
