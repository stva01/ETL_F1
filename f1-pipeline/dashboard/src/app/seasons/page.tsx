import { querySnowflake } from "@/lib/snowflake";
import SeasonSelector from "./SeasonSelector";

export const revalidate = 0;

export default async function SeasonsPage({ searchParams }: { searchParams: Promise<{ year?: string }> }) {
  const params = await searchParams;
  
  // Get all available seasons
  const seasonsData = await querySnowflake<{ season_year: number }>(`
    SELECT DISTINCT CAST(season_year AS INTEGER) as season_year 
    FROM MARTS.dim_race 
    ORDER BY season_year DESC
  `);
  const seasons = seasonsData.map(s => s.season_year);
  
  const currentYear = params.year ? parseInt(params.year) : (seasons[0] || 2024);

  // Fetch Season Stats, Driver Standings, and Race Results concurrently
  const [seasonStats, driverStandings, raceResults] = await Promise.all([
    querySnowflake<any>(`
      SELECT * FROM MARTS.mart_season_summary 
      WHERE season_year = ${currentYear} LIMIT 1
    `),
    querySnowflake<any>(`
      SELECT * FROM MARTS.mart_driver_season 
      WHERE season_year = ${currentYear} 
      ORDER BY season_points DESC 
      LIMIT 20
    `),
    querySnowflake<any>(`
      SELECT * FROM MARTS.mart_race_summary 
      WHERE season_year = ${currentYear} 
      ORDER BY race_date ASC
    `)
  ]);

  const stats = seasonStats[0] || {};

  return (
    <main className="section active" id="seasons">
        <div className="sec-head">
            <h2>Season <span>Archive</span></h2>
            <div className="sec-head-line"></div>
        </div>

        <SeasonSelector seasons={seasons} currentYear={currentYear} />

        <div className="grid-4" style={{ marginTop: '20px', marginBottom: '20px' }}>
            <div className="stat-card">
                <div className="num"><span>{stats.total_races || raceResults.length || 0}</span></div>
                <div className="label">Races in {currentYear}</div>
                <div className="sub">Season length</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--gold)' }}>
                <div className="num"><span style={{ color: 'var(--gold)', fontSize: '24px' }}>{stats.driver_champion || 'TBD'}</span></div>
                <div className="label">Driver Champion</div>
                <div className="sub">{currentYear}</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--green)' }}>
                <div className="num"><span style={{ color: 'var(--green)', fontSize: '24px' }}>{stats.constructor_champion || 'TBD'}</span></div>
                <div className="label">Constructor Champion</div>
                <div className="sub">{currentYear}</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--blue)' }}>
                <div className="num"><span style={{ color: 'var(--blue)' }}>{driverStandings.length}</span></div>
                <div className="label">Drivers Competed</div>
                <div className="sub">in {currentYear}</div>
            </div>
        </div>

        <div className="grid-2" style={{ marginTop: '20px' }}>
            <div className="card">
                <div className="card-title">{currentYear} Driver Standings (Top 10)</div>
                <div className="leaderboard">
                  <table className="f1-table">
                    <thead>
                      <tr>
                        <th>Pos</th>
                        <th>Driver</th>
                        <th>Points</th>
                        <th>Wins</th>
                      </tr>
                    </thead>
                    <tbody>
                      {driverStandings.slice(0, 10).map((d, i) => (
                        <tr key={i}>
                          <td>{i + 1}</td>
                          <td><strong>{d.driver_name || d.full_name || 'Unknown'}</strong></td>
                          <td className="mono">{d.season_points}</td>
                          <td className="mono">{d.season_wins || d.wins || 0}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
            </div>

            <div className="card">
                <div className="card-title">{currentYear} Race Calendar & Winners</div>
                <div className="leaderboard">
                  <table className="f1-table">
                    <thead>
                      <tr>
                        <th>Rnd</th>
                        <th>Grand Prix</th>
                        <th>Winner</th>
                        <th>Team</th>
                        <th>Pole Position</th>
                        <th>Fastest Lap</th>
                      </tr>
                    </thead>
                    <tbody>
                      {raceResults.map((r, i) => (
                        <tr key={i}>
                          <td>{i + 1}</td>
                          <td>{r.race_name}</td>
                          <td><strong>{r.winning_driver || r.winner_name || '-'}</strong></td>
                          <td className="mono" style={{ fontSize: '13px' }}>{r.winning_constructor || '-'}</td>
                          <td className="mono" style={{ fontSize: '13px' }}>{r.pole_sitter_name || '-'}</td>
                          <td className="mono" style={{ fontSize: '13px' }}>{r.fastest_lap_driver || '-'}</td>
                        </tr>
                      ))}
                      {raceResults.length === 0 && (
                        <tr>
                           <td colSpan={6} style={{ textAlign: 'center', padding: '20px' }}>No race data available for this season.</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
            </div>
        </div>
    </main>
  );
}
