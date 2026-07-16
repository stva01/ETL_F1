import { querySnowflake } from "@/lib/snowflake";
import CircuitsCharts from "./CircuitsCharts";

export const revalidate = 0;

export default async function CircuitsPage() {
  const [
    hostedData,
    countryData,
    topCircuits,
    fastestLapsData
  ] = await Promise.all([
    querySnowflake<{ label: string, value: number }>(`
      SELECT CAST(circuit_name AS VARCHAR) as label, CAST(total_races_held AS INTEGER) as value 
      FROM MARTS.mart_circuit_stats 
      ORDER BY total_races_held DESC NULLS LAST LIMIT 15
    `),
    querySnowflake<{ label: string, value: number }>(`
      SELECT CAST(country AS VARCHAR) as label, CAST(COUNT(*) AS INTEGER) as value 
      FROM MARTS.mart_circuit_stats 
      GROUP BY country 
      ORDER BY value DESC NULLS LAST LIMIT 8
    `),
    querySnowflake<{ circuit_name: string, country: string, total_races_held: number, most_wins_driver: string }>(`
      SELECT CAST(circuit_name AS VARCHAR) as circuit_name, 
             CAST(country AS VARCHAR) as country, 
             CAST(total_races_held AS INTEGER) as total_races_held,
             CAST(most_wins_driver AS VARCHAR) as most_wins_driver
      FROM MARTS.mart_circuit_stats 
      ORDER BY total_races_held DESC NULLS LAST LIMIT 8
    `),
    querySnowflake<{ full_name: string, total_fastest_laps: number }>(`
      SELECT full_name, CAST(total_fastest_laps AS INTEGER) as total_fastest_laps
      FROM MARTS.mart_driver_career
      ORDER BY total_fastest_laps DESC NULLS LAST LIMIT 10
    `)
  ]);

  return (
    <main className="section active" id="circuits">
        <div className="sec-head">
            <h2>Iconic <span>Circuits</span> & Venues</h2>
            <div className="sec-head-line"></div>
            <span className="badge badge-blue">77 Unique Venues</span>
        </div>

        <CircuitsCharts hostedData={hostedData} countryData={countryData} />

        <div className="grid-2-1" style={{ marginTop: '20px' }}>
            <div className="card" style={{ padding: '0', background: 'transparent', border: 'none' }}>
                <div className="sec-head" style={{ marginBottom: '16px' }}>
                    <h2 style={{ fontSize: '24px' }}>Iconic <span>Venues</span></h2>
                    <div className="sec-head-line"></div>
                </div>
                <div className="circuit-grid">
                    {topCircuits.map((c, i) => (
                      <div className="circuit-card" key={i}>
                          <div className="c-name">{c.circuit_name}</div>
                          <div className="c-country">{c.country}</div>
                          <div className="c-big">{c.total_races_held}</div>
                          <div className="c-stat">Races Hosted</div>
                          <div className="c-stat" style={{ marginTop: '8px', color: 'var(--text-faint)' }}>King: {c.most_wins_driver || 'N/A'}</div>
                      </div>
                    ))}
                </div>
            </div>

            <div className="card">
                <div className="card-title">All-Time Fastest Laps Leaderboard</div>
                <div className="leaderboard">
                  <table className="f1-table">
                    <thead>
                      <tr>
                        <th>Pos</th>
                        <th>Driver</th>
                        <th>Fastest Laps</th>
                      </tr>
                    </thead>
                    <tbody>
                      {fastestLapsData.map((d, i) => (
                        <tr key={i}>
                          <td>{i + 1}</td>
                          <td><strong>{d.full_name}</strong></td>
                          <td className="mono">{d.total_fastest_laps}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
            </div>
        </div>
    </main>
  );
}
