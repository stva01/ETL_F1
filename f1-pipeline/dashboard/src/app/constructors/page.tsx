import { querySnowflake } from "@/lib/snowflake";
import ConstructorsCharts from "./ConstructorsCharts";

export const revalidate = 0;

export default async function ConstructorsPage() {
  const [{ ferrari_titles }] = await querySnowflake<{ ferrari_titles: number }>(`
    SELECT CAST(constructor_titles AS INTEGER) as ferrari_titles 
    FROM MARTS.mart_constructor_career 
    WHERE constructor_name LIKE '%Ferrari%' LIMIT 1
  `);

  const [{ ferrari_wins }] = await querySnowflake<{ ferrari_wins: number }>(`
    SELECT CAST(total_wins AS INTEGER) as ferrari_wins 
    FROM MARTS.mart_constructor_career 
    WHERE constructor_name LIKE '%Ferrari%' LIMIT 1
  `);

  const [{ mercedes_titles }] = await querySnowflake<{ mercedes_titles: number }>(`
    SELECT CAST(constructor_titles AS INTEGER) as mercedes_titles 
    FROM MARTS.mart_constructor_career 
    WHERE constructor_name LIKE '%Mercedes%' LIMIT 1
  `);

  const [{ redbull_titles }] = await querySnowflake<{ redbull_titles: number }>(`
    SELECT CAST(constructor_titles AS INTEGER) as redbull_titles 
    FROM MARTS.mart_constructor_career 
    WHERE constructor_name LIKE '%Red Bull%' LIMIT 1
  `);

  const winsData = await querySnowflake<{ label: string, value: number }>(`
    SELECT constructor_name as label, CAST(total_wins AS INTEGER) as value 
    FROM MARTS.mart_constructor_career 
    ORDER BY total_wins DESC NULLS LAST LIMIT 10
  `);

  const titlesData = await querySnowflake<{ label: string, value: number }>(`
    SELECT constructor_name as label, CAST(constructor_titles AS INTEGER) as value 
    FROM MARTS.mart_constructor_career 
    WHERE constructor_titles > 0
    ORDER BY constructor_titles DESC NULLS LAST LIMIT 10
  `);

  const winShareData = await querySnowflake<{ label: string, value: number }>(`
    WITH top_wins AS (
      SELECT constructor_name as label, CAST(total_wins AS INTEGER) as value 
      FROM MARTS.mart_constructor_career 
      ORDER BY total_wins DESC NULLS LAST LIMIT 5
    )
    SELECT label, value FROM top_wins
    UNION ALL
    SELECT 'Others' as label, CAST(SUM(total_wins) AS INTEGER) as value 
    FROM MARTS.mart_constructor_career 
    WHERE constructor_name NOT IN (SELECT label FROM top_wins)
  `);

  const top4Names = ["'Ferrari'", "'McLaren'", "'Mercedes'", "'Red Bull'"].join(',');
  const trendData = await querySnowflake<{ constructor_name: string, season_year: number, season_wins: number, season_points: number }>(`
    SELECT constructor_name, CAST(season_year AS INTEGER) as season_year, 
           CAST(season_wins AS INTEGER) as season_wins, 
           CAST(season_points AS INTEGER) as season_points
    FROM MARTS.mart_constructor_season
    WHERE constructor_name IN (${top4Names})
    ORDER BY season_year ASC
  `);

  return (
    <main className="section active" id="constructors">
        <div className="sec-head">
            <h2><span>Constructor</span> Championship History</h2>
            <div className="sec-head-line"></div>
            <span className="badge badge-dim">Since 1958</span>
        </div>

        <div className="grid-4" style={{ marginBottom: '20px' }}>
            <div className="stat-card">
                <div className="num"><span>{ferrari_titles}</span></div>
                <div className="label">Ferrari Titles</div>
                <div className="sub">Most Constructor Championships</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--gold)' }}>
                <div className="num"><span style={{ color: 'var(--gold)' }}>{ferrari_wins}</span></div>
                <div className="label">Ferrari Race Wins</div>
                <div className="sub">All-time leading constructor</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--green)' }}>
                <div className="num"><span style={{ color: 'var(--green)' }}>{mercedes_titles}</span></div>
                <div className="label">Mercedes Titles</div>
                <div className="sub">2014–2021 Dominance</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--blue)' }}>
                <div className="num"><span style={{ color: 'var(--blue)' }}>{redbull_titles}</span></div>
                <div className="label">Red Bull Titles</div>
                <div className="sub">inc. 2022–2024 streak</div>
            </div>
        </div>

        <ConstructorsCharts 
          winsData={winsData} 
          titlesData={titlesData} 
          winShareData={winShareData} 
          trendData={trendData}
          top4Teams={['Ferrari', 'McLaren', 'Mercedes', 'Red Bull']}
        />
    </main>
  );
}
