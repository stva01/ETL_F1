import { querySnowflake } from "@/lib/snowflake";
import OverviewCharts from "./OverviewCharts";

export const revalidate = 0; // Dynamic rendering for live data

export default async function Home() {
  // Query 1: Total Races
  const [{ total_races }] = await querySnowflake<{ total_races: number }>(`
    SELECT CAST(COUNT(*) AS INTEGER) as total_races FROM MARTS.dim_race
  `);

  // Query 2: Champions
  const [{ total_champions }] = await querySnowflake<{ total_champions: number }>(`
    SELECT CAST(COUNT(*) AS INTEGER) as total_champions FROM MARTS.mart_driver_career WHERE championship_titles > 0
  `);

  // Query 3: Total Drivers
  const [{ total_drivers }] = await querySnowflake<{ total_drivers: number }>(`
    SELECT CAST(COUNT(*) AS INTEGER) as total_drivers FROM MARTS.dim_driver
  `);

  // Query 4: Total Constructors
  const [{ total_constructors }] = await querySnowflake<{ total_constructors: number }>(`
    SELECT CAST(COUNT(*) AS INTEGER) as total_constructors FROM MARTS.dim_constructor
  `);

  // Query 5: Circuits Used
  const [{ total_circuits }] = await querySnowflake<{ total_circuits: number }>(`
    SELECT CAST(COUNT(*) AS INTEGER) as total_circuits FROM MARTS.dim_circuit
  `);

  // Stat Cards
  const [{ max_wins, driver_wins }] = await querySnowflake<{ max_wins: number, driver_wins: string }>(`
    SELECT CAST(MAX(total_wins) AS INTEGER) as max_wins, (SELECT full_name FROM MARTS.mart_driver_career ORDER BY total_wins DESC NULLS LAST LIMIT 1) as driver_wins FROM MARTS.mart_driver_career
  `);

  const [{ max_champs, driver_champs }] = await querySnowflake<{ max_champs: number, driver_champs: string }>(`
    SELECT CAST(MAX(championship_titles) AS INTEGER) as max_champs, (SELECT full_name FROM MARTS.mart_driver_career ORDER BY championship_titles DESC NULLS LAST LIMIT 1) as driver_champs FROM MARTS.mart_driver_career
  `);

  const [{ max_poles, driver_poles }] = await querySnowflake<{ max_poles: number, driver_poles: string }>(`
    SELECT CAST(MAX(total_poles) AS INTEGER) as max_poles, (SELECT full_name FROM MARTS.mart_driver_career ORDER BY total_poles DESC NULLS LAST LIMIT 1) as driver_poles FROM MARTS.mart_driver_career
  `);

  const [{ max_cwins, constructor_wins }] = await querySnowflake<{ max_cwins: number, constructor_wins: string }>(`
    SELECT CAST(MAX(total_wins) AS INTEGER) as max_cwins, (SELECT constructor_name FROM MARTS.mart_constructor_career ORDER BY total_wins DESC NULLS LAST LIMIT 1) as constructor_wins FROM MARTS.mart_constructor_career
  `);

  // Charts Data
  const titlesData = await querySnowflake<{ label: string, value: number }>(`
    SELECT d.full_name as label, CAST(m.championship_titles AS INTEGER) as value 
    FROM MARTS.mart_driver_career m
    JOIN MARTS.dim_driver d ON m.driver_key = d.driver_key
    WHERE m.championship_titles > 0 
    ORDER BY m.championship_titles DESC LIMIT 12
  `);

  const nationalityData = await querySnowflake<{ label: string, value: number }>(`
    SELECT d.nationality as label, CAST(COUNT(*) AS INTEGER) as value 
    FROM MARTS.mart_driver_career m
    JOIN MARTS.dim_driver d ON m.driver_key = d.driver_key
    WHERE m.championship_titles > 0 
    GROUP BY d.nationality 
    ORDER BY value DESC LIMIT 8
  `);

  const racesPerDecadeData = await querySnowflake<{ label: string, value: number }>(`
    SELECT CAST(FLOOR(season_year / 10) * 10 AS VARCHAR) || 's' as label, CAST(COUNT(*) AS INTEGER) as value 
    FROM MARTS.dim_race 
    GROUP BY label 
    ORDER BY label
  `);

  return (
    <main className="section active" id="overview">
        <div className="hero">
            <h1>Formula <span>1</span> World Championship<br/>All-Time Analysis</h1>
            <p>75 years of racing. {total_champions} World Champions. {total_races.toLocaleString()}+ Grands Prix. Explore every record, driver, constructor, circuit, and era from 1950 to 2024.</p>
            
            <div className="hero-stats">
                <div className="hero-stat">
                    <div className="val">{total_races.toLocaleString()}</div>
                    <div className="lbl">Grands Prix</div>
                </div>
                <div className="divider"></div>
                <div className="hero-stat">
                    <div className="val">{total_champions}</div>
                    <div className="lbl">Champions</div>
                </div>
                <div className="divider"></div>
                <div className="hero-stat">
                    <div className="val">{total_drivers.toLocaleString()}</div>
                    <div className="lbl">Drivers</div>
                </div>
                <div className="divider"></div>
                <div className="hero-stat">
                    <div className="val">{total_constructors.toLocaleString()}</div>
                    <div className="lbl">Constructors</div>
                </div>
                <div className="divider"></div>
                <div className="hero-stat">
                    <div className="val">{total_circuits}</div>
                    <div className="lbl">Circuits Used</div>
                </div>
                <div className="divider"></div>
                <div className="hero-stat">
                    <div className="val">{max_wins}</div>
                    <div className="lbl">Hamilton Wins</div>
                </div>
            </div>
        </div>

        <div className="grid-4" style={{ marginBottom: '20px' }}>
            <div className="stat-card">
                <div className="num"><span>{max_wins}</span></div>
                <div className="label">Most Wins Ever</div>
                <div className="sub">{driver_wins}</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--gold)' }}>
                <div className="num"><span style={{ color: 'var(--gold)' }}>{max_champs}</span></div>
                <div className="label">Most Championships</div>
                <div className="sub">{driver_champs}</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--green)' }}>
                <div className="num"><span style={{ color: 'var(--green)' }}>{max_poles}</span></div>
                <div className="label">Most Pole Positions</div>
                <div className="sub">{driver_poles}</div>
            </div>
            <div className="stat-card" style={{ borderLeftColor: 'var(--blue)' }}>
                <div className="num"><span style={{ color: 'var(--blue)' }}>{max_cwins}</span></div>
                <div className="label">{constructor_wins} Race Wins</div>
                <div className="sub">Most Wins — Any Constructor</div>
            </div>
        </div>

        <OverviewCharts 
          titlesData={titlesData} 
          nationalityData={nationalityData} 
          racesPerDecadeData={racesPerDecadeData} 
        />
    </main>
  );
}
