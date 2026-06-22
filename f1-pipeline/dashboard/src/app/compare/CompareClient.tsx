"use client";

import { useState } from "react";
import Chart from "@/components/Chart";
import { DriverStat } from "./page";

interface TrendData {
  driver_key: number;
  season_year: number;
  season_wins: number;
}

export default function CompareClient({ drivers, trendData }: { drivers: DriverStat[], trendData: TrendData[] }) {
  // Sort drivers alphabetically for the dropdown
  const sortedDrivers = [...drivers].sort((a, b) => a.full_name.localeCompare(b.full_name));

  // Default selections: Hamilton and Schumacher
  const defaultD1 = sortedDrivers.find(d => d.full_name.includes("Hamilton")) || sortedDrivers[0];
  const defaultD2 = sortedDrivers.find(d => d.full_name.includes("Schumacher") && !d.full_name.includes("Ralf")) || sortedDrivers[1];

  const [d1Key, setD1Key] = useState<number>(defaultD1.driver_key);
  const [d2Key, setD2Key] = useState<number>(defaultD2.driver_key);

  const d1 = sortedDrivers.find(d => d.driver_key === d1Key)!;
  const d2 = sortedDrivers.find(d => d.driver_key === d2Key)!;

  const radarData = {
    labels: ['Wins', 'Podiums', 'Poles', 'Championships', 'Win Rate %'],
    datasets: [
      {
        label: d1.full_name,
        data: [
          (d1.total_wins / 103) * 100, // Normalized roughly based on Hamilton's 103
          (d1.total_podiums / 197) * 100,
          (d1.total_poles / 104) * 100,
          (d1.championship_titles / 7) * 100,
          d1.win_rate_pct,
        ],
        backgroundColor: 'rgba(225, 6, 0, 0.2)',
        borderColor: '#E10600',
        borderWidth: 2,
      },
      {
        label: d2.full_name,
        data: [
          (d2.total_wins / 103) * 100,
          (d2.total_podiums / 197) * 100,
          (d2.total_poles / 104) * 100,
          (d2.championship_titles / 7) * 100,
          d2.win_rate_pct,
        ],
        backgroundColor: 'rgba(30, 144, 255, 0.2)',
        borderColor: '#1E90FF',
        borderWidth: 2,
      }
    ]
  };

  const statRow = (label: string, v1: number | string, v2: number | string, isHigherBetter: boolean = true) => {
    const num1 = typeof v1 === 'number' ? v1 : parseFloat(v1.toString());
    const num2 = typeof v2 === 'number' ? v2 : parseFloat(v2.toString());
    
    let d1Wins = false;
    let d2Wins = false;
    if (num1 > num2) d1Wins = isHigherBetter;
    else if (num2 > num1) d2Wins = isHigherBetter;
    else { d1Wins = true; d2Wins = true; } // Draw

    return (
      <div style={{ display: 'flex', justifyContent: 'space-between', padding: '12px 0', borderBottom: '1px solid var(--border)' }}>
        <div style={{ width: '30%', textAlign: 'left', fontFamily: "'Barlow Condensed', sans-serif", fontSize: '20px', fontWeight: 700, color: d1Wins ? 'var(--red)' : 'var(--text-dim)' }}>
          {v1}
        </div>
        <div style={{ width: '40%', textAlign: 'center', fontSize: '12px', textTransform: 'uppercase', letterSpacing: '1px', color: 'var(--text-dim)' }}>
          {label}
        </div>
        <div style={{ width: '30%', textAlign: 'right', fontFamily: "'Barlow Condensed', sans-serif", fontSize: '20px', fontWeight: 700, color: d2Wins ? 'var(--blue)' : 'var(--text-dim)' }}>
          {v2}
        </div>
      </div>
    );
  };

  const d1Trends = trendData.filter(d => d.driver_key === d1Key);
  const d2Trends = trendData.filter(d => d.driver_key === d2Key);

  // For an overlaid line chart, it's best to plot "Years into Career" rather than absolute Calendar Year
  // Because comparing Fangio (1950s) to Verstappen (2020s) on a calendar timeline is useless.
  // Alternatively, just plot "Seasons Active" (1, 2, 3...)
  const maxYears = Math.max(d1Trends.length, d2Trends.length);
  const careerLabels = Array.from({ length: maxYears }, (_, i) => `Year ${i + 1}`);

  const winsCompareChart = {
    labels: careerLabels,
    datasets: [
      {
        label: d1.full_name,
        data: d1Trends.map(d => d.season_wins),
        borderColor: '#E10600',
        backgroundColor: '#E10600',
        borderWidth: 2,
        tension: 0.3,
      },
      {
        label: d2.full_name,
        data: d2Trends.map(d => d.season_wins),
        borderColor: '#1E90FF',
        backgroundColor: '#1E90FF',
        borderWidth: 2,
        tension: 0.3,
      }
    ]
  };

  return (
    <div>
      <div className="season-controls" style={{ marginBottom: '20px' }}>
          <label>DRIVER 1</label>
          <select className="f1-select" value={d1Key} onChange={e => setD1Key(parseInt(e.target.value))}>
              {sortedDrivers.map(d => (
                <option key={d.driver_key} value={d.driver_key}>{d.full_name}</option>
              ))}
          </select>
          <span style={{ fontFamily: "'Barlow Condensed', sans-serif", fontWeight: 900, fontSize: '22px', color: 'var(--text-dim)', margin: '0 10px' }}>VS</span>
          <label>DRIVER 2</label>
          <select className="f1-select" value={d2Key} onChange={e => setD2Key(parseInt(e.target.value))}>
              {sortedDrivers.map(d => (
                <option key={d.driver_key} value={d.driver_key}>{d.full_name}</option>
              ))}
          </select>
      </div>

      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Head-to-Head Stats</div>
              <div style={{ padding: '10px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '20px' }}>
                  <h3 style={{ fontFamily: "'Barlow Condensed', sans-serif", fontSize: '24px', color: 'var(--red)' }}>{d1.full_name}</h3>
                  <h3 style={{ fontFamily: "'Barlow Condensed', sans-serif", fontSize: '24px', color: 'var(--blue)' }}>{d2.full_name}</h3>
                </div>
                {statRow("Championships", d1.championship_titles, d2.championship_titles)}
                {statRow("Race Wins", d1.total_wins, d2.total_wins)}
                {statRow("Podiums", d1.total_podiums, d2.total_podiums)}
                {statRow("Pole Positions", d1.total_poles, d2.total_poles)}
                {statRow("Race Starts", d1.total_race_starts, d2.total_race_starts)}
                {statRow("Win Rate %", `${d1.win_rate_pct.toFixed(1)}%`, `${d2.win_rate_pct.toFixed(1)}%`)}
                {statRow("Active Years", `${d1.debut_year}-${d1.final_year}`, `${d2.debut_year}-${d2.final_year}`, false)}
              </div>
          </div>
          <div className="card">
              <div className="card-title">Radar — Multi-Dimensional Comparison</div>
              <div className="chart-wrap tall">
                  <Chart 
                    type="radar" 
                    data={radarData} 
                    options={{ 
                      scales: { r: { min: 0, max: 100, ticks: { display: false } } },
                      plugins: { legend: { position: 'bottom' } } 
                    }} 
                  />
              </div>
          </div>
      </div>

      <div className="grid-1" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Career Wins Progression (Season by Season)</div>
              <div className="chart-wrap tall">
                  <Chart type="line" data={winsCompareChart} options={{ plugins: { legend: { display: true, position: 'bottom' } } }} />
              </div>
          </div>
      </div>
    </div>
  );
}
