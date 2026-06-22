"use client";

import Chart from "@/components/Chart";

interface ChartData {
  label: string;
  value: number;
}

interface TrendData {
  constructor_name: string;
  season_year: number;
  season_wins: number;
  season_points: number;
}

interface ConstructorsChartsProps {
  winsData: ChartData[];
  titlesData: ChartData[];
  winShareData: ChartData[];
  trendData?: TrendData[];
  top4Teams?: string[];
}

export default function ConstructorsCharts({ winsData, titlesData, winShareData, trendData, top4Teams }: ConstructorsChartsProps) {
  const getChartConfig = (data: ChartData[], label: string, color: string) => ({
    labels: data.map(d => d.label),
    datasets: [{
      label,
      data: data.map(d => d.value),
      backgroundColor: color,
      borderColor: color,
      borderWidth: 1,
    }]
  });

  const winShareChart = {
    labels: winShareData.map(d => d.label),
    datasets: [{
      label: 'Wins',
      data: winShareData.map(d => d.value),
      backgroundColor: [
        'rgba(225, 6, 0, 0.6)', // Red (Ferrari)
        'rgba(0, 200, 100, 0.6)', // Green (Mercedes)
        'rgba(0, 160, 255, 0.6)', // Blue (Red Bull/Williams)
        'rgba(255, 128, 0, 0.6)', // Orange (McLaren)
        'rgba(255, 215, 0, 0.6)', // Gold
        'rgba(128, 0, 128, 0.6)', // Purple
        'rgba(255, 255, 255, 0.4)', // Others
      ],
      borderWidth: 0,
    }]
  };

  const years = Array.from(new Set(trendData?.map(d => d.season_year))).sort((a, b) => a - b);
  const colors = [
    'rgba(225, 6, 0, 1)',      // Ferrari
    'rgba(255, 128, 0, 1)',    // McLaren
    'rgba(0, 200, 100, 1)',    // Mercedes
    'rgba(0, 160, 255, 1)',    // Red Bull
  ];

  const winsTrendConfig = {
    labels: years,
    datasets: top4Teams?.map((team, i) => {
      const dataPoints = years.map(y => {
        const row = trendData?.find(d => d.constructor_name === team && d.season_year === y);
        return row ? row.season_wins : 0;
      });
      return {
        label: team,
        data: dataPoints,
        borderColor: colors[i],
        backgroundColor: colors[i].replace('1)', '0.4)'),
        borderWidth: 1,
        fill: true,
        tension: 0.3,
      };
    }) || []
  };

  const pointsTrendConfig = {
    labels: years,
    datasets: top4Teams?.map((team, i) => {
      const dataPoints = years.map(y => {
        const row = trendData?.find(d => d.constructor_name === team && d.season_year === y);
        return row ? row.season_points : 0;
      });
      return {
        label: team,
        data: dataPoints,
        borderColor: colors[i],
        backgroundColor: colors[i],
        borderWidth: 2,
        tension: 0.3,
      };
    }) || []
  };

  // Ensure stacked area chart scales are configured for wins
  const winsOptions = {
    plugins: { legend: { display: true, position: 'bottom' as const } },
    scales: {
      x: { stacked: true },
      y: { stacked: true }
    }
  };

  const pointsOptions = {
    plugins: { legend: { display: true, position: 'bottom' as const } }
  };

  return (
    <>
      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Constructor Race Wins (All-Time Top 10)</div>
              <div className="chart-wrap tall">
                  <Chart type="bar" data={getChartConfig(winsData, 'Wins', 'rgba(225, 6, 0, 0.6)')} />
              </div>
          </div>
          <div className="card">
              <div className="card-title">Team Wins Over Time (Stacked Area)</div>
              <div className="chart-wrap tall">
                  <Chart type="line" data={winsTrendConfig} options={winsOptions} />
              </div>
          </div>
      </div>

      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Points Progression (Modern Scoring)</div>
              <div className="chart-wrap tall">
                  <Chart type="line" data={pointsTrendConfig} options={pointsOptions} />
              </div>
          </div>
          <div className="card">
              <div className="card-title">Championship Titles by Constructor</div>
              <div className="chart-wrap tall">
                  <Chart type="bar" data={getChartConfig(titlesData, 'Titles', 'rgba(255, 215, 0, 0.6)')} />
              </div>
          </div>
      </div>

      <div className="grid-2">
          <div className="card">
              <div className="card-title">Constructor Win Share — Top 10 vs Others</div>
              <div className="chart-wrap tall">
                  <Chart type="doughnut" data={winShareChart} />
              </div>
          </div>
      </div>
    </>
  );
}
