"use client";

import Chart from "@/components/Chart";

interface ChartData {
  label: string;
  value: number;
}

interface TrendData {
  full_name: string;
  season_year: number;
  season_poles: number;
  season_podiums: number;
}

interface DriversChartsProps {
  winsData: ChartData[];
  polesData: ChartData[];
  pointsData: ChartData[];
  podiumsData: ChartData[];
  trendData?: TrendData[];
  top3Drivers?: string[];
}

export default function DriversCharts({ winsData, polesData, pointsData, podiumsData, trendData, top3Drivers }: DriversChartsProps) {
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

  const years = Array.from(new Set(trendData?.map(d => d.season_year))).sort((a, b) => a - b);
  const colors = ['rgba(225, 6, 0, 1)', 'rgba(30, 144, 255, 1)', 'rgba(255, 215, 0, 1)'];

  const polesTrendConfig = {
    labels: years,
    datasets: top3Drivers?.map((driver, i) => {
      const dataPoints = years.map(y => {
        const row = trendData?.find(d => d.full_name === driver && d.season_year === y);
        return row ? row.season_poles : 0;
      });
      return {
        label: driver,
        data: dataPoints,
        borderColor: colors[i],
        backgroundColor: colors[i],
        borderWidth: 2,
        tension: 0.3,
      };
    }) || []
  };

  const podiumsTrendConfig = {
    labels: years,
    datasets: top3Drivers?.map((driver, i) => {
      const dataPoints = years.map(y => {
        const row = trendData?.find(d => d.full_name === driver && d.season_year === y);
        return row ? row.season_podiums : 0;
      });
      return {
        label: driver,
        data: dataPoints,
        borderColor: colors[i],
        backgroundColor: colors[i].replace('1)', '0.2)'),
        borderWidth: 2,
        fill: true,
        tension: 0.3,
      };
    }) || []
  };

  return (
    <>
      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">All-Time Race Wins (Top 15)</div>
              <div className="chart-wrap tall">
                  <Chart type="bar" data={getChartConfig(winsData, 'Wins', 'rgba(225, 6, 0, 0.6)')} />
              </div>
          </div>
          <div className="card">
              <div className="card-title">Top 15 Drivers — Pole Positions</div>
              <div className="chart-wrap tall">
                  <Chart type="bar" data={getChartConfig(polesData, 'Poles', 'rgba(0, 160, 255, 0.6)')} />
              </div>
          </div>
      </div>

      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Career Points (Modern Scoring)</div>
              <div className="chart-wrap tall">
                  <Chart type="bar" data={getChartConfig(pointsData, 'Points', 'rgba(0, 200, 100, 0.6)')} />
              </div>
          </div>
          <div className="card">
              <div className="card-title">Pole Position Trends (Top 3 Drivers)</div>
              <div className="chart-wrap tall">
                  <Chart type="line" data={polesTrendConfig} options={{ plugins: { legend: { display: true, position: 'bottom' } } }} />
              </div>
          </div>
      </div>

      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Career Podiums — Top 15 Drivers</div>
              <div className="chart-wrap tall">
                  <Chart type="bar" data={getChartConfig(podiumsData, 'Podiums', 'rgba(255, 215, 0, 0.6)')} />
              </div>
          </div>
          <div className="card">
              <div className="card-title">Podium Trends (Area)</div>
              <div className="chart-wrap tall">
                  <Chart type="line" data={podiumsTrendConfig} options={{ plugins: { legend: { display: true, position: 'bottom' } } }} />
              </div>
          </div>
      </div>
    </>
  );
}
