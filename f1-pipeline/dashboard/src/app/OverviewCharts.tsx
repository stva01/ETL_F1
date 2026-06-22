"use client";

import Chart from "@/components/Chart";

interface ChartData {
  label: string;
  value: number;
}

interface OverviewChartsProps {
  titlesData: ChartData[];
  nationalityData: ChartData[];
  racesPerDecadeData: ChartData[];
}

export default function OverviewCharts({ titlesData, nationalityData, racesPerDecadeData }: OverviewChartsProps) {
  const titlesChart = {
    labels: titlesData.map(d => d.label),
    datasets: [{
      label: 'Championships',
      data: titlesData.map(d => d.value),
      backgroundColor: 'rgba(255, 215, 0, 0.6)', // Gold
      borderColor: 'rgba(255, 215, 0, 1)',
      borderWidth: 1,
    }]
  };

  const nationalityChart = {
    labels: nationalityData.map(d => d.label),
    datasets: [{
      label: 'Champions',
      data: nationalityData.map(d => d.value),
      backgroundColor: [
        'rgba(225, 6, 0, 0.6)', // Red
        'rgba(0, 160, 255, 0.6)', // Blue
        'rgba(0, 200, 100, 0.6)', // Green
        'rgba(255, 215, 0, 0.6)', // Gold
        'rgba(255, 255, 255, 0.4)', // White/Gray
      ],
      borderWidth: 0,
    }]
  };

  const decadesChart = {
    labels: racesPerDecadeData.map(d => d.label),
    datasets: [{
      label: 'Races',
      data: racesPerDecadeData.map(d => d.value),
      backgroundColor: 'rgba(225, 6, 0, 0.6)', // Red
      borderColor: 'rgba(225, 6, 0, 1)',
      borderWidth: 1,
    }]
  };

  return (
    <>
      <div className="grid-2" style={{ marginBottom: '20px' }}>
          <div className="card">
              <div className="card-title">Championship Titles by Driver (Top 12)</div>
              <div className="chart-wrap">
                  <Chart type="bar" data={titlesChart} />
              </div>
          </div>
          <div className="card">
              <div className="card-title">Races Per Decade</div>
              <div className="chart-wrap">
                  <Chart type="bar" data={decadesChart} />
              </div>
          </div>
      </div>

      <div className="grid-3">
          <div className="card">
              <div className="card-title">Nationality of World Champions</div>
              <div className="chart-wrap short">
                  <Chart type="doughnut" data={nationalityChart} />
              </div>
          </div>
          {/* Other charts can be added here */}
      </div>
    </>
  );
}
