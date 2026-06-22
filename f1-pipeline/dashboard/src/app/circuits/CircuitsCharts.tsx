"use client";

import Chart from "@/components/Chart";

interface ChartData {
  label: string;
  value: number;
}

interface CircuitsChartsProps {
  hostedData: ChartData[];
  countryData: ChartData[];
}

export default function CircuitsCharts({ hostedData, countryData }: CircuitsChartsProps) {
  const hostedChart = {
    labels: hostedData.map(d => d.label),
    datasets: [{
      label: 'Races Hosted',
      data: hostedData.map(d => d.value),
      backgroundColor: 'rgba(225, 6, 0, 0.6)',
      borderColor: 'rgba(225, 6, 0, 1)',
      borderWidth: 1,
    }]
  };

  const countryChart = {
    labels: countryData.map(d => d.label),
    datasets: [{
      label: 'Circuits',
      data: countryData.map(d => d.value),
      backgroundColor: [
        'rgba(225, 6, 0, 0.6)',
        'rgba(0, 160, 255, 0.6)',
        'rgba(0, 200, 100, 0.6)',
        'rgba(255, 215, 0, 0.6)',
        'rgba(255, 255, 255, 0.4)',
      ],
      borderWidth: 0,
    }]
  };

  return (
    <div className="grid-2" style={{ marginBottom: '20px' }}>
        <div className="card">
            <div className="card-title">Most Races Hosted (Top 15 Circuits)</div>
            <div className="chart-wrap tall">
                <Chart type="bar" data={hostedChart} />
            </div>
        </div>
        <div className="card">
            <div className="card-title">Circuits by Country</div>
            <div className="chart-wrap tall">
                <Chart type="doughnut" data={countryChart} />
            </div>
        </div>
    </div>
  );
}
