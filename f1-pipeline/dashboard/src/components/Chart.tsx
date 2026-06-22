"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  RadialLinearScale,
  RadarController,
  Filler,
  Title,
  Tooltip,
  Legend,
  ChartOptions
} from 'chart.js';
import { Bar, Line, Pie, Doughnut, Radar } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  RadialLinearScale,
  RadarController,
  Filler,
  Title,
  Tooltip,
  Legend
);

// Global Chart styling to match mockdashboard
ChartJS.defaults.color = 'rgba(255, 255, 255, 0.5)';
ChartJS.defaults.font.family = "'Share Tech Mono', monospace";
ChartJS.defaults.plugins.tooltip.backgroundColor = '#1e1e1e';
ChartJS.defaults.plugins.tooltip.titleFont = { family: "'Barlow Condensed', sans-serif", size: 16 };
ChartJS.defaults.plugins.tooltip.bodyFont = { family: "'Share Tech Mono', monospace", size: 13 };

interface ChartProps {
  type: 'bar' | 'line' | 'pie' | 'doughnut' | 'radar';
  data: any;
  options?: ChartOptions<any>;
}

export default function Chart({ type, data, options }: ChartProps) {
  const defaultOptions: ChartOptions<any> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false }
    },
    ...options
  };

  switch (type) {
    case 'bar':
      return <Bar data={data} options={defaultOptions} />;
    case 'line':
      return <Line data={data} options={defaultOptions} />;
    case 'pie':
      return <Pie data={data} options={defaultOptions} />;
    case 'doughnut':
      return <Doughnut data={data} options={defaultOptions} />;
    case 'radar':
      return <Radar data={data} options={defaultOptions} />;
    default:
      return null;
  }
}
