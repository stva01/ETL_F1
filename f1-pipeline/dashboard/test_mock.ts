import { getFallbackData } from './src/lib/mockFallback';

const queries = [
  `SELECT CAST(constructor_titles AS INTEGER) as ferrari_titles FROM MARTS.mart_constructor_career WHERE constructor_name LIKE '%Ferrari%' LIMIT 1`,
  `SELECT CAST(total_wins AS INTEGER) as ferrari_wins FROM MARTS.mart_constructor_career WHERE constructor_name LIKE '%Ferrari%' LIMIT 1`,
  `SELECT CAST(constructor_titles AS INTEGER) as mercedes_titles FROM MARTS.mart_constructor_career WHERE constructor_name LIKE '%Mercedes%' LIMIT 1`,
  `SELECT CAST(constructor_titles AS INTEGER) as redbull_titles FROM MARTS.mart_constructor_career WHERE constructor_name LIKE '%Red Bull%' LIMIT 1`,
  `SELECT constructor_name as label, CAST(total_wins AS INTEGER) as value FROM MARTS.mart_constructor_career ORDER BY total_wins DESC NULLS LAST LIMIT 10`,
  `SELECT constructor_name as label, CAST(constructor_titles AS INTEGER) as value FROM MARTS.mart_constructor_career WHERE constructor_titles > 0 ORDER BY constructor_titles DESC NULLS LAST LIMIT 10`,
  `WITH top_wins AS ( SELECT constructor_name as label, CAST(total_wins AS INTEGER) as value FROM MARTS.mart_constructor_career ORDER BY total_wins DESC NULLS LAST LIMIT 5 ) SELECT label, value FROM top_wins UNION ALL SELECT 'Others' as label, CAST(SUM(total_wins) AS INTEGER) as value FROM MARTS.mart_constructor_career WHERE constructor_name NOT IN (SELECT label FROM top_wins)`,
  `SELECT constructor_name, CAST(season_year AS INTEGER) as season_year, CAST(season_wins AS INTEGER) as season_wins, CAST(season_points AS INTEGER) as season_points FROM MARTS.mart_constructor_season WHERE constructor_name IN ('Ferrari','McLaren','Mercedes','Red Bull') ORDER BY season_year ASC`,
];

queries.forEach((q, i) => {
  const result = getFallbackData(q);
  console.log(`Query ${i + 1} result length:`, result.length);
  if (result.length === 0) {
    console.error(`Query ${i + 1} FAILED TO MATCH!`);
  }
});
