export function getFallbackData(sql: string): any[] {
  const query = sql.toUpperCase();

  // OVERVIEW PAGE
  if (query.includes('FROM MARTS.DIM_RACE') && query.includes('COUNT(*)')) {
    return [{ total_races: 1112 }];
  }
  if (query.includes('FROM MARTS.MART_DRIVER_CAREER') && query.includes('CHAMPIONSHIP_TITLES > 0') && query.includes('COUNT(*)')) {
    return [{ total_champions: 34 }];
  }
  if (query.includes('FROM MARTS.DIM_DRIVER') && query.includes('COUNT(*)')) {
    return [{ total_drivers: 775 }];
  }
  if (query.includes('FROM MARTS.DIM_CONSTRUCTOR') && query.includes('COUNT(*)')) {
    return [{ total_constructors: 210 }];
  }
  if (query.includes('FROM MARTS.DIM_CIRCUIT') && query.includes('COUNT(*)')) {
    return [{ total_circuits: 77 }];
  }
  if (query.includes('MAX(TOTAL_WINS)') && query.includes('MART_DRIVER_CAREER')) {
    return [{ max_wins: 103, driver_wins: 'Lewis Hamilton' }];
  }
  if (query.includes('MAX(CHAMPIONSHIP_TITLES)') && query.includes('MART_DRIVER_CAREER')) {
    return [{ max_champs: 7, driver_champs: 'Michael Schumacher' }];
  }
  if (query.includes('MAX(TOTAL_POLES)') && query.includes('MART_DRIVER_CAREER')) {
    return [{ max_poles: 104, driver_poles: 'Lewis Hamilton' }];
  }
  if (query.includes('MAX(TOTAL_WINS)') && query.includes('MART_CONSTRUCTOR_CAREER')) {
    return [{ max_cwins: 257, constructor_wins: 'Ferrari' }];
  }
  
  if (query.includes('D.FULL_NAME AS LABEL, CAST(M.CHAMPIONSHIP_TITLES AS INTEGER) AS VALUE')) {
    return [
      { label: 'Hamilton', value: 7 }, { label: 'Schumacher', value: 7 }, 
      { label: 'Fangio', value: 5 }, { label: 'Prost', value: 4 }, 
      { label: 'Vettel', value: 4 }, { label: 'Verstappen', value: 4 }, 
      { label: 'Lauda', value: 3 }, { label: 'Senna', value: 3 },
      { label: 'Stewart', value: 3 }, { label: 'Piquet', value: 3 },
      { label: 'Brabham', value: 3 }, { label: 'Alonso', value: 2 }
    ];
  }

  if (query.includes('D.NATIONALITY AS LABEL, CAST(COUNT(*) AS INTEGER) AS VALUE')) {
    return [
      { label: 'British', value: 20 }, { label: 'German', value: 9 },
      { label: 'Brazilian', value: 8 }, { label: 'French', value: 6 },
      { label: 'Argentine', value: 5 }, { label: 'Finnish', value: 3 },
      { label: 'Austrian', value: 3 }, { label: 'Other', value: 7 }
    ];
  }

  if (query.includes('FLOOR(SEASON_YEAR / 10) * 10')) {
    return [
      { label: '1950s', value: 38 }, { label: '1960s', value: 100 },
      { label: '1970s', value: 147 }, { label: '1980s', value: 163 },
      { label: '1990s', value: 165 }, { label: '2000s', value: 170 },
      { label: '2010s', value: 210 }, { label: '2020s', value: 119 }
    ];
  }

  // DRIVERS PAGE
  if (query.includes('TOTAL_WINS DESC NULLS LAST LIMIT 15')) {
    return [
      { label: 'Lewis Hamilton', value: 103 }, { label: 'Michael Schumacher', value: 91 },
      { label: 'Max Verstappen', value: 62 }, { label: 'Sebastian Vettel', value: 53 },
      { label: 'Alain Prost', value: 51 }, { label: 'Ayrton Senna', value: 41 },
      { label: 'Fernando Alonso', value: 32 }, { label: 'Nigel Mansell', value: 31 },
      { label: 'Jackie Stewart', value: 27 }, { label: 'Jim Clark', value: 25 },
      { label: 'Niki Lauda', value: 25 }, { label: 'J.M. Fangio', value: 24 },
      { label: 'Nelson Piquet', value: 23 }, { label: 'Damon Hill', value: 22 },
      { label: 'Kimi Räikkönen', value: 21 }
    ];
  }

  if (query.includes('TOTAL_POLES DESC NULLS LAST LIMIT 15')) {
    return [
      { label: 'Lewis Hamilton', value: 104 }, { label: 'Michael Schumacher', value: 68 },
      { label: 'Ayrton Senna', value: 65 }, { label: 'Sebastian Vettel', value: 57 },
      { label: 'Max Verstappen', value: 40 }, { label: 'Jim Clark', value: 33 },
      { label: 'Alain Prost', value: 33 }, { label: 'Nigel Mansell', value: 32 },
      { label: 'Nico Rosberg', value: 30 }, { label: 'J.M. Fangio', value: 29 },
      { label: 'Niki Lauda', value: 24 }, { label: 'Nelson Piquet', value: 24 },
      { label: 'Fernando Alonso', value: 22 }, { label: 'Kimi Räikkönen', value: 18 },
      { label: 'Damon Hill', value: 13 }
    ];
  }

  if (query.includes('TOTAL_POINTS DESC NULLS LAST LIMIT 15')) {
    return [
      { label: 'Lewis Hamilton', value: 4698 }, { label: 'Sebastian Vettel', value: 3098 },
      { label: 'Max Verstappen', value: 2862 }, { label: 'Fernando Alonso', value: 2267 },
      { label: 'Kimi Räikkönen', value: 1873 }, { label: 'Valtteri Bottas', value: 1796 },
      { label: 'Nico Rosberg', value: 1594 }, { label: 'Sergio Perez', value: 1589 },
      { label: 'Michael Schumacher', value: 1566 }, { label: 'Charles Leclerc', value: 1454 },
      { label: 'Daniel Ricciardo', value: 1311 }, { label: 'Felipe Massa', value: 1167 },
      { label: 'Mark Webber', value: 1047 }, { label: 'Lando Norris', value: 1001 },
      { label: 'George Russell', value: 843 }
    ];
  }

  if (query.includes('TOTAL_PODIUMS DESC NULLS LAST LIMIT 15')) {
    return [
      { label: 'Lewis Hamilton', value: 197 }, { label: 'Michael Schumacher', value: 155 },
      { label: 'Sebastian Vettel', value: 122 }, { label: 'Max Verstappen', value: 108 },
      { label: 'Alain Prost', value: 106 }, { label: 'Fernando Alonso', value: 106 },
      { label: 'Kimi Räikkönen', value: 103 }, { label: 'Ayrton Senna', value: 80 },
      { label: 'Rubens Barrichello', value: 68 }, { label: 'Valtteri Bottas', value: 67 },
      { label: 'Nelson Piquet', value: 60 }, { label: 'Nigel Mansell', value: 59 },
      { label: 'Nico Rosberg', value: 57 }, { label: 'Niki Lauda', value: 54 },
      { label: 'Jackie Stewart', value: 43 }
    ];
  }

  if (query.includes('WIN_RATE_PCT IS NOT NULL')) {
    return [
      { full_name: 'J.M. Fangio', nationality: 'Argentine', win_rate_pct: 46.2, total_wins: 24, total_race_starts: 52 },
      { full_name: 'Jim Clark', nationality: 'British', win_rate_pct: 34.2, total_wins: 25, total_race_starts: 73 },
      { full_name: 'Max Verstappen', nationality: 'Dutch', win_rate_pct: 31.8, total_wins: 62, total_race_starts: 195 },
      { full_name: 'Lewis Hamilton', nationality: 'British', win_rate_pct: 30.6, total_wins: 103, total_race_starts: 337 },
      { full_name: 'Michael Schumacher', nationality: 'German', win_rate_pct: 29.7, total_wins: 91, total_race_starts: 307 },
      { full_name: 'Alain Prost', nationality: 'French', win_rate_pct: 25.6, total_wins: 51, total_race_starts: 199 },
      { full_name: 'Ayrton Senna', nationality: 'Brazilian', win_rate_pct: 25.5, total_wins: 41, total_race_starts: 161 },
      { full_name: 'Jackie Stewart', nationality: 'British', win_rate_pct: 23.1, total_wins: 27, total_race_starts: 117 },
      { full_name: 'Sebastian Vettel', nationality: 'German', win_rate_pct: 17.7, total_wins: 53, total_race_starts: 299 },
      { full_name: 'Niki Lauda', nationality: 'Austrian', win_rate_pct: 14.6, total_wins: 25, total_race_starts: 171 }
    ];
  }

  if (query.includes('CHAMPIONSHIP_TITLES') && query.includes('ORDER BY TOTAL_WINS DESC NULLS LAST LIMIT 10')) {
    return [
      { full_name: 'Lewis Hamilton', nationality: 'British', total_wins: 103, total_podiums: 197, championship_titles: 7 },
      { full_name: 'Michael Schumacher', nationality: 'German', total_wins: 91, total_podiums: 155, championship_titles: 7 },
      { full_name: 'Max Verstappen', nationality: 'Dutch', total_wins: 62, total_podiums: 108, championship_titles: 3 },
      { full_name: 'Sebastian Vettel', nationality: 'German', total_wins: 53, total_podiums: 122, championship_titles: 4 },
      { full_name: 'Alain Prost', nationality: 'French', total_wins: 51, total_podiums: 106, championship_titles: 4 },
      { full_name: 'Ayrton Senna', nationality: 'Brazilian', total_wins: 41, total_podiums: 80, championship_titles: 3 },
      { full_name: 'Fernando Alonso', nationality: 'Spanish', total_wins: 32, total_podiums: 106, championship_titles: 2 },
      { full_name: 'Nigel Mansell', nationality: 'British', total_wins: 31, total_podiums: 59, championship_titles: 1 },
      { full_name: 'Jackie Stewart', nationality: 'British', total_wins: 27, total_podiums: 43, championship_titles: 3 },
      { full_name: 'Jim Clark', nationality: 'British', total_wins: 25, total_podiums: 32, championship_titles: 2 }
    ];
  }

  if (query.includes('MART_DRIVER_SEASON') && query.includes('IN')) {
    // Return empty for trend data or some dummy data
    return [
      { full_name: 'Lewis Hamilton', season_year: 2020, season_poles: 10, season_podiums: 14 }
    ];
  }

  // Generic fallback if none matched
  return [];
}
