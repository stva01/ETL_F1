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

  if (query.includes('MART_DRIVER_SEASON') && query.includes(' IN (')) {
    // Return empty for trend data or some dummy data
    return [
      { full_name: 'Lewis Hamilton', season_year: 2020, season_poles: 10, season_podiums: 14 }
    ];
  }

  // CONSTRUCTORS PAGE
  if (query.includes('FERRARI_TITLES')) {
    return [{ ferrari_titles: 16 }];
  }
  if (query.includes('FERRARI_WINS')) {
    return [{ ferrari_wins: 243 }];
  }
  if (query.includes('MERCEDES_TITLES')) {
    return [{ mercedes_titles: 8 }];
  }
  if (query.includes('REDBULL_TITLES')) {
    return [{ redbull_titles: 6 }];
  }
  if (query.includes('TOP_WINS AS')) {
    return [
      { label: 'Ferrari', value: 243 }, { label: 'McLaren', value: 183 },
      { label: 'Mercedes', value: 125 }, { label: 'Williams', value: 114 },
      { label: 'Red Bull', value: 113 }, { label: 'Others', value: 334 }
    ];
  }
  if (query.includes('MART_CONSTRUCTOR_CAREER') && query.includes('ORDER BY TOTAL_WINS DESC NULLS LAST LIMIT 10')) {
    return [
      { label: 'Ferrari', value: 243 }, { label: 'McLaren', value: 183 },
      { label: 'Mercedes', value: 125 }, { label: 'Williams', value: 114 },
      { label: 'Red Bull', value: 113 }, { label: 'Lotus', value: 79 },
      { label: 'Brabham', value: 35 }, { label: 'Renault', value: 35 },
      { label: 'Benetton', value: 27 }, { label: 'Tyrrell', value: 23 }
    ];
  }
  if (query.includes('MART_CONSTRUCTOR_CAREER') && query.includes('ORDER BY CONSTRUCTOR_TITLES DESC NULLS LAST LIMIT 10')) {
    return [
      { label: 'Ferrari', value: 16 }, { label: 'Williams', value: 9 },
      { label: 'McLaren', value: 8 }, { label: 'Mercedes', value: 8 },
      { label: 'Lotus', value: 7 }, { label: 'Red Bull', value: 6 },
      { label: 'Brabham', value: 2 }, { label: 'Renault', value: 2 },
      { label: 'Benetton', value: 2 }, { label: 'Tyrrell', value: 1 }
    ];
  }
  if (query.includes('MART_CONSTRUCTOR_SEASON') && query.includes(' IN (')) {
    return [
      { constructor_name: 'Ferrari', season_year: 2020, season_wins: 0, season_points: 131 },
      { constructor_name: 'Mercedes', season_year: 2020, season_wins: 13, season_points: 573 },
      { constructor_name: 'Red Bull', season_year: 2020, season_wins: 2, season_points: 319 }
    ];
  }

  // COMPARE PAGE
  if (query.includes('MART_DRIVER_CAREER') && query.includes('ORDER BY TOTAL_RACE_STARTS DESC')) {
    return [
      { driver_key: 1, full_name: 'Lewis Hamilton', nationality: 'British', debut_year: 2007, final_year: 2024, total_race_starts: 337, total_wins: 103, total_podiums: 197, total_poles: 104, total_points: 4698, championship_titles: 7, win_rate_pct: 30.6 },
      { driver_key: 2, full_name: 'Michael Schumacher', nationality: 'German', debut_year: 1991, final_year: 2012, total_race_starts: 307, total_wins: 91, total_podiums: 155, total_poles: 68, total_points: 1566, championship_titles: 7, win_rate_pct: 29.7 }
    ];
  }

  // CIRCUITS PAGE
  if (query.includes('MART_CIRCUIT_STATS') && query.includes('GROUP BY COUNTRY')) {
    return [
      { label: 'Italy', value: 104 }, { label: 'Germany', value: 79 },
      { label: 'United Kingdom', value: 74 }, { label: 'USA', value: 73 },
      { label: 'Monaco', value: 70 }, { label: 'Belgium', value: 68 },
      { label: 'France', value: 62 }, { label: 'Spain', value: 53 }
    ];
  }
  if (query.includes('MART_CIRCUIT_STATS') && query.includes('ORDER BY TOTAL_RACES_HELD DESC')) {
    if (query.includes('AS LABEL')) {
      return [
        { label: 'Monza', value: 73 }, { label: 'Monaco', value: 70 },
        { label: 'Silverstone', value: 58 }, { label: 'Spa-Francorchamps', value: 56 },
        { label: 'Circuit Gilles Villeneuve', value: 42 }, { label: 'Nürburgring', value: 41 },
        { label: 'Interlagos', value: 40 }, { label: 'Suzuka', value: 33 },
        { label: 'Hungaroring', value: 38 }, { label: 'Albert Park', value: 26 },
        { label: 'Red Bull Ring', value: 19 }, { label: 'Catalunya', value: 33 },
        { label: 'Imola', value: 30 }, { label: 'Zandvoort', value: 33 },
        { label: 'Hockenheimring', value: 37 }
      ];
    } else {
      return [
        { circuit_name: 'Monza', country: 'Italy', total_races_held: 73, most_wins_driver: 'Michael Schumacher' },
        { circuit_name: 'Monaco', country: 'Monaco', total_races_held: 70, most_wins_driver: 'Ayrton Senna' },
        { circuit_name: 'Silverstone', country: 'UK', total_races_held: 58, most_wins_driver: 'Lewis Hamilton' },
        { circuit_name: 'Spa-Francorchamps', country: 'Belgium', total_races_held: 56, most_wins_driver: 'Michael Schumacher' },
        { circuit_name: 'Circuit Gilles Villeneuve', country: 'Canada', total_races_held: 42, most_wins_driver: 'Michael Schumacher' },
        { circuit_name: 'Nürburgring', country: 'Germany', total_races_held: 41, most_wins_driver: 'Michael Schumacher' },
        { circuit_name: 'Interlagos', country: 'Brazil', total_races_held: 40, most_wins_driver: 'Alain Prost' },
        { circuit_name: 'Hungaroring', country: 'Hungary', total_races_held: 38, most_wins_driver: 'Lewis Hamilton' }
      ];
    }
  }
  if (query.includes('MART_DRIVER_CAREER') && query.includes('TOTAL_FASTEST_LAPS DESC')) {
    return [
      { full_name: 'Michael Schumacher', total_fastest_laps: 77 },
      { full_name: 'Lewis Hamilton', total_fastest_laps: 65 },
      { full_name: 'Kimi Räikkönen', total_fastest_laps: 46 },
      { full_name: 'Alain Prost', total_fastest_laps: 41 },
      { full_name: 'Sebastian Vettel', total_fastest_laps: 38 }
    ];
  }

  // SEASONS PAGE
  if (query.includes('DIM_RACE') && query.includes('DISTINCT')) {
    return [{ season_year: 2024 }, { season_year: 2023 }, { season_year: 2022 }, { season_year: 2021 }];
  }
  if (query.includes('MART_SEASON_SUMMARY')) {
    return [{ season_year: 2024, total_races: 24, driver_champion: 'Max Verstappen', constructor_champion: 'Red Bull' }];
  }
  if (query.includes('MART_DRIVER_SEASON') && query.includes('ORDER BY SEASON_POINTS DESC')) {
    return [
      { driver_name: 'Max Verstappen', season_points: 575, season_wins: 19 },
      { driver_name: 'Sergio Perez', season_points: 285, season_wins: 2 },
      { driver_name: 'Lewis Hamilton', season_points: 234, season_wins: 0 },
      { driver_name: 'Fernando Alonso', season_points: 206, season_wins: 0 },
      { driver_name: 'Charles Leclerc', season_points: 206, season_wins: 0 },
      { driver_name: 'Lando Norris', season_points: 205, season_wins: 0 },
      { driver_name: 'Carlos Sainz', season_points: 200, season_wins: 1 },
      { driver_name: 'George Russell', season_points: 175, season_wins: 0 },
      { driver_name: 'Oscar Piastri', season_points: 97, season_wins: 0 },
      { driver_name: 'Lance Stroll', season_points: 74, season_wins: 0 }
    ];
  }
  if (query.includes('MART_RACE_SUMMARY')) {
    return [
      { race_name: 'Bahrain Grand Prix', winning_driver: 'Max Verstappen', winning_constructor: 'Red Bull', pole_sitter_name: 'Max Verstappen', fastest_lap_driver: 'Max Verstappen' },
      { race_name: 'Saudi Arabian Grand Prix', winning_driver: 'Sergio Perez', winning_constructor: 'Red Bull', pole_sitter_name: 'Sergio Perez', fastest_lap_driver: 'Max Verstappen' },
      { race_name: 'Australian Grand Prix', winning_driver: 'Max Verstappen', winning_constructor: 'Red Bull', pole_sitter_name: 'Max Verstappen', fastest_lap_driver: 'Sergio Perez' },
      { race_name: 'Azerbaijan Grand Prix', winning_driver: 'Sergio Perez', winning_constructor: 'Red Bull', pole_sitter_name: 'Charles Leclerc', fastest_lap_driver: 'George Russell' },
      { race_name: 'Miami Grand Prix', winning_driver: 'Max Verstappen', winning_constructor: 'Red Bull', pole_sitter_name: 'Sergio Perez', fastest_lap_driver: 'Max Verstappen' }
    ];
  }

  // RECORDS PAGE
  if (query.includes('MART_RECORDS')) {
    return [
      { record_category: 'Most Championship Titles', record_type: 'Driver Championships', holder_name: 'Michael Schumacher', record_value_formatted: '7', context_note: 'Tied with Hamilton' },
      { record_category: 'Most Race Wins', record_type: 'Total Wins', holder_name: 'Lewis Hamilton', record_value_formatted: '103', context_note: null },
      { record_category: 'Most Wins in a Season', record_type: 'Season Wins', holder_name: 'Max Verstappen', record_value_formatted: '19', context_note: '2023 Season' },
      { record_category: 'Most Pole Positions', record_type: 'Career Poles', holder_name: 'Lewis Hamilton', record_value_formatted: '104', context_note: null },
      { record_category: 'Highest Win Rate', record_type: 'Win Percentage', holder_name: 'J.M. Fangio', record_value_formatted: '46.2%', context_note: 'Min 50 starts' },
      { record_category: 'Most Fastest Laps', record_type: 'Career Fastest Laps', holder_name: 'Michael Schumacher', record_value_formatted: '77', context_note: null }
    ];
  }

  // Generic fallback if none matched
  return [];
}
