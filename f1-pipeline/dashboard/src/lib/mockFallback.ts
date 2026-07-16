// ═══════════════════════════════════════════════════════════════════════
// F1 Analytics Dashboard — Mock Fallback Data
// ═══════════════════════════════════════════════════════════════════════
// Used when Snowflake credentials expire (USE_MOCK_DATA=true).
// Architecture: table-name dispatch → specific pattern matching.
// All data is pre-built at module load for O(1) dispatch.
// ═══════════════════════════════════════════════════════════════════════

// ─── HELPERS ─────────────────────────────────────────────────────────

function extractYear(q: string): number | null {
  const m = q.match(/SEASON_YEAR\s*=\s*(\d+)/);
  return m ? parseInt(m[1]) : null;
}

// ─── SEASON CHAMPIONS (1950–2024) ────────────────────────────────────
// [year, driverChamp, constructorChamp, driverNationality, totalRaces]
const CHAMPIONS: [number, string, string, string, number][] = [
  [1950,'Giuseppe Farina','','Italian',7],
  [1951,'Juan Manuel Fangio','','Argentine',8],
  [1952,'Alberto Ascari','','Italian',8],
  [1953,'Alberto Ascari','','Italian',9],
  [1954,'Juan Manuel Fangio','','Argentine',9],
  [1955,'Juan Manuel Fangio','','Argentine',7],
  [1956,'Juan Manuel Fangio','','Argentine',8],
  [1957,'Juan Manuel Fangio','','Argentine',8],
  [1958,'Mike Hawthorn','Vanwall','British',11],
  [1959,'Jack Brabham','Cooper','Australian',9],
  [1960,'Jack Brabham','Cooper','Australian',10],
  [1961,'Phil Hill','Ferrari','American',8],
  [1962,'Graham Hill','BRM','British',9],
  [1963,'Jim Clark','Lotus','British',10],
  [1964,'John Surtees','Ferrari','British',10],
  [1965,'Jim Clark','Lotus','British',10],
  [1966,'Jack Brabham','Brabham','Australian',9],
  [1967,'Denny Hulme','Brabham','New Zealander',11],
  [1968,'Graham Hill','Lotus','British',12],
  [1969,'Jackie Stewart','Matra','British',11],
  [1970,'Jochen Rindt','Lotus','Austrian',13],
  [1971,'Jackie Stewart','Tyrrell','British',11],
  [1972,'Emerson Fittipaldi','Lotus','Brazilian',12],
  [1973,'Jackie Stewart','Lotus','British',15],
  [1974,'Emerson Fittipaldi','McLaren','Brazilian',15],
  [1975,'Niki Lauda','Ferrari','Austrian',14],
  [1976,'James Hunt','Ferrari','British',16],
  [1977,'Niki Lauda','Ferrari','Austrian',17],
  [1978,'Mario Andretti','Lotus','American',16],
  [1979,'Jody Scheckter','Ferrari','South African',15],
  [1980,'Alan Jones','Williams','Australian',14],
  [1981,'Nelson Piquet','Williams','Brazilian',15],
  [1982,'Keke Rosberg','Ferrari','Finnish',16],
  [1983,'Nelson Piquet','Ferrari','Brazilian',15],
  [1984,'Niki Lauda','McLaren','Austrian',16],
  [1985,'Alain Prost','McLaren','French',16],
  [1986,'Alain Prost','Williams','French',16],
  [1987,'Nelson Piquet','Williams','Brazilian',16],
  [1988,'Ayrton Senna','McLaren','Brazilian',16],
  [1989,'Alain Prost','McLaren','French',16],
  [1990,'Ayrton Senna','McLaren','Brazilian',16],
  [1991,'Ayrton Senna','McLaren','Brazilian',16],
  [1992,'Nigel Mansell','Williams','British',16],
  [1993,'Alain Prost','Williams','French',16],
  [1994,'Michael Schumacher','Williams','German',16],
  [1995,'Michael Schumacher','Benetton','German',17],
  [1996,'Damon Hill','Williams','British',16],
  [1997,'Jacques Villeneuve','Williams','Canadian',17],
  [1998,'Mika Häkkinen','McLaren','Finnish',16],
  [1999,'Mika Häkkinen','Ferrari','Finnish',16],
  [2000,'Michael Schumacher','Ferrari','German',17],
  [2001,'Michael Schumacher','Ferrari','German',17],
  [2002,'Michael Schumacher','Ferrari','German',17],
  [2003,'Michael Schumacher','Ferrari','German',16],
  [2004,'Michael Schumacher','Ferrari','German',18],
  [2005,'Fernando Alonso','Renault','Spanish',19],
  [2006,'Fernando Alonso','Renault','Spanish',18],
  [2007,'Kimi Räikkönen','Ferrari','Finnish',17],
  [2008,'Lewis Hamilton','Ferrari','British',18],
  [2009,'Jenson Button','Brawn GP','British',17],
  [2010,'Sebastian Vettel','Red Bull','German',19],
  [2011,'Sebastian Vettel','Red Bull','German',19],
  [2012,'Sebastian Vettel','Red Bull','German',20],
  [2013,'Sebastian Vettel','Red Bull','German',19],
  [2014,'Lewis Hamilton','Mercedes','British',19],
  [2015,'Lewis Hamilton','Mercedes','British',19],
  [2016,'Nico Rosberg','Mercedes','German',21],
  [2017,'Lewis Hamilton','Mercedes','British',20],
  [2018,'Lewis Hamilton','Mercedes','British',21],
  [2019,'Lewis Hamilton','Mercedes','British',21],
  [2020,'Lewis Hamilton','Mercedes','British',17],
  [2021,'Max Verstappen','Mercedes','Dutch',22],
  [2022,'Max Verstappen','Red Bull','Dutch',22],
  [2023,'Max Verstappen','Red Bull','Dutch',23],
  [2024,'Max Verstappen','McLaren','Dutch',24],
];

const CHAMPION_MAP = new Map(
  CHAMPIONS.map(([y, dc, cc, , r]) => [y, {
    season_year: y, total_races: r,
    driver_champion: dc, constructor_champion: cc || 'N/A',
  }])
);

// ─── SEASON STANDINGS (2020–2024) ────────────────────────────────────
// [driverName, points, wins]
const STANDINGS: Record<number, [string, number, number][]> = {
  2024: [
    ['Max Verstappen',437,9],['Lando Norris',374,4],['Charles Leclerc',356,3],
    ['Oscar Piastri',292,2],['Carlos Sainz',290,2],['George Russell',245,2],
    ['Lewis Hamilton',223,2],['Sergio Perez',152,0],['Fernando Alonso',70,0],
    ['Pierre Gasly',42,0],['Nico Hulkenberg',41,0],['Yuki Tsunoda',30,0],
    ['Lance Stroll',24,0],['Esteban Ocon',23,0],['Daniel Ricciardo',12,0],
    ['Oliver Bearman',7,0],['Alexander Albon',6,0],['Kevin Magnussen',6,0],
    ['Valtteri Bottas',0,0],['Zhou Guanyu',0,0],
  ],
  2023: [
    ['Max Verstappen',575,19],['Sergio Perez',285,2],['Lewis Hamilton',234,0],
    ['Fernando Alonso',206,0],['Charles Leclerc',206,0],['Lando Norris',205,0],
    ['Carlos Sainz',200,1],['George Russell',175,0],['Oscar Piastri',97,0],
    ['Lance Stroll',74,0],['Pierre Gasly',62,0],['Esteban Ocon',58,0],
    ['Alexander Albon',27,0],['Yuki Tsunoda',17,0],['Valtteri Bottas',10,0],
    ['Nico Hulkenberg',9,0],['Daniel Ricciardo',6,0],['Zhou Guanyu',6,0],
    ['Kevin Magnussen',3,0],['Nyck de Vries',0,0],
  ],
  2022: [
    ['Max Verstappen',454,15],['Charles Leclerc',308,3],['Sergio Perez',305,2],
    ['George Russell',275,1],['Carlos Sainz',246,1],['Lewis Hamilton',240,0],
    ['Lando Norris',122,0],['Esteban Ocon',92,0],['Fernando Alonso',81,0],
    ['Valtteri Bottas',49,0],['Daniel Ricciardo',37,0],['Sebastian Vettel',37,0],
    ['Kevin Magnussen',25,0],['Pierre Gasly',23,0],['Lance Stroll',18,0],
    ['Mick Schumacher',12,0],['Yuki Tsunoda',12,0],['Zhou Guanyu',6,0],
    ['Alexander Albon',4,0],['Nicholas Latifi',2,0],
  ],
  2021: [
    ['Max Verstappen',395,10],['Lewis Hamilton',387,8],['Valtteri Bottas',226,1],
    ['Sergio Perez',190,1],['Carlos Sainz',164,0],['Lando Norris',160,0],
    ['Charles Leclerc',159,0],['Daniel Ricciardo',115,1],['Pierre Gasly',110,0],
    ['Fernando Alonso',81,0],['Esteban Ocon',74,1],['Sebastian Vettel',43,0],
    ['Lance Stroll',34,0],['Yuki Tsunoda',32,0],['George Russell',16,0],
    ['Kimi Räikkönen',10,0],['Nicholas Latifi',7,0],['Antonio Giovinazzi',3,0],
    ['Mick Schumacher',0,0],['Nikita Mazepin',0,0],
  ],
  2020: [
    ['Lewis Hamilton',347,11],['Valtteri Bottas',223,2],['Max Verstappen',214,2],
    ['Sergio Perez',125,1],['Daniel Ricciardo',119,0],['Carlos Sainz',105,0],
    ['Alexander Albon',105,0],['Charles Leclerc',98,0],['Lando Norris',97,0],
    ['Pierre Gasly',75,1],['Lance Stroll',75,0],['Esteban Ocon',62,0],
    ['Sebastian Vettel',33,0],['Daniil Kvyat',32,0],['Nico Hulkenberg',10,0],
  ],
};

// ─── SEASON RACE RESULTS (2023–2024) ─────────────────────────────────
// [raceName, winner, team, poleSitter, fastestLap]
const RACES: Record<number, [string, string, string, string, string][]> = {
  2024: [
    ['Bahrain Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Charles Leclerc'],
    ['Saudi Arabian Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Charles Leclerc'],
    ['Australian Grand Prix','Carlos Sainz','Ferrari','Max Verstappen','Charles Leclerc'],
    ['Japanese Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lewis Hamilton'],
    ['Chinese Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lando Norris'],
    ['Miami Grand Prix','Lando Norris','McLaren','Max Verstappen','Oscar Piastri'],
    ['Emilia Romagna Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Oscar Piastri'],
    ['Monaco Grand Prix','Charles Leclerc','Ferrari','Charles Leclerc','Lewis Hamilton'],
    ['Canadian Grand Prix','Max Verstappen','Red Bull','George Russell','Lewis Hamilton'],
    ['Spanish Grand Prix','Max Verstappen','Red Bull','Lando Norris','Lando Norris'],
    ['Austrian Grand Prix','George Russell','Mercedes','Max Verstappen','Oscar Piastri'],
    ['British Grand Prix','Lewis Hamilton','Mercedes','George Russell','Lewis Hamilton'],
    ['Hungarian Grand Prix','Oscar Piastri','McLaren','Lando Norris','Lando Norris'],
    ['Belgian Grand Prix','Lewis Hamilton','Mercedes','Charles Leclerc','Lewis Hamilton'],
    ['Dutch Grand Prix','Lando Norris','McLaren','Lando Norris','Lando Norris'],
    ['Italian Grand Prix','Charles Leclerc','Ferrari','Lando Norris','Oscar Piastri'],
    ['Azerbaijan Grand Prix','Oscar Piastri','McLaren','Charles Leclerc','George Russell'],
    ['Singapore Grand Prix','Lando Norris','McLaren','Lando Norris','Daniel Ricciardo'],
    ['United States Grand Prix','Charles Leclerc','Ferrari','Lando Norris','Carlos Sainz'],
    ['Mexico City Grand Prix','Carlos Sainz','Ferrari','Carlos Sainz','Charles Leclerc'],
    ['São Paulo Grand Prix','Max Verstappen','Red Bull','Lando Norris','Lando Norris'],
    ['Las Vegas Grand Prix','George Russell','Mercedes','George Russell','Oscar Piastri'],
    ['Qatar Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lando Norris'],
    ['Abu Dhabi Grand Prix','Lando Norris','McLaren','Lando Norris','Oscar Piastri'],
  ],
  2023: [
    ['Bahrain Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Max Verstappen'],
    ['Saudi Arabian Grand Prix','Sergio Perez','Red Bull','Sergio Perez','Max Verstappen'],
    ['Australian Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Carlos Sainz'],
    ['Azerbaijan Grand Prix','Sergio Perez','Red Bull','Charles Leclerc','George Russell'],
    ['Miami Grand Prix','Max Verstappen','Red Bull','Sergio Perez','Max Verstappen'],
    ['Monaco Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lewis Hamilton'],
    ['Spanish Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Max Verstappen'],
    ['Canadian Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lewis Hamilton'],
    ['Austrian Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Max Verstappen'],
    ['British Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Max Verstappen'],
    ['Hungarian Grand Prix','Max Verstappen','Red Bull','Lewis Hamilton','Max Verstappen'],
    ['Belgian Grand Prix','Max Verstappen','Red Bull','Charles Leclerc','Lewis Hamilton'],
    ['Dutch Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lando Norris'],
    ['Italian Grand Prix','Carlos Sainz','Ferrari','Carlos Sainz','Lewis Hamilton'],
    ['Singapore Grand Prix','Carlos Sainz','Ferrari','Carlos Sainz','Lando Norris'],
    ['Japanese Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Max Verstappen'],
    ['Qatar Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lando Norris'],
    ['United States Grand Prix','Max Verstappen','Red Bull','Charles Leclerc','Lando Norris'],
    ['Mexico City Grand Prix','Max Verstappen','Red Bull','Charles Leclerc','Lewis Hamilton'],
    ['São Paulo Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lando Norris'],
    ['Las Vegas Grand Prix','Max Verstappen','Red Bull','Charles Leclerc','Oscar Piastri'],
    ['Abu Dhabi Grand Prix','Max Verstappen','Red Bull','Max Verstappen','Lando Norris'],
  ],
};

// ─── DRIVER SEASON TRENDS (for Drivers page) ────────────────────────
// Keyed by driver name → [year, poles, podiums]
const DRIVER_TREND_RAW: Record<string, [number, number, number][]> = {
  'Lewis Hamilton': [
    [2007,6,12],[2008,7,10],[2009,4,5],[2010,1,8],[2011,1,6],[2012,1,6],
    [2013,5,5],[2014,7,16],[2015,11,17],[2016,12,17],[2017,11,13],
    [2018,11,17],[2019,5,17],[2020,10,14],[2021,2,17],[2022,0,9],
    [2023,0,6],[2024,1,6],
  ],
  'Michael Schumacher': [
    [1992,0,8],[1993,0,9],[1994,6,10],[1995,4,11],[1996,3,5],
    [1997,3,8],[1998,3,11],[1999,3,6],[2000,9,12],[2001,11,14],
    [2002,7,17],[2003,5,8],[2004,8,15],[2005,1,5],[2006,4,7],
    [2010,0,0],[2011,0,1],[2012,1,1],
  ],
  'Max Verstappen': [
    [2015,0,0],[2016,0,7],[2017,0,4],[2018,0,11],[2019,2,9],
    [2020,1,11],[2021,10,18],[2022,7,17],[2023,12,21],[2024,8,14],
  ],
};

const driverTrends = Object.entries(DRIVER_TREND_RAW).flatMap(
  ([name, data]) => data.map(([y, p, pd]) => ({
    full_name: name, season_year: y, season_poles: p, season_podiums: pd
  }))
);

// ─── CONSTRUCTOR SEASON TRENDS (for Constructors page) ──────────────
// Keyed by team → [year, wins, points]
const CONSTRUCTOR_TREND_RAW: Record<string, [number, number, number][]> = {
  'Ferrari': [
    [2010,5,396],[2011,1,375],[2012,3,400],[2013,2,354],[2014,0,216],
    [2015,3,428],[2016,0,398],[2017,5,522],[2018,6,571],[2019,3,504],
    [2020,0,131],[2021,0,323],[2022,4,554],[2023,1,406],[2024,5,652],
  ],
  'McLaren': [
    [2010,5,454],[2011,6,497],[2012,7,378],[2013,0,122],[2014,0,181],
    [2015,0,27],[2016,0,76],[2017,0,30],[2018,0,62],[2019,0,145],
    [2020,0,202],[2021,1,275],[2022,0,159],[2023,0,302],[2024,6,666],
  ],
  'Mercedes': [
    [2010,0,214],[2011,0,165],[2012,1,142],[2013,3,360],[2014,16,701],
    [2015,16,703],[2016,19,765],[2017,12,668],[2018,11,655],[2019,15,739],
    [2020,13,573],[2021,9,613],[2022,1,515],[2023,0,409],[2024,4,468],
  ],
  'Red Bull': [
    [2010,9,498],[2011,12,650],[2012,7,460],[2013,13,596],[2014,3,405],
    [2015,2,187],[2016,2,468],[2017,3,368],[2018,4,419],[2019,3,417],
    [2020,2,319],[2021,11,587],[2022,17,759],[2023,21,860],[2024,9,589],
  ],
};

const constructorTrends = Object.entries(CONSTRUCTOR_TREND_RAW).flatMap(
  ([name, data]) => data.map(([y, w, p]) => ({
    constructor_name: name, season_year: y, season_wins: w, season_points: p
  }))
);

// ─── COMPARE PAGE: DRIVER CAREER STATS (top 20) ─────────────────────

const COMPARE_DRIVERS = [
  { driver_key:1, full_name:'Kimi Räikkönen', nationality:'Finnish', debut_year:2001, final_year:2021, total_race_starts:353, total_wins:21, total_podiums:103, total_poles:18, total_points:1873, championship_titles:1, win_rate_pct:5.9 },
  { driver_key:2, full_name:'Lewis Hamilton', nationality:'British', debut_year:2007, final_year:2024, total_race_starts:337, total_wins:103, total_podiums:197, total_poles:104, total_points:4698, championship_titles:7, win_rate_pct:30.6 },
  { driver_key:3, full_name:'Fernando Alonso', nationality:'Spanish', debut_year:2001, final_year:2024, total_race_starts:385, total_wins:32, total_podiums:106, total_poles:22, total_points:2267, championship_titles:2, win_rate_pct:8.3 },
  { driver_key:4, full_name:'Rubens Barrichello', nationality:'Brazilian', debut_year:1993, final_year:2011, total_race_starts:326, total_wins:11, total_podiums:68, total_poles:14, total_points:658, championship_titles:0, win_rate_pct:3.4 },
  { driver_key:5, full_name:'Michael Schumacher', nationality:'German', debut_year:1991, final_year:2012, total_race_starts:307, total_wins:91, total_podiums:155, total_poles:68, total_points:1566, championship_titles:7, win_rate_pct:29.7 },
  { driver_key:6, full_name:'Jenson Button', nationality:'British', debut_year:2000, final_year:2017, total_race_starts:306, total_wins:15, total_podiums:50, total_poles:8, total_points:1235, championship_titles:1, win_rate_pct:4.9 },
  { driver_key:7, full_name:'Sebastian Vettel', nationality:'German', debut_year:2007, final_year:2022, total_race_starts:299, total_wins:53, total_podiums:122, total_poles:57, total_points:3098, championship_titles:4, win_rate_pct:17.7 },
  { driver_key:8, full_name:'Sergio Perez', nationality:'Mexican', debut_year:2011, final_year:2024, total_race_starts:279, total_wins:6, total_podiums:39, total_poles:3, total_points:1589, championship_titles:0, win_rate_pct:2.2 },
  { driver_key:9, full_name:'Felipe Massa', nationality:'Brazilian', debut_year:2002, final_year:2017, total_race_starts:269, total_wins:11, total_podiums:41, total_poles:16, total_points:1167, championship_titles:0, win_rate_pct:4.1 },
  { driver_key:10, full_name:'Daniel Ricciardo', nationality:'Australian', debut_year:2011, final_year:2024, total_race_starts:257, total_wins:8, total_podiums:32, total_poles:3, total_points:1311, championship_titles:0, win_rate_pct:3.1 },
  { driver_key:11, full_name:'David Coulthard', nationality:'British', debut_year:1994, final_year:2008, total_race_starts:246, total_wins:13, total_podiums:62, total_poles:12, total_points:535, championship_titles:0, win_rate_pct:5.3 },
  { driver_key:12, full_name:'Valtteri Bottas', nationality:'Finnish', debut_year:2013, final_year:2024, total_race_starts:243, total_wins:10, total_podiums:67, total_poles:20, total_points:1796, championship_titles:0, win_rate_pct:4.1 },
  { driver_key:13, full_name:'Mark Webber', nationality:'Australian', debut_year:2002, final_year:2013, total_race_starts:217, total_wins:9, total_podiums:42, total_poles:13, total_points:1047, championship_titles:0, win_rate_pct:4.1 },
  { driver_key:14, full_name:'Nico Rosberg', nationality:'German', debut_year:2006, final_year:2016, total_race_starts:207, total_wins:23, total_podiums:57, total_poles:30, total_points:1594, championship_titles:1, win_rate_pct:11.1 },
  { driver_key:15, full_name:'Max Verstappen', nationality:'Dutch', debut_year:2015, final_year:2024, total_race_starts:200, total_wins:62, total_podiums:108, total_poles:40, total_points:2862, championship_titles:4, win_rate_pct:31.0 },
  { driver_key:16, full_name:'Alain Prost', nationality:'French', debut_year:1980, final_year:1993, total_race_starts:199, total_wins:51, total_podiums:106, total_poles:33, total_points:798, championship_titles:4, win_rate_pct:25.6 },
  { driver_key:17, full_name:'Ayrton Senna', nationality:'Brazilian', debut_year:1984, final_year:1994, total_race_starts:161, total_wins:41, total_podiums:80, total_poles:65, total_points:614, championship_titles:3, win_rate_pct:25.5 },
  { driver_key:18, full_name:'Charles Leclerc', nationality:'Monegasque', debut_year:2018, final_year:2024, total_race_starts:143, total_wins:7, total_podiums:39, total_poles:24, total_points:1454, championship_titles:0, win_rate_pct:4.9 },
  { driver_key:19, full_name:'Lando Norris', nationality:'British', debut_year:2019, final_year:2024, total_race_starts:120, total_wins:4, total_podiums:28, total_poles:8, total_points:1001, championship_titles:0, win_rate_pct:3.3 },
  { driver_key:20, full_name:'George Russell', nationality:'British', debut_year:2019, final_year:2024, total_race_starts:120, total_wins:3, total_podiums:20, total_poles:5, total_points:843, championship_titles:0, win_rate_pct:2.5 },
];

// ─── COMPARE PAGE: SEASON WIN TRENDS ─────────────────────────────────
// [driverKey, year, wins]
const COMPARE_TREND_RAW: [number, number, number][] = [
  // Hamilton (key=2)
  [2,2007,4],[2,2008,5],[2,2009,2],[2,2010,3],[2,2011,3],[2,2012,4],
  [2,2013,1],[2,2014,11],[2,2015,10],[2,2016,10],[2,2017,9],[2,2018,11],
  [2,2019,11],[2,2020,11],[2,2021,8],[2,2022,0],[2,2023,0],[2,2024,2],
  // Schumacher (key=5)
  [5,1994,8],[5,1995,9],[5,1996,3],[5,1997,5],[5,1998,6],[5,1999,2],
  [5,2000,9],[5,2001,9],[5,2002,11],[5,2003,6],[5,2004,13],[5,2005,1],
  [5,2006,0],[5,2010,0],[5,2012,0],
  // Verstappen (key=15)
  [15,2016,1],[15,2017,2],[15,2018,2],[15,2019,3],[15,2020,2],
  [15,2021,10],[15,2022,15],[15,2023,19],[15,2024,9],
  // Vettel (key=7)
  [7,2008,1],[7,2009,4],[7,2010,5],[7,2011,11],[7,2012,5],[7,2013,13],
  [7,2014,0],[7,2015,3],[7,2017,5],[7,2018,5],[7,2019,1],[7,2020,0],
  // Alonso (key=3)
  [3,2003,1],[3,2005,7],[3,2006,7],[3,2008,2],[3,2010,5],[3,2011,1],
  [3,2012,3],[3,2013,2],[3,2014,0],[3,2021,0],[3,2023,0],[3,2024,0],
  // Räikkönen (key=1)
  [1,2003,1],[1,2004,1],[1,2005,7],[1,2007,6],[1,2008,2],[1,2009,1],
  [1,2012,1],[1,2013,1],[1,2018,1],[1,2021,0],
  // Rosberg (key=14)
  [14,2012,1],[14,2013,2],[14,2014,5],[14,2015,6],[14,2016,9],
  // Bottas (key=12)
  [12,2017,3],[12,2018,0],[12,2019,4],[12,2020,2],[12,2021,1],
  // Perez (key=8)
  [8,2020,1],[8,2021,1],[8,2022,2],[8,2023,2],[8,2024,0],
  // Ricciardo (key=10)
  [10,2014,3],[10,2016,1],[10,2017,1],[10,2018,2],[10,2021,1],
  // Button (key=6)
  [6,2006,1],[6,2009,6],[6,2010,2],[6,2011,3],[6,2012,3],
  // Leclerc (key=18)
  [18,2019,2],[18,2022,3],[18,2024,3],
  // Norris (key=19)
  [19,2024,4],
  // Russell (key=20)
  [20,2022,1],[20,2024,2],
  // Webber (key=13)
  [13,2009,2],[13,2010,4],[13,2011,1],[13,2012,2],
  // Massa (key=9)
  [9,2006,2],[9,2007,3],[9,2008,2],
  // Prost (key=16)
  [16,1981,3],[16,1983,4],[16,1984,7],[16,1985,5],[16,1986,4],
  [16,1988,7],[16,1989,4],[16,1990,5],[16,1993,7],
  // Senna (key=17)
  [17,1985,2],[17,1988,8],[17,1989,6],[17,1990,6],[17,1991,7],[17,1993,5],
  // Coulthard (key=11)
  [11,1997,2],[11,1999,2],[11,2000,1],[11,2001,2],[11,2003,1],
  // Barrichello (key=4)
  [4,2000,1],[4,2002,4],[4,2003,2],[4,2004,1],[4,2009,2],
];

const compareTrends = COMPARE_TREND_RAW.map(([dk, y, w]) => ({
  driver_key: dk, season_year: y, season_wins: w
}));

// ─── RECORDS ─────────────────────────────────────────────────────────

const RECORDS = [
  // Championship Records
  { record_category:'Most Championship Titles', record_type:'Driver Championships', holder_name:'Michael Schumacher', record_value_formatted:'7', context_note:'Tied with Hamilton' },
  { record_category:'Most Championship Titles', record_type:'Constructor Championships', holder_name:'Ferrari', record_value_formatted:'16', context_note:'1961–2008' },
  { record_category:'Most Championship Titles', record_type:'Consecutive Driver Titles', holder_name:'Michael Schumacher', record_value_formatted:'5', context_note:'2000–2004' },
  { record_category:'Most Championship Titles', record_type:'Consecutive Constructor Titles', holder_name:'Mercedes', record_value_formatted:'8', context_note:'2014–2021' },
  // Race Records
  { record_category:'Most Race Wins', record_type:'Total Wins', holder_name:'Lewis Hamilton', record_value_formatted:'103', context_note:null },
  { record_category:'Most Race Wins', record_type:'Constructor All-Time Wins', holder_name:'Ferrari', record_value_formatted:'243', context_note:'Since 1950' },
  { record_category:'Most Wins in a Season', record_type:'Season Wins', holder_name:'Max Verstappen', record_value_formatted:'19', context_note:'2023 Season' },
  { record_category:'Most Wins in a Season', record_type:'Consecutive Wins', holder_name:'Max Verstappen', record_value_formatted:'10', context_note:'2023' },
  { record_category:'Youngest Race Winner', record_type:'Youngest Winner', holder_name:'Max Verstappen', record_value_formatted:'18y 228d', context_note:'2016 Spanish GP' },
  { record_category:'Most Podiums', record_type:'Career Podiums', holder_name:'Lewis Hamilton', record_value_formatted:'197', context_note:null },
  { record_category:'Most Podiums', record_type:'Podiums in a Season', holder_name:'Max Verstappen', record_value_formatted:'21', context_note:'2023 Season' },
  { record_category:'Highest Win Rate', record_type:'Win Percentage', holder_name:'J.M. Fangio', record_value_formatted:'46.2%', context_note:'Min 50 starts' },
  { record_category:'Highest Win Rate', record_type:'Modern Era Win Rate', holder_name:'Max Verstappen', record_value_formatted:'31.0%', context_note:'200+ starts' },
  // Qualifying & Points Records
  { record_category:'Most Pole Positions', record_type:'Career Poles', holder_name:'Lewis Hamilton', record_value_formatted:'104', context_note:null },
  { record_category:'Most Pole Positions', record_type:'Poles in a Season', holder_name:'Sebastian Vettel', record_value_formatted:'15', context_note:'2011 Season' },
  { record_category:'Most Fastest Laps', record_type:'Career Fastest Laps', holder_name:'Michael Schumacher', record_value_formatted:'77', context_note:null },
  { record_category:'Most Fastest Laps', record_type:'FL in a Season', holder_name:'Michael Schumacher', record_value_formatted:'10', context_note:'2004 Season' },
  { record_category:'Most Points in a Season', record_type:'Season Points (Driver)', holder_name:'Max Verstappen', record_value_formatted:'575', context_note:'2023 Season' },
  { record_category:'Most Points in a Season', record_type:'Season Points (Constructor)', holder_name:'Red Bull', record_value_formatted:'860', context_note:'2023 Season' },
  { record_category:'Most Race Starts', record_type:'Career Starts', holder_name:'Kimi Räikkönen', record_value_formatted:'353', context_note:'2001–2021' },
  { record_category:'Most Race Starts', record_type:'Consecutive Starts', holder_name:'Kimi Räikkönen', record_value_formatted:'230', context_note:null },
];

// ─── STATIC PAGE DATA ────────────────────────────────────────────────

// Season list (descending) — pre-built from champions
const seasonList = CHAMPIONS.map(c => ({ season_year: c[0] })).reverse();

// Overview charts
const titlesChart = [
  { label:'Hamilton', value:7 }, { label:'Schumacher', value:7 },
  { label:'Fangio', value:5 }, { label:'Prost', value:4 },
  { label:'Vettel', value:4 }, { label:'Verstappen', value:4 },
  { label:'Lauda', value:3 }, { label:'Senna', value:3 },
  { label:'Stewart', value:3 }, { label:'Piquet', value:3 },
  { label:'Brabham', value:3 }, { label:'Alonso', value:2 },
];

const nationalityChart = [
  { label:'British', value:20 }, { label:'German', value:9 },
  { label:'Brazilian', value:8 }, { label:'French', value:6 },
  { label:'Argentine', value:5 }, { label:'Finnish', value:3 },
  { label:'Austrian', value:3 }, { label:'Other', value:7 },
];

const decadesChart = [
  { label:'1950s', value:85 }, { label:'1960s', value:100 },
  { label:'1970s', value:147 }, { label:'1980s', value:163 },
  { label:'1990s', value:165 }, { label:'2000s', value:174 },
  { label:'2010s', value:210 }, { label:'2020s', value:119 },
];

// Drivers page
const winsData = [
  { label:'Lewis Hamilton', value:103 }, { label:'Michael Schumacher', value:91 },
  { label:'Max Verstappen', value:62 }, { label:'Sebastian Vettel', value:53 },
  { label:'Alain Prost', value:51 }, { label:'Ayrton Senna', value:41 },
  { label:'Fernando Alonso', value:32 }, { label:'Nigel Mansell', value:31 },
  { label:'Jackie Stewart', value:27 }, { label:'Jim Clark', value:25 },
  { label:'Niki Lauda', value:25 }, { label:'Juan Manuel Fangio', value:24 },
  { label:'Nelson Piquet', value:23 }, { label:'Nico Rosberg', value:23 },
  { label:'Damon Hill', value:22 },
];

const polesData = [
  { label:'Lewis Hamilton', value:104 }, { label:'Michael Schumacher', value:68 },
  { label:'Ayrton Senna', value:65 }, { label:'Sebastian Vettel', value:57 },
  { label:'Max Verstappen', value:40 }, { label:'Jim Clark', value:33 },
  { label:'Alain Prost', value:33 }, { label:'Nigel Mansell', value:32 },
  { label:'Nico Rosberg', value:30 }, { label:'Juan Manuel Fangio', value:29 },
  { label:'Charles Leclerc', value:24 }, { label:'Niki Lauda', value:24 },
  { label:'Nelson Piquet', value:24 }, { label:'Fernando Alonso', value:22 },
  { label:'Valtteri Bottas', value:20 },
];

const pointsData = [
  { label:'Lewis Hamilton', value:4698 }, { label:'Sebastian Vettel', value:3098 },
  { label:'Max Verstappen', value:2862 }, { label:'Fernando Alonso', value:2267 },
  { label:'Kimi Räikkönen', value:1873 }, { label:'Valtteri Bottas', value:1796 },
  { label:'Nico Rosberg', value:1594 }, { label:'Sergio Perez', value:1589 },
  { label:'Michael Schumacher', value:1566 }, { label:'Charles Leclerc', value:1454 },
  { label:'Daniel Ricciardo', value:1311 }, { label:'Jenson Button', value:1235 },
  { label:'Felipe Massa', value:1167 }, { label:'Mark Webber', value:1047 },
  { label:'Lando Norris', value:1001 },
];

const podiumsData = [
  { label:'Lewis Hamilton', value:197 }, { label:'Michael Schumacher', value:155 },
  { label:'Sebastian Vettel', value:122 }, { label:'Max Verstappen', value:108 },
  { label:'Alain Prost', value:106 }, { label:'Fernando Alonso', value:106 },
  { label:'Kimi Räikkönen', value:103 }, { label:'Ayrton Senna', value:80 },
  { label:'Rubens Barrichello', value:68 }, { label:'Valtteri Bottas', value:67 },
  { label:'David Coulthard', value:62 }, { label:'Nelson Piquet', value:60 },
  { label:'Nigel Mansell', value:59 }, { label:'Nico Rosberg', value:57 },
  { label:'Niki Lauda', value:54 },
];

const winRateData = [
  { full_name:'Juan Manuel Fangio', nationality:'Argentine', win_rate_pct:46.2, total_wins:24, total_race_starts:52 },
  { full_name:'Jim Clark', nationality:'British', win_rate_pct:34.2, total_wins:25, total_race_starts:73 },
  { full_name:'Max Verstappen', nationality:'Dutch', win_rate_pct:31.0, total_wins:62, total_race_starts:200 },
  { full_name:'Lewis Hamilton', nationality:'British', win_rate_pct:30.6, total_wins:103, total_race_starts:337 },
  { full_name:'Michael Schumacher', nationality:'German', win_rate_pct:29.7, total_wins:91, total_race_starts:307 },
  { full_name:'Alain Prost', nationality:'French', win_rate_pct:25.6, total_wins:51, total_race_starts:199 },
  { full_name:'Ayrton Senna', nationality:'Brazilian', win_rate_pct:25.5, total_wins:41, total_race_starts:161 },
  { full_name:'Jackie Stewart', nationality:'British', win_rate_pct:23.1, total_wins:27, total_race_starts:117 },
  { full_name:'Sebastian Vettel', nationality:'German', win_rate_pct:17.7, total_wins:53, total_race_starts:299 },
  { full_name:'Niki Lauda', nationality:'Austrian', win_rate_pct:14.6, total_wins:25, total_race_starts:171 },
];

const leaderboardData = [
  { full_name:'Lewis Hamilton', nationality:'British', total_wins:103, total_podiums:197, championship_titles:7 },
  { full_name:'Michael Schumacher', nationality:'German', total_wins:91, total_podiums:155, championship_titles:7 },
  { full_name:'Max Verstappen', nationality:'Dutch', total_wins:62, total_podiums:108, championship_titles:4 },
  { full_name:'Sebastian Vettel', nationality:'German', total_wins:53, total_podiums:122, championship_titles:4 },
  { full_name:'Alain Prost', nationality:'French', total_wins:51, total_podiums:106, championship_titles:4 },
  { full_name:'Ayrton Senna', nationality:'Brazilian', total_wins:41, total_podiums:80, championship_titles:3 },
  { full_name:'Fernando Alonso', nationality:'Spanish', total_wins:32, total_podiums:106, championship_titles:2 },
  { full_name:'Nigel Mansell', nationality:'British', total_wins:31, total_podiums:59, championship_titles:1 },
  { full_name:'Jackie Stewart', nationality:'British', total_wins:27, total_podiums:43, championship_titles:3 },
  { full_name:'Jim Clark', nationality:'British', total_wins:25, total_podiums:32, championship_titles:2 },
];

// Constructors page
const constructorWinsData = [
  { label:'Ferrari', value:243 }, { label:'McLaren', value:183 },
  { label:'Mercedes', value:125 }, { label:'Williams', value:114 },
  { label:'Red Bull', value:113 }, { label:'Lotus', value:79 },
  { label:'Brabham', value:35 }, { label:'Renault', value:35 },
  { label:'Benetton', value:27 }, { label:'Tyrrell', value:23 },
];

const constructorTitlesData = [
  { label:'Ferrari', value:16 }, { label:'Williams', value:9 },
  { label:'McLaren', value:8 }, { label:'Mercedes', value:8 },
  { label:'Lotus', value:7 }, { label:'Red Bull', value:6 },
  { label:'Brabham', value:2 }, { label:'Renault', value:2 },
  { label:'Cooper', value:2 }, { label:'Benetton', value:1 },
];

const winShareData = [
  { label:'Ferrari', value:243 }, { label:'McLaren', value:183 },
  { label:'Mercedes', value:125 }, { label:'Williams', value:114 },
  { label:'Red Bull', value:113 }, { label:'Others', value:334 },
];

// Circuits page
const hostedData = [
  { label:'Monza', value:73 }, { label:'Monaco', value:70 },
  { label:'Silverstone', value:58 }, { label:'Spa-Francorchamps', value:56 },
  { label:'Circuit Gilles Villeneuve', value:42 }, { label:'Nürburgring', value:41 },
  { label:'Interlagos', value:40 }, { label:'Hungaroring', value:38 },
  { label:'Hockenheimring', value:37 }, { label:'Catalunya', value:33 },
  { label:'Zandvoort', value:33 }, { label:'Suzuka', value:33 },
  { label:'Imola', value:30 }, { label:'Albert Park', value:26 },
  { label:'Red Bull Ring', value:19 },
];

const countryData = [
  { label:'Italy', value:104 }, { label:'Germany', value:79 },
  { label:'United Kingdom', value:74 }, { label:'USA', value:73 },
  { label:'Monaco', value:70 }, { label:'Belgium', value:68 },
  { label:'France', value:62 }, { label:'Spain', value:53 },
];

const topCircuitsData = [
  { circuit_name:'Monza', country:'Italy', total_races_held:73, most_wins_driver:'Michael Schumacher' },
  { circuit_name:'Monaco', country:'Monaco', total_races_held:70, most_wins_driver:'Ayrton Senna' },
  { circuit_name:'Silverstone', country:'United Kingdom', total_races_held:58, most_wins_driver:'Lewis Hamilton' },
  { circuit_name:'Spa-Francorchamps', country:'Belgium', total_races_held:56, most_wins_driver:'Michael Schumacher' },
  { circuit_name:'Circuit Gilles Villeneuve', country:'Canada', total_races_held:42, most_wins_driver:'Michael Schumacher' },
  { circuit_name:'Nürburgring', country:'Germany', total_races_held:41, most_wins_driver:'Michael Schumacher' },
  { circuit_name:'Interlagos', country:'Brazil', total_races_held:40, most_wins_driver:'Alain Prost' },
  { circuit_name:'Hungaroring', country:'Hungary', total_races_held:38, most_wins_driver:'Lewis Hamilton' },
];

const fastestLapsData = [
  { full_name:'Michael Schumacher', total_fastest_laps:77 },
  { full_name:'Lewis Hamilton', total_fastest_laps:65 },
  { full_name:'Kimi Räikkönen', total_fastest_laps:46 },
  { full_name:'Alain Prost', total_fastest_laps:41 },
  { full_name:'Sebastian Vettel', total_fastest_laps:38 },
  { full_name:'Max Verstappen', total_fastest_laps:33 },
  { full_name:'Nigel Mansell', total_fastest_laps:30 },
  { full_name:'Fernando Alonso', total_fastest_laps:23 },
  { full_name:'Jim Clark', total_fastest_laps:28 },
  { full_name:'Mika Häkkinen', total_fastest_laps:25 },
];


// ═══════════════════════════════════════════════════════════════════════
// QUERY DISPATCHER — table-based routing + pattern matching
// ═══════════════════════════════════════════════════════════════════════

export function getFallbackData(sql: string): any[] {
  const q = sql.toUpperCase();

  // ── SPECIFIC PATTERNS (multi-table JOINs, CTEs) — check first ──

  // Overview: championship titles chart (JOINs driver_career + dim_driver)
  if (q.includes('D.FULL_NAME AS LABEL') && q.includes('CHAMPIONSHIP_TITLES')) return titlesChart;
  // Overview: nationality chart
  if (q.includes('D.NATIONALITY AS LABEL') && q.includes('COUNT(*)')) return nationalityChart;
  // Overview: races per decade
  if (q.includes('FLOOR(SEASON_YEAR / 10) * 10')) return decadesChart;
  // Constructors: win share CTE
  if (q.includes('TOP_WINS AS')) return winShareData;
  // Constructor stat cards (unique column aliases)
  if (q.includes('FERRARI_TITLES')) return [{ ferrari_titles: 16 }];
  if (q.includes('FERRARI_WINS')) return [{ ferrari_wins: 243 }];
  if (q.includes('MERCEDES_TITLES')) return [{ mercedes_titles: 8 }];
  if (q.includes('REDBULL_TITLES')) return [{ redbull_titles: 6 }];

  // ── TABLE DISPATCH ──

  // MART_RECORDS
  if (q.includes('MART_RECORDS')) return RECORDS;

  // MART_SEASON_SUMMARY (dynamic year)
  if (q.includes('MART_SEASON_SUMMARY')) {
    const year = extractYear(q);
    const champ = year ? CHAMPION_MAP.get(year) : null;
    return champ ? [champ] : [CHAMPION_MAP.get(2024)!];
  }

  // MART_RACE_SUMMARY (dynamic year)
  if (q.includes('MART_RACE_SUMMARY')) {
    const year = extractYear(q);
    if (year && RACES[year]) {
      return RACES[year].map(([n, w, t, p, f]) => ({
        race_name: n, winning_driver: w, winning_constructor: t,
        pole_sitter_name: p, fastest_lap_driver: f,
      }));
    }
    // Fallback for years without detailed race data
    const champ = year ? CHAMPION_MAP.get(year) : null;
    if (champ) {
      return [{ race_name: `${year} Grand Prix`, winning_driver: champ.driver_champion,
        winning_constructor: champ.constructor_champion, pole_sitter_name: champ.driver_champion,
        fastest_lap_driver: champ.driver_champion }];
    }
    return [];
  }

  // MART_CIRCUIT_STATS
  if (q.includes('MART_CIRCUIT_STATS')) {
    if (q.includes('GROUP BY COUNTRY')) return countryData;
    if (q.includes('AS LABEL')) return hostedData;
    return topCircuitsData;
  }

  // MART_DRIVER_SEASON (multiple consumers — order matters!)
  if (q.includes('MART_DRIVER_SEASON')) {
    // Seasons page: standings by year
    if (q.includes('SEASON_POINTS DESC')) {
      const year = extractYear(q);
      if (year && STANDINGS[year]) {
        return STANDINGS[year].map(([n, p, w]) => ({
          driver_name: n, season_points: p, season_wins: w
        }));
      }
      // Fallback: generate minimal standings from champions lookup
      const champ = year ? CHAMPION_MAP.get(year) : null;
      if (champ) {
        return [
          { driver_name: champ.driver_champion, season_points: 100, season_wins: 5 },
          { driver_name: 'Runner-up', season_points: 70, season_wins: 2 },
          { driver_name: 'Third Place', season_points: 50, season_wins: 1 },
        ];
      }
      // Default to 2024 standings
      return STANDINGS[2024].map(([n, p, w]) => ({ driver_name: n, season_points: p, season_wins: w }));
    }
    // Compare page: season wins by driver_key
    if (q.includes('DRIVER_KEY IN')) return compareTrends;
    // Drivers page: season trends by full_name
    if (q.includes('IN (')) return driverTrends;
    return [];
  }

  // MART_CONSTRUCTOR_SEASON
  if (q.includes('MART_CONSTRUCTOR_SEASON')) return constructorTrends;

  // MART_DRIVER_CAREER (many queries target this table)
  if (q.includes('MART_DRIVER_CAREER')) {
    // Compare page: full driver list by starts
    if (q.includes('TOTAL_RACE_STARTS DESC')) return COMPARE_DRIVERS;
    // Circuits page: fastest laps
    if (q.includes('TOTAL_FASTEST_LAPS DESC')) return fastestLapsData;
    // Drivers page: win rate table
    if (q.includes('WIN_RATE_PCT IS NOT NULL')) return winRateData;
    // Drivers page: leaderboard (championships + wins — check before generic wins)
    if (q.includes('CHAMPIONSHIP_TITLES') && q.includes('TOTAL_WINS DESC') && q.includes('LIMIT 10'))
      return leaderboardData;
    // Overview stat cards
    if (q.includes('MAX(TOTAL_WINS)'))
      return [{ max_wins: 103, driver_wins: 'Lewis Hamilton' }];
    if (q.includes('MAX(CHAMPIONSHIP_TITLES)'))
      return [{ max_champs: 7, driver_champs: 'Michael Schumacher' }];
    if (q.includes('MAX(TOTAL_POLES)'))
      return [{ max_poles: 104, driver_poles: 'Lewis Hamilton' }];
    // Overview: total champions count
    if (q.includes('CHAMPIONSHIP_TITLES > 0') && q.includes('COUNT(*)'))
      return [{ total_champions: 34 }];
    // Drivers page: top 15 charts
    if (q.includes('TOTAL_WINS DESC') && q.includes('LIMIT 15')) return winsData;
    if (q.includes('TOTAL_POLES DESC') && q.includes('LIMIT 15')) return polesData;
    if (q.includes('TOTAL_POINTS DESC') && q.includes('LIMIT 15')) return pointsData;
    if (q.includes('TOTAL_PODIUMS DESC') && q.includes('LIMIT 15')) return podiumsData;
    return [];
  }

  // MART_CONSTRUCTOR_CAREER
  if (q.includes('MART_CONSTRUCTOR_CAREER')) {
    if (q.includes('MAX(TOTAL_WINS)'))
      return [{ max_cwins: 257, constructor_wins: 'Ferrari' }];
    if (q.includes('TOTAL_WINS DESC') && q.includes('LIMIT 10')) return constructorWinsData;
    if (q.includes('CONSTRUCTOR_TITLES DESC') && q.includes('LIMIT 10')) return constructorTitlesData;
    return [];
  }

  // DIM_ tables (overview page)
  if (q.includes('DIM_RACE')) {
    if (q.includes('DISTINCT')) return seasonList;
    if (q.includes('COUNT(*)')) return [{ total_races: 1112 }];
    return [];
  }
  if (q.includes('DIM_DRIVER') && q.includes('COUNT(*)')) return [{ total_drivers: 775 }];
  if (q.includes('DIM_CONSTRUCTOR') && q.includes('COUNT(*)')) return [{ total_constructors: 210 }];
  if (q.includes('DIM_CIRCUIT') && q.includes('COUNT(*)')) return [{ total_circuits: 77 }];

  // No match — return empty
  return [];
}
