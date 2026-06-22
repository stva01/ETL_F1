import duckdb from 'duckdb';

const db = new duckdb.Database('../warehouse/dbt/f1_analytics.duckdb', duckdb.OPEN_READONLY);

function run(query: string) {
  return new Promise((resolve) => {
    db.all(query, (err, res) => {
      if (err) console.error("Error on query:", query, "\n", err);
      else console.log("Success on query:", query, "\n", res);
      resolve(true);
    });
  });
}

async function test() {
  await run(`SELECT COUNT(*) as total_races FROM main_marts.dim_race`);
  await run(`SELECT COUNT(*) as total_champions FROM main_marts.mart_driver_career WHERE championship_titles > 0`);
  await run(`SELECT COUNT(*) as total_drivers FROM main_marts.dim_driver`);
  await run(`SELECT COUNT(*) as total_constructors FROM main_marts.dim_constructor`);
  await run(`SELECT COUNT(*) as total_circuits FROM main_marts.dim_circuit`);
  await run(`
    SELECT m.total_race_wins as hamilton_wins 
    FROM main_marts.mart_driver_career m
    JOIN main_marts.dim_driver d ON m.driver_key = d.driver_key
    WHERE d.driver_ref = 'hamilton'
  `);
}

test();
