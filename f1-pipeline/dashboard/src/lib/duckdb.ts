import duckdb from 'duckdb';
import path from 'path';

// Path to the gold layer database
const DB_PATH = path.resolve(process.cwd(), '../warehouse/dbt/f1_analytics.duckdb');

// Create a single database connection instance
// READ_ONLY mode prevents locking issues and protects the data
const db = new duckdb.Database(DB_PATH, duckdb.OPEN_READONLY);

/**
 * Executes a query and returns the results as a Promise
 * @param query The SQL query to execute
 * @returns Array of result objects
 */
export async function queryDuckDB<T = any>(query: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    db.all(query, (err, res) => {
      if (err) {
        console.error('DuckDB Query Error:', err);
        reject(err);
      } else {
        resolve(res as T[]);
      }
    });
  });
}
