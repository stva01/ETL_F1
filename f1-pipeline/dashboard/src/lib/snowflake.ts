import snowflake from 'snowflake-sdk';
import { getFallbackData } from './mockFallback';

// Initialize connection options from environment variables
const connectionOptions = {
  account: process.env.SNOWFLAKE_ACCOUNT || '',
  username: process.env.SNOWFLAKE_USER || '',
  password: process.env.SNOWFLAKE_PASSWORD || '',
  database: process.env.SNOWFLAKE_DATABASE || '',
  schema: process.env.SNOWFLAKE_SCHEMA || '',
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || '',
  role: process.env.SNOWFLAKE_ROLE || '',
};

// Create a single connection pool or connection
// Note: In Next.js serverless functions, establishing connections can be slow.
// snowflake-sdk doesn't have a built-in connection pool standard for serverless, 
// so we'll create a standalone connection per query or reuse it if possible.
// For simplicity and to avoid connection leaks across hot-reloads in dev, 
// we will establish a connection on demand.

export async function querySnowflake<T = any>(query: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    // 👱‍♀️ ponytail: Fast-fail to mock data if feature flag is set
    if (process.env.USE_MOCK_DATA === 'true') {
      resolve(getFallbackData(query) as T[]);
      return;
    }

    const connection = snowflake.createConnection(connectionOptions);

    connection.connect((err, conn) => {
      if (err) {
        console.error('Unable to connect to Snowflake:', err.message);
        resolve(getFallbackData(query) as T[]);
        return;
      }

      conn.execute({
        sqlText: query,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error('Failed to execute statement due to the following error:', err.message);
            // Destroy connection on error to avoid leaks
            conn.destroy((destroyErr) => {
              if (destroyErr) {
                console.error('Failed to destroy connection:', destroyErr.message);
              }
            });
            resolve(getFallbackData(query) as T[]);
          } else {
            // Destroy connection when done
            conn.destroy((destroyErr) => {
              if (destroyErr) {
                console.error('Failed to destroy connection:', destroyErr.message);
              }
            });

            // Convert column names to lowercase to match DuckDB behavior
            // DuckDB query results usually have lowercase keys (e.g., total_wins)
            // Snowflake returns uppercase keys (e.g., TOTAL_WINS)
            const lowercaseRows = rows?.map(row => {
              const newRow: any = {};
              for (const [key, value] of Object.entries(row)) {
                newRow[key.toLowerCase()] = value;
              }
              return newRow;
            }) || [];

            // Fallback if db is completely empty
            if (lowercaseRows.length === 0) {
              resolve(getFallbackData(query) as T[]);
              return;
            }

            resolve(lowercaseRows as T[]);
          }
        }
      });
    });
  });
}
