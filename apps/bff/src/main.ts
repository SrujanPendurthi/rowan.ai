import http from 'node:http';
import pg from 'pg';
import { createApp } from './app.ts';
import { loadConfig } from './config.ts';

const config = loadConfig(process.env);
const pool = new pg.Pool({ connectionString: config.databaseUrl });
pool.on('error', (error: Error) => {
  console.error(error);
});
const server = http.createServer(createApp({ pool, config }));
server.listen(config.port);
process.once('SIGTERM', () => {
  server.close(() => {
    void pool.end().catch((error: unknown) => {
      console.error(error);
      process.exitCode = 1;
    });
  });
});
