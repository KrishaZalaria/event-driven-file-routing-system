import { Connector } from '@google-cloud/cloud-sql-connector';
import pkg from 'pg';
const { Pool } = pkg;

const connector = new Connector();

let pool;

export async function getDbPool() {
  if (pool) return pool;

  const clientOpts = await connector.getOptions({
    instanceConnectionName: process.env.INSTANCE_CONNECTION_NAME,
    ipType: 'PUBLIC', // We enabled public IP
  });

  pool = new Pool({
    ...clientOpts,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
  });

  return pool;
}