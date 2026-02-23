import dotenv from 'dotenv';
import { getDbPool } from './db.js';

dotenv.config();

async function testConnection() {
  try {
    const pool = await getDbPool();
    const result = await pool.query('SELECT NOW()');
    console.log('DB Connected:', result.rows[0]);
    process.exit(0);
  } catch (err) {
    console.error('DB Connection Failed:', err);
    process.exit(1);
  }
}

testConnection();