import express from 'express';
import cors from 'cors';
import multer from 'multer';
import csvParser from 'csv-parser';
import fs from 'node:fs';
import path from 'node:path';
import mysql from 'mysql2/promise';

const PORT = Number(process.env.PORT || 4000);
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || 'http://localhost:5173';
const UPLOAD_DIR = path.join(process.cwd(), 'uploads');

const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '12345',
  database: process.env.DB_NAME || 'inventory_db'
};

if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

const upload = multer({ dest: UPLOAD_DIR });
const app = express();

app.use(cors({ origin: FRONTEND_ORIGIN }));

const pool = mysql.createPool({
  ...DB_CONFIG,
  waitForConnections: true,
  connectionLimit: 10
});

async function ensureDatabase() {
  const admin = await mysql.createConnection({
    host: DB_CONFIG.host,
    port: DB_CONFIG.port,
    user: DB_CONFIG.user,
    password: DB_CONFIG.password
  });

  try {
    await admin.query(`CREATE DATABASE IF NOT EXISTS \`${DB_CONFIG.database}\``);
  } finally {
    await admin.end();
  }
}

async function ensureProductsTable() {
  await ensureDatabase();

  const sql = `
    CREATE TABLE IF NOT EXISTS products (
      sku VARCHAR(64) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      quantity INT,
      price DECIMAL(12, 2),
      reorder_level INT,
      status VARCHAR(32) DEFAULT 'active'
    ) ENGINE=InnoDB;
  `;

  const connection = await pool.getConnection();
  try {
    await connection.query(sql);
  } finally {
    connection.release();
  }
}

const ready = ensureProductsTable().catch((error) => {
  console.error('Database initialization failed:', error);
  process.exit(1);
});

const normalizeHeader = (header) => header.trim().toLowerCase().replace(/\s+/g, '_');

function parseCsv(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csvParser({ mapHeaders: ({ header }) => normalizeHeader(header) }))
      .on('data', (row) => rows.push(row))
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

const toNumber = (value) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const normalizeRow = (row) => {
  const sku = row.sku?.toString().trim();
  const name = row.name?.toString().trim();
  const status = row.status?.toString().trim();

  return {
    sku,
    name,
    quantity: toNumber(row.quantity),
    price: toNumber(row.price),
    reorder_level: toNumber(row.reorder_level),
    status: status || 'active'
  };
};

async function insertRows(rows) {
  const connection = await pool.getConnection();
  let inserted = 0;

  try {
    await connection.beginTransaction();

    for (const row of rows) {
      const [result] = await connection.query(
        `INSERT IGNORE INTO products (sku, name, quantity, price, reorder_level, status)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [row.sku, row.name, row.quantity, row.price, row.reorder_level, row.status]
      );

      inserted += result.affectedRows > 0 ? 1 : 0;
    }

    await connection.commit();
    return inserted;
  } catch (error) {
    await connection.rollback().catch(() => {});
    throw error;
  } finally {
    connection.release();
  }
}

async function handleUpload(req, res) {
  if (!req.file) {
    return res.status(400).json({ error: 'CSV file is required.' });
  }

  try {
    await ready;
    const rawRows = await parseCsv(req.file.path);
    const normalized = rawRows.map(normalizeRow).filter((row) => row.sku && row.name);

    if (!normalized.length) {
      return res.status(400).json({ error: 'No valid rows found in the CSV.' });
    }

    const inserted = await insertRows(normalized);

    res.json({
      message: 'File processed and data saved to DB',
      totalRows: rawRows.length,
      inserted
    });
  } catch (error) {
    console.error('Failed to process upload:', error);
    res.status(500).json({ error: 'Failed to process and store CSV data.' });
  } finally {
    await fs.promises.unlink(req.file.path).catch(() => {});
  }
}

app.post(['/api/upload', '/upload'], upload.single('file'), handleUpload);

app.use((req, res) => {
  res.status(404).json({ error: 'Not Found' });
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
