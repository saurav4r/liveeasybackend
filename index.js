const express = require('express');
const cors = require('cors');
const multer = require('multer');
const csv = require('csv-parser');
const { Readable } = require('stream');
const { Pool } = require('pg');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: 'Mysql1234@#',
  database: 'inventory_db'
});

async function parseCsv(buffer) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const stream = Readable.from(buffer.toString('utf-8'));
    stream
      .pipe(
        csv({
          mapHeaders: ({ header }) => header.trim().toLowerCase()
        })
      )
      .on('data', (data) => {
        const rawPrice = data.price?.toString().trim();
        const rawQuantity = data.quantity?.toString().trim();
        const parsedPrice = rawPrice ? parseFloat(rawPrice) : null;
        const parsedQuantity = rawQuantity ? parseInt(rawQuantity, 10) : null;
        rows.push({
          name: data.name?.trim(),
          price: Number.isNaN(parsedPrice) ? null : parsedPrice,
          quantity: Number.isNaN(parsedQuantity) ? null : parsedQuantity
        });
      })
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'CSV file is required.' });
  }

  try {
    const rows = await parseCsv(req.file.buffer);

    if (!rows.length) {
      return res.status(400).json({ error: 'CSV file is empty.' });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let insertedCount = 0;

      for (const row of rows) {
        if (!row.name) continue;
        await client.query(
          'INSERT INTO products (name, price, quantity) VALUES ($1, $2, $3)',
          [row.name, row.price, row.quantity]
        );
        insertedCount += 1;
      }

      await client.query('COMMIT');
      res.json({ message: 'CSV processed successfully.', inserted: insertedCount });
    } catch (dbError) {
      await client.query('ROLLBACK');
      console.error('Database error:', dbError);
      res.status(500).json({ error: 'Failed to save data to the database.' });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Processing error:', err);
    res.status(400).json({ error: `Failed to parse CSV file. ${err.message || ''}`.trim() });
  }
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
