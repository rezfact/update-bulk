const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const mysql = require('mysql2/promise');
require('dotenv').config();

async function bulkUpdate(csvPath) {
    const apolloIds = [];

    // Read CSV
    await new Promise((resolve, reject) => {
        fs.createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row) => {
                if (row.apollo_id) {
                    apolloIds.push(row.apollo_id);
                }
            })
            .on('end', () => {
                console.log(`✅ Loaded ${apolloIds.length} rows from CSV.`);
                resolve();
            })
            .on('error', reject);
    });

    if (apolloIds.length === 0) {
        console.log("⚠️ No valid apollo_id found in CSV.");
        return;
    }

    // Connect to MySQL
    const connection = await mysql.createConnection({
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
    });

    try {
        // Chunk the data to avoid query size limits
        const chunkSize = 1000;
        for (let i = 0; i < apolloIds.length; i += chunkSize) {
            const chunk = apolloIds.slice(i, i + chunkSize);
            const placeholders = chunk.map(() => '?').join(',');
            const query = `UPDATE images SET active = 1 WHERE apollo_id IN (${placeholders})`;
            const [result] = await connection.execute(query, chunk);
            console.log(`✅ Updated ${result.affectedRows} rows (chunk ${i / chunkSize + 1})`);
        }
    } catch (err) {
        console.error("❌ MySQL Update Error:", err);
    } finally {
        await connection.end();
    }
}

bulkUpdate(path.join(__dirname, 'data.csv'));
