// hash-passwords.js
// Run ONCE on Render shell or locally to migrate plain passwords to bcrypt hashes:
//   node hash-passwords.js
//
// Requires: DATABASE_URL env variable set (same as server.js)

const bcrypt = require('bcrypt');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  const users = await pool.query('SELECT id, "PasswordHash" FROM "Users"');
  
  for (const u of users.rows) {
    const plain = u.PasswordHash;
    
    // Skip if already hashed (bcrypt hashes start with $2b$)
    if (plain.startsWith('$2b$') || plain.startsWith('$2a$')) {
      console.log(`User ${u.id}: already hashed, skipping.`);
      continue;
    }
    
    const hash = await bcrypt.hash(plain, 10);
    await pool.query('UPDATE "Users" SET "PasswordHash"=$1 WHERE id=$2', [hash, u.id]);
    console.log(`User ${u.id}: "${plain}" → hashed ✅`);
  }
  
  console.log('\nDone! All passwords are now bcrypt hashed.');
  await pool.end();
}

run().catch(e => { console.error(e); process.exit(1); });
