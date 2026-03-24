// ═══════════════════════════════════════════════════════════
//  Nestlé Smart DMS — Backend (server.js)
//  Stack: Node.js · Express · pg (Neon PostgreSQL)
//  Deploy: Render  |  Start command: node server.js
// ═══════════════════════════════════════════════════════════

const express = require('express');
const { Pool } = require('pg');
const cors    = require('cors');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ── DB connection (Neon) ─────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ── Helpers ──────────────────────────────────────────────
function genId(prefix) {
  return prefix + '-' + Date.now().toString(36).toUpperCase();
}

async function notify(userId, title, message, type = 'info', refId = null) {
  await pool.query(
    `INSERT INTO "Notifications" ("userId", title, message, type, "refId")
     VALUES ($1,$2,$3,$4,$5)`,
    [userId, title, message, type, refId]
  );
}

// Resolve userId from role name (used internally)
async function getUsersByRole(role) {
  const r = await pool.query(`SELECT id FROM "Users" WHERE role = $1`, [role]);
  return r.rows.map(u => u.id);
}

// ════════════════════════════════════════════════════════
//  AUTH
// ════════════════════════════════════════════════════════
app.post('/api/login', async (req, res) => {
  const { email, password } = req.body;
  const r = await pool.query(
    `SELECT id, name, role, avatar FROM "Users" WHERE email=$1 AND password=$2`,
    [email, password]
  );
  if (!r.rows.length) return res.status(401).json({ error: 'Invalid credentials' });
  res.json(r.rows[0]);
});

// ════════════════════════════════════════════════════════
//  REFERENCE DATA
// ════════════════════════════════════════════════════════
app.get('/api/drivers', async (_req, res) => {
  const r = await pool.query(
    `SELECT id, name FROM "Users" WHERE role = 'distributor' ORDER BY name`
  );
  res.json(r.rows);
});

app.get('/api/vehicles', async (_req, res) => {
  const r = await pool.query(
    `SELECT id, plate, type, cap FROM "Vehicles" ORDER BY id`
  );
  res.json(r.rows);
});

// ════════════════════════════════════════════════════════
//  ORDERS
// ════════════════════════════════════════════════════════
app.get('/api/orders', async (req, res) => {
  const { role, userId } = req.query;
  let rows;
  if (role === 'retailer') {
    const r = await pool.query(
      `SELECT id, city, items, kg, priority AS prio, status,
              "rejectReason", "confirmedBy",
              TO_CHAR("createdAt", 'DD Mon HH24:MI') AS created
       FROM "Orders" WHERE "retailerId" = $1 ORDER BY "createdAt" DESC`,
      [userId]
    );
    rows = r.rows;
  } else {
    const r = await pool.query(
      `SELECT o.id, u.name AS retailer, o.city, o.items, o.kg,
              o.priority AS prio, o.status, o."rejectReason", o."confirmedBy",
              TO_CHAR(o."createdAt", 'DD Mon HH24:MI') AS created
       FROM "Orders" o
       JOIN "Users" u ON u.id = o."retailerId"
       ORDER BY o."createdAt" DESC`
    );
    rows = r.rows;
  }
  res.json(rows);
});

app.post('/api/orders', async (req, res) => {
  const { retailerId, retailerName, city, items, kg, priority, notes } = req.body;
  const id = genId('ORD');
  await pool.query(
    `INSERT INTO "Orders" (id, "retailerId", city, items, kg, priority, notes, status)
     VALUES ($1,$2,$3,$4,$5,$6,$7,'pending')`,
    [id, retailerId, city, items, kg, priority, notes || null]
  );
  // Notify all Order Team members
  const optUsers = await getUsersByRole('order_team');
  for (const uid of optUsers) {
    await notify(uid,
      '📦 New Order Request',
      `${retailerName} in ${city} submitted ${items} items (${kg} kg) — priority: ${priority}`,
      'info', id
    );
  }
  res.json({ id });
});

app.put('/api/orders/:id/confirm', async (req, res) => {
  const { id } = req.params;
  const { action, confirmedBy, rejectReason } = req.body;
  const newStatus = action === 'confirm' ? 'confirmed' : 'rejected';

  await pool.query(
    `UPDATE "Orders" SET status=$1, "confirmedBy"=$2, "rejectReason"=$3 WHERE id=$4`,
    [newStatus, confirmedBy, rejectReason || null, id]
  );

  // Notify the retailer
  const order = await pool.query(`SELECT "retailerId", city FROM "Orders" WHERE id=$1`, [id]);
  if (order.rows.length) {
    const { retailerId, city } = order.rows[0];
    if (action === 'confirm') {
      await notify(retailerId,
        '✅ Order Confirmed',
        `Your order ${id} (${city}) has been confirmed by the Order Team.`,
        'success', id
      );
    } else {
      await notify(retailerId,
        '❌ Order Rejected',
        `Your order ${id} was rejected. Reason: ${rejectReason || 'No reason given'}.`,
        'alert', id
      );
    }
  }
  res.json({ ok: true });
});

// ════════════════════════════════════════════════════════
//  DELIVERIES
// ════════════════════════════════════════════════════════
app.get('/api/deliveries', async (req, res) => {
  const { role, userId } = req.query;
  let query, params = [];

  if (role === 'distributor') {
    query = `
      SELECT d.id, u.name AS retailer, d.city, d.items, d.kg,
             d.priority AS prio, d.status, d."driverId", d."driverName",
             d."vehicleId", TO_CHAR(d.eta, 'HH24:MI') AS eta
      FROM "Deliveries" d
      JOIN "Orders" o ON o.id = d."orderId"
      JOIN "Users" u  ON u.id = o."retailerId"
      WHERE d."driverId" = $1
      ORDER BY d."createdAt" DESC`;
    params = [userId];
  } else {
    query = `
      SELECT d.id, u.name AS retailer, d.city, d.items, d.kg,
             d.priority AS prio, d.status, d."driverId", d."driverName",
             d."vehicleId", TO_CHAR(d.eta, 'HH24:MI') AS eta
      FROM "Deliveries" d
      JOIN "Orders" o ON o.id = d."orderId"
      JOIN "Users" u  ON u.id = o."retailerId"
      ORDER BY d."createdAt" DESC`;
  }
  const r = await pool.query(query, params);
  res.json(r.rows);
});

app.post('/api/deliveries/consolidate', async (req, res) => {
  // Fetch all confirmed orders not yet consolidated
  const { rows: confirmed } = await pool.query(
    `SELECT * FROM "Orders" WHERE status = 'confirmed'`
  );

  if (!confirmed.length) return res.json({ created: 0, held: 0 });

  // Single low-priority order → hold
  if (confirmed.length === 1 && ['normal', 'low'].includes(confirmed[0].priority)) {
    return res.json({ created: 0, held: 1, reason: 'single_low_priority', orderId: confirmed[0].id });
  }

  let created = 0;
  const rpUsers  = await getUsersByRole('route_planner');
  const whUsers  = await getUsersByRole('warehouse');

  for (const order of confirmed) {
    const delId = genId('DEL');
    const eta   = new Date(Date.now() + 4 * 60 * 60 * 1000); // +4h from now
    await pool.query(
      `INSERT INTO "Deliveries"
         (id, "orderId", city, items, kg, priority, status, eta)
       VALUES ($1,$2,$3,$4,$5,$6,'pending',$7)`,
      [delId, order.id, order.city, order.items, order.kg, order.priority, eta]
    );
    await pool.query(`UPDATE "Orders" SET status='consolidated' WHERE id=$1`, [order.id]);
    created++;

    const summary = `${order.items} items, ${order.kg}kg to ${order.city}`;
    for (const uid of rpUsers)
      await notify(uid, '🗺️ New Delivery to Plan', `Delivery ${delId}: ${summary}`, 'info', delId);
    for (const uid of whUsers)
      await notify(uid, '📦 Prepare Outgoing Cargo', `Delivery ${delId}: ${summary} — prepare for dispatch`, 'info', delId);
  }
  res.json({ created });
});

app.put('/api/deliveries/:id/assign', async (req, res) => {
  const { id } = req.params;
  const { driverId, driverName, vehicleId } = req.body;
  const eta = new Date(Date.now() + 3 * 60 * 60 * 1000); // +3h

  await pool.query(
    `UPDATE "Deliveries"
     SET status='assigned', "driverId"=$1, "driverName"=$2, "vehicleId"=$3, eta=$4
     WHERE id=$5`,
    [driverId, driverName, vehicleId, eta, id]
  );

  const etaStr = eta.toTimeString().slice(0, 5);

  // Notify driver
  const del = await pool.query(`SELECT city, items, kg FROM "Deliveries" WHERE id=$1`, [id]);
  if (del.rows.length) {
    const { city, items, kg } = del.rows[0];
    await notify(driverId,
      '🚚 New Delivery Assigned',
      `You have been assigned delivery ${id} to ${city} (${items} items, ${kg}kg). ETA: ${etaStr}`,
      'info', id
    );
  }

  // Notify warehouse
  const whUsers = await getUsersByRole('warehouse');
  for (const uid of whUsers)
    await notify(uid,
      '🏭 Driver Assigned',
      `${driverName} assigned to delivery ${id}. Vehicle: ${vehicleId}. Prepare cargo.`,
      'info', id
    );

  res.json({ eta: etaStr });
});

app.put('/api/deliveries/:id/warehouse-ready', async (req, res) => {
  const { id } = req.params;
  await pool.query(`UPDATE "Deliveries" SET status='warehouse_ready' WHERE id=$1`, [id]);

  // Notify driver
  const r = await pool.query(`SELECT "driverId", city FROM "Deliveries" WHERE id=$1`, [id]);
  if (r.rows[0]?.driverId) {
    await notify(r.rows[0].driverId,
      '✅ Cargo Ready for Pickup',
      `Delivery ${id} to ${r.rows[0].city} is packed and ready. Please come to the warehouse.`,
      'success', id
    );
  }
  res.json({ ok: true });
});

app.put('/api/deliveries/:id/loaded', async (req, res) => {
  const { id } = req.params;
  await pool.query(`UPDATE "Deliveries" SET status='loaded' WHERE id=$1`, [id]);

  // Notify driver
  const r = await pool.query(`SELECT "driverId", city FROM "Deliveries" WHERE id=$1`, [id]);
  if (r.rows[0]?.driverId) {
    await notify(r.rows[0].driverId,
      '📦 Vehicle Loaded — Confirm Pickup',
      `Your vehicle is loaded for delivery ${id} to ${r.rows[0].city}. Confirm pickup and depart.`,
      'success', id
    );
  }
  res.json({ ok: true });
});

app.put('/api/deliveries/:id/status', async (req, res) => {
  const { id } = req.params;
  const { newStatus, note } = req.body;

  await pool.query(
    `UPDATE "Deliveries" SET status=$1, note=$2 WHERE id=$3`,
    [newStatus, note || null, id]
  );

  // Get delivery + order + retailer info
  const r = await pool.query(
    `SELECT d.city, d.items, d."driverId", o."retailerId"
     FROM "Deliveries" d
     JOIN "Orders" o ON o.id = d."orderId"
     WHERE d.id = $1`,
    [id]
  );
  if (!r.rows.length) return res.json({ ok: true });
  const { city, items, retailerId } = r.rows[0];

  const icons   = { 'in-transit': '🚚', delivered: '✅', failed: '❌' };
  const types   = { 'in-transit': 'info', delivered: 'success', failed: 'alert' };
  const icon    = icons[newStatus]  || '📋';
  const type    = types[newStatus]  || 'info';
  const noteStr = note ? ` — ${note}` : '';

  // Notify retailer
  await notify(retailerId,
    `${icon} Delivery ${newStatus.replace('-', ' ')}`,
    `Your delivery ${id} (${items} items to ${city}) is now ${newStatus}${noteStr}`,
    type, id
  );
  // Notify OPT
  const optUsers = await getUsersByRole('order_team');
  for (const uid of optUsers)
    await notify(uid,
      `${icon} Delivery ${newStatus.replace('-', ' ')}`,
      `Delivery ${id} to ${city} — status: ${newStatus}${noteStr}`,
      type, id
    );
  // Notify warehouse
  const whUsers = await getUsersByRole('warehouse');
  for (const uid of whUsers)
    await notify(uid,
      `${icon} Delivery Update`,
      `Delivery ${id} to ${city} is now ${newStatus}${noteStr}`,
      type, id
    );

  res.json({ ok: true });
});

// ════════════════════════════════════════════════════════
//  ROUTES
// ════════════════════════════════════════════════════════
app.get('/api/routes', async (_req, res) => {
  const r = await pool.query(
    `SELECT id, name, "driverId", "driverName", "vehicleId",
            stops, "distKm", "durMins", cities,
            TO_CHAR("createdAt", 'DD Mon HH24:MI') AS created
     FROM "Routes" ORDER BY "createdAt" DESC`
  );
  res.json(r.rows);
});

app.post('/api/routes', async (req, res) => {
  const { driverId, driverName, vehicleId, stops, distKm, durMins, cities } = req.body;
  const id   = genId('RTE');
  const name = `Route – ${driverName} (${stops} stops)`;
  await pool.query(
    `INSERT INTO "Routes"
       (id, name, "driverId", "driverName", "vehicleId", stops, "distKm", "durMins", cities)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [id, name, driverId, driverName, vehicleId, stops, distKm, durMins, JSON.stringify(cities)]
  );
  res.json({ id });
});

// ════════════════════════════════════════════════════════
//  NOTIFICATIONS
// ════════════════════════════════════════════════════════
app.get('/api/notifications', async (req, res) => {
  const { userId } = req.query;
  const r = await pool.query(
    `SELECT id, title, message, type, "isRead", "refId",
            TO_CHAR("createdAt", 'DD Mon HH24:MI') AS time
     FROM "Notifications"
     WHERE "userId" = $1
     ORDER BY "createdAt" DESC
     LIMIT 60`,
    [userId]
  );
  res.json(r.rows);
});

app.get('/api/notifications/unread', async (req, res) => {
  const { userId } = req.query;
  const r = await pool.query(
    `SELECT COUNT(*)::int AS count FROM "Notifications"
     WHERE "userId" = $1 AND "isRead" = FALSE`,
    [userId]
  );
  res.json({ count: r.rows[0].count });
});

app.put('/api/notifications/:id/read', async (req, res) => {
  await pool.query(
    `UPDATE "Notifications" SET "isRead" = TRUE WHERE id = $1`,
    [req.params.id]
  );
  res.json({ ok: true });
});

app.put('/api/notifications/read-all', async (req, res) => {
  const { userId } = req.body;
  await pool.query(
    `UPDATE "Notifications" SET "isRead" = TRUE WHERE "userId" = $1`,
    [userId]
  );
  res.json({ ok: true });
});

// ════════════════════════════════════════════════════════
//  START
// ════════════════════════════════════════════════════════
app.listen(PORT, () => console.log(`Nestlé DMS API running on port ${PORT}`));