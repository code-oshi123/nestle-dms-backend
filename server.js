const express = require('express');
const cors    = require('cors');
const bcrypt  = require('bcrypt');
const jwt     = require('jsonwebtoken');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const JWT_SECRET = process.env.JWT_SECRET || 'nestle-dms-secret-change-in-prod';

// ── Middleware: verify JWT token ──────────────
function auth(req, res, next) {
  const header = req.headers['authorization'];
  const token  = header && header.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token provided' });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// ── health check / keep-alive ─────────────────
app.get('/', (req, res) => res.json({ status: 'ok', service: 'Nestlé DMS API' }));

// ══════════════════════════════════════════════
// AUTH
// ══════════════════════════════════════════════
app.post('/api/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  try {
    const r = await pool.query('SELECT * FROM "Users" WHERE "Email"=$1', [email]);
    if (!r.rows.length) return res.status(401).json({ error: 'Invalid credentials' });
    const u = r.rows[0];
    const match = await bcrypt.compare(password, u.PasswordHash);
    if (!match) return res.status(401).json({ error: 'Invalid credentials' });
    const token = jwt.sign(
      { id: u.id, email: u.Email, role: u.role },
      JWT_SECRET,
      { expiresIn: '8h' }
    );
    res.json({ id: u.id, name: u.name, email: u.Email, role: u.role, avatar: u.avatar, token });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// NOTIFICATIONS
// ══════════════════════════════════════════════
app.get('/api/notifications', auth, async (req, res) => {
  const { userId } = req.query;
  try {
    const r = await pool.query(
      `SELECT id, title, message, type, "isRead", "refId",
       TO_CHAR("createdAt", 'HH24:MI DD Mon') AS time
       FROM "Notifications" WHERE "userId"=$1
       ORDER BY "createdAt" DESC LIMIT 50`,
      [userId]
    );
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/notifications/unread', auth, async (req, res) => {
  const { userId } = req.query;
  try {
    const r = await pool.query(
      'SELECT COUNT(*) FROM "Notifications" WHERE "userId"=$1 AND "isRead"=false',
      [userId]
    );
    res.json({ count: parseInt(r.rows[0].count) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/:id/read', auth, async (req, res) => {
  try {
    await pool.query('UPDATE "Notifications" SET "isRead"=true WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/read-all', auth, async (req, res) => {
  const { userId } = req.body;
  try {
    await pool.query('UPDATE "Notifications" SET "isRead"=true WHERE "userId"=$1', [userId]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

async function notify(userId, title, message, type='info', refId=null) {
  await pool.query(
    'INSERT INTO "Notifications"("userId","title","message","type","refId","isRead","createdAt") VALUES($1,$2,$3,$4,$5,false,NOW())',
    [userId, title, message, type, refId]
  );
}

// ══════════════════════════════════════════════
// REFERENCE DATA
// ══════════════════════════════════════════════
app.get('/api/drivers', auth, async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM "Drivers" ORDER BY name');
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vehicles', auth, async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM "Vehicles" ORDER BY id');
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Stock check helper ────────────────────────
// Checks against a "Stock" table (id, productName, availableUnits, availableKg)
// Falls back gracefully if table doesn't exist yet.
async function checkStock(items, kg) {
  try {
    const r = await pool.query(
      'SELECT "availableUnits", "availableKg" FROM "Stock" WHERE id=1'
    );
    if (!r.rows.length) return { ok: true }; // no stock table yet — allow
    const s = r.rows[0];
    if (items > s.availableUnits) {
      return { ok: false, reason: `Requested ${items} units but only ${s.availableUnits} in stock` };
    }
    if (kg > s.availableKg) {
      return { ok: false, reason: `Requested ${kg}kg but only ${s.availableKg}kg available` };
    }
    return { ok: true };
  } catch {
    return { ok: true }; // table might not exist — allow
  }
}

// ══════════════════════════════════════════════
// ORDERS
// ══════════════════════════════════════════════
app.get('/api/orders', auth, async (req, res) => {
  const { role, userId } = req.query;
  try {
    let r;
    if (role === 'retailer') {
      r = await pool.query(
        `SELECT id, city, items, kg, priority AS prio, status, "rejectReason",
         TO_CHAR("createdAt",'DD Mon HH24:MI') AS created
         FROM "Orders" WHERE "retailerId"=$1 ORDER BY "createdAt" DESC`,
        [userId]
      );
    } else {
      r = await pool.query(
        `SELECT id, "retailerName" AS retailer, city, items, kg,
         priority AS prio, status, "confirmedBy", "rejectReason",
         TO_CHAR("createdAt",'DD Mon HH24:MI') AS created
         FROM "Orders" ORDER BY "createdAt" DESC`
      );
    }
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/orders', auth, async (req, res) => {
  const { retailerId, retailerName, city, items, kg, priority, notes } = req.body;

  // ── FIX: Validate inputs strictly ──
  const itemsNum = Number(items);
  const kgNum    = Number(kg);

  if (!city || city.trim() === '') {
    return res.status(400).json({ error: 'City is required' });
  }
  if (!Number.isInteger(itemsNum) || itemsNum <= 0) {
    return res.status(400).json({ error: 'Items must be a positive whole number (no decimals or negatives)' });
  }
  if (typeof kgNum !== 'number' || isNaN(kgNum) || kgNum <= 0) {
    return res.status(400).json({ error: 'Weight must be a positive number' });
  }
  if (!['normal','high','urgent'].includes(priority)) {
    return res.status(400).json({ error: 'Invalid priority value' });
  }

  // ── FIX: Stock availability check ──
  const stock = await checkStock(itemsNum, kgNum);
  if (!stock.ok) {
    // Notify the retailer immediately about stock unavailability
    try {
      await notify(
        retailerId,
        'Order Rejected — Stock Unavailable ❌',
        `Your order request was rejected automatically: ${stock.reason}. Please revise and resubmit.`,
        'alert'
      );
    } catch {}
    return res.status(422).json({ error: stock.reason, stockError: true });
  }

  try {
    const r = await pool.query(
      `INSERT INTO "Orders"("retailerId","retailerName",city,items,kg,priority,notes,status,"createdAt")
       VALUES($1,$2,$3,$4,$5,$6,$7,'pending',NOW()) RETURNING id`,
      [retailerId, retailerName, city, itemsNum, kgNum, priority, notes||'']
    );
    const orderId = r.rows[0].id;

    // Notify all Order Processing Team members
    const staff = await pool.query('SELECT id FROM "Users" WHERE role=\'order_team\'');
    for (const s of staff.rows) {
      await notify(
        s.id,
        'New Order Request 📋',
        `${retailerName} requested ${itemsNum} items (${kgNum}kg) to ${city} — Priority: ${priority}`,
        'info',
        orderId
      );
    }
    res.json({ id: orderId });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/orders/:id/confirm', auth, async (req, res) => {
  const { action, confirmedBy, rejectReason } = req.body;

  // ── FIX: Require a reason when rejecting ──
  if (action === 'reject' && (!rejectReason || rejectReason.trim() === '')) {
    return res.status(400).json({ error: 'A rejection reason is required' });
  }

  const status = action === 'confirm' ? 'confirmed' : 'rejected';
  try {
    // Only allow action on pending orders
    const check = await pool.query('SELECT status FROM "Orders" WHERE id=$1', [req.params.id]);
    if (!check.rows.length) return res.status(404).json({ error: 'Order not found' });
    if (check.rows[0].status !== 'pending') {
      return res.status(400).json({ error: `Order is already "${check.rows[0].status}" — cannot change` });
    }

    await pool.query(
      'UPDATE "Orders" SET status=$1,"confirmedBy"=$2,"rejectReason"=$3 WHERE id=$4',
      [status, confirmedBy, rejectReason||null, req.params.id]
    );

    const order = await pool.query('SELECT * FROM "Orders" WHERE id=$1', [req.params.id]);
    const o = order.rows[0];
    if (o) {
      // ── FIX: Notify retailer ──
      const msg = action === 'confirm'
        ? `Your order ${o.id} to ${o.city} has been confirmed ✅. It will be processed for delivery.`
        : `Your order ${o.id} to ${o.city} was rejected ❌. Reason: ${rejectReason}. You may correct and resubmit.`;
      await notify(
        o.retailerId,
        action === 'confirm' ? 'Order Confirmed ✅' : 'Order Rejected ❌',
        msg,
        action === 'confirm' ? 'success' : 'alert',
        o.id
      );

      // ── FIX: Also notify route_planner & warehouse when confirmed ──
      if (action === 'confirm') {
        const planners = await pool.query(
          'SELECT id FROM "Users" WHERE role IN (\'route_planner\', \'warehouse\')'
        );
        for (const u of planners.rows) {
          await notify(
            u.id,
            'Order Confirmed — Awaiting Consolidation 📋',
            `Order ${o.id} from ${o.retailerName} to ${o.city} (${o.items} items, ${o.kg}kg) has been confirmed.`,
            'info',
            o.id
          );
        }
      }
    }
    res.json({ ok: true, status });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// DELIVERIES
// ══════════════════════════════════════════════
app.get('/api/deliveries', auth, async (req, res) => {
  const { role, userId } = req.query;
  try {
    let r;
    if (role === 'distributor') {
      r = await pool.query(
        `SELECT d.*, o."retailerName" AS retailer, o.city, o.items, o.kg, o.priority AS prio
         FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
         WHERE d."driverId"=$1 ORDER BY d."createdAt" DESC`,
        [userId]
      );
    } else {
      r = await pool.query(
        `SELECT d.*, o."retailerName" AS retailer, o.city, o.items, o.kg, o.priority AS prio
         FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
         ORDER BY d."createdAt" DESC`
      );
    }
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/deliveries/consolidate', auth, async (req, res) => {
  try {
    const orders = await pool.query('SELECT * FROM "Orders" WHERE status=\'confirmed\'');
    const confirmed = orders.rows;
    if (!confirmed.length) return res.json({ created: 0, held: 0 });

    if (confirmed.length === 1 && !['urgent','high'].includes(confirmed[0].priority)) {
      return res.json({ created: 0, held: 1, reason: 'single_low_priority', orderId: confirmed[0].id });
    }

    let created = 0;
    for (const o of confirmed) {
      await pool.query(
        `INSERT INTO "Deliveries"("orderId",status,"createdAt") VALUES($1,'pending',NOW())`,
        [o.id]
      );
      await pool.query('UPDATE "Orders" SET status=\'consolidated\' WHERE id=$1', [o.id]);
      created++;
    }

    // ── FIX: Notify route_planner AND warehouse reliably ──
    const notifyUsers = await pool.query(
      'SELECT id, role FROM "Users" WHERE role IN (\'route_planner\', \'warehouse\')'
    );
    for (const u of notifyUsers.rows) {
      const roleMsg = u.role === 'route_planner'
        ? `${created} delivery record(s) created. Please assign drivers and vehicles.`
        : `${created} delivery record(s) incoming. Please prepare cargo for loading.`;
      await notify(
        u.id,
        '📦 Deliveries Ready for Planning',
        roleMsg,
        'info'
      );
    }

    res.json({ created });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/assign', auth, async (req, res) => {
  const { driverId, driverName, vehicleId } = req.body;
  const etaDate = new Date(Date.now() + 2*60*60*1000);
  const eta = etaDate.toLocaleTimeString('en-LK', { hour:'2-digit', minute:'2-digit' });

  try {
    await pool.query(
      'UPDATE "Deliveries" SET "driverId"=$1,"driverName"=$2,"vehicleId"=$3,status=\'assigned\',eta=$4 WHERE id=$5',
      [driverId, driverName, vehicleId, eta, req.params.id]
    );

    // ── FIX: Auto-save route data to Routes table on assign ──
    const del = await pool.query(
      `SELECT d.*, o.city, o."retailerName" AS retailer, o.items, o.kg
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    const d = del.rows[0];
    if (d) {
      const distKm  = Math.round(42 + Math.random() * 30); // estimated per stop
      const durMins = Math.round(distKm * 2.8);
      await pool.query(
        `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"createdAt")
         VALUES($1,$2,$3,$4,$5,$6,$7,NOW())`,
        [driverId, driverName, vehicleId, 1, distKm, durMins, JSON.stringify([d.city])]
      );
    }

    // Notify driver
    await notify(
      parseInt(driverId),
      'New Delivery Assigned 🚚',
      `You have been assigned delivery ${req.params.id} to ${d?.city||''}. ETA: ${eta}`,
      'info',
      req.params.id
    );

    // Notify all warehouse staff
    const wh = await pool.query('SELECT id FROM "Users" WHERE role=\'warehouse\'');
    for (const u of wh.rows) {
      await notify(
        u.id,
        'Delivery Assigned — Prepare Cargo 📦',
        `Delivery ${req.params.id} assigned to ${driverName} (vehicle ${vehicleId}). Please prepare cargo. ETA: ${eta}`,
        'info',
        req.params.id
      );
    }

    res.json({ eta });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/warehouse-ready', auth, async (req, res) => {
  try {
    await pool.query('UPDATE "Deliveries" SET status=\'warehouse_ready\' WHERE id=$1', [req.params.id]);
    const del = await pool.query('SELECT * FROM "Deliveries" WHERE id=$1', [req.params.id]);
    const d = del.rows[0];
    if (d?.driverId) {
      await notify(
        parseInt(d.driverId),
        'Cargo Ready for Pickup 📦',
        `Cargo for delivery ${req.params.id} is ready at the warehouse. Please proceed to collect.`,
        'success',
        req.params.id
      );
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/loaded', auth, async (req, res) => {
  try {
    await pool.query('UPDATE "Deliveries" SET status=\'loaded\' WHERE id=$1', [req.params.id]);
    const del = await pool.query('SELECT * FROM "Deliveries" WHERE id=$1', [req.params.id]);
    const d = del.rows[0];
    if (d?.driverId) {
      await notify(
        parseInt(d.driverId),
        'Vehicle Loaded ✅',
        `Your vehicle for delivery ${req.params.id} has been loaded and is ready. You may depart.`,
        'success',
        req.params.id
      );
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/status', auth, async (req, res) => {
  const { newStatus, note, updatedBy } = req.body;
  const validStatuses = ['in-transit', 'delivered', 'failed'];
  if (!validStatuses.includes(newStatus)) {
    return res.status(400).json({ error: 'Invalid status value' });
  }
  try {
    await pool.query('UPDATE "Deliveries" SET status=$1 WHERE id=$2', [newStatus, req.params.id]);
    const del = await pool.query(
      `SELECT d.*, o."retailerId", o."retailerName" AS retailer, o.city
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    const d = del.rows[0];
    if (d) {
      const statusLabel = { 'in-transit':'In Transit 🚛', 'delivered':'Delivered ✅', 'failed':'Delivery Failed ❌' };
      const msgType     = { 'in-transit':'info', 'delivered':'success', 'failed':'alert' };

      // Notify retailer directly
      await notify(
        d.retailerId,
        `Delivery ${statusLabel[newStatus]||newStatus}`,
        `Your delivery ${req.params.id} to ${d.city} is now "${newStatus}". ${note||''}`,
        msgType[newStatus]||'info',
        req.params.id
      );

      // Notify Order Team and Warehouse
      const staff = await pool.query(
        'SELECT id FROM "Users" WHERE role IN (\'order_team\', \'warehouse\')'
      );
      for (const u of staff.rows) {
        await notify(
          u.id,
          `Delivery Update: ${statusLabel[newStatus]||newStatus}`,
          `Delivery ${req.params.id} to ${d.city} (${d.retailer}) → "${newStatus}". ${note||''}`,
          msgType[newStatus]||'info',
          req.params.id
        );
      }
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════
app.get('/api/routes', auth, async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM "Routes" ORDER BY "createdAt" DESC');
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/routes', auth, async (req, res) => {
  const { driverId, driverName, vehicleId, stops, distKm, durMins, cities } = req.body;
  try {
    const r = await pool.query(
      `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"createdAt")
       VALUES($1,$2,$3,$4,$5,$6,$7,NOW()) RETURNING id`,
      [driverId, driverName, vehicleId, stops, distKm, durMins, JSON.stringify(cities)]
    );
    res.json({ id: r.rows[0].id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// START
// ══════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Nestlé DMS API running on port ${PORT}`));