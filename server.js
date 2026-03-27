const express = require('express');
const cors    = require('cors');
const bcrypt  = require('bcrypt');
const jwt     = require('jsonwebtoken');
const { Pool } = require('pg');

/*
 * ── DB MIGRATION: Run these ALTER TABLE statements once to support the new
 *    Route Planning feature (safe to run even if columns already exist):
 *
 *  ALTER TABLE "Routes" ADD COLUMN IF NOT EXISTS "stops_data"  TEXT;
 *  ALTER TABLE "Routes" ADD COLUMN IF NOT EXISTS "routeDate"   DATE;
 *  ALTER TABLE "Routes" ADD COLUMN IF NOT EXISTS "depart"      VARCHAR(10);
 *  ALTER TABLE "Routes" ADD COLUMN IF NOT EXISTS "routeNotes"  TEXT;
 *
 *  -- Delivery status update tracking (run once):
 *  ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "updatedAt" TIMESTAMPTZ;
 *
 *  The API will fall back gracefully if these columns don't exist yet.
 */

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

// FIX Bug 1: read-all MUST be before /:id/read — otherwise Express matches
// 'read-all' as the :id param and this route is never reached.
app.put('/api/notifications/read-all', auth, async (req, res) => {
  const { userId } = req.body;
  if (!userId) return res.status(400).json({ error: 'userId required' });
  try {
    await pool.query('UPDATE "Notifications" SET "isRead"=true WHERE "userId"=$1', [userId]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/:id/read', auth, async (req, res) => {
  try {
    await pool.query('UPDATE "Notifications" SET "isRead"=true WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// FIX Bug 2: wrapped in try/catch so a DB error here never crashes the calling endpoint
async function notify(userId, title, message, type='info', refId=null) {
  try {
    await pool.query(
      'INSERT INTO "Notifications"("userId","title","message","type","refId","isRead","createdAt") VALUES($1,$2,$3,$4,$5,false,NOW())',
      [userId, title, message, type, refId]
    );
  } catch (e) {
    console.error('[notify] Failed for user ' + userId + ':', e.message);
  }
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

// Returns available products (backed by the "Stock" table).
app.get('/api/products', auth, async (req, res) => {
  try {
    // Be tolerant to different casing/naming for product-name column.
    // (Some setups create columns with camelCase inside quotes; others may store them lowercase.)
    const queries = [
      'SELECT id, "productName"  AS "productName" FROM "Stock" ORDER BY "productName"',
      'SELECT id, "productname" AS "productName" FROM "Stock" ORDER BY "productname"',
      'SELECT id, productName     AS "productName" FROM "Stock" ORDER BY productName',
      'SELECT id, productname    AS "productName" FROM "Stock" ORDER BY productname',
    ];

    for (const q of queries) {
      try {
        const r = await pool.query(q);
        if (Array.isArray(r.rows)) return res.json(r.rows);
      } catch {}
    }

    return res.json([]);
  } catch (e) {
    // Stock table might not exist yet; keep frontend usable.
    return res.json([]);
  }
});

// ── Stock check helper ────────────────────────
// Checks against a "Stock" table (id, productName, availableUnits, availableKg)
// Falls back gracefully if table doesn't exist yet.
async function checkStock(productId, items, kg) {
  try {
    const r = await pool.query(
      'SELECT "productName", "availableUnits", "availableKg" FROM "Stock" WHERE id=$1',
      [productId]
    );
    if (!r.rows.length) return { ok: true, productName: null }; // stock table exists, but no row — allow
    const s = r.rows[0];
    if (items > s.availableUnits) {
      return { ok: false, reason: `Requested ${items} units but only ${s.availableUnits} in stock` };
    }
    if (kg > 0 && kg > s.availableKg) {
      return { ok: false, reason: `Requested ${kg}kg but only ${s.availableKg}kg available` };
    }
    return { ok: true, productName: s.productName || null };
  } catch {
    return { ok: true, productName: null }; // table might not exist — allow
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
  const {
    retailerId,
    retailerName,
    city,
    items,
    kg,
    priority,
    notes,
    productId,
    productName,
    specifics
  } = req.body;

  // ── FIX: Validate inputs strictly ──
  const itemsNum = Number(items);
  const kgNum    = kg != null ? Number(kg) : 0;   // kg is optional — retailer doesn't know weight
  const productIdNum = productId ? Number(productId) : 1;

  if (!city || city.trim() === '') {
    return res.status(400).json({ error: 'City is required' });
  }
  if (!Number.isInteger(itemsNum) || itemsNum <= 0) {
    return res.status(400).json({ error: 'Items must be a positive whole number (no decimals or negatives)' });
  }
  if (!Number.isInteger(productIdNum) || productIdNum <= 0) {
    return res.status(400).json({ error: 'Product must be selected' });
  }
  if (!['normal','high','urgent'].includes(priority)) {
    return res.status(400).json({ error: 'Invalid priority value' });
  }

  // ── FIX: Stock availability check ──
  const stock = await checkStock(productIdNum, itemsNum, kgNum);
  const productLabel =
    (typeof productName === 'string' && productName.trim()) ? productName.trim() :
    (typeof specifics === 'string' && specifics.trim()) ? specifics.trim() :
    (stock && typeof stock.productName === 'string' && stock.productName.trim()) ? stock.productName.trim() :
    `Product ${productIdNum}`;
  if (!stock.ok) {
    // Notify the retailer immediately about stock unavailability
    try {
      await notify(
        retailerId,
        'Order Rejected — Stock Unavailable ❌',
        `Your order request for ${productLabel} was rejected automatically: ${stock.reason}. Please revise and resubmit.`,
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
        `${retailerName} requested ${itemsNum} items (${kgNum}kg) of ${productLabel} to ${city} — Priority: ${priority}`,
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
        // Notify driver if already assigned to a delivery for this order
        try {
          const dRow = await pool.query(
            'SELECT "driverId" FROM "Deliveries" WHERE "orderId"=$1 AND "driverId" IS NOT NULL LIMIT 1',
            [o.id]
          );
          if (dRow.rows[0]?.driverId) {
            const dUid = await driverUserId(Number(dRow.rows[0].driverId));
            if (dUid) await notify(dUid, 'Order Confirmed ✅ — Your Delivery', `Order ${o.id} to ${o.city} for ${o.retailerName} has been confirmed by the order team. Your delivery will proceed as planned.`, 'success', o.id);
          }
        } catch {}
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

    // Notify any drivers already assigned to these newly consolidated deliveries
    try {
      const dRows = await pool.query(
        `SELECT DISTINCT d."driverId", o."retailerName", o.city, o.items, d.id AS "deliveryId"
         FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
         WHERE o.status='consolidated' AND d."driverId" IS NOT NULL`
      );
      for (const row of dRows.rows) {
        const dUid = await driverUserId(Number(row.driverId));
        if (dUid) await notify(dUid, '📦 Your Delivery Has Been Processed', `Your delivery to ${row.city} for ${row.retailerName} (${row.items} items) is ready for route planning. Check your route briefing for updates.`, 'info', row.deliveryId);
      }
    } catch {}
    res.json({ created });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/assign', auth, async (req, res) => {
  const { driverId, driverName, vehicleId, etaOverride, deliveryNotes } = req.body;

  // Calculate ETA
  let eta;
  if (etaOverride) {
    eta = etaOverride;
  } else {
    const etaDate = new Date(Date.now() + 2*60*60*1000);
    eta = etaDate.toLocaleTimeString('en-LK', { hour:'2-digit', minute:'2-digit' });
  }

  try {
    // Block reassignment if locked
    const check = await pool.query('SELECT status, "driverId" AS "oldDriverId" FROM "Deliveries" WHERE id=$1', [req.params.id]);
    if (!check.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const current = check.rows[0];
    const lockedStatuses = ['in-transit', 'delivered', 'failed'];
    if (lockedStatuses.includes(current.status)) {
      return res.status(409).json({ error: `Cannot reassign — delivery is already "${current.status}". Route is locked once in transit or completed.` });
    }

    // Notify old driver of de-assignment
    const oldDriverId = parseInt(current.oldDriverId);
    if (oldDriverId && oldDriverId !== parseInt(driverId)) {
      await notify(
        oldDriverId,
        'Delivery Reassigned ↩️',
        `Delivery ${req.params.id} has been reassigned to another driver. It has been removed from your route.`,
        'warning',
        req.params.id
      );
    }

    await pool.query(
      'UPDATE "Deliveries" SET "driverId"=$1,"driverName"=$2,"vehicleId"=$3,status=\'assigned\',eta=$4 WHERE id=$5',
      [driverId, driverName, vehicleId, eta, req.params.id]
    );

    // Get full delivery + order details for notifications
    const del = await pool.query(
      `SELECT d.*, o.city, o."retailerName" AS retailer, o.items, o.kg, o.priority, o."retailerId"
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    const d = del.rows[0];

    if (d) {
      const distKm  = Math.round(42 + Math.random() * 30);
      const durMins = Math.round(distKm * 2.8);

      // Save / update route record
      await pool.query('DELETE FROM "Routes" WHERE "deliveryId"=$1', [req.params.id]).catch(()=>{});
      const stopsData = JSON.stringify([{
        deliveryId: req.params.id,
        retailer:   d.retailer,
        city:       d.city,
        items:      d.items,
        priority:   d.priority,
        eta,
        stopNote:   deliveryNotes || ''
      }]);
      await pool.query(
        `INSERT INTO "Routes"("deliveryId","driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"stops_data","createdAt")
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) ON CONFLICT DO NOTHING`,
        [req.params.id, driverId, driverName, vehicleId, 1, distKm, durMins, JSON.stringify([d.city]), stopsData]
      ).catch(async () => {
        // Fallback without deliveryId column
        await pool.query(
          `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"stops_data","createdAt")
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,NOW())`,
          [driverId, driverName, vehicleId, 1, distKm, durMins, JSON.stringify([d.city]), stopsData]
        ).catch(async () => {
          // Final fallback without stops_data
          await pool.query(
            `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"createdAt")
             VALUES($1,$2,$3,$4,$5,$6,$7,NOW())`,
            [driverId, driverName, vehicleId, 1, distKm, durMins, JSON.stringify([d.city])]
          );
        });
      });

      // Rich driver notification with full briefing
      const notesLine = deliveryNotes ? `\nNotes: ${deliveryNotes}` : '';
      await notify(
        parseInt(driverId),
        '🚚 Delivery Assigned — Route Briefing',
        `You have been assigned delivery ${req.params.id}.\n\nStop 1: ${d.city} — ${d.retailer} (${d.items} items)\nVehicle: ${vehicleId}\nETA: ${eta}\nDistance: ~${distKm} km${notesLine}\n\nCheck "My Route Briefing" for full details.`,
        'info',
        req.params.id
      );

      // Notify warehouse
      const wh = await pool.query('SELECT id FROM "Users" WHERE role=\'warehouse\'');
      for (const u of wh.rows) {
        await notify(
          u.id,
          'Delivery Assigned — Prepare Cargo 📦',
          `Delivery ${req.params.id} assigned to ${driverName} (vehicle ${vehicleId}). Destination: ${d.city} — ${d.retailer}, ${d.items} items. ETA: ${eta}. Please prepare cargo.`,
          'info',
          req.params.id
        );
      }
    }

    res.json({ eta, driverName, vehicleId });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/warehouse-ready', auth, async (req, res) => {
  try {
    await pool.query('UPDATE "Deliveries" SET status=\'warehouse_ready\' WHERE id=$1', [req.params.id]);
    const del = await pool.query('SELECT * FROM "Deliveries" WHERE id=$1', [req.params.id]);
    const d = del.rows[0];
    const driverIdWR = d && d.driverId ? Number(d.driverId) : null;
    if (driverIdWR) {
      await notify(
        driverIdWR,
        'Cargo Ready for Pickup 📦',
        `Your cargo for delivery ${req.params.id} is packed and ready at the warehouse. Please proceed to collect your vehicle.`,
        'success',
        req.params.id
      );
    } else {
      console.warn('[warehouse-ready] No driverId on delivery ' + req.params.id + ' — distributor not notified');
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/loaded', auth, async (req, res) => {
  try {
    await pool.query('UPDATE "Deliveries" SET status=\'loaded\' WHERE id=$1', [req.params.id]);
    const del = await pool.query('SELECT * FROM "Deliveries" WHERE id=$1', [req.params.id]);
    const d = del.rows[0];
    const driverIdL = d && d.driverId ? Number(d.driverId) : null;
    if (driverIdL) {
      await notify(
        driverIdL,
        'Vehicle Loaded ✅ — Ready to Depart',
        `Your vehicle for delivery ${req.params.id} has been fully loaded. You are cleared to depart. Check "My Routes" for your stop sequence.`,
        'success',
        req.params.id
      );
    } else {
      console.warn('[loaded] No driverId on delivery ' + req.params.id + ' — distributor not notified');
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
  if (newStatus === 'failed' && (!note || note.trim() === '')) {
    return res.status(400).json({ error: 'A reason is required when marking a delivery as failed' });
  }
  try {
    const cur = await pool.query('SELECT status FROM "Deliveries" WHERE id=$1', [req.params.id]);
    if (!cur.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const currentStatus = cur.rows[0].status;
    if (['delivered', 'failed'].includes(currentStatus)) {
      return res.status(409).json({ error: `Delivery is already "${currentStatus}" — status cannot be changed.` });
    }

    await pool.query(
      'UPDATE "Deliveries" SET status=$1, "updatedAt"=NOW() WHERE id=$2',
      [newStatus, req.params.id]
    );

    const del = await pool.query(
      `SELECT d.*, o."retailerId", o."retailerName" AS retailer, o.city, o.items, o.kg
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    const d = del.rows[0];
    if (d) {
      const statusLabel = { 'in-transit': 'In Transit 🚛', 'delivered': 'Delivered ✅', 'failed': 'Delivery Failed ❌' };
      const msgType     = { 'in-transit': 'info', 'delivered': 'success', 'failed': 'alert' };
      const noteStr     = note && note.trim() ? ` Note: ${note.trim()}` : '';
      const driverStr   = d.driverName ? ` Driver: ${d.driverName}.` : '';

      // 1. Notify Retailer
      const retailerMsg = newStatus === 'delivered'
        ? `Great news! Your delivery ${req.params.id} to ${d.city} has been successfully delivered ✅.${noteStr}`
        : newStatus === 'failed'
        ? `Unfortunately, delivery ${req.params.id} to ${d.city} could not be completed ❌.${noteStr} Please contact us to reschedule.`
        : `Your delivery ${req.params.id} to ${d.city} is now on its way 🚛.${driverStr} Expected arrival: ${d.eta || 'soon'}.${noteStr}`;

      await notify(
        d.retailerId,
        `Delivery ${statusLabel[newStatus] || newStatus}`,
        retailerMsg,
        msgType[newStatus] || 'info',
        req.params.id
      );

      // 2. Notify Order Team, Warehouse, Route Planner
      const staff = await pool.query(
        `SELECT id, role FROM "Users" WHERE role IN ('order_team', 'warehouse', 'route_planner')`
      );
      for (const u of staff.rows) {
        const roleContext = u.role === 'warehouse'
          ? (newStatus === 'delivered' ? ' Delivery bay can be cleared.' : newStatus === 'failed' ? ' May need to re-stock or re-schedule.' : '')
          : u.role === 'route_planner'
          ? (newStatus === 'delivered' ? ' Route stop complete.' : newStatus === 'failed' ? ' Consider re-routing or rescheduling.' : '')
          : '';
        await notify(
          u.id,
          `Delivery Update: ${statusLabel[newStatus] || newStatus}`,
          `Delivery ${req.params.id} → ${d.city} (${d.retailer}, ${d.items} items).${driverStr} Status: "${newStatus}".${noteStr}${roleContext}`,
          msgType[newStatus] || 'info',
          req.params.id
        );
      }

      // 3. Notify the distributor (driver) — FIXED: Number() cast
      const driverId = d.driverId ? Number(d.driverId) : null;
      if (driverId) {
        const driverMsg = newStatus === 'delivered'
          ? `Delivery ${req.params.id} to ${d.city} (${d.retailer}) confirmed as delivered ✅. Check your route for remaining stops.`
          : newStatus === 'failed'
          ? `Delivery ${req.params.id} to ${d.city} (${d.retailer}) recorded as failed ❌.${noteStr} Contact your planner for next steps.`
          : `Delivery ${req.params.id} to ${d.city} (${d.retailer}) marked in-transit 🚛.${noteStr}`;
        await notify(
          driverId,
          `Your Delivery: ${statusLabel[newStatus] || newStatus}`,
          driverMsg,
          msgType[newStatus] || 'info',
          req.params.id
        );
      }
    }
    res.json({ ok: true, newStatus, notified: ['retailer', 'order_team', 'warehouse', 'route_planner', 'distributor'] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════

// Publish a full multi-stop route plan
app.post('/api/routes/publish', auth, async (req, res) => {
  const { driverId, driverName, vehicleId, routeDate, depart, routeNotes, stops, distKm, durMins, cities } = req.body;

  if (!driverId || !driverName || !vehicleId || !Array.isArray(stops) || !stops.length) {
    return res.status(400).json({ error: 'driverId, driverName, vehicleId, and stops[] are required' });
  }

  try {
    const stopsData = JSON.stringify(stops);
    const citiesJson = JSON.stringify(cities || stops.map(s => s.city));

    // Insert route record — try with all columns first, fall back gracefully
    let routeId;
    try {
      const rr = await pool.query(
        `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"stops_data","routeDate","depart","routeNotes","createdAt")
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW()) RETURNING id`,
        [driverId, driverName, vehicleId, stops.length, distKm, durMins, citiesJson, stopsData, routeDate||null, depart||null, routeNotes||null]
      );
      routeId = rr.rows[0].id;
    } catch {
      // Fallback: insert without newer columns
      const rr = await pool.query(
        `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"createdAt")
         VALUES($1,$2,$3,$4,$5,$6,$7,NOW()) RETURNING id`,
        [driverId, driverName, vehicleId, stops.length, distKm, durMins, citiesJson]
      );
      routeId = rr.rows[0].id;
    }

    // Assign each delivery to this driver/vehicle and update status
    const assignedDeliveryIds = [];
    for (let i = 0; i < stops.length; i++) {
      const s = stops[i];
      const eta = s.eta || null;
      try {
        // Check the delivery is still pending / reassignable
        const chk = await pool.query('SELECT status, "driverId" AS old FROM "Deliveries" WHERE id=$1', [s.deliveryId]);
        if (!chk.rows.length) continue;
        const cur = chk.rows[0];
        if (['in-transit','delivered','failed'].includes(cur.status)) continue;

        // Notify old driver if reassigned
        const oldDriverId = parseInt(cur.old);
        if (oldDriverId && oldDriverId !== parseInt(driverId)) {
          await notify(oldDriverId,
            'Delivery Reassigned ↩️',
            `Delivery ${s.deliveryId} has been moved to a new route. It is no longer on your schedule.`,
            'warning', s.deliveryId
          );
        }

        await pool.query(
          'UPDATE "Deliveries" SET "driverId"=$1,"driverName"=$2,"vehicleId"=$3,status=\'assigned\',eta=$4 WHERE id=$5',
          [driverId, driverName, vehicleId, eta, s.deliveryId]
        );
        assignedDeliveryIds.push(s.deliveryId);
      } catch (e) {
        console.error('Stop assign error:', e.message);
      }
    }

    // Build a rich route briefing for the driver notification
    const stopLines = stops.map((s, i) =>
      `  Stop ${i+1}: ${s.city} — ${s.retailer || ''} (${s.items || '?'} items)${s.eta ? ', ETA ' + s.eta : ''}${s.stopNote ? '\n    Note: ' + s.stopNote : ''}`
    ).join('\n');
    const summaryLine = `${stops.length} stops · ~${distKm} km · ~${Math.floor(durMins/60)}h ${durMins%60}m`;
    const dateLine    = routeDate ? `Date: ${routeDate}` : '';
    const departLine  = depart    ? `Depot departure: ${depart}` : '';

    await notify(
      parseInt(driverId),
      `🗺️ Route Plan Published — ${stops.length} Stops`,
      `Your route has been planned and published.\n\n${dateLine}${dateLine&&departLine?'\n':''}${departLine}\nVehicle: ${vehicleId}\n${summaryLine}\n\nStop Sequence:\n${stopLines}${routeNotes ? '\n\nRoute Notes: ' + routeNotes : ''}\n\nCheck "My Route Briefing" for the full plan.`,
      'info',
      String(routeId)
    );

    // Notify warehouse with full stop list
    const wh = await pool.query('SELECT id FROM "Users" WHERE role=\'warehouse\'');
    for (const u of wh.rows) {
      await notify(
        u.id,
        `📦 Route Published — ${stops.length} Stops to Prepare`,
        `Route #${routeId} published for ${driverName} (vehicle ${vehicleId}).\nStops: ${stops.map((s,i)=>`${i+1}. ${s.city} — ${s.retailer||''}`).join(' | ')}\nPlease prepare and load cargo for all ${stops.length} stops before ${depart||'departure'}.`,
        'info',
        String(routeId)
      );
    }

    // Notify each retailer their delivery is scheduled
    for (const s of stops) {
      try {
        const delRow = await pool.query(
          `SELECT o."retailerId" FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
          [s.deliveryId]
        );
        if (delRow.rows.length) {
          await notify(
            delRow.rows[0].retailerId,
            '🚚 Your Delivery is Scheduled',
            `Your delivery ${s.deliveryId} to ${s.city} has been scheduled. Driver: ${driverName}, Vehicle: ${vehicleId}.${s.eta ? ' Expected arrival: ' + s.eta + '.' : ''} You will be notified when the driver is en route.`,
            'info',
            s.deliveryId
          );
        }
      } catch {}
    }

    res.json({ id: routeId, assignedDeliveries: assignedDeliveryIds.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/routes', auth, async (req, res) => {
  const { driverId } = req.query;
  try {
    let r;
    if (driverId) {
      // Driver-scoped: used by planner's "Saved Routes" filtered by driver
      try {
        r = await pool.query(
          `SELECT rt.id, rt."driverId", rt."driverName", rt."vehicleId", rt.stops, rt."distKm", rt."durMins",
                  rt.cities, rt."stops_data", rt."routeDate", rt.depart, rt."routeNotes", rt."createdAt",
                  d.status AS "deliveryStatus", d.id AS "deliveryId", d.eta
           FROM "Routes" rt
           LEFT JOIN "Deliveries" d ON d.id = (
             SELECT id FROM "Deliveries" WHERE "driverId"=rt."driverId" ORDER BY "createdAt" DESC LIMIT 1
           )
           WHERE rt."driverId"=$1
           ORDER BY rt."createdAt" DESC`,
          [driverId]
        );
      } catch {
        r = await pool.query(
          `SELECT rt.id, rt."driverId", rt."driverName", rt."vehicleId", rt.stops,
                  rt."distKm", rt."durMins", rt.cities, rt."createdAt",
                  d.status AS "deliveryStatus", d.id AS "deliveryId", d.eta
           FROM "Routes" rt
           LEFT JOIN "Deliveries" d ON d."driverId"=rt."driverId"
           WHERE rt."driverId"=$1
           ORDER BY rt."createdAt" DESC`,
          [driverId]
        );
      }
    } else {
      // All routes — try with new columns, fall back to base
      try {
        r = await pool.query(
          `SELECT rt.id, rt."driverId", rt."driverName", rt."vehicleId", rt.stops, rt."distKm", rt."durMins",
                  rt.cities, rt."stops_data", rt."routeDate", rt.depart, rt."routeNotes", rt."createdAt",
                  d.status AS "deliveryStatus", d.id AS "deliveryId", d.eta
           FROM "Routes" rt
           LEFT JOIN "Deliveries" d ON d.id = (
             SELECT id FROM "Deliveries" WHERE "driverId"=rt."driverId" ORDER BY "createdAt" DESC LIMIT 1
           )
           ORDER BY rt."createdAt" DESC`
        );
      } catch {
        r = await pool.query(
          `SELECT rt.id, rt."driverId", rt."driverName", rt."vehicleId", rt.stops,
                  rt."distKm", rt."durMins", rt.cities, rt."createdAt",
                  d.status AS "deliveryStatus", d.id AS "deliveryId", d.eta
           FROM "Routes" rt
           LEFT JOIN "Deliveries" d ON d."driverId"=rt."driverId"
           ORDER BY rt."createdAt" DESC`
        );
      }
    }
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Driver's own routes endpoint — resilient fallback if new columns don't exist yet
app.get('/api/routes/my', auth, async (req, res) => {
  const { driverId } = req.query;
  if (!driverId) return res.status(400).json({ error: 'driverId required' });
  try {
    // Try to fetch published routes (with new columns)
    let routes = [];
    try {
      const r = await pool.query(
        `SELECT id,"driverId","driverName","vehicleId",stops,"distKm","durMins",cities,
                "stops_data","routeDate","depart","routeNotes","createdAt"
         FROM "Routes" WHERE "driverId"=$1 ORDER BY "createdAt" DESC`,
        [driverId]
      );
      routes = r.rows;
    } catch {
      // New columns don't exist yet — fall back to base columns only
      try {
        const r = await pool.query(
          `SELECT id,"driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"createdAt"
           FROM "Routes" WHERE "driverId"=$1 ORDER BY "createdAt" DESC`,
          [driverId]
        );
        routes = r.rows;
      } catch { routes = []; }
    }

    // If published routes found, return them
    if (routes.length > 0) return res.json(routes);

    // ── FALLBACK: reconstruct a synthetic route from the driver's assigned deliveries ──
    // This handles drivers who were quick-assigned before the route builder existed
    const dels = await pool.query(
      `SELECT d.id AS "deliveryId", d."vehicleId", d.eta, d.status,
              o."retailerName" AS retailer, o.city, o.items, o.priority
       FROM "Deliveries" d
       JOIN "Orders" o ON d."orderId"=o.id
       WHERE d."driverId"=$1
       ORDER BY d."createdAt" ASC`,
      [driverId]
    );
    if (!dels.rows.length) return res.json([]);

    const delivs     = dels.rows;
    const cities     = delivs.map(d => d.city);
    const distKm     = Math.round(delivs.length * 40);
    const durMins    = Math.round(distKm * 2.8);
    const vehicleId  = delivs[0].vehicleId || '—';
    const driverRow  = await pool.query('SELECT name FROM "Users" WHERE id=$1', [driverId]);
    const driverName = driverRow.rows[0]?.name || 'Driver';

    // Build stops_data so the frontend can render the full timeline
    const stopsData = delivs.map(d => ({
      deliveryId: d.deliveryId,
      retailer:   d.retailer,
      city:       d.city,
      items:      d.items,
      priority:   d.priority,
      eta:        d.eta || '',
      stopNote:   ''
    }));

    const syntheticRoute = {
      id:           null,
      driverId,
      driverName,
      vehicleId,
      stops:        delivs.length,
      distKm,
      durMins,
      cities:       JSON.stringify(cities),
      stops_data:   JSON.stringify(stopsData),
      routeDate:    null,
      depart:       null,
      routeNotes:   null,
      createdAt:    new Date(),
      _synthetic:   true   // flag so frontend knows this was auto-generated
    };

    return res.json([syntheticRoute]);
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
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ══════════════════════════════════════════════
// START
// ══════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Nestlé DMS API running on port ${PORT}`));