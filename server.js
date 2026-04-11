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
 *  ALTER TABLE "Routes" ADD COLUMN IF NOT EXISTS "warehouse"   VARCHAR(50);
 *
 *  -- Delivery status update tracking (run once):
 *  ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "updatedAt" TIMESTAMPTZ;
 *
 *  The API will fall back gracefully if these columns don't exist yet.
 */

const app = express();
app.use(cors());
app.use(express.json());

/*
 * ── GPS IMPROVEMENT MIGRATION (run once in Neon):
 *
 *  ALTER TABLE "VehicleLocations" ADD COLUMN IF NOT EXISTS accuracy  NUMERIC;
 *  ALTER TABLE "VehicleLocations" ADD COLUMN IF NOT EXISTS speed     NUMERIC;
 *  ALTER TABLE "VehicleLocations" ADD COLUMN IF NOT EXISTS heading   NUMERIC;
 *
 *  -- Keep only last 500 rows per delivery (auto-cleanup):
 *  CREATE OR REPLACE FUNCTION cleanup_vehicle_locations() RETURNS trigger AS $$
 *  BEGIN
 *    DELETE FROM "VehicleLocations"
 *    WHERE "deliveryId" = NEW."deliveryId"
 *      AND id NOT IN (
 *        SELECT id FROM "VehicleLocations"
 *        WHERE "deliveryId" = NEW."deliveryId"
 *        ORDER BY "recordedAt" DESC LIMIT 500
 *      );
 *    RETURN NEW;
 *  END;
 *  $$ LANGUAGE plpgsql;
 *
 *  DROP TRIGGER IF EXISTS trg_cleanup_locations ON "VehicleLocations";
 *  CREATE TRIGGER trg_cleanup_locations
 *    AFTER INSERT ON "VehicleLocations"
 *    FOR EACH ROW EXECUTE FUNCTION cleanup_vehicle_locations();
 */

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Set Sri Lanka timezone for all DB sessions
pool.on('connect', client => {
  client.query("SET timezone = 'Asia/Colombo'");
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

// ── diagnostic endpoint — check vehicles, drivers and pending deliveries
app.get('/api/debug/routing', async (req, res) => {
  try {
    const veh     = await pool.query(`SELECT id, type, cap FROM "Vehicles" ORDER BY id`);
    const drv     = await pool.query(`SELECT d.id, d.name, d."userId", u.id AS "linkedUserId" FROM "Drivers" d LEFT JOIN "Users" u ON lower(u.name)=lower(d.name) AND u.role='distributor'`);
    const pending = await pool.query(`SELECT COUNT(*) FROM "Deliveries" WHERE status='pending'`);
    const dels    = await pool.query(`SELECT id, status, "driverId", "vehicleId" FROM "Deliveries" WHERE status IN ('assigned','warehouse_ready','loaded','in-transit') ORDER BY id`);
    res.json({
      vehicles: veh.rows,
      drivers: drv.rows,
      pendingDeliveries: parseInt(pending.rows[0].count),
      activeDeliveries: dels.rows
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// AUTH
// ══════════════════════════════════════════════
app.post('/api/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  try {
    const r = await pool.query(
      'SELECT * FROM "Users" WHERE lower(\"Email\")=lower($1) OR lower(name)=lower($1)',
      [email.trim()]
    );
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
       TO_CHAR("createdAt" AT TIME ZONE 'Asia/Colombo', 'HH24:MI DD Mon') AS time
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
async function driverUserId(driversId) {
  if (!driversId) return null;
  try {
    const r1 = await pool.query('SELECT "userId" FROM "Drivers" WHERE id=$1', [driversId]);
    if (r1.rows[0]?.userId) return r1.rows[0].userId;
    const r2 = await pool.query(
      `SELECT u.id FROM "Drivers" d
       JOIN "Users" u ON lower(u.name) = lower(d.name) AND u.role = 'distributor'
       WHERE d.id = $1`, [driversId]
    );
    return r2.rows[0]?.id || null;
  } catch(e) { console.error('[driverUserId]', e.message); return null; }
}

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
    const r = await pool.query(`
      SELECT d.id, d.name, d.phone, u.id AS "userId",
             CASE WHEN EXISTS (
               SELECT 1 FROM "Deliveries" del
               WHERE del.status IN ('assigned','warehouse_ready','loaded','in-transit')
                 AND (
                   del."driverId" = d.id
                   OR (u.id IS NOT NULL AND del."driverId" = u.id)
                 )
             ) THEN true ELSE false END AS busy
      FROM "Drivers" d
      LEFT JOIN "Users" u ON lower(u.name) = lower(d.name) AND u.role = 'distributor'
      ORDER BY d.name
    `);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vehicles', auth, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT v.*,
             COALESCE((SELECT SUM(o.kg) FROM "Deliveries" d
               JOIN "Orders" o ON d."orderId" = o.id
               WHERE d."vehicleId" = v.id AND d.status = 'in-transit'), 0) AS "loadedKg",
             CASE WHEN EXISTS (SELECT 1 FROM "Deliveries" d2
               WHERE d2."vehicleId" = v.id
                 AND d2.status IN ('assigned','warehouse_ready','loaded','in-transit')
             ) THEN true ELSE false END AS busy
      FROM "Vehicles" v ORDER BY v.id
    `);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Returns available products (backed by the "Stock" table).
app.get('/api/products', auth, async (req, res) => {
  try {
    const queries = [
      'SELECT id, "productName" AS "productName" FROM "Stock" ORDER BY "productName"',
      'SELECT id, "productname" AS "productName" FROM "Stock" ORDER BY "productname"',
    ];
    for (const q of queries) {
      try { const r = await pool.query(q); if (Array.isArray(r.rows)) return res.json(r.rows); } catch {}
    }
    return res.json([]);
  } catch { return res.json([]); }
});

// Returns products WITH weight per unit — used by frontend kg auto-calc
app.get('/api/products/weights', auth, async (req, res) => {
  try {
    const queries = [
      `SELECT id, "productName" AS "productName", "weightPerUnit" FROM "Stock" ORDER BY "productName"`,
      `SELECT id, "productname" AS "productName", "weightperunit" AS "weightPerUnit" FROM "Stock" ORDER BY "productname"`,
    ];
    for (const q of queries) {
      try {
        const r = await pool.query(q);
        if (Array.isArray(r.rows)) return res.json(r.rows);
      } catch {}
    }
    return res.json([]);
  } catch { return res.json([]); }
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
        `SELECT o.id, o.city, o.items, o.kg, o.priority AS prio, o.status, o."rejectReason", COALESCE(o."rejectCategory",'other') AS "rejectCategory",
         COALESCE(d."deliveryPin",'') AS "deliveryPin", COALESCE(d."pinVerified",false) AS "pinVerified",
         TO_CHAR(o."createdAt" AT TIME ZONE 'Asia/Colombo','DD Mon HH24:MI') AS created,
         d.id AS "deliveryId", d.status AS "deliveryStatus",
         d."receiptConfirmed", TO_CHAR(d."receiptAt" AT TIME ZONE 'Asia/Colombo', 'DD Mon YYYY HH24:MI') AS "receiptAt",
         d."driverName", d."vehicleId"
         FROM "Orders" o
         LEFT JOIN "Deliveries" d ON d."orderId" = o.id
         WHERE o."retailerId"=$1 ORDER BY o."createdAt" DESC`,
        [userId]
      );
    } else {
      r = await pool.query(
        `SELECT id, "retailerName" AS retailer, city, items, kg,
         priority AS prio, status, "confirmedBy", "rejectReason", COALESCE("rejectCategory",'other') AS "rejectCategory",
         TO_CHAR("createdAt" AT TIME ZONE 'Asia/Colombo','DD Mon HH24:MI') AS created
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
      `INSERT INTO "Orders"("retailerId","retailerName",city,items,kg,priority,notes,"productId",status,"createdAt")
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending',NOW()) RETURNING id`,
      [retailerId, retailerName, city, itemsNum, kgNum, priority, notes||'', productIdNum]
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

// ── Actionable rejection messages per category
function getRejectionMessage(category, reason, order) {
  const base = `Your order ${order.id} to ${order.city} has been rejected.`;
  const messages = {
    out_of_stock:       `${base}

Reason: The product is temporarily out of stock. You will be notified automatically when stock is replenished — no action needed right now.`,
    duplicate_order:    `${base}

Reason: You already have an active order for this product. Please check My Orders before submitting again.`,
    coverage_area:      `${base}

Reason: We do not currently deliver to ${order.city}. Please contact your account manager to request coverage expansion.`,
    suspicious_quantity:`${base}

Reason: The quantity of ${order.items} units seems unusual. Please resubmit with the correct quantity or contact our Order Team directly.`,
    credit_hold:        `${base}

Reason: Your account has a pending balance. Please settle your account to resume ordering.`,
    product_discontinued:`${base}

Reason: This product has been discontinued from our catalogue. Please contact your account manager for alternatives.`,
    other:              `${base}

Reason: ${reason}

Please correct your order and resubmit, or contact our Order Team for assistance.`,
  };
  return messages[category] || messages.other;
}

app.put('/api/orders/:id/confirm', auth, async (req, res) => {
  const { action, confirmedBy, rejectReason, rejectCategory } = req.body;

  if (action === 'reject' && (!rejectReason || rejectReason.trim() === '')) {
    return res.status(400).json({ error: 'A rejection reason is required' });
  }
  if (action === 'reject' && (!rejectCategory || rejectCategory.trim() === '')) {
    return res.status(400).json({ error: 'A rejection category is required' });
  }

  const status = action === 'confirm' ? 'confirmed' : 'rejected';
  try {
    const check = await pool.query('SELECT status FROM "Orders" WHERE id=$1', [req.params.id]);
    if (!check.rows.length) return res.status(404).json({ error: 'Order not found' });
    if (check.rows[0].status !== 'pending') {
      return res.status(400).json({ error: `Order is already "${check.rows[0].status}" — cannot change` });
    }

    // Store category alongside reason
    try {
      await pool.query(
        `UPDATE "Orders" SET status=$1,"confirmedBy"=$2,"rejectReason"=$3,"rejectCategory"=$4 WHERE id=$5`,
        [status, confirmedBy, rejectReason||null, rejectCategory||null, req.params.id]
      );
    } catch {
      // rejectCategory column may not exist yet — add it then retry
      await pool.query(`ALTER TABLE "Orders" ADD COLUMN IF NOT EXISTS "rejectCategory" VARCHAR(50)`);
      await pool.query(
        `UPDATE "Orders" SET status=$1,"confirmedBy"=$2,"rejectReason"=$3,"rejectCategory"=$4 WHERE id=$5`,
        [status, confirmedBy, rejectReason||null, rejectCategory||null, req.params.id]
      );
    }

    const order = await pool.query('SELECT * FROM "Orders" WHERE id=$1', [req.params.id]);
    const o = order.rows[0];
    if (o) {
      let msg, notifType;
      if (action === 'confirm') {
        msg = `Your order ${o.id} to ${o.city} has been confirmed ✅. It will be processed for delivery.`;
        notifType = 'success';
      } else {
        msg = getRejectionMessage(rejectCategory, rejectReason, o);
        notifType = 'alert';

        // ── If rejected for out_of_stock, register a stock watch
        if (rejectCategory === 'out_of_stock' && o.productId) {
          try {
            await pool.query(`ALTER TABLE "Orders" ADD COLUMN IF NOT EXISTS "stockWatchActive" BOOLEAN DEFAULT false`);
            await pool.query(`UPDATE "Orders" SET "stockWatchActive"=true WHERE id=$1`, [o.id]);
            console.log(`[stock-watch] Registered watch for order ${o.id} product ${o.productId}`);
          } catch(e) { console.warn('[stock-watch setup]', e.message); }
        }
      }
      await notify(
        o.retailerId,
        action === 'confirm' ? 'Order Confirmed ✅' : 'Order Rejected ❌',
        msg,
        notifType,
        o.id
      );

      // ── AUTO BATCH: accumulate confirmed orders, dispatch optimised route every 10 orders ──
      const BATCH_SIZE = 3; // set to 10 for production
      if (action === 'confirm') {
        try {
          // 1. Auto-calculate kg from product weight if kg=0
          let orderKg = parseFloat(o.kg) || 0;
          if (orderKg === 0 && o.productId) {
            const wt = await pool.query(
              `SELECT "weightPerUnit" FROM "Stock" WHERE id=$1`, [o.productId]
            );
            if (wt.rows.length && wt.rows[0].weightPerUnit) {
              orderKg = parseFloat(wt.rows[0].weightPerUnit) * (parseInt(o.items) || 1);
              await pool.query(`UPDATE "Orders" SET kg=$1 WHERE id=$2`, [orderKg, o.id]);
              console.log(`[kg] Order ${o.id}: ${o.items} × ${wt.rows[0].weightPerUnit}kg = ${orderKg}kg`);
            }
          }

          // 2. Generate 4-digit delivery PIN
          const deliveryPin = String(Math.floor(1000 + Math.random() * 9000));

          // 3. Create delivery record with PIN
          try {
            await pool.query(
              `INSERT INTO "Deliveries"("orderId",status,"deliveryPin","createdAt") VALUES($1,'pending',$2,NOW())`,
              [o.id, deliveryPin]
            );
          } catch {
            // Add PIN columns if they don't exist yet
            await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "deliveryPin"   VARCHAR(4)`);
            await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "pinVerified"   BOOLEAN DEFAULT false`);
            await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "pinVerifiedAt" TIMESTAMPTZ`);
            await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "pinAttempts"   INTEGER DEFAULT 0`);
            await pool.query(
              `INSERT INTO "Deliveries"("orderId",status,"deliveryPin","createdAt") VALUES($1,'pending',$2,NOW())`,
              [o.id, deliveryPin]
            );
          }
          await pool.query(`UPDATE "Orders" SET status='consolidated' WHERE id=$1`, [o.id]);
          console.log(`[PIN] Generated ${deliveryPin} for order ${o.id} retailer ${o.retailerId}`);

          // 4. Notify retailer with PIN prominently
          await notify(
            o.retailerId,
            `✅ Order Confirmed — Your Delivery PIN: ${deliveryPin}`,
            `Your order ${o.id} to ${o.city} (${orderKg.toFixed(1)}kg) is confirmed and queued.

🔐 YOUR DELIVERY PIN: ${deliveryPin}

When your driver arrives, they will ask for this 4-digit PIN to verify your identity before handing over the goods.

⚠️ Do NOT share this PIN with anyone other than your delivery driver.`,
            'success', o.id
          );

          // 4. Count unassigned pending deliveries
          const pendingRes = await pool.query(`SELECT COUNT(*) FROM "Deliveries" WHERE status='pending'`);
          const pendingCount = parseInt(pendingRes.rows[0].count);

          // 5. Notify OPT of queue progress
          const optUsers = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
          for (const u of optUsers.rows) {
            const remaining = BATCH_SIZE - pendingCount;
            await notify(
              u.id,
              `📦 Order Queued (${pendingCount}/${BATCH_SIZE})`,
              pendingCount >= BATCH_SIZE
                ? `${pendingCount} orders ready — auto-route is being created now!`
                : `Order ${o.id} queued. ${remaining} more needed to trigger auto-route.`,
              pendingCount >= BATCH_SIZE ? 'success' : 'info', o.id
            );
          }

          // 6. Trigger route creation when batch is full
          if (pendingCount >= BATCH_SIZE) {

            // Fetch all pending deliveries — include calculated kg
            const batchRes = await pool.query(
              `SELECT d.id AS "deliveryId", o."retailerName", o.city, o.items,
                      COALESCE(o.kg, 0) AS kg, o.priority, o."retailerId", o.id AS "orderId",
                      o."productId"
               FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
               WHERE d.status='pending'
               ORDER BY CASE o.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
                        d."createdAt" ASC`
            );
            let batch = batchRes.rows;

            // Auto-fix any still-zero kg using Stock table
            for (const row of batch) {
              if ((parseFloat(row.kg)||0) === 0 && row.productId) {
                const wt = await pool.query(`SELECT "weightPerUnit" FROM "Stock" WHERE id=$1`, [row.productId]);
                if (wt.rows.length && wt.rows[0].weightPerUnit) {
                  row.kg = parseFloat(wt.rows[0].weightPerUnit) * (parseInt(row.items)||1);
                  await pool.query(`UPDATE "Orders" SET kg=$1 WHERE id=$2`, [row.kg, row.orderId]);
                }
              }
            }

            // Haversine distance helper
            const hv = (a,b) => {
              const R=6371, dLat=(b.lat-a.lat)*Math.PI/180, dLng=(b.lng-a.lng)*Math.PI/180;
              const x=Math.sin(dLat/2)**2+Math.cos(a.lat*Math.PI/180)*Math.cos(b.lat*Math.PI/180)*Math.sin(dLng/2)**2;
              return R*2*Math.atan2(Math.sqrt(x),Math.sqrt(1-x));
            };

            // Priority score: urgent=0, high=1, normal=2
            const priScore = p => p==='urgent'?0:p==='high'?1:2;

            const CC = {
              'Colombo':{lat:6.9271,lng:79.8612},'Galle':{lat:6.0535,lng:80.2210},
              'Kandy':{lat:7.2906,lng:80.6337},'Kurunegala':{lat:7.4875,lng:80.3647},
              'Matara':{lat:5.9549,lng:80.5550},'Negombo':{lat:7.2096,lng:79.8386},
              'Ratnapura':{lat:6.6828,lng:80.4004},'Jaffna':{lat:9.6615,lng:80.0255},
              'Peradeniya':{lat:7.2676,lng:80.5957},'Dehiwala':{lat:6.8519,lng:79.8674},
              'Nugegoda':{lat:6.8728,lng:79.8880},'Kiribathgoda':{lat:7.0003,lng:80.0170},
            };
            const WH = {
              'Colombo':{lat:6.9271,lng:79.8612},'Galle':{lat:6.0535,lng:80.2210},
              'Kandy':{lat:7.2906,lng:80.6337},'Kurunegala':{lat:7.4875,lng:80.3647},
            };

            // ── SMART ROUTING: Priority-first + nearest-neighbour within each tier ──
            // Step 1: group by priority
            const groups = { urgent:[], high:[], normal:[] };
            batch.forEach(d => {
              const p = d.priority==='urgent'?'urgent':d.priority==='high'?'high':'normal';
              groups[p].push(d);
            });

            // Step 2: nearest-neighbour within each priority group
            function nearestNeighbour(items, startCoord) {
              const rem = [...items];
              const result = [];
              let cur = startCoord;
              while (rem.length) {
                let bi=0, bd=Infinity;
                rem.forEach((d,i)=>{ const c=CC[d.city]||CC['Colombo']; const dist=hv(cur,c); if(dist<bd){bd=dist;bi=i;} });
                const ch=rem.splice(bi,1)[0]; result.push(ch); cur=CC[ch.city]||CC['Colombo'];
              }
              return result;
            }

            // Step 3: find nearest warehouse to first urgent/high stop (or normal if no others)
            const firstGroup = groups.urgent.length ? groups.urgent :
                               groups.high.length   ? groups.high   : groups.normal;
            const tempFirst = nearestNeighbour(firstGroup, WH['Colombo']);
            const fc = CC[tempFirst[0]?.city] || CC['Colombo'];
            let bestWH='Colombo', bestWHD=Infinity;
            for (const [wn,wc] of Object.entries(WH)) {
              const d=hv(fc,wc); if(d<bestWHD){bestWHD=d;bestWH=wn;}
            }
            const whCoord = WH[bestWH];

            // Step 4: optimise each group from current position, chain groups
            let cur2 = whCoord;
            const optimised = [];
            for (const tier of ['urgent','high','normal']) {
              if (!groups[tier].length) continue;
              const ordered = nearestNeighbour(groups[tier], cur2);
              optimised.push(...ordered);
              cur2 = CC[ordered[ordered.length-1].city] || CC['Colombo'];
            }

            // ── Calculate total weight of entire route
            const totalKg = optimised.reduce((sum,d) => sum + (parseFloat(d.kg)||0), 0);
            console.log(`[route] totalKg=${totalKg.toFixed(1)}, warehouse=${bestWH}, stops=${optimised.length}`);
            console.log(`[route] urgent=${groups.urgent.length} high=${groups.high.length} normal=${groups.normal.length}`);

            // Find available driver
            const drvRes = await pool.query(
              `SELECT d.id, d.name, COALESCE(u.id, d."userId", d.id) AS "userId"
               FROM "Drivers" d
               LEFT JOIN "Users" u ON lower(u.name)=lower(d.name) AND u.role='distributor'
               WHERE NOT EXISTS (
                 SELECT 1 FROM "Deliveries" del
                 WHERE del.status IN ('assigned','warehouse_ready','loaded','in-transit')
                   AND (
                     del."driverId" = d.id
                     OR (u.id IS NOT NULL AND del."driverId" = u.id)
                     OR (d."userId" IS NOT NULL AND del."driverId" = d."userId")
                   )
               ) LIMIT 1`
            );
            console.log('[route] available drivers:', drvRes.rows.length, drvRes.rows.map(r=>r.name));

            // Find smallest available vehicle that fits total cargo weight
            // cap is stored as plain numeric text e.g. "5000", "1500", "800"
            const allVeh = await pool.query(
              `SELECT id, plate, type, cap FROM "Vehicles" v
               WHERE NOT EXISTS (
                 SELECT 1 FROM "Deliveries" del
                 WHERE del.status IN ('assigned','warehouse_ready','loaded','in-transit')
                   AND del."vehicleId"=v.id
               ) ORDER BY id`
            );
            console.log('[route] all free vehicles:', allVeh.rows.map(r=>r.id+'(cap:'+r.cap+')'));

            // Pick smallest vehicle whose cap >= totalKg (fallback to any if totalKg=0)
            const freeVehicles = allVeh.rows.map(v => ({
              ...v,
              capNum: parseFloat(String(v.cap).replace(/[^0-9.]/g,'')) || 0
            }));
            const fittingVehicles = totalKg > 0
              ? freeVehicles.filter(v => v.capNum === 0 || v.capNum >= totalKg)
              : freeVehicles;
            fittingVehicles.sort((a,b) => a.capNum - b.capNum);
            const vehRes = { rows: fittingVehicles.length ? [fittingVehicles[0]] : [] };
            console.log('[route] selected vehicle:', vehRes.rows.map(r=>r.id+'(cap:'+r.capNum+'kg, needed:'+totalKg.toFixed(1)+'kg)'));

            console.log('[route] ASSIGNING: drivers='+drvRes.rows.length+' vehicles='+vehRes.rows.length+' totalKg='+totalKg.toFixed(1));
            if (drvRes.rows.length && vehRes.rows.length) {
              const drv=drvRes.rows[0], veh=vehRes.rows[0];
              console.log('[route] Assigning to driver:', drv.name, '| vehicle:', veh.id, '| cap:', veh.capNum||veh.cap, 'kg');

              // Build ETAs — 45 min per stop from now+1hr
              let dH=new Date().getHours()+1, dM=0;
              const stopsData = optimised.map((d,i) => {
                dH+=Math.floor((dM+45)/60); dM=(dM+45)%60;
                const eta=`${String(dH%24).padStart(2,'0')}:${String(dM).padStart(2,'0')}`;
                return { deliveryId:d.deliveryId, retailer:d.retailerName, city:d.city, items:d.items, priority:d.priority, eta, stopNote:'' };
              });

              // Total distance
              let totDist=0; let prev=WH[bestWH];
              optimised.forEach(d=>{ const c=CC[d.city]||CC['Colombo']; totDist+=hv(prev,c); prev=c; });
              const distKm=Math.round(totDist*1.3), durMins=Math.round(distKm*2.5);
              const cities=optimised.map(d=>d.city);

              // Assign all deliveries
              for(const stop of stopsData) {
                await pool.query(
                  `UPDATE "Deliveries" SET "driverId"=$1,"driverName"=$2,"vehicleId"=$3,status='assigned',eta=$4 WHERE id=$5`,
                  [drv.id,drv.name,veh.id,stop.eta,stop.deliveryId]
                );
              }

              // Create single route record
              try {
                await pool.query(
                  `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"stops_data","warehouse","createdAt")
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())`,
                  [drv.id,drv.name,veh.id,optimised.length,distKm,durMins,
                   JSON.stringify(cities),JSON.stringify(stopsData),bestWH]
                );
              } catch(re){ console.error('[auto-route]',re.message); }

              // Notify driver — full briefing
              const stopLines=stopsData.map((s,i)=>`  Stop ${i+1}: ${s.city} — ${s.retailer} (${s.items} items, ${parseFloat(optimised[i]?.kg||0).toFixed(1)}kg) [${s.priority||'normal'}] ETA ${s.eta}`).join('\n');
              await notify(
                drv.userId||drv.id,
                `🗺️ Auto-Route — ${optimised.length} Stops · ${totalKg.toFixed(1)}kg`,
                `New priority-optimised route assigned.\n\nVehicle: ${veh.id} (cap: ${veh.cap}kg)\nDeparting: ${bestWH} Warehouse\nTotal cargo: ${totalKg.toFixed(1)}kg\n${distKm} km · ~${Math.floor(durMins/60)}h ${durMins%60}m\n\nStop sequence (priority-ordered):\n${stopLines}\n\nCheck "My Route Briefing".`,
                'info'
              );

              // Notify warehouse
              const whUsers=await pool.query(`SELECT id FROM "Users" WHERE role='warehouse'`);
              const cargoList=stopsData.map((s,i)=>`Stop ${i+1}[${s.priority||'normal'}]: ${s.city} — ${s.retailer}, ${s.items} items (${parseFloat(optimised[i]?.kg||0).toFixed(1)}kg)`).join('\n');
              for(const u of whUsers.rows){
                await notify(u.id,`📦 Prepare Route — ${optimised.length} Stops · ${totalKg.toFixed(1)}kg`,
                  `Driver: ${drv.name}\nVehicle: ${veh.id} (capacity: ${veh.cap}kg)\nWarehouse: ${bestWH}\nTotal cargo weight: ${totalKg.toFixed(1)}kg\n\nCargo by stop:\n${cargoList}`,
                  'info'
                );
              }

              // Notify each retailer
              for(const stop of stopsData){
                const ord=optimised.find(d=>d.deliveryId===stop.deliveryId);
                if(ord) await notify(ord.retailerId,'🚚 Your Delivery is Scheduled',
                  `Your order to ${stop.city} is assigned. Driver: ${drv.name}, Vehicle: ${veh.id}. ETA: ${stop.eta}.`,
                  'success',ord.orderId
                );
              }

              // Notify OPT — summary
              const urgentCount = groups.urgent.length, highCount = groups.high.length, normalCount = groups.normal.length;
              for(const u of optUsers.rows){
                await notify(u.id,`✅ Route Created — ${optimised.length} Stops · ${totalKg.toFixed(1)}kg`,
                  `Priority-optimised route dispatched to ${drv.name} (${veh.id}) from ${bestWH} Warehouse.\n${distKm} km · ${Math.floor(durMins/60)}h ${durMins%60}m · cargo: ${totalKg.toFixed(1)}kg/${veh.cap}kg\nStop breakdown: ${urgentCount} urgent · ${highCount} high · ${normalCount} normal`,
                  'success'
                );
              }

            } else {
              // No driver/vehicle — warn OPT with weight info
              const reason = !drvRes.rows.length && !vehRes.rows.length
                ? 'No available driver or vehicle found'
                : !drvRes.rows.length
                ? 'No available driver found (all drivers busy)'
                : `No vehicle with capacity ≥ ${totalKg.toFixed(1)}kg is available`;
              console.error('[route] Cannot assign:', reason, '| totalKg:', totalKg, '| drivers:', drvRes.rows.length, '| vehicles:', vehRes.rows.length);
              for(const u of optUsers.rows){
                await notify(u.id,'⚠️ Batch Ready — Cannot Assign',
                  `${pendingCount} orders queued (total ${totalKg.toFixed(1)}kg) but route could not be auto-assigned.\nReason: ${reason}.\nCheck that at least one driver and one vehicle are free.`,
                  'warning'
                );
              }
            }
          }
        } catch(autoErr) {
          console.error('[auto-batch] CRITICAL ERROR:', autoErr.message, autoErr.stack);
          try {
            const optFail = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
            for (const u of optFail.rows) {
              await notify(
                u.id,
                '🔴 Auto-Route Failed (System Error)',
                `Auto-assignment crashed with error: ${autoErr.message}. Please assign the batch manually.`,
                'alert'
              );
            }
          } catch {}
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
      // Resolve both Users.id and linked Drivers.id so deliveries are found regardless of which ID was stored
      r = await pool.query(
        `SELECT d.*, o."retailerName" AS retailer, o.city, o.items, o.kg, o.priority AS prio
         FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
         WHERE d."driverId" = $1
            OR d."driverId" = (
              SELECT dr.id FROM "Drivers" dr
              JOIN "Users" u ON lower(u.name) = lower(dr.name)
              WHERE u.id = $1 AND u.role = 'distributor' LIMIT 1
            )
         ORDER BY d."createdAt" DESC`,
        [userId]
      );
    } else {
      r = await pool.query(
        `SELECT d.*, o."retailerName" AS retailer, o.city, o.items, o.kg, o.priority AS prio,
         TO_CHAR(d."receiptAt" AT TIME ZONE 'Asia/Colombo', 'DD Mon YYYY HH24:MI') AS "receiptAt"
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

    // Notify warehouse only
    const notifyUsers = await pool.query(`SELECT id FROM "Users" WHERE role='warehouse'`);
    for (const u of notifyUsers.rows) {
      await notify(u.id, '📦 Deliveries Ready', `${created} delivery record(s) incoming. Please prepare cargo.`, 'info');
    }

    res.json({ created });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/assign', auth, async (req, res) => {
  const { driverId, driverName, vehicleId, etaOverride, deliveryNotes } = req.body;

  let eta;
  if (etaOverride) {
    eta = etaOverride;
  } else {
    const etaDate = new Date(Date.now() + 2*60*60*1000);
    eta = etaDate.toLocaleString('en-LK', { timeZone:'Asia/Colombo', hour:'2-digit', minute:'2-digit', hour12:false });
  }

  try {
    // Block if delivery itself is locked
    const check = await pool.query('SELECT status, "driverId" AS "oldDriverId" FROM "Deliveries" WHERE id=$1', [req.params.id]);
    if (!check.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const current = check.rows[0];
    if (['in-transit', 'delivered', 'failed'].includes(current.status)) {
      return res.status(409).json({ error: `Cannot reassign — delivery is already "${current.status}". Route is locked once in transit or completed.` });
    }

    // Block: driver has any incomplete delivery — resolve both Drivers.id and Users.id
    // First get the linked Users.id for this Drivers.id (may be same or different)
    const driverLinked = await pool.query(
      `SELECT COALESCE(u.id, $1::int) AS uid
       FROM "Drivers" dr
       LEFT JOIN "Users" u ON lower(u.name) = lower(dr.name) AND u.role = 'distributor'
       WHERE dr.id = $1 LIMIT 1`,
      [driverId]
    );
    const linkedUserId = driverLinked.rows[0]?.uid || driverId;
    const driverBusy = await pool.query(
      `SELECT id FROM "Deliveries"
       WHERE id != $3
         AND status IN ('assigned','warehouse_ready','loaded','in-transit')
         AND "driverId" IN ($1, $2)
       LIMIT 1`,
      [driverId, linkedUserId, req.params.id]
    );
    if (driverBusy.rows.length) {
      return res.status(409).json({
        error: `Driver ${driverName} has not completed delivery #${driverBusy.rows[0].id}. All deliveries must be completed before assigning a new one.`
      });
    }

    // Block: vehicle has any incomplete delivery
    const vehicleBusy = await pool.query(
      `SELECT "driverName" FROM "Deliveries"
       WHERE "vehicleId"=$1 AND id != $2
         AND status IN ('assigned','warehouse_ready','loaded','in-transit') LIMIT 1`,
      [vehicleId, req.params.id]
    );
    if (vehicleBusy.rows.length) {
      return res.status(409).json({
        error: `Vehicle ${vehicleId} has an incomplete delivery with driver ${vehicleBusy.rows[0].driverName}. It cannot be assigned until that delivery is completed.`
      });
    }

    // Block: vehicle capacity exceeded
    const thisKgRow = await pool.query(
      `SELECT COALESCE(o.kg,0) AS kg FROM "Deliveries" d
       JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`, [req.params.id]
    );
    const thisKg = parseFloat(thisKgRow.rows[0]?.kg) || 0;
    const capRow = await pool.query('SELECT cap FROM "Vehicles" WHERE id=$1', [vehicleId]);
    if (capRow.rows.length) {
      const cap = parseFloat(capRow.rows[0].cap) || 0;
      if (cap > 0) {
        const loadedRow = await pool.query(
          `SELECT COALESCE(SUM(o.kg),0) AS total FROM "Deliveries" d
           JOIN "Orders" o ON d."orderId"=o.id
           WHERE d."vehicleId"=$1 AND d.id != $2 AND d.status = 'in-transit'`,
          [vehicleId, req.params.id]
        );
        const usedKg = parseFloat(loadedRow.rows[0].total) || 0;
        if (usedKg + thisKg > cap) {
          return res.status(409).json({
            error: `Vehicle ${vehicleId} overloaded. Capacity: ${cap}kg, In transit: ${usedKg}kg, This delivery: ${thisKg}kg.`
          });
        }
      }
    }

    // UPDATE DB first, then notify old driver
    const oldDriverId = parseInt(current.oldDriverId);
    await pool.query(
      'UPDATE "Deliveries" SET "driverId"=$1,"driverName"=$2,"vehicleId"=$3,status=\'assigned\',eta=$4 WHERE id=$5',
      [driverId, driverName, vehicleId, eta, req.params.id]
    );

    // Notify removed driver AFTER DB is updated
    if (oldDriverId && oldDriverId !== parseInt(driverId)) {
      const oldUid = await driverUserId(oldDriverId);
      if (oldUid) {
        await notify(oldUid, 'Delivery Reassigned ↩️',
          `Delivery ${req.params.id} has been reassigned to another driver. It has been removed from your route.`,
          'warning', req.params.id);
      }
    }

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
      const _assignUid = await driverUserId(parseInt(driverId));
      await notify(
        _assignUid,
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
    const rawDriverId = d && d.driverId ? Number(d.driverId) : null;
    if (rawDriverId) {
      const notifyUid = await driverUserId(rawDriverId);
      const targetUid = notifyUid || rawDriverId;
      await notify(
        targetUid,
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
    const rawDriverIdL = d && d.driverId ? Number(d.driverId) : null;
    if (rawDriverIdL) {
      const notifyUidL = await driverUserId(rawDriverIdL);
      const targetUidL = notifyUidL || rawDriverIdL;
      await notify(
        targetUidL,
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

// ══════════════════════════════════════════════
// EXCEPTION HANDLING HELPERS
// ══════════════════════════════════════════════

// Detect failure reason from driver note
function classifyFailure(note) {
  const n = (note||'').toLowerCase();
  if (n.includes('breakdown') || n.includes('broke down') || n.includes('puncture') ||
      n.includes('accident') || n.includes('engine') || n.includes('tyre') || n.includes('tire')) {
    return 'vehicle_breakdown';
  }
  if (n.includes('stock') || n.includes('out of') || n.includes('no stock') ||
      n.includes('unavailable') || n.includes('not available') || n.includes('missing')) {
    return 'out_of_stock';
  }
  if (n.includes('refused') || n.includes('reject') || n.includes('not accept')) {
    return 'customer_refused';
  }
  if (n.includes('address') || n.includes('wrong') || n.includes('location') || n.includes('not found')) {
    return 'wrong_address';
  }
  if (n.includes('closed') || n.includes('absent') || n.includes('unavailable') || n.includes('no one')) {
    return 'customer_absent';
  }
  return 'other';
}

// Auto-reassign delivery to next available driver
async function autoReassign(deliveryId, currentDriverId, reason) {
  try {
    // Find next available driver (excluding current)
    const drvRes = await pool.query(
      `SELECT d.id, d.name, COALESCE(u.id, d."userId", d.id) AS "userId"
       FROM "Drivers" d
       LEFT JOIN "Users" u ON lower(u.name)=lower(d.name) AND u.role='distributor'
       WHERE d.id != $1
         AND NOT EXISTS (
           SELECT 1 FROM "Deliveries" del
           WHERE del.status IN ('assigned','warehouse_ready','loaded','in-transit')
             AND (del."driverId"=d.id OR (u.id IS NOT NULL AND del."driverId"=u.id))
         ) LIMIT 1`,
      [currentDriverId]
    );

    if (!drvRes.rows.length) return { reassigned: false, reason: 'No available driver found' };

    const newDrv = drvRes.rows[0];

    // Reset delivery to assigned with new driver
    await pool.query(
      `UPDATE "Deliveries"
       SET status='assigned', "driverId"=$1, "driverName"=$2, "updatedAt"=NOW()
       WHERE id=$3`,
      [newDrv.id, newDrv.name, deliveryId]
    );

    console.log(`[auto-reassign] Delivery ${deliveryId} reassigned to ${newDrv.name} — reason: ${reason}`);
    return { reassigned: true, driverName: newDrv.name, driverUserId: newDrv.userId };
  } catch(e) {
    console.error('[auto-reassign error]', e.message);
    return { reassigned: false, reason: e.message };
  }
}

app.put('/api/deliveries/:id/status', auth, async (req, res) => {
  const { newStatus, note, updatedBy, failType } = req.body;
  const validStatuses = ['in-transit', 'delivered', 'failed'];

  if (!validStatuses.includes(newStatus)) {
    return res.status(400).json({ error: 'Invalid status value' });
  }
  if (newStatus === 'failed' && (!note || note.trim() === '')) {
    return res.status(400).json({ error: 'A reason is required when marking a delivery as failed' });
  }

  try {
    const cur = await pool.query(
      `SELECT d.status, d."driverId", d."driverName", d."vehicleId", d.eta,
              o."retailerId", o."retailerName" AS retailer, o.city, o.items, o.kg, o.priority
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    if (!cur.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const current = cur.rows[0];

    if (['delivered', 'failed'].includes(current.status)) {
      return res.status(409).json({ error: `Delivery is already "${current.status}" — cannot change.` });
    }
    if (['assigned', 'warehouse_ready'].includes(current.status)) {
      return res.status(403).json({ error: 'Warehouse has not finished loading yet. You will be notified when ready.' });
    }

    // ── Classify failure reason
    const failureType = newStatus === 'failed'
      ? (failType || classifyFailure(note))
      : null;

    // ── Update status
    await pool.query(
      `UPDATE "Deliveries" SET status=$1, "updatedAt"=NOW() WHERE id=$2`,
      [newStatus, req.params.id]
    );

    const noteStr   = note && note.trim() ? ` Note: ${note.trim()}` : '';
    const driverStr = current.driverName ? ` Driver: ${current.driverName}.` : '';
    const optUsers  = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
    const whUsers   = await pool.query(`SELECT id FROM "Users" WHERE role='warehouse'`);
    const driverId  = current.driverId ? Number(current.driverId) : null;

    // ══════════════════════════════════════════
    // CASE 1: DELIVERED ✅
    // ══════════════════════════════════════════
    if (newStatus === 'delivered') {
      await notify(current.retailerId, '✅ Delivery Successful',
        `Your delivery to ${current.city} has been delivered successfully.${noteStr} Please confirm receipt in the app.`,
        'success', req.params.id);

      for (const u of optUsers.rows) {
        await notify(u.id, '✅ Delivered — ' + current.city,
          `Delivery ${req.params.id} → ${current.city} (${current.retailer}) confirmed delivered.${driverStr}${noteStr}`,
          'success', req.params.id);
      }
      for (const u of whUsers.rows) {
        await notify(u.id, '✅ Delivered — Bay Clear',
          `Delivery ${req.params.id} to ${current.city} delivered. Vehicle ${current.vehicleId} can return or proceed to next stop.`,
          'success', req.params.id);
      }
      if (driverId) {
        await notify(driverId, '✅ Delivery Confirmed',
          `Delivery ${req.params.id} to ${current.city} (${current.retailer}) marked as delivered. Check your route for remaining stops.`,
          'success', req.params.id);
      }
      return res.json({ ok: true, newStatus, failureType: null, notified: 4 });
    }

    // ══════════════════════════════════════════
    // CASE 2: FAILED — VEHICLE BREAKDOWN 🔧
    // ══════════════════════════════════════════
    if (newStatus === 'failed' && failureType === 'vehicle_breakdown') {
      // Mark vehicle as unavailable
      await pool.query(
        `UPDATE "Vehicles" SET status='breakdown' WHERE id=$1`,
        [current.vehicleId]
      ).catch(() => {}); // graceful if column doesn't exist

      // Try auto-reassign to another driver
      const reassign = driverId
        ? await autoReassign(req.params.id, driverId, 'vehicle_breakdown')
        : { reassigned: false };

      // Notify driver
      if (driverId) {
        await notify(driverId, '🔧 Breakdown Recorded',
          `Delivery ${req.params.id} marked as failed due to vehicle breakdown.${noteStr} Please contact your supervisor immediately and stay with the vehicle.`,
          'alert', req.params.id);
      }

      // Notify OPT — urgent
      for (const u of optUsers.rows) {
        await notify(u.id, '🚨 VEHICLE BREAKDOWN — Delivery ' + req.params.id,
          `Driver ${current.driverName} has reported a vehicle breakdown (${current.vehicleId}) on route to ${current.city}.${noteStr}

` +
          (reassign.reassigned
            ? `✅ Auto-reassigned to ${reassign.driverName}. Please coordinate warehouse re-loading.`
            : `⚠️ No available driver for auto-reassignment. Manual action required.`),
          'alert', req.params.id);
      }

      // Notify warehouse
      for (const u of whUsers.rows) {
        await notify(u.id, '🔧 Vehicle Breakdown — ' + current.vehicleId,
          `Vehicle ${current.vehicleId} has broken down on delivery ${req.params.id} to ${current.city}.
` +
          (reassign.reassigned
            ? `Cargo may need to be transferred to another vehicle for driver ${reassign.driverName}.`
            : `No replacement driver available yet. Await OPT instructions.`),
          'alert', req.params.id);
      }

      // Notify retailer
      await notify(current.retailerId, '⚠️ Delivery Delayed — Vehicle Issue',
        `We are sorry, your delivery to ${current.city} has been delayed due to a vehicle issue on our end.` +
        (reassign.reassigned
          ? ` We have assigned a replacement driver and will deliver as soon as possible.`
          : ` Our team is arranging a replacement and will update you shortly.`),
        'warning', req.params.id);

      // Notify new driver if reassigned
      if (reassign.reassigned && reassign.driverUserId) {
        await notify(reassign.driverUserId, '🚨 Urgent — Breakdown Reassignment',
          `Delivery ${req.params.id} to ${current.city} (${current.retailer}, ${current.items} items) has been reassigned to you due to a vehicle breakdown. Please coordinate with warehouse for cargo transfer.`,
          'alert', req.params.id);
      }

      return res.json({ ok: true, newStatus, failureType, reassigned: reassign.reassigned, newDriver: reassign.driverName || null });
    }

    // ══════════════════════════════════════════
    // CASE 3: FAILED — OUT OF STOCK 📦
    // ══════════════════════════════════════════
    if (newStatus === 'failed' && failureType === 'out_of_stock') {
      // Notify OPT to reorder
      for (const u of optUsers.rows) {
        await notify(u.id, '📦 Out of Stock — Delivery ' + req.params.id,
          `Delivery ${req.params.id} to ${current.city} (${current.retailer}) failed due to stock unavailability.${noteStr}

Action required: Check Stock table and reorder ${current.items} units. Retailer has been notified.`,
          'alert', req.params.id);
      }

      // Notify warehouse to audit stock
      for (const u of whUsers.rows) {
        await notify(u.id, '📦 Stock Issue — Immediate Audit Required',
          `Delivery ${req.params.id} failed — driver reported stock unavailable for ${current.retailer} (${current.city}, ${current.items} items).${noteStr}
Please audit current stock levels and update the system immediately.`,
          'alert', req.params.id);
      }

      // Notify retailer with apology and reschedule promise
      await notify(current.retailerId, '📦 Delivery Failed — Stock Issue',
        `We sincerely apologise — your delivery to ${current.city} could not be completed due to a stock shortage on our end. Our team is resolving this and will reschedule your delivery at the earliest opportunity.`,
        'alert', req.params.id);

      if (driverId) {
        await notify(driverId, '📦 Out of Stock Recorded',
          `Delivery ${req.params.id} recorded as failed due to stock issue.${noteStr} Return to warehouse and inform the team.`,
          'info', req.params.id);
      }

      return res.json({ ok: true, newStatus, failureType, notified: 4 });
    }

    // ══════════════════════════════════════════
    // CASE 4: FAILED — CUSTOMER ABSENT / REFUSED
    // ══════════════════════════════════════════
    if (newStatus === 'failed' && (failureType === 'customer_absent' || failureType === 'customer_refused')) {
      const isRefused = failureType === 'customer_refused';

      await notify(current.retailerId,
        isRefused ? '❌ Delivery Refused' : '❌ Delivery Attempted — You Were Unavailable',
        isRefused
          ? `Our driver attempted delivery to ${current.city} but the delivery was refused.${noteStr} Please contact us to discuss and reschedule.`
          : `Our driver attempted delivery to ${current.city} but no one was available to receive it.${noteStr} Please confirm your availability for rescheduling.`,
        'alert', req.params.id);

      for (const u of optUsers.rows) {
        await notify(u.id,
          isRefused ? '❌ Customer Refused — ' + current.city : '❌ Customer Absent — ' + current.city,
          `Delivery ${req.params.id} to ${current.retailer} (${current.city}) failed — ${isRefused ? 'customer refused' : 'customer absent'}.${noteStr}
Please contact retailer to reschedule.`,
          'warning', req.params.id);
      }

      if (driverId) {
        await notify(driverId, isRefused ? '❌ Refusal Recorded' : '❌ Absence Recorded',
          `Delivery ${req.params.id} to ${current.city} recorded as failed (${isRefused ? 'refused' : 'absent'}).${noteStr} Proceed to next stop.`,
          'info', req.params.id);
      }

      return res.json({ ok: true, newStatus, failureType, notified: 3 });
    }

    // ══════════════════════════════════════════
    // CASE 5: FAILED — WRONG ADDRESS
    // ══════════════════════════════════════════
    if (newStatus === 'failed' && failureType === 'wrong_address') {
      await notify(current.retailerId, '❌ Delivery Failed — Address Issue',
        `Our driver could not locate your delivery address in ${current.city}.${noteStr} Please update your address details and contact us to reschedule.`,
        'alert', req.params.id);

      for (const u of optUsers.rows) {
        await notify(u.id, '❌ Wrong Address — ' + current.city,
          `Delivery ${req.params.id} to ${current.retailer} failed — address could not be found.${noteStr}
Please verify retailer address and reschedule.`,
          'warning', req.params.id);
      }

      if (driverId) {
        await notify(driverId, '❌ Address Issue Recorded',
          `Delivery ${req.params.id} recorded as failed due to address issue.${noteStr} Proceed to next stop.`,
          'info', req.params.id);
      }

      return res.json({ ok: true, newStatus, failureType, notified: 3 });
    }

    // ══════════════════════════════════════════
    // CASE 6: IN TRANSIT or OTHER FAILED
    // ══════════════════════════════════════════
    const statusLabel = { 'in-transit':'In Transit 🚛', 'delivered':'Delivered ✅', 'failed':'Delivery Failed ❌' };
    const msgType     = { 'in-transit':'info', 'delivered':'success', 'failed':'alert' };

    await notify(current.retailerId, `Delivery ${statusLabel[newStatus]}`,
      newStatus === 'in-transit'
        ? `Your delivery to ${current.city} is now on its way 🚛.${driverStr} Expected arrival: ${current.eta || 'soon'}.`
        : `Your delivery to ${current.city} could not be completed.${noteStr} Our team will contact you to reschedule.`,
      msgType[newStatus], req.params.id);

    for (const u of [...optUsers.rows, ...whUsers.rows]) {
      await notify(u.id, `Delivery ${statusLabel[newStatus]}`,
        `Delivery ${req.params.id} → ${current.city} (${current.retailer}).${driverStr} Status: ${newStatus}.${noteStr}`,
        msgType[newStatus], req.params.id);
    }

    if (driverId) {
      await notify(driverId, `Your Delivery: ${statusLabel[newStatus]}`,
        newStatus === 'in-transit'
          ? `Delivery ${req.params.id} to ${current.city} marked in-transit.${noteStr}`
          : `Delivery ${req.params.id} to ${current.city} recorded as failed.${noteStr}`,
        msgType[newStatus], req.params.id);
    }

    res.json({ ok: true, newStatus, failureType, notified: 4 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Vehicle Breakdown Report (standalone endpoint)
app.post('/api/vehicles/:id/breakdown', auth, async (req, res) => {
  const { note, driverDeliveryId } = req.body;
  const vehicleId = req.params.id;
  try {
    // Mark all active deliveries for this vehicle as failed
    const activeDeliveries = await pool.query(
      `SELECT d.id, d."driverId", d."driverName", o.city, o."retailerName" AS retailer, o."retailerId", o.items
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
       WHERE d."vehicleId"=$1 AND d.status='in-transit'`,
      [vehicleId]
    );

    const optUsers = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
    const whUsers  = await pool.query(`SELECT id FROM "Users" WHERE role='warehouse'`);

    for (const del of activeDeliveries.rows) {
      await pool.query(
        `UPDATE "Deliveries" SET status='failed', "updatedAt"=NOW() WHERE id=$1`,
        [del.id]
      );

      // Try auto-reassign
      const reassign = del.driverId
        ? await autoReassign(del.id, del.driverId, 'vehicle_breakdown')
        : { reassigned: false };

      // Notify retailer
      await notify(del.retailerId, '🚨 Delivery Delayed — Vehicle Breakdown',
        `We apologise — your delivery to ${del.city} has been delayed due to a vehicle breakdown.` +
        (reassign.reassigned ? ` A replacement driver has been arranged.` : ` Our team is arranging a replacement urgently.`),
        'alert', del.id);

      // Notify new driver
      if (reassign.reassigned && reassign.driverUserId) {
        await notify(reassign.driverUserId, '🚨 Emergency Reassignment',
          `You have been assigned delivery ${del.id} to ${del.city} (${del.retailer}, ${del.items} items) — original driver's vehicle broke down. Please proceed to warehouse immediately.`,
          'alert', del.id);
      }
    }

    // Notify OPT and warehouse
    for (const u of optUsers.rows) {
      await notify(u.id, `🚨 Vehicle Breakdown — ${vehicleId}`,
        `Vehicle ${vehicleId} has broken down. ${activeDeliveries.rows.length} active deliveries affected.${note ? ' Note: '+note : ''}
Auto-reassignment attempted for all affected deliveries.`,
        'alert');
    }
    for (const u of whUsers.rows) {
      await notify(u.id, `🔧 Vehicle ${vehicleId} — Breakdown`,
        `Vehicle ${vehicleId} is out of service. ${activeDeliveries.rows.length} deliveries need cargo transfer. Await OPT instructions.`,
        'alert');
    }

    res.json({ ok: true, vehicleId, affectedDeliveries: activeDeliveries.rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Reschedule failed delivery
app.post('/api/deliveries/:id/reschedule', auth, async (req, res) => {
  try {
    const del = await pool.query(
      `SELECT d.*, o."retailerId", o."retailerName" AS retailer, o.city, o.items, o.kg, o.priority
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    if (!del.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const d = del.rows[0];

    if (d.status !== 'failed') {
      return res.status(400).json({ error: 'Only failed deliveries can be rescheduled' });
    }

    // Find available driver and vehicle
    const drvRes = await pool.query(
      `SELECT d.id, d.name, COALESCE(u.id, d."userId", d.id) AS "userId"
       FROM "Drivers" d
       LEFT JOIN "Users" u ON lower(u.name)=lower(d.name) AND u.role='distributor'
       WHERE NOT EXISTS (
         SELECT 1 FROM "Deliveries" del
         WHERE del.status IN ('assigned','warehouse_ready','loaded','in-transit')
           AND (del."driverId"=d.id OR (u.id IS NOT NULL AND del."driverId"=u.id))
       ) LIMIT 1`
    );
    const vehRes = await pool.query(
      `SELECT id, cap FROM "Vehicles"
       WHERE NOT EXISTS (
         SELECT 1 FROM "Deliveries" del
         WHERE del.status IN ('assigned','warehouse_ready','loaded','in-transit')
           AND del."vehicleId"=id
       ) LIMIT 1`
    );

    if (!drvRes.rows.length || !vehRes.rows.length) {
      return res.status(409).json({ error: 'No available driver or vehicle for rescheduling' });
    }

    const drv = drvRes.rows[0], veh = vehRes.rows[0];
    const newEta = new Date(Date.now() + 2*60*60*1000)
      .toLocaleTimeString('en-LK', { timeZone:'Asia/Colombo', hour:'2-digit', minute:'2-digit', hour12:false });

    await pool.query(
      `UPDATE "Deliveries"
       SET status='assigned', "driverId"=$1, "driverName"=$2, "vehicleId"=$3, eta=$4, "updatedAt"=NOW()
       WHERE id=$5`,
      [drv.id, drv.name, veh.id, newEta, req.params.id]
    );

    const optUsers = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
    const whUsers  = await pool.query(`SELECT id FROM "Users" WHERE role='warehouse'`);

    // Notify retailer
    await notify(d.retailerId, '🔄 Delivery Rescheduled',
      `Good news! Your delivery to ${d.city} has been rescheduled. New driver: ${drv.name}, Vehicle: ${veh.id}. Estimated arrival: ${newEta}.`,
      'success', req.params.id);

    // Notify new driver
    await notify(drv.userId, '📦 Rescheduled Delivery Assigned',
      `Delivery ${req.params.id} to ${d.city} (${d.retailer}, ${d.items} items) has been rescheduled and assigned to you. Vehicle: ${veh.id}. ETA: ${newEta}.`,
      'info', req.params.id);

    // Notify warehouse
    for (const u of whUsers.rows) {
      await notify(u.id, '🔄 Rescheduled — Prepare Cargo',
        `Delivery ${req.params.id} rescheduled. Driver: ${drv.name}, Vehicle: ${veh.id}. Please prepare cargo for ${d.city} (${d.retailer}, ${d.items} items).`,
        'info', req.params.id);
    }

    // Notify OPT
    for (const u of optUsers.rows) {
      await notify(u.id, '✅ Delivery Rescheduled',
        `Delivery ${req.params.id} to ${d.city} (${d.retailer}) rescheduled to ${drv.name} (${veh.id}). ETA: ${newEta}.`,
        'success', req.params.id);
    }

    res.json({ ok: true, driverName: drv.name, vehicleId: veh.id, eta: newEta });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════

// Publish a full multi-stop route plan
app.post('/api/routes/publish', auth, async (req, res) => {
  const { driverId, driverName, vehicleId, routeDate, depart, routeNotes, warehouse, stops, distKm, durMins, cities } = req.body;

  if (!driverId || !driverName || !vehicleId || !Array.isArray(stops) || !stops.length) {
    return res.status(400).json({ error: 'driverId, driverName, vehicleId, and stops[] are required' });
  }

  try {
    // Block: driver has any incomplete delivery — resolve both Drivers.id and Users.id
    const driverLinkedPub = await pool.query(
      `SELECT COALESCE(u.id, $1::int) AS uid
       FROM "Drivers" dr
       LEFT JOIN "Users" u ON lower(u.name) = lower(dr.name) AND u.role = 'distributor'
       WHERE dr.id = $1 LIMIT 1`,
      [driverId]
    );
    const linkedUserIdPub = driverLinkedPub.rows[0]?.uid || driverId;
    const driverBusyPub = await pool.query(
      `SELECT id FROM "Deliveries"
       WHERE status IN ('assigned','warehouse_ready','loaded','in-transit')
         AND "driverId" IN ($1, $2)
       LIMIT 1`,
      [driverId, linkedUserIdPub]
    );
    if (driverBusyPub.rows.length) {
      return res.status(409).json({
        error: `Driver ${driverName} has not completed delivery #${driverBusyPub.rows[0].id}. All deliveries must be completed before publishing a new route.`
      });
    }

    // Block: vehicle has any incomplete delivery for a different driver
    const vehicleBusyPub = await pool.query(
      `SELECT "driverName" FROM "Deliveries"
       WHERE "vehicleId"=$1 AND "driverId" != $2
         AND status IN ('assigned','warehouse_ready','loaded','in-transit') LIMIT 1`,
      [vehicleId, driverId]
    );
    if (vehicleBusyPub.rows.length) {
      return res.status(409).json({
        error: `Vehicle ${vehicleId} has an incomplete delivery with driver ${vehicleBusyPub.rows[0].driverName}. Choose a different vehicle.`
      });
    }

    const stopsData = JSON.stringify(stops);
    const citiesJson = JSON.stringify(cities || stops.map(s => s.city));

    // Insert route record — try with all columns first, fall back gracefully
    let routeId;
    try {
      const rr = await pool.query(
        `INSERT INTO "Routes"("driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"stops_data","routeDate","depart","routeNotes","warehouse","createdAt")
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW()) RETURNING id`,
        [driverId, driverName, vehicleId, stops.length, distKm, durMins, citiesJson, stopsData, routeDate||null, depart||null, routeNotes||null, warehouse||null]
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

    const warehouseLine = warehouse ? `Departing warehouse: ${warehouse}` : '';
    await notify(
      parseInt(driverId),
      `🗺️ Route Plan Published — ${stops.length} Stops`,
      `Your route has been planned and published.\n\n${dateLine}${dateLine&&departLine?'\n':''}${departLine}${warehouseLine?'\n'+warehouseLine:''}\nVehicle: ${vehicleId}\n${summaryLine}\n\nStop Sequence:\n${stopLines}${routeNotes ? '\n\nRoute Notes: ' + routeNotes : ''}\n\nCheck "My Route Briefing" for the full plan.`,
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
                  rt.cities, rt."stops_data", rt."routeDate", rt.depart, rt."routeNotes", rt.warehouse, rt."createdAt",
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
                  rt.cities, rt."stops_data", rt."routeDate", rt.depart, rt."routeNotes", rt.warehouse, rt."createdAt",
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
                "stops_data","routeDate","depart","routeNotes",warehouse,"createdAt"
         FROM "Routes"
         WHERE "driverId" = $1
            OR "driverId" = (
              SELECT dr.id FROM "Drivers" dr
              JOIN "Users" u ON lower(u.name) = lower(dr.name)
              WHERE u.id = $1 AND u.role = 'distributor' LIMIT 1
            )
         ORDER BY "createdAt" DESC`,
        [driverId]
      );
      routes = r.rows;
    } catch {
      // New columns don't exist yet — fall back to base columns only
      try {
        const r = await pool.query(
          `SELECT id,"driverId","driverName","vehicleId",stops,"distKm","durMins",cities,"createdAt"
           FROM "Routes"
           WHERE "driverId" = $1
              OR "driverId" = (
                SELECT dr.id FROM "Drivers" dr
                JOIN "Users" u ON lower(u.name) = lower(dr.name)
                WHERE u.id = $1 AND u.role = 'distributor' LIMIT 1
              )
           ORDER BY "createdAt" DESC`,
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
       WHERE (
         d."driverId" = $1
         OR d."driverId" = (
           SELECT dr.id FROM "Drivers" dr
           JOIN "Users" u ON lower(u.name) = lower(dr.name)
           WHERE u.id = $1 AND u.role = 'distributor' LIMIT 1
         )
       )
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
      createdAt:    new Date().toLocaleString('en-LK', { timeZone:'Asia/Colombo' }),
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
// ══════════════════════════════════════════════
// USER MANAGEMENT (admin only)
// ══════════════════════════════════════════════

function adminOnly(req, res, next) {
  if (req.user?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
  next();
}

app.get('/api/users', auth, adminOnly, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT id, name, "Email", role, avatar,
       TO_CHAR("createdAt" AT TIME ZONE 'Asia/Colombo', 'DD Mon YYYY') AS "createdAt"
       FROM "Users" ORDER BY role, name`
    );
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/users', auth, adminOnly, async (req, res) => {
  const { name, email, password, role, avatar } = req.body;
  const validRoles = ['retailer','order_team','warehouse','distributor'];
  if (!name||!name.trim())     return res.status(400).json({ error: 'Name is required' });
  if (!email||!email.trim())   return res.status(400).json({ error: 'Email is required' });
  if (!password||password.length<4) return res.status(400).json({ error: 'Password must be at least 4 characters' });
  if (!validRoles.includes(role)) return res.status(400).json({ error: 'Invalid role' });
  try {
    const exists = await pool.query('SELECT id FROM "Users" WHERE lower("Email")=lower($1)', [email]);
    if (exists.rows.length) return res.status(409).json({ error: `Email ${email} is already registered` });
    const hash = await bcrypt.hash(password, 10);
    const initials = (avatar&&avatar.trim()) ? avatar.trim().toUpperCase().slice(0,2)
      : name.trim().split(' ').map(w=>w[0]).join('').toUpperCase().slice(0,2);
    const r = await pool.query(
      `INSERT INTO "Users"(name,"Email","PasswordHash",role,avatar,"createdAt")
       VALUES($1,$2,$3,$4,$5,NOW()) RETURNING id,name,"Email",role,avatar`,
      [name.trim(),email.trim(),hash,role,initials]
    );
    res.json({ ok: true, user: r.rows[0] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/users/:id/reset-password', auth, adminOnly, async (req, res) => {
  const { password } = req.body;
  if (!password||password.length<4) return res.status(400).json({ error: 'Password must be at least 4 characters' });
  try {
    const hash = await bcrypt.hash(password, 10);
    const r = await pool.query('UPDATE "Users" SET "PasswordHash"=$1 WHERE id=$2 RETURNING id', [hash, req.params.id]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/users/:id', auth, adminOnly, async (req, res) => {
  if (parseInt(req.params.id) === req.user.id) return res.status(400).json({ error: 'Cannot delete your own account' });
  try {
    const r = await pool.query('DELETE FROM "Users" WHERE id=$1 RETURNING id,name', [req.params.id]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, deleted: r.rows[0].name });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


// ══════════════════════════════════════════════
// DELIVERY RECEIPT CONFIRMATION (retailer)
// ══════════════════════════════════════════════
app.put('/api/deliveries/:id/confirm-receipt', auth, async (req, res) => {
  const { note } = req.body;
  try {
    // Only retailer who owns the order can confirm
    const del = await pool.query(
      `SELECT d.*, o."retailerId", o."retailerName" AS retailer, o.city, o.items
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    if (!del.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const d = del.rows[0];

    if (d.retailerId !== req.user.id) {
      return res.status(403).json({ error: 'You can only confirm your own deliveries' });
    }
    if (d.status !== 'delivered') {
      return res.status(400).json({ error: 'Can only confirm receipt of a delivered order' });
    }

    // Try with receiptConfirmed columns — add them if missing
    try {
      await pool.query(
        `UPDATE "Deliveries"
         SET "receiptConfirmed"=true, "receiptNote"=$1, "receiptAt"=NOW()
         WHERE id=$2`,
        [note||null, req.params.id]
      );
    } catch {
      // Columns don't exist yet — add them then retry
      await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "receiptConfirmed" BOOLEAN DEFAULT false`);
      await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "receiptNote" TEXT`);
      await pool.query(`ALTER TABLE "Deliveries" ADD COLUMN IF NOT EXISTS "receiptAt" TIMESTAMPTZ`);
      await pool.query(
        `UPDATE "Deliveries"
         SET "receiptConfirmed"=true, "receiptNote"=$1, "receiptAt"=NOW()
         WHERE id=$2`,
        [note||null, req.params.id]
      );
    }

    // Notify driver
    const rawDriverId = d.driverId ? Number(d.driverId) : null;
    if (rawDriverId) {
      const driverUid = await driverUserId(rawDriverId);
      await notify(
        driverUid || rawDriverId,
        '✅ Receipt Confirmed by Retailer',
        `${d.retailer} has confirmed receipt of delivery ${req.params.id} to ${d.city} (${d.items} items).${note ? ' Note: ' + note : ''}`,
        'success', req.params.id
      );
    }

    // Notify order team
    const staff = await pool.query(
      `SELECT id FROM "Users" WHERE role='order_team'`
    );
    for (const u of staff.rows) {
      await notify(
        u.id,
        '📋 Delivery Receipt Confirmed',
        `${d.retailer} confirmed receipt of delivery ${req.params.id} to ${d.city}.${note ? ' Retailer note: ' + note : ''} Delivery is fully complete.`,
        'success', req.params.id
      );
    }

    res.json({ ok: true, receiptAt: new Date().toLocaleString('en-LK', { timeZone:'Asia/Colombo' }) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/deliveries/:id/receipt', auth, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT d.id, d.status, d."receiptConfirmed", d."receiptNote", d."receiptAt",
              d."driverName", d."vehicleId", d.eta,
              o."retailerName" AS retailer, o.city, o.items, o.kg,
              TO_CHAR(d."receiptAt" AT TIME ZONE 'Asia/Colombo', 'DD Mon YYYY HH24:MI') AS "receiptAtFormatted"
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id
       WHERE d.id=$1`,
      [req.params.id]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/deliveries/:id/history', auth, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT status, note, "updatedBy",
       TO_CHAR("createdAt" AT TIME ZONE 'Asia/Colombo', 'HH24:MI DD Mon YYYY') AS time
       FROM "StatusHistory" WHERE "deliveryId"=$1 ORDER BY "createdAt" ASC`,
      [req.params.id]
    );
    res.json(r.rows);
  } catch { res.json([]); }
});

// ══════════════════════════════════════════════
// RETAILER MANAGEMENT (order_team only)
// ══════════════════════════════════════════════

function orderTeamOnly(req, res, next) {
  if (!['order_team','admin'].includes(req.user?.role)) {
    return res.status(403).json({ error: 'Order Team access required' });
  }
  next();
}

app.get('/api/retailers', auth, orderTeamOnly, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT id, name, "Email", avatar,
       TO_CHAR("createdAt" AT TIME ZONE 'Asia/Colombo', 'DD Mon YYYY') AS "createdAt"
       FROM "Users" WHERE role='retailer' ORDER BY name`
    );
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/retailers', auth, orderTeamOnly, async (req, res) => {
  const { name, email, password, avatar } = req.body;
  if (!name||!name.trim())   return res.status(400).json({ error: 'Name is required' });
  if (!email||!email.trim()) return res.status(400).json({ error: 'Email is required' });
  if (!password||password.length<4) return res.status(400).json({ error: 'Password must be at least 4 characters' });
  try {
    const exists = await pool.query('SELECT id FROM "Users" WHERE lower("Email")=lower($1)', [email]);
    if (exists.rows.length) return res.status(409).json({ error: `Email ${email} is already registered` });
    const hash = await bcrypt.hash(password, 10);
    const initials = (avatar&&avatar.trim()) ? avatar.trim().toUpperCase().slice(0,2)
      : name.trim().split(' ').map(w=>w[0]).join('').toUpperCase().slice(0,2);
    const r = await pool.query(
      `INSERT INTO "Users"(name,"Email","PasswordHash",role,avatar,"createdAt")
       VALUES($1,$2,$3,'retailer',$4,NOW()) RETURNING id,name,"Email",role,avatar`,
      [name.trim(), email.trim(), hash, initials]
    );
    res.json({ ok: true, user: r.rows[0] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/retailers/:id', auth, orderTeamOnly, async (req, res) => {
  try {
    const r = await pool.query(
      `DELETE FROM "Users" WHERE id=$1 AND role='retailer' RETURNING id,name`, [req.params.id]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Retailer not found' });
    res.json({ ok: true, deleted: r.rows[0].name });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/retailers/:id/reset-password', auth, orderTeamOnly, async (req, res) => {
  const { password } = req.body;
  if (!password||password.length<4) return res.status(400).json({ error: 'Password must be at least 4 characters' });
  try {
    const hash = await bcrypt.hash(password, 10);
    const r = await pool.query(
      `UPDATE "Users" SET "PasswordHash"=$1 WHERE id=$2 AND role='retailer' RETURNING id`,
      [hash, req.params.id]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Retailer not found' });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 3000;

// ══════════════════════════════════════════════
// SPRINT 2 — GPS TRACKING
// ══════════════════════════════════════════════

// Driver sends current GPS location (called every 10s when in-transit)
app.post('/api/tracking/update', auth, async (req, res) => {
  try {
    const { deliveryId, lat, lng } = req.body;
    if (!deliveryId || lat === undefined || lng === undefined) {
      return res.status(400).json({ error: 'deliveryId, lat and lng required' });
    }
    const latNum = parseFloat(lat);
    const lngNum = parseFloat(lng);
    if (isNaN(latNum) || isNaN(lngNum)) {
      return res.status(400).json({ error: 'lat and lng must be valid numbers' });
    }
    // Verify this delivery belongs to this driver before inserting
    const check = await pool.query(
      `SELECT d.id FROM "Deliveries" d
       JOIN "Drivers" dr ON dr.id=d."driverId"
       WHERE d.id=$1 AND (d."driverId"=$2 OR dr."userId"=$2)
       LIMIT 1`,
      [deliveryId, req.user.userId]
    );
    // Allow even if check fails (driver may use Drivers.id not Users.id)
    const accNum     = req.body.accuracy ? parseFloat(req.body.accuracy) : null;
    const speedNum   = req.body.speed    ? parseFloat(req.body.speed)    : null;
    const headingNum = req.body.heading  ? parseFloat(req.body.heading)  : null;

    // Try inserting with extra columns — fall back if columns don't exist yet
    try {
      await pool.query(
        `INSERT INTO "VehicleLocations"("deliveryId","driverId",lat,lng,accuracy,speed,heading,"recordedAt")
         VALUES($1,$2,$3,$4,$5,$6,$7,NOW())`,
        [deliveryId, req.user.userId, latNum, lngNum, accNum, speedNum, headingNum]
      );
    } catch {
      await pool.query(
        `INSERT INTO "VehicleLocations"("deliveryId","driverId",lat,lng,"recordedAt")
         VALUES($1,$2,$3,$4,NOW())`,
        [deliveryId, req.user.userId, latNum, lngNum]
      );
    }
    console.log(`[GPS] delivery=${deliveryId} driver=${req.user.userId} lat=${latNum.toFixed(5)} lng=${lngNum.toFixed(5)} acc=${accNum}m speed=${speedNum}m/s`);
    res.json({ ok: true });
  } catch(e) {
    console.error('[GPS update error]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Get latest location for a delivery
app.get('/api/tracking/:deliveryId', auth, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT vl.lat, vl.lng,
              TO_CHAR(vl."recordedAt" AT TIME ZONE 'Asia/Colombo', 'HH24:MI:SS') AS "recordedAt",
              d."driverName", d."vehicleId", d.eta, d.status,
              o.city, o."retailerName"
       FROM "VehicleLocations" vl
       JOIN "Deliveries" d ON vl."deliveryId"=d.id
       JOIN "Orders" o ON d."orderId"=o.id
       WHERE vl."deliveryId"=$1
       ORDER BY vl."recordedAt" DESC LIMIT 1`,
      [req.params.deliveryId]
    );
    if (!r.rows.length) return res.json({ found: false });
    res.json({ found: true, ...r.rows[0] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get all active tracked deliveries (for map overview)
app.get('/api/tracking', auth, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT DISTINCT ON (vl."deliveryId")
              vl."deliveryId", vl.lat, vl.lng,
              vl.accuracy, vl.speed, vl.heading,
              TO_CHAR(vl."recordedAt" AT TIME ZONE 'Asia/Colombo', 'HH24:MI:SS') AS "recordedAt",
              EXTRACT(EPOCH FROM (NOW() - vl."recordedAt")) AS "secondsAgo",
              d."driverName", d."vehicleId", d.status, d.eta,
              o.city, o."retailerName",
              (SELECT COUNT(*) FROM "Deliveries" d2
               WHERE d2."driverId"=d."driverId"
                 AND d2.status NOT IN ('delivered','failed')) AS "stopsRemaining"
       FROM "VehicleLocations" vl
       JOIN "Deliveries" d ON vl."deliveryId"=d.id
       JOIN "Orders" o ON d."orderId"=o.id
       WHERE d.status IN ('in-transit','loaded')
         AND vl."recordedAt" > NOW() - INTERVAL '2 hours'
       ORDER BY vl."deliveryId", vl."recordedAt" DESC`
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Stock Restocked Notification (call when warehouse updates stock)
app.put('/api/stock/:id/update', auth, async (req, res) => {
  const { availableUnits, availableKg } = req.body;
  const productId = req.params.id;
  try {
    // Update stock levels
    await pool.query(
      `UPDATE "Stock" SET "availableUnits"=$1, "availableKg"=$2 WHERE id=$3`,
      [availableUnits, availableKg, productId]
    );

    // Check for orders that were rejected for out_of_stock and are watching this product
    let watchedOrders = [];
    try {
      watchedOrders = (await pool.query(
        `SELECT o.id, o."retailerId", o."retailerName", o.city, o.items, o.kg,
                o.priority, o.product, s."productName"
         FROM "Orders" o
         JOIN "Stock" s ON s.id=$1
         WHERE o."productId"=$1
           AND o.status='rejected'
           AND o."rejectCategory"='out_of_stock'
           AND o."stockWatchActive"=true
           AND o.items <= $2`,
        [productId, availableUnits]
      )).rows;
    } catch(e) { console.warn('[stock-watch query]', e.message); }

    // Notify each watching retailer
    for (const o of watchedOrders) {
      await notify(
        o.retailerId,
        '🟢 Stock Restocked — Resubmit Your Order',
        `Good news! ${o.productName||'The product'} you ordered is back in stock.

Your previous order ${o.id} to ${o.city} (${o.items} items) was rejected due to stock shortage — you can now resubmit it.

Tap "Resubmit" on your rejected order to pre-fill the form automatically.`,
        'success',
        o.id
      );
      // Clear the watch flag
      try {
        await pool.query(`UPDATE "Orders" SET "stockWatchActive"=false WHERE id=$1`, [o.id]);
      } catch {}
      console.log(`[stock-watch] Notified retailer ${o.retailerId} for order ${o.id} — stock restored`);
    }

    res.json({ ok: true, restockedCount: watchedOrders.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Get all stock levels (for warehouse stock management page)
app.get('/api/stock', auth, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT s.id, s."productName", s."availableUnits", s."availableKg", s."weightPerUnit",
       (SELECT COUNT(*) FROM "Orders" o WHERE o."productId"=s.id AND o.status='rejected'
        AND o."rejectCategory"='out_of_stock' AND o."stockWatchActive"=true) AS "watchCount"
       FROM "Stock" s ORDER BY s."productName"`
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════
// AI ASSISTANT PROXY — keeps Anthropic API key safe on server
// Set ANTHROPIC_API_KEY in Render environment variables
// ══════════════════════════════════════════════
app.post('/api/ai/chat', auth, async (req, res) => {
  const { messages, systemPrompt } = req.body;
  if (!messages || !Array.isArray(messages)) {
    return res.status(400).json({ error: 'messages array required' });
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'AI service not configured. Please set ANTHROPIC_API_KEY in Render environment variables.' });
  }

  try {
    const https = require('https');
    const body = JSON.stringify({
      model: 'claude-sonnet-4-5',
      max_tokens: 1000,
      system: systemPrompt || 'You are a helpful delivery management assistant.',
      messages
    });

    const response = await new Promise((resolve, reject) => {
      const options = {
        hostname: 'api.anthropic.com',
        path: '/v1/messages',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01',
          'Content-Length': Buffer.byteLength(body)
        }
      };
      const req2 = https.request(options, (resp) => {
        let data = '';
        resp.on('data', chunk => data += chunk);
        resp.on('end', () => {
          try { resolve({ status: resp.statusCode, body: JSON.parse(data) }); }
          catch(e) { reject(new Error('Invalid JSON from Anthropic: ' + data.slice(0,200))); }
        });
      });
      req2.on('error', reject);
      req2.write(body);
      req2.end();
    });

    if (response.status !== 200) {
      console.error('[AI proxy] Anthropic error:', response.status, JSON.stringify(response.body));
      return res.status(response.status).json({ error: response.body?.error?.message || 'AI service error' });
    }

    const text = (response.body.content || []).map(c => c.text || '').join('');
    res.json({ text, usage: response.body.usage });
  } catch(e) {
    console.error('[AI proxy error]', e.message);
    res.status(500).json({ error: 'AI service temporarily unavailable: ' + e.message });
  }
});

// ══════════════════════════════════════════════
// PIN PROOF OF DELIVERY
// ══════════════════════════════════════════════

// Driver verifies retailer PIN → auto-marks delivered
app.post('/api/deliveries/:id/verify-pin', auth, async (req, res) => {
  const { pin } = req.body;
  if (!pin || String(pin).trim().length !== 4) {
    return res.status(400).json({ error: 'PIN must be exactly 4 digits' });
  }
  try {
    const r = await pool.query(
      `SELECT d.*, o."retailerId", o."retailerName" AS retailer, o.city, o.items, o.kg
       FROM "Deliveries" d JOIN "Orders" o ON d."orderId"=o.id WHERE d.id=$1`,
      [req.params.id]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Delivery not found' });
    const d = r.rows[0];

    if (d.status === 'delivered') {
      return res.status(409).json({ error: 'This delivery is already marked as delivered.' });
    }
    if (d.status !== 'in-transit') {
      return res.status(400).json({ error: 'Delivery must be In Transit before PIN verification.' });
    }
    if (!d.deliveryPin) {
      return res.status(400).json({ error: 'No PIN assigned to this delivery.' });
    }

    // Track wrong attempts
    const attempts = (parseInt(d.pinAttempts) || 0) + 1;
    if (String(pin).trim() !== String(d.deliveryPin)) {
      try { await pool.query(`UPDATE "Deliveries" SET "pinAttempts"=$1 WHERE id=$2`, [attempts, req.params.id]); } catch {}
      const remaining = 3 - attempts;
      if (remaining <= 0) {
        const optUsers = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
        for (const u of optUsers.rows) {
          await notify(u.id, '🚨 PIN Verification Failed — Delivery ' + req.params.id,
            `Driver ${d.driverName} failed PIN verification 3 times for delivery ${req.params.id} to ${d.retailer} (${d.city}). Please investigate.`,
            'alert', req.params.id);
        }
        return res.status(403).json({ error: 'Too many incorrect attempts. OPT has been notified.', locked: true });
      }
      return res.status(401).json({ error: `Incorrect PIN. ${remaining} attempt${remaining !== 1 ? 's' : ''} remaining.`, remaining });
    }

    // ✅ PIN CORRECT — mark delivered
    await pool.query(
      `UPDATE "Deliveries" SET status='delivered', "pinVerified"=true, "pinVerifiedAt"=NOW(), "pinAttempts"=0, "updatedAt"=NOW() WHERE id=$1`,
      [req.params.id]
    );
    console.log(`[PIN] ✅ Verified delivery ${req.params.id}`);

    const driverId = d.driverId ? Number(d.driverId) : null;
    const optUsers = await pool.query(`SELECT id FROM "Users" WHERE role='order_team'`);
    const whUsers  = await pool.query(`SELECT id FROM "Users" WHERE role='warehouse'`);

    await notify(d.retailerId, '✅ Delivery Verified — PIN Confirmed',
      `Your delivery ${req.params.id} to ${d.city} was verified with your PIN and successfully completed 🔐✅.

Items: ${d.items} · Driver: ${d.driverName || '—'}

Please confirm receipt in the app to finalise.`,
      'success', req.params.id);

    if (driverId) {
      await notify(driverId, '✅ PIN Verified — Move to Next Stop',
        `PIN verified for delivery ${req.params.id} to ${d.retailer} (${d.city}). Delivery complete ✅. Check your route for remaining stops.`,
        'success', req.params.id);
    }
    for (const u of optUsers.rows) {
      await notify(u.id, `✅ PIN Verified — ${d.city}`,
        `Delivery ${req.params.id} to ${d.retailer} (${d.city}) PIN-verified and marked delivered.`,
        'success', req.params.id);
    }
    for (const u of whUsers.rows) {
      await notify(u.id, `✅ Delivered & Verified — ${d.city}`,
        `Delivery ${req.params.id} PIN-verified. Vehicle ${d.vehicleId} can proceed.`,
        'success', req.params.id);
    }

    res.json({ ok: true, message: 'PIN verified — delivery complete' });
  } catch(e) {
    console.error('[PIN]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`Nestlé DMS API running on port ${PORT}`));