// ══════════════════════════════════════════════════════════
// NESTLÉ DMS — Sprint 1 v3  (Render + Neon PostgreSQL)
// ══════════════════════════════════════════════════════════
const express = require('express');
const { Pool } = require('pg');
const cors    = require('cors');

const app = express();

app.use(cors({
  origin: [
    'https://smart-distribution-and-delivery-man.vercel.app',
    'http://localhost:3000',
    'http://127.0.0.1:5500',
    // add your Vercel URL here once deployed
  ]
}));
app.use(express.json());

// ── DB config (Neon PostgreSQL) ───────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function q(qry, params = []) {
  const res = await pool.query(qry, params);
  return res.rows;
}

async function ex(qry, params = []) {
  return pool.query(qry, params);
}

// ── Helpers ───────────────────────────────────────────────
const REGIONS = {
  Colombo:'Western', Dehiwala:'Western', Nugegoda:'Western', Negombo:'Western',
  Kiribathgoda:'Western', Kandy:'Central', Peradeniya:'Central',
  Kurunegala:'North Western', Galle:'Southern', Matara:'Southern',
  Jaffna:'Northern', Ratnapura:'Sabaragamuwa'
};
function region(city) { return REGIONS[city] || 'Western'; }

function calcEta(city) {
  const mins = {
    Colombo:30, Dehiwala:45, Nugegoda:50, Negombo:75, Kiribathgoda:60,
    Kandy:180, Peradeniya:195, Galle:240, Matara:270, Jaffna:390,
    Ratnapura:150, Kurunegala:180
  };
  const t = new Date(Date.now() + (mins[city]||60)*60000);
  return t.toLocaleTimeString('en-LK',{hour:'2-digit',minute:'2-digit'});
}

async function nextId(table, col, prefix) {
  // Extract numeric part from IDs like ORD001, D001, R001
  const rows = await q(
    `SELECT MAX(CAST(SUBSTRING(${col}, ${prefix.length + 1}, 10) AS INTEGER)) AS n FROM "${table}"`
  );
  return prefix + String((rows[0].n || 0) + 1).padStart(3, '0');
}

// ══════════════════════════════════════════════════════════
// NOTIFICATION ENGINE
// ══════════════════════════════════════════════════════════
async function notifyUser(userId, type, title, message, refId) {
  await ex(
    `INSERT INTO "Notifications"("UserID","Type","Title","Message","RefID") VALUES($1,$2,$3,$4,$5)`,
    [+userId, type, title, message, refId || null]
  );
}

async function notifyRole(role, type, title, message, refId) {
  const users = await q(`SELECT "UserID" FROM "Users" WHERE "Role"=$1 AND "IsActive"=true`, [role]);
  for (const u of users) await notifyUser(u.UserID, type, title, message, refId);
}

async function notifyRoles(roles, type, title, message, refId) {
  for (const role of roles) await notifyRole(role, type, title, message, refId);
}

// ══════════════════════════════════════════════════════════
// HEALTH CHECK
// ══════════════════════════════════════════════════════════
app.get('/', (req, res) => res.json({ status: 'Nestlé DMS API running ✓' }));

// ══════════════════════════════════════════════════════════
// AUTH
// ══════════════════════════════════════════════════════════
app.post('/api/login', async (req, res) => {
  try {
    const rows = await q(
      `SELECT "UserID" AS id, "FullName" AS name, "Email" AS email, "Role" AS role, "Avatar" AS avatar
       FROM "Users" WHERE "Email"=$1 AND "PasswordHash"=$2 AND "IsActive"=true`,
      [req.body.email, req.body.password]
    );
    if (!rows.length) return res.status(401).json({ error: 'Invalid credentials' });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
// NOTIFICATIONS API
// ══════════════════════════════════════════════════════════
app.get('/api/notifications', async (req, res) => {
  try {
    const rows = await q(
      `SELECT "NotifID" AS id, "Type" AS type, "Title" AS title, "Message" AS message,
              "RefID" AS "refId", "IsRead" AS "isRead",
              TO_CHAR("CreatedAt", 'HH24:MI DD-Mon') AS time
       FROM "Notifications" WHERE "UserID"=$1
       ORDER BY "CreatedAt" DESC`,
      [+req.query.userId]
    );
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/notifications/unread', async (req, res) => {
  try {
    const rows = await q(
      `SELECT COUNT(*) AS cnt FROM "Notifications" WHERE "UserID"=$1 AND "IsRead"=false`,
      [+req.query.userId]
    );
    res.json({ count: parseInt(rows[0].cnt) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/:id/read', async (req, res) => {
  try {
    await ex(`UPDATE "Notifications" SET "IsRead"=true WHERE "NotifID"=$1`, [+req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/read-all', async (req, res) => {
  try {
    await ex(`UPDATE "Notifications" SET "IsRead"=true WHERE "UserID"=$1`, [+req.body.userId]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
// ORDERS
// ══════════════════════════════════════════════════════════
app.get('/api/orders', async (req, res) => {
  try {
    const { role, userId } = req.query;
    const where = role === 'retailer' ? `WHERE o."RetailerID"=${+userId}` : '';
    const rows = await q(`
      SELECT o."OrderID" AS id, o."RetailerName" AS retailer, o."City" AS city,
             o."Region" AS region, o."ItemCount" AS items, o."WeightKG" AS kg,
             o."Priority" AS prio, o."Status" AS status, o."Notes" AS notes,
             o."RejectionReason" AS "rejectReason", o."RetailerID" AS "retailerId",
             TO_CHAR(o."CreatedAt", 'HH24:MI DD-Mon') AS created,
             u."FullName" AS "confirmedBy"
      FROM "Orders" o LEFT JOIN "Users" u ON o."ConfirmedBy"=u."UserID"
      ${where} ORDER BY o."CreatedAt" DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/orders', async (req, res) => {
  try {
    const { retailerId, retailerName, city, items, kg, priority, notes } = req.body;
    const id = await nextId('Orders', '"OrderID"', 'ORD');
    await ex(
      `INSERT INTO "Orders"("OrderID","RetailerID","RetailerName","City","Region","ItemCount","WeightKG","Priority","Notes","Status")
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')`,
      [id, +retailerId, retailerName, city, region(city), +items, +kg, priority||'normal', notes||'']
    );

    await notifyRole('order_team', 'info',
      `New order request — ${id}`,
      `${retailerName} has requested ${items} items (${kg} kg) to ${city}. Priority: ${priority||'normal'}. Please review and confirm or reject.`,
      id
    );

    res.json({ id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/orders/:id/confirm', async (req, res) => {
  try {
    const { action, confirmedBy, rejectReason } = req.body;
    const status = action === 'confirm' ? 'confirmed' : 'rejected';
    const order = (await q(`SELECT "RetailerID","RetailerName","City" FROM "Orders" WHERE "OrderID"=$1`, [req.params.id]))[0];
    await ex(
      `UPDATE "Orders" SET "Status"=$1,"ConfirmedBy"=$2,"RejectionReason"=$3,"UpdatedAt"=NOW() WHERE "OrderID"=$4`,
      [status, +confirmedBy, rejectReason||null, req.params.id]
    );

    if (action === 'confirm') {
      await notifyUser(order.RetailerID, 'success',
        `✅ Order ${req.params.id} confirmed`,
        `Your order for ${order.City} has been confirmed by the Order Processing Team. It will be prepared and dispatched to you shortly.`,
        req.params.id
      );
    } else {
      await notifyUser(order.RetailerID, 'alert',
        `❌ Order ${req.params.id} rejected`,
        `Your order for ${order.City} was rejected. Reason: ${rejectReason||'Not specified'}. Please submit a new order request.`,
        req.params.id
      );
    }

    res.json({ ok: true, status });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
// DELIVERIES
// ══════════════════════════════════════════════════════════
app.get('/api/deliveries', async (req, res) => {
  try {
    const { role, userId } = req.query;
    const where = role === 'distributor' ? `WHERE d."DriverID"=${+userId}` : '';
    const rows = await q(`
      SELECT d."DeliveryID" AS id, d."OrderID" AS "orderId",
             d."RetailerName" AS retailer, d."City" AS city, d."Region" AS region,
             d."ItemCount" AS items, d."WeightKG" AS kg, d."Priority" AS prio,
             d."Status" AS status, d."ETA" AS eta, d."Notes" AS notes,
             d."DriverID" AS "driverId", d."VehicleID" AS "vehicleId",
             u."FullName" AS "driverName", u."Avatar" AS "driverAvatar"
      FROM "Deliveries" d LEFT JOIN "Users" u ON d."DriverID"=u."UserID"
      ${where} ORDER BY d."CreatedAt" DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/deliveries/consolidate', async (req, res) => {
  try {
    const confirmed = await q(`SELECT * FROM "Orders" WHERE "Status"='confirmed'`);
    if (!confirmed.length) return res.json({ created: 0, held: 0 });

    if (confirmed.length === 1) {
      const solo = confirmed[0];
      const isHighPriority = ['urgent','high'].includes((solo.Priority||'').toLowerCase());

      if (!isHighPriority) {
        await notifyRole('order_team', 'warning',
          `⏳ Order ${solo.OrderID} held — waiting for batch`,
          `Only 1 confirmed order exists (${solo.RetailerName}, ${solo.City} — priority: ${solo.Priority}). Low/normal priority single orders are held until more orders are confirmed.`,
          solo.OrderID
        );
        return res.json({ created: 0, held: 1, reason: 'single_low_priority', orderId: solo.OrderID });
      }
    }

    let created = 0;
    const cities = [];
    for (const o of confirmed) {
      const id = await nextId('Deliveries', '"DeliveryID"', 'D');
      await ex(
        `INSERT INTO "Deliveries"("DeliveryID","OrderID","RetailerName","City","Region","ItemCount","WeightKG","Priority","Status")
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending')`,
        [id, o.OrderID, o.RetailerName, o.City, o.Region, o.ItemCount, o.WeightKG, o.Priority]
      );
      await ex(`UPDATE "Orders" SET "Status"='consolidated' WHERE "OrderID"=$1`, [o.OrderID]);
      cities.push(o.City);
      created++;
    }

    const isSingleUrgent = confirmed.length === 1;
    const batchNote = isSingleUrgent
      ? `Single high-priority order fast-tracked.`
      : `Batch of ${created} orders consolidated.`;

    await notifyRole('route_planner', 'info',
      `📋 ${created} deliver${created===1?'y':'ies'} ready to plan`,
      `${batchNote} Cities: ${cities.join(', ')}. Please assign drivers and vehicles now.`,
      null
    );

    await notifyRole('warehouse', 'info',
      `📦 ${created} new deliver${created===1?'y':'ies'} incoming`,
      `${batchNote} Drivers and vehicles will be assigned shortly. Please prepare warehouse for cargo packing.`,
      null
    );

    res.json({ created, held: 0 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/assign', async (req, res) => {
  try {
    const { driverId, vehicleId, driverName } = req.body;
    const del = (await q(
      `SELECT "City","RetailerName","WeightKG","ItemCount" FROM "Deliveries" WHERE "DeliveryID"=$1`,
      [req.params.id]
    ))[0];
    const etaVal = del ? calcEta(del.City) : '—';

    await ex(
      `UPDATE "Deliveries" SET "DriverID"=$1,"VehicleID"=$2,"Status"='assigned',"ETA"=$3,"UpdatedAt"=NOW() WHERE "DeliveryID"=$4`,
      [+driverId, vehicleId, etaVal, req.params.id]
    );
    await ex(`UPDATE "Vehicles" SET "Status"='assigned' WHERE "VehicleID"=$1`, [vehicleId]);

    await notifyUser(+driverId, 'info',
      `🚚 New delivery assigned — ${req.params.id}`,
      `You have been assigned delivery ${req.params.id}. Destination: ${del.RetailerName}, ${del.City}. ${del.ItemCount} items · ${del.WeightKG} kg. Vehicle: ${vehicleId}. ETA: ${etaVal}. Wait for warehouse to prepare and load the cargo.`,
      req.params.id
    );

    await notifyRole('warehouse', 'info',
      `📦 Prepare cargo — Delivery ${req.params.id}`,
      `Delivery ${req.params.id} assigned to ${driverName} (${vehicleId}). Destination: ${del.RetailerName}, ${del.City}. ${del.ItemCount} items · ${del.WeightKG} kg. Please pick, pack and load vehicle now.`,
      req.params.id
    );

    res.json({ ok: true, eta: etaVal });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/warehouse-ready', async (req, res) => {
  try {
    const del = (await q(
      `SELECT "RetailerName","City","DriverID","VehicleID","ItemCount","WeightKG" FROM "Deliveries" WHERE "DeliveryID"=$1`,
      [req.params.id]
    ))[0];
    await ex(`UPDATE "Deliveries" SET "Status"='warehouse_ready',"UpdatedAt"=NOW() WHERE "DeliveryID"=$1`, [req.params.id]);

    if (del?.DriverID) {
      await notifyUser(del.DriverID, 'success',
        `✅ Cargo ready — come pick up ${req.params.id}`,
        `Your cargo for ${del.RetailerName}, ${del.City} (${del.ItemCount} items · ${del.WeightKG} kg) has been packed and is ready. Please come to the warehouse to collect vehicle ${del.VehicleID}.`,
        req.params.id
      );
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/loaded', async (req, res) => {
  try {
    const del = (await q(
      `SELECT "RetailerName","City","DriverID","VehicleID" FROM "Deliveries" WHERE "DeliveryID"=$1`,
      [req.params.id]
    ))[0];
    await ex(`UPDATE "Deliveries" SET "Status"='loaded',"UpdatedAt"=NOW() WHERE "DeliveryID"=$1`, [req.params.id]);

    if (del?.DriverID) {
      await notifyUser(del.DriverID, 'alert',
        `🚛 Vehicle loaded — confirm pickup to begin route`,
        `Vehicle ${del.VehicleID} has been fully loaded with cargo for ${del.RetailerName}, ${del.City}. Please open your app, confirm the pickup and begin your delivery route.`,
        req.params.id
      );
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/deliveries/:id/status', async (req, res) => {
  try {
    const { newStatus, note, updatedBy } = req.body;
    const delRows = await q(
      `SELECT d."Status", d."RetailerName", d."City", d."ETA", d."ItemCount", d."WeightKG", d."VehicleID",
              o."RetailerID"
       FROM "Deliveries" d LEFT JOIN "Orders" o ON d."OrderID"=o."OrderID"
       WHERE d."DeliveryID"=$1`,
      [req.params.id]
    );
    const del = delRows[0];
    const oldStatus = del?.Status || 'unknown';

    await ex(`UPDATE "Deliveries" SET "Status"=$1,"UpdatedAt"=NOW() WHERE "DeliveryID"=$2`, [newStatus, req.params.id]);
    await ex(
      `INSERT INTO "DeliveryStatus"("DeliveryID","UpdatedByID","OldStatus","NewStatus","Note") VALUES($1,$2,$3,$4,$5)`,
      [req.params.id, +updatedBy, oldStatus, newStatus, note||null]
    );

    if (newStatus === 'in-transit') {
      if (del?.RetailerID) {
        await notifyUser(del.RetailerID, 'info',
          `🚚 Your delivery is on the way!`,
          `Delivery ${req.params.id} has been dispatched and is heading to ${del.City}. Your ${del.ItemCount} items should arrive by ${del.ETA}.`,
          req.params.id
        );
      }
      await notifyRoles(['order_team','warehouse'], 'info',
        `🚚 Delivery ${req.params.id} dispatched`,
        `${del.RetailerName}, ${del.City} — now in transit. ETA: ${del.ETA}. Driver note: ${note||'—'}.`,
        req.params.id
      );
    }

    if (newStatus === 'delivered') {
      await ex(`UPDATE "Vehicles" SET "Status"='idle' WHERE "VehicleID"=(SELECT "VehicleID" FROM "Deliveries" WHERE "DeliveryID"=$1)`, [req.params.id]);
      if (del?.RetailerID) {
        await notifyUser(del.RetailerID, 'success',
          `✅ Your order has been delivered!`,
          `Delivery ${req.params.id} — your ${del.ItemCount} items have been successfully delivered to ${del.City}. Thank you for choosing Nestlé!`,
          req.params.id
        );
      }
      await notifyRoles(['order_team','warehouse'], 'success',
        `✅ Delivery ${req.params.id} completed`,
        `${del.RetailerName}, ${del.City} — delivered successfully. Note: ${note||'—'}.`,
        req.params.id
      );
    }

    if (newStatus === 'failed') {
      await ex(`UPDATE "Vehicles" SET "Status"='idle' WHERE "VehicleID"=(SELECT "VehicleID" FROM "Deliveries" WHERE "DeliveryID"=$1)`, [req.params.id]);
      if (del?.RetailerID) {
        await notifyUser(del.RetailerID, 'alert',
          `❌ Delivery ${req.params.id} failed`,
          `Your delivery to ${del.City} could not be completed. Reason: ${note||'Not specified'}. Please contact the Order Processing Team to reschedule.`,
          req.params.id
        );
      }
      await notifyRoles(['order_team','warehouse'], 'alert',
        `❌ Delivery ${req.params.id} failed`,
        `${del.RetailerName}, ${del.City} — delivery failed. Reason: ${note||'Not specified'}. Immediate action required.`,
        req.params.id
      );
    }

    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════════════════
app.get('/api/routes', async (req, res) => {
  try {
    const rows = await q(`
      SELECT r."RouteID" AS id, r."RouteName" AS name, r."StopCount" AS stops,
             r."DistanceKM" AS "distKm", r."DurationMins" AS "durMins",
             r."CitySequence" AS cities, r."Status" AS status,
             u."FullName" AS "driverName", r."VehicleID" AS "vehicleId"
      FROM "Routes" r LEFT JOIN "Users" u ON r."DriverID"=u."UserID"
      ORDER BY r."CreatedAt" DESC`);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/routes', async (req, res) => {
  try {
    const { driverId, driverName, vehicleId, stops, distKm, durMins, cities } = req.body;
    const id = await nextId('Routes', '"RouteID"', 'R');
    await ex(
      `INSERT INTO "Routes"("RouteID","RouteName","DriverID","VehicleID","StopCount","DistanceKM","DurationMins","CitySequence","Status")
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,'planned')`,
      [id, `${driverName} Route`, +driverId, vehicleId, +stops, +distKm, +durMins, JSON.stringify(cities)]
    );
    res.json({ id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════
// REFERENCE DATA
// ══════════════════════════════════════════════════════════
app.get('/api/drivers', async (req, res) => {
  try {
    res.json(await q(`SELECT "UserID" AS id, "FullName" AS name, "Avatar" AS avatar FROM "Users" WHERE "Role"='distributor' AND "IsActive"=true`));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vehicles', async (req, res) => {
  try {
    res.json(await q(`SELECT "VehicleID" AS id, "Plate" AS plate, "VehicleType" AS type, "Capacity" AS cap, "FuelPercent" AS fuel, "Status" AS status FROM "Vehicles"`));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/status-log/:deliveryId', async (req, res) => {
  try {
    const rows = await q(`
      SELECT ds."OldStatus" AS "oldStatus", ds."NewStatus" AS "newStatus", ds."Note" AS note,
             TO_CHAR(ds."UpdatedAt", 'HH24:MI DD-Mon') AS time, u."FullName" AS "updatedBy"
      FROM "DeliveryStatus" ds JOIN "Users" u ON ds."UpdatedByID"=u."UserID"
      WHERE ds."DeliveryID"=$1 ORDER BY ds."UpdatedAt" DESC`,
      [req.params.deliveryId]
    );
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 5001;
app.listen(PORT, () => {
  console.log(`✓ Nestlé DMS API running on port ${PORT}`);
});