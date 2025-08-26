/* NC Sigma Portal - consolidated server with:
   - Chapters, Yearly stats, Profiles, Documents, Media
   - Events + ICS calendar
   - Image uploads + CSV exports
   - Import endpoints (Chapters+EOY, Yearly History) with dryRun
   - META table for "as of" date on yearly uploads
*/

const path = require("path");
const fs = require("fs");
const express = require("express");
const multer = require("multer");
const xlsx = require("xlsx");
const Database = require("better-sqlite3");
const { v4: uuidv4 } = require("uuid");
const cors = require("cors");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 5000;
const ADMIN_KEY = process.env.ADMIN_KEY || "changeme123";

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

/* -------------------------- File uploads -------------------------- */
const uploadMem = multer({ storage: multer.memoryStorage() }); // for spreadsheets

// Disk storage for images (chapters/events)
const uploadDisk = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const kind = (req.params.kind || "chapters").toLowerCase();
      const safe = kind === "events" ? "events" : "chapters";
      const dir = path.join(__dirname, "public", "uploads", safe);
      fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename: (req, file, cb) => {
      const base = path.basename(file.originalname).replace(/\s+/g, "_");
      cb(null, `${Date.now()}_${base}`);
    }
  })
});

/* ------------------------------ DB ------------------------------- */
const db = new Database(path.join(__dirname, "app.db"));
db.pragma("foreign_keys = ON");

db.exec(`
CREATE TABLE IF NOT EXISTS chapters (
  id TEXT PRIMARY KEY,
  code TEXT,
  name TEXT UNIQUE,
  type TEXT,            -- 'Collegiate' | 'Alumni'
  city TEXT,
  university TEXT,
  charter_date TEXT,
  status TEXT,          -- 'Active' | 'Inactive'
  latitude REAL,
  longitude REAL
);

CREATE TABLE IF NOT EXISTS quarterly_stats (
  id TEXT PRIMARY KEY,
  chapter_id TEXT,
  year INTEGER,
  quarter INTEGER,
  quarter_start_date TEXT,
  active_members INTEGER,
  new_initiates INTEGER,
  transfers_in INTEGER,
  transfers_out INTEGER,
  reactivated INTEGER,
  suspended INTEGER,
  notes TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS yearly_stats (
  id TEXT PRIMARY KEY,
  chapter_id TEXT NOT NULL,
  year INTEGER NOT NULL,
  active_members INTEGER NOT NULL,
  notes TEXT,
  UNIQUE(chapter_id, year),
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS documents (
  id TEXT PRIMARY KEY,
  title TEXT,
  doc_type TEXT,
  "group" TEXT,
  publish_date TEXT,
  file_url TEXT,
  visibility TEXT,
  tags TEXT
);

CREATE TABLE IF NOT EXISTS media (
  id TEXT PRIMARY KEY,
  title TEXT,
  category TEXT,
  event_date TEXT,
  album_url TEXT,
  thumbnail_url TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS chapter_profiles (
  chapter_id TEXT PRIMARY KEY,
  crest_url TEXT,
  president_name TEXT,
  president_email TEXT,
  president_photo_url TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  chapter_id TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  location TEXT,
  start_utc TEXT NOT NULL,
  end_utc TEXT,
  visibility TEXT DEFAULT 'Public',   -- Public | Brothers | Officers
  status TEXT DEFAULT 'Approved',     -- Pending | Approved | Rejected
  created_by TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

/* NEW: key-value meta store (for "as-of" last yearly upload date) */
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
`);

/* ---------------------------- Helpers ---------------------------- */
function norm(s){ return (s || "").toString().trim(); }
function isBlankRow(arr){ return arr.every(v => !norm(v)); }
function parseDate(s, fallback="1900-01-01"){
  const v = norm(s); if (!v) return fallback;
  const d = new Date(v); return isNaN(d) ? fallback : d.toISOString().slice(0,10);
}
function getMeta(key){
  const r = db.prepare(`SELECT value FROM meta WHERE key=?`).get(key);
  return r ? r.value : null;
}
function setMeta(key, value){
  db.prepare(`
    INSERT INTO meta (key, value) VALUES (?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
  `).run(key, value);
}

/* ---------------------------- Public API ---------------------------- */
// Chapters & profiles
app.get("/api/chapters", (req, res) => {
  res.json(db.prepare(`SELECT * FROM chapters ORDER BY name`).all());
});
app.get("/api/chapters/:id", (req, res) => {
  const row = db.prepare(`SELECT * FROM chapters WHERE id = ?`).get(req.params.id);
  if (!row) return res.status(404).json({ error: "Not found" });
  res.json(row);
});
app.get("/api/chapters/:id/profile", (req, res) => {
  const row = db.prepare(`SELECT * FROM chapter_profiles WHERE chapter_id = ?`).get(req.params.id);
  res.json(row || { crest_url:null, president_name:null, president_email:null, president_photo_url:null });
});
app.get("/api/chapters/:id/history-yearly", (req, res) => {
  const rows = db.prepare(`
    SELECT year, active_members
    FROM yearly_stats
    WHERE chapter_id = ?
    ORDER BY year
  `).all(req.params.id);
  res.json(rows);
});

// Statewide totals (latest YEAR only)
app.get("/api/stats/active-brothers/by-type-latest", (req, res) => {
  const latest = db.prepare(`SELECT MAX(year) AS y FROM yearly_stats`).get();
  if (!latest || !latest.y) return res.json({ year: null, total: 0, alumni: 0, collegiate: 0 });

  const rows = db.prepare(`
    SELECT c.type AS type, SUM(y.active_members) AS total
    FROM yearly_stats y
    JOIN chapters c ON c.id = y.chapter_id
    WHERE y.year = ?
    GROUP BY c.type
  `).all(latest.y);

  const out = { year: latest.y, total: 0, alumni: 0, collegiate: 0 };
  rows.forEach(r => {
    out.total += r.total || 0;
    const t = (r.type || '').toLowerCase();
    if (t === 'alumni') out.alumni = r.total || 0;
    if (t === 'collegiate') out.collegiate = r.total || 0;
  });
  res.json(out);
});

// Top 5 by membership (latest year) per type
app.get("/api/stats/top-membership", (req, res) => {
  const type = (req.query.type || "").toLowerCase() === "alumni" ? "Alumni" : "Collegiate";
  const latest = db.prepare(`SELECT MAX(year) AS y FROM yearly_stats`).get();
  if (!latest || !latest.y) return res.json({ year:null, type, rows: [] });

  const rows = db.prepare(`
    SELECT c.id, c.name, y.active_members AS members
    FROM yearly_stats y
    JOIN chapters c ON c.id = y.chapter_id
    WHERE c.type = ? AND y.year = ?
    ORDER BY y.active_members DESC, c.name ASC
    LIMIT 5
  `).all(type, latest.y);

  res.json({ year: latest.y, type, rows });
});

// NEW: expose last yearly upload time for "as of"
app.get('/api/stats/yearly-last-upload', (req, res) => {
  const v = getMeta('yearly_last_upload_at');
  res.json({ last_upload_at: v || null });
});

// Documents & Media
app.get("/api/documents", (req, res) => {
  res.json(db.prepare(`SELECT * FROM documents ORDER BY publish_date DESC`).all());
});
app.get("/api/media", (req, res) => {
  res.json(db.prepare(`SELECT * FROM media ORDER BY event_date DESC`).all());
});

// Events list (public)
app.get("/api/events", (req, res) => {
  const from = req.query.from ? new Date(req.query.from).toISOString() : null;
  const to   = req.query.to   ? new Date(req.query.to).toISOString()   : null;
  const visibility = (req.query.visibility || "Public");
  const status = (req.query.status || "Approved");

  let sql = `
    SELECT e.*, c.name AS chapter_name
    FROM events e
    JOIN chapters c ON c.id = e.chapter_id
    WHERE e.status = ? AND e.visibility = ?
  `;
  const args = [status, visibility];

  if (from) { sql += " AND e.start_utc >= ?"; args.push(from); }
  if (to)   { sql += " AND e.start_utc <= ?"; args.push(to);   }

  sql += " ORDER BY e.start_utc ASC";
  const rows = db.prepare(sql).all(...args);
  res.json(rows);
});

// ICS calendar (approved/public)
app.get('/calendar.ics', (req,res)=>{
  const rows = db.prepare(`
    SELECT e.*, c.name as chapter_name
    FROM events e JOIN chapters c ON c.id = e.chapter_id
    WHERE e.status='Approved' AND e.visibility='Public'
    ORDER BY e.start_utc DESC
    LIMIT 1000
  `).all();

  const lines = [
    'BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//NC Sigma//Calendar//EN'
  ];
  for (const e of rows){
    const uid = e.id + '@ncsigma';
    const dtStart = new Date(e.start_utc).toISOString().replace(/[-:]/g,'').split('.')[0]+'Z';
    const dtEnd   = e.end_utc ? new Date(e.end_utc).toISOString().replace(/[-:]/g,'').split('.')[0]+'Z' : dtStart;
    lines.push('BEGIN:VEVENT');
    lines.push(`UID:${uid}`);
    lines.push(`DTSTAMP:${dtStart}`);
    lines.push(`DTSTART:${dtStart}`);
    lines.push(`DTEND:${dtEnd}`);
    lines.push(`SUMMARY:${(e.title||'').replace(/\n/g,' ')}`);
    if (e.location) lines.push(`LOCATION:${e.location.replace(/\n/g,' ')}`);
    const desc = `${e.chapter_name||''}${e.description? '\\n\\n'+e.description.replace(/\n/g,'\\n') : ''}`;
    if (desc) lines.push(`DESCRIPTION:${desc}`);
    lines.push('END:VEVENT');
  }
  lines.push('END:VCALENDAR');

  res.type('text/calendar').send(lines.join('\r\n'));
});

/* ---------------------------- Admin API ---------------------------- */
// Image upload (returns URL to use in profiles/events)
app.post("/api/admin/upload-image/:kind", uploadDisk.single("image"), (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  if (!req.file) return res.status(400).json({ error: "No file" });
  const kind = (req.params.kind || "chapters").toLowerCase() === "events" ? "events" : "chapters";
  const rel = `/uploads/${kind}/${req.file.filename}`;
  const full = `${req.protocol}://${req.get("host")}${rel}`;
  res.json({ url: rel, absolute: full, file: req.file.filename });
});

// Upsert chapter profile
app.post("/api/admin/chapters/:id/profile", (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const id = req.params.id;
  const body = req.body || {};
  const row = db.prepare(`SELECT 1 FROM chapters WHERE id=?`).get(id);
  if (!row) return res.status(404).json({ error:"Chapter not found" });

  const upsert = db.prepare(`
    INSERT INTO chapter_profiles (chapter_id,crest_url,president_name,president_email,president_photo_url)
    VALUES (@chapter_id,@crest_url,@president_name,@president_email,@president_photo_url)
    ON CONFLICT(chapter_id) DO UPDATE SET
      crest_url=excluded.crest_url,
      president_name=excluded.president_name,
      president_email=excluded.president_email,
      president_photo_url=excluded.president_photo_url
  `);
  upsert.run({
    chapter_id:id,
    crest_url: body.crest_url || null,
    president_name: body.president_name || null,
    president_email: body.president_email || null,
    president_photo_url: body.president_photo_url || null
  });
  res.json({ saved:true });
});

// Admin: create/update events (minimal)
app.post("/api/admin/events", (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const b = req.body || {};
  const id = b.id || uuidv4();

  if (!b.chapter_id || !b.title || !b.start_utc) {
    return res.status(400).json({ error:"Provide chapter_id, title, start_utc (ISO)" });
  }
  const exists = db.prepare(`SELECT id FROM chapters WHERE id=?`).get(b.chapter_id);
  if (!exists) return res.status(400).json({ error:"Invalid chapter_id" });

  const now = new Date().toISOString();
  const upsert = db.prepare(`
    INSERT INTO events (id,chapter_id,title,description,location,start_utc,end_utc,visibility,status,created_by,created_at,updated_at)
    VALUES (@id,@chapter_id,@title,@description,@location,@start_utc,@end_utc,@visibility,@status,@created_by,@created_at,@updated_at)
    ON CONFLICT(id) DO UPDATE SET
      chapter_id=excluded.chapter_id,
      title=excluded.title,
      description=excluded.description,
      location=excluded.location,
      start_utc=excluded.start_utc,
      end_utc=excluded.end_utc,
      visibility=excluded.visibility,
      status=excluded.status,
      updated_at=excluded.updated_at
  `);
  upsert.run({
    id,
    chapter_id: b.chapter_id,
    title: b.title,
    description: b.description || null,
    location: b.location || null,
    start_utc: new Date(b.start_utc).toISOString(),
    end_utc: b.end_utc ? new Date(b.end_utc).toISOString() : null,
    visibility: b.visibility || "Public",
    status: b.status || "Approved",
    created_by: b.created_by || null,
    created_at: now,
    updated_at: now
  });
  res.json({ saved:true, id });
});

/* --------- Import: Chapters + EOY (Q4), supports ?dryRun=true --------- */
app.post("/api/admin/import", uploadMem.fields([
  { name: "chaptersFile", maxCount: 1 },
  { name: "eoyFile", maxCount: 1 }
]), (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const dryRun = (req.query.dryRun === "true") || (req.body?.dryRun === true);

  const chaptersFile = req.files?.chaptersFile?.[0];
  const eoyFile = req.files?.eoyFile?.[0];
  if (!chaptersFile || !eoyFile) return res.status(400).json({ error: "Provide both files" });

  // (1) Chapters
  const wbCh = xlsx.read(chaptersFile.buffer, { type: "buffer" });
  let aoa = xlsx.utils.sheet_to_json(wbCh.Sheets[wbCh.SheetNames[0]], { header: 1, raw: false });
  const headers = (aoa[0] || []).map(norm);
  const idxChapter = headers.findIndex(h => h.toLowerCase()==="chapter");
  const idxType    = headers.findIndex(h => h.toLowerCase().includes("type"));
  const idxLoc     = headers.findIndex(h => h.toLowerCase().includes("location"));
  const idxCharter = headers.findIndex(h => h.toLowerCase().includes("charter"));
  if (idxChapter < 0) return res.status(400).json({ error: "Chapters file missing 'Chapter' column on first sheet." });

  const upsert = db.prepare(`
    INSERT INTO chapters (id, code, name, type, city, university, charter_date, status)
    VALUES (@id,@code,@name,@type,@city,@university,@charter_date,@status)
    ON CONFLICT(name) DO UPDATE SET
      type=excluded.type, city=excluded.city, university=excluded.university,
      charter_date=excluded.charter_date, status=excluded.status
  `);

  const nameToId = new Map();
  const existing = db.prepare(`SELECT id, name FROM chapters`).all();
  existing.forEach(r => nameToId.set(r.name.toLowerCase(), r.id));

  let chaptersUpserted = 0;

  for (let i=1;i<aoa.length;i++) {
    const row = aoa[i] || [];
    if (isBlankRow(row)) continue;
    const name = norm(row[idxChapter]); if (!name) continue;

    const typeRaw = idxType>=0 ? norm(row[idxType]) : "";
    const loc = idxLoc>=0 ? norm(row[idxLoc]) : "";
    const charter = idxCharter>=0 ? parseDate(row[idxCharter]) : "1900-01-01";
    const isCollegiate = typeRaw.toLowerCase().startsWith("c");

    let id = nameToId.get(name.toLowerCase());
    if (!id) { id = uuidv4(); nameToId.set(name.toLowerCase(), id); }

    if (!dryRun) {
      upsert.run({
        id, code:name, name,
        type: isCollegiate ? "Collegiate" : "Alumni",
        city: loc,
        university: isCollegiate ? loc : null,
        charter_date: charter,
        status: "Active"
      });
    }
    chaptersUpserted++;
  }

  // (2) EOY -> quarterly Q4
  const wbEoy = xlsx.read(eoyFile.buffer, { type: "buffer" });
  const se = wbEoy.Sheets["Southeastern"];
  if (!se) return res.status(400).json({ error:"No 'Southeastern' sheet" });
  const seAoA = xlsx.utils.sheet_to_json(se, { header:1, raw:false });

  let activeCol = 3;
  for (let i=0;i<Math.min(30, seAoA.length);i++) {
    const row = seAoA[i]||[];
    for (let j=0;j<row.length;j++) {
      if (norm(row[j]).toLowerCase().includes("active")) { activeCol=j; break; }
    }
  }
  const dataStart = 23; // A24
  const yearMatch = (eoyFile.originalname||"").match(/(20\\d{2})/);
  const year = yearMatch? parseInt(yearMatch[1],10): new Date().getFullYear();
  const qStart = `${year}-10-01`;

  if (!dryRun) {
    db.prepare(`DELETE FROM quarterly_stats WHERE year=? AND quarter=4`).run(year);
  }
  const ins = db.prepare(`INSERT INTO quarterly_stats (id,chapter_id,year,quarter,quarter_start_date,active_members,notes)
    VALUES (?,?,?,?,?,?,?)`);

  let statsInserted = 0;
  for (let r=dataStart;r<seAoA.length;r++) {
    const row = seAoA[r]||[];
    const chapter = norm(row[0]); if (!chapter) break;
    const active = parseInt(norm(row[activeCol]).replace(/,/g,""))||0;
    const chId = nameToId.get(chapter.toLowerCase());
    if (!chId) continue;
    if (!dryRun) ins.run(uuidv4(), chId, year, 4, qStart, active, "EOY import");
    statsInserted++;
  }

  res.json({ imported: !dryRun, dryRun, chaptersUpserted, statsInserted, year, quarter:4 });
});

/* -------- Import: Yearly history (LONG or WIDE), supports ?dryRun=true -------- */
app.post("/api/admin/import-yearly", uploadMem.single("historyFile"), (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const dryRun = (req.query.dryRun === "true") || (req.body?.dryRun === true);
  const f = req.file;
  if (!f) return res.status(400).json({ error: "Provide historyFile" });

  const wb = xlsx.read(f.buffer, { type: "buffer" });
  const ws = wb.Sheets[wb.SheetNames[0]];
  if (!ws) return res.status(400).json({ error: "No sheet found" });

  const aoa = xlsx.utils.sheet_to_json(ws, { header: 1, raw: false });
  const headers = (aoa[0] || []).map(h => (h||"").toString().trim());

  const iChapter = headers.findIndex(h => h.toLowerCase() === "chapter");
  const iYear    = headers.findIndex(h => h.toLowerCase() === "year");
  const iActive  = headers.findIndex(h => h.toLowerCase().includes("active"));
  const yearCols = headers.map((h, idx) => ({ h, idx })).filter(({h}) => /^\d{4}$/.test(h));

  if (iChapter < 0) return res.status(400).json({ error: "Missing 'Chapter' column." });
  if (iYear < 0 && yearCols.length === 0)
    return res.status(400).json({ error: "Provide LONG (Chapter|Year|Active Members) or WIDE (Chapter|2021|2022|...)."} );

  const rows = db.prepare(`SELECT id, name FROM chapters`).all();
  const byName = new Map(rows.map(r => [r.name.toLowerCase(), r.id]));

  const ensureChapterId = (name) => {
    const key = (name||"").trim().toLowerCase();
    if (!key) return null;
    if (byName.has(key)) return byName.get(key);
    const id = uuidv4();
    if (!dryRun) {
      db.prepare(`INSERT INTO chapters (id, code, name, type, status)
                  VALUES (?,?,?,?,?) ON CONFLICT(name) DO NOTHING`)
        .run(id, name, name, 'Alumni', 'Active');
    }
    byName.set(key, id);
    return id;
  };

  const upsert = db.prepare(`
    INSERT INTO yearly_stats (id, chapter_id, year, active_members, notes)
    VALUES (@id, @chapter_id, @year, @active_members, 'Yearly import')
    ON CONFLICT(chapter_id, year) DO UPDATE SET
      active_members = excluded.active_members,
      notes = excluded.notes
  `);

  let inserted=0, updated=0, skipped=0;

  // WIDE shape
  if (yearCols.length > 0) {
    for (let r=1; r<aoa.length; r++) {
      const row = aoa[r] || [];
      if (row.every(v => !norm(v))) break;
      const name = norm(row[iChapter]); if (!name) { skipped++; continue; }
      const chId = ensureChapterId(name); if (!chId) { skipped++; continue; }
      for (const { h, idx } of yearCols) {
        const y = parseInt(h,10);
        const raw = norm(row[idx]).replace(/,/g,"");
        if (raw==="") continue;
        const val = parseInt(raw,10) || 0;
        if (!dryRun) {
          const res2 = upsert.run({ id: uuidv4(), chapter_id: chId, year: y, active_members: val });
          if (res2.changes === 1) inserted++; else updated++;
        } else {
          inserted++; // count for preview
        }
      }
    }
    // NEW: record the "as of" timestamp when we actually write
    if (!dryRun) setMeta('yearly_last_upload_at', new Date().toISOString());
    return res.json({ imported: !dryRun, dryRun, shape:"wide", inserted, updated, skipped });
  }

  // LONG shape
  for (let r=1; r<aoa.length; r++) {
    const row = aoa[r] || [];
    if (row.every(v => !norm(v))) break;
    const name = norm(row[iChapter]);
    const y = parseInt(norm(row[iYear]),10);
    const raw = norm(row[iActive]).replace(/,/g,"");
    if (!name || !y || raw==="") { skipped++; continue; }
    const val = parseInt(raw,10) || 0;
    const chId = ensureChapterId(name); if (!chId) { skipped++; continue; }
    if (!dryRun) {
      const res2 = upsert.run({ id: uuidv4(), chapter_id: chId, year: y, active_members: val });
      if (res2.changes === 1) inserted++; else updated++;
    } else {
      inserted++;
    }
  }
  if (!dryRun) setMeta('yearly_last_upload_at', new Date().toISOString()); // NEW
  res.json({ imported: !dryRun, dryRun, shape:"long", inserted, updated, skipped });
});

/* ------------------------------ Exports ------------------------------ */
app.get("/api/export/chapters.csv", (req, res) => {
  const rows = db.prepare(`SELECT id, name, type, city, university, charter_date, status, latitude, longitude FROM chapters ORDER BY name`).all();
  const header = "id,name,type,city,university,charter_date,status,latitude,longitude";
  const csv = [header].concat(rows.map(r => [
    r.id, r.name, r.type, r.city, r.university, r.charter_date, r.status, r.latitude, r.longitude
  ].map(v => `"${(v??"").toString().replace(/"/g,'""')}"`).join(","))).join("\n");
  res.setHeader("Content-Type","text/csv");
  res.setHeader("Content-Disposition","attachment; filename=chapters.csv");
  res.send(csv);
});

app.get("/api/export/yearly_stats.csv", (req, res) => {
  const rows = db.prepare(`
    SELECT y.chapter_id, c.name AS chapter_name, y.year, y.active_members
    FROM yearly_stats y JOIN chapters c ON c.id = y.chapter_id
    ORDER BY c.name, y.year
  `).all();
  const header = "chapter_id,chapter_name,year,active_members";
  const csv = [header].concat(rows.map(r => [
    r.chapter_id, r.chapter_name, r.year, r.active_members
  ].map(v => `"${(v??"").toString().replace(/"/g,'""')}"`).join(","))).join("\n");
  res.setHeader("Content-Type","text/csv");
  res.setHeader("Content-Disposition","attachment; filename=yearly_stats.csv");
  res.send(csv);
});

/* -------------------------- Pretty URLs -------------------------- */
app.get("/admin", (req, res) => res.sendFile(path.join(__dirname, "public", "admin.html")));
app.get("/chapter/:id", (req, res) => res.sendFile(path.join(__dirname, "public", "chapter.html")));
app.get("/calendar", (req, res) => res.sendFile(path.join(__dirname, "public", "calendar.html")));

app.listen(PORT, () => {
  console.log(`NC Sigma Portal http://localhost:${PORT}`);
});
