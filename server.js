/* NC Sigma Portal - Node.js + Express + SQLite + XLSX importer (FULL + Calendar, ET w/ UTC) */

const path = require("path");
const fs = require("fs");
const express = require("express");
const multer = require("multer");
const xlsx = require("xlsx");
const Database = require("better-sqlite3");
const { v4: uuidv4 } = require("uuid");
const cors = require("cors");
require("dotenv").config();
const { parse: parseCsvSync } = require("csv-parse/sync");

const app = express();
const PORT = process.env.PORT || 5000;
const ADMIN_KEY = process.env.ADMIN_KEY || "changeme123";

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

const db = new Database(path.join(__dirname, "app.db"));
db.pragma("foreign_keys = ON");

/* ---------- SCHEMA ---------- */
// Turn on FKs
db.exec("PRAGMA foreign_keys=ON;");

// Core tables
db.exec(`
CREATE TABLE IF NOT EXISTS chapters (
  id TEXT PRIMARY KEY,
  code TEXT,
  name TEXT UNIQUE,
  type TEXT,
  city TEXT,
  university TEXT,
  charter_date TEXT,
  status TEXT,
  instagram_url TEXT,
  facebook_url TEXT,
  latitude REAL,
  longitude REAL
);

CREATE TABLE IF NOT EXISTS yearly_history (
  id TEXT PRIMARY KEY,
  chapter_id TEXT NOT NULL,
  year INTEGER NOT NULL,
  active_members INTEGER DEFAULT 0,
  notes TEXT,
  UNIQUE(chapter_id, year),
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS documents (
  id TEXT PRIMARY KEY,
  chapter_id TEXT,
  title TEXT,
  doc_type TEXT,
  "group" TEXT,
  publish_date TEXT,
  file_url TEXT,
  visibility TEXT,
  tags TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS media (
  id TEXT PRIMARY KEY,
  chapter_id TEXT,
  title TEXT,
  category TEXT,
  event_date TEXT,
  album_url TEXT,
  thumbnail_url TEXT,
  notes TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chapter_profiles (
  chapter_id TEXT PRIMARY KEY,
  crest_url TEXT,
  president_name TEXT,
  president_email TEXT,
  president_photo_url TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chapter_advisors (
  id TEXT PRIMARY KEY,
  chapter_id TEXT NOT NULL,
  advising_chapter_id TEXT NOT NULL,
  name TEXT NOT NULL,
  email TEXT,
  phone TEXT,
  role TEXT,
  photo_url TEXT,
  order_index INTEGER DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE,
  FOREIGN KEY(advising_chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS uploads (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pia_entries (
  id TEXT PRIMARY KEY,
  chapter_id TEXT NOT NULL,
  activity_date TEXT,
  report_year INTEGER,
  hours REAL DEFAULT 0,
  is_bbb INTEGER DEFAULT 0,
  is_education INTEGER DEFAULT 0,
  is_social INTEGER DEFAULT 0,
  is_sbc INTEGER DEFAULT 0,
  description TEXT,
  brothers_attending INTEGER,
  created_at TEXT,
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  chapter_id TEXT,
  title TEXT NOT NULL,
  description TEXT,
  location TEXT,
  start_iso TEXT NOT NULL,
  end_iso TEXT,
  flyer_url TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL,
  approved_at TEXT,
  start_utc TEXT,
  end_utc TEXT,
  tz TEXT DEFAULT 'America/New_York',
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS members (
  id TEXT PRIMARY KEY,
  chapter_id TEXT NOT NULL,
  first_name TEXT,
  last_name TEXT,
  member_number TEXT,
  initiated_date TEXT,
  financial_through_year INTEGER,
  status TEXT DEFAULT 'Active',
  transitioned_alumni_chapter_id TEXT,
  graduation_year TEXT,
  UNIQUE(chapter_id, member_number),
  FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
);
`);

// Pipeline transfers (clean rebuild to ensure correct schema)
db.exec(`
DROP TABLE IF EXISTS pipeline_transfers;
CREATE TABLE pipeline_transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  member_number TEXT NOT NULL UNIQUE,
  from_collegiate_chapter_id TEXT NOT NULL,
  to_alumni_chapter_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('collegiate','transferred')),
  transferred_at TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (from_collegiate_chapter_id) REFERENCES chapters(id) ON DELETE SET NULL,
  FOREIGN KEY (to_alumni_chapter_id) REFERENCES chapters(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_pipeline_status_alumni
  ON pipeline_transfers (status, to_alumni_chapter_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_from_collegiate
  ON pipeline_transfers (from_collegiate_chapter_id);

CREATE TRIGGER IF NOT EXISTS trg_pipeline_transfers_updated_at
AFTER UPDATE ON pipeline_transfers
FOR EACH ROW BEGIN
  UPDATE pipeline_transfers
    SET updated_at = datetime('now')
    WHERE id = NEW.id;
END;

CREATE TABLE IF NOT EXISTS alumni_members (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  member_number TEXT UNIQUE,
  full_name TEXT,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  affiliated_chapter TEXT,
  affiliated_chapter_number TEXT,
  affiliated_chapter_region TEXT,
  affiliated_chapter_university TEXT,
  initiated_chapter TEXT,
  initiated_chapter_region TEXT,
  initiated_chapter_university TEXT,
  initiated_year INTEGER,
  initiated_date TEXT,
  member_type TEXT,
  life_member_type TEXT,
  currently_financial TEXT,
  consecutive_dues TEXT,
  financial_through INTEGER,
  career_field_code TEXT,
  career_field TEXT,
  military_affiliation TEXT,   -- e.g., Army, Navy, None, Unknown
  active_duty TEXT,            -- Yes/No/Unknown
  last_rank_achieved TEXT,
  former_sbc TEXT,
  dsc_member TEXT,
  dsc_number TEXT,
  al_locke_scholar TEXT,
  al_locke_scholar_number TEXT,
  jt_floyd_hof_member TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_alumni_member_number ON alumni_members(member_number);
CREATE INDEX IF NOT EXISTS idx_alumni_affiliated_chapter ON alumni_members(affiliated_chapter);
CREATE INDEX IF NOT EXISTS idx_alumni_career_field ON alumni_members(career_field);
CREATE INDEX IF NOT EXISTS idx_alumni_military ON alumni_members(military_affiliation);
CREATE INDEX IF NOT EXISTS idx_alumni_active_duty ON alumni_members(active_duty);
`);


/* ---------- MIGRATIONS (safe, idempotent) ---------- */
function ensureColumn(table, column, typeSql){
  const cols = db.prepare(`PRAGMA table_info(${table})`).all();
  const has = cols.some(c => (c.name||'').toLowerCase() === column.toLowerCase());
  if (!has){
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeSql}`);
  }
}
function ensureTable(table, createSql){
  try { db.exec(createSql); } catch(e){ /* ignore exists */ }
}

function runRosterMigrations(){
  try { db.exec(`CREATE INDEX IF NOT EXISTS idx_members_chapter ON members(chapter_id)`); } catch(e){}
  try { db.exec(`CREATE INDEX IF NOT EXISTS idx_members_last_first ON members(last_name, first_name)`); } catch(e){}
  try { db.exec(`CREATE INDEX IF NOT EXISTS idx_members_status ON members(status)`); } catch(e){}
}
runRosterMigrations();

function runAdvisorMigrations(){
  try { db.exec(`CREATE INDEX IF NOT EXISTS idx_advisors_chapter ON chapter_advisors(chapter_id, order_index, name)`); } catch(e){}
}
runAdvisorMigrations();

// make sure advising_chapter_id exists
ensureColumn('chapter_advisors', 'advising_chapter_id', 'TEXT');
ensureColumn('members', 'graduation_year', 'TEXT');
ensureColumn('members', 'transitioned_alumni_chapter_id', 'TEXT');

// helpful indexes
try { db.exec(`CREATE INDEX IF NOT EXISTS idx_advisors_advising ON chapter_advisors(advising_chapter_id, chapter_id)`); } catch (e) {}

const EASTERN_TZ = "America/New_York";

/* Convert local ET date+time -> UTC Date using Intl (DST-safe) */
function localTzToUtcDate(dateStr /* YYYY-MM-DD */, timeStr /* HH:mm */, tz = EASTERN_TZ){
  const [y, m, d] = dateStr.split("-").map(n => parseInt(n, 10));
  const [hh, mm]  = timeStr.split(":").map(n => parseInt(n, 10));
  let utc = new Date(Date.UTC(y, (m-1), d, hh, mm, 0));

  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: tz, hour12:false,
    year:"numeric", month:"2-digit", day:"2-digit",
    hour:"2-digit", minute:"2-digit", second:"2-digit"
  });
  function tzWallMinutes(dt){
    const parts = fmt.formatToParts(dt);
    const get = k => parseInt(parts.find(p=>p.type===k).value,10);
    const H = get("hour"), M = get("minute");
    return H*60 + M;
  }
  const want = hh*60 + mm;
  let seen  = tzWallMinutes(utc);
  let diff  = seen - want;
  if (diff !== 0){
    utc = new Date(utc.getTime() - diff*60000);
    const seen2 = tzWallMinutes(utc);
    const diff2 = seen2 - want;
    if (diff2 !== 0) utc = new Date(utc.getTime() - diff2*60000);
  }
  return utc;
}
function easternLocalToUtcISO(dateStr, timeStr){
  return localTzToUtcDate(dateStr, timeStr, EASTERN_TZ).toISOString();
}

function runMigrations(){
  // Ensure missing cols/tables exist
  ensureColumn('documents','chapter_id','TEXT');

  // --- NEW: add PIA money columns (idempotent) ---
  ensureColumn('pia_entries', 'black_spend_amount', 'REAL');            // Column N
  ensureColumn('pia_entries', 'scholarship_funds_disbursed', 'REAL');   // Column P

// --- NEW: social links on chapters (idempotent) ---
  ensureColumn('chapters', 'instagram_url', 'TEXT');
  ensureColumn('chapters', 'facebook_url', 'TEXT');

  ensureTable('events', `
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      chapter_id TEXT,
      title TEXT NOT NULL,
      description TEXT,
      location TEXT,
      start_iso TEXT NOT NULL,
      end_iso TEXT,
      flyer_url TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT NOT NULL,
      approved_at TEXT,
      start_utc TEXT,
      end_utc TEXT,
      tz TEXT DEFAULT 'America/New_York',
      FOREIGN KEY(chapter_id) REFERENCES chapters(id) ON DELETE SET NULL
    );
  `);

  // Make sure columns exist on older DBs (idempotent)
  ensureColumn('events','chapter_id','TEXT');
  ensureColumn('events','title','TEXT');
  ensureColumn('events','description','TEXT');
  ensureColumn('events','location','TEXT');
  ensureColumn('events','start_iso','TEXT');
  ensureColumn('events','end_iso','TEXT');
  ensureColumn('events','flyer_url','TEXT');
  ensureColumn('events','status','TEXT');
  ensureColumn('events','created_at','TEXT');
  ensureColumn('events','approved_at','TEXT');
  ensureColumn('events','start_utc','TEXT');
  ensureColumn('events','end_utc','TEXT');
  ensureColumn('events','tz',`TEXT DEFAULT '${EASTERN_TZ}'`);

  // Helpful indexes
  try { db.exec(`CREATE INDEX IF NOT EXISTS idx_events_status_start ON events(status, start_iso)`); } catch(e){}
  try { db.exec(`CREATE INDEX IF NOT EXISTS idx_documents_group_pub ON documents("group", publish_date)`); } catch(e){}

  // Backfill start_utc/end_utc for any existing rows missing them
  const missing = db.prepare(`
    SELECT id, start_iso, end_iso
    FROM events
    WHERE start_iso IS NOT NULL AND (start_utc IS NULL OR start_utc='')
  `).all();

  const upd = db.prepare(`UPDATE events SET start_utc=?, end_utc=?, tz=? WHERE id=?`);
  for (const r of missing){
    let startUtc = null, endUtc = null;
    if (r.start_iso && r.start_iso.includes('T')){
      const [dStr, tStr] = r.start_iso.split('T');
      startUtc = easternLocalToUtcISO(dStr, tStr);
    }
    if (r.end_iso && r.end_iso.includes('T')){
      const [d2, t2] = r.end_iso.split('T');
      endUtc = easternLocalToUtcISO(d2, t2);
    }
    upd.run(startUtc, endUtc, EASTERN_TZ, r.id);
  }
}
runMigrations();

// --- helpers for case-insensitive "true"-ish text in SQLite
const SQL_TRUE_SET = `('true','1','yes','y','x')`; // include 'x' just in case spreadsheet marks

function whereCurrentlyFinancial(queryHasFilter) {
  // If a filter is passed (e.g. currently_financial=True), enforce it; otherwise no WHERE clause
  if (!queryHasFilter) return '';
  return `WHERE LOWER(TRIM(COALESCE(currently_financial,''))) IN ${SQL_TRUE_SET}`;
}

/* ---------- UTILS ---------- */
function norm(s){ return (s||"").toString().trim(); }
function isBlankObj(o){ return !o || Object.values(o).every(v => norm(v)===""); }
function isBlankRow(arr){ return arr.every(v => !norm(v)); }
const nowIso = ()=> new Date().toISOString();
function slugify(s){
  return (s||'').toString().toLowerCase()
    .replace(/[^a-z0-9]+/g,'-')
    .replace(/^-+|-+$/g,'')
    .slice(0,60) || 'file';
}
/** Robust Excel/CSV date parsing -> ISO yyyy-mm-dd (UTC date only) */
function parseExcelDate(value, fallback=null){
  if (value == null || value === "") return fallback;
  const asNum = typeof value === "number"
    ? value
    : (typeof value === "string" && /^[0-9]+(\.[0-9]+)?$/.test(value) ? parseFloat(value) : NaN);
  if (!isNaN(asNum)) {
    const ms = Math.round(asNum * 86400000);
    const epoch = Date.UTC(1899, 11, 30);
    const d = new Date(epoch + ms);
    return d.toISOString().slice(0,10);
  }
  const s = norm(value);
  let m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
  if (m) {
    const d = new Date(Date.UTC(+m[1], +m[2]-1, +m[3]));
    if (!isNaN(d)) return d.toISOString().slice(0,10);
  }
  m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2}|\d{4})$/);
  if (m) {
    let y = +m[3]; if (y < 100) y = 2000 + y;
    const d = new Date(Date.UTC(y, +m[1]-1, +m[2]));
    if (!isNaN(d)) return d.toISOString().slice(0,10);
  }
  const d = new Date(s);
  if (!isNaN(d)) return new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())).toISOString().slice(0,10);
  return fallback;
}

/** Parse a "Years Paid" string like "23,24,25" -> 2025 */
function parseFinancialThrough(yrs){
  if (!yrs) return null;
  const parts = yrs.toString().match(/\d{1,4}/g) || [];
  if (!parts.length) return null;
  const nums = parts.map(n => parseInt(n, 10)).filter(n => !isNaN(n));
  if (!nums.length) return null;
  let max = Math.max(...nums);
  if (max < 100) max = 2000 + max;
  return max;
}

/** currency parser: "$1,234.50" -> 1234.5 (or null) */
function parseMoney(v){
  if (v == null) return null;
  const s = v.toString().replace(/\$/g,'').replace(/,/g,'').trim();
  if (s === '') return null;
  const n = Number(s);
  return isNaN(n) ? null : n;
}

/** Generic string cleaner */
function cleanStr(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}
/** Split "Full Name" -> { first, last } (very simple heuristic) */
function splitName(full) {
  if (!full) return { first: null, last: null };
  const parts = String(full).trim().split(/\s+/);
  if (parts.length === 1) return { first: parts[0], last: "" };
  return { first: parts.slice(0, -1).join(" "), last: parts.slice(-1)[0] };
}
function toInt(v) {
  const s = (v == null ? "" : String(v)).replace(/\D+/g, "");
  if (!s) return null;
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

/* ---------- PAGE ROUTES ---------- */
app.get("/chapter/:id", (req,res)=>{
  const file = path.join(__dirname, "public", "chapter.html");
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send("chapter.html not found");
});
app.get("/admin", (req,res)=>{
  const file = path.join(__dirname, "public", "admin.html");
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send("admin.html not found");
});
app.get("/conference", (req,res)=>{
  const file = path.join(__dirname, "public", "conference.html");
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send("conference.html not found");
});
/* NEW: calendar page */
app.get("/calendar", (req,res)=>{
  const file = path.join(__dirname, "public", "calendar.html");
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send("calendar.html not found");
});
app.get("/network", (req,res)=>{
  const file = path.join(__dirname, "public", "network.html");
  if (fs.existsSync(file)) return res.sendFile(file);
  res.status(404).send("network.html not found");
});

/* ---------- PUBLIC APIs (existing) ---------- */
app.get("/api/chapters", (req,res)=>{
  res.json(db.prepare(`SELECT * FROM chapters ORDER BY name`).all());
});
app.get("/api/chapters/:id", (req,res)=>{
  const row = db.prepare(`SELECT * FROM chapters WHERE id=?`).get(req.params.id);
  if (!row) return res.status(404).json({error:"Not found"});
  res.json(row);
});
app.get("/api/chapters/:id/profile", (req,res)=>{
  res.json(db.prepare(`SELECT * FROM chapter_profiles WHERE chapter_id=?`).get(req.params.id) || {});
});
app.get("/api/chapters/:id/advisors", (req,res)=>{
  const rows = db.prepare(`
    SELECT a.*, c2.name AS advising_chapter_name
    FROM chapter_advisors a
    LEFT JOIN chapters c2 ON c2.id = a.advising_chapter_id
    WHERE a.chapter_id=?
    ORDER BY a.order_index, a.name
  `).all(req.params.id);
  res.json(rows);
});
app.get("/api/chapters/:id/history-yearly", (req,res)=>{
  res.json(db.prepare(`
    SELECT year, active_members FROM yearly_history
    WHERE chapter_id=? ORDER BY year
  `).all(req.params.id));
});
app.get("/api/chapters/:id/active-latest", (req,res)=>{
  const rows = db.prepare(`
    SELECT year, active_members FROM yearly_history
    WHERE chapter_id=? ORDER BY year
  `).all(req.params.id);
  if (!rows.length) return res.json({ latest:0, year:null, prev:0, delta:0 });
  const last = rows[rows.length-1];
  const prev = rows.length>=2 ? rows[rows.length-2] : { active_members:0, year:null };
  res.json({
    latest: last.active_members||0,
    year: last.year||null,
    prev: prev.active_members||0,
    delta: (last.active_members||0) - (prev.active_members||0)
  });
});

// GET /api/chapters/:id/alumni-roster (single, correct version)
app.get("/api/chapters/:id/alumni-roster", (req, res) => {
  const cid = req.params.id;

  const chapter = db.prepare(`SELECT id, name, type FROM chapters WHERE id=?`).get(cid);
  if (!chapter) return res.status(404).json({ error: "Unknown chapter" });

  // Only for Alumni chapters
  if (String(chapter.type || '').toLowerCase() !== 'alumni') {
    return res.json({ rows: [] });
  }

  const rows = db.prepare(`
    SELECT
      member_number,
      full_name,
      email,
      career_field,
      military_affiliation,
      active_duty,
      financial_through,
      initiated_year,
      member_type,
      currently_financial
    FROM alumni_members
    WHERE lower(trim(affiliated_chapter)) = lower(trim(?))
    AND lower(trim(member_type)) != 'deceased alumni'
    ORDER BY full_name COLLATE NOCASE
  `).all(chapter.name || '');

  // robust: keep digits only, then take the last 4 if present
function last4(v) {
  const digits = (v == null ? '' : String(v)).replace(/\D+/g, '');
  if (!digits) return null;
  return digits.length >= 4 ? digits.slice(-4) : digits; // e.g., "12311997" -> "1997"
  };

  const toBool = (v) => {
    // handle booleans, ints, and common truthy strings
    if (typeof v === 'boolean') return v;
    if (typeof v === 'number') return v !== 0;
    const s = String(v ?? '').trim().toLowerCase();
    return ['true','t','yes','y','1','x'].includes(s);
  };

  res.json({
    rows: rows.map(r => ({
      name: r.full_name || null,
      member_number: r.member_number || null,
      email: r.email || null,
      career_field: r.career_field || null,
      military_affiliation: r.military_affiliation || null,
      active_duty: r.active_duty || null,
      financial_through: last4(r.financial_through),
      initiated_year: r.initiated_year || null,
      member_type: r.member_type || null,
      currently_financial: toBool(r.currently_financial)
    }))
  });
});

/* ---------- Public: Alumni Network Search ---------- */
// /api/network?q=&chapter=&industry=&military=&active_duty=&financial_through_gte=
app.get("/api/network", (req, res) => {
  const { q, chapter, industry, military, active_duty, financial_through_gte } = req.query;
  const where = [];
  const params = {};

  if (q) { where.push(`(full_name LIKE @q OR email LIKE @q OR member_number LIKE @q OR career_field LIKE @q)`); params.q = `%${q}%`; }
  if (chapter) { where.push(`affiliated_chapter = @chapter`); params.chapter = chapter; }
  if (industry) { where.push(`career_field LIKE @industry`); params.industry = `%${industry}%`; }
  if (military) { where.push(`military_affiliation LIKE @military`); params.military = `%${military}%`; }
  if (active_duty) { where.push(`active_duty = @active_duty`); params.active_duty = active_duty; }
  if (financial_through_gte) { where.push(`financial_through >= @ft`); params.ft = parseInt(financial_through_gte, 10); }

  const sql = `
    SELECT member_number, full_name, email, affiliated_chapter,
           career_field, military_affiliation, active_duty,
           financial_through, initiated_year
    FROM alumni_members
    ${where.length ? "WHERE " + where.join(" AND ") : ""}
    ORDER BY full_name COLLATE NOCASE
    LIMIT 500
  `;
  const rows = db.prepare(sql).all(params);
  res.json(rows);
});

/* ---------- Network filter options ---------- */
app.get("/api/network/options", (req, res) => {
  try {
    const careerFields = db.prepare(`
      SELECT DISTINCT career_field AS v
      FROM alumni_members
      WHERE career_field IS NOT NULL AND TRIM(career_field) <> ''
      ORDER BY v COLLATE NOCASE
    `).all().map(r => r.v);

    const affiliatedChapters = db.prepare(`
      SELECT DISTINCT affiliated_chapter AS v
      FROM alumni_members
      WHERE affiliated_chapter IS NOT NULL AND TRIM(affiliated_chapter) <> ''
      ORDER BY v COLLATE NOCASE
    `).all().map(r => r.v);

    res.json({ career_fields: careerFields, affiliated_chapters: affiliatedChapters });
  } catch (e) {
    console.error("[/api/network/options] error", e);
    res.status(500).json({ error: "failed-options" });
  }
});

/* ---------- Public: Alumni by Chapter (for chapter page) ---------- */
app.get("/api/chapters/:id/alumni", (req, res) => {
  // NOTE: If your :id is a chapter UUID, translate to chapter name first and match on affiliated_chapter.
  // Many rosters store the "affiliated_chapter" as the NAME; if you have a mapping table, join on it instead.
  const chapterIdentifier = req.params.id;

  // Attempt: if :id is a UUID, map to chapter name; else treat as name directly
  let chapterName = chapterIdentifier;
  const found = db.prepare(`SELECT name FROM chapters WHERE id=?`).get(chapterIdentifier);
  if (found && found.name) chapterName = found.name;

  const rows = db.prepare(`
    SELECT member_number, full_name, email, career_field, military_affiliation,
           active_duty, financial_through, initiated_year
    FROM alumni_members
    WHERE affiliated_chapter = ?
    ORDER BY full_name COLLATE NOCASE
    LIMIT 1000
  `).all(chapterName);

  res.json(rows);
});

/* ---------- Public: Alumni counts by chapter (for Chapters list badges) ---------- */
app.get("/api/alumni/counts-by-chapter", (req, res) => {
  const rows = db.prepare(`
    SELECT affiliated_chapter AS chapter, COUNT(*) AS count
    FROM alumni_members
    WHERE affiliated_chapter IS NOT NULL AND TRIM(affiliated_chapter) <> ''
    GROUP BY affiliated_chapter
    ORDER BY count DESC
  `).all();
  res.json(rows);
});

// Alumni chapter -> list of collegiate chapters they advise (with crest if available)
app.get("/api/chapters/:id/advised-collegiate", (req, res) => {
  const alumniId = req.params.id;

  try {
    const rows = db.prepare(`
      SELECT DISTINCT
        c.id,
        c.name,
        c.university,
        c.city,
        cp.crest_url
      FROM chapter_advisors ca
      JOIN chapters c
        ON c.id = ca.chapter_id
      LEFT JOIN chapter_profiles cp
        ON cp.chapter_id = c.id
      WHERE ca.advising_chapter_id = ?
        AND lower(c.type) = 'collegiate'
      ORDER BY c.name COLLATE NOCASE
    `).all(alumniId);

    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to load advised collegiate chapters" });
  }
});
app.get("/api/stats/yearly-last-upload", (req,res)=>{
  res.json(db.prepare(`
    SELECT occurred_at AS last_upload_at
    FROM uploads WHERE kind IN ('yearly','history')
    ORDER BY occurred_at DESC LIMIT 1
  `).get() || { last_upload_at:null });
});
app.get("/api/stats/active-brothers/by-type-latest", (req,res)=>{
  const yr = db.prepare(`SELECT MAX(year) AS y FROM yearly_history`).get()?.y;
  if (!yr) return res.json({ year:null, total:0, alumni:0, collegiate:0 });
  const rows = db.prepare(`
    SELECT c.type AS type, SUM(y.active_members) AS members
    FROM yearly_history y
    JOIN chapters c ON c.id=y.chapter_id
    WHERE y.year=? GROUP BY c.type
  `).all(yr);
  let alumni=0, collegiate=0;
  for (const r of rows){
    if ((r.type||'').toLowerCase()==='alumni') alumni=r.members||0;
    if ((r.type||'').toLowerCase()==='collegiate') collegiate=r.members||0;
  }
  res.json({ year:yr, total:alumni+collegiate, alumni, collegiate });
});
app.get("/api/stats/top-membership", (req,res)=>{
  const type = (req.query.type||"").toLowerCase();
  const yr = db.prepare(`SELECT MAX(year) AS y FROM yearly_history`).get()?.y;
  if (!yr) return res.json({ year:null, rows:[] });
  let sql = `
    SELECT c.id, c.name, c.type, y.active_members AS members
    FROM yearly_history y JOIN chapters c ON c.id=y.chapter_id
    WHERE y.year=?`;
  const args=[yr];
  if (type==='alumni' || type==='collegiate'){ sql += ` AND LOWER(c.type)=?`; args.push(type); }
  sql += ` ORDER BY members DESC, c.name LIMIT 5`;
  res.json({ year:yr, rows: db.prepare(sql).all(...args) });
});
/* Total growth KPI: latest vs previous year (all chapters) */
app.get("/api/stats/active-brothers/growth-total", (req, res) => {
  const yNow = db.prepare(`SELECT MAX(year) as y FROM yearly_history`).get()?.y;
  if (!yNow) return res.json({ year: null, prev_year: null, total_now: 0, total_prev: 0, delta: 0, pct: 0 });

  const yPrev = db.prepare(`SELECT MAX(year) as y FROM yearly_history WHERE year < ?`).get(yNow)?.y || null;

  const totalNow = db.prepare(`SELECT SUM(active_members) as s FROM yearly_history WHERE year=?`).get(yNow)?.s || 0;
  const totalPrev = yPrev
    ? (db.prepare(`SELECT SUM(active_members) as s FROM yearly_history WHERE year=?`).get(yPrev)?.s || 0)
    : 0;

  const delta = totalNow - totalPrev;
  const pct = totalPrev > 0 ? (delta / totalPrev) * 100 : (totalNow > 0 ? 100 : 0);

  res.json({
    year: yNow,
    prev_year: yPrev,
    total_now: totalNow,
    total_prev: totalPrev,
    delta,
    pct
  });
});

/* ---------- Public: Aggregated DSC / JTF / Life Member counts ---------- */
// GET /api/stats/alumni-members/honors?currently_financial=True
app.get('/api/stats/alumni-members/honors', (req, res) => {
  const hasFilter = typeof req.query.currently_financial !== 'undefined';
  const whereCF = whereCurrentlyFinancial(hasFilter);

  // Count DSC + JTF as true-ish, Life Member breakdown via LIKE
  const row = db.prepare(`
    SELECT
      SUM(CASE WHEN LOWER(TRIM(COALESCE(dsc_member,'')))        IN ${SQL_TRUE_SET} THEN 1 ELSE 0 END) AS dsc,
      SUM(CASE WHEN LOWER(TRIM(COALESCE(jt_floyd_hof_member,''))) IN ${SQL_TRUE_SET} THEN 1 ELSE 0 END) AS jtf,

      SUM(CASE WHEN LOWER(TRIM(COALESCE(life_member_type,''))) LIKE '%gold%'     THEN 1 ELSE 0 END) AS life_gold,
      SUM(CASE WHEN LOWER(TRIM(COALESCE(life_member_type,''))) LIKE '%sapphire%' THEN 1 ELSE 0 END) AS life_sapphire,
      SUM(CASE WHEN LOWER(TRIM(COALESCE(life_member_type,''))) LIKE '%platinum%' THEN 1 ELSE 0 END) AS life_platinum
    FROM alumni_members
    ${whereCF}
  `).get();

  const life_total = (row?.life_gold || 0) + (row?.life_sapphire || 0) + (row?.life_platinum || 0);

  res.json({
    dsc: row?.dsc || 0,
    jtf: row?.jtf || 0,
    life_gold: row?.life_gold || 0,
    life_sapphire: row?.life_sapphire || 0,
    life_platinum: row?.life_platinum || 0,
    life_total
  });
});

/* ---------- Public: Raw alumni list (filtered) ---------- */
// GET /api/alumni-members?currently_financial=True
app.get('/api/alumni-members', (req, res) => {
  const hasCF = typeof req.query.currently_financial !== 'undefined';
  const whereCF = whereCurrentlyFinancial(hasCF);

  // Keep it simple; limit to something sane
  const rows = db.prepare(`
    SELECT
      member_number, full_name, email,
      life_member_type, currently_financial,
      dsc_member, jt_floyd_hof_member
    FROM alumni_members
    ${whereCF}
    ORDER BY full_name COLLATE NOCASE
    LIMIT 5000
  `).all();

  res.json(rows);
});


/* Top 5 by members added (growth) per type */
app.get("/api/stats/top-growth", (req, res) => {
  const type = (req.query.type || "").toLowerCase(); // 'alumni' | 'collegiate' | ''
  const yNow = db.prepare(`SELECT MAX(year) as y FROM yearly_history`).get()?.y;
  if (!yNow) return res.json({ year: null, prev_year: null, rows: [] });

  const yPrev = db.prepare(`SELECT MAX(year) as y FROM yearly_history WHERE year < ?`).get(yNow)?.y || null;

  // Per-chapter now & prev
  let sql = `
    WITH now AS (
      SELECT chapter_id, active_members FROM yearly_history WHERE year = ?
    ),
    prev AS (
      SELECT chapter_id, active_members FROM yearly_history WHERE year = ?
    )
    SELECT
      c.id, c.name, LOWER(c.type) AS type,
      COALESCE(n.active_members, 0) AS members_now,
      COALESCE(p.active_members, 0) AS members_prev,
      (COALESCE(n.active_members, 0) - COALESCE(p.active_members, 0)) AS delta
    FROM chapters c
    LEFT JOIN now  n ON n.chapter_id = c.id
    LEFT JOIN prev p ON p.chapter_id = c.id
    WHERE 1=1
  `;
  const args = [yNow, yPrev || -1];

  if (type === "alumni" || type === "collegiate") {
    sql += ` AND lower(c.type)=?`;
    args.push(type);
  }

  sql += ` ORDER BY delta DESC, c.name LIMIT 5`;

  const base = db.prepare(sql).all(...args);
  const rows = base.map(r => {
    const prev = r.members_prev || 0;
    const pct = prev > 0 ? (r.delta / prev) * 100 : (r.members_now > 0 ? 100 : 0);
    return {
      id: r.id,
      name: r.name,
      type: r.type,
      members_now: r.members_now,
      members_prev: r.members_prev,
      delta: r.delta,
      pct
    };
  });

  res.json({ year: yNow, prev_year: yPrev, rows });
});

/* Statewide PIA financial totals (scholarships + Black-owned spend) */
app.get("/api/stats/pia/financial-totals", (req, res) => {
  // Robustly sum numbers even if stored as "$1,234" text
  const row = db.prepare(`
    SELECT
      SUM(
        CASE
          WHEN black_spend_amount IS NULL THEN 0
          WHEN typeof(black_spend_amount)='text'
            THEN CAST(REPLACE(REPLACE(black_spend_amount,'$',''),',','') AS REAL)
          ELSE COALESCE(black_spend_amount,0)
        END
      ) AS black_spend_total,
      SUM(
        CASE
          WHEN scholarship_funds_disbursed IS NULL THEN 0
          WHEN typeof(scholarship_funds_disbursed)='text'
            THEN CAST(REPLACE(REPLACE(scholarship_funds_disbursed,'$',''),',','') AS REAL)
          ELSE COALESCE(scholarship_funds_disbursed,0)
        END
      ) AS scholarship_total
    FROM pia_entries
  `).get();

  const last = db.prepare(`
    SELECT occurred_at AS as_of
    FROM uploads
    WHERE kind='pia'
    ORDER BY occurred_at DESC
    LIMIT 1
  `).get();

  res.json({
    black_spend_total: +row?.black_spend_total || 0,
    scholarship_total: +row?.scholarship_total || 0,
    as_of: last?.as_of || null
  });
});

/* Public: chapter advisors */
app.get("/api/chapters/:id/advisors", (req,res)=>{
  const cid = req.params.id;
  const rows = db.prepare(`
    SELECT id, name, email, phone, role, photo_url
    FROM chapter_advisors
    WHERE chapter_id=?
    ORDER BY order_index ASC, name COLLATE NOCASE ASC
  `).all(cid);
  res.json(rows);
});

/* PIA Top N by program & type */
app.get("/api/stats/pia/top", (req,res)=>{
  const program = (req.query.program||"total").toLowerCase();
  const type = (req.query.type||"").toLowerCase();
  const limit = Math.max(1, Math.min(20, parseInt(req.query.limit||"5")));

  let whereProg = "1=1";
  if (program === "bbb") whereProg = "p.is_bbb=1";
  else if (program === "education") whereProg = "p.is_education=1";
  else if (program === "social") whereProg = "p.is_social=1";
  else if (program === "sbc") whereProg = "p.is_sbc=1";

  let whereType = "";
  let params = [];
  if (type === "collegiate" || type === "alumni") {
    whereType = "AND lower(c.type)=?";
    params.push(type);
  }

  const sql = `
    SELECT c.id, c.name, LOWER(c.type) AS type, ROUND(SUM(p.hours),2) AS value
    FROM pia_entries p
    JOIN chapters c ON c.id = p.chapter_id
    WHERE ${whereProg} ${whereType}
    GROUP BY c.id, c.name, c.type
    ORDER BY value DESC, c.name
    LIMIT ?
  `;
  params.push(limit);

  const rows = db.prepare(sql).all(...params);
  res.json({ program, type, rows });
});

/* PIA Summary + Details + Last */
app.get("/api/chapters/:id/pia/summary", (req,res)=>{
  const cid = req.params.id;
  const sum = db.prepare(`
    SELECT
      SUM(hours) AS total_hours,
      SUM(CASE WHEN is_bbb=1 THEN hours ELSE 0 END) AS bbb_hours,
      SUM(CASE WHEN is_social=1 THEN hours ELSE 0 END) AS social_hours,
      SUM(CASE WHEN is_education=1 THEN hours ELSE 0 END) AS education_hours,
      SUM(CASE WHEN is_sbc=1 THEN hours ELSE 0 END) AS sbc_hours,
      /* Robust coercion: handle NULL, numbers, '$', and commas */
      SUM(
        CASE
          WHEN black_spend_amount IS NULL THEN 0
          WHEN typeof(black_spend_amount)='text'
            THEN CAST(REPLACE(REPLACE(black_spend_amount,'$',''),',','') AS REAL)
          ELSE COALESCE(black_spend_amount,0)
        END
      ) AS black_spend_total,

      SUM(
        CASE
          WHEN scholarship_funds_disbursed IS NULL THEN 0
          WHEN typeof(scholarship_funds_disbursed)='text'
            THEN CAST(REPLACE(REPLACE(scholarship_funds_disbursed,'$',''),',','') AS REAL)
          ELSE COALESCE(scholarship_funds_disbursed,0)
        END
      ) AS scholarship_total

    FROM pia_entries WHERE chapter_id=?
  `).get(cid);
  const last = db.prepare(`
    SELECT occurred_at AS as_of FROM uploads
    WHERE kind='pia' ORDER BY occurred_at DESC LIMIT 1
  `).get();
  res.json({
    total_hours:+sum?.total_hours||0,
    bbb_hours:+sum?.bbb_hours||0,
    social_hours:+sum?.social_hours||0,
    education_hours:+sum?.education_hours||0,
    sbc_hours:+sum?.sbc_hours||0,
    black_spend_total: +sum?.black_spend_total || 0,
    scholarship_total: +sum?.scholarship_total || 0,
    as_of:last?.as_of||null
  });
});
app.get("/api/chapters/:id/pia/details", (req,res)=>{
  const cid = req.params.id;
  const program = (req.query.program||"").toLowerCase().trim();
  let where = "chapter_id=?";
  if (program==="bbb") where += " AND is_bbb=1";
  else if (program==="education") where += " AND is_education=1";
  else if (program==="social") where += " AND is_social=1";
  else if (program==="sbc") where += " AND is_sbc=1";

  const rows = db.prepare(`
    SELECT activity_date, description, brothers_attending, hours, black_spend_amount, scholarship_funds_disbursed
    FROM pia_entries
    WHERE ${where}
    ORDER BY COALESCE(activity_date,'' ) DESC, COALESCE(created_at,'') DESC
  `).all(cid);

  res.json(rows.map(r=>({
    activity_date: r.activity_date || null,
    description: r.description || "",
    brothers_attending: (r.brothers_attending==null ? null : Number(r.brothers_attending)),
    hours: (r.hours==null ? null : Number(r.hours)),
    black_spend_amount: (r.black_spend_amount == null ? null : Number(r.black_spend_amount)),
    scholarship_funds_disbursed: (r.scholarship_funds_disbursed == null ? null : Number(r.scholarship_funds_disbursed))
  })));
});
app.get("/api/chapters/:id/pia/last", (req,res)=>{
  const cid = req.params.id;
  const row = db.prepare(`
    SELECT activity_date, hours, description
    FROM pia_entries
    WHERE chapter_id=?
    ORDER BY COALESCE(activity_date, created_at) DESC
    LIMIT 1
  `).get(cid);
  if (!row) return res.json(null);
  res.json({
    date: row.activity_date,
    hours: row.hours==null ? null : Number(row.hours),
    description: row.description || ""
  });
});

/* Public: chapter roster */
app.get("/api/chapters/:id/roster", (req, res) => {
  const cid = req.params.id;
  const rows = db.prepare(`
    SELECT id, first_name, last_name, member_number, initiated_date, financial_through_year, status,  transitioned_alumni_chapter_id, graduation_year
    FROM members
    WHERE chapter_id=?
    ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE
  `).all(cid);
  res.json(rows.map(r => ({
    id: r.id,
    first_name: r.first_name || "",
    last_name: r.last_name || "",
    member_number: r.member_number || "",
    initiated_date: r.initiated_date || null,
    financial_through: r.financial_through_year || null,
    status: r.status || "Active",
     graduation_year: (r.graduation_year == null ? null : String(r.graduation_year).trim()), // <-- "Spring 2025" ok
    transitioned_alumni_chapter_id: r.transitioned_alumni_chapter_id || null
  })));
});

/* ---------- Admin & Imports ---------- */
const memoryUpload = multer({ storage: multer.memoryStorage() });

app.post("/api/admin/chapters/import", memoryUpload.single("chaptersFile"), (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const f = req.file; if (!f) return res.status(400).json({error:"Upload chaptersFile"});
  const wb = xlsx.read(f.buffer, { type:"buffer" });
  const aoa = xlsx.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { header:1 });
  if (!aoa.length) return res.status(400).json({error:"Chapters sheet empty"});
  const hdr = (aoa[0]||[]).map(h=>norm(h).toLowerCase());
  const iChapter = hdr.indexOf("chapter");
  const iType = hdr.findIndex(h=>h.includes("type"));
  const iLoc = hdr.findIndex(h=>h.includes("location"));
  const iCharter = hdr.findIndex(h=>h.includes("charter"));

  const upsert = db.prepare(`
    INSERT INTO chapters (id, code, name, type, city, university, charter_date, status)
    VALUES (@id,@code,@name,@type,@city,@university,@charter_date,@status)
    ON CONFLICT(name) DO UPDATE SET
      type=excluded.type, city=excluded.city, university=excluded.university,
      charter_date=excluded.charter_date, status=excluded.status
  `);

  let n=0;
  for (let r=1;r<aoa.length;r++){
    const row = aoa[r]||[]; if (isBlankRow(row)) continue;
    const name = norm(row[iChapter]); if (!name) continue;
    const typeRaw = iType>=0 ? norm(row[iType]) : "";
    const loc = iLoc>=0 ? norm(row[iLoc]) : "";
    const charterRaw = iCharter>=0 ? row[iCharter] : null;
    const charter = parseExcelDate(charterRaw, null);

    const isCollegiate = typeRaw.toLowerCase().startsWith("c");
    const id = db.prepare(`SELECT id FROM chapters WHERE name=?`).get(name)?.id || uuidv4();
    upsert.run({
      id, code:name, name,
      type: isCollegiate ? "Collegiate" : "Alumni",
      city: loc,
      university: isCollegiate ? loc : null,
      charter_date: charter,
      status: "Active"
    });
    n++;
  }
  db.prepare(`INSERT INTO uploads (id,kind,occurred_at) VALUES (?,?,?)`).run(uuidv4(),'chapters',nowIso());
  res.json({ ok:true, chapters_upserted:n });
});

app.post("/api/admin/eoy/import", memoryUpload.single("eoyFile"), (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const f = req.file; if (!f) return res.status(400).json({error:"Upload eoyFile"});
  const wb = xlsx.read(f.buffer, { type:"buffer" });
  const se = wb.Sheets["Southeastern"];
  if (!se) return res.status(400).json({error:"No 'Southeastern' sheet"});
  const aoa = xlsx.utils.sheet_to_json(se, { header:1 });
  if (!aoa.length) return res.status(400).json({error:"EOY sheet empty"});

  let activeCol = -1;
  for (let i=0;i<30;i++){
    const row = aoa[i]||[];
    for (let j=0;j<row.length;j++){
      const txt = norm(row[j]).toLowerCase();
      if (txt.includes("active")) { activeCol=j; break; }
    }
    if (activeCol>=0) break;
  }
  if (activeCol<0) return res.status(400).json({error:"Could not locate 'Active' column in EOY sheet"});

  const nameToId = new Map(db.prepare(`SELECT id,name FROM chapters`).all().map(r=>[r.name.toLowerCase(), r.id]));
  const startRow = 23; // A24
  const year = (f.originalname||"").match(/(20\d{2})/) ? parseInt((f.originalname.match(/(20\d{2})/))[1]) : new Date().getFullYear();

  const upsertY = db.prepare(`
    INSERT INTO yearly_history (id,chapter_id,year,active_members,notes)
    VALUES (@id,@chapter_id,@year,@active_members,@notes)
    ON CONFLICT(chapter_id,year) DO UPDATE SET
      active_members=excluded.active_members, notes=excluded.notes
  `);

  let imported=0;
  for (let r=startRow; r<aoa.length; r++){
    const row = aoa[r]||[];
    const chap = norm(row[0]); if (!chap) break;
    const id = nameToId.get(chap.toLowerCase()); if (!id) continue;
    const active = parseInt((row[activeCol]||"").toString().replace(/,/g,""))||0;
    upsertY.run({ id:uuidv4(), chapter_id:id, year, active_members:active, notes:"EOY import" });
    imported++;
  }
  db.prepare(`INSERT INTO uploads (id,kind,occurred_at) VALUES (?,?,?)`).run(uuidv4(),'yearly',nowIso());
  res.json({ ok:true, year, rows:imported });
});

app.post("/api/admin/history/import", memoryUpload.single("historyFile"), (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const f = req.file; if (!f) return res.status(400).json({error:"Upload historyFile"});
  const wb = xlsx.read(f.buffer, { type:"buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const aoa = xlsx.utils.sheet_to_json(sheet, { header:1 });
  if (!aoa.length) return res.status(400).json({error:"Empty sheet"});

  const hdr = (aoa[0]||[]).map(h=>norm(h));
  const lower = hdr.map(h=>h.toLowerCase());
  const nameToId = new Map(db.prepare(`SELECT id,name FROM chapters`).all().map(r=>[r.name.toLowerCase(), r.id]));
  const upsertY = db.prepare(`
    INSERT INTO yearly_history (id,chapter_id,year,active_members,notes)
    VALUES (@id,@chapter_id,@year,@active_members,@notes)
    ON CONFLICT(chapter_id,year) DO UPDATE SET
      active_members=excluded.active_members, notes=excluded.notes
  `);

  let imported=0;

  const iChapter = lower.indexOf("chapter");
  const iYear = lower.indexOf("year");
  const iActive = lower.indexOf("active members");

  if (iChapter>=0 && iYear>=0 && iActive>=0){
    for (let r=1;r<aoa.length;r++){
      const row = aoa[r]||[]; if (isBlankRow(row)) continue;
      const name = norm(row[iChapter]); if (!name) continue;
      const id = nameToId.get(name.toLowerCase()); if (!id) continue;
      const year = parseInt(row[iYear]); if (!year) continue;
      const members = parseInt((row[iActive]||"").toString().replace(/,/g,"")) || 0;
      upsertY.run({ id:uuidv4(), chapter_id:id, year, active_members:members, notes:"history import" });
      imported++;
    }
  } else {
    const yearCols=[];
    for (let c=1;c<hdr.length;c++){ const y=parseInt(hdr[c]); if (y) yearCols.push({col:c,year:y}); }
    if (!yearCols.length) return res.status(400).json({error:"Provide LONG (Chapter|Year|Active Members) or WIDE (Chapter|2021|2022|...) format."});
    for (let r=1;r<aoa.length;r++){
      const row = aoa[r]||[]; if (isBlankRow(row)) continue;
      const name = norm(row[0]); if (!name) continue;
      const id = nameToId.get(name.toLowerCase()); if (!id) continue;
      for (const yc of yearCols){
        const members = parseInt((row[yc.col]||"").toString().replace(/,/g,"")) || 0;
        upsertY.run({ id:uuidv4(), chapter_id:id, year:yc.year, active_members:members, notes:"history import (wide)" });
        imported++;
      }
    }
  }

  db.prepare(`INSERT INTO uploads (id,kind,occurred_at) VALUES (?,?,?)`).run(uuidv4(),'history',nowIso());
  res.json({ ok:true, imported });
});

/* --------- NEW: PIA Import (wipes table first, then inserts) --------- */
app.post("/api/admin/pia/import", (req, res) => {
  // Accept ANY field name, then pick the first matching one
  memoryUpload.any()(req, res, (err) => {
    if (err) return res.status(400).send(`Upload error: ${err.message}`);

    // Accept several likely field names from admin.html
    const FIELD_CANDIDATES = ["piaFile", "pia_file", "file", "upload", "pia"];
    const f =
      (req.file && req.file.buffer && req.file) ||
      (Array.isArray(req.files) &&
        (req.files.find(x => FIELD_CANDIDATES.includes(x.fieldname)) || req.files[0]));

    if (!f) return res.status(400).json({ error: "No file received. Use field name 'piaFile' (or 'pia_file')." });

    // --------------------------
    // keep your existing import logic below, swapping any references
    // of `req.file` with the local `f`.
    // Example:
    // let wb; try { wb = xlsx.read(f.buffer, { type:"buffer" }); } ...
    // --------------------------

    let wb;
    try { wb = xlsx.read(f.buffer, { type: "buffer" }); }
    catch (e) { return res.status(400).json({ error: "Unable to read workbook" }); }

    const sheet = wb.Sheets[wb.SheetNames[0]];
    if (!sheet) return res.status(400).json({ error: "No sheet found" });

    const aoa = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: "" });
    if (!aoa.length) return res.status(400).json({ error: "Empty sheet" });

    let headerRow = 0;
    for (let i = 0; i < Math.min(6, aoa.length); i++) {
      const row = (aoa[i] || []).map(x => norm(x).toLowerCase());
      if (row.includes("chapter")) { headerRow = i; break; }
    }

    const json = xlsx.utils.sheet_to_json(sheet, { defval: "", range: headerRow, raw: false });

  // header normalizer (treat underscores & dashes like spaces)
const keyOf = (labelCandidates, rowObj) => {
  const keys = Object.keys(rowObj || {});
  const normalize = k => k.toString().trim().replace(/[-_\s]+/g, " ").toLowerCase();
  const cands = (Array.isArray(labelCandidates) ? labelCandidates : [labelCandidates])
    .map(c => c.toString().trim().replace(/[-_\s]+/g, " ").toLowerCase());
  for (const k of keys) {
    const nk = normalize(k);
    if (cands.some(lbl => nk === lbl || nk.includes(lbl))) return k;
  }
  return null;
};

    // wipe to avoid duplicates
    db.prepare(`DELETE FROM pia_entries`).run();

    const nameToId = new Map(db.prepare(`SELECT id,name FROM chapters`).all().map(r => [r.name.toLowerCase(), r.id]));

    const insert = db.prepare(`
      INSERT INTO pia_entries (
        id, chapter_id, activity_date, report_year, hours,
        is_bbb, is_education, is_social, is_sbc,
        description, brothers_attending,
        black_spend_amount, scholarship_funds_disbursed,
        created_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);

    const tryBool = (v) => {
      const s = (v ?? "").toString().trim().toLowerCase();
      if (!s) return 0;
      if (["x","yes","y","true","1"].includes(s)) return 1;
      const n = Number(s); return isNaN(n) ? 0 : (n > 0 ? 1 : 0);
    };
    const parseMoney = (v) => {
      if (v == null) return null;
      const s = v.toString().replace(/\$/g, "").replace(/,/g, "").trim();
      if (!s) return null;
      const n = Number(s); return isNaN(n) ? null : n;
    };

    let imported = 0, skipped = 0, unknownChapters = new Set();

    for (const r of json) {
      if (!r || Object.values(r).every(v => (v ?? "").toString().trim() === "")) continue;

      const kChapter    = keyOf(["chapter","chapter name"], r);
      const kDate       = keyOf(["program date","activity date","date"], r);
      const kHours      = keyOf(["total hours","hours"], r);
      const kProgram    = keyOf(["program type","program"], r);
      const kBBB        = keyOf(["bbb","bigger & better business","bigger and better business"], r);
      const kEdu        = keyOf(["education"], r);
      const kSocial     = keyOf(["social action","social"], r);
      const kSBC        = keyOf(["sbc","sigma beta club"], r);
      const kDesc       = keyOf(["program description","description","activity description","notes"], r);
      const kBro        = keyOf(["sigma brothers attending","brothers attending","number of sigmas","sigmas attending","# brothers","# of brothers","brothers"], r);

      // money columns (support underscores/typos)
      const kBlackSpend = keyOf([
  "black spend amount",
  "black spend",
  "black dollars spent",
  "black-owned spend"
], r);
      const kSchol = keyOf([
  "scholarship funds disbursed",   // canonical
  "scholarship funds distributed", // alt
  "scholarship funds dispursed",   // common misspelling
  "scholarship amount",
  "scholarship",
], r);

      const chapterName = (kChapter ? r[kChapter] : "").toString().trim();
      if (!chapterName) { skipped++; continue; }
      const chapterId = nameToId.get(chapterName.toLowerCase());
      if (!chapterId) { unknownChapters.add(chapterName); skipped++; continue; }

      const activity_date = parseExcelDate(kDate ? r[kDate] : null, null);
      const hours = Number((kHours ? (r[kHours] || "") : "0").toString().replace(/,/g, "")) || 0;

      let is_bbb=0, is_education=0, is_social=0, is_sbc=0;
      const ptxt = (kProgram ? r[kProgram] : "").toString().trim().toLowerCase();
      if (ptxt.includes("bbb")) is_bbb=1;
      if (ptxt.includes("education")) is_education=1;
      if (ptxt.includes("social")) is_social=1;
      if (ptxt.includes("sbc") || ptxt.includes("sigma beta")) is_sbc=1;
      if (kBBB)   is_bbb = tryBool(r[kBBB]);
      if (kEdu)   is_education = tryBool(r[kEdu]);
      if (kSocial) is_social = tryBool(r[kSocial]);
      if (kSBC)   is_sbc = tryBool(r[kSBC]);

      const description = kDesc ? (r[kDesc] ?? "").toString().trim() : null;

      let brothers_attending = null;
      if (kBro) {
        const v = (r[kBro] ?? "").toString().replace(/,/g, "").trim();
        const n = parseInt(v, 10);
        brothers_attending = Number.isFinite(n) ? n : null;
      }

      const black_spend_amount = parseMoney(kBlackSpend ? r[kBlackSpend] : null);
      const scholarship_funds_disbursed = parseMoney(kSchol ? r[kSchol] : null);

      const report_year = activity_date ? Number(activity_date.slice(0,4)) : null;

      insert.run(
        uuidv4(), chapterId, activity_date, report_year, hours,
        is_bbb, is_education, is_social, is_sbc,
        description, brothers_attending,
        black_spend_amount, scholarship_funds_disbursed,
        nowIso()
      );
      imported++;
    }

    db.prepare(`INSERT INTO uploads (id,kind,occurred_at) VALUES (?,?,?)`).run(uuidv4(),'pia',nowIso());

    res.json({
      ok: true,
      imported,
      skipped,
      unknown_chapters: Array.from(unknownChapters)
    });
  });
});


/* Admin: upsert a resource/document into the documents table (link-based) */
app.post("/api/admin/documents/upsert", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { title, doc_type, group, publish_date, file_url, visibility, tags, chapter_id } = req.body||{};
  if (!title || !group || !file_url){
    return res.status(400).json({ error:"title, group, and file_url are required" });
  }
  let pub = publish_date || null;
  if (pub){
    const d = new Date(pub);
    pub = isNaN(d) ? null : d.toISOString().slice(0,10);
  }
  const id = uuidv4();
  db.prepare(`
    INSERT INTO documents (id, chapter_id, title, doc_type, "group", publish_date, file_url, visibility, tags)
    VALUES (@id, @chapter_id, @title, @doc_type, @group, @publish_date, @file_url, @visibility, @tags)
  `).run({
    id,
    chapter_id: chapter_id || null,
    title,
    doc_type: doc_type || null,
    group,
    publish_date: pub,
    file_url,
    visibility: visibility || 'public',
    tags: tags || null
  });
  res.json({ ok:true, id });
});

/* Local file uploads for Resources/Branding (disk) */
const uploadBaseDir = path.join(__dirname, "public", "uploads", "resources");
fs.mkdirSync(uploadBaseDir, { recursive: true });

const diskStorage = multer.diskStorage({
  destination: (req, file, cb) => {
    const group = slugify((req.body.group || "misc"));
    const dest = path.join(uploadBaseDir, group);
    fs.mkdirSync(dest, { recursive: true });
    cb(null, dest);
  },
  filename: (req, file, cb) => {
    const base = slugify(path.parse(file.originalname).name);
    const ext = path.extname(file.originalname).toLowerCase() || "";
    cb(null, `${Date.now()}-${base}${ext}`);
  }
});
const fileUpload = multer({ storage: diskStorage });

/* Upload file + insert into documents, return public URL */
app.post("/api/admin/documents/upload", fileUpload.single("file"), (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const { title, doc_type, group, publish_date, visibility, tags, chapter_id } = req.body || {};
  if (!req.file) return res.status(400).json({ error: "file is required" });
  if (!title || !group) return res.status(400).json({ error: "title and group are required" });

  let pub = publish_date || null;
  if (pub) {
    const d = new Date(pub);
    pub = isNaN(d) ? null : d.toISOString().slice(0, 10);
  }
  const groupSlug = slugify(group);
  const relPath = `/uploads/resources/${groupSlug}/${req.file.filename}`;
  const id = uuidv4();

  db.prepare(`
    INSERT INTO documents (id, chapter_id, title, doc_type, "group", publish_date, file_url, visibility, tags)
    VALUES (@id, @chapter_id, @title, @doc_type, @group, @publish_date, @file_url, @visibility, @tags)
  `).run({
    id,
    chapter_id: chapter_id || null,
    title,
    doc_type: doc_type || null,
    group,
    publish_date: pub,
    file_url: relPath,
    visibility: visibility || 'public',
    tags: tags || null
  });

  res.json({ ok: true, id, file_url: relPath });
});

/* List documents (used by resources.html) */
app.get("/api/documents", (req,res)=>{
  const rows = db.prepare(`
    SELECT id, chapter_id, title, doc_type, "group", publish_date, file_url, visibility, tags
    FROM documents
    ORDER BY COALESCE(publish_date,'9999-12-31') DESC, title
  `).all();
  res.json(rows);
});

/* Admin: upsert advisor (create or update) */
app.post("/api/admin/advisors/upsert", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { id, chapter_id, name, email, phone, role, photo_url, order_index } = req.body || {};
  if (!chapter_id || !name) return res.status(400).json({error:"chapter_id and name are required"});

  const now = nowIso();
  // Ensure chapter exists
  const ch = db.prepare(`SELECT 1 FROM chapters WHERE id=?`).get(chapter_id);
  if (!ch) return res.status(400).json({error:"Unknown chapter_id"});

  if (id){
    const exists = db.prepare(`SELECT 1 FROM chapter_advisors WHERE id=?`).get(id);
    if (!exists) return res.status(404).json({error:"Advisor not found"});
    db.prepare(`
      UPDATE chapter_advisors
      SET name=?, email=?, phone=?, role=?, photo_url=?, order_index=COALESCE(?,order_index), updated_at=?
      WHERE id=?
    `).run(name, email||null, phone||null, role||null, photo_url||null, (order_index==null?null:+order_index), now, id);
    return res.json({ ok:true, id });
  } else {
    const newId = uuidv4();
    db.prepare(`
      INSERT INTO chapter_advisors (id, chapter_id, name, email, phone, role, photo_url, order_index, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?,0), ?, ?)
    `).run(newId, chapter_id, name, email||null, phone||null, role||null, photo_url||null, (order_index==null?0:+order_index), now, now);
    return res.json({ ok:true, id:newId });
  }
});

/* Admin: upsert advisor (create or update) */
app.post("/api/admin/advisors/upsert", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});

  const {
    id,
    chapter_id,             // the advised chapter (where the advisor appears)
    advising_chapter_id,    // the alumni chapter doing the advising
    name,
    email,
    phone,
    role,
    photo_url,
    order_index
  } = req.body || {};

  if (!chapter_id || !name) return res.status(400).json({error:"chapter_id and name are required"});

  // both chapter ids should exist if provided
  const targetExists  = db.prepare(`SELECT 1 FROM chapters WHERE id=?`).get(chapter_id);
  if (!targetExists) return res.status(400).json({error:"Unknown chapter_id"});

  if (advising_chapter_id) {
    const advisingExists = db.prepare(`SELECT 1 FROM chapters WHERE id=?`).get(advising_chapter_id);
    if (!advisingExists) return res.status(400).json({error:"Unknown advising_chapter_id"});
  }

  const now = nowIso();

  if (id){
    const exists = db.prepare(`SELECT 1 FROM chapter_advisors WHERE id=?`).get(id);
    if (!exists) return res.status(404).json({error:"Advisor not found"});

    db.prepare(`
      UPDATE chapter_advisors
      SET
        chapter_id = ?,
        advising_chapter_id = COALESCE(?, advising_chapter_id),
        name = ?,
        email = ?,
        phone = ?,
        role = ?,
        photo_url = ?,
        order_index = COALESCE(?, order_index),
        updated_at = ?
      WHERE id = ?
    `).run(
      chapter_id,
      advising_chapter_id || null,
      name,
      email || null,
      phone || null,
      role || null,
      photo_url || null,
      (order_index==null ? null : +order_index),
      now,
      id
    );
    return res.json({ ok:true, id });
  } else {
    const newId = uuidv4();
    db.prepare(`
      INSERT INTO chapter_advisors (
        id, chapter_id, advising_chapter_id,
        name, email, phone, role, photo_url,
        order_index, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?,0), ?, ?)
    `).run(
      newId, chapter_id, advising_chapter_id || null,
      name, email || null, phone || null, role || null, photo_url || null,
      (order_index==null ? 0 : +order_index), now, now
    );
    return res.json({ ok:true, id:newId });
  }
});

/* Admin: delete advisor */
app.delete("/api/admin/advisors/:id", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { id } = req.params;
  const row = db.prepare(`SELECT 1 FROM chapter_advisors WHERE id=?`).get(id);
  if (!row) return res.status(404).json({error:"Advisor not found"});
  db.prepare(`DELETE FROM chapter_advisors WHERE id=?`).run(id);
  res.json({ ok:true });
});

/* Admin: reorder advisors (array of {id, order_index}) */
app.post("/api/admin/advisors/reorder", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const items = Array.isArray(req.body?.items) ? req.body.items : [];
  const upd = db.prepare(`UPDATE chapter_advisors SET order_index=?, updated_at=? WHERE id=?`);
  const now = nowIso();
  const tx = db.transaction(()=> items.forEach(it => upd.run(+it.order_index||0, now, it.id)));
  tx();
  res.json({ ok:true, count: items.length });
});


/* ---------- CALENDAR API ---------- */
/* Storage for flyers */
const flyersDir = path.join(__dirname, "public", "uploads", "flyers");
fs.mkdirSync(flyersDir, { recursive: true });
const flyerStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, flyersDir),
  filename: (req, file, cb) => {
    const base = slugify(path.parse(file.originalname).name);
    const ext = path.extname(file.originalname).toLowerCase() || "";
    cb(null, `${Date.now()}-${base}${ext}`);
  }
});
const flyerUpload = multer({ storage: flyerStorage });

/* Public: list approved events (FullCalendar will pass start/end) */
app.get("/api/calendar/events", (req,res)=>{
  const start = req.query.start || "1900-01-01";
  const end   = req.query.end   || "2100-12-31";
  const rows = db.prepare(`
    SELECT e.*, c.name AS chapter_name
    FROM events e
    LEFT JOIN chapters c ON c.id=e.chapter_id
    WHERE e.status='approved'
      AND date(substr(e.start_iso,1,10)) < date(?)
      AND date(substr(e.start_iso,1,10)) >= date(?)
    ORDER BY e.start_iso
  `).all(end, start);

  // Map to FullCalendar event shape (uses local Eastern strings)
  res.json(rows.map(r=>({
    id: r.id,
    title: r.title,
    start: r.start_iso,
    end: r.end_iso || null,
    allDay: false,
    extendedProps: {
      chapter_id: r.chapter_id,
      chapter_name: r.chapter_name || null,
      location: r.location || "",
      description: r.description || "",
      flyer_url: r.flyer_url || null,
      status: r.status,
      tz: r.tz || EASTERN_TZ
    }
  })));
});

/* Public: submit an event -> stored as 'pending' */
app.post("/api/calendar/submit", flyerUpload.single("flyer"), (req,res)=>{
  const { chapter_id, title, description, location, date, start_time, end_time } = req.body || {};
  if (!title || !date || !start_time){
    return res.status(400).json({ error: "title, date, start_time are required" });
  }
  // Optional chapter_id must exist if provided
  if (chapter_id){
    const exists = db.prepare(`SELECT 1 FROM chapters WHERE id=?`).get(chapter_id);
    if (!exists) return res.status(400).json({ error: "Unknown chapter_id" });
  }

  // Local Eastern strings for display
  const start_iso = `${date}T${start_time}`;
  const end_iso   = end_time ? `${date}T${end_time}` : null;

  // UTC mirrors (satisfy DBs that enforced NOT NULL previously)
  const start_utc = easternLocalToUtcISO(date, start_time);
  const end_utc   = end_time ? easternLocalToUtcISO(date, end_time) : null;

  const flyer_url = req.file ? `/uploads/flyers/${req.file.filename}` : null;
  const id = uuidv4();

  db.prepare(`
    INSERT INTO events (
      id, chapter_id, title, description, location,
      start_iso, end_iso, start_utc, end_utc, tz,
      flyer_url, status, created_at
    )
    VALUES (
      @id, @chapter_id, @title, @description, @location,
      @start_iso, @end_iso, @start_utc, @end_utc, @tz,
      @flyer_url, 'pending', @created_at
    )
  `).run({
    id,
    chapter_id: chapter_id || null,
    title: title.trim(),
    description: description || null,
    location: location || null,
    start_iso,
    end_iso,
    start_utc,
    end_utc,
    tz: EASTERN_TZ,
    flyer_url,
    created_at: nowIso()
  });

  res.json({ ok:true, id, status:'pending' });
});

/* Get pipeline status for many members in one call */
/* Set / update a member’s pipeline status (requires admin key) */
app.post('/api/pipeline/transfer', (req, res) => {
  const key = req.get('x-admin-key') || '';
  if (key !== process.env.ADMIN_KEY) return res.status(401).send('unauthorized');

  const {
    member_number,
    from_collegiate_chapter_id,
    status,                     // 'collegiate' | 'transferred'
    to_alumni_chapter_id        // required if status === 'transferred'
  } = req.body || {};

  // Basic required fields
  if (!member_number || !from_collegiate_chapter_id || !status) {
    return res.status(400).send('missing required fields');
  }

  const statusNorm = String(status).toLowerCase();
  if (!['collegiate','transferred'].includes(statusNorm)) {
    return res.status(400).send('invalid status');
  }

  // If transferring, alumni chapter must be provided
  if (statusNorm === 'transferred' && !to_alumni_chapter_id) {
    return res.status(400).send('missing to_alumni_chapter_id');
  }

  const nowIso = new Date().toISOString();
  const memberNumStr = String(member_number);
  const fromIdStr = String(from_collegiate_chapter_id);
  const toIdStr = to_alumni_chapter_id != null ? String(to_alumni_chapter_id) : null;

  const existing = db.prepare(`
    SELECT id FROM pipeline_transfers WHERE member_number = ?
  `).get(memberNumStr);

  if (existing) {
    // If status moves back to collegiate, clear the alumni assignment
    db.prepare(`
      UPDATE pipeline_transfers
      SET status = ?,
          to_alumni_chapter_id = CASE
            WHEN ? = 'collegiate' THEN NULL
            WHEN ? IS NOT NULL THEN ?
            ELSE to_alumni_chapter_id
          END,
          transferred_at = CASE WHEN ? = 'transferred' THEN ? ELSE transferred_at END
      WHERE member_number = ?
    `).run(
      statusNorm,
      statusNorm,
      toIdStr, toIdStr,
      statusNorm, nowIso,
      memberNumStr
    );
  } else {
    db.prepare(`
      INSERT INTO pipeline_transfers
        (member_number, from_collegiate_chapter_id, to_alumni_chapter_id, status, transferred_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(
      memberNumStr,
      fromIdStr,                    // <-- NO unary +
      toIdStr,                      // may be null
      statusNorm,
      statusNorm === 'transferred' ? nowIso : null
    );
  }

  res.json({ ok: true });
});

/* Alumni page pipeline: show brothers assigned to this alumni chapter */
app.get('/api/chapters/:alumniId/pipeline', (req, res) => {
  const alumniId = String(req.params.alumniId); // keep as text; ids in DB are TEXT
  try {
    const rows = db.prepare(`
      SELECT pt.member_number,
             pt.from_collegiate_chapter_id,
             pt.transferred_at,
             c_from.name AS from_collegiate_name
      FROM pipeline_transfers pt
      LEFT JOIN chapters c_from ON c_from.id = pt.from_collegiate_chapter_id
      WHERE pt.to_alumni_chapter_id = ? AND pt.status = 'transferred'
      ORDER BY (pt.transferred_at IS NULL) ASC, pt.transferred_at DESC, pt.member_number
    `).all(alumniId);
    res.json({ rows });
  } catch (e) {
    res.status(500).json({ error: 'failed-alumni-pipeline' });
  }
});

/* Admin: list pending events */
app.get("/api/admin/calendar/pending", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const rows = db.prepare(`
    SELECT e.*, c.name AS chapter_name
    FROM events e
    LEFT JOIN chapters c ON c.id=e.chapter_id
    WHERE e.status='pending'
    ORDER BY e.created_at DESC
  `).all();
  res.json(rows);
});

/* Admin: approve event */
app.post("/api/admin/calendar/approve", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { id } = req.body || {};
  if (!id) return res.status(400).json({error:"id required"});
  const row = db.prepare(`SELECT 1 FROM events WHERE id=?`).get(id);
  if (!row) return res.status(404).json({error:"Event not found"});

  db.prepare(`UPDATE events SET status='approved', approved_at=? WHERE id=?`).run(nowIso(), id);
  res.json({ ok:true });
});

/* Admin: reject (mark rejected) */
app.post("/api/admin/calendar/reject", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { id } = req.body || {};
  if (!id) return res.status(400).json({error:"id required"});
  const row = db.prepare(`SELECT 1 FROM events WHERE id=?`).get(id);
  if (!row) return res.status(404).json({error:"Event not found"});

  db.prepare(`UPDATE events SET status='rejected' WHERE id=?`).run(id);
  res.json({ ok:true });
});

/* ---------- Admin: Chapter Profile (for admin.html Save Profile) ---------- */
app.post("/api/admin/chapter-profile", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { chapter_id, crest_url, president_name, president_email, president_photo_url } = req.body||{};
  if (!chapter_id) return res.status(400).json({error:"chapter_id required"});
  const exists = db.prepare(`SELECT 1 FROM chapters WHERE id=?`).get(chapter_id);
  if (!exists) return res.status(400).json({error:"Unknown chapter_id"});

  db.prepare(`
    INSERT INTO chapter_profiles (chapter_id, crest_url, president_name, president_email, president_photo_url)
    VALUES (@chapter_id, @crest_url, @president_name, @president_email, @president_photo_url)
    ON CONFLICT(chapter_id) DO UPDATE SET
      crest_url=excluded.crest_url,
      president_name=excluded.president_name,
      president_email=excluded.president_email,
      president_photo_url=excluded.president_photo_url
  `).run({ chapter_id, crest_url, president_name, president_email, president_photo_url });

  res.json({ ok:true });
});

/* ---------- Admin: Roster import (Full Roster Excel) ---------- */
app.post("/api/admin/roster/import", memoryUpload.single("rosterFile"), (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const f = req.file; if (!f) return res.status(400).json({ error: "Upload rosterFile" });

  // Read first sheet
  const wb = xlsx.read(f.buffer, { type:"buffer" });
  const ws = wb.Sheets[wb.SheetNames[0]];
  if (!ws) return res.status(400).json({ error: "No sheet found" });

  const rows = xlsx.utils.sheet_to_json(ws, { defval: "" }); // array of objects by header names

  // Expected headers: Chapter, First Name, Last Name, Member Number, Initiated date, Years Paid, Status (optional)
  const nameToId = new Map(db.prepare(`SELECT id,name FROM chapters`).all().map(r => [r.name.toLowerCase(), r.id]));

  const upsert = db.prepare(`
    INSERT INTO members (id, chapter_id, first_name, last_name, member_number, initiated_date, financial_through_year, status)
    VALUES (@id, @chapter_id, @first_name, @last_name, @member_number, @initiated_date, @financial_through_year, @status)
    ON CONFLICT(chapter_id, member_number) DO UPDATE SET
      first_name = excluded.first_name,
      last_name = excluded.last_name,
      initiated_date = excluded.initiated_date,
      financial_through_year = excluded.financial_through_year,
      status = excluded.status
  `);

  let imported = 0, skipped = 0, unknownChapters = new Set();

  const tx = db.transaction(() => {
    for (const r of rows) {
      const chapterName = (r["Chapter"] || r["chapter"] || "").toString().trim();
      if (!chapterName) { skipped++; continue; }
      const chapter_id = nameToId.get(chapterName.toLowerCase());
      if (!chapter_id) { unknownChapters.add(chapterName); skipped++; continue; }

      const first = (r["First Name"] || r["First"] || "").toString().trim();
      const last  = (r["Last Name"]  || r["Last"]  || "").toString().trim();
      const memNo = (r["Member Number"] || r["Member #"] || r["Member"] || "").toString().trim();
      if (!first && !last && !memNo) { skipped++; continue; }

      const initRaw = r["Initiated date"] || r["Initiated Date"] || r["Initiation Date"] || "";
      const initiated_date = parseExcelDate(initRaw, null);

      const yrsPaid = r["Years Paid"] || r["Years paid"] || r["YearsPaid"] || "";
      const financial_through_year = parseFinancialThrough(yrsPaid);

      let status = (r["Status"] || "").toString().trim();
      if (!status) {
        const nowY = new Date().getFullYear();
        status = (financial_through_year && financial_through_year >= nowY) ? "Active" : "Not Financial";
      }

      upsert.run({
        id: uuidv4(),
        chapter_id,
        first_name: first || null,
        last_name:  last  || null,
        member_number: memNo || null,
        initiated_date,
        financial_through_year,
        status
      });
      imported++;
    }
  });
  tx();

  // Track upload
  db.prepare(`INSERT INTO uploads (id,kind,occurred_at) VALUES (?,?,?)`).run(uuidv4(), 'roster', nowIso());

  res.json({
    ok: true,
    imported,
    skipped,
    unknown_chapters: Array.from(unknownChapters)
  });
});

/* ---------- Admin: Alumni Roster (CSV) ---------- */
app.post("/api/admin/alumni/import", memoryUpload.single("alumniFile"), (req, res) => {
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({ error: "Unauthorized" });
  const f = req.file; if (!f) return res.status(400).json({ error: "Upload alumniFile (.csv)" });

  // Try UTF-8 first, fall back to latin-1 if needed
  let content = f.buffer.toString("utf8");
  if (/�/.test(content)) {
    try { content = f.buffer.toString("latin1"); } catch {}
  }

  let records;
  try {
    records = parseCsvSync(content, { columns: true, skip_empty_lines: true });
  } catch (e) {
    return res.status(400).json({ error: "Unable to parse CSV", detail: String(e.message || e) });
  }

  const upsert = db.prepare(`
    INSERT INTO alumni_members (
      member_number, full_name, first_name, last_name, email,
      affiliated_chapter, affiliated_chapter_number, affiliated_chapter_region, affiliated_chapter_university,
      initiated_chapter, initiated_chapter_region, initiated_chapter_university,
      initiated_year, initiated_date, member_type, life_member_type, currently_financial,
      consecutive_dues, financial_through, career_field_code, career_field,
      military_affiliation, active_duty, last_rank_achieved, former_sbc, dsc_member,
      dsc_number, al_locke_scholar, al_locke_scholar_number, jt_floyd_hof_member, updated_at
    ) VALUES (
      @member_number, @full_name, @first_name, @last_name, @email,
      @affiliated_chapter, @affiliated_chapter_number, @affiliated_chapter_region, @affiliated_chapter_university,
      @initiated_chapter, @initiated_chapter_region, @initiated_chapter_university,
      @initiated_year, @initiated_date, @member_type, @life_member_type, @currently_financial,
      @consecutive_dues, @financial_through, @career_field_code, @career_field,
      @military_affiliation, @active_duty, @last_rank_achieved, @former_sbc, @dsc_member,
      @dsc_number, @al_locke_scholar, @al_locke_scholar_number, @jt_floyd_hof_member, datetime('now')
    )
    ON CONFLICT(member_number) DO UPDATE SET
      full_name = excluded.full_name,
      first_name = excluded.first_name,
      last_name = excluded.last_name,
      email = excluded.email,
      affiliated_chapter = excluded.affiliated_chapter,
      affiliated_chapter_number = excluded.affiliated_chapter_number,
      affiliated_chapter_region = excluded.affiliated_chapter_region,
      affiliated_chapter_university = excluded.affiliated_chapter_university,
      initiated_chapter = excluded.initiated_chapter,
      initiated_chapter_region = excluded.initiated_chapter_region,
      initiated_chapter_university = excluded.initiated_chapter_university,
      initiated_year = excluded.initiated_year,
      initiated_date = excluded.initiated_date,
      member_type = excluded.member_type,
      life_member_type = excluded.life_member_type,
      currently_financial = excluded.currently_financial,
      consecutive_dues = excluded.consecutive_dues,
      financial_through = excluded.financial_through,
      career_field_code = excluded.career_field_code,
      career_field = excluded.career_field,
      military_affiliation = excluded.military_affiliation,
      active_duty = excluded.active_duty,
      last_rank_achieved = excluded.last_rank_achieved,
      former_sbc = excluded.former_sbc,
      dsc_member = excluded.dsc_member,
      dsc_number = excluded.dsc_number,
      al_locke_scholar = excluded.al_locke_scholar,
      al_locke_scholar_number = excluded.al_locke_scholar_number,
      jt_floyd_hof_member = excluded.jt_floyd_hof_member,
      updated_at = datetime('now')
  `);

  let inserted = 0, updated = 0, errors = 0;

  const tx = db.transaction((rows) => {
    for (const r of rows) {
      const full_name = cleanStr(r["Full Name"]);
      const nm = splitName(full_name);

      const payload = {
        member_number:            cleanStr(r["Member #"] || r["Member Number"]),
        full_name,
        first_name:               cleanStr(nm.first),
        last_name:                cleanStr(nm.last),
        email:                    cleanStr(r["Email"]),
        affiliated_chapter:      cleanStr(r["Affiliated Chapter"]),
        affiliated_chapter_number: cleanStr(r["Affiliated Chapter Number"]),
        affiliated_chapter_region: cleanStr(r["Affiliated Chapter Region"]),
        affiliated_chapter_university: cleanStr(r["Affiliated Chapter University/Location"]),
        initiated_chapter:       cleanStr(r["Initiated Chapter"]),
        initiated_chapter_region: cleanStr(r["Initiated Chapter Region"]),
        initiated_chapter_university: cleanStr(r["Initiated Chapter University/Location"]),
        initiated_year:          toInt(r["Initiated Year"]),
        initiated_date:          cleanStr(r["Initiated Date"]),
        member_type:             cleanStr(r["Member Type"]),
        life_member_type:        cleanStr(r["Life Member Type"]),
        currently_financial:     cleanStr(r["Currently Financial"]),
        consecutive_dues:        cleanStr(r["Consecutive Dues"]),
        financial_through:       toInt(r["Financial Through"]),
        career_field_code:       cleanStr(r["Career Field Code"]),
        career_field:            cleanStr(r["Career Field"]),
        military_affiliation:    cleanStr(r["Military Affiliation"]),
        active_duty:             cleanStr(r["Active Duty"]),
        last_rank_achieved:      cleanStr(r["Last Rank Achieved"]),
        former_sbc:              cleanStr(r["Former SBC"]),
        dsc_member:              cleanStr(r["DSC Member"]),
        dsc_number:              cleanStr(r["DSC Number"]),
        al_locke_scholar:        cleanStr(r["AL Locke Scholar"]),
        al_locke_scholar_number: cleanStr(r["AL Locke Scholar Number"]),
        jt_floyd_hof_member:     cleanStr(r["JT Floyd HoF Member"])
      };

      if (!payload.member_number) { errors++; continue; }
      const existed = db.prepare(`SELECT 1 FROM alumni_members WHERE member_number=?`).get(payload.member_number);
      upsert.run(payload);
      existed ? updated++ : inserted++;
    }
  });

  try {
    tx(records);
  } catch (e) {
    return res.status(500).json({ error: "DB upsert failed", detail: String(e.message || e) });
  }

  db.prepare(`INSERT INTO uploads (id,kind,occurred_at) VALUES (?,?,?)`)
    .run(uuidv4(), "alumni_csv", new Date().toISOString());

  return res.json({ ok: true, inserted, updated, errors, total: records.length });
});

/* Admin: update a member status (by internal member id) */
app.post("/api/admin/roster/member/status", (req,res)=>{
  if (req.get("x-admin-key") !== ADMIN_KEY) return res.status(401).json({error:"Unauthorized"});
  const { id, status } = req.body || {};
  if (!id || !status) return res.status(400).json({error:"id and status required"});
  const valid = ["Active","Not Financial","Graduated","Inactive","Suspended","Alumni"];
  if (!valid.includes(status)) return res.status(400).json({error:"Invalid status"});
  const row = db.prepare(`SELECT 1 FROM members WHERE id=?`).get(id);
  if (!row) return res.status(404).json({error:"Member not found"});
  db.prepare(`UPDATE members SET status=? WHERE id=?`).run(status, id);
  res.json({ ok:true });
});

/* Admin: update a member status (by chapter + id OR member_number) — this is what your dropdown uses */
app.post('/api/admin/roster/status', (req,res)=>{
  if (req.get('x-admin-key') !== ADMIN_KEY) return res.status(401).json({error:'Unauthorized'});
  const { chapter_id, member_id, status } = req.body||{};
  if(!chapter_id || !member_id || !status) return res.status(400).json({error:'chapter_id, member_id, status required'});
  const valid = ["Active","Not Financial","Graduated","Inactive","Suspended","Alumni"];
  if (!valid.includes(status)) return res.status(400).json({error:"Invalid status"});

  const row = db.prepare(`SELECT id FROM members WHERE chapter_id=? AND (id=? OR member_number=?)`)
                .get(chapter_id, member_id, member_id);
  if (!row) return res.status(404).json({error:"Member not found for chapter"});

  db.prepare(`UPDATE members SET status=? WHERE id=?`).run(status, row.id);
  res.json({ ok:true });
});

/* ---------- START ---------- */
app.listen(PORT, ()=> console.log(`[NC-SIGMA] http://localhost:${PORT}`));
