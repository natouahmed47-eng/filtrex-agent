from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import os
import json
import sqlite3
import datetime

ULTRAMSG_INSTANCE = os.getenv("ULTRAMSG_INSTANCE", "")
ULTRAMSG_TOKEN         = os.getenv("ULTRAMSG_TOKEN", "")
ADMIN_WHATSAPP_NUMBER  = os.getenv("ADMIN_WHATSAPP_NUMBER", "")
print(f"[STARTUP] ADMIN_WHATSAPP_NUMBER={ADMIN_WHATSAPP_NUMBER!r}")

def ultramsg_send(to, text):
    import traceback as _tb
    if any(kw in (text or "") for kw in ("حجز جديد", "📦", "📥", "هل ترغب")):
        print("[TRACE_DUPLICATE_ADMIN_MESSAGE_SOURCE] ⚠️  suspicious text fired:")
        _tb.print_stack()
    url = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"
    payload = {"token": ULTRAMSG_TOKEN, "to": to, "body": text}
    print(f"[ULTRAMSG] sending to={to!r} body={text!r}")
    try:
        resp = requests.post(url, data=payload, timeout=10)
    except Exception as req_err:
        print(f"[ULTRAMSG_ERROR] request failed: {repr(req_err)}")
        return None
    print(f"[ULTRAMSG] response status={resp.status_code} body={resp.text!r}")
    body_lower = resp.text.lower()
    if "demo" in body_lower and "limit" in body_lower:
        print(
            "[ULTRAMSG_LIMIT] ⚠️  Demo daily sending limit exceeded. "
            "Message was NOT delivered. "
            "Either wait for the daily reset (midnight UTC) or upgrade your UltraMsg plan."
        )
    elif resp.status_code != 200 or '"sent"' not in resp.text.lower():
        print(f"[ULTRAMSG_WARN] message may not have been delivered — status={resp.status_code} body={resp.text!r}")
    return resp

app = Flask(__name__)
print("🚀 WHATSAPP TEST VERSION LIVE")

import secrets as _secrets
_session_secret = os.getenv("SESSION_SECRET")
if not _session_secret:
    import warnings
    warnings.warn(
        "SESSION_SECRET environment variable is not set. "
        "A temporary random key is being used — sessions will not persist across restarts. "
        "Set SESSION_SECRET in production.",
        stacklevel=2
    )
    _session_secret = _secrets.token_hex(32)
app.secret_key = _session_secret

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

DB_FILE = "bookings.db"

def get_db_connection():
    con = sqlite3.connect(DB_FILE, timeout=10)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    print("[DB] init_db opening connection")
    con = get_db_connection()
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   TEXT,
                name      TEXT,
                service   TEXT,
                time      TEXT,
                timestamp TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id       INTEGER PRIMARY KEY,
                username TEXT UNIQUE,
                password TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS business_settings (
                user_id          INTEGER PRIMARY KEY,
                business_name    TEXT,
                services         TEXT,
                default_language TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_state (
                phone         TEXT PRIMARY KEY,
                known_service TEXT,
                known_day     TEXT,
                known_time    TEXT,
                known_name    TEXT,
                current_step  TEXT DEFAULT 'service',
                lang          TEXT DEFAULT ''
            )
        """)
        con.execute("INSERT OR IGNORE INTO users (id, username, password) VALUES (1, 'admin', '123456')")
        con.execute("INSERT OR IGNORE INTO users (id, username, password) VALUES (2, 'clinic2', '123456')")
        con.execute("INSERT OR IGNORE INTO business_settings (user_id, business_name, services, default_language) VALUES (1, 'Veltrix Dental Clinic', 'تنظيف أسنان,تبييض أسنان', 'ar')")
        con.execute("INSERT OR IGNORE INTO business_settings (user_id, business_name, services, default_language) VALUES (2, 'Bright Smile Studio', 'فحص أسنان,تبييض أسنان', 'ar')")
        rows = con.execute("SELECT id, password FROM users").fetchall()
        for row in rows:
            pwd = row["password"]
            if not pwd.startswith("pbkdf2:") and not pwd.startswith("scrypt:"):
                con.execute("UPDATE users SET password = ? WHERE id = ?",
                            (generate_password_hash(pwd), row["id"]))
        con.commit()
        print("[DB] init_db committed")
    finally:
        con.close()
        print("[DB] init_db connection closed")

init_db()

def _migrate_whatsapp_state():
    con = get_db_connection()
    try:
        cols = [row[1] for row in con.execute("PRAGMA table_info(whatsapp_state)").fetchall()]
        if "known_day" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN known_day TEXT")
            print("[DB] migration: added known_day")
        if "current_step" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN current_step TEXT DEFAULT 'service'")
            print("[DB] migration: added current_step")
        if "lang" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN lang TEXT DEFAULT ''")
            print("[DB] migration: added lang")
        if "upsell_offered" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN upsell_offered INTEGER DEFAULT 0")
            print("[DB] migration: added upsell_offered")
        if "upsell_rejected" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN upsell_rejected INTEGER DEFAULT 0")
            print("[DB] migration: added upsell_rejected")
        if "completed" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN completed INTEGER DEFAULT 0")
            print("[DB] migration: added completed")
        con.commit()
    finally:
        con.close()

_migrate_whatsapp_state()

# ── SAAS SCHEMA MIGRATION ─────────────────────────────────────────────────────

def _migrate_saas():
    con = get_db_connection()
    try:
        # ── STEP 1: clients ───────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                name              TEXT NOT NULL,
                business_type     TEXT NOT NULL DEFAULT 'clinic',
                default_language  TEXT DEFAULT 'ar',
                currency          TEXT DEFAULT 'SAR',
                timezone          TEXT DEFAULT 'Africa/Nouakchott',
                admin_whatsapp    TEXT,
                ultramsg_instance TEXT,
                ultramsg_token    TEXT,
                is_active         INTEGER DEFAULT 1,
                created_at        TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # ── STEP 2: catalogs ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalogs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id    INTEGER NOT NULL,
                title        TEXT NOT NULL,
                type         TEXT NOT NULL DEFAULT 'service',
                price        REAL NOT NULL DEFAULT 0,
                sale_price   REAL,
                description  TEXT,
                duration_min INTEGER,
                stock_qty    INTEGER,
                is_active    INTEGER DEFAULT 1,
                created_at   TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── STEP 3: catalog_aliases (lang before alias per spec) ─────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog_aliases (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                catalog_id INTEGER NOT NULL,
                lang       TEXT NOT NULL,
                alias      TEXT NOT NULL
            )
        """)

        # ── STEP 4: catalog_options (spec columns) ────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog_options (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                catalog_id   INTEGER NOT NULL,
                option_type  TEXT NOT NULL,
                option_value TEXT NOT NULL,
                extra_price  REAL DEFAULT 0
            )
        """)

        # ── STEP 5: upsells (spec columns) ───────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS upsells (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id          INTEGER NOT NULL,
                source_catalog_id  INTEGER NOT NULL,
                target_catalog_id  INTEGER NOT NULL,
                priority           INTEGER DEFAULT 1
            )
        """)

        # ── STEP 6: conversations ─────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id             INTEGER NOT NULL,
                phone                 TEXT NOT NULL,
                lang                  TEXT DEFAULT '',
                current_step          TEXT DEFAULT 'service',
                known_catalog_ids_json TEXT DEFAULT '[]',
                known_day             TEXT,
                known_time            TEXT,
                known_name            TEXT,
                upsell_offered        INTEGER DEFAULT 0,
                upsell_rejected       INTEGER DEFAULT 0,
                updated_at            TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(client_id, phone)
            )
        """)

        # ── STEP 7: bookings_or_orders ────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS bookings_or_orders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id     INTEGER NOT NULL,
                phone         TEXT NOT NULL,
                customer_name TEXT,
                items_json    TEXT NOT NULL DEFAULT '[]',
                day           TEXT,
                time          TEXT,
                total_price   REAL DEFAULT 0,
                status        TEXT DEFAULT 'new',
                created_at    TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── Legacy orders table (keep for backward compat) ────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER NOT NULL,
                phone      TEXT,
                name       TEXT,
                items      TEXT,
                scheduled  TEXT,
                status     TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── Column migrations for existing tables ─────────────────────────
        # upsells: old schema had trigger_item_id/upsell_item_id/is_active
        _upsell_cols = [r[1] for r in con.execute("PRAGMA table_info(upsells)").fetchall()]
        if "source_catalog_id" not in _upsell_cols:
            con.execute("ALTER TABLE upsells ADD COLUMN source_catalog_id INTEGER DEFAULT 0")
            con.execute("ALTER TABLE upsells ADD COLUMN target_catalog_id INTEGER DEFAULT 0")
            con.execute("ALTER TABLE upsells ADD COLUMN priority INTEGER DEFAULT 1")
            if "trigger_item_id" in _upsell_cols:
                con.execute("UPDATE upsells SET source_catalog_id = trigger_item_id")
                con.execute("UPDATE upsells SET target_catalog_id = upsell_item_id")
            con.commit()
            print("[SAAS] migrated upsells → source_catalog_id/target_catalog_id/priority")

        # catalog_options: old schema had option_key/option_val
        _opt_cols = [r[1] for r in con.execute("PRAGMA table_info(catalog_options)").fetchall()]
        if "option_type" not in _opt_cols:
            con.execute("ALTER TABLE catalog_options ADD COLUMN option_type  TEXT DEFAULT ''")
            con.execute("ALTER TABLE catalog_options ADD COLUMN option_value TEXT DEFAULT ''")
            con.execute("ALTER TABLE catalog_options ADD COLUMN extra_price  REAL DEFAULT 0")
            con.commit()
            print("[SAAS] migrated catalog_options → option_type/option_value/extra_price")

        # catalog_aliases: old schema had (catalog_id, alias, lang) — add lang index if needed
        _alias_cols = [r[1] for r in con.execute("PRAGMA table_info(catalog_aliases)").fetchall()]
        if "lang" not in _alias_cols:
            con.execute("ALTER TABLE catalog_aliases ADD COLUMN lang TEXT DEFAULT 'ar'")
            con.commit()
            print("[SAAS] migrated catalog_aliases → added lang column")

        # ── STEP 8: Seed demo client ──────────────────────────────────────
        exists = con.execute("SELECT id FROM clients WHERE id = 1").fetchone()
        if not exists:
            con.execute("""
                INSERT INTO clients (id, name, business_type, default_language,
                    currency, timezone, admin_whatsapp, is_active)
                VALUES (1, 'Veltrix Dental Clinic', 'clinic', 'ar',
                    'SAR', 'Africa/Nouakchott', ?, 1)
            """, (ADMIN_WHATSAPP_NUMBER,))
            con.commit()
            print("[SAAS] seeded client id=1")

        # ── STEP 8–9: Seed catalog items + multilingual aliases ───────────
        cat_count = con.execute("SELECT COUNT(*) FROM catalogs WHERE client_id=1").fetchone()[0]
        if cat_count == 0:
            _seed = [
                ("تنظيف أسنان", "service", 100, None,
                 "تنظيف احترافي للأسنان يزيل الجير واللويحات الجرثومية", 30, None),
                ("تبييض الأسنان", "service", 250, None,
                 "تبييض متقدم بتقنية LED لابتسامة أكثر إشراقاً", 60, None),
                ("فحص الأسنان", "service", 50, None,
                 "فحص شامل مع تقرير صحة الأسنان", 20, None),
            ]
            cat_ids = []
            for title, typ, price, sale, desc, dur, stock in _seed:
                cur = con.execute("""
                    INSERT INTO catalogs (client_id, title, type, price, sale_price,
                        description, duration_min, stock_qty)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                """, (title, typ, price, sale, desc, dur, stock))
                cat_ids.append(cur.lastrowid)
            con.commit()

            # STEP 9 — multilingual aliases per spec (catalog_id, lang, alias)
            _aliases = [
                (cat_ids[0], "ar", "تنظيف"),
                (cat_ids[0], "ar", "تنظيف أسنان"),
                (cat_ids[0], "en", "cleaning"),
                (cat_ids[0], "en", "teeth cleaning"),
                (cat_ids[0], "fr", "nettoyage"),
                (cat_ids[0], "fr", "nettoyage des dents"),

                (cat_ids[1], "ar", "تبييض"),
                (cat_ids[1], "ar", "تبييض أسنان"),
                (cat_ids[1], "ar", "تبييض الأسنان"),
                (cat_ids[1], "en", "whitening"),
                (cat_ids[1], "en", "teeth whitening"),
                (cat_ids[1], "fr", "blanchiment"),
                (cat_ids[1], "fr", "blanchiment des dents"),

                (cat_ids[2], "ar", "فحص"),
                (cat_ids[2], "ar", "فحص أسنان"),
                (cat_ids[2], "ar", "فحص الأسنان"),
                (cat_ids[2], "en", "checkup"),
                (cat_ids[2], "en", "dental checkup"),
                (cat_ids[2], "fr", "controle"),
                (cat_ids[2], "fr", "consultation"),
            ]
            con.executemany(
                "INSERT INTO catalog_aliases (catalog_id, lang, alias) VALUES (?, ?, ?)",
                _aliases,
            )

            # upsell: cleaning → whitening, checkup → cleaning
            con.executemany(
                "INSERT INTO upsells (client_id, source_catalog_id, target_catalog_id, priority) VALUES (?,?,?,?)",
                [
                    (1, cat_ids[0], cat_ids[1], 1),
                    (1, cat_ids[2], cat_ids[0], 1),
                ]
            )
            con.commit()
            print(f"[SAAS] seeded catalog ids={cat_ids} + aliases + upsells")

    finally:
        con.close()

_migrate_saas()

# ── SAAS HELPERS ──────────────────────────────────────────────────────────────

CLIENT_ID = 1   # single-tenant MVP; future: resolve by webhook token

def get_client(client_id=CLIENT_ID):
    con = get_db_connection()
    try:
        row = con.execute("SELECT * FROM clients WHERE id=?", (client_id,)).fetchone()
    finally:
        con.close()
    return dict(row) if row else {}

def find_catalog_match(client_id, msg, lang="ar"):
    """
    STEP 10 — Matching engine:
    1. Query catalog items for this client filtered by lang
    2. For each item, fetch ALL its aliases (any lang) and check against msg
    3. Return first match as dict, or None
    """
    if not msg:
        return None
    text = (msg or "").lower()
    con = get_db_connection()
    try:
        # Get distinct catalog items that have at least one alias in the detected lang
        cur = con.execute("""
            SELECT DISTINCT c.id, c.title, c.price, c.sale_price, c.type,
                            c.description, c.duration_min, c.is_active, c.client_id
            FROM catalogs c
            JOIN catalog_aliases a ON a.catalog_id = c.id
            WHERE c.client_id = ? AND a.lang = ? AND c.is_active = 1
        """, (client_id, lang))
        rows = cur.fetchall()
        for row in rows:
            cid = row["id"]
            # Fetch ALL aliases for this catalog item (any lang) — longest first
            alias_rows = con.execute(
                "SELECT alias FROM catalog_aliases WHERE catalog_id = ? ORDER BY LENGTH(alias) DESC",
                (cid,)
            ).fetchall()
            aliases = [r["alias"].lower() for r in alias_rows]
            for alias in aliases:
                if alias and alias in text:
                    result = dict(row)
                    print(f"[CATALOG_MATCH] lang={lang!r} alias={alias!r} → id={cid} title={row['title']!r}")
                    return result
    finally:
        con.close()
    # Fallback: try without lang filter (catches cross-language messages)
    con = get_db_connection()
    try:
        all_rows = con.execute("""
            SELECT DISTINCT c.id, c.title, c.price, c.sale_price, c.type,
                            c.description, c.duration_min, c.is_active, c.client_id
            FROM catalogs c
            JOIN catalog_aliases a ON a.catalog_id = c.id
            WHERE c.client_id = ? AND c.is_active = 1
            ORDER BY LENGTH(a.alias) DESC
        """, (client_id,)).fetchall()
        for row in all_rows:
            alias_rows = con.execute(
                "SELECT alias FROM catalog_aliases WHERE catalog_id = ? ORDER BY LENGTH(alias) DESC",
                (row["id"],)
            ).fetchall()
            for ar in alias_rows:
                if ar["alias"].lower() in text:
                    print(f"[CATALOG_MATCH] fallback alias={ar['alias']!r} → id={row['id']}")
                    return dict(row)
    finally:
        con.close()
    print(f"[CATALOG_MATCH] no match for msg={msg!r} lang={lang!r}")
    return None

def get_catalog_item(catalog_id):
    con = get_db_connection()
    try:
        row = con.execute("SELECT * FROM catalogs WHERE id=?", (catalog_id,)).fetchone()
    finally:
        con.close()
    return dict(row) if row else {}

def get_catalog_items(client_id, ids):
    """Return list of active catalog row dicts for given IDs belonging to client_id."""
    if not ids:
        return []
    con = get_db_connection()
    try:
        placeholders = ",".join("?" * len(ids))
        rows = con.execute(
            f"SELECT * FROM catalogs WHERE id IN ({placeholders}) AND client_id=? AND is_active=1",
            (*ids, client_id)
        ).fetchall()
    finally:
        con.close()
    return [dict(r) for r in rows]

def calculate_total(client_id, ids):
    """Sum sale_price (if set and > 0) or price for each catalog item."""
    total = 0.0
    for item in get_catalog_items(client_id, ids):
        p = item.get("sale_price") or item.get("price") or 0
        total += float(p)
    return total

def determine_flow_type(items):
    """Return 'booking', 'order', or 'mixed' based on catalog item types.
    'service' items → booking flow (day/time/name required, slot check applies)
    'product' items → order flow (quantity/address/name required, no slot check)
    mixed → both sets of fields required"""
    if not items:
        return "booking"          # safe default
    types = {(it.get("type") or "service").lower() for it in items}
    if types <= {"service"}:
        return "booking"
    if types <= {"product"}:
        return "order"
    return "mixed"

def get_required_fields(flow_type, items=None):
    """Return ordered list of state keys required to complete the flow."""
    if flow_type == "booking":
        return ["known_name", "known_day", "known_time"]
    if flow_type == "order":
        return ["known_name", "quantity", "address"]
    if flow_type == "mixed":
        return ["known_name", "known_day", "known_time", "quantity", "address"]
    return ["known_name", "known_day", "known_time"]   # safe default

def get_missing_fields(state, required_fields):
    """Return list of required fields not yet present in state."""
    return [f for f in required_fields if not state.get(f)]

# Maps state field name → step name the bot advances to when asking for it
_FIELD_TO_STEP = {
    "known_day":  "day",
    "known_time": "time",
    "known_name": "name",
    "quantity":   "quantity",
    "address":    "address",
}

def get_upsell_for_item(client_id, catalog_id):
    """Return upsell catalog row dict using spec columns (source/target), or None."""
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT c.*
            FROM upsells u
            JOIN catalogs c ON c.id = u.target_catalog_id
            WHERE u.client_id=? AND u.source_catalog_id=? AND c.is_active=1
            ORDER BY u.priority ASC
            LIMIT 1
        """, (client_id, catalog_id)).fetchone()
    finally:
        con.close()
    return dict(row) if row else None

def save_booking_or_order(client_id, phone, name, catalog_ids, day, time, total_price=0):
    """Write to bookings_or_orders (spec table) and legacy orders in one call."""
    # Resolve catalog titles from IDs
    items = []
    for cid in (catalog_ids or []):
        item = get_catalog_item(cid)
        if item:
            items.append(item.get("title", str(cid)))
    items_json = json.dumps(items, ensure_ascii=False)
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO bookings_or_orders
                (client_id, phone, customer_name, items_json, day, time, total_price, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmed')
        """, (client_id, phone, name, items_json, day, time, total_price))
        con.execute("""
            INSERT INTO orders (client_id, phone, name, items, scheduled, status)
            VALUES (?, ?, ?, ?, ?, 'confirmed')
        """, (client_id, phone, name, items_json, f"{day or ''} {time or ''}".strip()))
        con.commit()
    finally:
        con.close()
    print(f"[ORDER_SAVED] bookings_or_orders + orders client={client_id} name={name!r} items={items_json!r}")

def save_order(client_id, phone, name, items, scheduled, status="confirmed"):
    items_json = json.dumps(items, ensure_ascii=False) if isinstance(items, list) else items
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO orders (client_id, phone, name, items, scheduled, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (client_id, phone, name, items_json, scheduled, status))
        con.commit()
    finally:
        con.close()
    print(f"[ORDER_SAVED] client={client_id} phone={phone!r} name={name!r} items={items_json!r}")

bookings = []

def get_biz(user_id):
    print(f"[DB] get_biz opening connection user_id={user_id}")
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT business_name, services, default_language FROM business_settings WHERE user_id = ?",
            (user_id,)
        ).fetchone()
    finally:
        con.close()
        print(f"[DB] get_biz connection closed")
    if row:
        return {
            "business_name": row["business_name"] or "",
            "services": [s.strip() for s in (row["services"] or "").split(",") if s.strip()],
            "default_language": row["default_language"] or "ar"
        }
    return {"business_name": "", "services": [], "default_language": "ar"}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


@app.route("/")
def home():
    if session.get("logged_in"):
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("login"))

@app.route("/assistant")
def assistant():
    return render_template("index.html")

WHATSAPP_USER_ID = 1

def wa_load(phone):
    con = get_db_connection()
    try:
        row = con.execute(
            """SELECT known_service, known_day, known_time, known_name,
                      current_step, lang, upsell_offered, upsell_rejected, completed
               FROM whatsapp_state WHERE phone = ?""",
            (phone,)
        ).fetchone()
    finally:
        con.close()
    if row:
        _svc_raw = row["known_service"]
        if _svc_raw:
            try:
                _parsed = json.loads(_svc_raw)
                _svc_val = _parsed if isinstance(_parsed, list) else [_svc_raw]
            except Exception:
                _svc_val = [_svc_raw]
        else:
            _svc_val = []
        state = {
            "known_service":          _svc_val,
            "known_catalog_ids_json": "[]",     # loaded from conversations below
            "known_day":              row["known_day"],
            "known_time":             row["known_time"],
            "known_name":             row["known_name"],
            "current_step":           row["current_step"] or "service",
            "lang":                   row["lang"] or "",
            "upsell_offered":         bool(row["upsell_offered"]),
            "upsell_rejected":        bool(row["upsell_rejected"]),
            "completed":              bool(row["completed"]),
        }
    else:
        state = {
            "known_service": [], "known_catalog_ids_json": "[]",
            "known_day": None, "known_time": None, "known_name": None,
            "current_step": "service", "lang": "",
            "upsell_offered": False, "upsell_rejected": False,
            "completed": False,
        }
    # ── Load known_catalog_ids_json from conversations table ──────────────
    con2 = get_db_connection()
    try:
        conv = con2.execute(
            "SELECT known_catalog_ids_json FROM conversations WHERE client_id=? AND phone=?",
            (CLIENT_ID, phone)
        ).fetchone()
        if conv and conv["known_catalog_ids_json"]:
            state["known_catalog_ids_json"] = conv["known_catalog_ids_json"]
    finally:
        con2.close()
    print(f"[STATE_LOAD] sender={phone} state={state}")
    return state

def wa_save(phone, state):
    print(f"[STATE_SAVE] sender={phone} state={state}")
    con = get_db_connection()
    try:
        _svc_to_save = state.get("known_service")
        if isinstance(_svc_to_save, list):
            _svc_to_save = json.dumps(_svc_to_save, ensure_ascii=False) if _svc_to_save else None
        # ── whatsapp_state ────────────────────────────────────────────────
        con.execute(
            """INSERT INTO whatsapp_state (phone, known_service, known_day, known_time, known_name, current_step, lang, upsell_offered, upsell_rejected, completed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   known_service   = excluded.known_service,
                   known_day       = excluded.known_day,
                   known_time      = excluded.known_time,
                   known_name      = excluded.known_name,
                   current_step    = excluded.current_step,
                   lang            = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE whatsapp_state.lang END,
                   upsell_offered  = excluded.upsell_offered,
                   upsell_rejected = excluded.upsell_rejected,
                   completed       = excluded.completed""",
            (phone,
             _svc_to_save,
             state.get("known_day"),
             state.get("known_time"),
             state.get("known_name"),
             state.get("current_step", "service"),
             state.get("lang", ""),
             1 if state.get("upsell_offered") else 0,
             1 if state.get("upsell_rejected") else 0,
             1 if state.get("completed") else 0)
        )
        # ── conversations (spec table) ────────────────────────────────────
        _cat_ids_json = state.get("known_catalog_ids_json", "[]")
        con.execute(
            """INSERT INTO conversations
                   (client_id, phone, lang, current_step, known_catalog_ids_json,
                    known_day, known_time, known_name, upsell_offered, upsell_rejected)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(client_id, phone) DO UPDATE SET
                   lang                   = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE conversations.lang END,
                   current_step           = excluded.current_step,
                   known_catalog_ids_json = excluded.known_catalog_ids_json,
                   known_day              = excluded.known_day,
                   known_time             = excluded.known_time,
                   known_name             = excluded.known_name,
                   upsell_offered         = excluded.upsell_offered,
                   upsell_rejected        = excluded.upsell_rejected,
                   updated_at             = CURRENT_TIMESTAMP""",
            (CLIENT_ID, phone,
             state.get("lang", ""),
             state.get("current_step", "service"),
             _cat_ids_json,
             state.get("known_day"),
             state.get("known_time"),
             state.get("known_name"),
             1 if state.get("upsell_offered") else 0,
             1 if state.get("upsell_rejected") else 0)
        )
        con.commit()
        print(f"[STATE_SAVE] committed lang={state.get('lang')!r} catalog_ids={_cat_ids_json!r}")
    except Exception as db_err:
        print(f"[DB] wa_save ERROR: {repr(db_err)}")
        raise
    finally:
        con.close()

def wa_clear(phone):
    print(f"[DB] wa_clear opening connection phone={phone}")
    con = get_db_connection()
    try:
        con.execute("DELETE FROM whatsapp_state WHERE phone = ?", (phone,))
        con.commit()
        print(f"[DB] wa_clear committed")
    except Exception as db_err:
        print(f"[DB] wa_clear ERROR: {repr(db_err)}")
        raise
    finally:
        con.close()
        print(f"[DB] wa_clear connection closed")
    print(f"[WHATSAPP] state_cleared phone={phone}")

WHATSAPP_SYSTEM_PROMPT = """You are a smart WhatsApp business assistant for a dental clinic.

LANGUAGE RULE (MOST IMPORTANT):
- ALWAYS reply in the exact same language the user is writing in.
- If the user writes in English → reply in English.
- If the user writes in Arabic → reply in Arabic.
- If the user writes in French → reply in French.
- Never switch languages unless the user switches first.

IMPORTANT RULES:
- NEVER restart the conversation
- Always continue based on the user's last message
- If user asks about service → continue booking flow
- If user repeats the same request → continue, do NOT restart

BOOKING FLOW:
1. If user asks for a service → suggest booking
2. If user asks price → give price, then ask for booking day
3. If user gives day → ask for exact time
4. If user gives time → ask for name
5. If user gives name → confirm booking

STYLE:
- Short (2-3 lines max)
- Direct
- Friendly
- Sales-focused

DO NOT:
- Reset conversation
- Repeat greeting
- Ask unnecessary questions"""

def detect_lang(msg):
    print(f"[LANG_DETECT] detecting for msg={msg[:40]!r}")
    msg_lower = msg.lower()
    if any(w in msg_lower for w in ["hello", "hi", "hey", "good morning", "good evening", "how are you", "i want", "i need", "please", "thank"]):
        print("[LANG_DETECT] rule=en")
        return "en"
    if any(w in msg_lower for w in ["bonjour", "salut", "bonsoir", "merci", "je veux", "je voudrais"]):
        print("[LANG_DETECT] rule=fr")
        return "fr"
    if any(w in msg_lower for w in ["hola", "buenos", "gracias", "quiero"]):
        print("[LANG_DETECT] rule=es")
        return "es"
    print("[LANG_DETECT] rule=ar (default)")
    return "ar"

def openai_chat(user_message, lang="ar"):
    print(f"[OPENAI] sending message={user_message!r} lang={lang!r}")
    lang_note = f"\n\nSYSTEM LANGUAGE RULE (STRICT):\nYou MUST reply ONLY in this language: {lang}\nDO NOT use any other language.\nDO NOT translate unless the user asks."
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": WHATSAPP_SYSTEM_PROMPT + lang_note},
                {"role": "user",   "content": user_message}
            ]
        },
        timeout=20
    )
    print(f"[OPENAI] response status={resp.status_code} body={resp.text[:300]!r}")
    if resp.status_code == 200:
        return resp.json()["choices"][0]["message"]["content"].strip()
    _err = {"ar": "عذراً، حدث خطأ. يرجى المحاولة مجدداً.",
            "en": "Sorry, an error occurred. Please try again.",
            "fr": "Désolé, une erreur s'est produite. Veuillez réessayer."}
    return _err.get(lang, _err["ar"])

def normalize_number(raw):
    """Return a WhatsApp number in the canonical form  DIGITS@c.us.
    Handles every known malformed variant:
      • "22923289"            → "22923289@c.us"
      • "+22923289"           → "22923289@c.us"
      • "22923289@c.us"       → "22923289@c.us"   (already correct)
      • "c.us@22923289"       → "22923289@c.us"   (reversed)
      • "whatsapp:22923289"   → "22923289@c.us"
      • "  22923289  "        → "22923289@c.us"
    """
    import re as _re
    s = str(raw).strip()
    # Remove known text prefixes
    s = s.replace("whatsapp:", "").replace("@c.us", "").replace("c.us@", "")
    # Strip non-digit characters (handles +, spaces, dashes, dots)
    digits = _re.sub(r"\D", "", s)
    if not digits:
        # Fallback: return raw with @c.us so the caller can log it
        return str(raw).strip() + "@c.us"
    return digits + "@c.us"

def wa_reply(to, text):
    """Send a message to the CUSTOMER only. Never call this with admin content."""
    raw = to
    to  = normalize_number(to)
    print(f"[SEND_CUSTOMER] to={to!r}")
    print(f"[SEND_CUSTOMER] body={text!r}")
    resp = ultramsg_send(to, text)
    print(f"[SEND_CUSTOMER] status={resp.status_code if resp else 'N/A'}")
    return "", 200


# wa_send_admin() REMOVED — use send_booking_messages() as the single send point

_WA_PRICES = {
    "تنظيف أسنان":   "100 ريال",
    "تبييض الأسنان": "250 ريال",
    "فحص الأسنان":   "50 ريال",
}

_STRINGS = {
    "ask_service": {
        "ar": "أهلاً! 😊 كيف يمكنني مساعدتك؟ هل تريد حجز:\n• تنظيف أسنان\n• تبييض الأسنان\n• فحص الأسنان",
        "en": "Hello! 😊 How can I help you? Would you like to book:\n• Teeth cleaning\n• Teeth whitening\n• Dental checkup",
        "fr": "Bonjour! 😊 Comment puis-je vous aider? Souhaitez-vous réserver:\n• Nettoyage des dents\n• Blanchiment des dents\n• Contrôle dentaire",
    },
    "service_confirmed": {
        "ar": "خيار ممتاز ✨ {svc} {benefit}.\nالسعر {price} فقط.",
        "en": "Great choice ✨ {svc} {benefit}.\nOnly {price}.",
        "fr": "Excellent choix ✨ {svc} {benefit}.\nSeulement {price}.",
    },
    "price_list": {
        "ar": (
            "يسعدنا خدمتك! 😊 أسعارنا:\n"
            "• تنظيف أسنان — 100 ريال\n"
            "• تبييض الأسنان — 250 ريال\n"
            "• فحص الأسنان — 50 ريال\n"
            "أي خدمة تناسبك؟"
        ),
        "en": (
            "Happy to help! 😊 Our prices:\n"
            "• Teeth cleaning — 100 SAR\n"
            "• Teeth whitening — 250 SAR\n"
            "• Dental checkup — 50 SAR\n"
            "Which service suits you?"
        ),
        "fr": (
            "Avec plaisir! 😊 Nos tarifs:\n"
            "• Nettoyage des dents — 100 SAR\n"
            "• Blanchiment des dents — 250 SAR\n"
            "• Contrôle dentaire — 50 SAR\n"
            "Quel service vous convient?"
        ),
    },
    "ask_day": {
        "ar": "ممتاز! في أي يوم تفضل؟ (اليوم أو غدًا) 🗓️",
        "en": "Great! Which day do you prefer? (Today or tomorrow) 🗓️",
        "fr": "Parfait! Quel jour préférez-vous? (Aujourd'hui ou demain) 🗓️",
    },
    "ask_time": {
        "ar": "في أي وقت بالضبط؟ 🕐",
        "en": "What time exactly? 🕐",
        "fr": "À quelle heure exactement? 🕐",
    },
    "slot_taken_header": {
        "ar": "عذرًا، هذا الموعد محجوز 🌟\nأقرب الأوقات المتاحة:\n\n",
        "en": "Sorry, that slot is taken 🌟\nNearest available times:\n\n",
        "fr": "Désolé, ce créneau est pris 🌟\nProchains créneaux disponibles:\n\n",
    },
    "slot_taken_footer": {
        "ar": "\n\nهل يناسبك أحدها؟ 😊",
        "en": "\n\nDoes one of these work for you? 😊",
        "fr": "\n\nL'un de ces créneaux vous convient-il? 😊",
    },
    "no_slots": {
        "ar": "عذرًا، لا توجد مواعيد متاحة في هذا اليوم 😔\nهل ترغب في اختيار يوم آخر؟",
        "en": "Sorry, no available slots on that day 😔\nWould you like to choose another day?",
        "fr": "Désolé, aucun créneau disponible ce jour-là 😔\nVoulez-vous choisir un autre jour?",
    },
    "ask_name": {
        "ar": "وما اسمك الكريم؟ 😊",
        "en": "What is your name? 😊",
        "fr": "Quel est votre nom? 😊",
    },
    "booking_confirmed": {
        "ar": "تم حجز موعدك بنجاح ✅\nالخدمة: {svc}\nالموعد: {day} {time}\nالاسم: {name}\nنحن بانتظارك 🌟",
        "en": "Booking confirmed ✅\nService: {svc}\nAppointment: {day} {time}\nName: {name}\nWe look forward to seeing you 🌟",
        "fr": "Réservation confirmée ✅\nService: {svc}\nRendez-vous: {day} {time}\nNom: {name}\nNous avons hâte de vous accueillir 🌟",
    },
    "error": {
        "ar": "عذراً، حدث خطأ. يرجى المحاولة مجدداً.",
        "en": "Sorry, an error occurred. Please try again.",
        "fr": "Désolé, une erreur s'est produite. Veuillez réessayer.",
    },
}

def t(key, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    return _STRINGS.get(key, {}).get(lang) or _STRINGS.get(key, {}).get("ar", "")

def build_ask_service(client_id, lang):
    """Build 'choose a service' prompt dynamically from the catalog."""
    l = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT title, price, sale_price FROM catalogs WHERE client_id=? AND is_active=1 ORDER BY id",
            (client_id,)
        ).fetchall()
    finally:
        con.close()
    cur = get_client(client_id).get("currency", "SAR")
    if not rows:
        return t("ask_service", l)
    bullets = ""
    for r in rows:
        p = r["sale_price"] or r["price"] or 0
        bullets += f"\n• {r['title']} — {int(p)} {cur}"
    _headers = {
        "ar": f"أهلاً! 😊 كيف يمكنني مساعدتك؟ يمكنك الاختيار من:{bullets}",
        "en": f"Hello! 😊 How can I help you? Choose from:{bullets}",
        "fr": f"Bonjour! 😊 Comment puis-je vous aider? Choisissez parmi:{bullets}",
    }
    return _headers[l]

def build_price_list(client_id, lang):
    """Build price list dynamically from the catalog."""
    l = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT title, price, sale_price FROM catalogs WHERE client_id=? AND is_active=1 ORDER BY id",
            (client_id,)
        ).fetchall()
    finally:
        con.close()
    cur = get_client(client_id).get("currency", "SAR")
    if not rows:
        return t("price_list", l)
    bullets = ""
    for r in rows:
        p = r["sale_price"] or r["price"] or 0
        bullets += f"\n• {r['title']} — {int(p)} {cur}"
    _headers = {
        "ar": f"يسعدنا خدمتك! 😊 أسعارنا:{bullets}\nأي خدمة تناسبك؟",
        "en": f"Happy to help! 😊 Our prices:{bullets}\nWhich service suits you?",
        "fr": f"Avec plaisir! 😊 Nos tarifs:{bullets}\nQuel service vous convient?",
    }
    return _headers[l]

_SVC_DISPLAY = {
    "تنظيف أسنان": {
        "ar": "تنظيف أسنان",
        "en": "Teeth Cleaning",
        "fr": "Nettoyage des dents",
    },
    "تبييض الأسنان": {
        "ar": "تبييض الأسنان",
        "en": "Teeth Whitening",
        "fr": "Blanchiment des dents",
    },
    "فحص الأسنان": {
        "ar": "فحص الأسنان",
        "en": "Dental Checkup",
        "fr": "Contrôle dentaire",
    },
}

_PRICE_DISPLAY = {
    "تنظيف أسنان": {
        "ar": "100 ريال",
        "en": "100 SAR",
        "fr": "100 SAR",
    },
    "تبييض الأسنان": {
        "ar": "250 ريال",
        "en": "250 SAR",
        "fr": "250 SAR",
    },
    "فحص الأسنان": {
        "ar": "50 ريال",
        "en": "50 SAR",
        "fr": "50 SAR",
    },
}

def svc_name(canonical, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    return _SVC_DISPLAY.get(canonical, {}).get(lang, canonical)

def svc_price(canonical, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT price, sale_price FROM catalogs WHERE client_id=? AND title=? AND is_active=1",
            (CLIENT_ID, canonical)
        ).fetchone()
    finally:
        con.close()
    if row:
        p = row["sale_price"] or row["price"]
        _cur = get_client(CLIENT_ID).get("currency", "MAD")
        return f"{int(p)} {_cur}"
    return _PRICE_DISPLAY.get(canonical, {}).get(lang, _WA_PRICES.get(canonical, ""))

_SVC_BENEFITS = {
    "تنظيف أسنان": {
        "ar": "يساعد على صحة اللثة ويمنحك إحساسًا بالنظافة والانتعاش",
        "en": "helps improve gum health and leaves your teeth feeling fresh",
        "fr": "aide à garder des gencives saines et une sensation de fraîcheur",
    },
    "تبييض الأسنان": {
        "ar": "يحسّن بياض الابتسامة ويمنحك مظهرًا أكثر إشراقًا",
        "en": "brightens your smile and boosts your appearance and confidence",
        "fr": "illumine votre sourire et améliore votre apparence et confiance",
    },
    "فحص الأسنان": {
        "ar": "يكشف المشاكل مبكرًا ويريحك من القلق على صحة أسنانك",
        "en": "detects issues early and gives you peace of mind about your dental health",
        "fr": "détecte les problèmes tôt et vous rassure sur votre santé dentaire",
    },
}

_RECOMMENDATION = {
    "ar": (
        "أنصحك بـ {svc} كبداية ✨\n"
        "{benefit}.\n"
        "السعر {price} فقط — هل تفضل اليوم أو غدًا؟"
    ),
    "en": (
        "I'd recommend starting with {svc} ✨\n"
        "It {benefit}.\n"
        "Only {price} — would you prefer today or tomorrow?"
    ),
    "fr": (
        "Je vous recommande de commencer par {svc} ✨\n"
        "Cela {benefit}.\n"
        "Seulement {price} — aujourd'hui ou demain?"
    ),
}

_RECOMMEND_KEYWORDS = [
    "recommend", "suggest", "best", "what do you offer", "what should i",
    "which service", "not sure", "don't know", "what's good",
    "ماذا تنصح", "ماذا تقترح", "ايش تنصح", "ما الأفضل", "ما هو الأفضل",
    "مش عارف", "مو عارف", "ما أدري", "شو تنصح",
    "que recommandez", "que conseillez", "quoi choisir", "pas sûr",
]

def svc_benefit(canonical, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT description FROM catalogs WHERE client_id=? AND title=? AND is_active=1",
            (CLIENT_ID, canonical)
        ).fetchone()
    finally:
        con.close()
    if row and row["description"]:
        return row["description"]
    return _SVC_BENEFITS.get(canonical, {}).get(lang, "")

def is_recommendation_request(msg):
    msg_lower = msg.lower()
    return any(kw in msg_lower for kw in _RECOMMEND_KEYWORDS)

def is_affirmation(msg):
    msg = (msg or "").strip().lower()
    return msg in {"yes", "oui", "نعم", "ok", "okay", "يعم", "ايه", "اوك"}

_ADD_INTENT_KEYWORDS = ["أضيف", "اضف", "أضف", "add", "ajoute", "ajouter"]

def is_add_intent(msg):
    text = (msg or "").lower()
    return any(kw in text for kw in _ADD_INTENT_KEYWORDS)

def ensure_svc_list(val):
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]

def format_services(services, lang="ar"):
    if not services:
        return ""
    if isinstance(services, str):
        services = [services]
    _lang  = lang if lang in ("ar", "en", "fr") else "ar"
    _label = {"ar": "الخدمات", "en": "Services", "fr": "Services"}
    names  = [svc_name(s, lang) for s in services]
    lines  = "\n".join(f"• {n}" for n in names)
    return f"{_label[_lang]}:\n{lines}"

def format_svcs(svcs, lang):
    return format_services(svcs, lang)

_NOISE_MESSAGES = {
    "سلام", "السلام", "السلام عليكم", "وعليكم السلام",
    "hi", "hello", "hey",
    "bonjour", "bonsoir", "salut",
}

def is_noise_message(msg):
    return (msg or "").strip().lower() in _NOISE_MESSAGES

_CANONICAL_SERVICE_MAP = {
    "teeth_cleaning":  "تنظيف أسنان",
    "teeth_whitening": "تبييض الأسنان",
    "dental_checkup":  "فحص الأسنان",
}

def extract_entities(msg):
    import re
    text    = (msg or "").lower().strip()
    service = None
    day     = None
    time    = None
    _svc_keywords = {
        "teeth_cleaning":  ["تنظيف", "cleaning", "nettoyage"],
        "teeth_whitening": ["تبييض", "whitening", "blanchiment"],
        "dental_checkup":  ["فحص", "checkup", "consultation", "contrôle", "controle"],
    }
    for _canon, _kws in _svc_keywords.items():
        if any(kw in text for kw in _kws):
            service = _canon
            break
    if "اليوم" in text or "today" in text or "aujourd'hui" in text:
        day = "today"
    elif any(w in text for w in ["غد", "غدا", "غدًا", "tomorrow", "demain"]):
        day = "tomorrow"
    m = re.search(r"(?<!\d)\d{1,2}:\d{2}(?!\d)", text)
    if m:
        time = normalize_time_input(m.group())
    else:
        m = re.search(r"(?<!\d)\d{1,2}(am|pm)(?!\w)", text)
        if m:
            time = normalize_time_input(m.group())
        else:
            m = re.search(r"(?<!\d)\d{1,2}(?!\d)", text)
            if m:
                candidate = normalize_time_input(m.group())
                if is_valid_time(candidate):
                    time = candidate
    print(f"[ENTITY_EXTRACT] service={service!r} day={day!r} time={time!r}")
    return service, day, time

_PARSE_SYSTEM_PROMPT = (
    "You are a dental clinic booking parser. "
    "Parse the user message and return ONLY a valid JSON object with exactly these keys:\n"
    "  intent        — one of: book_service | add_service | cancel | query | affirm | reject | other\n"
    "  service       — one of: teeth_cleaning | teeth_whitening | dental_checkup | null\n"
    "  add_on_service— one of: teeth_cleaning | teeth_whitening | dental_checkup | null\n"
    "  day           — one of: today | tomorrow | null\n"
    "  time          — 24-hour string HH:MM or null\n"
    "  name          — person name string (1-2 words) or null\n"
    "  affirmation   — true if message means yes/ok/confirm, else false\n"
    "  rejection     — true if message means no/refuse, else false\n\n"
    "Rules:\n"
    "- service = main service the user wants to book\n"
    "- add_on_service = service mentioned with add-intent words (أضيف/add/ajoute); if set, service=null\n"
    "- day: اليوم/today/aujourd'hui → today; غدا/غداً/tomorrow/demain → tomorrow\n"
    "- time: normalize to 24-hour HH:MM; 'الساعة 5' or '5 مساء' → '17:00' (assume PM for 1-9)\n"
    "- name: only a person's first name, never a service or sentence\n"
    "- affirmation: نعم/yes/oui/ok/تمام/أكيد/بالتأكيد\n"
    "- rejection: لا/no/non/لأ/ما أبي/ما أريد\n"
    "- Return ONLY the JSON object. No markdown, no explanation."
)

def parse_user_message(msg, lang="ar"):
    _empty = {
        "intent": "other", "service": None, "add_on_service": None,
        "day": None, "time": None, "name": None,
        "affirmation": False, "rejection": False,
    }
    print(f"[PARSE] raw={msg!r}")
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": _PARSE_SYSTEM_PROMPT},
                    {"role": "user",   "content": msg},
                ],
                "temperature": 0,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"[PARSE] OpenAI error status={resp.status_code}")
            return _empty
        import json as _json
        raw_content = resp.json()["choices"][0]["message"]["content"].strip()
        parsed = _json.loads(raw_content)
        for k, v in _empty.items():
            if k not in parsed:
                parsed[k] = v
        print(f"[PARSE] result={parsed}")
        return parsed
    except Exception as _pe:
        print(f"[PARSE] failed={repr(_pe)} — falling back to regex")
        return _empty

_FULL_INTENT_PROMPT = (
    "You are a booking intent extractor for any business type.\n"
    "The user may mention multiple services, a day, a time, and their name all in ONE message.\n\n"
    "Return ONLY a single valid JSON object — no markdown, no explanation, no extra keys:\n"
    "{\n"
    '  "services": ["<service text 1>", "<service text 2>"],\n'
    '  "day": "today" | "tomorrow" | null,\n'
    '  "time": "HH:MM" | null,\n'
    '  "name": "<first name>" | null\n'
    "}\n\n"
    "Rules:\n"
    "- services: list every service the user mentions (book, add, request). Return exact phrasing. Empty list [] if none.\n"
    "- day: اليوم/today/aujourd'hui → \"today\"; غدا/غدً/غداً/tomorrow/demain → \"tomorrow\"; null if absent.\n"
    "- time: normalize any format to 24-hour HH:MM string.\n"
    "  Examples: '5 مساء' → '17:00', '5pm' → '17:00', '9 صباح' → '09:00', '14:30' → '14:30'.\n"
    "  Assume PM (add 12) for 1–9 without explicit AM/morning indicator. null if absent.\n"
    "- name: extract only after words like اسمي/اسم/my name is/je m'appelle/أنا. null if absent.\n"
    "  Must be 1-3 words, a real person name, NOT a service or sentence.\n"
    "- Return ONLY the JSON object. No markdown fences, no explanations."
)

def extract_full_intent(message):
    """
    Use OpenAI to extract structured data from a single message.
    Returns dict: {services: [...], day: str|None, time: str|None, name: str|None}
    Never raises — returns empty structure on any error.
    """
    _empty = {"services": [], "day": None, "time": None, "name": None}
    if not message or len(message.strip()) < 3:
        return _empty
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":    "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": _FULL_INTENT_PROMPT},
                    {"role": "user",   "content": message},
                ],
                "temperature": 0,
            },
            timeout=12,
        )
        if resp.status_code != 200:
            print(f"[INTENT] OpenAI error status={resp.status_code}")
            return _empty
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if model wraps in them
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        out = {
            "services": result.get("services") or [],
            "day":      result.get("day"),
            "time":     result.get("time"),
            "name":     result.get("name"),
        }
        print(f"[INTENT] {out}")
        return out
    except Exception as _e:
        print(f"[INTENT] failed={repr(_e)}")
        return _empty

def is_valid_time(text):
    import re
    text = (text or "").strip().lower()
    if "الساعة" in text:
        return True
    if re.match(r"^\d{1,2}(:\d{2})?$", text):
        return True
    if any(x in text for x in ["am", "pm"]):
        return True
    return False

def is_valid_day(text):
    text = (text or "").strip().lower()
    valid_days = [
        "اليوم", "غدا", "غدًا",
        "today", "tomorrow",
        "aujourd'hui", "demain",
    ]
    return text in valid_days

def is_valid_name(text):
    text = (text or "").strip().lower()
    bad_words = [
        "je veux", "i want", "bonjour", "hello", "salam",
        "تنظيف", "تبييض", "فحص", "اريد", "أريد",
        "service", "nettoyage", "cleaning", "whitening", "checkup",
        "today", "tomorrow", "demain", "اليوم", "غدا", "غدًا",
    ]
    if any(w in text for w in bad_words):
        return False
    if len(text.split()) > 4:
        return False
    if len(text.strip()) < 2:
        return False
    return True

def sanitize_booking_field(text, max_len=40):
    if not text:
        return ""
    text = str(text).strip()
    return text[:max_len]

def build_confirmation(state, items, flow_type, client_config, lang, name):
    """Unified Confirmation Engine — returns formatted confirmation string.
    Fully dynamic: no hardcoded service/product names.
    Branches on flow_type: 'booking' | 'order' | 'mixed'.
    Supports per-item options (size/color) and quantity."""
    l   = lang if lang in ("ar", "en", "fr") else "ar"
    cur = client_config.get("currency", "SAR")
    _ids = [it["id"] for it in items] if items else []

    print(f"[CONFIRM_FLOW_TYPE] {flow_type!r}")
    print(f"[CONFIRM_ITEMS] {[it.get('title') for it in items]}")

    # ── Per-item quantity and options from state ──────────────────────────
    qty_global  = str(state.get("quantity") or state.get("known_quantity") or "").strip()
    known_opts  = state.get("known_options") or {}   # {str(catalog_id): {key: val}}

    def _fmt_item(it):
        title   = it.get("title", "?")
        price   = int(it.get("sale_price") or it.get("price") or 0)
        it_type = (it.get("type") or "service").lower()
        opts    = known_opts.get(str(it.get("id", ""))) or {}
        if opts:
            opts_str = " / ".join(f"{v}" for v in opts.values())
            label = f"• {title} ({opts_str}) — {price} {cur}"
        else:
            label = f"• {title} — {price} {cur}"
        if it_type == "product" and qty_global and qty_global not in ("", "1"):
            label += f" × {qty_global}"
        return label

    svc_items  = [it for it in items if (it.get("type") or "service").lower() == "service"]
    prod_items = [it for it in items if (it.get("type") or "service").lower() == "product"]
    fallback   = items or []

    # ── Total price ──────────────────────────────────────────────────────
    total     = calculate_total(CLIENT_ID, _ids) if _ids else 0.0
    total_str = f"{int(total)} {cur}"
    print(f"[CONFIRM_TOTAL] {total_str}")

    # ── i18n labels — 100% dynamic, nothing hardcoded outside this dict ──
    _L = {
        "hdr_booking": {"ar": "✅ تم تأكيد حجزك",          "en": "✅ Booking confirmed",                       "fr": "✅ Réservation confirmée"},
        "hdr_order":   {"ar": "✅ تم تأكيد طلبك",          "en": "✅ Order confirmed",                         "fr": "✅ Commande confirmée"},
        "hdr_mixed":   {"ar": "✅ تم تأكيد طلبك",          "en": "✅ Order confirmed",                         "fr": "✅ Commande confirmée"},
        "services":    {"ar": "الخدمات:",                  "en": "Services:",                                 "fr": "Services:"},
        "products":    {"ar": "المنتجات:",                 "en": "Products:",                                 "fr": "Produits:"},
        "total":       {"ar": "الإجمالي:",                 "en": "Total:",                                    "fr": "Total:"},
        "appointment": {"ar": "الموعد:",                   "en": "Appointment:",                              "fr": "Rendez-vous:"},
        "address":     {"ar": "عنوان التوصيل:",            "en": "Delivery address:",                         "fr": "Adresse de livraison:"},
        "name":        {"ar": "الاسم:",                    "en": "Name:",                                     "fr": "Nom:"},
        "close_bk":    {"ar": "نحن بانتظارك ⭐",           "en": "We look forward to seeing you ⭐",           "fr": "Nous avons hâte de vous accueillir ⭐"},
        "close_ord":   {"ar": "شكرًا على طلبك 🚀",         "en": "Thank you for your order 🚀",               "fr": "Merci pour votre commande 🚀"},
        "close_mix":   {"ar": "شكرًا! سنتواصل معك قريباً 🚀", "en": "Thanks! We'll be in touch soon 🚀",     "fr": "Merci! Nous vous contacterons bientôt 🚀"},
    }
    def lbl(key): return _L[key].get(l, _L[key]["ar"])

    day     = sanitize_booking_field(state.get("known_day"))
    time    = sanitize_booking_field(state.get("known_time"))
    address = (state.get("known_address") or state.get("address") or "").strip()
    parts   = []

    if flow_type == "booking":
        parts += [lbl("hdr_booking"), ""]
        parts += [lbl("services")] + [_fmt_item(it) for it in (svc_items or fallback)]
        if total > 0:
            parts += ["", f"{lbl('total')} {total_str}"]
        if day or time:
            parts.append(f"{lbl('appointment')} {day} {time}".strip())
        parts.append(f"{lbl('name')} {name}")
        parts += ["", lbl("close_bk")]

    elif flow_type == "order":
        parts += [lbl("hdr_order"), ""]
        parts += [lbl("products")] + [_fmt_item(it) for it in (prod_items or fallback)]
        parts += ["", f"{lbl('total')} {total_str}"]
        if address:
            parts.append(f"{lbl('address')} {address}")
        parts.append(f"{lbl('name')} {name}")
        parts += ["", lbl("close_ord")]

    else:   # mixed
        parts += [lbl("hdr_mixed"), ""]
        if svc_items:
            parts += [lbl("services")] + [_fmt_item(it) for it in svc_items]
        if prod_items:
            parts += [lbl("products")] + [_fmt_item(it) for it in prod_items]
        if not svc_items and not prod_items:
            parts += [_fmt_item(it) for it in fallback]
        parts += ["", f"{lbl('total')} {total_str}"]
        if day or time:
            parts.append(f"{lbl('appointment')} {day} {time}".strip())
        if address:
            parts.append(f"{lbl('address')} {address}")
        parts.append(f"{lbl('name')} {name}")
        parts += ["", lbl("close_mix")]

    return "\n".join(parts)


def confirmation_message(state, name, lang, phone=None):
    """Thin wrapper — resolves items/flow/client then delegates to build_confirmation()."""
    _ids         = json.loads(state.get("known_catalog_ids_json") or "[]")
    items        = get_catalog_items(CLIENT_ID, _ids)
    flow         = determine_flow_type(items)
    client_cfg   = get_client(CLIENT_ID)
    l            = lang if lang in ("ar", "en", "fr") else "ar"
    # Fallback item list from known_service if catalog IDs resolve nothing
    if not items:
        _svcs = ensure_svc_list(state.get("known_service"))
        items = [{"id": None, "title": s, "type": "service", "price": 0, "sale_price": None}
                 for s in _svcs]
        flow  = "booking"
    return build_confirmation(state, items, flow, client_cfg, l, name)

_RECOMMENDED_SERVICE = "تنظيف أسنان"

_UPSELL_MAP = {
    "تنظيف أسنان":   "تبييض الأسنان",
    "فحص الأسنان":   "تنظيف أسنان",
    "تبييض الأسنان": "فحص الأسنان",
}

def build_times_hint(svc, lang, day_offset=0, day=None):
    _day = (day or "اليوم").strip()
    _day_label = {"اليوم": {"ar": "اليوم", "en": "today", "fr": "aujourd'hui"},
                  "غدا":   {"ar": "غداً",  "en": "tomorrow", "fr": "demain"}}
    _dl = _day_label.get(_day, {}).get(lang, _day)

    priority = get_time_priority(_day)          # [(time, count), ...]
    available = [(t, c) for t, c in priority]  # all priority slots with counts

    urgency = len([t for t, c in available if c == 0]) == 1  # exactly 1 free slot

    if not available:
        _fallback = _ALL_TIMES[-2:]
        t1, t2 = _fallback[0], _fallback[1]
        _hints = {
            "ar": f"لدينا مواعيد {_dl} الساعة {t1} أو {t2}، أيهما يناسبك؟",
            "en": f"We have slots {_dl} at {t1} or {t2}. Which works best?",
            "fr": f"Nous avons des créneaux {_dl} à {t1} ou {t2}. Lequel vous convient?",
        }
        return _hints.get(lang, _hints["ar"])

    t1, c1 = available[0]
    if urgency and c1 == 0:
        _urgent = {
            "ar": f"بقي موعد أخير {_dl} الساعة {t1} 🔥\nأيهم يناسبك؟ 😊",
            "en": f"Only one slot left {_dl} at {t1} 🔥\nDoes that work for you? 😊",
            "fr": f"Il ne reste qu'un créneau {_dl} à {t1} 🔥\nCela vous convient? 😊",
        }
        return _urgent.get(lang, _urgent["ar"])

    if len(available) >= 2:
        t2, _ = available[1]
        _hints = {
            "ar": (f"لدينا موعد متاح {_dl} الساعة {t1} ⭐\n"
                   f"ويوجد أيضًا {t2} إذا رغبت\n"
                   f"أيهم يناسبك؟ 😊"),
            "en": (f"We have a slot {_dl} at {t1} ⭐\n"
                   f"Also available: {t2}\n"
                   f"Which works best? 😊"),
            "fr": (f"Nous avons un créneau {_dl} à {t1} ⭐\n"
                   f"Aussi disponible: {t2}\n"
                   f"Lequel vous convient? 😊"),
        }
    else:
        _hints = {
            "ar": f"لدينا موعد متاح {_dl} الساعة {t1} ⭐\nأيهم يناسبك؟ 😊",
            "en": f"We have a slot {_dl} at {t1} ⭐\nDoes that work? 😊",
            "fr": f"Nous avons un créneau {_dl} à {t1} ⭐\nCela vous convient? 😊",
        }
    return _hints.get(lang, _hints["ar"])

def _catalog_id_for_title(title):
    """Return catalog id for an exact title match, or None."""
    if not title:
        return None
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT id FROM catalogs WHERE client_id=? AND title=? AND is_active=1",
            (CLIENT_ID, title)
        ).fetchone()
    finally:
        con.close()
    return row["id"] if row else None

def build_upsell(svc, lang):
    _lang = lang if lang in ("ar", "en", "fr") else "ar"
    # ── DB-first upsell lookup ────────────────────────────────────────────
    cat_id = _catalog_id_for_title(svc)
    if cat_id:
        upsell_item = get_upsell_for_item(CLIENT_ID, cat_id)
        if upsell_item:
            uname = upsell_item["title"]
            print(f"[UPSELL] DB suggested={uname!r} for svc={svc!r}")
            _upsell = {
                "ar": f"وإذا رغبت، يمكن إضافة {uname} بعد ذلك لنتيجة أجمل 🌟",
                "en": f"If you'd like, you can also add {uname} afterwards for an even better result 🌟",
                "fr": f"Si vous le souhaitez, vous pouvez aussi ajouter {uname} après pour un résultat encore meilleur 🌟",
            }
            return _upsell[_lang]
    # ── Hardcoded fallback ────────────────────────────────────────────────
    upsell_svc = _UPSELL_MAP.get(svc)
    if not upsell_svc:
        return ""
    uname = svc_name(upsell_svc, lang)
    print(f"[UPSELL] hardcoded suggested={upsell_svc!r} for svc={svc!r}")
    _upsell = {
        "ar": f"وإذا رغبت، يمكن إضافة {uname} بعد ذلك لنتيجة أجمل 🌟",
        "en": f"If you'd like, you can also add {uname} afterwards for an even better result 🌟",
        "fr": f"Si vous le souhaitez, vous pouvez aussi ajouter {uname} après pour un résultat encore meilleur 🌟",
    }
    return _upsell[_lang]

_UPSELL_CANONICAL_MAP = {
    "تنظيف أسنان":   "تبييض الأسنان",
    "فحص الأسنان":   "تنظيف أسنان",
    "تبييض الأسنان": "فحص الأسنان",
}

def can_show_upsell(state):
    step          = state.get("current_step", "service")
    svcs          = ensure_svc_list(state.get("known_service"))
    offered       = state.get("upsell_offered", False)
    rejected      = state.get("upsell_rejected", False)

    if offered:
        print("[UPSELL_CHECK] allowed=False (already offered)")
        return False
    if rejected:
        print("[UPSELL_CHECK] allowed=False (user rejected)")
        return False
    if step not in ("service", "day"):
        print(f"[UPSELL_CHECK] allowed=False (step={step!r} too late)")
        return False
    if not svcs:
        print("[UPSELL_CHECK] allowed=False (no service yet)")
        return False
    primary     = svcs[-1]
    upsell_svc  = _UPSELL_CANONICAL_MAP.get(primary)
    if not upsell_svc:
        print("[UPSELL_CHECK] allowed=False (no upsell mapping)")
        return False
    if upsell_svc in svcs:
        print("[UPSELL_CHECK] allowed=False (upsell already in cart)")
        return False
    print(f"[UPSELL_CHECK] allowed=True primary={primary!r} suggested={upsell_svc!r}")
    return True

_REJECTION_WORDS = {"لا", "no", "non", "لأ", "la", "nope", "ما أبي", "ما أريد", "not interested"}

def is_rejection(msg):
    return (msg or "").strip().lower() in _REJECTION_WORDS

_WA_SERVICE_ALIASES = {
    "تنظيف أسنان": [
        "تنظيف", "تنظيف أسنان",
        "teeth cleaning", "cleaning",
        "nettoyage", "nettoyage des dents",
    ],
    "تبييض الأسنان": [
        "تبييض", "تبييض الأسنان",
        "whitening", "teeth whitening",
        "blanchiment", "blanchiment des dents",
    ],
    "فحص الأسنان": [
        "فحص", "فحص الأسنان",
        "checkup", "dental checkup",
        "consultation", "contrôle", "controle", "contrôle dentaire",
    ],
}

_WA_PRICE_KEYWORDS = ["كم", "سعر", "ثمن", "تكلفة", "بكم", "السعر", "الثمن", "price", "how much", "combien", "tarif", "coût", "cout"]

_WA_GREETINGS = [
    "السلام", "سلام", "مرحبا", "مرحبً", "أهلا", "اهلا", "أهلً",
    "هلا", "هلو", "hello", "hi", "hey", "صباح الخير", "مساء الخير",
    "صباح", "مساء", "كيف حالك", "كيف الحال",
]

def is_greeting_only(msg):
    cleaned = msg.strip().lower()
    return any(cleaned.startswith(g.lower()) for g in _WA_GREETINGS) and len(cleaned) < 40

def is_greeting(msg):
    msg = (msg or "").lower().strip()
    greetings = [
        "hello", "hi", "hey",
        "bonjour", "salut",
        "السلام عليكم", "السلام", "مرحبا", "أهلا", "اهلا",
    ]
    return any(g in msg for g in greetings)

_WEAK_REPLIES = {"ok","okay","yes","no","sure","yep","nope","yeah","fine",
                 "نعم","لا","اوك","تمام","حسنا","حسناً","موافق",
                 "oui","non","merci","d'accord","daccord"}

def is_lang_switch_worthy(msg):
    cleaned = msg.strip().lower()
    if cleaned in _WEAK_REPLIES:
        return False
    if is_greeting_only(msg):
        return False
    words = cleaned.split()
    return len(words) >= 3 or len(cleaned) >= 10

def detect_wa_service(msg):
    msg_lower = msg.lower()
    for normalized_svc, aliases in _WA_SERVICE_ALIASES.items():
        for alias in aliases:
            if alias.lower() in msg_lower:
                print(f"[SERVICE_DETECT] raw={msg!r} matched_alias={alias!r} normalized={normalized_svc!r}")
                return normalized_svc
    print(f"[SERVICE_DETECT] raw={msg!r} no match")
    return None

def is_price_question(msg):
    return any(kw in msg for kw in _WA_PRICE_KEYWORDS)

def send_booking_messages(sender, state, name, lang):
    """SINGLE SEND POINT for all post-completion messages (WhatsApp Sales Engine).
    Sends exactly ONE message to the customer and ONE to the admin.
    This is the ONLY function allowed to send completion messages."""

    print(f"[TRACE] sending from send_booking_messages ONLY")
    print(f"[DEBUG_SEND_CHECK] sender={sender!r} name={name!r}")

    # ── Determine flow type from cart ────────────────────────────────────────
    _ids   = json.loads(state.get("known_catalog_ids_json") or "[]")
    items  = get_catalog_items(CLIENT_ID, _ids)
    flow   = determine_flow_type(items)
    print(f"[FLOW_TYPE] send_booking_messages flow={flow!r}")

    # ── 1. Build customer confirmation (flow-aware) ──────────────────────────
    customer_message = confirmation_message(state, name, lang, phone=None)

    # ── 2. Send to customer (sender) — ONLY the confirmation ────────────────
    print(f"[SEND_CUSTOMER] to={normalize_number(sender)!r}")
    print(f"[SEND_CUSTOMER] body={customer_message!r}")
    wa_reply(sender, customer_message)

    # ── 3. Build admin notification (flow-aware label) ───────────────────────
    _cur      = get_client(CLIENT_ID).get("currency", "SAR")
    if items:
        item_lines = "\n".join(
            f"  • {it['title']} — {int(it.get('sale_price') or it.get('price') or 0)} {_cur}"
            for it in items
        )
        total_str = f"{int(calculate_total(CLIENT_ID, _ids))} {_cur}"
    else:
        _svcs = ensure_svc_list(state.get("known_service"))
        item_lines = "\n".join(f"  • {s}" for s in _svcs) if _svcs else "  غير محدد"
        total_str = "-"

    if flow == "order":
        _admin_label = "📦 طلب جديد"
        _extra = f"العنوان: {state.get('known_address', '-')}"
    elif flow == "mixed":
        _admin_label = "🛒 طلب مختلط (خدمات + منتجات)"
        _extra = f"الموعد: {state.get('known_day', '')} {state.get('known_time', '')}".strip()
    else:
        _admin_label = "📥 حجز جديد"
        _extra = f"الموعد: {state.get('known_day', '')} {state.get('known_time', '')}".strip()

    admin_message = (
        f"{_admin_label}\n"
        f"الاسم: {name}\n"
        f"الرقم: {sender}\n"
        f"العناصر:\n{item_lines}\n"
        f"الإجمالي: {total_str}\n"
        f"{_extra}"
    ).strip()

    # ── 4. Send to admin — ONLY if admin number is different from sender ─────
    if not ADMIN_WHATSAPP_NUMBER or not ADMIN_WHATSAPP_NUMBER.strip():
        print("[SEND_ADMIN] SKIPPED — ADMIN_WHATSAPP_NUMBER not configured")
        return

    print(f"[ADMIN_RAW] {ADMIN_WHATSAPP_NUMBER!r}")
    _admin_to   = normalize_number(ADMIN_WHATSAPP_NUMBER.strip())
    _customer_n = normalize_number(sender)
    print(f"[ADMIN_FINAL] {_admin_to!r}")
    print(f"[CUSTOMER] {_customer_n!r}")

    if _admin_to == _customer_n:
        print(f"[SEND_ADMIN] SKIPPED — admin == customer ({_admin_to!r})")
        return

    # resp = ultramsg_send(_admin_to, admin_message)   # DISABLED FOR TEST
    print("[SEND_ADMIN] DISABLED FOR TEST")

_ALL_TIMES = [
    "09:00", "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00", "16:00",
    "17:00", "18:00", "19:00",
]

def get_top_times(times, limit=3):
    return times[:limit]

def normalize_time_input(msg):
    msg = msg.strip()
    mapping = {
        "الصباح": "09:00",
        "بدري":   "10:00",
        "الظهر":  "12:00",
        "العصر":  "16:00",
        "المغرب": "18:00",
        "المساء": "19:00",
        "الليل":  "20:00",
        "الساعة 9":  "09:00",
        "الساعة 10": "10:00",
        "الساعة 11": "11:00",
        "الساعة 12": "12:00",
        "الساعة 1":  "13:00",
        "الساعة 2":  "14:00",
        "الساعة 3":  "15:00",
        "الساعة 4":  "16:00",
        "الساعة 5":  "17:00",
        "الساعة 6":  "18:00",
        "الساعة 7":  "19:00",
        "9am": "09:00", "10am": "10:00", "11am": "11:00",
        "12pm": "12:00", "1pm": "13:00", "2pm": "14:00",
        "3pm": "15:00", "4pm": "16:00", "5pm": "17:00",
        "6pm": "18:00", "7pm": "19:00",
        "9h": "09:00", "10h": "10:00", "11h": "11:00",
        "12h": "12:00", "13h": "13:00", "14h": "14:00",
        "15h": "15:00", "16h": "16:00", "17h": "17:00",
        "18h": "18:00", "19h": "19:00",
    }
    for k, v in mapping.items():
        if k in msg:
            print(f"[TIME_NORMALIZE] raw={msg!r} → normalized={v!r}")
            return v
    print(f"[TIME_NORMALIZE] raw={msg!r} → normalized={msg!r}")
    return msg

def get_available_times(service, day):
    print(f"[AVAILABILITY] checking available times service={service!r} day={day!r}")
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT time FROM bookings WHERE service = ? AND time LIKE ?",
            (service, f"{day}%")
        ).fetchall()
    finally:
        con.close()
    booked    = {normalize_slot_text(row["time"]) for row in rows}
    available = [t for t in _ALL_TIMES if normalize_slot_text(f"{day} {t}") not in booked]
    print(f"[AVAILABILITY] booked={booked} result={available}")
    return available

_PRIORITY_TIMES = ["16:00", "17:00", "18:00"]

def get_time_priority(day):
    """Return _PRIORITY_TIMES sorted by booking count ascending (least booked first)."""
    day = (day or "اليوم").strip()
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT time FROM bookings WHERE time LIKE ?",
            (f"{day}%",)
        ).fetchall()
    finally:
        con.close()
    counts = {t: 0 for t in _PRIORITY_TIMES}
    for row in rows:
        slot = normalize_slot_text(row["time"])
        for pt in _PRIORITY_TIMES:
            if pt in slot:
                counts[pt] += 1
                break
    sorted_times = sorted(counts.items(), key=lambda x: x[1])
    print(f"[TIME_PRIORITY] day={day!r} counts={counts} sorted={[t for t,_ in sorted_times]}")
    return sorted_times   # list of (time, count)

def normalize_slot_text(text):
    text = (text or "").strip()
    text = text.replace("غدًا", "غدا")
    text = text.replace("الساعة ", "الساعة")
    text = text.replace("  ", " ")
    return text

def is_time_slot_taken(service, day, time_val):
    incoming_slot = normalize_slot_text(f"{day} {time_val}")
    print(f"[SLOT_CHECK] service={service!r}")
    print(f"[SLOT_CHECK] known_day={day!r}")
    print(f"[SLOT_CHECK] incoming_time={time_val!r}")
    print(f"[SLOT_CHECK] normalized_slot={incoming_slot!r}")
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT time FROM bookings WHERE service = ?",
            (service,)
        ).fetchall()
    finally:
        con.close()
    stored_slots = [normalize_slot_text(row["time"]) for row in rows]
    print(f"[SLOT_CHECK] stored_slots={stored_slots}")
    taken = incoming_slot in stored_slots
    print(f"[SLOT_CHECK] conflict={taken}")
    return taken

def wa_save_booking(phone, state, name):
    print("[SAVE_BOOKING] START")
    print(f"[SAVE_BOOKING] client_id={CLIENT_ID}")
    print(f"[SAVE_BOOKING] phone={phone}")
    print(f"[SAVE_BOOKING] name={name}")
    print(f"[SAVE_BOOKING] state={state}")

    day  = state.get("known_day")  or ""
    time = state.get("known_time") or ""

    # ── Resolve catalog items + total ─────────────────────────────────────
    _ids   = json.loads(state.get("known_catalog_ids_json") or "[]")
    _items = get_catalog_items(CLIENT_ID, _ids)
    _total = calculate_total(CLIENT_ID, _ids) if _ids else 0.0

    # Build items list — titles from catalog, fallback to known_service
    if _items:
        item_titles = [it.get("title", str(it.get("id", "?"))) for it in _items]
    else:
        item_titles = ensure_svc_list(state.get("known_service")) or []
    items_json = json.dumps(item_titles, ensure_ascii=False)

    print(f"[SAVE_BOOKING] ids={_ids} items={item_titles} total={_total} day={day!r} time={time!r}")

    # ── 1. INSERT into bookings_or_orders (primary admin table) ───────────
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO bookings_or_orders
                (client_id, phone, customer_name, items_json,
                 day, time, total_price, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?)
        """, (
            CLIENT_ID,
            phone,
            name,
            items_json,
            day,
            time,
            _total,
            datetime.datetime.now().isoformat(),
        ))
        con.commit()
        print("[SAVE_BOOKING] SUCCESS — bookings_or_orders row committed")
    except Exception as e:
        print(f"[SAVE_BOOKING] ERROR bookings_or_orders: {repr(e)}")
        import traceback as _tb; _tb.print_exc()
    finally:
        con.close()

    # ── 2. Legacy bookings table (kept for backwards compat) ──────────────
    svc_str = " / ".join(item_titles) if item_titles else "غير محدد"
    slot    = f"{day} {time}".strip()
    con2 = get_db_connection()
    try:
        con2.execute(
            "INSERT INTO bookings (user_id, name, service, time, timestamp) VALUES (?, ?, ?, ?, ?)",
            (str(WHATSAPP_USER_ID), name, svc_str, slot, datetime.datetime.now().isoformat())
        )
        con2.commit()
        print("[SAVE_BOOKING] bookings (legacy) row committed")
    except Exception as e:
        print(f"[SAVE_BOOKING] ERROR bookings (legacy): {repr(e)}")
    finally:
        con2.close()

    # ── 3. orders table (secondary, independent transaction) ──────────────
    con3 = get_db_connection()
    try:
        con3.execute(
            "INSERT INTO orders (client_id, phone, name, items, scheduled, status) VALUES (?, ?, ?, ?, ?, 'confirmed')",
            (CLIENT_ID, phone, name, items_json, slot)
        )
        con3.commit()
        print("[SAVE_BOOKING] orders row committed")
    except Exception as e:
        print(f"[SAVE_BOOKING] ERROR orders: {repr(e)}")
    finally:
        con3.close()

_SERVICE_MAP = {
    "تنظيف": "تنظيف أسنان",
    "تبييض": "تبييض أسنان",
    "فحص":   "فحص أسنان",
    "cleaning":  "teeth cleaning",
    "whitening": "teeth whitening",
    "checkup":   "dental checkup",
    "check-up":  "dental checkup",
}
_DAY_MAP = {
    "غد": "غدًا", "غدا": "غدًا", "غدًا": "غدًا", "بكرة": "غدًا",
    "today": "اليوم", "اليوم": "اليوم", "tomorrow": "غدًا",
}
_PERIOD_MAP = {
    "مساء": "مساءً", "evening": "مساءً", "afternoon": "مساءً",
    "صباح": "صباحًا", "morning": "صباحًا",
}

def extract_booking_fields(message, allowed_services=None):
    import re
    msg_lower = message.lower()

    # Service — detect keyword then validate against allowed list
    raw_service = next((val for key, val in _SERVICE_MAP.items() if key in msg_lower), None)
    service = None
    if raw_service:
        if allowed_services:
            service = next(
                (s for s in allowed_services if
                 raw_service.lower() == s.lower() or
                 raw_service.lower() in s.lower() or
                 s.lower() in raw_service.lower()),
                None
            )
        else:
            service = raw_service

    # Time — combine day + period
    detected_day    = next((v for k, v in _DAY_MAP.items()    if k in msg_lower), None)
    detected_period = next((v for k, v in _PERIOD_MAP.items() if k in msg_lower), None)
    if detected_day and detected_period:
        time = f"{detected_day} {detected_period}"
    elif detected_day:
        time = detected_day
    elif detected_period:
        time = detected_period
    else:
        time = None

    # Name — extract from explicit prefix patterns (Arabic and English)
    name = None
    name_patterns = [
        r'اسمي\s+(\S+)',
        r'باسم\s+(\S+)',
        r'أنا\s+(\S+)',
        r'انا\s+(\S+)',
        r'(?i)my name is\s+(\S+)',
        r'(?i)i am\s+(\S+)',
        r"(?i)i'm\s+(\S+)",
        r'(?i)name[:\s]+(\S+)',
    ]
    for pat in name_patterns:
        m = re.search(pat, message)
        if m:
            name = m.group(1).strip()
            break

    return {"service": service, "time": time, "name": name, "raw_service": raw_service}

@app.route("/build-id")
def build_id():
    return "BUILD_ID: REPLIT_DEPLOY_TEST_001", 200

@app.route("/whatsapp", methods=["POST"])
def whatsapp():
    print("🔥 WHATSAPP ROUTE HIT")
    try:
        data = request.get_json(force=True, silent=True) or {}
        print(f"[TRACE_PAYLOAD] {data}")          # full dump — reveals UltraMsg echo payloads
        msg_data     = data.get("data", {})
        sender       = msg_data.get("from", "").strip()
        incoming_msg = msg_data.get("body", "").strip()
        msg_type     = msg_data.get("type", "")

        # ── Harden fromMe: UltraMsg may send True, 1, "true", "1" ────────────
        _from_me_raw = msg_data.get("fromMe", False)
        from_me = _from_me_raw in (True, 1, "true", "1", "True")

        print(f"[WHATSAPP] sender={sender!r} message={incoming_msg!r} type={msg_type!r} fromMe_raw={_from_me_raw!r} fromMe={from_me!r}")

        # ── Ignore outbound messages the bot itself sent ─────────────────────
        if from_me:
            print(f"[WHATSAPP] IGNORED outbound (fromMe={_from_me_raw!r}) — body={incoming_msg!r}")
            return "", 200

        if msg_type != "chat" or not sender or not incoming_msg:
            print("[WHATSAPP] ignored non-chat or empty message")
            return "", 200

        state = wa_load(sender)
        _step_early = state.get("current_step", "service")

        print(f"[FINAL STATE] {state}")
        print(f"[COMPLETED] step={_step_early!r} is_done={_step_early == 'done'}")

        # ── COMPLETED LOCK — booking already done, offer new booking ─────────
        if state.get("completed") or _step_early == "done":
            _cl = state.get("lang") or "ar"
            _new_booking_q = {
                "ar": "هل ترغب في حجز جديد؟",
                "en": "Would you like to make a new booking?",
                "fr": "Souhaitez-vous faire une nouvelle réservation?",
            }
            print(f"[COMPLETED] completed={state.get('completed')} step={_step_early!r} — offering new booking")
            return wa_reply(sender, _new_booking_q.get(_cl, _new_booking_q["ar"]))

        if is_noise_message(incoming_msg) and _step_early != "service":
            print(f"[NOISE] ignored mid-booking greeting at step={_step_early!r}")
            return "", 200

        # ── STEP 11: CATALOG MATCH → known_catalog_ids_json ───────────────
        # Run this before the LLM parse so IDs are always up-to-date
        _early_lang = state.get("lang") or detect_lang(incoming_msg) or "ar"
        _cat_match  = find_catalog_match(CLIENT_ID, incoming_msg, lang=_early_lang)
        if _cat_match:
            print(f"[CATALOG_MATCH] {_cat_match}")
            _ids = json.loads(state.get("known_catalog_ids_json") or "[]")
            if _cat_match["id"] not in _ids:
                _ids.append(_cat_match["id"])
                state["known_catalog_ids_json"] = json.dumps(_ids)
                wa_save(sender, state)
                print(f"[CATALOG_IDS] updated ids={_ids}")

        # ── FULL INTENT EXTRACTION (multi-field, any language) ─────────────
        # Runs on every message — merges services, day, time, name into state
        # regardless of current step. Enables one-shot booking.
        _DAY_NORM = {"today": "اليوم", "tomorrow": "غدا"}
        state["known_service"] = ensure_svc_list(state.get("known_service"))
        _intent = extract_full_intent(incoming_msg)
        _intent_changed = False

        # Merge services: match each extracted phrase against catalog
        for _svc_phrase in (_intent.get("services") or []):
            _svc_match = find_catalog_match(CLIENT_ID, _svc_phrase, lang=_early_lang)
            if not _svc_match:
                # Retry with full message to handle phrase variation
                _svc_match = find_catalog_match(CLIENT_ID, incoming_msg, lang=_early_lang)
            if not _svc_match:
                # Last resort: reverse LIKE — find alias that CONTAINS any word from phrase
                _con_r = get_db_connection()
                try:
                    for _word in sorted(_svc_phrase.split(), key=len, reverse=True):
                        if len(_word) >= 3:
                            _row_r = _con_r.execute("""
                                SELECT DISTINCT c.* FROM catalogs c
                                JOIN catalog_aliases a ON a.catalog_id=c.id
                                WHERE c.client_id=? AND a.lang=? AND c.is_active=1
                                  AND a.alias LIKE ?
                                ORDER BY LENGTH(a.alias) DESC LIMIT 1
                            """, (CLIENT_ID, _early_lang, f"%{_word}%")).fetchone()
                            if _row_r:
                                _svc_match = dict(_row_r)
                                print(f"[INTENT_MERGE] reverse-LIKE matched {_word!r} → {_svc_match['title']!r}")
                                break
                finally:
                    _con_r.close()
            if _svc_match:
                _svc_title = _svc_match["title"]
                if _svc_title not in state["known_service"]:
                    state["known_service"].append(_svc_title)
                    _intent_changed = True
                    print(f"[INTENT_MERGE] service added={_svc_title!r}")
                _i_ids = json.loads(state.get("known_catalog_ids_json") or "[]")
                if _svc_match["id"] not in _i_ids:
                    _i_ids.append(_svc_match["id"])
                    state["known_catalog_ids_json"] = json.dumps(_i_ids)
                    _intent_changed = True

        # Merge day
        _i_day = _intent.get("day")
        if _i_day and _i_day in _DAY_NORM and not state.get("known_day"):
            state["known_day"] = _DAY_NORM[_i_day]
            _intent_changed = True
            print(f"[INTENT_MERGE] day={state['known_day']!r}")

        # Merge time
        _i_time = _intent.get("time")
        if _i_time and not state.get("known_time"):
            _i_time_norm = normalize_time_input(str(_i_time))
            if is_valid_time(_i_time_norm):
                state["known_time"] = _i_time_norm
                _intent_changed = True
                print(f"[INTENT_MERGE] time={_i_time_norm!r}")

        # Merge name — unconditional (no step restriction)
        _i_name = (_intent.get("name") or "").strip()
        if _i_name and not state.get("known_name") and is_valid_name(_i_name):
            state["known_name"] = _i_name
            _intent_changed = True
            print(f"[INTENT_MERGE] name={_i_name!r}")

        if _intent_changed:
            wa_save(sender, state)
        print(f"[STATE AFTER MERGE] step={state.get('current_step')!r} "
              f"services={state.get('known_service')} day={state.get('known_day')!r} "
              f"time={state.get('known_time')!r} name={state.get('known_name')!r} "
              f"ids={state.get('known_catalog_ids_json')}")

        # ── LLM PARSE ─────────────────────────────────────────────────────
        _parsed = parse_user_message(incoming_msg, lang=state.get("lang") or "ar")
        _DAY_NORM   = {"today": "اليوم", "tomorrow": "غدا"}
        _VALID_SVCS = {"teeth_cleaning", "teeth_whitening", "dental_checkup"}
        state["known_service"] = ensure_svc_list(state.get("known_service"))
        _changed = False

        _p_svc   = _parsed.get("service")
        _p_addon = _parsed.get("add_on_service")
        _p_day   = _parsed.get("day")
        _p_time  = _parsed.get("time")
        _p_name  = _parsed.get("name")
        _parsed_affirmation = bool(_parsed.get("affirmation"))
        _parsed_rejection   = bool(_parsed.get("rejection"))

        if _p_svc in _VALID_SVCS:
            _arabic_svc = _CANONICAL_SERVICE_MAP.get(_p_svc, _p_svc)
            if not state["known_service"]:
                state["known_service"] = [_arabic_svc]
                _changed = True
                print(f"[STATE_MERGE] set service={_arabic_svc!r}")

        if _p_addon in _VALID_SVCS:
            _arabic_addon = _CANONICAL_SERVICE_MAP.get(_p_addon, _p_addon)
            if _arabic_addon not in state["known_service"]:
                state["known_service"].append(_arabic_addon)
                _changed = True
                print(f"[STATE_MERGE] appended add_on={_arabic_addon!r}")

        if _p_day in ("today", "tomorrow") and not state.get("known_day"):
            state["known_day"] = _DAY_NORM[_p_day]
            _changed = True
            print(f"[STATE_MERGE] set day={state['known_day']!r}")

        if _p_time and not state.get("known_time"):
            _norm_t = normalize_time_input(_p_time)
            if is_valid_time(_norm_t):
                state["known_time"] = _norm_t
                _changed = True
                print(f"[STATE_MERGE] set time={_norm_t!r}")

        if _p_name and not state.get("known_name"):
            if is_valid_name(_p_name):
                state["known_name"] = _p_name
                _changed = True
                print(f"[STATE_MERGE] set name={_p_name!r}")

        if _changed:
            wa_save(sender, state)
            print(f"[STATE_MERGE] updated_state={state}")

        # ── REGEX + CATALOG FALLBACK (only when LLM found nothing) ───────
        _parser_found = any([_p_svc, _p_addon, _p_day, _p_time])
        if not _parser_found:
            _e_svc, _e_day, _e_time = extract_entities(incoming_msg)
            _re_changed = False
            # Catalog alias match as extra fallback for service
            if not _e_svc:
                _cat_match = find_catalog_match(CLIENT_ID, incoming_msg,
                                                lang=state.get("lang") or "ar")
                if _cat_match:
                    _e_svc = _cat_match["title"]
            if _e_svc:
                _arabic_svc = _CANONICAL_SERVICE_MAP.get(_e_svc, _e_svc)
                _cur_svcs   = state["known_service"]
                if is_add_intent(incoming_msg):
                    if _arabic_svc not in _cur_svcs:
                        _cur_svcs.append(_arabic_svc)
                        state["known_service"] = _cur_svcs
                        _re_changed = True
                        print(f"[ENTITY_EXTRACT] appended svc={_arabic_svc!r} list={_cur_svcs!r}")
                elif not _cur_svcs:
                    state["known_service"] = [_arabic_svc]
                    _re_changed = True
                    print(f"[ENTITY_EXTRACT] set svc={_arabic_svc!r}")
            if _e_day and not state.get("known_day"):
                state["known_day"] = _DAY_NORM.get(_e_day, _e_day)
                _re_changed = True
            if _e_time and not state.get("known_time"):
                state["known_time"] = _e_time
                _re_changed = True
            if _re_changed:
                wa_save(sender, state)
                print(f"[ENTITY_EXTRACT] merged day={_e_day!r} time={_e_time!r}")

        step  = state["current_step"]

        old_lang     = state.get("lang") or ""
        new_lang     = detect_lang(incoming_msg)
        print(f"[LANG_DETECT] detected={new_lang!r} stored={old_lang!r}")

        if new_lang and new_lang != old_lang:
            print(f"[LANG_SWITCH] old={old_lang!r} new={new_lang!r} sender={sender!r}")
            state["lang"] = new_lang
            wa_save(sender, state)

        lang = state.get("lang") or new_lang or "ar"
        print(f"[LANG_FINAL] using={lang!r}")
        print(f"[WHATSAPP] step={step!r} lang={lang!r}")

        print(f"[FLOW] current_step={step!r}")

        # ── GREETING — only reset if state is empty AND message is pure greeting
        if is_greeting(incoming_msg):
            _state_has_data = bool(
                ensure_svc_list(state.get("known_service")) or
                state.get("known_day") or state.get("known_time") or state.get("known_name")
            )
            _intent_has_data = bool(
                _intent.get("services") or _intent.get("day") or
                _intent.get("time")     or _intent.get("name")
            )
            if _state_has_data:
                # State already has booking progress — never reset, fall through
                print(f"[GREETING] skipping reset — state has existing data (guard)")
            elif _intent_has_data:
                # Message is a greeting + booking data — don't reset, fall through
                print(f"[GREETING] skipping reset — intent has data={_intent}")
            elif step == "service":
                print(f"[GREETING] pure greeting — resetting state for sender={sender!r}")
                wa_clear(sender)
                return wa_reply(sender, build_ask_service(CLIENT_ID, lang))
            else:
                _ask_map = {
                    "day":     "Ask the user for the appointment day (today or tomorrow only).",
                    "time":    "Ask the user for the appointment time (example: 16:00).",
                    "name":    "Ask the user for their name to complete the booking.",
                    "confirm": "Ask the user to confirm their booking (yes or no).",
                }
                _ask = _ask_map.get(step, "Ask the user what service they need.")
                print(f"[FLOW] asking_for={step!r} (after mid-booking greeting)")
                return wa_reply(sender, openai_chat(_ask, lang=lang))

        # ── UPSELL REJECTION DETECTION ────────────────────────────────────
        if state.get("upsell_offered") and not state.get("upsell_rejected") and (is_rejection(incoming_msg) or _parsed_rejection):
            state["upsell_rejected"] = True
            wa_save(sender, state)
            print(f"[UPSELL_REJECTED] sender={sender!r}")
            _ask_map = {
                "day":     "Ask the user for the appointment day (today or tomorrow only).",
                "time":    "Ask the user for the appointment time (example: 16:00).",
                "name":    "Ask the user for their name to complete the booking.",
                "confirm": "Ask the user to confirm their booking (yes or no).",
            }
            _ask = _ask_map.get(step, "Ask the user what service they need.")
            return wa_reply(sender, openai_chat(_ask, lang=lang))

        # ── ONE-SHOT SHORTCUT — all fields present → go directly to confirmation
        _sc_svcs = ensure_svc_list(state.get("known_service"))
        _sc_day  = state.get("known_day")
        _sc_time = state.get("known_time")
        _sc_name = state.get("known_name")
        # Resolve current cart items + flow type for shortcut decision
        _sc_ids_pre  = json.loads(state.get("known_catalog_ids_json") or "[]")
        _sc_items    = get_catalog_items(CLIENT_ID, _sc_ids_pre) if _sc_ids_pre else []
        _sc_flow     = determine_flow_type(_sc_items)
        print(f"[FLOW_TYPE] shortcut check flow={_sc_flow!r}")
        # Services/mixed require day+time; products require only name
        _sc_needs_appt = _sc_flow in ("booking", "mixed")
        _sc_ready = bool(
            _sc_svcs and _sc_name and
            (not _sc_needs_appt or (_sc_day and _sc_time))
        )
        if _sc_ready:
            print(f"[SHORTCUT] all required fields complete — skipping step flow")
            print(f"[SHORTCUT] flow={_sc_flow!r} services={_sc_svcs} day={_sc_day!r} time={_sc_time!r} name={_sc_name!r}")
            # Ensure catalog IDs are merged for all items in cart
            _sc_ids = json.loads(state.get("known_catalog_ids_json") or "[]")
            for _sv in _sc_svcs:
                _sv_id = _catalog_id_for_title(_sv)
                if _sv_id and _sv_id not in _sc_ids:
                    _sc_ids.append(_sv_id)
            state["known_catalog_ids_json"] = json.dumps(_sc_ids)
            state["current_step"] = "done"
            state["completed"]    = True
            wa_save_booking(sender, state, _sc_name)
            wa_save(sender, state)
            print(f"[STATE_COMPLETED] True")
            print(f"[FINAL STATE] {state}")
            send_booking_messages(sender, state, _sc_name, lang)
            return "", 200

        # ── STEP: service ─────────────────────────────────────────────────
        if step == "service":
            # Prefer catalog match from STEP 11 over hardcoded detect_wa_service
            _cat_item = _cat_match  # set by STEP 11 block above (or None)
            svc = _cat_item["title"] if _cat_item else detect_wa_service(incoming_msg)

            if svc:
                _cur_svcs = ensure_svc_list(state.get("known_service"))
                if is_add_intent(incoming_msg):
                    if svc not in _cur_svcs:
                        _cur_svcs.append(svc)
                else:
                    _cur_svcs = [svc]
                state["known_service"] = _cur_svcs
                print(f"[ENTITY_EXTRACT] service list={_cur_svcs!r} (from_catalog={bool(_cat_item)})")

                # ── Merge catalog IDs from all svcs in cart ────────────────
                _ids_set = json.loads(state.get("known_catalog_ids_json") or "[]")
                for _sv in _cur_svcs:
                    _sv_id = _catalog_id_for_title(_sv)
                    if _sv_id and _sv_id not in _ids_set:
                        _ids_set.append(_sv_id)
                state["known_catalog_ids_json"] = json.dumps(_ids_set)

                # ── Required Fields Engine ────────────────────────────────
                _svc_items_now = get_catalog_items(CLIENT_ID, _ids_set)
                _svc_flow      = determine_flow_type(_svc_items_now)
                _req_fields    = get_required_fields(_svc_flow, _svc_items_now)
                _miss_fields   = get_missing_fields(state, _req_fields)
                _next_field    = _miss_fields[0] if _miss_fields else None
                _next_step     = _FIELD_TO_STEP.get(_next_field, "name") if _next_field else "done"
                state["current_step"] = _next_step
                print(f"[FLOW_TYPE]       {_svc_flow!r}")
                print(f"[REQUIRED_FIELDS] {_req_fields}")
                print(f"[MISSING_FIELDS]  {_miss_fields}")
                print(f"[ASKING_FOR]      {_next_field!r} → step={_next_step!r}")
                wa_save(sender, state)

                # ── Build item-confirmed reply using catalog data ──────────
                _primary = _cur_svcs[-1]
                _cur     = get_client(CLIENT_ID).get("currency", "SAR")
                if _cat_item:
                    _p_raw   = _cat_item.get("sale_price") or _cat_item.get("price") or 0
                    _price   = f"{int(_p_raw)} {_cur}"
                    _benefit = _cat_item.get("description") or svc_benefit(_primary, lang)
                else:
                    _price   = svc_price(_primary, lang)
                    _benefit = svc_benefit(_primary, lang)

                # Multi-item: show all items + total when cart has >1 item
                if len(_cur_svcs) > 1:
                    _all_items = get_catalog_items(CLIENT_ID, _ids_set)
                    if _all_items:
                        _list_lines = "\n".join(
                            f"• {it['title']} — {int(it.get('sale_price') or it.get('price') or 0)} {_cur}"
                            for it in _all_items
                        )
                        _total = calculate_total(CLIENT_ID, _ids_set)
                        _cart_hdrs = {
                            "ar": f"تم إضافة {svc} ✨\nسلة طلباتك:\n{_list_lines}\n\nالإجمالي: {int(_total)} {_cur}",
                            "en": f"Added {svc} ✨\nYour cart:\n{_list_lines}\n\nTotal: {int(_total)} {_cur}",
                            "fr": f"{svc} ajouté ✨\nVotre panier:\n{_list_lines}\n\nTotal: {int(_total)} {_cur}",
                        }
                        reply = _cart_hdrs[lang if lang in ("ar","en","fr") else "ar"]
                    else:
                        reply = t("service_confirmed", lang).format(
                            svc=format_svcs(_cur_svcs, lang),
                            price=_price,
                            benefit=_benefit,
                        )
                else:
                    reply = t("service_confirmed", lang).format(
                        svc=svc,
                        price=_price,
                        benefit=_benefit,
                    )

                # Append times hint only when service/mixed flow needs day next
                if _next_field == "known_day" and _svc_flow in ("booking", "mixed"):
                    reply += "\n" + build_times_hint(_primary, lang, day=state.get("known_day"))

                # ── Upsell from catalog (DB-first, no hardcoded map) ───────
                if can_show_upsell(state):
                    _pid     = _cat_item["id"] if _cat_item else _catalog_id_for_title(_primary)
                    _upsell_item = get_upsell_for_item(CLIENT_ID, _pid) if _pid else None
                    if _upsell_item:
                        _uname    = _upsell_item["title"]
                        _up_price = _upsell_item.get("sale_price") or _upsell_item.get("price") or 0
                        _up_hdrs  = {
                            "ar": f"وإذا رغبت، يمكن إضافة {_uname} ({int(_up_price)} {_cur}) لنتيجة أجمل 🌟",
                            "en": f"If you'd like, you can add {_uname} ({int(_up_price)} {_cur}) for an even better result 🌟",
                            "fr": f"Si vous le souhaitez, ajoutez {_uname} ({int(_up_price)} {_cur}) pour un résultat encore meilleur 🌟",
                        }
                        upsell = _up_hdrs[lang if lang in ("ar","en","fr") else "ar"]
                        reply += "\n" + upsell
                        state["upsell_offered"] = True
                        wa_save(sender, state)
                        print(f"[UPSELL_OFFER] catalog source={_pid} target={_upsell_item['id']} ({_uname!r})")

            elif is_price_question(incoming_msg):
                reply = build_price_list(CLIENT_ID, lang)

            elif is_recommendation_request(incoming_msg):
                # Recommend first active catalog item dynamically
                _rec_con = get_db_connection()
                try:
                    _rec_ids = [r["id"] for r in _rec_con.execute(
                        "SELECT id FROM catalogs WHERE client_id=? AND is_active=1 ORDER BY id LIMIT 1",
                        (CLIENT_ID,)
                    ).fetchall()]
                finally:
                    _rec_con.close()
                _all_cat = get_catalog_items(CLIENT_ID, _rec_ids)
                if _all_cat:
                    _rec     = _all_cat[0]
                    _cur     = get_client(CLIENT_ID).get("currency", "SAR")
                    _rp      = _rec.get("sale_price") or _rec.get("price") or 0
                    _rec_lang = lang if lang in ("ar", "en", "fr") else "ar"
                    reply = _RECOMMENDATION[_rec_lang].format(
                        svc=_rec["title"],
                        benefit=_rec.get("description") or svc_benefit(_rec["title"], lang),
                        price=f"{int(_rp)} {_cur}",
                    )
                else:
                    reply = openai_chat(incoming_msg, lang=lang)

            else:
                reply = openai_chat(incoming_msg, lang=lang)

        # ── STEP: day ─────────────────────────────────────────────────────
        elif step == "day":
            if not is_valid_day(incoming_msg):
                print(f"[DAY_INVALID] rejected={incoming_msg!r}")
                return wa_reply(sender, openai_chat(
                    "Ask the user to choose a valid day like today or tomorrow only.",
                    lang=lang,
                ))
            svc = detect_wa_service(incoming_msg)
            if svc and not ensure_svc_list(state.get("known_service")):
                state["known_service"] = [svc]
            state["known_day"] = incoming_msg.strip()
            if state.get("known_time"):
                state["current_step"] = "name"
                print(f"[FLOW] asking_for='name' (time already known)")
                wa_save(sender, state)
                return wa_reply(sender, openai_chat(
                    "Ask the user for their name to complete the booking.",
                    lang=lang,
                ))
            else:
                state["current_step"] = "time"
                print(f"[FLOW] asking_for='time'")
                wa_save(sender, state)
                return wa_reply(sender, openai_chat(
                    "Ask the user for the exact time.",
                    lang=lang,
                ))

        # ── STEP: time ────────────────────────────────────────────────────
        elif step == "time":
            if is_affirmation(incoming_msg) or _parsed_affirmation:
                svc_tmp = state.get("known_service") or ""
                day_tmp = state.get("known_day") or ""
                avail   = get_available_times(svc_tmp, day_tmp)
                top     = get_top_times(avail, 2)
                if top:
                    slots_str = " / ".join(top)
                    return wa_reply(sender, openai_chat(
                        f"Ask the user to choose one of these available times: {slots_str}",
                        lang=lang,
                    ))
                else:
                    return wa_reply(sender, openai_chat(
                        "Ask the user for the exact time.",
                        lang=lang,
                    ))
            else:
                time_val = normalize_time_input(incoming_msg)
                if not is_valid_time(time_val):
                    print(f"[TIME_INVALID] rejected={incoming_msg!r} normalized={time_val!r}")
                    return wa_reply(sender, openai_chat(
                        "Ask the user to provide a valid time (example: 16:00).",
                        lang=lang,
                    ))
                _svcs_t = ensure_svc_list(state.get("known_service"))
                svc = _svcs_t[0] if _svcs_t else ""
                day = state.get("known_day") or ""
                # Resolve flow type — slot check applies ONLY to service bookings
                _t_ids   = json.loads(state.get("known_catalog_ids_json") or "[]")
                _t_items = get_catalog_items(CLIENT_ID, _t_ids) if _t_ids else []
                _t_flow  = determine_flow_type(_t_items)
                print(f"[FLOW_TYPE] time-step slot check flow={_t_flow!r}")
                if _t_flow != "order" and is_time_slot_taken(svc, day, time_val):
                    available = get_available_times(svc, day)
                    print(f"[SMART_SUGGEST] full={available}")
                    top = get_top_times(available)
                    print(f"[SMART_SUGGEST] top={top}")
                    if top:
                        slots = "\n".join(f"- {slot}" for slot in top)
                        reply = t("slot_taken_header", lang) + slots + t("slot_taken_footer", lang)
                    else:
                        reply = t("no_slots", lang)
                else:
                    state["known_time"]   = time_val
                    state["current_step"] = "name"
                    wa_save(sender, state)
                    reply = t("ask_name", lang)

        # ── STEP: name → confirm + save ───────────────────────────────────
        elif step == "name":
            name = (_p_name or (incoming_msg or "")).strip()
            print(f"[DEBUG] validating name={name!r} (parser={_p_name!r})")

            if not is_valid_name(name):
                print("[DEBUG] invalid name detected — rejecting")
                return wa_reply(sender, openai_chat(
                    "Ask the user politely to provide their name only (one or two words). Do not accept sentences.",
                    lang=lang,
                ))

            print("[DEBUG] name accepted — saving booking")

            state["current_step"] = "done"
            state["completed"]    = True
            wa_save_booking(sender, state, name)
            wa_save(sender, state)
            print(f"[STATE_COMPLETED] True")
            print(f"[FINAL STATE] {state}")
            send_booking_messages(sender, state, name, lang)
            return "", 200

        else:
            _reprompts = {
                "day":     "Ask the user for the appointment day (today or tomorrow only).",
                "time":    "Ask the user for the appointment time (example: 16:00).",
                "name":    "Ask the user for their name to complete the booking.",
                "confirm": "Ask the user to confirm their booking (yes or no).",
            }
            _prompt = _reprompts.get(step, "Ask the user what service they need.")
            reply = openai_chat(_prompt, lang=lang)

        print(f"[WHATSAPP] reply={reply!r}")
        return wa_reply(sender, reply)

    except Exception as e:
        import traceback
        print(f"[WHATSAPP] EXCEPTION: {repr(e)}")
        print(traceback.format_exc())
        return "", 200

def _admin_guard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return None

# ── /admin/dashboard ──────────────────────────────────────────────────────────
@app.route("/admin/dashboard")
def admin_dashboard():
    guard = _admin_guard()
    if guard:
        return guard
    client = get_client(CLIENT_ID)
    con = get_db_connection()
    try:
        total_orders  = con.execute("SELECT COUNT(*) FROM orders WHERE client_id=?", (CLIENT_ID,)).fetchone()[0]
        today_str     = datetime.datetime.now().strftime("%Y-%m-%d")
        today_orders  = con.execute(
            "SELECT COUNT(*) FROM orders WHERE client_id=? AND created_at LIKE ?",
            (CLIENT_ID, today_str + "%")
        ).fetchone()[0]
        catalog_count = con.execute(
            "SELECT COUNT(*) FROM catalogs WHERE client_id=? AND is_active=1", (CLIENT_ID,)
        ).fetchone()[0]
        active_convos = con.execute(
            "SELECT COUNT(*) FROM whatsapp_state WHERE current_step != 'service'"
        ).fetchone()[0]
        recent_orders = [dict(r) for r in con.execute(
            "SELECT * FROM orders WHERE client_id=? ORDER BY id DESC LIMIT 10", (CLIENT_ID,)
        ).fetchall()]
    finally:
        con.close()
    stats = dict(total_orders=total_orders, today_orders=today_orders,
                 catalog_count=catalog_count, active_convos=active_convos)
    return render_template("admin/dashboard.html", client=client, stats=stats,
                           recent_orders=recent_orders, active="dashboard")

# ── /admin/catalog ────────────────────────────────────────────────────────────
@app.route("/admin/catalog")
def admin_catalog():
    guard = _admin_guard()
    if guard:
        return guard
    client = get_client(CLIENT_ID)
    con = get_db_connection()
    try:
        items = [dict(r) for r in con.execute(
            "SELECT * FROM catalogs WHERE client_id=? ORDER BY id ASC", (CLIENT_ID,)
        ).fetchall()]
    finally:
        con.close()
    return render_template("admin/catalog.html", items=items,
                           currency=client.get("currency", "MAD"), active="catalog")

# ── /admin/catalog/new ────────────────────────────────────────────────────────
@app.route("/admin/catalog/new", methods=["GET", "POST"])
def admin_catalog_new():
    guard = _admin_guard()
    if guard:
        return guard
    client = get_client(CLIENT_ID)
    if request.method == "POST":
        title       = request.form.get("title", "").strip()
        typ         = request.form.get("type", "service")
        price       = float(request.form.get("price") or 0)
        sale_price  = request.form.get("sale_price") or None
        description = request.form.get("description", "").strip()
        duration    = request.form.get("duration_min") or None
        stock       = request.form.get("stock_qty") or None
        is_active   = int(request.form.get("is_active", 1))
        aliases_raw = request.form.get("aliases", "")
        if not title:
            flash("Title is required.", "error")
        else:
            con = get_db_connection()
            try:
                cur = con.execute("""
                    INSERT INTO catalogs (client_id,title,type,price,sale_price,
                        description,duration_min,stock_qty,is_active)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (CLIENT_ID, title, typ, price,
                      float(sale_price) if sale_price else None,
                      description, int(duration) if duration else None,
                      int(stock) if stock else None, is_active))
                cat_id = cur.lastrowid
                for alias in [a.strip() for a in aliases_raw.split(",") if a.strip()]:
                    con.execute(
                        "INSERT INTO catalog_aliases (catalog_id, alias, lang) VALUES (?,?,?)",
                        (cat_id, alias.lower(), "ar")
                    )
                con.commit()
            finally:
                con.close()
            flash("Catalog item created.", "success")
            return redirect(url_for("admin_catalog"))
    return render_template("admin/catalog_form.html", item=None, aliases_str="",
                           currency=client.get("currency", "MAD"), active="catalog")

# ── /admin/catalog/<id>/edit ──────────────────────────────────────────────────
@app.route("/admin/catalog/<int:cat_id>/edit", methods=["GET", "POST"])
def admin_catalog_edit(cat_id):
    guard = _admin_guard()
    if guard:
        return guard
    client = get_client(CLIENT_ID)
    con = get_db_connection()
    try:
        item_row = con.execute(
            "SELECT * FROM catalogs WHERE id=? AND client_id=?", (cat_id, CLIENT_ID)
        ).fetchone()
        if not item_row:
            flash("Item not found.", "error")
            return redirect(url_for("admin_catalog"))
        item = dict(item_row)
        aliases_list = [r["alias"] for r in con.execute(
            "SELECT alias FROM catalog_aliases WHERE catalog_id=?", (cat_id,)
        ).fetchall()]
    finally:
        con.close()
    aliases_str = ", ".join(aliases_list)
    if request.method == "POST":
        title       = request.form.get("title", "").strip()
        typ         = request.form.get("type", "service")
        price       = float(request.form.get("price") or 0)
        sale_price  = request.form.get("sale_price") or None
        description = request.form.get("description", "").strip()
        duration    = request.form.get("duration_min") or None
        stock       = request.form.get("stock_qty") or None
        is_active   = int(request.form.get("is_active", 1))
        aliases_raw = request.form.get("aliases", "")
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE catalogs SET title=?,type=?,price=?,sale_price=?,
                    description=?,duration_min=?,stock_qty=?,is_active=?
                WHERE id=? AND client_id=?
            """, (title, typ, price,
                  float(sale_price) if sale_price else None,
                  description, int(duration) if duration else None,
                  int(stock) if stock else None, is_active, cat_id, CLIENT_ID))
            con.execute("DELETE FROM catalog_aliases WHERE catalog_id=?", (cat_id,))
            for alias in [a.strip() for a in aliases_raw.split(",") if a.strip()]:
                con.execute(
                    "INSERT INTO catalog_aliases (catalog_id, alias, lang) VALUES (?,?,?)",
                    (cat_id, alias.lower(), "ar")
                )
            con.commit()
        finally:
            con.close()
        flash("Changes saved.", "success")
        return redirect(url_for("admin_catalog"))
    return render_template("admin/catalog_form.html", item=item, aliases_str=aliases_str,
                           currency=client.get("currency", "MAD"), active="catalog")

# ── /admin/catalog/<id>/delete ────────────────────────────────────────────────
@app.route("/admin/catalog/<int:cat_id>/delete", methods=["POST"])
def admin_catalog_delete(cat_id):
    guard = _admin_guard()
    if guard:
        return guard
    con = get_db_connection()
    try:
        con.execute("DELETE FROM catalog_aliases WHERE catalog_id=?", (cat_id,))
        con.execute("DELETE FROM upsells WHERE trigger_item_id=? OR upsell_item_id=?",
                    (cat_id, cat_id))
        con.execute("DELETE FROM catalogs WHERE id=? AND client_id=?", (cat_id, CLIENT_ID))
        con.commit()
    finally:
        con.close()
    flash("Item deleted.", "success")
    return redirect(url_for("admin_catalog"))

# ── /admin/orders ─────────────────────────────────────────────────────────────
@app.route("/admin/orders")
def admin_orders():
    guard = _admin_guard()
    if guard:
        return guard
    con = get_db_connection()
    try:
        raw = con.execute(
            "SELECT * FROM bookings_or_orders WHERE client_id=? ORDER BY id DESC",
            (CLIENT_ID,)
        ).fetchall()
        rows = [{k: r[k] for k in r.keys()} for r in raw]
    finally:
        con.close()

    for row in rows:
        # ── parse stored title list ──────────────────────────────────────
        try:
            titles = json.loads(row.get("items_json") or "[]")
        except Exception:
            titles = []
        row["items_parsed"] = titles

        # ── resolve catalog rows by title → get type + price ────────────
        catalog_items = []
        if titles:
            cat_con = get_db_connection()
            try:
                for title in titles:
                    r = cat_con.execute(
                        "SELECT * FROM catalogs WHERE title=? AND client_id=? LIMIT 1",
                        (title, CLIENT_ID)
                    ).fetchone()
                    if r:
                        catalog_items.append({k: r[k] for k in r.keys()})
                    else:
                        catalog_items.append({"title": title, "type": "service",
                                              "price": 0, "sale_price": None})
            finally:
                cat_con.close()
        else:
            catalog_items = []

        # ── flow type ────────────────────────────────────────────────────
        row["flow_type"] = determine_flow_type(catalog_items)
        print(f"[ADMIN_RENDER_ITEMS] id={row['id']} titles={titles} flow={row['flow_type']}")

        # ── rich item list for template: [{title, price, currency}] ─────
        row["items_rich"] = [
            {
                "title":    it.get("title", "?"),
                "price":    float(it.get("sale_price") or it.get("price") or 0),
                "currency": "MAD",
            }
            for it in catalog_items
        ]

        # ── total — use stored value (calculated at save time) ───────────
        row["total_display"] = float(row.get("total_price") or 0)
        print(f"[ADMIN_RENDER_TOTAL] id={row['id']} total={row['total_display']}")

    return render_template("admin/orders.html", orders=rows, active="orders")

# ── /admin/orders/<id>/status ──────────────────────────────────────────────────
@app.route("/admin/orders/<int:order_id>/status", methods=["POST"])
def admin_order_status(order_id):
    guard = _admin_guard()
    if guard:
        return guard
    ALLOWED = {"new", "confirmed", "done", "cancelled"}
    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ALLOWED:
        flash(f"Invalid status: {new_status!r}", "error")
        return redirect(url_for("admin_orders"))
    con = get_db_connection()
    try:
        con.execute(
            "UPDATE bookings_or_orders SET status=? WHERE id=? AND client_id=?",
            (new_status, order_id, CLIENT_ID)
        )
        con.commit()
    finally:
        con.close()
    flash(f"Order #{order_id} marked as {new_status}.", "success")
    return redirect(url_for("admin_orders"))

# ── /admin/settings ───────────────────────────────────────────────────────────
@app.route("/admin/settings", methods=["GET", "POST"])
def admin_settings():
    guard = _admin_guard()
    if guard:
        return guard
    client = get_client(CLIENT_ID)
    if request.method == "POST":
        name             = request.form.get("name", "").strip()
        business_type    = request.form.get("business_type", "clinic")
        default_language = request.form.get("default_language", "ar")
        currency         = request.form.get("currency", "MAD").strip()
        timezone         = request.form.get("timezone", "Africa/Casablanca").strip()
        admin_whatsapp   = request.form.get("admin_whatsapp", "").strip()
        ultramsg_inst    = request.form.get("ultramsg_instance", "").strip()
        ultramsg_tok     = request.form.get("ultramsg_token", "").strip()
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients SET name=?,business_type=?,default_language=?,
                    currency=?,timezone=?,admin_whatsapp=?,
                    ultramsg_instance=?,ultramsg_token=?
                WHERE id=?
            """, (name, business_type, default_language, currency, timezone,
                  admin_whatsapp, ultramsg_inst or None, ultramsg_tok or None, CLIENT_ID))
            con.commit()
        finally:
            con.close()
        flash("Settings saved.", "success")
        return redirect(url_for("admin_settings"))
    return render_template("admin/settings.html", client=client, active="settings")

# ── /admin/bookings ──────────────────────────────────────────────────────────── (legacy)
@app.route("/admin/bookings")
def admin_bookings():
    con = get_db_connection()
    try:
        rows = con.execute("SELECT * FROM bookings ORDER BY id DESC").fetchall()
    finally:
        con.close()
    html = """
<html>
<head>
    <title>Bookings Dashboard</title>
    <style>
        body { font-family: Arial; padding: 20px; background:#f5f5f5; }
        h2 { margin-bottom:20px; }
        table { border-collapse: collapse; width: 100%; background:white; }
        th, td { border: 1px solid #ddd; padding: 10px; text-align: center; }
        th { background: #333; color: white; }
        tr:nth-child(even) { background:#f9f9f9; }
    </style>
</head>
<body>
    <h2>&#128197; Bookings Dashboard</h2>
    <table>
        <tr>
            <th>ID</th>
            <th>User ID</th>
            <th>Name</th>
            <th>Service</th>
            <th>Time</th>
            <th>Timestamp</th>
        </tr>
"""
    for r in rows:
        html += f"""
        <tr>
            <td>{r['id']}</td>
            <td>{r['user_id']}</td>
            <td>{r['name']}</td>
            <td>{r['service']}</td>
            <td>{r['time']}</td>
            <td>{r['timestamp']}</td>
        </tr>
"""
    html += """
    </table>
</body>
</html>
"""
    return html

@app.route("/whatsapp-test", methods=["GET"])
def whatsapp_test():
    return "WHATSAPP TEST ROUTE LIVE", 200, {"Content-Type": "text/plain"}

@app.route("/register", methods=["GET", "POST"])
def register():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        business_name = request.form.get("business_name", "").strip()
        raw_services = request.form.get("services", "")
        services_str = ",".join(s.strip() for s in raw_services.split(",") if s.strip())
        default_language = request.form.get("default_language", "ar").strip()
        if not username or not password:
            error = "Username and password are required."
        else:
            con = get_db_connection()
            try:
                existing = con.execute(
                    "SELECT id FROM users WHERE username = ?", (username,)
                ).fetchone()
                if existing:
                    error = "Username already exists."
                else:
                    cur = con.execute(
                        "INSERT INTO users (username, password) VALUES (?, ?)",
                        (username, generate_password_hash(password))
                    )
                    new_id = cur.lastrowid
                    con.execute(
                        "INSERT INTO business_settings (user_id, business_name, services, default_language) VALUES (?, ?, ?, ?)",
                        (new_id, business_name, services_str, default_language)
                    )
                    con.commit()
            finally:
                con.close()
            if not error:
                return redirect(url_for("login"))
    return render_template("register.html", error=error)

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        con = get_db_connection()
        try:
            row = con.execute(
                "SELECT id, password FROM users WHERE username = ?", (username,)
            ).fetchone()
        finally:
            con.close()
        if row and check_password_hash(row["password"], password):
            session["logged_in"] = True
            session["user_id"] = row["id"]
            return redirect(url_for("admin_dashboard"))
        error = "Invalid username or password."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/settings", methods=["GET", "POST"])
def settings():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    user_id = session.get("user_id")
    message = None
    if request.method == "POST":
        business_name = request.form.get("business_name", "").strip()
        raw_services = request.form.get("services", "")
        services_str = ",".join(s.strip() for s in raw_services.split(",") if s.strip())
        default_language = request.form.get("default_language", "ar").strip()
        con = get_db_connection()
        try:
            con.execute(
                "INSERT OR REPLACE INTO business_settings (user_id, business_name, services, default_language) VALUES (?, ?, ?, ?)",
                (user_id, business_name, services_str, default_language)
            )
            con.commit()
        finally:
            con.close()
        message = "Settings saved."
    biz = get_biz(user_id)
    return render_template("settings.html", biz=biz, message=message)

@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    user_id = str(session.get("user_id", ""))
    con = get_db_connection()
    try:
        rows = [dict(row) for row in con.execute(
            "SELECT user_id, name, service, time, timestamp FROM bookings WHERE user_id = ? ORDER BY id DESC",
            (user_id,)
        ).fetchall()]
    finally:
        con.close()
    return render_template("dashboard.html", rows=rows)

def confirm_booking(name, service, time, reply):
    booking = {"service": service, "time": time, "name": name}
    bookings.append(booking)
    print(f"[BOOKING CONFIRMED] {booking}")
    con = get_db_connection()
    try:
        con.execute(
            "INSERT INTO bookings (user_id, name, service, time, timestamp) VALUES (?, ?, ?, ?, ?)",
            (
                str(session.get("user_id", "")),
                name,
                service,
                time,
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        con.commit()
    finally:
        con.close()
    session.pop("known_service", None)
    session.pop("known_time", None)
    session.pop("known_name", None)
    session.pop("awaiting_name", None)
    return jsonify({"reply": reply, "booking_confirmed": True, "booking": booking})


@app.route("/chat", methods=["POST"])
def chat():
    user_message = request.json.get("message")
    msg_lower = user_message.strip().lower()

    # Load business settings
    _biz = get_biz(session.get("user_id"))
    _allowed_services = _biz.get("services", [])
    biz_name = _biz.get("business_name", "")
    biz_language = _biz.get("default_language", "ar")

    def validate_service(service):
        if not _allowed_services:
            return True
        return any(
            service.lower() == s.lower() or
            service.lower() in s.lower() or
            s.lower() in service.lower()
            for s in _allowed_services
        )

    def unavailable_service_reply():
        names = "، ".join(_allowed_services)
        return jsonify({"reply": f"عذراً، هذه الخدمة غير متاحة. الخدمات المتاحة هي: {names}. أيها تفضل؟"})

    # Step 1: Greeting
    if msg_lower in ["سلام", "مرحبا", "اهلا", "hello", "hi"]:
        session.clear()
        return jsonify({"reply": "أهلاً 👋 كيف أقدر أساعدك اليوم؟"})

    # Step 2: Load session state
    known_service = session.get("known_service")
    known_time = session.get("known_time")
    awaiting_name = session.get("awaiting_name", False)

    # Step 3: awaiting_name → capture name and confirm immediately
    if awaiting_name:
        name = user_message.strip()
        session["awaiting_name"] = False
        if known_service and known_time:
            return confirm_booking(
                name, known_service, known_time,
                f"تم تأكيد حجزك بنجاح ✅\nالخدمة: {known_service}\nالموعد: {known_time}\nالاسم: {name}"
            )
        return jsonify({"reply": "حدث خطأ، حاول مرة أخرى."})

    # Step 4: Detect service (if not already known)
    SERVICE_MAP = {
        "تنظيف": "تنظيف أسنان",
        "تبييض": "تبييض أسنان",
        "فحص": "فحص أسنان",
        "cleaning": "teeth cleaning",
        "whitening": "teeth whitening",
        "checkup": "dental checkup",
        "check-up": "dental checkup",
    }
    if not known_service:
        for key, val in SERVICE_MAP.items():
            if key in msg_lower:
                if validate_service(val):
                    known_service = val
                else:
                    return unavailable_service_reply()
                break

    # Step 5: Detect time (if not already known)
    DAY_MAP = {
        "غد": "غدًا", "غدا": "غدًا", "غدًا": "غدًا", "بكرة": "غدًا",
        "today": "اليوم", "اليوم": "اليوم", "tomorrow": "غدًا",
    }
    PERIOD_MAP = {
        "مساء": "مساءً", "evening": "مساءً", "afternoon": "مساءً",
        "صباح": "صباحًا", "morning": "صباحًا",
    }
    if not known_time:
        detected_day = next((DAY_MAP[k] for k in DAY_MAP if k in msg_lower), None)
        detected_period = next((PERIOD_MAP[k] for k in PERIOD_MAP if k in msg_lower), None)
        if detected_day and detected_period:
            known_time = f"{detected_day} {detected_period}"
        elif detected_day:
            known_time = detected_day
        elif detected_period:
            known_time = detected_period

    # Step 6: Save session
    session["known_service"] = known_service
    session["known_time"] = known_time

    # Step 7: Ask for next missing field
    booking_intent = any(w in msg_lower for w in ["حجز", "موعد", "book", "appointment"])

    if known_service and known_time:
        session["awaiting_name"] = True
        return jsonify({"reply": f"رائع! ما الاسم الذي تريد تأكيد الحجز باسمه؟"})

    if known_service and not known_time:
        return jsonify({"reply": f"متى تفضل موعد {known_service}؟"})

    if booking_intent and not known_service:
        return jsonify({"reply": "ما نوع الخدمة التي تريد حجزها؟"})

    # Step 8: OpenAI fallback (non-booking messages only)
    biz_str = ""
    if biz_name:
        biz_str += "\n\nBUSINESS CONTEXT:\n"
        biz_str += f"- Business name: {biz_name}\n"
        biz_str += "- Use this name naturally in greetings.\n"
    if _allowed_services:
        biz_str += f"- Available services: {', '.join(_allowed_services)}\n"
    if biz_language:
        biz_str += f"- Default language: {'Arabic' if biz_language == 'ar' else biz_language}.\n"

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are Filtrex, a friendly assistant. "
                        "Answer the user's question naturally and helpfully. "
                        "Do not attempt to confirm or save a booking — that is handled separately. "
                        "Do not mention AI. Reply in the same language as the user."
                        + biz_str
                    )
                },
                {"role": "user", "content": user_message}
            ]
        }
    )

    reply = response.json()["choices"][0]["message"]["content"]
    return jsonify({"reply": reply})

if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=5000, debug=debug)
