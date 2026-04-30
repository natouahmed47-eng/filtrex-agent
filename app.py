from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash, g
from werkzeug.security import generate_password_hash, check_password_hash 
import requests
import os
import json
import sqlite3
import datetime
import random

# Meta WhatsApp Cloud API configuration
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "filtrex_verify_123")

print(f"[STARTUP] META_ACCESS_TOKEN={'set' if META_ACCESS_TOKEN else 'not set'}")
print(f"[STARTUP] META_PHONE_NUMBER_ID={META_PHONE_NUMBER_ID!r}")
print(f"[STARTUP] VERIFY_TOKEN={'set' if VERIFY_TOKEN else 'not set'}")

DEFAULT_CLIENT_ID = int(os.getenv("DEFAULT_CLIENT_ID", "1"))
print(f"[STARTUP] DEFAULT_CLIENT_ID={DEFAULT_CLIENT_ID}")

def meta_send_message(to_phone, message_text):
    """Send a WhatsApp message using Meta Cloud API.
    
    Args:
        to_phone: Recipient phone number (with country code, e.g., 1234567890)
        message_text: Message text to send
    
    Returns:
        requests.Response object or None on error
    """
    if not META_ACCESS_TOKEN or not META_PHONE_NUMBER_ID:
        print("[META_SEND_ERROR] Missing META_ACCESS_TOKEN or META_PHONE_NUMBER_ID")
        return None
    
    url = f"https://graph.facebook.com/v25.0/{META_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_phone,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message_text,
        },
    }
    
    print(f"[META_SEND] to={to_phone!r} body={message_text!r}")
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        print(f"[META_SEND] status={resp.status_code} response={resp.text!r}")
        return resp
    except Exception as e:
        print(f"[META_SEND_ERROR] {repr(e)}")
        return None


def normalize_phone_number(raw_phone):
    """Extract and normalize phone number to digits only.
    
    Args:
        raw_phone: Raw phone number (may contain +, spaces, etc.)
    
    Returns:
        Phone number as digits only (e.g., '1234567890')
    """
    import re
    digits = re.sub(r'\D', '', str(raw_phone))
    return digits


app = Flask(__name__)
print("🚀 META WHATSAPP CLOUD API LIVE")

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

# ── White-label: resolve branding once per request ────────────────────────────
_SKIP_BRANDING_PREFIXES = ("/static/", "/webhook")

@app.before_request
def _resolve_branding():
    if any(request.path.startswith(p) for p in _SKIP_BRANDING_PREFIXES):
        g.branding = {"brand_name": "Filtrex AI", "logo_url": None,
                      "primary_color": "#4f46e5", "white_label_enabled": 0}
        return

    host = request.host.split(":")[0].lower()
    _local_hosts = {"localhost", "127.0.0.1", "0.0.0.0"}
    _replit_suffixes = (".replit.dev", ".repl.co", ".replit.app")

    g.branding = {"brand_name": "Filtrex AI", "logo_url": None,
                  "primary_color": "#4f46e5", "white_label_enabled": 0}

    # 1. Custom-domain match (strict — only non-local, non-Replit hosts)
    is_custom_host = (
        host not in _local_hosts
        and not any(host.endswith(s) for s in _replit_suffixes)
    )
    if is_custom_host:
        _con = sqlite3.connect("bookings.db", timeout=10)
        _con.row_factory = sqlite3.Row
        try:
            _row = _con.execute(
                "SELECT * FROM clients WHERE custom_domain=? AND white_label_enabled=1",
                (host,)
            ).fetchone()
        finally:
            _con.close()
        if _row:
            g.domain_client_id = _row["id"]
            g.branding = {
                "brand_name":          _row["brand_name"]    or "Filtrex AI",
                "logo_url":            _row["logo_url"]      or None,
                "primary_color":       _row["primary_color"] or "#4f46e5",
                "white_label_enabled": 1,
            }
            print(f"[DOMAIN_MATCH] host={host!r} client_id={_row['id']}")
            print(f"[WHITE_LABEL_APPLIED] client_id={_row['id']} brand={g.branding['brand_name']!r}")
            return

    # 2. Authenticated session — load branding for that client
    cid = session.get("client_id")
    if cid:
        _con = sqlite3.connect("bookings.db", timeout=10)
        _con.row_factory = sqlite3.Row
        try:
            _row = _con.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
        finally:
            _con.close()
        if _row and _row["white_label_enabled"]:
            g.branding = {
                "brand_name":          _row["brand_name"]    or "Filtrex AI",
                "logo_url":            _row["logo_url"]      or None,
                "primary_color":       _row["primary_color"] or "#4f46e5",
                "white_label_enabled": 1,
            }
            print(f"[BRAND_LOADED] client_id={cid} brand={g.branding['brand_name']!r}")


@app.context_processor
def _inject_branding():
    return {"branding": getattr(g, "branding", {"brand_name": "Filtrex AI",
                                                  "logo_url": None,
                                                  "primary_color": "#4f46e5",
                                                  "white_label_enabled": 0})}


# ═══════════════════════════════════════════════════════════════
# TRANSLATION SYSTEM
# ═══════════════════════════════════════════════════════════════

TRANSLATIONS = {
    "en": {
        "nav_dashboard":    "Dashboard",
        "nav_catalog":      "Catalog",
        "nav_orders":       "Orders",
        "nav_whatsapp":     "WhatsApp",
        "nav_billing":      "Billing",
        "nav_branding":     "Branding",
        "nav_integrations": "Integrations",
        "nav_settings":     "Settings",
        "nav_logout":       "Logout",
    },
    "ar": {
        "nav_dashboard":    "لوحة التحكم",
        "nav_catalog":      "الكتالوج",
        "nav_orders":       "الطلبات",
        "nav_whatsapp":     "واتساب",
        "nav_billing":      "الفواتير",
        "nav_branding":     "العلامة التجارية",
        "nav_integrations": "التكاملات",
        "nav_settings":     "الإعدادات",
        "nav_logout":       "تسجيل الخروج",
    },
}


def t(key, lang="en"):
    """Return translated string for key in given language, falling back to English."""
    lang = lang if lang in TRANSLATIONS else "en"
    return TRANSLATIONS[lang].get(key) or TRANSLATIONS["en"].get(key, key)


@app.context_processor
def _inject_lang():
    """Inject lang and t() into every template."""
    cid = session.get("client_id")
    lang = "en"
    if cid:
        _con = sqlite3.connect("bookings.db", timeout=10)
        _con.row_factory = sqlite3.Row
        try:
            _row = _con.execute(
                "SELECT default_language FROM clients WHERE id=?", (cid,)
            ).fetchone()
        finally:
            _con.close()
        if _row:
            lang = _row["default_language"] or "en"
    return {"lang": lang, "t": t}


@app.context_processor
def _inject_trial_info():
    """Inject trial_info into every admin template so the banner shows everywhere."""
    try:
        cid = session.get("client_id")
        if cid:
            _client = get_client(cid)
            return {"trial_info": get_trial_status(_client)}
    except Exception:
        pass
    return {"trial_info": None}


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
        con.execute("""
            CREATE TABLE IF NOT EXISTS wa_connect_tokens (
                token      TEXT PRIMARY KEY,
                client_id  INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used       INTEGER DEFAULT 0
            )
        """)
        con.execute("INSERT OR IGNORE INTO users (id, username, password) VALUES (1, 'admin', '123456')")
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
        if "msg_intent" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN msg_intent TEXT DEFAULT ''")
            print("[DB] migration: added msg_intent")
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
                business_type     TEXT NOT NULL DEFAULT '',
                default_language  TEXT DEFAULT 'ar',
                currency          TEXT DEFAULT 'SAR',
                timezone          TEXT DEFAULT 'Africa/Nouakchott',
                admin_whatsapp    TEXT,
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
        # clients: add whatsapp_connected + onboarding_step columns if missing
        _cli_cols = [r[1] for r in con.execute("PRAGMA table_info(clients)").fetchall()]
        if "whatsapp_connected" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_connected INTEGER DEFAULT 0")
            con.commit()
            print("[DB] migration: added whatsapp_connected")
        if "onboarding_step" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN onboarding_step INTEGER DEFAULT 0")
            con.execute("UPDATE clients SET onboarding_step=5 WHERE id=1")
            con.commit()
            print("[DB] migration: added onboarding_step, existing client=1 marked done (step=5)")
        else:
            con.execute("UPDATE clients SET onboarding_step=5 WHERE onboarding_step=3")
            con.commit()
        if "white_label_enabled" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN brand_name          TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN logo_url            TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN primary_color       TEXT DEFAULT '#4f46e5'")
            con.execute("ALTER TABLE clients ADD COLUMN custom_domain       TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN white_label_enabled INTEGER DEFAULT 0")
            con.commit()
            print("[WHITE_LABEL] migrated clients → brand_name, logo_url, primary_color, custom_domain, white_label_enabled")

        if "referral_code" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN referral_code   TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN referred_by     INTEGER")
            con.execute("ALTER TABLE clients ADD COLUMN referral_count  INTEGER DEFAULT 0")
            con.commit()
            _no_code = con.execute("SELECT id FROM clients WHERE referral_code IS NULL").fetchall()
            for _r in _no_code:
                _code = f"REF{_r['id']}{random.randint(1000, 9999)}"
                con.execute("UPDATE clients SET referral_code=? WHERE id=?", (_code, _r["id"]))
            if _no_code:
                con.commit()
            print(f"[REFERRAL_CREATED] migrated clients → referral columns, generated {len(_no_code)} code(s)")

        if "business_whatsapp_number" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN business_whatsapp_number TEXT")
            con.commit()
            print("[DB] migration: added business_whatsapp_number")
        if "whatsapp_connection_status" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_connection_status TEXT DEFAULT 'not_connected'")
            con.execute("""
                UPDATE clients
                SET whatsapp_connection_status = CASE
                    WHEN whatsapp_connected = 1 THEN 'connected'
                    ELSE 'not_connected'
                END
            """)
            con.commit()
            print("[DB] migration: added whatsapp_connection_status, backfilled existing")
        if "whatsapp_provider" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_provider TEXT DEFAULT 'meta'")
            con.commit()
            print("[DB] migration: added whatsapp_provider")

        # ── Affiliate columns (clients) ───────────────────────────────────────
        if "affiliate_code" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_enabled  INTEGER DEFAULT 1")
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_code     TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_earnings REAL    DEFAULT 0.0")
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_rate     REAL    DEFAULT 0.20")
            con.commit()
            _no_aff = con.execute("SELECT id FROM clients WHERE affiliate_code IS NULL").fetchall()
            for _r in _no_aff:
                con.execute("UPDATE clients SET affiliate_code=? WHERE id=?",
                            (f"AFF{_r['id']}", _r["id"]))
            if _no_aff:
                con.commit()
            print(f"[AFFILIATE_CREATED] migrated clients → affiliate columns, generated {len(_no_aff)} code(s)")

        # ── Trial columns ─────────────────────────────────────────────────────
        if "is_trial" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN is_trial            INTEGER DEFAULT 0")
            con.execute("ALTER TABLE clients ADD COLUMN trial_started_at    TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN trial_ends_at       TEXT")
            con.commit()
            print("[DB] migration: added is_trial, trial_started_at, trial_ends_at")
        if "trial_reminder_day" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN trial_reminder_day  INTEGER DEFAULT 0")
            con.commit()
            print("[DB] migration: added trial_reminder_day")

        # ── conversations: add collected_data column ──────────────────────────
        _conv_cols = [r[1] for r in con.execute("PRAGMA table_info(conversations)").fetchall()]
        if "collected_data" not in _conv_cols:
            con.execute("ALTER TABLE conversations ADD COLUMN collected_data TEXT DEFAULT '{}'")
            con.commit()
            print("[DB] migration: added collected_data")

        # ── orders: add intent + customer_phone + payment columns ───────────────
        _ord_cols = [r[1] for r in con.execute("PRAGMA table_info(orders)").fetchall()]
        if "intent" not in _ord_cols:
            con.execute("ALTER TABLE orders ADD COLUMN intent          TEXT DEFAULT 'unknown'")
            con.execute("ALTER TABLE orders ADD COLUMN customer_phone  TEXT DEFAULT ''")
            con.commit()
            print("[DB] migration: added intent, customer_phone")
        if "amount" not in _ord_cols:
            con.execute("ALTER TABLE orders ADD COLUMN amount           REAL DEFAULT 0")
            con.execute("ALTER TABLE orders ADD COLUMN payment_status   TEXT DEFAULT 'pending'")
            con.execute("ALTER TABLE orders ADD COLUMN payment_link     TEXT DEFAULT ''")
            con.execute("ALTER TABLE orders ADD COLUMN payment_provider TEXT DEFAULT 'paypal'")
            con.commit()
            print("[DB] migration: added amount, payment_status, payment_link, payment_provider")

        # ── AI Brain columns ──────────────────────────────────────────────────
        if "assistant_tone" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN assistant_tone        TEXT DEFAULT 'friendly'")
            con.execute("ALTER TABLE clients ADD COLUMN assistant_goal        TEXT DEFAULT 'book_appointments'")
            con.execute("ALTER TABLE clients ADD COLUMN business_description  TEXT DEFAULT ''")
            con.execute("ALTER TABLE clients ADD COLUMN policies              TEXT DEFAULT ''")
            con.execute("ALTER TABLE clients ADD COLUMN fallback_message      TEXT DEFAULT ''")
            con.commit()
            print("[DB] migration: added assistant_tone, assistant_goal, business_description, policies, fallback_message")

        # users: add email + client_id columns for multi-tenant auth
        _usr_cols = [r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()]
        if "affiliate_id" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN affiliate_id INTEGER")
            con.commit()
            print("[DB] migration: added affiliate_id")
        if "email" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN email TEXT")
            con.commit()
            print("[DB] migration: added email")
        if "client_id" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN client_id INTEGER")
            con.execute("UPDATE users SET client_id=1 WHERE client_id IS NULL")
            con.commit()
            print("[DB] migration: added client_id, linked existing users → 1")

        # ── STEP 7b: subscription_plans ──────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS subscription_plans (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                name               TEXT NOT NULL,
                price_monthly      REAL NOT NULL DEFAULT 0,
                max_messages       INTEGER NOT NULL DEFAULT 100,
                max_catalog_items  INTEGER NOT NULL DEFAULT 5,
                max_orders         INTEGER NOT NULL DEFAULT 20,
                features_json      TEXT DEFAULT '[]',
                is_active          INTEGER DEFAULT 1
            )
        """)

        # ── STEP 7c: client_subscriptions ────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS client_subscriptions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id      INTEGER NOT NULL,
                plan_id        INTEGER NOT NULL,
                status         TEXT NOT NULL DEFAULT 'active',
                started_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                expires_at     TEXT,
                messages_used  INTEGER DEFAULT 0,
                orders_used    INTEGER DEFAULT 0,
                bonus_messages INTEGER DEFAULT 0
            )
        """)
        con.commit()

        # client_subscriptions: bonus_messages for referral rewards (existing DBs)
        _sub_cols = [r[1] for r in con.execute("PRAGMA table_info(client_subscriptions)").fetchall()]
        if "bonus_messages" not in _sub_cols:
            con.execute("ALTER TABLE client_subscriptions ADD COLUMN bonus_messages INTEGER DEFAULT 0")
            con.commit()
            print("[DB] migration: added bonus_messages")
        if "paypal_subscription_id" not in _sub_cols:
            con.execute("ALTER TABLE client_subscriptions ADD COLUMN paypal_subscription_id TEXT")
            con.commit()
            print("[DB] migration: added paypal_subscription_id")

        # clients: plan shortcut + raw subscription_id for quick lookups
        if "plan" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN plan TEXT DEFAULT 'free'")
            con.commit()
            print("[DB] migration: added plan")
        if "subscription_id" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN subscription_id TEXT")
            con.commit()
            print("[DB] migration: added subscription_id")
        if "subscription_status" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN subscription_status TEXT DEFAULT 'inactive'")
            con.execute("""
                UPDATE clients
                SET subscription_status = 'active'
                WHERE plan IS NOT NULL AND plan != 'free' AND plan != ''
            """)
            con.commit()
            print("[DB] migration: added subscription_status")

        # ── STEP 7d: api_keys ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER NOT NULL,
                api_key    TEXT NOT NULL UNIQUE,
                label      TEXT DEFAULT 'Default',
                is_active  INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── STEP 7e: webhooks ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER NOT NULL,
                url        TEXT NOT NULL,
                event_type TEXT NOT NULL,
                is_active  INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── STEP 7f: client_integrations ─────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS client_integrations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id   INTEGER NOT NULL,
                provider    TEXT NOT NULL,
                config_json TEXT DEFAULT '{}',
                is_active   INTEGER DEFAULT 1,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── STEP 7g: paypal_payments ──────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS paypal_payments (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id       INTEGER,
                subscription_id TEXT,
                sale_id         TEXT UNIQUE,
                amount          REAL,
                currency        TEXT DEFAULT 'USD',
                event_type      TEXT,
                raw_json        TEXT,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── STEP 7h: analytics_events ─────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS analytics_events (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER,
                event_name TEXT NOT NULL,
                metadata   TEXT DEFAULT '{}',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── Seed default plans ────────────────────────────────────────────
        plan_count = con.execute("SELECT COUNT(*) FROM subscription_plans").fetchone()[0]
        if plan_count == 0:
            import json as _json
            _plans = [
                ("Free",     0,  100,  5,   20,  '["WhatsApp bot","Up to 5 catalog items","Basic support"]'),
                ("Starter",  9,  1000, 25,  100, '["WhatsApp bot","Up to 25 catalog items","Email support","Multilingual"]'),
                ("Pro",      29, 5000, 100, 500, '["WhatsApp bot","Up to 100 catalog items","Priority support","Multilingual","Upsells","Analytics"]'),
                ("Business", 79, -1,  -1,  -1,  '["Everything in Pro","Unlimited messages","Unlimited catalog","Dedicated support","Custom branding"]'),
            ]
            con.executemany("""
                INSERT INTO subscription_plans
                    (name, price_monthly, max_messages, max_catalog_items, max_orders, features_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """, _plans)
            con.commit()
            print("[DB] migration: seeded 4 default plans")

        # ── Price migration: ensure pricing matches current values ─────────
        _price_map = {"starter": 9, "pro": 29, "business": 79, "free": 0}
        for _pname, _pprice in _price_map.items():
            con.execute(
                "UPDATE subscription_plans SET price_monthly=? WHERE LOWER(name)=? AND price_monthly!=?",
                (_pprice, _pname, _pprice)
            )
        con.commit()

        # ── Assign Free plan to any client without a subscription ─────────
        free_plan = con.execute(
            "SELECT id FROM subscription_plans WHERE name='Free' LIMIT 1"
        ).fetchone()
        if free_plan:
            unsubscribed = con.execute("""
                SELECT id FROM clients
                WHERE id NOT IN (
                    SELECT DISTINCT client_id FROM client_subscriptions WHERE status='active'
                )
            """).fetchall()
            for cli in unsubscribed:
                con.execute("""
                    INSERT INTO client_subscriptions (client_id, plan_id, status)
                    VALUES (?, ?, 'active')
                """, (cli["id"], free_plan["id"]))
            if unsubscribed:
                con.commit()
                print(f"[DB] migration: assigned Free plan to {len(unsubscribed)} client(s)")

        # ── STEP 8: Seed default client ──────────────────────────────────────
        exists = con.execute("SELECT id FROM clients WHERE id = 1").fetchone()
        if not exists:
            con.execute("""
                INSERT INTO clients (id, name, business_type, default_language,
                    currency, timezone, admin_whatsapp, is_active)
                VALUES (1, 'My Business', '', 'ar',
                    'SAR', 'Africa/Nouakchott', NULL, 1)
            """)
            con.commit()
            print("[DB] migration: seeded default client id=1")

    finally:
        con.close()

_migrate_saas()

# ── SAAS HELPERS ───────────────────────────────────────────────────────────

CLIENT_ID = DEFAULT_CLIENT_ID

def _session_client_id():
    """Return the authenticated client's ID from session. Falls back to CLIENT_ID."""
    cid = session.get("client_id")
    return int(cid) if cid else CLIENT_ID

def get_client(client_id=CLIENT_ID):
    con = get_db_connection()
    try:
        row = con.execute("SELECT * FROM clients WHERE id=?", (client_id,)).fetchone()
    finally:
        con.close()
    return dict(row) if row else {}

def get_client_subscription(client_id):
    """Return dict with subscription + plan data for the active subscription, or None."""
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT cs.id, cs.client_id, cs.plan_id, cs.status,
                   cs.started_at, cs.expires_at,
                   cs.messages_used, cs.orders_used, cs.bonus_messages,
                   sp.name        AS plan_name,
                   sp.price_monthly,
                   sp.max_messages, sp.max_catalog_items, sp.max_orders,
                   sp.features_json
            FROM   client_subscriptions cs
            JOIN   subscription_plans   sp ON sp.id = cs.plan_id
            WHERE  cs.client_id = ? AND cs.status = 'active'
            ORDER  BY cs.id DESC LIMIT 1
        """, (client_id,)).fetchone()
    finally:
        con.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["features"] = json.loads(d.get("features_json") or "[]")
    except Exception:
        d["features"] = []
    return d


def check_usage_limit(client_id, limit_type):
    """Check whether client_id is within their plan limits."""
    sub = get_client_subscription(client_id)
    if not sub:
        print(f"[BILLING_LIMIT_CHECK] client={client_id} type={limit_type} NO_SUB → allowed")
        return True, None

    plan_name = sub.get("plan_name", "?")

    if limit_type == "messages":
        limit = sub.get("max_messages", 100) + sub.get("bonus_messages", 0)
        used  = sub.get("messages_used", 0)
    elif limit_type == "catalog_items":
        limit = sub.get("max_catalog_items", 5)
        con = get_db_connection()
        try:
            used = con.execute(
                "SELECT COUNT(*) FROM catalogs WHERE client_id=?", (client_id,)
            ).fetchone()[0]
        finally:
            con.close()
    elif limit_type == "orders":
        limit = sub.get("max_orders", 20)
        used  = sub.get("orders_used", 0)
    else:
        print(f"[BILLING_LIMIT_CHECK] client={client_id} UNKNOWN limit_type={limit_type!r} → allowed")
        return True, sub

    if limit == -1:
        print(f"[BILLING_LIMIT_CHECK] client={client_id} plan={plan_name!r} type={limit_type} used={used}/∞ → allowed (unlimited)")
        return True, sub

    allowed = used < limit
    status  = "allowed" if allowed else "BLOCKED"
    print(f"[BILLING_LIMIT_CHECK] client={client_id} plan={plan_name!r} type={limit_type} used={used}/{limit} → {status}")
    if not allowed:
        print(f"[BILLING_BLOCKED] client={client_id} plan={plan_name!r} type={limit_type} limit={limit} used={used}")
    return allowed, sub


def _billing_increment(client_id, field):
    """Increment messages_used or orders_used for the active subscription."""
    con = get_db_connection()
    try:
        con.execute(f"""
            UPDATE client_subscriptions
            SET    {field} = {field} + 1
            WHERE  client_id = ? AND status = 'active'
        """, (client_id,))
        con.commit()
    finally:
        con.close()


# ── Plan configuration ─────────────────────────────────────────────────────────
PLANS = {
    "free": {
        "max_messages":      100,
        "max_catalog_items": 5,
        "max_orders":        10,
        "features": {
            "whatsapp_bot": True,
            "multilingual": False,
            "upsell":       False,
            "analytics":    False,
            "white_label":  False,
        },
    },
    "starter": {
        "max_messages":      1000,
        "max_catalog_items": 25,
        "max_orders":        100,
        "features": {
            "whatsapp_bot": True,
            "multilingual": True,
            "upsell":       False,
            "analytics":    False,
            "white_label":  False,
        },
    },
    "pro": {
        "max_messages":      5000,
        "max_catalog_items": 100,
        "max_orders":        500,
        "features": {
            "whatsapp_bot": True,
            "multilingual": True,
            "upsell":       True,
            "analytics":    True,
            "white_label":  False,
        },
    },
    "business": {
        "max_messages":      None,
        "max_catalog_items": None,
        "max_orders":        None,
        "features": {
            "whatsapp_bot": True,
            "multilingual": True,
            "upsell":       True,
            "analytics":    True,
            "white_label":  True,
        },
    },
}


def get_client_plan(client_id):
    """Return the client's active plan name as a lowercase string."""
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT plan FROM clients WHERE id=?", (client_id,)
        ).fetchone()
    finally:
        con.close()
    plan = (row["plan"] if row and row["plan"] else "free").lower().strip()
    print(f"[PLAN_CHECK] client={client_id} plan={plan!r}")
    return plan


def has_feature(client_id, feature):
    """Return True if the client's current plan includes 'feature'."""
    plan    = get_client_plan(client_id)
    allowed = PLANS.get(plan, PLANS["free"])["features"].get(feature, False)
    if not allowed:
        print(f"[FEATURE_BLOCKED] client={client_id} plan={plan!r} feature={feature!r} → blocked")
    return allowed


def check_limit(client_id, limit_type):
    """Return (allowed: bool, sub: dict|None)."""
    plan     = get_client_plan(client_id)
    plan_cfg = PLANS.get(plan, PLANS["free"])
    _key_map = {
        "messages":      "max_messages",
        "catalog_items": "max_catalog_items",
        "orders":        "max_orders",
    }
    static_limit = plan_cfg.get(_key_map.get(limit_type, ""), 0)

    if static_limit is None:
        print(f"[LIMIT_CHECK] client={client_id} plan={plan!r} type={limit_type} → unlimited ✓")
        return True, None

    allowed, sub = check_usage_limit(client_id, limit_type)
    status = "allowed" if allowed else "EXCEEDED"
    print(f"[LIMIT_CHECK] client={client_id} plan={plan!r} type={limit_type} static_limit={static_limit} → {status}")
    if not allowed:
        print(f"[LIMIT_EXCEEDED] client={client_id} plan={plan!r} type={limit_type} limit={static_limit}")
    return allowed, sub


def check_plan_limit(client_id, limit_name):
    """Public alias for check_limit()."""
    return check_limit(client_id, limit_name)


def increment_usage(client_id, usage_type):
    """Increment a usage counter for the client's active subscription."""
    _billing_increment(client_id, usage_type)
    print(f"[USAGE_INCREMENTED] client={client_id} type={usage_type}")


def get_trial_status(client):
    """Return a dict describing the client's free-trial state."""
    if not client or not client.get("is_trial"):
        return {"is_trial": False, "active": False, "expired": False}

    ends_str = client.get("trial_ends_at")
    if not ends_str:
        return {"is_trial": True, "active": False, "expired": True}

    try:
        ends_at = datetime.datetime.fromisoformat(ends_str)
    except (ValueError, TypeError):
        return {"is_trial": True, "active": False, "expired": True}

    remaining = (ends_at - datetime.datetime.now()).total_seconds()

    if remaining <= 0:
        print(f"[TRIAL_EXPIRED] client={client.get('id')} trial_ends_at={ends_str!r}")
        return {
            "is_trial": True, "active": False, "expired": True,
            "remaining_seconds": 0, "days": 0, "hours": 0, "minutes": 0,
            "warning": False, "ends_at": ends_str,
        }

    days    = int(remaining // 86400)
    hours   = int((remaining % 86400) // 3600)
    minutes = int((remaining % 3600) // 60)
    warning = remaining < 86400

    if warning:
        print(f"[TRIAL_WARNING] client={client.get('id')} remaining={hours}h {minutes}m")
    else:
        print(f"[TRIAL_ACTIVE] client={client.get('id')} remaining={days}d {hours}h")

    return {
        "is_trial": True, "active": True, "expired": False,
        "remaining_seconds": remaining,
        "days": days, "hours": hours, "minutes": minutes,
        "warning": warning, "ends_at": ends_str,
    }


def expire_trial_if_needed(client_id):
    """Downgrade a client to the free plan if their trial has ended."""
    client = get_client(client_id)
    if not client.get("is_trial"):
        return False

    trial = get_trial_status(client)
    if not trial.get("expired"):
        return False

    con = get_db_connection()
    try:
        con.execute("""
            UPDATE clients
            SET    is_trial=0, plan='free', subscription_status='expired'
            WHERE  id=? AND is_trial=1
        """, (client_id,))
        con.execute("""
            UPDATE client_subscriptions
            SET    status='cancelled'
            WHERE  client_id=? AND status IN ('active', 'pending')
        """, (client_id,))
        con.commit()
    finally:
        con.close()

    print(f"[TRIAL_EXPIRED] client={client_id} → downgraded to free plan")
    track_event(client_id, "trial_expired", {})
    return True


def track_event(client_id, event_name, metadata=None):
    """Insert one row into analytics_events."""
    _meta = json.dumps(metadata or {})
    try:
        con = get_db_connection()
        try:
            con.execute(
                "INSERT INTO analytics_events (client_id, event_name, metadata) VALUES (?, ?, ?)",
                (client_id, event_name, _meta)
            )
            con.commit()
        finally:
            con.close()
        print(f"[EVENT_TRACKED] client={client_id} event={event_name!r} meta={_meta}")
    except Exception as _te:
        print(f"[EVENT_TRACK_ERROR] {event_name!r}: {_te}")


def handle_limit_exceeded(client_id, limit_type):
    """Central paywall handler."""
    print(f"[PAYWALL_TRIGGERED] client={client_id} limit_type={limit_type!r} → upgrade required")
    return {
        "error":       "limit_exceeded",
        "limit_type":  limit_type,
        "message_ar":  "لقد وصلت إلى الحد الأقصى لباقتك.",
        "message_en":  "You have reached your plan limit.",
        "upgrade_url": "/admin/billing",
    }


def generate_referral_code(client_id):
    """Generate a unique referral code for a client."""
    digits = random.randint(1000, 9999)
    return f"REF{client_id}{digits}"


def generate_affiliate_code(client_id):
    """Generate a deterministic affiliate code for a client."""
    return f"AFF{client_id}"


@app.route("/")
def home():
    if session.get("logged_in"):
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("login"))

@app.route("/assistant")
def assistant():
    return render_template("index.html")


@app.route("/whatsapp", methods=["GET", "POST"])
def whatsapp():
    """Meta WhatsApp Cloud API webhook endpoint.
    
    GET: Verifies webhook subscription
    POST: Processes incoming messages and sends replies
    """
    if request.method == "GET":
        # ── Webhook verification ──────────────────────────────────────────
        mode           = request.args.get("hub.mode")
        challenge      = request.args.get("hub.challenge")
        verify_token   = request.args.get("hub.verify_token")
        
        print(f"[META_WEBHOOK_VERIFICATION] mode={mode!r} token_match={verify_token == VERIFY_TOKEN}")
        
        if mode == "subscribe" and verify_token == VERIFY_TOKEN:
            print(f"[META_WEBHOOK_VERIFIED] challenge accepted")
            return challenge, 200
        
        print(f"[META_WEBHOOK_VERIFICATION_FAILED] mode={mode!r} token_match={verify_token == VERIFY_TOKEN}")
        return "Verification failed", 403
    
    # ── POST: Process incoming message ────────────────────────────────────
    print("[META_WEBHOOK_RECEIVED]")
    
    try:
        payload = request.get_json(force=True, silent=True) or {}
        print(f"[META_WEBHOOK_PAYLOAD] {json.dumps(payload, ensure_ascii=False)[:500]}")
        
        # Extract message data from Meta webhook structure
        # Structure: { "entry": [{ "changes": [{ "value": { "messages": [...] } }] }] }
        entry = (payload.get("entry") or [{}])[0]
        change = (entry.get("changes") or [{}])[0]
        value = change.get("value") or {}
        messages = value.get("messages") or []
        
        if not messages:
            print("[META_WEBHOOK_RECEIVED] no messages in payload")
            return jsonify({"status": "ok"}), 200
        
        message = messages[0]
        sender_phone = message.get("from")
        message_text = (message.get("text") or {}).get("body") or ""
        message_type = message.get("type", "text")
        
        print(f"[META_INCOMING_MESSAGE] from={sender_phone!r} type={message_type!r} text={message_text!r}")
        
        if not sender_phone or not message_text or message_type != "text":
            print("[META_WEBHOOK_RECEIVED] skipped non-text message")
            return jsonify({"status": "ok"}), 200
        
        # ── Send automatic reply using Meta Cloud API ──────────────────────
        reply_text = "أهلاً بك 👋 كيف أقدر أساعدك اليوم؟"
        response = meta_send_message(sender_phone, reply_text)
        
        if response and response.status_code == 200:
            print(f"[META_REPLY_SENT] to={sender_phone!r} status={response.status_code}")
        else:
            status = response.status_code if response else "N/A"
            print(f"[META_REPLY_SENT] to={sender_phone!r} status={status} error=failed to send")
        
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        import traceback
        print(f"[META_WEBHOOK_ERROR] {repr(e)}")
        print(traceback.format_exc())
        return jsonify({"status": "error"}), 500


@app.route("/build-id")
def build_id():
    return "BUILD_ID: META_WHATSAPP_CLOUD_API_001", 200


@app.route("/admin/dashboard")
def admin_dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid    = _session_client_id()
    client = get_client(cid)

    if not (int(client.get("onboarding_step") or 0) >= 5):
        print(f"[ONBOARDING_STEP] client={cid} step={client.get('onboarding_step', 0)} → redirect to onboarding")
        return redirect(url_for("admin_onboarding"))

    con = get_db_connection()
    try:
        total_orders  = con.execute("SELECT COUNT(*) FROM orders WHERE client_id=?", (cid,)).fetchone()[0]
        today_str     = datetime.datetime.now().strftime("%Y-%m-%d")
        today_orders  = con.execute(
            "SELECT COUNT(*) FROM orders WHERE client_id=? AND created_at LIKE ?",
            (cid, today_str + "%")
        ).fetchone()[0]
        catalog_count = con.execute(
            "SELECT COUNT(*) FROM catalogs WHERE client_id=? AND is_active=1", (cid,)
        ).fetchone()[0]
        active_convos = con.execute(
            "SELECT COUNT(*) FROM whatsapp_state WHERE current_step != 'service'"
        ).fetchone()[0]
        recent_orders = [dict(r) for r in con.execute(
            "SELECT * FROM orders WHERE client_id=? ORDER BY id DESC LIMIT 10", (cid,)
        ).fetchall()]
    finally:
        con.close()

    sub = get_client_subscription(cid)
    referral_link = f"{request.host_url.rstrip('/')}signup?ref={client.get('referral_code', '')}"
    stats = dict(total_orders=total_orders, today_orders=today_orders,
                 catalog_count=catalog_count, active_convos=active_convos)
    
    expire_trial_if_needed(cid)
    _fresh_client = get_client(cid)
    trial_info    = get_trial_status(_fresh_client)
    
    _aff_con = get_db_connection()
    try:
        _aff_count = _aff_con.execute(
            "SELECT COUNT(*) FROM users WHERE affiliate_id=?", (cid,)
        ).fetchone()[0]
    finally:
        _aff_con.close()
    affiliate_link = f"{request.host_url.rstrip('/')}signup?aff={_fresh_client.get('affiliate_code', '')}"
    affiliate_info = {
        "enabled":  _fresh_client.get("affiliate_enabled", 1),
        "code":     _fresh_client.get("affiliate_code", ""),
        "earnings": _fresh_client.get("affiliate_earnings") or 0.0,
        "count":    _aff_count,
        "rate":     int((_fresh_client.get("affiliate_rate") or 0.20) * 100),
        "link":     affiliate_link,
    }
    return render_template("admin/dashboard.html", client=client, stats=stats,
                           recent_orders=recent_orders, sub=sub,
                           referral_link=referral_link, active="dashboard",
                           trial_info=trial_info, affiliate_info=affiliate_info)


@app.route("/admin/onboarding", methods=["GET", "POST"])
def admin_onboarding():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    
    cid    = _session_client_id()
    client = get_client(cid)
    _lang  = client.get("default_language") or "en"

    if request.method == "POST":
        action = request.form.get("action", "")
        cur_step = int(client.get("onboarding_step") or 0)

        def _advance(new_step, updates=None):
            con = get_db_connection()
            try:
                if updates:
                    set_clause = ", ".join(f"{k}=?" for k in updates)
                    vals = list(updates.values()) + [cid]
                    con.execute(f"UPDATE clients SET {set_clause} WHERE id=?", vals)
                con.execute("UPDATE clients SET onboarding_step=? WHERE id=?",
                            (max(cur_step, new_step), cid))
                con.commit()
            finally:
                con.close()

        if action == "welcome_done":
            _advance(1)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=0 (welcome)")
            return redirect(url_for("admin_onboarding"))

        elif action == "save_business":
            biz_name = request.form.get("name", "").strip()
            biz_type = request.form.get("business_type", "").strip()
            lang_val  = request.form.get("default_language", "en").strip()
            currency  = request.form.get("currency", "").strip()
            timezone  = request.form.get("timezone", "").strip()
            updates = {}
            if biz_name:
                updates["name"] = biz_name
            if biz_type:
                updates["business_type"] = biz_type
            if lang_val:
                updates["default_language"] = lang_val
            if currency:
                updates["currency"] = currency
            if timezone:
                updates["timezone"] = timezone
            _advance(2, updates if updates else None)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=1 (business_info)")
            return redirect(url_for("admin_onboarding"))

        elif action == "save_ai":
            tone      = request.form.get("assistant_tone", "friendly").strip()
            goal      = request.form.get("assistant_goal", "book_appointments").strip()
            biz_desc  = request.form.get("business_description", "").strip()
            updates = {
                "assistant_tone":       tone,
                "assistant_goal":       goal,
                "business_description": biz_desc,
            }
            _advance(3, updates)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=2 (ai_settings)")
            return redirect(url_for("admin_onboarding"))

        elif action in ("whatsapp_done", "skip_whatsapp"):
            _advance(4)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=3 (whatsapp)")
            return redirect(url_for("admin_onboarding"))

        elif action == "complete":
            _now       = datetime.datetime.now()
            _trial_end = _now + datetime.timedelta(days=3)
            _now_iso   = _now.isoformat(timespec="seconds")
            _end_iso   = _trial_end.isoformat(timespec="seconds")
            con = get_db_connection()
            try:
                _existing = con.execute(
                    "SELECT is_trial, trial_started_at FROM clients WHERE id=?", (cid,)
                ).fetchone()
                if _existing and not _existing["is_trial"]:
                    con.execute("""
                        UPDATE clients
                        SET onboarding_step=5,
                            is_trial=1, is_active=1,
                            trial_started_at=?, trial_ends_at=?
                        WHERE id=?
                    """, (_now_iso, _end_iso, cid))
                    print(f"[TRIAL_STARTED] client={cid}")
                    track_event(cid, "trial_started", {})
                else:
                    con.execute("UPDATE clients SET onboarding_step=5 WHERE id=?", (cid,))
                con.commit()
            finally:
                con.close()
            track_event(cid, "onboarding_completed", {})
            print(f"[ONBOARDING_FINISHED] client={cid}")
            flash("Setup complete! Welcome to Filtrex AI.", "success")
            return redirect(url_for("admin_dashboard"))

        return redirect(url_for("admin_onboarding"))

    client = get_client(cid)
    step = int(client.get("onboarding_step") or 0)

    if step == 0:
        print(f"[ONBOARDING_STARTED] client={cid}")

    if step >= 5:
        return redirect(url_for("admin_dashboard"))

    wa_connected = bool(client.get("whatsapp_connected"))

    return render_template("admin/onboarding.html", client=client, step=step,
                           wa_connected=wa_connected, lang=_lang, active="dashboard")


@app.route("/onboarding", methods=["GET", "POST"])
def onboarding_alias():
    return admin_onboarding()


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        email    = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        con = get_db_connection()
        try:
            row = con.execute(
                "SELECT id, password, client_id, email FROM users WHERE email=? OR username=?",
                (email, email)
            ).fetchone()
        finally:
            con.close()
        if row and check_password_hash(row["password"], password):
            client_id = row["client_id"] or CLIENT_ID
            session.clear()
            session["logged_in"]  = True
            session["user_id"]    = row["id"]
            session["client_id"]  = client_id
            session["user_email"] = row["email"] or email
            print(f"[AUTH_LOGIN] email={email!r} client_id={client_id}")
            return redirect(url_for("admin_dashboard"))
        error = "Invalid email or password."
    return render_template("login.html", error=error)


@app.route("/signup", methods=["GET", "POST"])
def signup():
    error = None
    if request.method == "POST":
        business_name = request.form.get("business_name", "").strip()
        email         = request.form.get("email", "").strip().lower()
        password      = request.form.get("password", "").strip()
        if not business_name or not email or not password:
            error = "All fields are required."
        elif len(password) < 6:
            error = "Password must be at least 6 characters."
        else:
            con = get_db_connection()
            try:
                existing = con.execute(
                    "SELECT id FROM users WHERE email=?", (email,)
                ).fetchone()
                if existing:
                    error = "An account with this email already exists."
                else:
                    cur_c = con.execute("""
                        INSERT INTO clients
                            (name, business_type, default_language,
                             currency, timezone, is_active)
                        VALUES (?, 'other', 'ar', 'MAR', 'Africa/Casablanca', 1)
                    """, (business_name,))
                    new_client_id = cur_c.lastrowid
                    new_ref_code = generate_referral_code(new_client_id)
                    con.execute(
                        "UPDATE clients SET referral_code=? WHERE id=?",
                        (new_ref_code, new_client_id)
                    )
                    new_aff_code = generate_affiliate_code(new_client_id)
                    con.execute(
                        "UPDATE clients SET affiliate_code=? WHERE id=?",
                        (new_aff_code, new_client_id)
                    )
                    _t_now = datetime.datetime.now()
                    _t_end = _t_now + datetime.timedelta(days=3)
                    con.execute("""
                        UPDATE clients
                        SET    is_trial=1,
                               trial_started_at=?,
                               trial_ends_at=?,
                               plan='starter'
                        WHERE  id=?
                    """, (_t_now.isoformat(timespec="seconds"),
                          _t_end.isoformat(timespec="seconds"),
                          new_client_id))
                    cur_u = con.execute("""
                        INSERT INTO users (username, email, password, client_id)
                        VALUES (?, ?, ?, ?)
                    """, (email, email, generate_password_hash(password), new_client_id))
                    new_user_id = cur_u.lastrowid
                    con.commit()
                    print(f"[AUTH_SIGNUP] user_id={new_user_id} client_id={new_client_id}")
                    track_event(new_client_id, "user_registered", {"email": email})
                    
                    session.clear()
                    session["logged_in"]  = True
                    session["user_id"]    = new_user_id
                    session["client_id"]  = new_client_id
                    session["user_email"] = email
                    return redirect(url_for("admin_dashboard"))
            finally:
                con.close()
    return render_template("signup.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=5000, debug=debug)
