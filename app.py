from flask import Flask, request, jsonify, render_template, session, redirect, url_for
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
        con.commit()
    finally:
        con.close()

_migrate_whatsapp_state()

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
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))

@app.route("/assistant")
def assistant():
    return render_template("index.html")

WHATSAPP_USER_ID = 1

def wa_load(phone):
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT known_service, known_day, known_time, known_name, current_step, lang, upsell_offered, upsell_rejected FROM whatsapp_state WHERE phone = ?",
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
            "known_service":  _svc_val,
            "known_day":      row["known_day"],
            "known_time":     row["known_time"],
            "known_name":     row["known_name"],
            "current_step":   row["current_step"] or "service",
            "lang":           row["lang"] or "",
            "upsell_offered": bool(row["upsell_offered"]),
            "upsell_rejected": bool(row["upsell_rejected"]),
        }
    else:
        state = {"known_service": [], "known_day": None, "known_time": None, "known_name": None,
                 "current_step": "service", "lang": "", "upsell_offered": False, "upsell_rejected": False}
    print(f"[STATE_LOAD] sender={phone} state={state}")
    return state

def wa_save(phone, state):
    print(f"[STATE_SAVE] sender={phone} state={state}")
    con = get_db_connection()
    try:
        _svc_to_save = state.get("known_service")
        if isinstance(_svc_to_save, list):
            _svc_to_save = json.dumps(_svc_to_save, ensure_ascii=False) if _svc_to_save else None
        con.execute(
            """INSERT INTO whatsapp_state (phone, known_service, known_day, known_time, known_name, current_step, lang, upsell_offered, upsell_rejected)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   known_service  = excluded.known_service,
                   known_day      = excluded.known_day,
                   known_time     = excluded.known_time,
                   known_name     = excluded.known_name,
                   current_step   = excluded.current_step,
                   lang           = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE whatsapp_state.lang END,
                   upsell_offered = excluded.upsell_offered,
                   upsell_rejected = excluded.upsell_rejected""",
            (phone,
             _svc_to_save,
             state.get("known_day"),
             state.get("known_time"),
             state.get("known_name"),
             state.get("current_step", "service"),
             state.get("lang", ""),
             1 if state.get("upsell_offered") else 0,
             1 if state.get("upsell_rejected") else 0)
        )
        con.commit()
        print(f"[STATE_SAVE] committed lang={state.get('lang')!r}")
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

def normalize_number(sender):
    sender = sender.replace("whatsapp:", "").replace("+", "").strip()
    if not sender.endswith("@c.us"):
        sender = sender + "@c.us"
    return sender

def wa_reply(to, text):
    raw = to
    to  = normalize_number(to)
    print(f"[WHATSAPP] raw_sender={raw!r} normalized_sender={to!r}")
    print(f"[WHATSAPP] final_reply to={to!r} text={text!r}")
    resp = ultramsg_send(to, text)
    print(f"[ULTRAMSG] reply status={resp.status_code if resp else 'N/A'} body={resp.text[:200] if resp else 'N/A'}")
    return "", 200

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

def confirmation_message(state, name, lang):
    _svcs = ensure_svc_list(state.get("known_service"))
    svc   = format_services(_svcs, lang) if _svcs else "-"
    day   = sanitize_booking_field(state.get("known_day"))
    time  = sanitize_booking_field(state.get("known_time"))
    return t("booking_confirmed", lang).format(
        svc=svc,
        day=day,
        time=time,
        name=name,
    )

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

def build_upsell(svc, lang):
    upsell_svc = _UPSELL_MAP.get(svc)
    if not upsell_svc:
        return ""
    uname = svc_name(upsell_svc, lang)
    print(f"[UPSELL] suggested={upsell_svc!r} for svc={svc!r}")
    _lang = lang if lang in ("ar", "en", "fr") else "ar"
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

def notify_admin_booking(phone, state, name):
    msg = (
        f"📥 حجز جديد\n"
        f"الاسم: {name}\n"
        f"الرقم: {phone}\n"
        f"{format_services(ensure_svc_list(state.get('known_service'))) or 'الخدمة: غير محدد'}\n"
        f"الموعد: {state.get('known_day')} {state.get('known_time')}"
    )
    print("[ADMIN_NOTIFY] sending notification...")
    print(f"[ADMIN_NOTIFY] TO={ADMIN_WHATSAPP_NUMBER!r}")
    print(f"[ADMIN_NOTIFY] MSG={msg!r}")
    try:
        resp = ultramsg_send(normalize_number(ADMIN_WHATSAPP_NUMBER), msg.strip())
        print(f"[ADMIN_NOTIFY] status={resp.status_code} body={resp.text}")
    except Exception as e:
        print(f"[ADMIN_NOTIFY_ERROR] {e}")

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
    _svcs = ensure_svc_list(state.get("known_service"))
    svc   = " / ".join(_svcs) if _svcs else "غير محدد"
    day  = state.get("known_day")  or ""
    time = state.get("known_time") or ""
    slot = f"{day} {time}".strip()
    print(f"[DB] wa_save_booking phone={phone} service={svc} slot={slot} name={name}")
    con = get_db_connection()
    try:
        con.execute(
            "INSERT INTO bookings (user_id, name, service, time, timestamp) VALUES (?, ?, ?, ?, ?)",
            (str(WHATSAPP_USER_ID), name, svc, slot, datetime.datetime.now().isoformat())
        )
        con.commit()
        print(f"[DB] wa_save_booking committed")
    except Exception as db_err:
        print(f"[DB] wa_save_booking ERROR: {repr(db_err)}")
        raise
    finally:
        con.close()

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
        msg_data     = data.get("data", {})
        sender       = msg_data.get("from", "").strip()
        incoming_msg = msg_data.get("body", "").strip()
        msg_type     = msg_data.get("type", "")

        print(f"[WHATSAPP] sender={sender!r} message={incoming_msg!r} type={msg_type!r}")

        if msg_type != "chat" or not sender or not incoming_msg:
            print("[WHATSAPP] ignored non-chat or empty message")
            return "", 200

        state = wa_load(sender)
        _step_early = state.get("current_step", "service")

        if is_noise_message(incoming_msg) and _step_early != "service":
            print(f"[NOISE] ignored mid-booking greeting at step={_step_early!r}")
            return "", 200

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

        if _p_name and not state.get("known_name") and step in ("name", "confirm"):
            if is_valid_name(_p_name):
                state["known_name"] = _p_name
                _changed = True
                print(f"[STATE_MERGE] set name={_p_name!r}")

        if _changed:
            wa_save(sender, state)
            print(f"[STATE_MERGE] updated_state={state}")

        # ── REGEX FALLBACK (only when LLM found nothing) ──────────────────
        _parser_found = any([_p_svc, _p_addon, _p_day, _p_time])
        if not _parser_found:
            _e_svc, _e_day, _e_time = extract_entities(incoming_msg)
            _re_changed = False
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

        # ── GREETING — only reset at entry step, re-ask mid-booking ──────────
        if is_greeting(incoming_msg):
            if step == "service":
                print(f"[GREETING] resetting state for sender={sender!r}")
                wa_clear(sender)
                reply = openai_chat(
                    "User greeted you. Reply politely and ask how you can help.",
                    lang=lang,
                )
                return wa_reply(sender, reply)
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

        # ── STEP: service ─────────────────────────────────────────────────
        if step == "service":
            svc = detect_wa_service(incoming_msg)
            if svc:
                _cur_svcs = ensure_svc_list(state.get("known_service"))
                if is_add_intent(incoming_msg):
                    if svc not in _cur_svcs:
                        _cur_svcs.append(svc)
                else:
                    _cur_svcs = [svc]
                state["known_service"] = _cur_svcs
                print(f"[ENTITY_EXTRACT] service list={_cur_svcs!r}")
                _has_day  = bool(state.get("known_day"))
                _has_time = bool(state.get("known_time"))
                if _has_day and _has_time:
                    state["current_step"] = "name"
                elif _has_day:
                    state["current_step"] = "time"
                else:
                    state["current_step"] = "day"
                print(f"[FLOW] asking_for={state['current_step']!r}")
                wa_save(sender, state)
                _primary = _cur_svcs[-1]
                reply = t("service_confirmed", lang).format(
                    svc=format_svcs(_cur_svcs, lang),
                    price=svc_price(_primary, lang),
                    benefit=svc_benefit(_primary, lang),
                )
                if not _has_day:
                    reply += "\n" + build_times_hint(_primary, lang, day=state.get("known_day"))
                if can_show_upsell(state):
                    upsell = build_upsell(_primary, lang)
                    if upsell:
                        reply += "\n" + upsell
                        state["upsell_offered"] = True
                        wa_save(sender, state)
                        print(f"[UPSELL_OFFER] service={_primary!r} suggested={_UPSELL_CANONICAL_MAP.get(_primary)!r}")
            elif is_price_question(incoming_msg):
                reply = t("price_list", lang)
            elif is_recommendation_request(incoming_msg):
                rec = _RECOMMENDED_SERVICE
                _rec_lang = lang if lang in ("ar", "en", "fr") else "ar"
                reply = _RECOMMENDATION[_rec_lang].format(
                    svc=svc_name(rec, lang),
                    benefit=svc_benefit(rec, lang),
                    price=svc_price(rec, lang),
                )
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
                if is_time_slot_taken(svc, day, time_val):
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

            wa_save_booking(sender, state, name)

            print("[WHATSAPP] booking saved — calling notify_admin_booking")

            try:
                notify_admin_booking(sender, state, name)
            except Exception as _ne:
                print(f"[ADMIN_NOTIFY_OUTER_ERROR] {repr(_ne)}")

            wa_clear(sender)

            return wa_reply(sender, confirmation_message(state, name, lang))

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
            return redirect(url_for("dashboard"))
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
