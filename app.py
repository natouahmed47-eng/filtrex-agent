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
    resp = requests.post(url, data=payload, timeout=10)
    print(f"[ULTRAMSG] response status={resp.status_code} body={resp.text!r}")
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
            "SELECT known_service, known_day, known_time, known_name, current_step, lang FROM whatsapp_state WHERE phone = ?",
            (phone,)
        ).fetchone()
    finally:
        con.close()
    if row:
        state = {
            "known_service": row["known_service"],
            "known_day":     row["known_day"],
            "known_time":    row["known_time"],
            "known_name":    row["known_name"],
            "current_step":  row["current_step"] or "service",
            "lang":          row["lang"] or "",
        }
    else:
        state = {"known_service": None, "known_day": None, "known_time": None, "known_name": None, "current_step": "service", "lang": ""}
    print(f"[STATE_LOAD] sender={phone} state={state}")
    return state

def wa_save(phone, state):
    print(f"[STATE_SAVE] sender={phone} state={state}")
    con = get_db_connection()
    try:
        con.execute(
            """INSERT INTO whatsapp_state (phone, known_service, known_day, known_time, known_name, current_step, lang)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   known_service = excluded.known_service,
                   known_day     = excluded.known_day,
                   known_time    = excluded.known_time,
                   known_name    = excluded.known_name,
                   current_step  = excluded.current_step,
                   lang          = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE whatsapp_state.lang END""",
            (phone,
             state.get("known_service"),
             state.get("known_day"),
             state.get("known_time"),
             state.get("known_name"),
             state.get("current_step", "service"),
             state.get("lang", ""))
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
        "ar": "ممتاز! 😊 {svc} متاح بسعر {price}\nهل تفضل موعدك اليوم أو غدًا؟",
        "en": "Great! 😊 {svc} is available for {price}\nWould you prefer today or tomorrow?",
        "fr": "Parfait! 😊 {svc} est disponible pour {price}\nPréférez-vous aujourd'hui ou demain?",
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
        f"الخدمة: {state.get('known_service')}\n"
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
    "الساعة 9", "الساعة 10", "الساعة 11", "الساعة 12",
    "الساعة 1", "الساعة 2", "الساعة 3", "الساعة 4",
    "الساعة 5", "الساعة 6", "الساعة 7",
]

def get_top_times(times, limit=3):
    return times[:limit]

def normalize_time_input(msg):
    msg = msg.strip()
    mapping = {
        "الصباح": "الساعة 9",
        "بدري":   "الساعة 10",
        "الظهر":  "الساعة 12",
        "العصر":  "الساعة 4",
        "المغرب": "الساعة 6",
        "المساء": "الساعة 7",
        "الليل":  "الساعة 8",
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
    svc  = state.get("known_service") or "غير محدد"
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
        step  = state["current_step"]

        stored_lang  = state.get("lang") or ""
        detected_lang = detect_lang(incoming_msg)
        print(f"[LANG_DEBUG_BEFORE] stored={stored_lang!r}")

        if not stored_lang:
            # First message — store whatever we detect
            lang = detected_lang
            state["lang"] = lang
            wa_save(sender, state)
            print(f"[LANG] first-detect={lang!r} stored for sender={sender!r}")
        elif detected_lang != stored_lang and is_lang_switch_worthy(incoming_msg):
            # User clearly switched language — follow them
            print(f"[LANG_SWITCH_CHECK] stored={stored_lang!r} detected={detected_lang!r} msg={incoming_msg!r}")
            print(f"[LANG_SWITCH] updated sender={sender!r} from={stored_lang!r} to={detected_lang!r}")
            lang = detected_lang
            state["lang"] = lang
            wa_save(sender, state)
        else:
            # Same language or too short to be certain — keep stored
            print(f"[LANG_SWITCH_CHECK] stored={stored_lang!r} detected={detected_lang!r} msg={incoming_msg!r}")
            lang = stored_lang

        print(f"[LANG_DEBUG_AFTER] {lang!r}")
        print(f"[LANG_FINAL] using={lang!r}")
        print(f"[WHATSAPP] step={step!r} lang={lang!r}")

        # ── STEP: service ─────────────────────────────────────────────────
        if step == "service":
            if is_greeting_only(incoming_msg):
                reply = openai_chat(incoming_msg, lang=lang)
            else:
                svc = detect_wa_service(incoming_msg)
                if svc:
                    price = _WA_PRICES[svc]
                    state["known_service"] = svc
                    state["current_step"]  = "day"
                    wa_save(sender, state)
                    reply = t("service_confirmed", lang).format(svc=svc, price=price)
                elif is_price_question(incoming_msg):
                    reply = t("price_list", lang)
                else:
                    reply = openai_chat(incoming_msg, lang=lang)

        # ── STEP: day ─────────────────────────────────────────────────────
        elif step == "day":
            svc = detect_wa_service(incoming_msg)
            if svc and not state["known_service"]:
                state["known_service"] = svc
            state["known_day"]    = incoming_msg.strip()
            state["current_step"] = "time"
            wa_save(sender, state)
            reply = t("ask_time", lang)

        # ── STEP: time ────────────────────────────────────────────────────
        elif step == "time":
            time_val = normalize_time_input(incoming_msg)
            svc      = state.get("known_service") or ""
            day      = state.get("known_day")     or ""
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
            name = incoming_msg.strip()
            wa_save_booking(sender, state, name)
            print("[WHATSAPP] booking saved — calling notify_admin_booking")
            try:
                notify_admin_booking(sender, state, name)
            except Exception as _ne:
                print(f"[ADMIN_NOTIFY_OUTER_ERROR] {repr(_ne)}")
            wa_clear(sender)
            svc  = state.get("known_service") or "-"
            day  = state.get("known_day")     or ""
            time = state.get("known_time")    or ""
            reply = t("booking_confirmed", lang).format(svc=svc, day=day, time=time, name=name)

        else:
            state["current_step"] = "service"
            wa_save(sender, state)
            reply = openai_chat(incoming_msg, lang=lang)

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
