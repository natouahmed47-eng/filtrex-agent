from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
from twilio.twiml.messaging_response import MessagingResponse
import requests
import os
import json
import sqlite3
import datetime

app = Flask(__name__)

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

def init_db():
    con = sqlite3.connect(DB_FILE)
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
            known_time    TEXT,
            known_name    TEXT,
            awaiting_name INTEGER DEFAULT 0
        )
    """)
    con.execute("INSERT OR IGNORE INTO users (id, username, password) VALUES (1, 'admin', '123456')")
    con.execute("INSERT OR IGNORE INTO users (id, username, password) VALUES (2, 'clinic2', '123456')")
    con.execute("INSERT OR IGNORE INTO business_settings (user_id, business_name, services, default_language) VALUES (1, 'Veltrix Dental Clinic', 'تنظيف أسنان,تبييض أسنان', 'ar')")
    con.execute("INSERT OR IGNORE INTO business_settings (user_id, business_name, services, default_language) VALUES (2, 'Bright Smile Studio', 'فحص أسنان,تبييض أسنان', 'ar')")
    rows = con.execute("SELECT id, password FROM users").fetchall()
    for row in rows:
        pwd = row[1]
        if not pwd.startswith("pbkdf2:") and not pwd.startswith("scrypt:"):
            con.execute("UPDATE users SET password = ? WHERE id = ?",
                        (generate_password_hash(pwd), row[0]))
    con.commit()
    con.close()

init_db()

bookings = []

def get_biz(user_id):
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT business_name, services, default_language FROM business_settings WHERE user_id = ?",
        (user_id,)
    ).fetchone()
    con.close()
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
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT known_service, known_time, known_name, awaiting_name FROM whatsapp_state WHERE phone = ?",
        (phone,)
    ).fetchone()
    con.close()
    if row:
        return {
            "known_service": row["known_service"],
            "known_time":    row["known_time"],
            "known_name":    row["known_name"],
            "awaiting_name": bool(row["awaiting_name"]),
        }
    return {"known_service": None, "known_time": None, "known_name": None, "awaiting_name": False}

def wa_save(phone, known_service, known_time, known_name, awaiting_name):
    con = sqlite3.connect(DB_FILE)
    con.execute(
        """INSERT INTO whatsapp_state (phone, known_service, known_time, known_name, awaiting_name)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(phone) DO UPDATE SET
               known_service = excluded.known_service,
               known_time    = excluded.known_time,
               known_name    = excluded.known_name,
               awaiting_name = excluded.awaiting_name""",
        (phone, known_service, known_time, known_name, 1 if awaiting_name else 0)
    )
    con.commit()
    con.close()
    print(f"[WHATSAPP] state_saved service={known_service} time={known_time} name={known_name} awaiting_name={awaiting_name}")

def wa_clear(phone):
    con = sqlite3.connect(DB_FILE)
    con.execute("DELETE FROM whatsapp_state WHERE phone = ?", (phone,))
    con.commit()
    con.close()
    print(f"[WHATSAPP] state_cleared phone={phone}")

def twilio_reply(text):
    print(f"[WHATSAPP] final_reply={text!r}")
    resp = MessagingResponse()
    resp.message(text)
    return str(resp), 200, {"Content-Type": "text/xml"}

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

@app.route("/whatsapp", methods=["POST"])
def whatsapp():
    try:
        # Step 1: Read inputs
        sender       = request.form.get("From", "").strip()
        incoming_msg = request.form.get("Body", "").strip()
        print(f"[WHATSAPP] sender={sender}")
        print(f"[WHATSAPP] message={incoming_msg!r}")

        # Step 2: Load state
        state         = wa_load(sender)
        known_service = state["known_service"]
        known_time    = state["known_time"]
        known_name    = state["known_name"]
        awaiting_name = state["awaiting_name"]
        print(f"[WHATSAPP] loaded_state={state}")

        msg_lower = incoming_msg.lower()

        # Step 3: Greeting — clear and return
        if msg_lower in {"سلام", "مرحبا", "اهلا", "hello", "hi"}:
            wa_clear(sender)
            print("[WHATSAPP] greeting — state cleared")
            return twilio_reply("أهلاً 👋 كيف أقدر أساعدك اليوم؟")

        # Step 4: Extract all fields from message
        biz     = get_biz(WHATSAPP_USER_ID)
        allowed = biz.get("services", [])
        extracted = extract_booking_fields(incoming_msg, allowed)
        print(f"[WHATSAPP] extracted={extracted}")

        # Step 5: Merge extracted values into state (only if not already set)
        state_changed = False

        if extracted["service"] and not known_service:
            print(f"[WHATSAPP] branch=service_detected value={extracted['service']!r}")
            known_service = extracted["service"]
            state_changed = True
        elif extracted["raw_service"] and not known_service:
            # Service keyword found but not in the allowed list
            names = "، ".join(allowed)
            print(f"[WHATSAPP] branch=service_not_allowed raw={extracted['raw_service']!r} available={names}")
            return twilio_reply(f"عذراً، هذه الخدمة غير متاحة. الخدمات المتاحة: {names}")
        else:
            print(f"[WHATSAPP] branch=service_no_change known_service={known_service!r}")

        if extracted["time"] and not known_time:
            print(f"[WHATSAPP] branch=time_detected value={extracted['time']!r}")
            known_time    = extracted["time"]
            state_changed = True
        else:
            print(f"[WHATSAPP] branch=time_no_change known_time={known_time!r}")

        if extracted["name"] and not known_name:
            print(f"[WHATSAPP] branch=name_extracted value={extracted['name']!r}")
            known_name    = extracted["name"]
            state_changed = True
        else:
            print(f"[WHATSAPP] branch=name_no_extract awaiting_name={awaiting_name}")

        # Step 6: awaiting_name fallback — only if message contains NO service or time keywords
        # BUG FIX: without this guard, re-sending "غدًا مساءً" while awaiting name
        # would be captured as the name and save a wrong booking.
        if (awaiting_name and not known_name
                and extracted["raw_service"] is None
                and extracted["time"] is None):
            known_name    = incoming_msg.strip()
            state_changed = True
            print(f"[WHATSAPP] branch=awaiting_name_fallback capturing_name={known_name!r}")
        elif awaiting_name and not known_name:
            print(f"[WHATSAPP] branch=awaiting_name_skipped (service/time keyword present in message)")

        # BUG FIX: always save with awaiting_name=False here.
        # awaiting_name=True is only set explicitly in step 10.
        # Passing the loaded value caused the flag to persist incorrectly when
        # the user sent new service/time info after the bot had already asked for name.
        if state_changed:
            wa_save(sender, known_service, known_time, known_name, False)
            print(f"[WHATSAPP] state_saved service={known_service!r} time={known_time!r} name={known_name!r} awaiting_name=False")

        print(f"[WHATSAPP] merged_state service={known_service!r} time={known_time!r} name={known_name!r}")

        # Step 7: All three fields known — confirm booking immediately
        if known_service and known_time and known_name:
            print("[WHATSAPP] branch=confirm_booking")
            con = sqlite3.connect(DB_FILE)
            con.execute(
                "INSERT INTO bookings (user_id, name, service, time, timestamp) VALUES (?, ?, ?, ?, ?)",
                (str(WHATSAPP_USER_ID), known_name, known_service, known_time,
                 datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            con.commit()
            con.close()
            wa_clear(sender)
            reply = (
                f"تم تأكيد حجزك بنجاح ✅\n"
                f"الخدمة: {known_service}\n"
                f"الموعد: {known_time}\n"
                f"الاسم: {known_name}"
            )
            print(f"[WHATSAPP] booking_saved name={known_name!r} service={known_service!r} time={known_time!r}")
            return twilio_reply(reply)

        # Step 8: Ask for missing service
        if not known_service:
            if known_time:
                print("[WHATSAPP] branch=ask_service (time known)")
                return twilio_reply("ما الخدمة التي تريد حجزها؟")
            names = "، ".join(allowed) or "الخدمات المتاحة"
            print("[WHATSAPP] branch=ask_service (nothing known)")
            return twilio_reply(f"أهلاً! الخدمات المتاحة: {names}. أيها تفضل؟")

        # Step 9: Ask for missing time
        if not known_time:
            print("[WHATSAPP] branch=ask_time")
            return twilio_reply("ممتاز! متى تفضل موعدك؟ (مثال: غدًا صباحًا)")

        # Step 10: Ask for missing name — set awaiting_name=True explicitly
        print("[WHATSAPP] branch=ask_name")
        wa_save(sender, known_service, known_time, known_name, True)
        print("[WHATSAPP] state_saved awaiting_name=True")
        return twilio_reply("رائع! ما الاسم الذي تريد تأكيد الحجز باسمه؟")

    except Exception as e:
        import traceback
        print(f"[WHATSAPP] EXCEPTION: {e}")
        print(traceback.format_exc())
        return twilio_reply("حدث خطأ مؤقت، حاول مرة أخرى.")

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
            con = sqlite3.connect(DB_FILE)
            existing = con.execute(
                "SELECT id FROM users WHERE username = ?", (username,)
            ).fetchone()
            if existing:
                con.close()
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
                con.close()
                return redirect(url_for("login"))
    return render_template("register.html", error=error)

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        con = sqlite3.connect(DB_FILE)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT id, password FROM users WHERE username = ?", (username,)
        ).fetchone()
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
        con = sqlite3.connect(DB_FILE)
        con.execute(
            "INSERT OR REPLACE INTO business_settings (user_id, business_name, services, default_language) VALUES (?, ?, ?, ?)",
            (user_id, business_name, services_str, default_language)
        )
        con.commit()
        con.close()
        message = "Settings saved."
    biz = get_biz(user_id)
    return render_template("settings.html", biz=biz, message=message)

@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    user_id = str(session.get("user_id", ""))
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    cur = con.execute(
        "SELECT user_id, name, service, time, timestamp FROM bookings WHERE user_id = ? ORDER BY id DESC",
        (user_id,)
    )
    rows = [dict(row) for row in cur.fetchall()]
    con.close()
    return render_template("dashboard.html", rows=rows)

def confirm_booking(name, service, time, reply):
    booking = {"service": service, "time": time, "name": name}
    bookings.append(booking)
    print(f"[BOOKING CONFIRMED] {booking}")
    con = sqlite3.connect(DB_FILE)
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
