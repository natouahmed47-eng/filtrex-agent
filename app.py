from flask import Flask, request, jsonify, render_template, session, redirect, url_for
import requests
import os
import json
import csv
import datetime

app = Flask(__name__)
app.secret_key = os.getenv("SESSION_SECRET", "dev-secret")

bookings = []

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

SERVICE_KEYWORDS = [
    "تنظيف", "تبييض", "فحص", "علاج", "حجز",
    "cleaning", "whitening", "consultation", "checkup", "check-up", "treatment", "appointment"
]

TIME_KEYWORDS = [
    "اليوم", "غداً", "غدًا", "مساء", "صباح",
    "today", "tomorrow", "morning", "evening", "afternoon",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
]

NAME_TRIGGERS = [
    "what name", "your name", "اسم", "الاسم", "confirm the booking under", "booking under"
]

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("username") == "admin" and request.form.get("password") == "123456":
            session["logged_in"] = True
            return redirect(url_for("dashboard"))
        error = "Invalid username or password."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/dashboard")
def dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    rows = []
    csv_file = "bookings.csv"
    if os.path.isfile(csv_file):
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    return render_template("dashboard.html", rows=rows)

@app.route("/chat", methods=["POST"])
def chat():
    user_message = request.json.get("message")

    greetings = ["سلام", "مرحبا", "اهلا", "hello", "hi"]
    clean_msg = user_message.strip().lower()
    if clean_msg in greetings:
        session.clear()

    known_service = session.get("known_service")
    known_time = session.get("known_time")
    known_name = session.get("known_name")
    awaiting_name = session.get("awaiting_name", False)

    msg_lower = user_message.lower()

    if not known_service:
        for kw in SERVICE_KEYWORDS:
            if kw.lower() in msg_lower:
                known_service = user_message.strip()
                break

    if not known_time:
        for kw in TIME_KEYWORDS:
            if kw.lower() in msg_lower:
                known_time = kw
                break

    if awaiting_name and not known_name:
        words = user_message.strip().split()
        if 1 <= len(words) <= 3:
            known_name = user_message.strip()

    session["known_service"] = known_service
    session["known_time"] = known_time
    session["known_name"] = known_name

    user_lower = user_message.strip().lower()

    if user_lower in ["اهلا", "مرحبا", "سلام", "hello", "hi"]:
        session.clear()
        return jsonify({"reply": "أهلاً 👋 كيف أقدر أساعدك اليوم؟"})

    booking_intent = any(word in user_lower for word in ["حجز", "موعد", "book", "appointment"])
    if booking_intent:
        service_keywords = {
            "تنظيف": "تنظيف أسنان",
            "تبييض": "تبييض أسنان",
            "cleaning": "teeth cleaning",
            "whitening": "teeth whitening"
        }
        day_keywords = {
            "غد": "غدًا", "غدا": "غدًا", "غدًا": "غدًا", "بكرة": "غدًا",
            "today": "اليوم", "اليوم": "اليوم"
        }
        period_keywords = {
            "مساء": "مساءً", "evening": "مساءً",
            "صباح": "صباحًا", "morning": "صباحًا"
        }

        detected_service = None
        for key in service_keywords:
            if key in user_lower:
                detected_service = service_keywords[key]
                break

        detected_day = None
        for key in day_keywords:
            if key in user_lower:
                detected_day = day_keywords[key]
                break

        detected_period = None
        for key in period_keywords:
            if key in user_lower:
                detected_period = period_keywords[key]
                break

        if detected_day and detected_period:
            detected_time = f"{detected_day} {detected_period}"
        elif detected_day:
            detected_time = detected_day
        elif detected_period:
            detected_time = detected_period
        else:
            detected_time = None

        session.clear()
        session["known_service"] = detected_service
        session["known_time"] = detected_time
        session["known_name"] = None
        session["awaiting_name"] = False

        if not detected_service:
            return jsonify({"reply": "ما نوع الخدمة التي تريد حجزها؟"})
        elif not detected_time:
            return jsonify({"reply": f"ممتاز! متى تفضل موعد {detected_service}؟"})
        else:
            return jsonify({"reply": f"رائع! ما الاسم الذي تريد تأكيد الحجز باسمه؟"})

    context_str = (
        "\n\nKNOWN DATA (DO NOT ASK AGAIN):\n"
        f"- Service: {known_service or 'UNKNOWN'}\n"
        f"- Time: {known_time or 'UNKNOWN'}\n"
        f"- Name: {known_name or 'UNKNOWN'}\n\n"
        "RULES:\n"
        "- If a field is not UNKNOWN, you must NOT ask for it again.\n"
        "- If only one field is UNKNOWN, ask ONLY for that field.\n"
        "- If all fields are known, immediately confirm the booking.\n"
    )

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
                        "You are Filtrex, a high-performance sales closer. Your primary goal is to convert every conversation into a confirmed booking as quickly as possible.\n\n"
                        "You are confident, persuasive, and proactive. You lead the conversation — the user never leads you.\n\n"
                        "Conversation strategy:\n"
                        "- Immediately guide the user toward booking.\n"
                        "- Ask one focused question at a time.\n"
                        "- Collect key information: service type, preferred time, and name.\n"
                        "- Always assume the user is interested — never act uncertain.\n\n"
                        "Closing behavior:\n"
                        "- Move fast toward commitment.\n"
                        "- Use phrases like:\n"
                        "  'Let's get this booked for you.'\n"
                        "  'I'll secure your slot now.'\n"
                        "  'What name should I confirm the booking under?'\n"
                        "- Once you have enough information, act as if the booking is already in progress.\n\n"
                        "Handling hesitation:\n"
                        "- Acknowledge briefly, then redirect.\n"
                        "- Offer simple choices instead of open-ended questions.\n"
                        "  Example: 'Would you prefer today or tomorrow?'\n\n"
                        "Rules:\n"
                        "- Always reply in the same language as the user (Arabic or English).\n"
                        "- Never give long explanations.\n"
                        "- Never end without asking a forward-moving question.\n"
                        "- Never say you cannot complete the booking.\n\n"
                        "Tone:\n"
                        "- Human, confident, direct, and helpful.\n"
                        "- Not robotic. Not passive.\n\n"
                        "Goal:\n"
                        "Convert → Confirm → Close.\n\n"
                        "Additional rules:\n"
                        "- Never add random or irrelevant words.\n"
                        "- Never mention 'AI' or 'artificial intelligence' in replies.\n"
                        "- Responses must sound natural, human, and professional.\n"
                        "- Avoid repetition or unnatural phrasing.\n"
                        "- Every sentence must be clear and intentional.\n\n"
                        "Booking confirmation:\n"
                        "- When you have enough information (service, time, and name), summarize the booking clearly and confirm it.\n"
                        "- Example: 'Perfect, I've got you down for [service] at [time] under the name [name]. I'm finalizing your booking now.'\n"
                        "- Always try to reach this confirmation stage as quickly as possible.\n"
                        "- STRICT MANDATORY RULE: When confirming a booking, you MUST ALWAYS append the following line EXACTLY at the very end of your reply, on its own line:\n"
                        "  BOOKING_DATA: {\"service\":\"...\",\"time\":\"...\",\"name\":\"...\"}\n"
                        "- Replace the ... values with the actual service, time, and name collected.\n"
                        "- This is mandatory. If you do not include this line, the booking will NOT be saved.\n"
                        "- The JSON must be valid and on a single line.\n"
                        "- Do not skip it under any condition.\n"
                        "- Only append this line when the booking is fully confirmed. Never include it in other replies.\n\n"
                        "Conversation control rules:\n"
                        "- You must strictly follow this order:\n"
                        "  1. Identify service\n"
                        "  2. Identify preferred time\n"
                        "  3. Identify name\n"
                        "  4. Confirm booking\n"
                        "- Never jump backward in the flow.\n"
                        "- Never ask about something already known.\n"
                        "- If two pieces of information are already known, immediately ask for the missing one.\n"
                        "- If all three are known, immediately confirm the booking (no extra questions).\n"
                        "- Always keep the conversation moving forward step-by-step.\n"
                        "- Do not restart or rephrase previous steps.\n"
                        "- Do not ask open-ended questions if a specific question is possible.\n\n"
                        "Goal:\n"
                        "Fastest path to booking confirmation with zero repetition.\n\n"
                        "Confirmation tone:\n"
                        "- When confirming a booking, always use a strong, final, action-oriented tone.\n"
                        "- Examples of correct confirmations:\n"
                        "  'Perfect 👌 Your booking for [service] at [time] under the name [name] is now confirmed.'\n"
                        "  'Done ✅ I've secured your [service] appointment for [time] under the name [name].'\n"
                        "  'All set. Your appointment is confirmed for [service] at [time].'\n"
                        "- Do not sound hesitant.\n"
                        "- Do not ask any more questions after confirmation.\n"
                        "- Make it feel like the booking is already secured.\n\n"
                        "Strict data rules:\n"
                        "- Never assume or invent service, time, or name.\n"
                        "- Only use information explicitly provided by the user in the current conversation.\n"
                        "- If any required information is missing, ask for it.\n"
                        "- If no information is known, start from step 1 (ask for service).\n"
                        "- After a session reset, behave as if this is a completely new user with no prior data.\n\n"
                        "Conversation start rule:\n"
                        "- If the user message is only a greeting (e.g., 'سلام', 'مرحبا', 'hello', 'hi'), do NOT proceed with the booking flow.\n"
                        "- Instead, respond with a simple, friendly greeting and ask what service they are looking for.\n"
                        "- Examples:\n"
                        "  'أهلاً 👋 كيف أقدر أساعدك اليوم؟'\n"
                        "  'Hi 👋 What service are you looking to book?'\n"
                        "- Do not mention any service, time, or name unless the user provides it.\n"
                        "- Do not skip directly to booking steps on greetings.\n\n"
                        "Intent override rule:\n"
                        "- If the user message contains a clear intent (e.g. booking, service request, time, or name), you MUST ignore greeting behavior.\n"
                        "- NEVER greet again after the first greeting.\n"
                        "- If the user says something like 'I want to book', immediately proceed to step 1 (identify service).\n"
                        "- Do NOT restart the conversation.\n"
                        "- Do NOT say 'How can I help you?' more than once at the very beginning.\n"
                        "- This rule overrides the greeting rule completely."
                        + context_str
                    )
                },
                {
                    "role": "user",
                    "content": user_message
                }
            ]
        }
    )

    reply = response.json()["choices"][0]["message"]["content"]

    booking = None
    clean_lines = []
    for line in reply.splitlines():
        if line.strip().startswith("BOOKING_DATA:"):
            try:
                json_str = line.strip()[len("BOOKING_DATA:"):].strip()
                booking = json.loads(json_str)
            except Exception:
                pass
        else:
            clean_lines.append(line)

    clean_reply = "\n".join(clean_lines).strip()

    reply_lower = clean_reply.lower()
    if any(trigger in reply_lower for trigger in NAME_TRIGGERS) and not known_name:
        session["awaiting_name"] = True
    else:
        session["awaiting_name"] = awaiting_name

    if booking:
        bookings.append(booking)
        print(f"[BOOKING CONFIRMED] {booking}")

        csv_file = "bookings.csv"
        file_exists = os.path.isfile(csv_file)
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "name", "service", "time"])
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "name": booking.get("name", ""),
                "service": booking.get("service", ""),
                "time": booking.get("time", "")
            })

        session.clear()
        return jsonify({"reply": clean_reply, "booking_confirmed": True, "booking": booking})

    return jsonify({"reply": clean_reply})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
