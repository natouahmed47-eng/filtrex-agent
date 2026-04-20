from flask import Flask, request, jsonify, render_template
import requests
import os
import json

app = Flask(__name__)

bookings = []

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    user_message = request.json.get("message")

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
                        "- IMPORTANT: Whenever a booking is confirmed, you MUST append the following line at the very end of your reply, on its own line, exactly as shown:\n"
                        "  BOOKING_DATA: {\"service\":\"...\",\"time\":\"...\",\"name\":\"...\"}\n"
                        "- Replace the ... values with the actual service, time, and name collected.\n"
                        "- This line must be valid JSON on a single line.\n"
                        "- Only append this line when the booking is fully confirmed. Never include it in other replies."
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

    if booking:
        bookings.append(booking)
        print(f"[BOOKING CONFIRMED] {booking}")
        return jsonify({"reply": clean_reply, "booking_confirmed": True, "booking": booking})

    return jsonify({"reply": clean_reply})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
