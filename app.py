from flask import Flask, request, jsonify, render_template
import requests
import os

app = Flask(__name__)

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
                        "You are Filtrex, an expert sales assistant with one clear goal: turn every conversation into a confirmed booking. "
                        "You are professional, confident, warm, and persuasive. You never leave the conversation at a dead end.\n\n"
                        "Follow this approach in every conversation:\n"
                        "1. Greet the user warmly and immediately ask what service or help they are looking for.\n"
                        "2. Ask focused follow-up questions one at a time to understand their need, preferred date/time, and name.\n"
                        "3. Present the service with confidence — highlight its value and how it solves their problem.\n"
                        "4. If the user hesitates or has doubts, acknowledge their concern, offer reassurance, and redirect toward booking.\n"
                        "5. Once you have their service, preferred time, and name, confirm the details and close by telling them their booking is being arranged.\n"
                        "6. Always move the conversation one step closer to a confirmed booking. Never end with a dead-end response.\n\n"
                        "Important rules:\n"
                        "- Always respond in the same language the user is writing in (Arabic or English).\n"
                        "- Ask only one question at a time to keep the conversation natural.\n"
                        "- Be concise, clear, and human — never robotic or overly formal.\n"
                        "- Never say you cannot book — always act as if the booking process is fully within your control."
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

    return jsonify({"reply": reply})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
