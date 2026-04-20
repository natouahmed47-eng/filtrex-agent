# Filtrex AI

A Flask-based sales-oriented chat assistant that converts conversations into confirmed bookings using OpenAI GPT-4o-mini. Supports multi-user accounts, per-user business settings, persistent SQLite storage, and an admin dashboard.

---

## Features

- Deterministic booking flow (service → time → name → confirm)
- Service validation against per-user allowed services list
- Multi-user support with separate business settings per account
- SQLite storage for users, business settings, and bookings
- Admin dashboard showing confirmed bookings per user
- User registration and login with hashed passwords
- OpenAI GPT-4o-mini fallback for non-booking messages

---

## Required Environment Variables

| Variable | Description |
|---|---|
| `OPENAI_API_KEY` | Your OpenAI API key (required) |
| `SESSION_SECRET` | Secret key for Flask session signing (required in production) |
| `FLASK_DEBUG` | Set to `true` to enable debug mode (default: `false`) |

Set these in Replit's Secrets panel or as environment variables in your deployment environment.

---

## Running Locally

```bash
pip install -r requirements.txt
export OPENAI_API_KEY=your_key_here
export SESSION_SECRET=your_secret_here
python app.py
```

The app starts on `http://0.0.0.0:5000`.

---

## Default Test Accounts

Two accounts are seeded automatically on first run:

| Username | Password |
|---|---|
| `admin` | `123456` |
| `clinic2` | `123456` |

Passwords are hashed on first startup. Change them after your first login via the Settings page.

---

## Project Structure

```
app.py              # Main Flask application
bookings.db         # SQLite database (auto-created)
requirements.txt    # Python dependencies
templates/
  index.html        # Chat UI
  login.html        # Login page
  register.html     # Registration page
  dashboard.html    # Bookings dashboard
  settings.html     # Business settings page
```

---

## Deployment

Set `SESSION_SECRET` and `OPENAI_API_KEY` as secrets before deploying. Do not use the default test credentials in production.
