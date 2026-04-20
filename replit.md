# Filtrex AI

A lightweight Flask API that acts as an AI assistant to convert conversations into bookings, powered by OpenAI's GPT-4o-mini model.

## Project Structure

- `app.py` — Main Flask application with `/` and `/chat` endpoints
- `requirements.txt` — Python dependencies (flask, requests)

## Setup

- Python 3.12
- Dependencies: flask, requests, gunicorn
- Runs on port 5000 (host 0.0.0.0)

## Environment Variables

- `OPENAI_API_KEY` — Required. OpenAI API key for GPT-4o-mini access.

## API Endpoints

- `GET /` — Health check, returns "Filtrex AI is running"
- `POST /chat` — Accepts `{"message": "..."}` and returns `{"reply": "..."}` from the AI assistant

## Running

Development: `python app.py`
Production: `gunicorn --bind=0.0.0.0:5000 app:app`
