# Threat Model

## Project Overview

Filtrex AI is a Flask web application for small businesses to manage AI-assisted appointment booking. It provides public registration and login pages, a public browser chat UI, authenticated dashboard and settings pages, a SQLite-backed user and booking store, and outbound calls to OpenAI's chat completions API. The same application also exposes Twilio/WhatsApp webhook endpoints. Production is assumed to run behind Replit-managed TLS with `NODE_ENV=production`; mockup/dev sandboxes are out of scope unless production reachability is shown.

## Assets

- **User accounts and sessions** — usernames, password hashes, and signed Flask session cookies. Compromise allows impersonation and unauthorized access to business settings and bookings.
- **Booking data** — customer names, appointment times, and associated services. This is business data and may also contain personal data.
- **Business settings** — business names, enabled services, and default language per tenant. Unauthorized modification can tamper with booking behavior and assistant responses.
- **Application secrets** — `SESSION_SECRET` and `OPENAI_API_KEY`. Leakage of the session secret enables cookie forgery; leakage or abuse of the OpenAI key can create direct financial impact and service disruption.
- **Webhook message data** — WhatsApp sender phone numbers and message contents processed at the webhook boundary. This is sensitive communications data.
- **Application logs** — stdout logging includes operational and potentially user-derived values. Logs must not become a secondary disclosure channel for sensitive data.

## Trust Boundaries

- **Browser to Flask server** — all form posts and JSON requests cross from an untrusted client into trusted server code. The server must authenticate, authorize, validate, and rate-limit requests; client behavior cannot be trusted.
- **Public to authenticated boundary** — `/login`, `/register`, `/assistant`, `/chat`, `/whatsapp`, and `/whatsapp-test` are reachable without an authenticated session, while `/dashboard` and `/settings` are intended for logged-in users only.
- **Flask session cookie to server state** — authorization decisions rely on signed session cookie fields such as `logged_in` and `user_id`. Session integrity, lifecycle, and fixation resistance matter.
- **Application to SQLite** — the Flask app has direct write access to `bookings.db`. Query scoping and parameterization must prevent cross-tenant access and tampering.
- **Application to OpenAI API** — server-side requests made with `OPENAI_API_KEY` cross into an external provider. Public endpoints that can trigger these calls must be controlled to prevent key abuse, data leakage, or denial of wallet.
- **Twilio/WhatsApp to webhook endpoint** — `/whatsapp` receives externally supplied POST data and must treat the request origin and payload as untrusted until verified.
- **Production to dev/test boundary** — `/whatsapp-test` and placeholder WhatsApp behavior appear operationally test-oriented, but they are still production-reachable unless explicitly removed or gated.

## Scan Anchors

- **Production entry points:** `app.py` routes `/`, `/assistant`, `/chat`, `/register`, `/login`, `/logout`, `/settings`, `/dashboard`, `/whatsapp`, `/whatsapp-test`.
- **Highest-risk code areas:** authentication/session handling in `app.py`; public AI invocation in `app.py` `/chat`; credential seeding in `init_db()`; DOM rendering in `templates/index.html`; webhook handling and logging in `app.py` WhatsApp helpers.
- **Surface split:** public = login/register/assistant/chat/whatsapp endpoints; authenticated = dashboard/settings/logout; no separate admin role enforcement exists in current code.
- **Usually-ignore areas:** pure styling in HTML templates unless it affects DOM sinks or form security. No separate frontend build output, worker tier, or admin package is present.

## Threat Categories

### Spoofing

Authentication is username/password based and session-backed. The application must prevent unauthorized users from obtaining valid sessions through predictable credentials, weak bootstrap accounts, brute-force-friendly login flows, or forged webhook origins. All protected routes and actions that consume tenant context must require a valid authenticated session, and external callbacks such as Twilio webhooks must verify the sender before trusting request metadata.

### Tampering

Users can modify business settings and create bookings through both form and chat flows. The server must ensure booking state and tenant configuration are only changed by the owning authenticated account, and must not trust client-controlled fields or pre-login session state to determine which tenant data is modified. State-changing requests must resist cross-site request forgery and similar cross-origin triggering where browser cookies could be attached.

### Information Disclosure

The application stores booking records, business configuration, WhatsApp message data, and credentials in a single SQLite file and may emit user-derived content to stdout logs and browser DOM sinks. Responses, logs, and templates must avoid exposing other tenants' data, secrets, or sensitive identifiers. User-controlled or model-controlled text rendered into the DOM must be treated as untrusted to avoid disclosure through XSS or session compromise.

### Denial of Service

Public routes can create database writes and outbound OpenAI requests. The system must prevent unauthenticated or low-cost abuse from exhausting API credits, filling the SQLite database, or tying up worker threads with unbounded external calls. Login and webhook endpoints also need abuse resistance to reduce credential stuffing and spam-driven resource exhaustion.

### Elevation of Privilege

There is no dedicated admin subsystem, but multi-user separation still matters. One tenant must never be able to read or alter another tenant's bookings or settings, and public endpoints must not act with authenticated-user privileges implicitly. All database queries must remain parameterized, and externally supplied content must not be able to execute as script in a privileged browser session.
