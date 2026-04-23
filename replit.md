# Filtrex AI — WhatsApp Sales Engine

A Flask-based WhatsApp sales assistant and SaaS platform. Converts conversations into confirmed bookings via UltraMsg + OpenAI GPT-4o-mini. Built as a multi-tenant-ready SaaS with catalog system and admin panel.

## Architecture

- `app.py` — Main Flask app (~2300 lines)
- `bookings.db` — SQLite database (WAL mode)
- `templates/` — Jinja2 HTML templates
  - `admin/` — Admin panel pages (layout, dashboard, catalog, orders, settings)
  - `login.html`, `dashboard.html`, `settings.html`, `register.html`

## Database Tables

| Table | Purpose |
|---|---|
| `clients` | Business tenants (name, type, currency, timezone, UltraMsg config) |
| `catalogs` | Products/services per client (price, sale_price, description, duration, stock) |
| `catalog_aliases` | Multilingual message-matching aliases per catalog item |
| `catalog_options` | Key-value options per catalog item (size, color, etc.) |
| `upsells` | Trigger→upsell item pairs per client |
| `orders` | Confirmed bookings/orders with JSON items list |
| `bookings` | Legacy bookings table (still written for compatibility) |
| `whatsapp_state` | Per-phone conversation state (step, language, known fields) |
| `users` | Admin login accounts |
| `business_settings` | Legacy per-user settings |

## Admin Panel Routes

- `GET /admin/dashboard` — Stats + recent orders
- `GET /admin/catalog` — List catalog items
- `GET/POST /admin/catalog/new` — Create item
- `GET/POST /admin/catalog/<id>/edit` — Edit item + aliases
- `POST /admin/catalog/<id>/delete` — Delete item
- `GET /admin/orders` — All confirmed orders
- `GET/POST /admin/settings` — Client business settings

## WhatsApp Flow

1. Incoming message → `parse_user_message()` (LLM, JSON-only, temperature=0)
2. Merge extracted fields into `whatsapp_state` (service, day, time, name, affirmation, rejection)
3. Regex + catalog alias fallback if LLM returns nothing
4. Step machine: service → day → time → name → confirm
5. Upsell offered once (DB-driven via `upsells` table), rejection tracked
6. On confirm: writes to both `bookings` (legacy) and `orders` (new) tables, notifies admin via WhatsApp

## Key Helpers

- `parse_user_message(msg, lang)` — LLM structured extractor
- `find_catalog_match(client_id, msg, lang)` — Alias-based catalog lookup
- `get_upsell_for_item(client_id, catalog_id)` — DB-driven upsell suggestion
- `save_order(client_id, phone, name, items, scheduled)` — Writes to orders table
- `svc_price(title, lang)` — DB-first, hardcoded fallback
- `svc_benefit(title, lang)` — DB-first (description field), hardcoded fallback
- `build_upsell(svc, lang)` — DB-first upsell, hardcoded fallback

## Multi-Tenant Design

- `CLIENT_ID = 1` constant for MVP (single tenant)
- All catalog, upsell, order queries are scoped by `client_id`
- Future: resolve `CLIENT_ID` from UltraMsg webhook token to support multiple tenants

## Environment Variables

- `OPENAI_API_KEY` — GPT-4o-mini for chat + message parsing
- `ULTRAMSG_INSTANCE` — UltraMsg instance ID
- `ULTRAMSG_TOKEN` — UltraMsg API token
- `ADMIN_WHATSAPP_NUMBER` — Receives booking notifications
- `SESSION_SECRET` — Flask session key (random fallback if not set)

## Running

Development: `python app.py` (port 5000)
Production: `gunicorn --bind=0.0.0.0:5000 app:app`

## Seeded Data (auto on first run)

- Client: Veltrix Dental Clinic (id=1)
- 3 catalog items: تنظيف الأسنان (200 MAD), تبييض الأسنان (350/300 MAD), فحص الأسنان (100 MAD)
- Aliases in ar/en/fr for each item
- Upsells: cleaning→whitening, checkup→cleaning
