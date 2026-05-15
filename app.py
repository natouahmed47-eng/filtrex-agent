from flask import Flask, request, jsonify, render_template, render_template_string, session, redirect, url_for, flash, g
from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()

ai_client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url=os.getenv("OPENROUTER_BASE_URL")
)
from werkzeug.security import generate_password_hash, check_password_hash 
from werkzeug.utils import secure_filename
import requests
import os
import json 
import sqlite3
import datetime
import random
import sys
import warnings
import difflib
from urllib.parse import quote_plus
from services.product_intelligence import detect_product_type
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(errors="replace")

if load_dotenv:
    load_dotenv()
else:
    warnings.warn("python-dotenv is not installed; .env file was not loaded", RuntimeWarning)

# Meta WhatsApp Cloud API configuration
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN")

print(f"META_ACCESS_TOKEN: {'loaded' if META_ACCESS_TOKEN else 'missing'}")
print(f"META_PHONE_NUMBER_ID: {'loaded' if META_PHONE_NUMBER_ID else 'missing'}")
print(f"VERIFY_TOKEN: {'loaded' if VERIFY_TOKEN else 'missing'}")

for env_name, env_value in {
    "META_ACCESS_TOKEN": META_ACCESS_TOKEN,
    "META_PHONE_NUMBER_ID": META_PHONE_NUMBER_ID,
    "VERIFY_TOKEN": VERIFY_TOKEN,
}.items():
    if not env_value:
        warnings.warn(f"{env_name} is missing; related WhatsApp features may not work", RuntimeWarning)

DEFAULT_CLIENT_ID = int(os.getenv("DEFAULT_CLIENT_ID", "1"))
print(f"[STARTUP] DEFAULT_CLIENT_ID={DEFAULT_CLIENT_ID}")


def save_catalog_image(file_storage):
    if not file_storage or not file_storage.filename:
        return ""
    upload_dir = os.path.join(app.root_path, "static", "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    filename = secure_filename(file_storage.filename)
    stem, ext = os.path.splitext(filename)
    filename = f"{stem}_{int(datetime.datetime.now().timestamp())}{ext}"
    file_storage.save(os.path.join(upload_dir, filename))
    return url_for("static", filename=f"uploads/{filename}", _external=True)


def save_catalog_aliases(con, catalog_id, aliases_text):
    aliases = []
    for alias in (aliases_text or "").replace("\n", ",").split(","):
        alias = alias.strip()
        if alias and alias not in aliases:
            aliases.append(alias)
    con.execute("DELETE FROM catalog_aliases WHERE catalog_id=?", (catalog_id,))
    for alias in aliases:
        con.execute(
            "INSERT INTO catalog_aliases (catalog_id, lang, alias) VALUES (?, 'any', ?)",
            (catalog_id, alias)
        )


AI_CATALOG_FIELDS = [
    ("ai_category", "TEXT"),
    ("ai_subcategory", "TEXT"),
    ("ai_brand", "TEXT"),
    ("ai_product_identity", "TEXT"),
    ("ai_style", "TEXT"),
    ("ai_gender", "TEXT"),
    ("ai_luxury_level", "TEXT"),
    ("ai_features", "TEXT"),
    ("ai_usage_contexts", "TEXT"),
    ("ai_semantic_tags", "TEXT"),
    ("ai_searchable_intents", "TEXT"),
    ("ai_target_customer", "TEXT"),
    ("ai_tone", "TEXT"),
    ("ai_search_text", "TEXT"),
    ("ai_tags", "TEXT"),
    ("ai_intent", "TEXT"),
    ("ai_metadata", "TEXT"),
    ("ai_embedding_text", "TEXT"),
    ("ai_embedding", "TEXT"),
    ("ai_confidence", "REAL"),
    ("ai_analysis_source", "TEXT"),
]


def _json_list(value):
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except (TypeError, ValueError):
            pass
        return [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]
    return []


def _json_object(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _price_segment(price):
    try:
        amount = float(price or 0)
    except (TypeError, ValueError):
        amount = 0.0
    if amount <= 0:
        return "unknown"
    if amount < 50:
        return "budget"
    if amount < 200:
        return "mid"
    if amount < 600:
        return "mid_high"
    return "premium"


def build_ai_metadata(title="", price="", category="", keywords="", intelligence=None):
    intelligence = intelligence or {}
    existing = _json_object(intelligence.get("ai_metadata") or intelligence.get("metadata"))
    semantic_keywords = []
    semantic_keywords.extend(_json_list(keywords))
    semantic_keywords.extend(_json_list(intelligence.get("ai_tags")))
    semantic_keywords.extend(_json_list(intelligence.get("ai_semantic_tags")))
    semantic_keywords.extend(_json_list(intelligence.get("ai_searchable_intents")))
    semantic_keywords.extend(_json_list(intelligence.get("ai_features")))
    metadata = {
        "category": str(existing.get("category") or intelligence.get("ai_category") or category or ""),
        "subcategory": str(existing.get("subcategory") or intelligence.get("ai_subcategory") or ""),
        "brand": str(existing.get("brand") or intelligence.get("ai_brand") or ""),
        "identity": str(existing.get("identity") or intelligence.get("ai_product_identity") or title or ""),
        "style": str(existing.get("style") or intelligence.get("ai_style") or ""),
        "intent": _json_list(existing.get("intent") or intelligence.get("ai_searchable_intents") or intelligence.get("ai_intent")),
        "audience": _json_list(existing.get("audience") or intelligence.get("ai_target_customer")),
        "usage": _json_list(existing.get("usage") or intelligence.get("ai_usage_contexts")),
        "luxury_level": str(existing.get("luxury_level") or intelligence.get("ai_luxury_level") or ""),
        "price_segment": str(existing.get("price_segment") or _price_segment(price)),
        "semantic_keywords": list(dict.fromkeys([term for term in semantic_keywords if term])),
        "commerce_context": {
            "sales_tone": str(existing.get("sales_tone") or intelligence.get("ai_tone") or ""),
            "analysis_source": str(existing.get("analysis_source") or intelligence.get("ai_analysis_source") or ""),
            "future_visual_embeddings": {
                "image_understanding": False,
                "clip_similarity": False,
                "visual_embedding": None
            }
        }
    }
    return metadata


def build_ai_search_text(intelligence, keywords=""):
    parts = [
        intelligence.get("ai_category"),
        intelligence.get("ai_subcategory"),
        intelligence.get("ai_brand"),
        intelligence.get("ai_product_identity"),
        intelligence.get("ai_style"),
        intelligence.get("ai_gender"),
        intelligence.get("ai_luxury_level"),
        intelligence.get("ai_target_customer"),
        intelligence.get("ai_tone"),
        keywords,
    ]
    parts.extend(_json_list(intelligence.get("ai_features")))
    parts.extend(_json_list(intelligence.get("ai_usage_contexts")))
    parts.extend(_json_list(intelligence.get("ai_semantic_tags")))
    parts.extend(_json_list(intelligence.get("ai_searchable_intents")))
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def build_product_embedding_text(title="", description="", category="", intelligence=None):
    intelligence = intelligence or {}
    parts = [
        title,
        category,
        description,
        intelligence.get("ai_category"),
        intelligence.get("ai_subcategory"),
        intelligence.get("ai_brand"),
        intelligence.get("ai_product_identity"),
        intelligence.get("ai_style"),
        intelligence.get("ai_luxury_level"),
        intelligence.get("ai_target_customer"),
        intelligence.get("ai_search_text"),
    ]
    parts.extend(_json_list(intelligence.get("ai_features")))
    parts.extend(_json_list(intelligence.get("ai_usage_contexts")))
    parts.extend(_json_list(intelligence.get("ai_semantic_tags")))
    parts.extend(_json_list(intelligence.get("ai_searchable_intents")))
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())




def build_ai_embedding_text(title="", description="", category="", keywords="", intelligence=None):
    intelligence = intelligence or {}
    parts = [
        title,
        description,
        keywords,
        category,
        intelligence.get("ai_category"),
        intelligence.get("ai_tags"),
        intelligence.get("ai_intent"),
        json.dumps(_json_object(intelligence.get("ai_metadata")), ensure_ascii=False),
        intelligence.get("ai_search_text"),
    ]
    parts.extend(_json_list(intelligence.get("ai_usage_contexts")))
    parts.extend(_json_list(intelligence.get("ai_semantic_tags")))
    parts.extend(_json_list(intelligence.get("ai_searchable_intents")))
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def create_ai_embedding(text_value):
    if not ai_client or not (text_value or "").strip():
        return []
    model = os.getenv("OPENAI_EMBEDDING_MODEL", os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"))
    try:
        response = ai_client.embeddings.create(
            model=model,
            input=(text_value or "")[:8000]
        )
        embedding = response.data[0].embedding
        return [float(value) for value in embedding]
    except Exception as exc:
        print(f"[AI_EMBEDDING_ERROR] {repr(exc)}")
        return []


def _parse_embedding(value):
    if isinstance(value, list):
        return [float(item) for item in value if isinstance(item, (int, float))]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [float(item) for item in parsed if isinstance(item, (int, float))]
        except (TypeError, ValueError):
            return []
    return []


def cosine_similarity(vector_a, vector_b):
    if not vector_a or not vector_b or len(vector_a) != len(vector_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vector_a, vector_b))
    norm_a = sum(a * a for a in vector_a) ** 0.5
    norm_b = sum(b * b for b in vector_b) ** 0.5
    if not norm_a or not norm_b:
        return 0.0
    return dot / (norm_a * norm_b)


def _is_ambiguous_product_name(title):
    compact = "".join(ch for ch in (title or "") if ch.isalnum())
    words = [word for word in (title or "").replace("-", " ").replace("_", " ").split() if word.strip()]
    generic_words = {"pro", "max", "plus", "elite", "nova", "reef", "air", "x", "ultra", "mini"}
    return bool(
        title
        and len(words) <= 2
        and len(compact) <= 14
        and (
            any(ch.isdigit() for ch in compact)
            or compact.lower() in generic_words
            or any(word.lower() in generic_words for word in words)
        )
    )


def _normalize_product_intelligence(intelligence, keywords="", analysis_source="text"):
    intelligence = intelligence or {}
    normalized = {
        "ai_category": str(intelligence.get("ai_category") or intelligence.get("category") or ""),
        "ai_subcategory": str(intelligence.get("ai_subcategory") or intelligence.get("subcategory") or ""),
        "ai_brand": str(intelligence.get("ai_brand") or intelligence.get("brand") or ""),
        "ai_product_identity": str(intelligence.get("ai_product_identity") or intelligence.get("product_identity") or ""),
        "ai_style": str(intelligence.get("ai_style") or intelligence.get("product_style") or intelligence.get("style") or ""),
        "ai_gender": str(intelligence.get("ai_gender") or intelligence.get("gender") or ""),
        "ai_luxury_level": str(intelligence.get("ai_luxury_level") or intelligence.get("luxury_level") or intelligence.get("price_level") or ""),
        "ai_features": _json_list(intelligence.get("ai_features") or intelligence.get("features")),
        "ai_usage_contexts": _json_list(intelligence.get("ai_usage_contexts") or intelligence.get("usage_contexts")),
        "ai_semantic_tags": _json_list(intelligence.get("ai_semantic_tags") or intelligence.get("semantic_tags")),
        "ai_searchable_intents": _json_list(intelligence.get("ai_searchable_intents") or intelligence.get("searchable_intents")),
        "ai_tags": str(intelligence.get("ai_tags") or intelligence.get("tags") or ""),
        "ai_intent": str(intelligence.get("ai_intent") or intelligence.get("buyer_intent") or intelligence.get("intent") or ""),
        "ai_metadata": _json_object(intelligence.get("ai_metadata") or intelligence.get("metadata")),
        "ai_target_customer": str(intelligence.get("ai_target_customer") or intelligence.get("target_customer") or ""),
        "ai_tone": str(intelligence.get("ai_tone") or intelligence.get("tone") or ""),
        "ai_analysis_source": str(intelligence.get("ai_analysis_source") or analysis_source or ""),
    }
    try:
        confidence = float(intelligence.get("ai_confidence") or intelligence.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    normalized["ai_confidence"] = max(0.0, min(1.0, confidence))
    normalized["ai_search_text"] = build_ai_search_text(normalized, keywords)
    normalized["ai_embedding_text"] = build_ai_embedding_text(keywords=keywords, intelligence=normalized)
    normalized["ai_embedding"] = _parse_embedding(intelligence.get("ai_embedding"))
    return normalized


def analyze_catalog_product_with_ai(title, description="", image_url="", price="", category="", keywords="", item_type="product", product_hint=""):
    if not ai_client:
        return None, "missing_ai_client"
    detected_type = detect_product_type(title)
    analysis_source = "vision_text" if image_url else "text"
    user_content = [
        {
            "type": "text",
            "text": json.dumps({
                "title": title,
                "detected_product_type": detected_type,
                "description": description,
                "image_url": image_url,
                "price": price,
                "category": category,
                "keywords": keywords,
                "type": item_type,
                "product_hint": product_hint,
                "pipeline": [
                    "product name",
                    "vision analysis when image exists",
                    "AI semantic analysis",
                    "metadata generation",
                    "semantic indexing",
                    "recommendation ready"
                ],
            }, ensure_ascii=False)
        }
    ]
    if image_url:
        user_content.append({"type": "image_url", "image_url": {"url": image_url}})
    model = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
    try:
        completion = ai_client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a Product Intelligence Engine for commerce catalogs. "
                        "You are an elite ecommerce AI. Analyze products intelligently from the product name even when description or image is missing. "
                        "Understand the real-world meaning of the product name and detected product type. "
                        "Generate professional product meaning, SEO keywords, buyer intent, use cases, emotional commerce language, and category detection. "
                        "Do not hallucinate random categories; when uncertain, use broad safe categories and keep confidence moderate. "
                        "Use image evidence only for visible product type, broad category, usage, style, and visible features. "
                        "Return only valid JSON with keys: ai_category, ai_subcategory, ai_brand, ai_product_identity, ai_style, ai_gender, ai_luxury_level, ai_features, ai_usage_contexts, ai_semantic_tags, ai_searchable_intents, ai_tags, ai_intent, ai_metadata, ai_target_customer, ai_tone, ai_confidence. "
                        "Array fields must contain concise lowercase semantic labels in Arabic and/or English when useful. "
                        "ai_searchable_intents should express customer intents like daily use, gift, work, travel, budget, premium, gaming, camera, comfort, formal, casual. "
                        "ai_tags should be comma-separated SEO and WhatsApp search terms. "
                        "ai_intent should summarize likely buyer intent. "
                        "ai_confidence must be a number from 0 to 1 based on evidence quality."
                    )
                },
                {"role": "user", "content": user_content}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        intelligence = json.loads(completion.choices[0].message.content or "{}")
    except Exception as exc:
        print(f"[AI_PRODUCT_ANALYZER_ERROR] {repr(exc)}")
        return None, "ai_error"

    intelligence = _normalize_product_intelligence(intelligence, keywords, analysis_source)
    intelligence["ai_metadata"] = build_ai_metadata(
        title=title,
        price=price,
        category=category,
        keywords=keywords,
        intelligence=intelligence
    )
    intelligence["ai_embedding_text"] = build_ai_embedding_text(
        title=title,
        description=description,
        category=category,
        keywords=keywords,
        intelligence=intelligence
    )
    intelligence["ai_embedding"] = create_ai_embedding(intelligence["ai_embedding_text"])
    return intelligence, None


def save_catalog_ai_intelligence(con, catalog_id, intelligence):
    if not intelligence:
        return
    con.execute("""
        UPDATE catalogs
        SET ai_category=?,
            ai_subcategory=?,
            ai_brand=?,
            ai_product_identity=?,
            ai_style=?,
            ai_gender=?,
            ai_luxury_level=?,
            ai_features=?,
            ai_usage_contexts=?,
            ai_semantic_tags=?,
            ai_searchable_intents=?,
            ai_target_customer=?,
            ai_tone=?,
            ai_search_text=?,
            ai_tags=?,
            ai_intent=?,
            ai_metadata=?,
            ai_embedding_text=?,
            ai_embedding=?,
            ai_confidence=?,
            ai_analysis_source=?
        WHERE id=?
    """, (
        str(intelligence.get("ai_category") or ""),
        str(intelligence.get("ai_subcategory") or ""),
        str(intelligence.get("ai_brand") or ""),
        str(intelligence.get("ai_product_identity") or ""),
        str(intelligence.get("ai_style") or ""),
        str(intelligence.get("ai_gender") or ""),
        str(intelligence.get("ai_luxury_level") or ""),
        json.dumps(_json_list(intelligence.get("ai_features")), ensure_ascii=False),
        json.dumps(_json_list(intelligence.get("ai_usage_contexts")), ensure_ascii=False),
        json.dumps(_json_list(intelligence.get("ai_semantic_tags")), ensure_ascii=False),
        json.dumps(_json_list(intelligence.get("ai_searchable_intents")), ensure_ascii=False),
        str(intelligence.get("ai_target_customer") or ""),
        str(intelligence.get("ai_tone") or ""),
        str(intelligence.get("ai_search_text") or ""),
        str(intelligence.get("ai_tags") or ""),
        str(intelligence.get("ai_intent") or ""),
        json.dumps(_json_object(intelligence.get("ai_metadata")), ensure_ascii=False),
        str(intelligence.get("ai_embedding_text") or ""),
        json.dumps(_parse_embedding(intelligence.get("ai_embedding")), ensure_ascii=False),
        float(intelligence.get("ai_confidence") or 0),
        str(intelligence.get("ai_analysis_source") or ""),
        catalog_id,
    ))


def meta_send_message(to_phone, message_text):
    """Send a WhatsApp message using Meta Cloud API.
    
    Args:
        to_phone: Recipient phone number (with country code, e.g., 1234567890)
        message_text: Message text to send
    
    Returns:
        requests.Response object or None on error
    """
    message_text = _sanitize_outgoing_text(message_text)
    if not META_ACCESS_TOKEN or not META_PHONE_NUMBER_ID:
        print("[META_SEND_ERROR] Missing META_ACCESS_TOKEN or META_PHONE_NUMBER_ID")
        return None
    
    url = f"https://graph.facebook.com/v18.0/{META_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_phone,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message_text,
        },
    }
    
    print(f"[META_SEND] to={to_phone!r} body={message_text!r}")
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        print(f"[META_SEND] status={resp.status_code} response={resp.text!r}")
        return resp
    except Exception as e:
        print(f"[META_SEND_ERROR] {repr(e)}")
        return None


def meta_send_image(to_phone, image_url, caption):
    if not META_ACCESS_TOKEN or not META_PHONE_NUMBER_ID:
        print("[META_SEND_IMAGE_ERROR] Missing META_ACCESS_TOKEN or META_PHONE_NUMBER_ID")
        return None

    url = f"https://graph.facebook.com/v18.0/{META_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_phone,
        "type": "image",
        "image": {
            "link": image_url,
            "caption": caption,
        },
    }

    print(f"[META_SEND_IMAGE] to={to_phone!r} image_url={image_url!r} caption={caption!r}")
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        print(f"[META_SEND_IMAGE] status={resp.status_code} response={resp.text!r}")
        return resp
    except Exception as e:
        print(f"[META_SEND_IMAGE_ERROR] {repr(e)}")
        return None


def _format_raw_product_reply(product):
    if not isinstance(product, dict) or not product.get("title"):
        return ""
    title = str(product.get("title") or "").strip()
    price_value = product.get("sale_price") if product.get("sale_price") not in (None, "") else product.get("price")
    currency_value = str(product.get("currency") or "MRU").strip()
    try:
        price = f"{float(price_value):g} {currency_value}".strip() if price_value not in (None, "") else ""
    except (TypeError, ValueError):
        price = f"{price_value} {currency_value}".strip() if price_value not in (None, "") else ""
    description = str(product.get("description") or "").strip()
    lines = [f"🛍️ {title}"]
    if price:
        lines.append(f"السعر: {price}")
    if description:
        lines.append(f"الوصف: {description}")
    return "\n\n".join(lines).strip()


def _sanitize_outgoing_text(message_text):
    if not isinstance(message_text, str):
        return message_text
    blocked_prefixes = (
        "category:",
        "type:",
        "price:",
        "raw price:",
        "description:",
        "raw description:",
        "sale_price:",
        "keywords:",
        "ai_metadata:",
    )
    cleaned_lines = []
    for line in message_text.splitlines():
        normalized = line.strip().lower()
        if any(prefix in normalized for prefix in blocked_prefixes):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def normalize_phone_number(raw_phone):
    """Extract and normalize phone number to digits only.
    
    Args:
        raw_phone: Raw phone number (may contain +, spaces, etc.)
    
    Returns:
        Phone number as digits only (e.g., '1234567890')
    """
    import re
    digits = re.sub(r'\D', '', str(raw_phone))
    return digits


app = Flask(__name__)
print("META WHATSAPP CLOUD API LIVE")

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

# ── White-label: resolve branding once per request ────────────────────────────
_SKIP_BRANDING_PREFIXES = ("/static/", "/webhook", "/whatsapp")

@app.before_request
def _resolve_branding():
    if any(request.path.startswith(p) for p in _SKIP_BRANDING_PREFIXES):
        g.branding = {"brand_name": "Filtrex AI", "logo_url": None,
                      "primary_color": "#4f46e5", "white_label_enabled": 0}
        return

    host = request.host.split(":")[0].lower()
    _local_hosts = {"localhost", "127.0.0.1", "0.0.0.0"}
    _replit_suffixes = (".replit.dev", ".repl.co", ".replit.app")

    g.branding = {"brand_name": "Filtrex AI", "logo_url": None,
                  "primary_color": "#4f46e5", "white_label_enabled": 0}

    # 1. Custom-domain match (strict — only non-local, non-Replit hosts)
    is_custom_host = (
        host not in _local_hosts
        and not any(host.endswith(s) for s in _replit_suffixes)
    )
    if is_custom_host:
        _con = sqlite3.connect("bookings.db", timeout=10)
        _con.row_factory = sqlite3.Row
        try:
            _row = _con.execute(
                "SELECT * FROM clients WHERE custom_domain=? AND white_label_enabled=1",
                (host,)
            ).fetchone()
        finally:
            _con.close()
        if _row:
            g.domain_client_id = _row["id"]
            g.branding = {
                "brand_name":          _row["brand_name"]    or "Filtrex AI",
                "logo_url":            _row["logo_url"]      or None,
                "primary_color":       _row["primary_color"] or "#4f46e5",
                "white_label_enabled": 1,
            }
            print(f"[DOMAIN_MATCH] host={host!r} client_id={_row['id']}")
            print(f"[WHITE_LABEL_APPLIED] client_id={_row['id']} brand={g.branding['brand_name']!r}")
            return

    # 2. Authenticated session — load branding for that client
    cid = session.get("client_id")
    if cid:
        _con = sqlite3.connect("bookings.db", timeout=10)
        _con.row_factory = sqlite3.Row
        try:
            _row = _con.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
        finally:
            _con.close()
        if _row and _row["white_label_enabled"]:
            g.branding = {
                "brand_name":          _row["brand_name"]    or "Filtrex AI",
                "logo_url":            _row["logo_url"]      or None,
                "primary_color":       _row["primary_color"] or "#4f46e5",
                "white_label_enabled": 1,
            }
            print(f"[BRAND_LOADED] client_id={cid} brand={g.branding['brand_name']!r}")


@app.context_processor
def _inject_branding():
    return {"branding": getattr(g, "branding", {"brand_name": "Filtrex AI",
                                                  "logo_url": None,
                                                  "primary_color": "#4f46e5",
                                                  "white_label_enabled": 0})}


# ═══════════════════════════════════════════════════════════════
# TRANSLATION SYSTEM
# ═══════════════════════════════════════════════════════════════

TRANSLATIONS = {
    "en": {
        "nav_dashboard":    "Dashboard",
        "nav_catalog":      "Catalog",
        "nav_orders":       "Orders",
        "nav_whatsapp":     "WhatsApp",
        "nav_billing":      "Billing",
        "nav_branding":     "Branding",
        "nav_integrations": "Integrations",
        "nav_settings":     "Settings",
        "nav_logout":       "Logout",
    },
    "ar": {
        "nav_dashboard":    "لوحة التحكم",
        "nav_catalog":      "الكتالوج",
        "nav_orders":       "الطلبات",
        "nav_whatsapp":     "واتساب",
        "nav_billing":      "الفواتير",
        "nav_branding":     "العلامة التجارية",
        "nav_integrations": "التكاملات",
        "nav_settings":     "الإعدادات",
        "nav_logout":       "تسجيل الخروج",
    },
}


def t(key, lang="en"):
    """Return translated string for key in given language, falling back to English."""
    lang = lang if lang in TRANSLATIONS else "en"
    return TRANSLATIONS[lang].get(key) or TRANSLATIONS["en"].get(key, key)


@app.context_processor
def _inject_lang():
    """Inject lang and t() into every template."""
    cid = session.get("client_id")
    lang = "en"
    if cid:
        _con = sqlite3.connect("bookings.db", timeout=10)
        _con.row_factory = sqlite3.Row
        try:
            _row = _con.execute(
                "SELECT default_language FROM clients WHERE id=?", (cid,)
            ).fetchone()
        finally:
            _con.close()
        if _row:
            lang = _row["default_language"] or "en"
    return {"lang": lang, "t": t}


@app.context_processor
def _inject_trial_info():
    """Inject trial_info into every admin template so the banner shows everywhere."""
    try:
        cid = session.get("client_id")
        if cid:
            _client = get_client(cid)
            return {"trial_info": get_trial_status(_client)}
    except Exception:
        pass
    return {"trial_info": None}


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
        con.execute("""
            CREATE TABLE IF NOT EXISTS wa_connect_tokens (
                token      TEXT PRIMARY KEY,
                client_id  INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used       INTEGER DEFAULT 0
            )
        """)
        con.execute("INSERT OR IGNORE INTO users (id, username, password) VALUES (1, 'admin', '123456')")
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
        if "completed" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN completed INTEGER DEFAULT 0")
            print("[DB] migration: added completed")
        if "msg_intent" not in cols:
            con.execute("ALTER TABLE whatsapp_state ADD COLUMN msg_intent TEXT DEFAULT ''")
            print("[DB] migration: added msg_intent")
        con.commit()
    finally:
        con.close()

_migrate_whatsapp_state()

# ── SAAS SCHEMA MIGRATION ─────────────────────────────────────────────────────

def _migrate_saas():
    con = get_db_connection()
    try:
        # ── STEP 1: clients ───────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                name              TEXT NOT NULL,
                business_type     TEXT NOT NULL DEFAULT '',
                default_language  TEXT DEFAULT 'ar',
                currency          TEXT DEFAULT 'SAR',
                timezone          TEXT DEFAULT 'Africa/Nouakchott',
                admin_whatsapp    TEXT,
                is_active         INTEGER DEFAULT 1,
                created_at        TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # ── STEP 2: catalogs ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalogs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id    INTEGER NOT NULL,
                title        TEXT NOT NULL,
                type         TEXT NOT NULL DEFAULT 'service',
                price        REAL NOT NULL DEFAULT 0,
                currency     TEXT,
                sale_price   REAL,
                category     TEXT,
                image_url    TEXT,
                description  TEXT,
                duration_min INTEGER,
                stock_qty    INTEGER,
                is_active    INTEGER DEFAULT 1,
                created_at   TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── STEP 3: catalog_aliases (lang before alias per spec) ─────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog_aliases (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                catalog_id INTEGER NOT NULL,
                lang       TEXT NOT NULL,
                alias      TEXT NOT NULL
            )
        """)

        # ── STEP 4: catalog_options (spec columns) ────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog_options (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                catalog_id   INTEGER NOT NULL,
                option_type  TEXT NOT NULL,
                option_value TEXT NOT NULL,
                extra_price  REAL DEFAULT 0
            )
        """)

        # ── STEP 5: upsells (spec columns) ───────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS upsells (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id          INTEGER NOT NULL,
                source_catalog_id  INTEGER NOT NULL,
                target_catalog_id  INTEGER NOT NULL,
                priority           INTEGER DEFAULT 1
            )
        """)

        # ── STEP 6: conversations ─────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id             INTEGER NOT NULL,
                phone                 TEXT NOT NULL,
                lang                  TEXT DEFAULT '',
                current_step          TEXT DEFAULT 'greeting',
                known_catalog_ids_json TEXT DEFAULT '[]',
                known_day             TEXT,
                known_time            TEXT,
                known_name            TEXT,
                upsell_offered        INTEGER DEFAULT 0,
                upsell_rejected       INTEGER DEFAULT 0,
                updated_at            TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(client_id, phone)
            )
        """)

        # ── STEP 7: bookings_or_orders ────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS bookings_or_orders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id     INTEGER NOT NULL,
                phone         TEXT NOT NULL,
                customer_name TEXT,
                items_json    TEXT NOT NULL DEFAULT '[]',
                day           TEXT,
                time          TEXT,
                total_price   REAL DEFAULT 0,
                status        TEXT DEFAULT 'new',
                created_at    TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── Legacy orders table (keep for backward compat) ────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER NOT NULL,
                phone      TEXT,
                name       TEXT,
                items      TEXT,
                scheduled  TEXT,
                status     TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── Column migrations for existing tables ─────────────────────────
        # clients: add whatsapp_connected + onboarding_step columns if missing
        _cli_cols = [r[1] for r in con.execute("PRAGMA table_info(clients)").fetchall()]
        if "whatsapp_connected" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_connected INTEGER DEFAULT 0")
            con.commit()
            print("[DB] migration: added whatsapp_connected")
        if "onboarding_step" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN onboarding_step INTEGER DEFAULT 0")
            con.execute("UPDATE clients SET onboarding_step=5 WHERE id=1")
            con.commit()
            print("[DB] migration: added onboarding_step, existing client=1 marked done (step=5)")
        else:
            con.execute("UPDATE clients SET onboarding_step=5 WHERE onboarding_step=3")
            con.commit()
        if "white_label_enabled" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN brand_name          TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN logo_url            TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN primary_color       TEXT DEFAULT '#4f46e5'")
            con.execute("ALTER TABLE clients ADD COLUMN custom_domain       TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN white_label_enabled INTEGER DEFAULT 0")
            con.commit()
            print("[WHITE_LABEL] migrated clients → brand_name, logo_url, primary_color, custom_domain, white_label_enabled")

        if "referral_code" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN referral_code   TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN referred_by     INTEGER")
            con.execute("ALTER TABLE clients ADD COLUMN referral_count  INTEGER DEFAULT 0")
            con.commit()
            _no_code = con.execute("SELECT id FROM clients WHERE referral_code IS NULL").fetchall()
            for _r in _no_code:
                _code = f"REF{_r['id']}{random.randint(1000, 9999)}"
                con.execute("UPDATE clients SET referral_code=? WHERE id=?", (_code, _r["id"]))
            if _no_code:
                con.commit()
            print(f"[REFERRAL_CREATED] migrated clients → referral columns, generated {len(_no_code)} code(s)")

        if "business_whatsapp_number" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN business_whatsapp_number TEXT")
            con.commit()
            print("[DB] migration: added business_whatsapp_number")
        if "whatsapp_connection_status" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_connection_status TEXT DEFAULT 'not_connected'")
            con.execute("""
                UPDATE clients
                SET whatsapp_connection_status = CASE
                    WHEN whatsapp_connected = 1 THEN 'connected'
                    ELSE 'not_connected'
                END
            """)
            con.commit()
            print("[DB] migration: added whatsapp_connection_status, backfilled existing")
        if "whatsapp_provider" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_provider TEXT DEFAULT 'meta'")
            con.commit()
            print("[DB] migration: added whatsapp_provider")

        _catalog_cols = [r[1] for r in con.execute("PRAGMA table_info(catalogs)").fetchall()]
        if "currency" not in _catalog_cols:
            con.execute("ALTER TABLE catalogs ADD COLUMN currency TEXT")
            con.commit()
        if "category" not in _catalog_cols:
            con.execute("ALTER TABLE catalogs ADD COLUMN category TEXT")
            con.commit()
        if "image_url" not in _catalog_cols:
            con.execute("ALTER TABLE catalogs ADD COLUMN image_url TEXT")
            con.commit()
        _catalog_cols = [r[1] for r in con.execute("PRAGMA table_info(catalogs)").fetchall()]
        for field_name, field_type in AI_CATALOG_FIELDS:
            if field_name not in _catalog_cols:
                con.execute(f"ALTER TABLE catalogs ADD COLUMN {field_name} {field_type}")
                print(f"[DB] migration: added catalogs.{field_name}")
        con.commit()
        _tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "catalog_items" in _tables:
            _catalog_item_cols = [r[1] for r in con.execute("PRAGMA table_info(catalog_items)").fetchall()]
            for field_name, field_type in AI_CATALOG_FIELDS:
                if field_name not in _catalog_item_cols:
                    con.execute(f"ALTER TABLE catalog_items ADD COLUMN {field_name} {field_type}")
                    print(f"[DB] migration: added catalog_items.{field_name}")
            con.commit()

        # ── Affiliate columns (clients) ───────────────────────────────────────
        if "affiliate_code" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_enabled  INTEGER DEFAULT 1")
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_code     TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_earnings REAL    DEFAULT 0.0")
            con.execute("ALTER TABLE clients ADD COLUMN affiliate_rate     REAL    DEFAULT 0.20")
            con.commit()
            _no_aff = con.execute("SELECT id FROM clients WHERE affiliate_code IS NULL").fetchall()
            for _r in _no_aff:
                con.execute("UPDATE clients SET affiliate_code=? WHERE id=?",
                            (f"AFF{_r['id']}", _r["id"]))
            if _no_aff:
                con.commit()
            print(f"[AFFILIATE_CREATED] migrated clients → affiliate columns, generated {len(_no_aff)} code(s)")

        # ── Trial columns ─────────────────────────────────────────────────────
        if "is_trial" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN is_trial            INTEGER DEFAULT 0")
            con.execute("ALTER TABLE clients ADD COLUMN trial_started_at    TEXT")
            con.execute("ALTER TABLE clients ADD COLUMN trial_ends_at       TEXT")
            con.commit()
            print("[DB] migration: added is_trial, trial_started_at, trial_ends_at")
        if "trial_reminder_day" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN trial_reminder_day  INTEGER DEFAULT 0")
            con.commit()
            print("[DB] migration: added trial_reminder_day")

        # ── conversations: add collected_data column ──────────────────────────
        _conv_cols = [r[1] for r in con.execute("PRAGMA table_info(conversations)").fetchall()]
        if "collected_data" not in _conv_cols:
            con.execute("ALTER TABLE conversations ADD COLUMN collected_data TEXT DEFAULT '{}'")
            con.commit()
            print("[DB] migration: added collected_data")
        con.execute("UPDATE conversations SET current_step='greeting' WHERE current_step IS NULL OR current_step='' OR current_step='service'")
        con.commit()

        # ── orders: add intent + customer_phone + payment columns ───────────────
        _ord_cols = [r[1] for r in con.execute("PRAGMA table_info(orders)").fetchall()]
        if "intent" not in _ord_cols:
            con.execute("ALTER TABLE orders ADD COLUMN intent          TEXT DEFAULT 'unknown'")
            con.execute("ALTER TABLE orders ADD COLUMN customer_phone  TEXT DEFAULT ''")
            con.commit()
            print("[DB] migration: added intent, customer_phone")
        if "amount" not in _ord_cols:
            con.execute("ALTER TABLE orders ADD COLUMN amount           REAL DEFAULT 0")
            con.execute("ALTER TABLE orders ADD COLUMN payment_status   TEXT DEFAULT 'pending'")
            con.execute("ALTER TABLE orders ADD COLUMN payment_link     TEXT DEFAULT ''")
            con.execute("ALTER TABLE orders ADD COLUMN payment_provider TEXT DEFAULT 'paypal'")
            con.commit()
            print("[DB] migration: added amount, payment_status, payment_link, payment_provider")

        # ── AI Brain columns ──────────────────────────────────────────────────
        if "assistant_tone" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN assistant_tone        TEXT DEFAULT 'friendly'")
            con.execute("ALTER TABLE clients ADD COLUMN assistant_goal        TEXT DEFAULT 'book_appointments'")
            con.execute("ALTER TABLE clients ADD COLUMN business_description  TEXT DEFAULT ''")
            con.execute("ALTER TABLE clients ADD COLUMN policies              TEXT DEFAULT ''")
            con.execute("ALTER TABLE clients ADD COLUMN fallback_message      TEXT DEFAULT ''")
            con.commit()
            print("[DB] migration: added assistant_tone, assistant_goal, business_description, policies, fallback_message")

        # users: add email + client_id columns for multi-tenant auth
        _usr_cols = [r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()]
        if "affiliate_id" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN affiliate_id INTEGER")
            con.commit()
            print("[DB] migration: added affiliate_id")
        if "email" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN email TEXT")
            con.commit()
            print("[DB] migration: added email")
        if "client_id" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN client_id INTEGER")
            con.execute("UPDATE users SET client_id=1 WHERE client_id IS NULL")
            con.commit()
            print("[DB] migration: added client_id, linked existing users → 1")

        # ── STEP 7b: subscription_plans ──────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS subscription_plans (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                name               TEXT NOT NULL,
                price_monthly      REAL NOT NULL DEFAULT 0,
                max_messages       INTEGER NOT NULL DEFAULT 100,
                max_catalog_items  INTEGER NOT NULL DEFAULT 5,
                max_orders         INTEGER NOT NULL DEFAULT 20,
                features_json      TEXT DEFAULT '[]',
                is_active          INTEGER DEFAULT 1
            )
        """)

        # ── STEP 7c: client_subscriptions ────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS client_subscriptions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id      INTEGER NOT NULL,
                plan_id        INTEGER NOT NULL,
                status         TEXT NOT NULL DEFAULT 'active',
                started_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                expires_at     TEXT,
                messages_used  INTEGER DEFAULT 0,
                orders_used    INTEGER DEFAULT 0,
                bonus_messages INTEGER DEFAULT 0
            )
        """)
        con.commit()

        # client_subscriptions: bonus_messages for referral rewards (existing DBs)
        _sub_cols = [r[1] for r in con.execute("PRAGMA table_info(client_subscriptions)").fetchall()]
        if "bonus_messages" not in _sub_cols:
            con.execute("ALTER TABLE client_subscriptions ADD COLUMN bonus_messages INTEGER DEFAULT 0")
            con.commit()
            print("[DB] migration: added bonus_messages")
        if "paypal_subscription_id" not in _sub_cols:
            con.execute("ALTER TABLE client_subscriptions ADD COLUMN paypal_subscription_id TEXT")
            con.commit()
            print("[DB] migration: added paypal_subscription_id")

        # clients: plan shortcut + raw subscription_id for quick lookups
        if "plan" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN plan TEXT DEFAULT 'free'")
            con.commit()
            print("[DB] migration: added plan")
        if "subscription_id" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN subscription_id TEXT")
            con.commit()
            print("[DB] migration: added subscription_id")
        if "subscription_status" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN subscription_status TEXT DEFAULT 'inactive'")
            con.execute("""
                UPDATE clients
                SET subscription_status = 'active'
                WHERE plan IS NOT NULL AND plan != 'free' AND plan != ''
            """)
            con.commit()
            print("[DB] migration: added subscription_status")

        # ── STEP 7d: api_keys ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER NOT NULL,
                api_key    TEXT NOT NULL UNIQUE,
                label      TEXT DEFAULT 'Default',
                is_active  INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── STEP 7e: webhooks ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER NOT NULL,
                url        TEXT NOT NULL,
                event_type TEXT NOT NULL,
                is_active  INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── STEP 7f: client_integrations ─────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS client_integrations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id   INTEGER NOT NULL,
                provider    TEXT NOT NULL,
                config_json TEXT DEFAULT '{}',
                is_active   INTEGER DEFAULT 1,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── STEP 7g: paypal_payments ──────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS paypal_payments (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id       INTEGER,
                subscription_id TEXT,
                sale_id         TEXT UNIQUE,
                amount          REAL,
                currency        TEXT DEFAULT 'USD',
                event_type      TEXT,
                raw_json        TEXT,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── STEP 7h: analytics_events ─────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS analytics_events (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id  INTEGER,
                event_name TEXT NOT NULL,
                metadata   TEXT DEFAULT '{}',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()

        # ── Seed default plans ────────────────────────────────────────────
        plan_count = con.execute("SELECT COUNT(*) FROM subscription_plans").fetchone()[0]
        if plan_count == 0:
            import json as _json
            _plans = [
                ("Free",     0,  100,  5,   20,  '["WhatsApp bot","Up to 5 catalog items","Basic support"]'),
                ("Starter",  9,  1000, 25,  100, '["WhatsApp bot","Up to 25 catalog items","Email support","Multilingual"]'),
                ("Pro",      29, 5000, 100, 500, '["WhatsApp bot","Up to 100 catalog items","Priority support","Multilingual","Upsells","Analytics"]'),
                ("Business", 79, -1,  -1,  -1,  '["Everything in Pro","Unlimited messages","Unlimited catalog","Dedicated support","Custom branding"]'),
            ]
            con.executemany("""
                INSERT INTO subscription_plans
                    (name, price_monthly, max_messages, max_catalog_items, max_orders, features_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """, _plans)
            con.commit()
            print("[DB] migration: seeded 4 default plans")

        # ── Price migration: ensure pricing matches current values ─────────
        _price_map = {"starter": 9, "pro": 29, "business": 79, "free": 0}
        for _pname, _pprice in _price_map.items():
            con.execute(
                "UPDATE subscription_plans SET price_monthly=? WHERE LOWER(name)=? AND price_monthly!=?",
                (_pprice, _pname, _pprice)
            )
        con.commit()

        # ── Assign Free plan to any client without a subscription ─────────
        free_plan = con.execute(
            "SELECT id FROM subscription_plans WHERE name='Free' LIMIT 1"
        ).fetchone()
        if free_plan:
            unsubscribed = con.execute("""
                SELECT id FROM clients
                WHERE id NOT IN (
                    SELECT DISTINCT client_id FROM client_subscriptions WHERE status='active'
                )
            """).fetchall()
            for cli in unsubscribed:
                con.execute("""
                    INSERT INTO client_subscriptions (client_id, plan_id, status)
                    VALUES (?, ?, 'active')
                """, (cli["id"], free_plan["id"]))
            if unsubscribed:
                con.commit()
                print(f"[DB] migration: assigned Free plan to {len(unsubscribed)} client(s)")

        # ── STEP 8: Seed default client ──────────────────────────────────────
        exists = con.execute("SELECT id FROM clients WHERE id = 1").fetchone()
        if not exists:
            con.execute("""
                INSERT INTO clients (id, name, business_type, default_language,
                    currency, timezone, admin_whatsapp, is_active)
                VALUES (1, 'My Business', '', 'ar',
                    'SAR', 'Africa/Nouakchott', NULL, 1)
            """)
            con.commit()
            print("[DB] migration: seeded default client id=1")

    finally:
        con.close()

_migrate_saas()

# ── SAAS HELPERS ───────────────────────────────────────────────────────────

CLIENT_ID = DEFAULT_CLIENT_ID

def _session_client_id():
    """Return the authenticated client's ID from session. Falls back to CLIENT_ID."""
    cid = session.get("client_id")
    return int(cid) if cid else CLIENT_ID

def get_client(client_id=CLIENT_ID):
    con = get_db_connection()
    try:
        row = con.execute("SELECT * FROM clients WHERE id=?", (client_id,)).fetchone()
    finally:
        con.close()
    return dict(row) if row else {}

def get_client_subscription(client_id):
    """Return dict with subscription + plan data for the active subscription, or None."""
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT cs.id, cs.client_id, cs.plan_id, cs.status,
                   cs.started_at, cs.expires_at,
                   cs.messages_used, cs.orders_used, cs.bonus_messages,
                   sp.name        AS plan_name,
                   sp.price_monthly,
                   sp.max_messages, sp.max_catalog_items, sp.max_orders,
                   sp.features_json
            FROM   client_subscriptions cs
            JOIN   subscription_plans   sp ON sp.id = cs.plan_id
            WHERE  cs.client_id = ? AND cs.status = 'active'
            ORDER  BY cs.id DESC LIMIT 1
        """, (client_id,)).fetchone()
    finally:
        con.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["features"] = json.loads(d.get("features_json") or "[]")
    except Exception:
        d["features"] = []
    return d


def check_usage_limit(client_id, limit_type):
    """Check whether client_id is within their plan limits."""
    sub = get_client_subscription(client_id)
    if not sub:
        print(f"[BILLING_LIMIT_CHECK] client={client_id} type={limit_type} NO_SUB → allowed")
        return True, None

    plan_name = sub.get("plan_name", "?")

    if limit_type == "messages":
        limit = sub.get("max_messages", 100) + sub.get("bonus_messages", 0)
        used  = sub.get("messages_used", 0)
    elif limit_type == "catalog_items":
        limit = sub.get("max_catalog_items", 5)
        con = get_db_connection()
        try:
            used = con.execute(
                "SELECT COUNT(*) FROM catalogs WHERE client_id=?", (client_id,)
            ).fetchone()[0]
        finally:
            con.close()
    elif limit_type == "orders":
        limit = sub.get("max_orders", 20)
        used  = sub.get("orders_used", 0)
    else:
        print(f"[BILLING_LIMIT_CHECK] client={client_id} UNKNOWN limit_type={limit_type!r} → allowed")
        return True, sub

    if limit == -1:
        print(f"[BILLING_LIMIT_CHECK] client={client_id} plan={plan_name!r} type={limit_type} used={used}/∞ → allowed (unlimited)")
        return True, sub

    allowed = used < limit
    status  = "allowed" if allowed else "BLOCKED"
    print(f"[BILLING_LIMIT_CHECK] client={client_id} plan={plan_name!r} type={limit_type} used={used}/{limit} → {status}")
    if not allowed:
        print(f"[BILLING_BLOCKED] client={client_id} plan={plan_name!r} type={limit_type} limit={limit} used={used}")
    return allowed, sub


def _billing_increment(client_id, field):
    """Increment messages_used or orders_used for the active subscription."""
    con = get_db_connection()
    try:
        con.execute(f"""
            UPDATE client_subscriptions
            SET    {field} = {field} + 1
            WHERE  client_id = ? AND status = 'active'
        """, (client_id,))
        con.commit()
    finally:
        con.close()


# ── Plan configuration ─────────────────────────────────────────────────────────
PLANS = {
    "free": {
        "max_messages":      100,
        "max_catalog_items": 5,
        "max_orders":        10,
        "features": {
            "whatsapp_bot": True,
            "multilingual": False,
            "upsell":       False,
            "analytics":    False,
            "white_label":  False,
        },
    },
    "starter": {
        "max_messages":      1000,
        "max_catalog_items": 25,
        "max_orders":        100,
        "features": {
            "whatsapp_bot": True,
            "multilingual": True,
            "upsell":       False,
            "analytics":    False,
            "white_label":  False,
        },
    },
    "pro": {
        "max_messages":      5000,
        "max_catalog_items": 100,
        "max_orders":        500,
        "features": {
            "whatsapp_bot": True,
            "multilingual": True,
            "upsell":       True,
            "analytics":    True,
            "white_label":  False,
        },
    },
    "business": {
        "max_messages":      None,
        "max_catalog_items": None,
        "max_orders":        None,
        "features": {
            "whatsapp_bot": True,
            "multilingual": True,
            "upsell":       True,
            "analytics":    True,
            "white_label":  True,
        },
    },
}


def get_client_plan(client_id):
    """Return the client's active plan name as a lowercase string."""
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT plan FROM clients WHERE id=?", (client_id,)
        ).fetchone()
    finally:
        con.close()
    plan = (row["plan"] if row and row["plan"] else "free").lower().strip()
    print(f"[PLAN_CHECK] client={client_id} plan={plan!r}")
    return plan


def has_feature(client_id, feature):
    """Return True if the client's current plan includes 'feature'."""
    plan    = get_client_plan(client_id)
    allowed = PLANS.get(plan, PLANS["free"])["features"].get(feature, False)
    if not allowed:
        print(f"[FEATURE_BLOCKED] client={client_id} plan={plan!r} feature={feature!r} → blocked")
    return allowed


def check_limit(client_id, limit_type):
    """Return (allowed: bool, sub: dict|None)."""
    plan     = get_client_plan(client_id)
    plan_cfg = PLANS.get(plan, PLANS["free"])
    _key_map = {
        "messages":      "max_messages",
        "catalog_items": "max_catalog_items",
        "orders":        "max_orders",
    }
    static_limit = plan_cfg.get(_key_map.get(limit_type, ""), 0)

    if static_limit is None:
        print(f"[LIMIT_CHECK] client={client_id} plan={plan!r} type={limit_type} → unlimited ✓")
        return True, None

    allowed, sub = check_usage_limit(client_id, limit_type)
    status = "allowed" if allowed else "EXCEEDED"
    print(f"[LIMIT_CHECK] client={client_id} plan={plan!r} type={limit_type} static_limit={static_limit} → {status}")
    if not allowed:
        print(f"[LIMIT_EXCEEDED] client={client_id} plan={plan!r} type={limit_type} limit={static_limit}")
    return allowed, sub


def check_plan_limit(client_id, limit_name):
    """Public alias for check_limit()."""
    return check_limit(client_id, limit_name)


def increment_usage(client_id, usage_type):
    """Increment a usage counter for the client's active subscription."""
    _billing_increment(client_id, usage_type)
    print(f"[USAGE_INCREMENTED] client={client_id} type={usage_type}")


def get_trial_status(client):
    """Return a dict describing the client's free-trial state."""
    if not client or not client.get("is_trial"):
        return {"is_trial": False, "active": False, "expired": False}

    ends_str = client.get("trial_ends_at")
    if not ends_str:
        return {"is_trial": True, "active": False, "expired": True}

    try:
        ends_at = datetime.datetime.fromisoformat(ends_str)
    except (ValueError, TypeError):
        return {"is_trial": True, "active": False, "expired": True}

    remaining = (ends_at - datetime.datetime.now()).total_seconds()

    if remaining <= 0:
        print(f"[TRIAL_EXPIRED] client={client.get('id')} trial_ends_at={ends_str!r}")
        return {
            "is_trial": True, "active": False, "expired": True,
            "remaining_seconds": 0, "days": 0, "hours": 0, "minutes": 0,
            "warning": False, "ends_at": ends_str,
        }

    days    = int(remaining // 86400)
    hours   = int((remaining % 86400) // 3600)
    minutes = int((remaining % 3600) // 60)
    warning = remaining < 86400

    if warning:
        print(f"[TRIAL_WARNING] client={client.get('id')} remaining={hours}h {minutes}m")
    else:
        print(f"[TRIAL_ACTIVE] client={client.get('id')} remaining={days}d {hours}h")

    return {
        "is_trial": True, "active": True, "expired": False,
        "remaining_seconds": remaining,
        "days": days, "hours": hours, "minutes": minutes,
        "warning": warning, "ends_at": ends_str,
    }


def expire_trial_if_needed(client_id):
    """Downgrade a client to the free plan if their trial has ended."""
    client = get_client(client_id)
    if not client.get("is_trial"):
        return False

    trial = get_trial_status(client)
    if not trial.get("expired"):
        return False

    con = get_db_connection()
    try:
        con.execute("""
            UPDATE clients
            SET    is_trial=0, plan='free', subscription_status='expired'
            WHERE  id=? AND is_trial=1
        """, (client_id,))
        con.execute("""
            UPDATE client_subscriptions
            SET    status='cancelled'
            WHERE  client_id=? AND status IN ('active', 'pending')
        """, (client_id,))
        con.commit()
    finally:
        con.close()

    print(f"[TRIAL_EXPIRED] client={client_id} → downgraded to free plan")
    track_event(client_id, "trial_expired", {})
    return True


def track_event(client_id, event_name, metadata=None):
    """Insert one row into analytics_events."""
    _meta = json.dumps(metadata or {})
    try:
        con = get_db_connection()
        try:
            con.execute(
                "INSERT INTO analytics_events (client_id, event_name, metadata) VALUES (?, ?, ?)",
                (client_id, event_name, _meta)
            )
            con.commit()
        finally:
            con.close()
        print(f"[EVENT_TRACKED] client={client_id} event={event_name!r} meta={_meta}")
    except Exception as _te:
        print(f"[EVENT_TRACK_ERROR] {event_name!r}: {_te}")


def handle_limit_exceeded(client_id, limit_type):
    """Central paywall handler."""
    print(f"[PAYWALL_TRIGGERED] client={client_id} limit_type={limit_type!r} → upgrade required")
    return {
        "error":       "limit_exceeded",
        "limit_type":  limit_type,
        "message_ar":  "لقد وصلت إلى الحد الأقصى لباقتك.",
        "message_en":  "You have reached your plan limit.",
        "upgrade_url": "/admin/billing",
    }


def generate_referral_code(client_id):
    """Generate a unique referral code for a client."""
    digits = random.randint(1000, 9999)
    return f"REF{client_id}{digits}"


def generate_affiliate_code(client_id):
    """Generate a deterministic affiliate code for a client."""
    return f"AFF{client_id}"


@app.route("/")
def home():
    if session.get("logged_in"):
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("login"))

@app.route("/assistant")
def assistant():
    return render_template("index.html")

def resolve_whatsapp_client_id(phone_number_id=None):
    con = get_db_connection()
    try:
        if phone_number_id:
            row = con.execute("""
                SELECT id
                FROM clients
                WHERE business_whatsapp_number=?
                ORDER BY id DESC
                LIMIT 1
            """, (phone_number_id,)).fetchone()
            if row:
                print(f"[BOT_CLIENT_RESOLVE] phone_number_id={phone_number_id!r} client_id={row['id']}")
                return row["id"]

        default_count = con.execute("""
            SELECT COUNT(*)
            FROM catalogs
            WHERE client_id=? AND is_active=1
        """, (DEFAULT_CLIENT_ID,)).fetchone()[0]
        if default_count:
            print(f"[BOT_CLIENT_RESOLVE] using DEFAULT_CLIENT_ID={DEFAULT_CLIENT_ID} active_items={default_count}")
            return DEFAULT_CLIENT_ID

        row = con.execute("""
            SELECT client_id, COUNT(*) AS item_count
            FROM catalogs
            WHERE is_active=1
            GROUP BY client_id
            ORDER BY item_count DESC, client_id DESC
            LIMIT 1
        """).fetchone()
        if row:
            print(f"[BOT_CLIENT_RESOLVE] fallback_client_id={row['client_id']} active_items={row['item_count']}")
            return row["client_id"]
    finally:
        con.close()

    print(f"[BOT_CLIENT_RESOLVE] no active catalog found; using DEFAULT_CLIENT_ID={DEFAULT_CLIENT_ID}")
    return DEFAULT_CLIENT_ID


def detect_language(text):
    text = (text or "").strip().lower()
    if not text or text.isdigit() or len(text) <= 2:
        return None
    if any("\u0600" <= char <= "\u06ff" for char in text):
        return "ar"
    language_markers = {
        "fr": ["bonjour", "salut", "merci", "acheter", "moins cher", "cher", "luxe", "je veux", "je ne veux pas", "produit"],
        "es": ["hola", "gracias", "comprar", "barato", "lujo", "quiero", "no quiero", "producto", "precio"],
        "it": ["ciao", "grazie", "comprare", "economico", "lusso", "voglio", "non voglio", "prodotto", "prezzo"],
        "en": ["hello", "hi", "thanks", "buy", "cheap", "luxury", "want", "product", "price"],
    }
    scores = {
        lang_code: sum(1 for marker in markers if marker in text)
        for lang_code, markers in language_markers.items()
    }
    best_lang, best_score = max(scores.items(), key=lambda pair: pair[1], default=("en", 0))
    return best_lang if best_score > 0 else None


TRANSLATIONS = {
    "ar": {
        "welcome": "مرحباً بك في {business_name}.",
        "catalog_heading": "كتالوج {business_name}:",
        "price": "السعر",
        "sale_price": "سعر العرض",
        "description": "الوصف",
        "type": "النوع",
        "duration": "المدة",
        "what_next": "ماذا تريد أن تفعل؟",
        "view_details": "1. عرض التفاصيل",
        "buy_now": "2. الشراء الآن",
        "back_catalog": "3. العودة إلى الكتالوج",
        "send_name_city": "🛒 ممتاز! لإتمام الطلب، أرسل:\n- اسمك\n- مدينتك",
        "order_received": "✅ تم استلام طلبك بنجاح! سنتواصل معك قريباً.",
        "unknown": "لم أفهم طلبك، يمكنك اختيار:\n1. الكتالوج\n2. التحدث مع الدعم",
        "alternative_options": "لا أملك خياراً مطابقاً تماماً، لكن قد يعجبك هذا:",
        "category_fallback_options": "لم أجد خيارًا مطابقًا تمامًا داخل هذه الفئة، لكن يمكنني اقتراح بدائل قريبة:",
        "send_product_name": "أرسل اسم المنتج أو الخدمة لمعرفة التفاصيل.",
        "catalog_already": "الكتالوج معروض بالأعلى. أرسل اسم المنتج أو الخدمة لمعرفة التفاصيل.",
        "help_extra": "وإذا تحب، أقدر أساعدك أيضاً بـ: الصورة، المواصفات، أو منتج مشابه.",
        "menu_question": "كيف يمكننا مساعدتك اليوم؟",
        "menu_catalog": "1. تصفح الكتالوج",
        "menu_prices": "2. السؤال عن الأسعار",
        "menu_booking": "3. حجز موعد",
        "menu_support": "4. التحدث مع الدعم",
        "menu_hint": "يمكنك أيضاً إرسال اسم المنتج أو الخدمة التي تبحث عنها.",
        "recommend_intro_1": "أرشح لك هذا لأنه قريب من طلبك 👌",
        "recommend_intro_2": "هذا اختيار مناسب بناءً على المواصفات التي ذكرتها.",
        "recommend_intro_3": "هذا المنتج قد يناسبك جداً من حيث القيمة والمواصفات.",
        "unsure_intro_1": "ولا يهمك، خليني أبسطها لك 👌",
        "unsure_intro_2": "طبيعي تحتار، خليني أقسمها لك حسب الاستخدام والميزانية 😊",
        "unsure_intro_3": "أفهم ترددك، هذه اختيارات سهلة حسب احتياجك:",
        "reason_high_quality": "إذا تبحث عن جودة أعلى",
        "reason_budget": "إذا تفضّل خيار اقتصادي",
        "reason_mid": "إذا تريد خياراً متوسطاً",
        "reason_alternative": "إذا تريد بديلاً مختلفاً",
        "unsure_outro": "إذا تحب، قل لي الميزانية أو الاستخدام أو أهم ميزة عندك — وأرشح لك الأقرب مباشرة.",
        "reject_options": "لا مشكلة 😊\nهل تفضّل أن أقترح لك:\n1. خيار أعلى جودة\n2. خيار أقوى بالمواصفات\n3. خيار بسعر أقل\n4. الرجوع إلى الكتالوج",
        "price_intro_1": "أتفهمك، مو لازم نبدأ بالأغلى 😊 خليني أقترح لك خيار أذكى بالسعر:",
        "price_intro_2": "صحيح السعر مهم. هذه خيارات أخف على الميزانية وقريبة من ذوقك:",
        "price_intro_3": "ولا يهمك 👌 خليني أبدّل لك لخيار مناسب أكثر بالسعر:",
        "price_outro_1": "إذا تحب، أقدر أقارنها لك بالخيارات السابقة.",
        "price_outro_2": "أي خيار يناسب ميزانيتك؟",
        "price_outro_3": "تحب أختار لك الأفضل قيمة بينهم؟",
        "premium_intro_1": "إذا تبحث عن خيار أعلى جودة فهذا قد يعجبك جداً:",
        "premium_intro_2": "لو تفضّل الفئة الأعلى، هذه أقرب الخيارات:",
        "premium_intro_3": "هذه ترشيحات بطابع premium ومناسبة لطلبك:",
        "premium_outro_1": "أي خيار أعجبك أكثر؟ 😊",
        "premium_outro_2": "تحب أعطيك تفاصيل واحد منها؟",
        "premium_outro_3": "أي واحد تشوفه أقرب لاحتياجك؟",
        "details_already": "أنت تشاهد تفاصيل المنتج بالفعل.",
        "order_received": "✅ تم استلام طلبك بنجاح! سنتواصل معك قريباً.",
        "no_items": "مرحباً بك في {business_name}.\nلا توجد منتجات أو خدمات مضافة حالياً. يرجى المحاولة لاحقاً أو التواصل مع الدعم.",
        "closed_thanks": "شكراً لتواصلك معنا ❤️",
        "search_more": "أرسل كلمة بحث لعرض نتائج أدق.",
        "price_request_prompt": "أكيد 👌\nأرسل اسم المنتج أو الخدمة التي تريد معرفة سعرها.",
        "booking_prompt": "📅 لحجز موعد أرسل:\n- اسمك\n- الخدمة المطلوبة\n- اليوم المناسب\n- الوقت المناسب",
        "support_prompt": "🎧 تم تحويلك للدعم.\nأرسل سؤالك وسنساعدك في أقرب وقت.",
        "menu_already": "القائمة معروضة بالأعلى. أرسل رقم الخيار أو اسم المنتج الذي تبحث عنه.",
        "similar_products_intro": "أكيد، هذه منتجات مشابهة قد تعجبك:",
        "special_offers_intro": "هذه بعض العروض الخاصة المتاحة الآن:",
        "order_product_prompt": "للطلب أرسل اسم المنتج الذي أعجبك.",
        "no_special_offers": "لا توجد عروض خاصة حالياً، يمكنك تصفح الكتالوج:",
        "post_order_options": "هل ترغب أيضاً في:\n\n1. مشاهدة منتجات مشابهة\n\n2. عرض العروض الخاصة\n\n3. إنهاء المحادثة",
        "compare_need_recommendations": "اطلب ترشيحات أولاً، وبعدها أقارن لك بين آخر الخيارات المعروضة.",
        "compare_heading": "الفرق بينهم باختصار:",
        "trait_budget": "اقتصادي",
        "trait_premium": "فئة أعلى",
        "trait_fast": "سريع",
        "trait_strong": "قوي",
        "trait_daily": "مناسب للاستخدام اليومي",
        "trait_default": "خيار مناسب حسب الكتالوج",
        "business_fashion_q1": "أكيد، أساعدك تختار الأنسب 👌\nما المقاس واللون المفضل؟ وتحبها رسمي أو casual؟",
        "business_fashion_q2": "خليني أحدد لك خيارات أدق: ما المقاس؟ أي لون؟ واستخدامها رسمي ولا يومي؟",
        "business_electronics_q1": "تمام، حتى أرشح لك بدقة: ما الميزانية؟ والاستخدام أهم شيء عندك بطارية أو أداء أو مساحة؟",
        "business_electronics_q2": "أقدر أساعدك. هل تبحث عن بطارية قوية، أداء سريع، أو خيار اقتصادي؟ وما ميزانيتك؟",
        "business_restaurant_q1": "أكيد 👌 تحب أي نوع وجبة؟ حار أو عادي؟ وتحتاج توصيل؟",
        "business_restaurant_q2": "خليني أختار لك الأفضل: تفضّل وجبة خفيفة أو مشبعة؟ حار؟ ومع توصيل؟",
        "business_general_q1": "أكيد، أساعدك تختار الأنسب 👌\nهل تفضّل خيار اقتصادي، فاخر، عملي، أو للاستخدام اليومي؟",
        "business_general_q2": "خليني أرشح لك بدقة: ما الميزانية؟ وهل تبحث عن جودة أعلى، سعر أقل، أو استخدام معيّن؟",
    },
    "en": {
        "welcome": "Welcome to {business_name}.",
        "catalog_heading": "{business_name} catalog:",
        "price": "Price",
        "sale_price": "Sale price",
        "description": "Description",
        "type": "Type",
        "duration": "Duration",
        "what_next": "What would you like to do?",
        "view_details": "1. View details",
        "buy_now": "2. Buy now",
        "back_catalog": "3. Back to catalog",
        "send_name_city": "🛒 Great! To complete the order, send:\n- Your name\n- Your city",
        "order_received": "✅ Your order has been received successfully! We will contact you soon.",
        "unknown": "I did not understand your request. You can choose:\n1. Catalog\n2. Talk to support",
        "alternative_options": "I do not have an exact match, but you may like this:",
        "category_fallback_options": "I did not find an exact match inside this category, but I can suggest close alternatives:",
        "send_product_name": "Send the product or service name for details.",
        "catalog_already": "The catalog is already shown above. Send a product or service name for details.",
        "help_extra": "I can also help with: image, specifications, or a similar product.",
        "menu_question": "How can we help you today?",
        "menu_catalog": "1. Browse catalog",
        "menu_prices": "2. Ask about prices",
        "menu_booking": "3. Book an appointment",
        "menu_support": "4. Talk to support",
        "menu_hint": "You can also send the product or service name you are looking for.",
        "recommend_intro_1": "I recommend this because it is close to your request 👌",
        "recommend_intro_2": "This is a suitable option based on the details you mentioned.",
        "recommend_intro_3": "This product may fit you well in terms of value and specifications.",
        "unsure_intro_1": "No problem, let me make it simpler for you 👌",
        "unsure_intro_2": "It is normal to be unsure. Let me split it by use and budget 😊",
        "unsure_intro_3": "I understand your hesitation. Here are easy choices based on your needs:",
        "reason_high_quality": "If you want higher quality",
        "reason_budget": "If you prefer a budget option",
        "reason_mid": "If you want a mid-range option",
        "reason_alternative": "If you want a different alternative",
        "unsure_outro": "Tell me your budget, use case, or most important feature — and I will recommend the closest option.",
        "reject_options": "No problem 😊\nWould you prefer that I suggest:\n1. A higher-quality option\n2. An option with stronger specs\n3. A lower-price option\n4. Back to catalog",
        "price_intro_1": "I understand. We do not have to start with the most expensive option 😊 Here is a smarter price choice:",
        "price_intro_2": "You are right, price matters. These options are lighter on the budget and close to your request:",
        "price_intro_3": "No problem 👌 Let me switch to a more suitable price option:",
        "price_outro_1": "I can compare it with the previous options if you like.",
        "price_outro_2": "Which option fits your budget?",
        "price_outro_3": "Would you like me to choose the best value among them?",
        "premium_intro_1": "If you are looking for higher quality, this may suit you well:",
        "premium_intro_2": "If you prefer the higher tier, these are the closest options:",
        "premium_intro_3": "These are premium-style recommendations that match your request:",
        "premium_outro_1": "Which option did you like most? 😊",
        "premium_outro_2": "Would you like details for one of them?",
        "premium_outro_3": "Which one feels closest to your need?",
        "details_already": "You are already viewing the product details.",
        "no_items": "Welcome to {business_name}.\nNo products or services have been added yet. Please check again later or contact support.",
        "closed_thanks": "Thank you for contacting us ❤️",
        "search_more": "Send a search term to see more specific results.",
        "price_request_prompt": "Sure 👌\nSend the product or service name you want the price for.",
        "booking_prompt": "📅 To book an appointment, send:\n- Your name\n- The requested service\n- Preferred day\n- Preferred time",
        "support_prompt": "🎧 You have been transferred to support.\nSend your question and we will help you as soon as possible.",
        "menu_already": "The menu is already shown above. Please send an option number or product name.",
        "similar_products_intro": "Sure, here are similar products you may like:",
        "special_offers_intro": "Here are some special offers currently available:",
        "order_product_prompt": "To order, send the name of the product you liked.",
        "no_special_offers": "There are no special offers right now. You can browse the catalog:",
        "post_order_options": "Would you also like to:\n\n1. See similar products\n\n2. View special offers\n\n3. End the conversation",
        "compare_need_recommendations": "Ask for recommendations first, then I can compare the latest shown options.",
        "compare_heading": "Here is the difference in brief:",
        "trait_budget": "budget-friendly",
        "trait_premium": "higher tier",
        "trait_fast": "fast",
        "trait_strong": "strong",
        "trait_daily": "suitable for daily use",
        "trait_default": "suitable option based on the catalog",
        "business_fashion_q1": "Sure, I can help you choose the best option 👌\nWhat size and color do you prefer? Formal or casual?",
        "business_fashion_q2": "Let me narrow it down: what size, which color, and is it for formal or daily use?",
        "business_electronics_q1": "Great, to recommend accurately: what is your budget? Is battery, performance, or storage most important?",
        "business_electronics_q2": "I can help. Are you looking for strong battery, fast performance, or a budget option? What is your budget?",
        "business_restaurant_q1": "Sure 👌 What type of meal do you prefer? Spicy or regular? Do you need delivery?",
        "business_restaurant_q2": "Let me choose the best option: do you prefer a light or filling meal? Spicy? With delivery?",
        "business_general_q1": "Sure, I can help you choose the best option 👌\nDo you prefer a budget, premium, practical, or daily-use option?",
        "business_general_q2": "Let me recommend accurately: what is your budget? Are you looking for higher quality, lower price, or a specific use?",
    },
    "fr": {
        "welcome": "Bienvenue chez {business_name}.",
        "catalog_heading": "Catalogue {business_name} :",
        "price": "Prix",
        "sale_price": "Prix promo",
        "description": "Description",
        "type": "Type",
        "duration": "Durée",
        "what_next": "Que souhaitez-vous faire ?",
        "view_details": "1. Voir les détails",
        "buy_now": "2. Acheter maintenant",
        "back_catalog": "3. Retour au catalogue",
        "send_name_city": "🛒 Parfait ! Pour finaliser la commande, envoyez :\n- Votre nom\n- Votre ville",
        "order_received": "✅ Votre commande a bien été reçue ! Nous vous contacterons bientôt.",
        "unknown": "Je n'ai pas compris votre demande. Vous pouvez choisir :\n1. Catalogue\n2. Contacter le support",
        "alternative_options": "Je n'ai pas d'option exactement correspondante, mais ceci peut vous plaire :",
        "category_fallback_options": "Je n'ai pas trouvé d'option exacte dans cette catégorie, mais je peux proposer des alternatives proches :",
        "send_product_name": "Envoyez le nom du produit ou service pour voir les détails.",
        "catalog_already": "Le catalogue est déjà affiché. Envoyez le nom d'un produit ou service.",
        "help_extra": "Je peux aussi vous aider avec : image, caractéristiques ou produit similaire.",
        "menu_question": "Comment pouvons-nous vous aider aujourd'hui ?",
        "menu_catalog": "1. Parcourir le catalogue",
        "menu_prices": "2. Demander les prix",
        "menu_booking": "3. Prendre rendez-vous",
        "menu_support": "4. Contacter le support",
        "menu_hint": "Vous pouvez aussi envoyer le nom du produit ou service recherché.",
        "recommend_intro_1": "Je vous recommande ceci, car c'est proche de votre demande 👌",
        "recommend_intro_2": "C'est une option adaptée selon les détails que vous avez mentionnés.",
        "recommend_intro_3": "Ce produit peut bien vous convenir en valeur et caractéristiques.",
        "unsure_intro_1": "Pas de souci, je vais simplifier le choix pour vous 👌",
        "unsure_intro_2": "C'est normal d'hésiter. Je vais classer les options par usage et budget 😊",
        "unsure_intro_3": "Je comprends votre hésitation. Voici des choix simples selon vos besoins :",
        "reason_high_quality": "Si vous cherchez une meilleure qualité",
        "reason_budget": "Si vous préférez une option économique",
        "reason_mid": "Si vous voulez une option intermédiaire",
        "reason_alternative": "Si vous voulez une alternative différente",
        "unsure_outro": "Dites-moi votre budget, l'usage ou la caractéristique la plus importante — et je vous recommanderai l'option la plus proche.",
        "reject_options": "Pas de problème 😊\nPréférez-vous que je propose :\n1. Une option de meilleure qualité\n2. Une option avec de meilleures caractéristiques\n3. Une option moins chère\n4. Retour au catalogue",
        "price_intro_1": "Je comprends. Nous n'avons pas besoin de commencer par le plus cher 😊 Voici un choix plus intelligent côté prix :",
        "price_intro_2": "Vous avez raison, le prix est important. Voici des options plus légères pour le budget et proches de votre demande :",
        "price_intro_3": "Pas de souci 👌 Je vous propose une option plus adaptée au prix :",
        "price_outro_1": "Je peux la comparer aux options précédentes si vous voulez.",
        "price_outro_2": "Quelle option correspond à votre budget ?",
        "price_outro_3": "Voulez-vous que je choisisse le meilleur rapport qualité-prix ?",
        "premium_intro_1": "Si vous cherchez une qualité supérieure, ceci peut très bien vous convenir :",
        "premium_intro_2": "Si vous préférez la gamme supérieure, voici les options les plus proches :",
        "premium_intro_3": "Voici des recommandations premium adaptées à votre demande :",
        "premium_outro_1": "Quelle option vous plaît le plus ? 😊",
        "premium_outro_2": "Voulez-vous les détails de l'une d'elles ?",
        "premium_outro_3": "Laquelle semble la plus proche de votre besoin ?",
        "details_already": "Vous consultez déjà les détails du produit.",
        "no_items": "Bienvenue chez {business_name}.\nAucun produit ou service n'a encore été ajouté. Veuillez réessayer plus tard ou contacter le support.",
        "closed_thanks": "Merci de nous avoir contactés ❤️",
        "search_more": "Envoyez un terme de recherche pour voir des résultats plus précis.",
        "price_request_prompt": "Bien sûr 👌\nEnvoyez le nom du produit ou service dont vous voulez connaître le prix.",
        "booking_prompt": "📅 Pour prendre rendez-vous, envoyez :\n- Votre nom\n- Le service demandé\n- Le jour souhaité\n- L'heure souhaitée",
        "support_prompt": "🎧 Vous avez été transféré au support.\nEnvoyez votre question et nous vous aiderons dès que possible.",
        "menu_already": "Le menu est déjà affiché. Envoyez un numéro d'option ou le nom d'un produit.",
        "similar_products_intro": "Bien sûr, voici des produits similaires qui peuvent vous plaire :",
        "special_offers_intro": "Voici quelques offres spéciales disponibles actuellement :",
        "order_product_prompt": "Pour commander, envoyez le nom du produit qui vous plaît.",
        "no_special_offers": "Il n'y a pas d'offres spéciales actuellement. Vous pouvez parcourir le catalogue :",
        "post_order_options": "Souhaitez-vous aussi :\n\n1. Voir des produits similaires\n\n2. Voir les offres spéciales\n\n3. Terminer la conversation",
        "compare_need_recommendations": "Demandez d'abord des recommandations, puis je pourrai comparer les dernières options affichées.",
        "compare_heading": "Voici la différence en bref :",
        "trait_budget": "économique",
        "trait_premium": "gamme supérieure",
        "trait_fast": "rapide",
        "trait_strong": "puissant",
        "trait_daily": "adapté à un usage quotidien",
        "trait_default": "option adaptée selon le catalogue",
        "business_fashion_q1": "Bien sûr, je peux vous aider à choisir la meilleure option 👌\nQuelle taille et quelle couleur préférez-vous ? Formel ou casual ?",
        "business_fashion_q2": "Pour affiner le choix : quelle taille, quelle couleur, et pour un usage formel ou quotidien ?",
        "business_electronics_q1": "Très bien, pour recommander précisément : quel est votre budget ? Batterie, performance ou stockage est le plus important ?",
        "business_electronics_q2": "Je peux vous aider. Cherchez-vous une bonne batterie, une performance rapide ou une option économique ? Quel est votre budget ?",
        "business_restaurant_q1": "Bien sûr 👌 Quel type de repas préférez-vous ? Épicé ou normal ? Avez-vous besoin de livraison ?",
        "business_restaurant_q2": "Je vais choisir le meilleur : préférez-vous un repas léger ou copieux ? Épicé ? Avec livraison ?",
        "business_general_q1": "Bien sûr, je peux vous aider à choisir la meilleure option 👌\nPréférez-vous une option économique, premium, pratique ou pour un usage quotidien ?",
        "business_general_q2": "Pour recommander précisément : quel est votre budget ? Cherchez-vous une meilleure qualité, un prix plus bas ou un usage spécifique ?",
    },
    "es": {
        "welcome": "Bienvenido a {business_name}.",
        "catalog_heading": "Catálogo de {business_name}:",
        "price": "Precio",
        "sale_price": "Precio de oferta",
        "description": "Descripción",
        "type": "Tipo",
        "duration": "Duración",
        "what_next": "¿Qué te gustaría hacer?",
        "view_details": "1. Ver detalles",
        "buy_now": "2. Comprar ahora",
        "back_catalog": "3. Volver al catálogo",
        "send_name_city": "🛒 ¡Perfecto! Para completar el pedido, envía:\n- Tu nombre\n- Tu ciudad",
        "order_received": "✅ ¡Hemos recibido tu pedido correctamente! Te contactaremos pronto.",
        "unknown": "No entendí tu solicitud. Puedes elegir:\n1. Catálogo\n2. Hablar con soporte",
        "alternative_options": "No tengo una opción exacta, pero esto puede gustarte:",
        "category_fallback_options": "No encontré una opción exacta dentro de esta categoría, pero puedo sugerir alternativas cercanas:",
        "send_product_name": "Envía el nombre del producto o servicio para ver detalles.",
        "catalog_already": "El catálogo ya está mostrado. Envía el nombre de un producto o servicio.",
        "help_extra": "También puedo ayudarte con: imagen, especificaciones o un producto similar.",
        "menu_question": "¿Cómo podemos ayudarte hoy?",
        "menu_catalog": "1. Ver catálogo",
        "menu_prices": "2. Preguntar por precios",
        "menu_booking": "3. Reservar una cita",
        "menu_support": "4. Hablar con soporte",
        "menu_hint": "También puedes enviar el nombre del producto o servicio que buscas.",
        "recommend_intro_1": "Te recomiendo esto porque se acerca a tu solicitud 👌",
        "recommend_intro_2": "Esta es una opción adecuada según los detalles que mencionaste.",
        "recommend_intro_3": "Este producto puede encajarte bien por valor y especificaciones.",
        "unsure_intro_1": "No hay problema, te lo simplifico 👌",
        "unsure_intro_2": "Es normal tener dudas. Lo separaré por uso y presupuesto 😊",
        "unsure_intro_3": "Entiendo tu duda. Aquí tienes opciones simples según tus necesidades:",
        "reason_high_quality": "Si buscas mayor calidad",
        "reason_budget": "Si prefieres una opción económica",
        "reason_mid": "Si quieres una opción intermedia",
        "reason_alternative": "Si quieres una alternativa diferente",
        "unsure_outro": "Dime tu presupuesto, uso o característica más importante — y te recomendaré la opción más cercana.",
        "reject_options": "No hay problema 😊\n¿Prefieres que te sugiera:\n1. Una opción de mayor calidad\n2. Una opción con mejores especificaciones\n3. Una opción de menor precio\n4. Volver al catálogo",
        "price_intro_1": "Te entiendo. No tenemos que empezar por lo más caro 😊 Aquí tienes una opción más inteligente en precio:",
        "price_intro_2": "Tienes razón, el precio importa. Estas opciones son más ligeras para el presupuesto y cercanas a tu solicitud:",
        "price_intro_3": "No hay problema 👌 Te cambio a una opción más adecuada en precio:",
        "price_outro_1": "Puedo compararla con las opciones anteriores si quieres.",
        "price_outro_2": "¿Qué opción se ajusta a tu presupuesto?",
        "price_outro_3": "¿Quieres que elija la mejor relación calidad-precio?",
        "premium_intro_1": "Si buscas mayor calidad, esto puede gustarte mucho:",
        "premium_intro_2": "Si prefieres la gama superior, estas son las opciones más cercanas:",
        "premium_intro_3": "Estas son recomendaciones premium adecuadas para tu solicitud:",
        "premium_outro_1": "¿Qué opción te gustó más? 😊",
        "premium_outro_2": "¿Quieres detalles de alguna de ellas?",
        "premium_outro_3": "¿Cuál se acerca más a lo que necesitas?",
        "details_already": "Ya estás viendo los detalles del producto.",
        "no_items": "Bienvenido a {business_name}.\nAún no se han añadido productos o servicios. Vuelve a intentarlo más tarde o contacta con soporte.",
        "closed_thanks": "Gracias por contactarnos ❤️",
        "search_more": "Envía un término de búsqueda para ver resultados más específicos.",
        "price_request_prompt": "Claro 👌\nEnvía el nombre del producto o servicio del que quieres saber el precio.",
        "booking_prompt": "📅 Para reservar una cita, envía:\n- Tu nombre\n- El servicio solicitado\n- Día preferido\n- Hora preferida",
        "support_prompt": "🎧 Te hemos transferido a soporte.\nEnvía tu pregunta y te ayudaremos lo antes posible.",
        "menu_already": "El menú ya está mostrado. Envía un número de opción o el nombre de un producto.",
        "similar_products_intro": "Claro, aquí tienes productos similares que pueden gustarte:",
        "special_offers_intro": "Estas son algunas ofertas especiales disponibles ahora:",
        "order_product_prompt": "Para pedir, envía el nombre del producto que te gustó.",
        "no_special_offers": "No hay ofertas especiales ahora. Puedes ver el catálogo:",
        "post_order_options": "¿También te gustaría:\n\n1. Ver productos similares\n\n2. Ver ofertas especiales\n\n3. Terminar la conversación",
        "compare_need_recommendations": "Pide recomendaciones primero, y luego podré comparar las últimas opciones mostradas.",
        "compare_heading": "Aquí está la diferencia en resumen:",
        "trait_budget": "económico",
        "trait_premium": "gama superior",
        "trait_fast": "rápido",
        "trait_strong": "potente",
        "trait_daily": "adecuado para uso diario",
        "trait_default": "opción adecuada según el catálogo",
        "business_fashion_q1": "Claro, puedo ayudarte a elegir la mejor opción 👌\n¿Qué talla y color prefieres? ¿Formal o casual?",
        "business_fashion_q2": "Para afinar la elección: ¿qué talla, qué color y es para uso formal o diario?",
        "business_electronics_q1": "Perfecto, para recomendar con precisión: ¿cuál es tu presupuesto? ¿Batería, rendimiento o almacenamiento es lo más importante?",
        "business_electronics_q2": "Puedo ayudarte. ¿Buscas buena batería, rendimiento rápido o una opción económica? ¿Cuál es tu presupuesto?",
        "business_restaurant_q1": "Claro 👌 ¿Qué tipo de comida prefieres? ¿Picante o normal? ¿Necesitas entrega?",
        "business_restaurant_q2": "Déjame elegir lo mejor: ¿prefieres una comida ligera o abundante? ¿Picante? ¿Con entrega?",
        "business_general_q1": "Claro, puedo ayudarte a elegir la mejor opción 👌\n¿Prefieres una opción económica, premium, práctica o para uso diario?",
        "business_general_q2": "Para recomendar con precisión: ¿cuál es tu presupuesto? ¿Buscas mayor calidad, menor precio o un uso específico?",
    },
    "it": {
        "welcome": "Benvenuto da {business_name}.",
        "catalog_heading": "Catalogo {business_name}:",
        "price": "Prezzo",
        "sale_price": "Prezzo in offerta",
        "description": "Descrizione",
        "type": "Tipo",
        "duration": "Durata",
        "what_next": "Cosa vuoi fare?",
        "view_details": "1. Vedi dettagli",
        "buy_now": "2. Acquista ora",
        "back_catalog": "3. Torna al catalogo",
        "send_name_city": "🛒 Perfetto! Per completare l'ordine, invia:\n- Il tuo nome\n- La tua città",
        "order_received": "✅ Il tuo ordine è stato ricevuto con successo! Ti contatteremo presto.",
        "unknown": "Non ho capito la tua richiesta. Puoi scegliere:\n1. Catalogo\n2. Parlare con il supporto",
        "alternative_options": "Non ho un'opzione esatta, ma questa potrebbe piacerti:",
        "category_fallback_options": "Non ho trovato un'opzione esatta in questa categoria, ma posso suggerire alternative simili:",
        "send_product_name": "Invia il nome del prodotto o servizio per vedere i dettagli.",
        "catalog_already": "Il catalogo è già mostrato. Invia il nome di un prodotto o servizio.",
        "help_extra": "Posso aiutarti anche con: immagine, specifiche o prodotto simile.",
        "menu_question": "Come possiamo aiutarti oggi?",
        "menu_catalog": "1. Sfoglia il catalogo",
        "menu_prices": "2. Chiedere i prezzi",
        "menu_booking": "3. Prenotare un appuntamento",
        "menu_support": "4. Parlare con il supporto",
        "menu_hint": "Puoi anche inviare il nome del prodotto o servizio che cerchi.",
        "recommend_intro_1": "Ti consiglio questo perché è vicino alla tua richiesta 👌",
        "recommend_intro_2": "Questa è un'opzione adatta in base ai dettagli che hai indicato.",
        "recommend_intro_3": "Questo prodotto può andare bene per valore e specifiche.",
        "unsure_intro_1": "Nessun problema, te lo semplifico 👌",
        "unsure_intro_2": "È normale avere dubbi. Divido le opzioni per uso e budget 😊",
        "unsure_intro_3": "Capisco la tua indecisione. Ecco scelte semplici in base alle tue esigenze:",
        "reason_high_quality": "Se cerchi una qualità superiore",
        "reason_budget": "Se preferisci un'opzione economica",
        "reason_mid": "Se vuoi un'opzione intermedia",
        "reason_alternative": "Se vuoi un'alternativa diversa",
        "unsure_outro": "Dimmi il budget, l'uso o la caratteristica più importante — e ti consiglierò l'opzione più vicina.",
        "reject_options": "Nessun problema 😊\nPreferisci che ti suggerisca:\n1. Un'opzione di qualità superiore\n2. Un'opzione con specifiche migliori\n3. Un'opzione a prezzo più basso\n4. Tornare al catalogo",
        "price_intro_1": "Capisco. Non dobbiamo partire dall'opzione più costosa 😊 Ecco una scelta più intelligente nel prezzo:",
        "price_intro_2": "Hai ragione, il prezzo conta. Queste opzioni sono più leggere per il budget e vicine alla tua richiesta:",
        "price_intro_3": "Nessun problema 👌 Passo a un'opzione più adatta nel prezzo:",
        "price_outro_1": "Posso confrontarla con le opzioni precedenti se vuoi.",
        "price_outro_2": "Quale opzione rientra nel tuo budget?",
        "price_outro_3": "Vuoi che scelga il miglior rapporto qualità-prezzo?",
        "premium_intro_1": "Se cerchi una qualità superiore, questo potrebbe piacerti molto:",
        "premium_intro_2": "Se preferisci la fascia superiore, queste sono le opzioni più vicine:",
        "premium_intro_3": "Queste sono raccomandazioni premium adatte alla tua richiesta:",
        "premium_outro_1": "Quale opzione ti è piaciuta di più? 😊",
        "premium_outro_2": "Vuoi i dettagli di una di queste?",
        "premium_outro_3": "Quale sembra più vicina alla tua esigenza?",
        "details_already": "Stai già visualizzando i dettagli del prodotto.",
        "no_items": "Benvenuto da {business_name}.\nNon sono ancora stati aggiunti prodotti o servizi. Riprova più tardi o contatta il supporto.",
        "closed_thanks": "Grazie per averci contattato ❤️",
        "search_more": "Invia un termine di ricerca per vedere risultati più specifici.",
        "price_request_prompt": "Certo 👌\nInvia il nome del prodotto o servizio di cui vuoi conoscere il prezzo.",
        "booking_prompt": "📅 Per prenotare un appuntamento, invia:\n- Il tuo nome\n- Il servizio richiesto\n- Giorno preferito\n- Orario preferito",
        "support_prompt": "🎧 Sei stato trasferito al supporto.\nInvia la tua domanda e ti aiuteremo il prima possibile.",
        "menu_already": "Il menu è già mostrato. Invia un numero di opzione o il nome di un prodotto.",
        "similar_products_intro": "Certo, ecco prodotti simili che potrebbero piacerti:",
        "special_offers_intro": "Ecco alcune offerte speciali disponibili ora:",
        "order_product_prompt": "Per ordinare, invia il nome del prodotto che ti è piaciuto.",
        "no_special_offers": "Al momento non ci sono offerte speciali. Puoi sfogliare il catalogo:",
        "post_order_options": "Vuoi anche:\n\n1. Vedere prodotti simili\n\n2. Vedere offerte speciali\n\n3. Terminare la conversazione",
        "compare_need_recommendations": "Chiedi prima delle raccomandazioni, poi potrò confrontare le ultime opzioni mostrate.",
        "compare_heading": "Ecco la differenza in breve:",
        "trait_budget": "economico",
        "trait_premium": "fascia superiore",
        "trait_fast": "veloce",
        "trait_strong": "potente",
        "trait_daily": "adatto all'uso quotidiano",
        "trait_default": "opzione adatta secondo il catalogo",
        "business_fashion_q1": "Certo, posso aiutarti a scegliere l'opzione migliore 👌\nChe taglia e colore preferisci? Formale o casual?",
        "business_fashion_q2": "Per restringere la scelta: che taglia, quale colore, e per uso formale o quotidiano?",
        "business_electronics_q1": "Perfetto, per consigliare con precisione: qual è il tuo budget? Batteria, prestazioni o memoria sono più importanti?",
        "business_electronics_q2": "Posso aiutarti. Cerchi una batteria forte, prestazioni veloci o un'opzione economica? Qual è il tuo budget?",
        "business_restaurant_q1": "Certo 👌 Che tipo di pasto preferisci? Piccante o normale? Hai bisogno della consegna?",
        "business_restaurant_q2": "Fammi scegliere il meglio: preferisci un pasto leggero o abbondante? Piccante? Con consegna?",
        "business_general_q1": "Certo, posso aiutarti a scegliere l'opzione migliore 👌\nPreferisci un'opzione economica, premium, pratica o per uso quotidiano?",
        "business_general_q2": "Per consigliare con precisione: qual è il tuo budget? Cerchi qualità superiore, prezzo più basso o un uso specifico?",
    },
}


def t(lang, key):
    lang_code = (lang or "en").lower()
    return TRANSLATIONS.get(lang_code, TRANSLATIONS["en"]).get(key, TRANSLATIONS["en"].get(key, key))


def detect_intent(text):
    text = (text or "").strip().lower()

    reject_words = [
        "لا اريد", "لا أريد", "لا يعجبني", "غير مناسب",
        "ليس هذا", "مش هذا", "أريد شيء آخر", "اريد شيء اخر",
        "غيره", "بدل", "no", "not this", "i don't want", "dont want",
        "je ne veux pas", "pas ça", "no quiero", "no lo quiero", "non voglio", "non questo"
    ]

    price_words = ["غالي", "السعر مرتفع", "أريد أرخص", "أرخص", "ارخص", "رخيص", "cheap", "budget", "too expensive", "expensive", "moins cher", "pas cher", "cher", "barato", "económico", "economico"]
    premium_words = ["أفخم", "فاخر", "فخم", "راقي", "premium", "luxury", "elegant", "high end", "luxe", "lujo", "lusso"]

    if any(word in text for word in reject_words):
        return "reject_product"

    if any(word in text for word in price_words):
        return "price_objection"

    if any(word in text for word in premium_words):
        return "premium_request"

    return "unknown"


def generate_bot_reply(client_id, sender_phone, message_text):
    text = (message_text or "").strip().lower()
    
    # ══════════════════════════════════════════════════════════════════════════════
    # STATE ROUTING ARCHITECTURE v2.0 - PRIORITY-BASED STATE MACHINE
    # ══════════════════════════════════════════════════════════════════════════════
    # 
    # PRIORITY ORDER (highest to lowest):
    # 1. Greeting Handler - السلام يرد فورًا
    # 2. Checkout Locked States - الشراء لا ينكسر
    # 3. Explicit Buy Intent - نية الشراء الصريحة
    # 4. Product Selection - اختيار المنتج
    # 5. Recommendation Engine - محرك التوصيات
    # 6. Semantic AI - الذكاء الدلالي
    # 7. Objection Logic - منطق الاعتراضات
    #
    # LOCKED CHECKOUT STATES (Purchase Lock Protection):
    # - awaiting_order_info
    # - collecting_shipping  
    # - collecting_payment
    # - ready_to_buy
    #
    # When in locked states, DISABLE:
    # - objection detection
    # - semantic analyzer
    # - recommendation engine
    # - upsell logic
    # - alternative products
    # ══════════════════════════════════════════════════════════════════════════════
    
    # ════════════════════════════════════════════════════════════════════════
    # PRIORITY 1: GREETING GUARD - FAST GREETING HANDLER
    # ════════════════════════════════════════════════════════════════════════
    # This MUST be checked FIRST before any other logic to ensure greetings
    # are responded to immediately without interference from other systems.
    
    def _is_pure_greeting(msg_text):
        """Check if message is a pure greeting that should be handled immediately."""
        normalized = msg_text.strip().lower()
        # Remove common punctuation for matching
        normalized = normalized.replace("!", "").replace("؟", "").replace("?", "").replace(".", "").replace(",", "").strip()
        
        # Pure greeting patterns (exact match or very close)
        pure_greetings = {
            # Arabic greetings
            "السلام عليكم",
            "السلام عليكم ورحمه الله",
            "السلام عليكم ورحمه الله وبركاته",
            "السلام عليكم ورحمة الله",
            "السلام عليكم ورحمة الله وبركاته",
            "سلام",
            "سلام عليكم",
            "مرحبا",
            "مرحبًا",
            "اهلا",
            "أهلا",
            "اهلا وسهلا",
            "أهلا وسهلا",
            "هلا",
            "هلا وغلا",
            "هاي",
            "صباح الخير",
            "مساء الخير",
            "صباح النور",
            "مساء النور",
            # English greetings
            "hello",
            "hi",
            "hey",
            "good morning",
            "good evening",
            "good afternoon",
            # French greetings
            "bonjour",
            "bonsoir",
            "salut",
            # Spanish greetings
            "hola",
            "buenos dias",
            "buenas tardes",
            "buenas noches",
        }
        
        # Check for exact match
        if normalized in pure_greetings:
            return True
        
        # Check if message starts with greeting and is short (< 5 words)
        words = normalized.split()
        if len(words) <= 4:
            first_word = words[0] if words else ""
            greeting_starters = {"السلام", "سلام", "مرحبا", "مرحبًا", "اهلا", "أهلا", "هلا", "هاي", "صباح", "مساء", "hello", "hi", "hey", "bonjour", "hola", "salut"}
            if first_word in greeting_starters:
                return True
        
        return False
    
    # GREETING GUARD: Check immediately before any other processing
    if _is_pure_greeting(text):
        print(f"[GREETING_GUARD] Pure greeting detected: {text!r}")
        # Return greeting response immediately - bypass ALL other systems
        greeting_responses = {
            "ar": "وعليكم السلام 👋 كيف يمكنني مساعدتك؟ يمكنك أن تطلب منتجًا أو خدمة من الكتالوج.",
            "en": "Hello! 👋 How can I help you? You can ask for any product or service from our catalog.",
            "fr": "Bonjour! 👋 Comment puis-je vous aider? Vous pouvez demander un produit ou service de notre catalogue.",
        }
        # Default to Arabic for Arabic greetings
        arabic_greetings = {"السلام", "سلام", "مرحبا", "اهلا", "هلا", "صباح", "مساء"}
        detected_lang = "ar" if any(g in text for g in arabic_greetings) else "en"
        return greeting_responses.get(detected_lang, greeting_responses["ar"])
    
    # ═══════════════════════════════════════════════════════════════════════════
    # GREETING FAST-PATH (GUARANTEED - BEFORE ALL ANALYSIS)
    # ═══════════════════════════════════════════════════════════════════════════
    # Fix regression: Prevent greeting fast-path for product requests
    normalized_greeting = " ".join(text.replace("!", " ").replace("؟", " ").replace(".", " ").replace(",", " ").split())
    greeting_patterns = {
        "سلام",
        "السلام عليكم",
        "السلام عليكم ورحمه الله",
        "السلام عليكم ورحمه الله وبركاته",
        "السلام عليكم ورحمه الله",
        "السلام عليكم ورحمه الله وبركاته",
        "مرحبا",
        "اهلا",
        "اهلا وسهلا",
        "أهلا",
        "أهلا وسهلا",
        "هلا",
        "هاي",
        "hello",
        "hi",
        "hey",
        "bonjour",
        "bonsoir",
        "hola",
        "ciao",
    }
    
    # Product request keywords to prevent greeting fast-path
    product_keywords = {
        "أريد", "ابغى", "بغيت", "عندكم", "سعر", "ثمن", 
        "هاتف", "آيفون", "حذاء", "سماعات", "لابتوب", "منتج",
        "want", "need", "looking for", "price", "do you have"
    }
    
    # Check if the message is a pure greeting
    is_pure_greeting = normalized_greeting in greeting_patterns
    
    # Check if the message contains any product request keywords
    has_product_keywords = any(keyword in text.lower() for keyword in product_keywords)
    
    if is_pure_greeting and not has_product_keywords:
        print(f"[GREETING_FAST_PATH_TRIGGERED] phone={sender_phone} text={message_text!r}")
        return "وعليكم السلام 👋 كيف يمكنني مساعدتك؟ يمكنك أن تطلب منتجًا أو خدمة من الكتالوج."
    # ═══════════════════════════════════════════════════════════════════════════
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMPOUND ARABIC MESSAGE PARSER (BEFORE SEMANTIC AI / FALLBACK)
    # ═══════════════════════════════════════════════════════════════════════════
    def _normalize_arabic_text(text_value):
        """Normalize Arabic text for consistent parsing."""
        normalized = (text_value or "").strip()
        # Common Arabic typo/variant corrections
        replacements = {
            "عريد": "اريد",
            "أريد": "اريد",
            "بغيت": "اريد",
            "نريد": "اريد",
            "أ": "ا",
            "إ": "ا",
            "آ": "ا",
            "ة": "ه",
            "ى": "ي",
            "ً": "",
            "ٌ": "",
            "ٍ": "",
            "َ": "",
            "ُ": "",
            "ِ": "",
            "ّ": "",
            "ْ": "",
        }
        for source, target in replacements.items():
            normalized = normalized.replace(source, target)
        # Strip extra spaces
        normalized = " ".join(normalized.split())
        return normalized
    
    def _detect_checkout_intent(normalized_text):
        """Detect if message contains checkout/purchase intent."""
        checkout_terms = [
            "الشراء",
            "شراء",
            "اكمال الشراء",
            "اريد هذا",
            "نعم اريد",
            "عند الاستلام",
            "الدفع عند الاستلام",
            "اشتري",
            "اطلب",
            "اريد شراء",
        ]
        for term in checkout_terms:
            if term in normalized_text:
                print(f"[CHECKOUT_INTENT_DETECTED] term={term!r} in text={normalized_text!r}")
                return True
        return False
    
    def _extract_customer_name(normalized_text):
        """Extract customer name from Arabic text patterns."""
        import re
        # Patterns: اسمي X, الاسم X, أنا X, انا X
        name_patterns = [
            r"اسمي\s+([^\s,،]+)",
            r"الاسم\s+([^\s,،]+)",
            r"انا\s+([^\s,،]+)",
        ]
        for pattern in name_patterns:
            match = re.search(pattern, normalized_text)
            if match:
                name = match.group(1).strip()
                # Filter out common non-name words
                non_names = {"من", "في", "الى", "على", "عند", "نواكشوط", "انواكشوط", "الدفع"}
                if name and name not in non_names:
                    print(f"[EXTRACTED_NAME] name={name!r} pattern={pattern!r}")
                    return name
        return None
    
    def _extract_customer_city(normalized_text, extracted_name=None):
        """Extract city/address from Arabic text."""
        # Known cities
        known_cities = ["نواكشوط", "انواكشوط", "nouakchott"]
        normalized_lower = normalized_text.lower()
        
        for city in known_cities:
            if city in normalized_lower:
                print(f"[EXTRACTED_CITY] city={city!r}")
                return city
        
        # Try to extract location after name pattern
        if extracted_name:
            import re
            # Look for text after "اسمي NAME LOCATION"
            pattern = rf"اسمي\s+{re.escape(extracted_name)}\s+([^\s,،]+)"
            match = re.search(pattern, normalized_text)
            if match:
                potential_city = match.group(1).strip()
                # Filter out payment terms
                payment_terms = {"الدفع", "عند", "الاستلام", "كاش", "بطاقه"}
                if potential_city and potential_city not in payment_terms:
                    print(f"[EXTRACTED_CITY] city={potential_city!r} (after name)")
                    return potential_city
        
        return None
    
    def _extract_payment_method(normalized_text):
        """Extract payment method from Arabic text."""
        if "عند الاستلام" in normalized_text or "الدفع عند الاستلام" in normalized_text:
            print(f"[EXTRACTED_PAYMENT] method=cod")
            return "cod"
        if "كاش" in normalized_text or "نقد" in normalized_text:
            print(f"[EXTRACTED_PAYMENT] method=cash")
            return "cash"
        if "بطاقه" in normalized_text or "فيزا" in normalized_text or "visa" in normalized_text.lower():
            print(f"[EXTRACTED_PAYMENT] method=card")
            return "card"
        if "تحويل" in normalized_text:
            print(f"[EXTRACTED_PAYMENT] method=transfer")
            return "transfer"
        return None
    
    def _extract_product_from_compound(normalized_text, original_text):
        """Extract product name/reference from compound message."""
        import re
        # Common product patterns in Arabic
        product_patterns = [
            r"(ايفون\s*\d+)",
            r"(iphone\s*\d+)",
            r"(سامسونج\s*[a-zA-Z]*\s*\d+)",
            r"(samsung\s*[a-zA-Z]*\s*\d+)",
            r"(هواوي\s*[a-zA-Z]*\s*\d*)",
            r"(شاومي\s*[a-zA-Z]*\s*\d*)",
        ]
        original_lower = original_text.lower()
        for pattern in product_patterns:
            match = re.search(pattern, original_lower, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        # Also check normalized text
        for pattern in product_patterns:
            match = re.search(pattern, normalized_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _parse_compound_arabic_message(original_text):
        """
        Parse compound Arabic messages that contain multiple intents.
        Example: "أريد شراء آيفون 12 اسمي ناتو من نواكشوط الدفع عند الاستلام"
        Returns dict with extracted fields and flags.
        """
        normalized = _normalize_arabic_text(original_text)
        print(f"[COMPOUND_PARSE] original={original_text!r} normalized={normalized!r}")
        
        result = {
            "has_checkout_intent": False,
            "customer_name": None,
            "customer_city": None,
            "payment_method": None,
            "product_reference": None,
            "skip_fallback": False,
        }
        
        # Detect checkout intent
        result["has_checkout_intent"] = _detect_checkout_intent(normalized)
        
        # Extract customer name
        result["customer_name"] = _extract_customer_name(normalized)
        
        # Extract city/address
        result["customer_city"] = _extract_customer_city(normalized, result["customer_name"])
        
        # Extract payment method
        result["payment_method"] = _extract_payment_method(normalized)
        
        # Extract product reference
        result["product_reference"] = _extract_product_from_compound(normalized, original_text)
        
        # Determine if we should skip fallback/recommendation
        if result["has_checkout_intent"]:
            result["skip_fallback"] = True
            print(f"[SKIP_FALLBACK_DUE_TO_CHECKOUT] parsed={result}")
        
        return result
    
    # Parse the compound message early
    compound_parsed = _parse_compound_arabic_message(message_text)
    
    active_convo = None

    con = get_db_connection()
    try:
        active_convo = con.execute("""
            SELECT *
            FROM conversations
            WHERE client_id=? AND phone=?
            ORDER BY updated_at DESC
            LIMIT 1
        """, (client_id, sender_phone)).fetchone()
        client = con.execute(
            "SELECT * FROM clients WHERE id=?",
            (client_id,)
        ).fetchone()

        catalog_rows = con.execute("""
            SELECT *
            FROM catalogs
            WHERE client_id=?
            ORDER BY id DESC
        """, (client_id,)).fetchall()
        items = [
            row for row in catalog_rows
            if row["is_active"] in (1, "1", True, None, "")
        ]

        alias_rows = con.execute("""
            SELECT ca.catalog_id, ca.alias
            FROM catalog_aliases ca
            JOIN catalogs c ON c.id = ca.catalog_id
            WHERE c.client_id=? AND c.is_active=1
        """, (client_id,)).fetchall()

        total_items_count = con.execute("""
            SELECT COUNT(*)
            FROM catalogs
            WHERE client_id=?
        """, (client_id,)).fetchone()[0]
    finally:
        con.close()

    print(
        f"[BOT_CATALOG_DEBUG] db={DB_FILE!r} client_id={client_id} "
        f"active_items={len(items)} total_items={total_items_count} aliases={len(alias_rows)} "
        f"message={message_text!r}"
    )
    print(f"[CATALOG_DEBUG] found {len(items)} items")

    business_name = client["name"] if client else "our business"
    currency = client["currency"] if client else ""
    saved_data = {}
    try:
        saved_data = json.loads(active_convo["collected_data"] or "{}") if active_convo else {}
    except (TypeError, ValueError, KeyError):
        saved_data = {}
    detected_lang = detect_language(message_text)
    saved_lang = saved_data.get("customer_language")
    default_lang = (client["default_language"] if client else "en") or "en"
    lang = detected_lang or saved_lang or default_lang
    lang = lang if lang in TRANSLATIONS else "en"
    is_ar = lang.lower().startswith("ar")

    aliases_by_item = {}
    for row in alias_rows:
        aliases_by_item.setdefault(row["catalog_id"], []).append(row["alias"] or "")

    def _field(item, name):
        try:
            return item[name] or ""
        except (KeyError, IndexError):
            return ""

    def _item_searchable(item):
        return " ".join([
            _field(item, "title"),
            _field(item, "type"),
            _field(item, "category"),
            _field(item, "description"),
            _field(item, "keywords"),
            _field(item, "tags"),
            _field(item, "ai_search_text"),
            _field(item, "ai_tags"),
            _field(item, "ai_intent"),
            _field(item, "ai_metadata"),
            _field(item, "ai_embedding_text"),
            _field(item, "ai_semantic_tags"),
            _field(item, "ai_searchable_intents"),
            _field(item, "ai_features"),
            _field(item, "ai_usage_contexts"),
            _field(item, "ai_category"),
            _field(item, "ai_subcategory"),
            _field(item, "ai_brand"),
            _field(item, "ai_product_identity"),
            _field(item, "ai_style"),
            _field(item, "ai_luxury_level"),
            _field(item, "ai_target_customer"),
            " ".join(aliases_by_item.get(item["id"], [])),
        ]).lower()

    def _normalize_catalog_text(value):
        normalized = (value or "").lower()
        replacements = {
            "أ": "ا",
            "إ": "ا",
            "آ": "ا",
            "ة": "ه",
            "ى": "ي",
            "ً": "",
            "ٌ": "",
            "ٍ": "",
            "َ": "",
            "ُ": "",
            "ِ": "",
            "ّ": "",
            "ْ": "",
        }
        for source, target in replacements.items():
            normalized = normalized.replace(source, target)
        return normalized

    def _semantic_tokens(value):
        normalized = _normalize_catalog_text(value)
        for char in [",", ".", "؟", "!", ":", ";", "\n", "\t", "[", "]", "{", "}", '"', "'"]:
            normalized = normalized.replace(char, " ")
        stop_words = {
            "أريد", "اريد", "ابي", "أبي", "ابغى", "بغيت", "احتاج", "شيء", "شي", "منتج", "واحد", "هذا", "هذه",
            "want", "need", "looking", "for", "product", "item", "something", "this", "the", "and", "with",
            "je", "veux", "cherche", "produit", "quiero", "busco", "producto", "voglio", "cerco", "prodotto"
        }
        normalized_stop_words = {_normalize_catalog_text(word) for word in stop_words}
        return {
            token.strip()
            for token in normalized.split()
            if len(token.strip()) > 1 and token.strip() not in normalized_stop_words
        }

    def _semantic_profile_text(item):
        return " ".join([
            _field(item, "ai_search_text"),
            _field(item, "ai_tags"),
            _field(item, "ai_intent"),
            _field(item, "ai_metadata"),
            _field(item, "ai_embedding_text"),
            _field(item, "ai_semantic_tags"),
            _field(item, "ai_searchable_intents"),
            _field(item, "ai_features"),
            _field(item, "ai_usage_contexts"),
            _field(item, "ai_category"),
            _field(item, "ai_subcategory"),
            _field(item, "ai_brand"),
            _field(item, "ai_product_identity"),
            _field(item, "ai_style"),
            _field(item, "ai_luxury_level"),
            _field(item, "ai_target_customer"),
        ])

    def _semantic_similarity_score(query_text, item):
        query_tokens = _semantic_tokens(query_text)
        if not query_tokens:
            return 0, []
        title_tokens = _semantic_tokens(_field(item, "title") + " " + " ".join(aliases_by_item.get(item["id"], [])))
        category_tokens = _semantic_tokens(_field(item, "category") + " " + _field(item, "type"))
        description_tokens = _semantic_tokens(_field(item, "description"))
        semantic_tokens = _semantic_tokens(_semantic_profile_text(item))
        exact_text = _normalize_catalog_text(query_text)
        fields = [
            ("title", title_tokens, 14),
            ("semantic", semantic_tokens, 18),
            ("category", category_tokens, 10),
            ("description", description_tokens, 4),
        ]
        score = 0
        matched = []
        for field_name, tokens, weight in fields:
            overlap = query_tokens & tokens
            if overlap:
                score += len(overlap) * weight
                matched.append(field_name)
        semantic_text = _normalize_catalog_text(_semantic_profile_text(item))
        title_text = _normalize_catalog_text(_field(item, "title") + " " + " ".join(aliases_by_item.get(item["id"], [])))
        if exact_text and exact_text in title_text:
            score += 24
            matched.append("title_exact")
        elif exact_text and exact_text in semantic_text:
            score += 20
            matched.append("semantic_phrase")
        if semantic_tokens:
            score += int((len(query_tokens & semantic_tokens) / max(len(query_tokens), 1)) * 25)
        return score, matched

    def _same_category(item, category):
        if not category:
            return True
        item_category = _normalize_catalog_text(_item_category(item))
        wanted = _normalize_catalog_text(category)
        return bool(wanted and (wanted in item_category or item_category in wanted))

    def _detect_requested_category():
        product_terms = {
            "shoe": ["حذاء", "حذاءا", "حذاءً", "احذيه", "جزمه", "shoes", "shoe", "sneaker", "sneakers", "chaussure", "chaussures", "zapato", "zapatos", "scarpa", "scarpe"],
            "phone": ["هاتف", "هاتفا", "هاتفًا", "جوال", "موبايل", "تليفون", "phone", "mobile", "smartphone", "téléphone", "telefono", "teléfono"],
            "perfume": ["عطر", "عطرا", "عطرًا", "عطور", "برفان", "perfume", "fragrance", "parfum", "profumo"],
            "clothes": ["ملابس", "لبس", "قميص", "بنطال", "تيشيرت", "clothes", "clothing", "shirt", "pants", "vêtements", "ropa", "vestiti"],
        }
        normalized_text = _normalize_catalog_text(text)
        for terms in product_terms.values():
            normalized_terms = [_normalize_catalog_text(term) for term in terms]
            if any(term in normalized_text for term in normalized_terms):
                matched = [
                    item for item in items
                    if any(term in _normalize_catalog_text(_field(item, "category") + " " + _field(item, "type") + " " + _field(item, "title") + " " + " ".join(aliases_by_item.get(item["id"], []))) for term in normalized_terms)
                ]
                if matched:
                    return _item_category(matched[0])
        return None

    def _catalog_text():
        return " ".join(_item_searchable(item) for item in items)

    def _detect_business_type():
        catalog_text = _catalog_text()
        business_signals = {
            "perfumes": ["perfume", "fragrance", "oud", "scent", "عطر", "عطور", "عود"],
            "fashion": ["fashion", "size", "sizes", "clothes", "shirt", "dress", "pants", "shoe", "shoes", "مقاس", "لون", "ملابس", "فستان", "حذاء"],
            "electronics": ["phone", "battery", "ram", "storage", "charger", "screen", "laptop", "camera", "هاتف", "بطارية", "ذاكرة", "شاحن"],
            "restaurant": ["restaurant", "meal", "food", "delivery", "spicy", "burger", "pizza", "وجبة", "مطعم", "توصيل", "حار", "طلب"],
        }
        scores = {
            business_type: sum(catalog_text.count(term) for term in terms)
            for business_type, terms in business_signals.items()
        }
        best_type, best_score = max(scores.items(), key=lambda pair: pair[1], default=("general", 0))
        return best_type if best_score > 0 else "general"

    business_type = _detect_business_type()

    def _business_questions():
        question_sets = {
            "fashion": [t(lang, "business_fashion_q1"), t(lang, "business_fashion_q2")],
            "electronics": [t(lang, "business_electronics_q1"), t(lang, "business_electronics_q2")],
            "restaurant": [t(lang, "business_restaurant_q1"), t(lang, "business_restaurant_q2")],
            "perfumes": [t(lang, "business_general_q1"), t(lang, "business_general_q2")],
            "general": [
                t(lang, "business_general_q1"),
                t(lang, "business_general_q2")
            ],
        }
        return random.choice(question_sets.get(business_type, question_sets["general"]))

    def _generic_request_detected():
        request_words = ["أريد", "اريد", "ابي", "أبي", "ابغى", "بغيت", "احتاج", "need", "want", "looking for"]
        commerce_words = ["منتج", "شي", "خيار", "اشتري", "product", "item", "option", "buy"]
        return text and any(word in text for word in request_words) and (
            any(word in text for word in commerce_words) or len(text.split()) <= 3
        )

    def _item_price(item):
        return item["sale_price"] if item["sale_price"] not in (None, "") else item["price"]

    def _format_price(item):
        price = _item_price(item)
        if price in (None, ""):
            return ""
        try:
            price_text = f"{float(price):g}"
        except (TypeError, ValueError):
            price_text = str(price)
        item_currency = item["currency"] if "currency" in item.keys() and item["currency"] else currency
        return f"{price_text} {item_currency}".strip()

    def _money(value, item=None):
        if value in (None, ""):
            return ""
        try:
            value_text = f"{float(value):g}"
        except (TypeError, ValueError):
            value_text = str(value)
        item_currency = item["currency"] if item and "currency" in item.keys() and item["currency"] else currency
        return f"{value_text} {item_currency}".strip()

    def format_product(product, index=None):
        title = _field(product, "name") or _field(product, "title")
        desc = _field(product, "description").strip()
        price = _money(_field(product, "price"), product)
        prefix = f"{index}. " if index else ""
        lines = [f"{prefix}🛍️ {title}".strip()]
        if price:
            lines.append(f"السعر: {price}")
        if desc:
            lines.append(f"الوصف: {desc}")
        return "\n\n".join(lines).strip()

    def _format_item(item, index=None):
        return format_product(item, index)

    def sanitize_customer_reply(reply):
        if not isinstance(reply, str):
            return reply
        blocked_prefixes = (
            "category:",
            "type:",
            "price:",
            "raw price:",
            "description:",
            "raw description:",
            "sale_price:",
            "keywords:",
            "ai_metadata:",
        )
        cleaned_lines = []
        for line in reply.splitlines():
            stripped = line.strip()
            normalized = stripped.lower()
            if any(prefix in normalized for prefix in blocked_prefixes):
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines).strip()

    def _with_product_image(item, caption):
        image_url = item["image_url"] if "image_url" in item.keys() and item["image_url"] else ""
        if image_url:
            if image_url.startswith("/"):
                image_url = url_for("static", filename=image_url.lstrip("/").replace("static/", "", 1), _external=True)
            return {"type": "image", "image_url": image_url, "caption": caption}
        return caption

    def _format_product_message(product):
        _profile_update(
            viewed=[product["id"]],
            favorite_categories=[_item_category(product)],
            last_product_id=product["id"],
            last_category=_item_category(product)
        )
        sales_intro = random.choice([t(lang, "recommend_intro_1"), t(lang, "recommend_intro_2"), t(lang, "recommend_intro_3")])
        lines = [
            sales_intro,
            "",
            _format_item(product),
            "",
            t(lang, "what_next"),
            t(lang, "view_details"),
            t(lang, "buy_now"),
            t(lang, "back_catalog"),
            "",
            t(lang, "help_extra"),
        ]
        return "\n".join(lines).strip()

    def _unsure_recommendations():
        ranked = sorted(items, key=lambda item: float(_item_price(item) or 0), reverse=True)
        balanced = sorted(items, key=lambda item: float(_item_price(item) or 0))
        picks = []
        if ranked:
            picks.append((t(lang, "reason_high_quality"), ranked[0]))
        if balanced:
            picks.append((t(lang, "reason_budget"), balanced[0]))
        if len(ranked) > 1:
            picks.append((t(lang, "reason_mid"), ranked[len(ranked) // 2]))
        for item in items:
            if item not in [pick[1] for pick in picks]:
                picks.append((t(lang, "reason_alternative"), item))
            if len(picks) >= 4:
                break
        lines = [
            random.choice([t(lang, "unsure_intro_1"), t(lang, "unsure_intro_2"), t(lang, "unsure_intro_3")])
        ]
        recommended = [product for _, product in picks]
        for reason, product in picks:
            lines.append(f"{reason} → {product['title']}")
        if recommended:
            existing_data = _read_convo_data(active_convo)
            shown_ids = existing_data.get("shown_product_ids") or []
            new_ids = [item["id"] for item in recommended[:4]]
            _state_update(
                current_step,
                data={
                    "shown_product_ids": shown_ids + [item_id for item_id in new_ids if item_id not in shown_ids],
                    "last_shown_product_ids": new_ids[:3],
                    "last_bot_message_type": "sales_guidance"
                },
                force=True
            )
            _profile_update(
                recommended=new_ids[:4],
                favorite_categories=[_item_category(item) for item in recommended[:4]],
                last_intent="unsure",
                last_product_id=new_ids[0] if new_ids else None
            )
        lines.append(t(lang, "unsure_outro"))
        return "\n".join(lines)

    def _find_product_by_text(search_text):
        if not search_text:
            return None
        for item in items:
            searchable_parts = [
                item["title"] or "",
                item["type"] or "",
                item["description"] or "",
                " ".join(aliases_by_item.get(item["id"], [])),
            ]
            searchable = " ".join(searchable_parts).lower()
            title = (item["title"] or "").lower()
            if search_text == title or search_text in searchable:
                return item
        return None

    def _catalog_search_allowed():
        try:
            return catalog_allowed is True and conversation_intent.get("intent") in {"product_request", "service_request"}
        except NameError:
            return False

    def _smart_catalog_matches(limit=4):
        if not _catalog_search_allowed():
            print(json.dumps({"log": "[CATALOG_GUARD_BLOCKED]", "phone": sender_phone, "source": "_smart_catalog_matches"}, ensure_ascii=False))
            return []
        if not text:
            return []
        def _normalize_query(value):
            return _normalize_catalog_text(value)

        stop_words = {
            "أريد", "اريد", "ابي", "أبي", "ابغى", "بغيت", "احتاج", "شيء", "شي", "شيئا", "شيئًا", "منتج", "واحد",
            "want", "need", "looking", "for", "product", "item", "something",
            "je", "veux", "cherche", "produit", "quiero", "busco", "producto", "voglio", "cerco", "prodotto"
        }
        normalized_text = _normalize_query(text)
        normalized_stop_words = {_normalize_query(word) for word in stop_words}
        intent_terms = {
            "sport": ["رياضي", "رياضة", "sport", "sports", "sporty", "sportif", "deportivo", "sportivo"],
            "lasting": ["ثابت", "ثبات", "يدوم", "long lasting", "lasting", "durable", "dure", "duradero", "duraturo"],
            "luxury": ["فاخر", "فخم", "راقي", "luxury", "premium", "luxe", "lujo", "lusso"],
            "budget": ["اقتصادي", "رخيص", "أرخص", "ارخص", "budget", "cheap", "affordable", "barato", "economico"],
            "daily": ["يومي", "عملي", "daily", "everyday", "practical", "quotidien", "diario", "giornaliero"],
            "strong": ["قوي", "أقوى", "اقوى", "strong", "powerful", "intense", "puissant", "potente", "forte"],
            "fresh": ["منعش", "fresh", "frais", "fresco"],
            "elegant": ["أنيق", "انيق", "elegant", "élégant", "elegante"],
        }
        product_terms = {
            "shoe": ["حذاء", "حذاءا", "حذاءً", "احذيه", "جزمه", "shoes", "shoe", "sneaker", "sneakers", "chaussure", "chaussures", "zapato", "zapatos", "scarpa", "scarpe"],
            "phone": ["هاتف", "هاتفا", "هاتفًا", "جوال", "موبايل", "تليفون", "phone", "mobile", "smartphone", "téléphone", "telefono", "teléfono"],
            "perfume": ["عطر", "عطرا", "عطرًا", "عطور", "برفان", "perfume", "fragrance", "parfum", "perfume", "profumo"],
        }
        query_tokens = [
            _normalize_query(token.strip())
            for token in normalized_text.replace(",", " ").replace(".", " ").replace("؟", " ").split()
            if len(token.strip()) > 1 and _normalize_query(token.strip()) not in normalized_stop_words
        ]
        expanded_terms = list(query_tokens)
        for terms in intent_terms.values():
            normalized_terms = [_normalize_query(term) for term in terms]
            if any(term in normalized_text for term in normalized_terms):
                expanded_terms.extend(normalized_terms)
        requested_category = _detect_requested_category()
        convo_data = _read_convo_data(active_convo)
        current_category = requested_category or convo_data.get("current_category") or convo_data.get("last_category")
        for terms in product_terms.values():
            normalized_terms = [_normalize_query(term) for term in terms]
            if any(term in normalized_text for term in normalized_terms):
                expanded_terms.extend(normalized_terms)
        if not expanded_terms:
            return []

        def _score_candidates(candidate_items):
            scored_candidates = []
            for item in candidate_items:
                score, semantic_matches = _semantic_similarity_score(normalized_text, item)
                matched_fields = set(semantic_matches)
                keywords = _normalize_query(_field(item, "keywords") + " " + _field(item, "tags"))
                for term in set(expanded_terms):
                    term = term.lower()
                    if not term:
                        continue
                    if term in keywords:
                        score += 9
                        matched_fields.add("keywords")
                if normalized_text in keywords:
                    score += 14
                if score >= 10 and matched_fields:
                    scored_candidates.append((score, item))
            scored_candidates.sort(key=lambda pair: pair[0], reverse=True)
            return scored_candidates

        candidate_items = items
        category_items = [item for item in candidate_items if _same_category(item, current_category)]
        if current_category and category_items:
            scored = _score_candidates(category_items)
            if not scored:
                scored = _score_candidates(candidate_items)
        else:
            scored = _score_candidates(candidate_items)
        return [item for _, item in scored[:limit]]

    def _recommend_products(kind):
        if not _catalog_search_allowed():
            print(json.dumps({"log": "[CATALOG_GUARD_BLOCKED]", "phone": sender_phone, "source": "_recommend_products", "kind": kind}, ensure_ascii=False))
            return ([], True)
        def _field(item, name):
            try:
                return item[name] or ""
            except (KeyError, IndexError):
                return ""

        def _searchable(item):
            return " ".join([
                _field(item, "title"),
                _field(item, "type"),
                _field(item, "category"),
                _field(item, "description"),
                _field(item, "keywords"),
                _field(item, "tags"),
                _field(item, "ai_search_text"),
                _field(item, "ai_tags"),
                _field(item, "ai_intent"),
                _field(item, "ai_metadata"),
                _field(item, "ai_embedding_text"),
                _field(item, "ai_semantic_tags"),
                _field(item, "ai_searchable_intents"),
                _field(item, "ai_features"),
                _field(item, "ai_style"),
                _field(item, "ai_usage_contexts"),
                _field(item, "ai_category"),
                _field(item, "ai_subcategory"),
                _field(item, "ai_brand"),
                _field(item, "ai_product_identity"),
                _field(item, "ai_luxury_level"),
                _field(item, "ai_target_customer"),
                " ".join(aliases_by_item.get(item["id"], [])),
            ]).lower()

        def _semantic_parts(item):
            return " ".join([
                _field(item, "ai_search_text"),
                _field(item, "ai_tags"),
                _field(item, "ai_intent"),
                _field(item, "ai_metadata"),
                _field(item, "ai_embedding_text"),
                _field(item, "ai_semantic_tags"),
                _field(item, "ai_searchable_intents"),
                _field(item, "ai_features"),
                _field(item, "ai_style"),
                _field(item, "ai_usage_contexts"),
                _field(item, "ai_category"),
                _field(item, "ai_subcategory"),
                _field(item, "ai_brand"),
                _field(item, "ai_product_identity"),
                _field(item, "ai_luxury_level"),
                _field(item, "ai_target_customer"),
            ]).lower()

        def _debug_score(item, matched_terms, score):
            print(
                "[RECOMMEND_DEBUG]\n"
                f"Product: {item['title']}\n"
                f"Matched: {', '.join(matched_terms) if matched_terms else '-'}\n"
                f"Score: {score}"
            )

        def _price(item):
            try:
                return float(_item_price(item) or 0)
            except (TypeError, ValueError):
                return 0

        data = _read_convo_data(active_convo)
        profile = _customer_profile(data)
        rejected_product_id = data.get("rejected_product_id")
        rejected_product_ids = set(profile.get("rejected_product_ids") or [])
        requested_category = _detect_requested_category()
        current_category = requested_category or data.get("current_category") or data.get("last_category") or profile.get("last_category")
        current_ids = []
        if active_convo:
            try:
                current_ids = json.loads(active_convo["known_catalog_ids_json"] or "[]")
            except (TypeError, ValueError):
                current_ids = []
        if not current_ids:
            current_ids = list(profile.get("last_recommendation_ids") or [])
        if not current_ids and profile.get("last_product_id"):
            current_ids = [profile["last_product_id"]]
        current_item = next((item for item in items if current_ids and item["id"] == current_ids[0]), None)
        current_price = _price(current_item) if current_item else None
        if current_item:
            print(json.dumps({
                "log": "[CUSTOMER_CONTEXT_USED]",
                "phone": sender_phone,
                "reason": "contextual_recommendation",
                "last_product_id": current_item["id"],
                "last_category": current_category,
                "last_intent": profile.get("last_intent"),
                "last_filters": profile.get("last_filters"),
            }, ensure_ascii=False))

        generic_intent_terms = {
            "premium": ["فاخر", "فخم", "راقي", "premium", "luxury", "high end", "elegant", "luxe", "lujo", "lusso"],
            "similar": ["similar", "مشابه", "شبيه", "same style", "نفس"],
            "strong": ["قوي", "strong", "powerful", "durable", "heavy duty", "puissant", "potente", "forte"],
            "fast": ["سريع", "fast", "speed", "quick", "rapid", "rapide", "rápido", "veloce"],
            "light": ["خفيف", "light", "compact", "portable", "léger", "ligero", "leggero"],
            "daily": ["يومي", "daily", "everyday", "casual", "quotidien", "diario", "giornaliero"],
            "gift": ["هدية", "gift", "present", "cadeau", "regalo"],
            "sport": ["رياضي", "رياضة", "sport", "sports", "sporty", "fitness", "gym", "running"],
        }
        request_terms = []
        for intent_name, terms in generic_intent_terms.items():
            if kind == intent_name or any(term in text for term in terms):
                request_terms.extend(terms)
        for preference in profile.get("preferences") or []:
            request_terms.extend(generic_intent_terms.get(preference, []))
        for style in profile.get("preferred_styles") or []:
            request_terms.append(str(style))
        for token in text.replace(",", " ").replace(".", " ").split():
            if len(token) > 2:
                request_terms.append(token)
        current_words = set(_searchable(current_item).split()) if current_item else set()
        scored_items = []
        shown_product_ids = set(data.get("shown_product_ids") or [])
        shown_product_ids.update(profile.get("last_recommendation_ids") or [])
        available_items = [
            item for item in items
            if (not rejected_product_id or item["id"] != rejected_product_id)
            and item["id"] not in rejected_product_ids
            and item["id"] != profile.get("last_product_id")
        ]
        category_items = [item for item in available_items if _same_category(item, current_category)]
        if category_items:
            available_items = category_items
        unseen_items = [item for item in available_items if item["id"] not in shown_product_ids]
        if unseen_items:
            available_items = unseen_items

        if kind not in ("price", "budget"):
            vector_semantic = {
                "intent": "recommendation",
                "category": current_category,
                "style": kind if kind in {"sport", "daily", "strong", "fresh", "elegant", "similar"} else None,
                "luxury_level": "premium" if kind == "premium" else None,
                "price_preference": kind if kind in {"premium", "budget"} else None,
                "usage_context": kind if kind in {"gift", "daily", "sport"} else None,
            }
            vector_items = _vector_hybrid_rank(text, vector_semantic, limit=4)
            available_ids = {item["id"] for item in available_items}
            vector_items = [item for item in vector_items if item["id"] in available_ids]
            if vector_items:
                return (vector_items[:2], False)

        if kind in ("price", "budget"):
            candidates = available_items
            category_candidates = [item for item in candidates if _same_category(item, current_category)]
            if category_candidates:
                candidates = category_candidates
            if current_price is not None:
                cheaper_candidates = [item for item in candidates if _price(item) < current_price]
                candidates = cheaper_candidates or [item for item in candidates if _price(item) <= current_price]
            candidates = sorted(candidates, key=_price)
            return (candidates[:2], not bool(candidates))

        for item in available_items:
            if current_item and item["id"] == current_item["id"]:
                continue

            item_price = _price(item)
            searchable = _searchable(item)
            semantic = _semantic_parts(item)
            words = set(searchable.split())
            score = 0
            matched_terms = []
            semantic_matches = 0

            if kind == "premium":
                if current_price is not None and item_price >= current_price:
                    score += 2
                score += min(item_price / 1000, 8)

            for term in request_terms:
                if term in searchable:
                    matched_terms.append(term)
                    score += 5
                if term in semantic:
                    semantic_matches += 1
                    score += 10

            if kind == "premium" and any(term in searchable for term in generic_intent_terms["premium"]):
                score += 5
            if kind == "similar" and current_item:
                current_semantic_words = set(_semantic_parts(current_item).split())
                item_semantic_words = set(semantic.split())
                overlap = len(current_semantic_words & item_semantic_words)
                score += min(overlap * 3, 18)
            if current_category and _same_category(item, current_category):
                score += 8
            if kind == "premium" and _field(item, "ai_luxury_level").lower() in {"premium", "luxury", "high_end", "high end"}:
                score += 10
            if kind in ("price", "budget") and current_price is not None and item_price <= current_price:
                score += 8
            if matched_terms and current_item and item["type"] == current_item["type"]:
                score += 2
            if matched_terms and current_words:
                score += min(len(words & current_words), 5)

            _debug_score(item, matched_terms, score)
            if score > 0 and (matched_terms or semantic_matches or current_category):
                scored_items.append({"score": score, "item": item})

        if not scored_items:
            fallback_candidates = available_items
            category_fallback = [item for item in fallback_candidates if _same_category(item, current_category)]
            if category_fallback:
                fallback_candidates = category_fallback
            if kind in ("price", "budget"):
                fallback_candidates = sorted(fallback_candidates, key=_price)
            elif kind == "premium":
                fallback_candidates = sorted(fallback_candidates, key=_price, reverse=True)
            elif current_item:
                fallback_candidates = sorted(
                    fallback_candidates,
                    key=lambda item: (
                        item["type"] == current_item["type"],
                        len(set(_searchable(item).split()) & current_words)
                    ),
                    reverse=True
                )
            return (fallback_candidates[:1], True)

        products = [{"score": scored_item["score"], "item": scored_item["item"]} for scored_item in scored_items]
        products = sorted(products, key=lambda x: x["score"], reverse=True)
        if kind == "premium":
            products = sorted(products, key=lambda x: (x["score"], _price(x["item"])), reverse=True)
        top_products = products[:4]
        if len(top_products) > 2:
            top_products = random.sample(top_products, 2)
        return ([product["item"] for product in top_products[:2]], False)

    def _semantic_intent_analyzer():
        """
        UPGRADED: Enhanced semantic intent analyzer with conversation memory, 
        improved Arabic understanding, buying intent detection, emotional tone,
        vague request handling, and confidence scoring.
        """
        # UPGRADE 1: Enhanced fallback with new fields for backward compatibility
        fallback = {
            "intent": "unknown",
            "category": None,
            "style": None,
            "luxury_level": None,
            "usage_context": None,
            "price_preference": None,
            "target_customer": None,
            "sentiment": "neutral",
            "urgency": "normal",
            "confidence": 0.0,
            # NEW FIELDS (backward compatible - optional)
            "emotional_state": "neutral",  # Goal 4: Emotional tone
            "buying_stage": "browsing",     # Goal 3: Buying intent
            "vague_request": False,         # Goal 5: Vague request handling
            "urgency_level": "normal",      # Goal 3: Urgency detection
        }
        
        # UPGRADE 2: Expanded local_map with enhanced Arabic language support
        local_map = {
            "style": {
                "sporty": ["رياضي", "رياضة", "للرياضة", "رياضيه", "sport", "sports", "sporty", "fitness", "gym", "running", "athletic"],
                "elegant": ["راقي", "أنيق", "انيق", "فخم", "راقيه", "أنيقة", "elegant", "classy", "sophisticated", "chic"],
                "casual": ["كاجوال", "يومي", "عملي", "عادي", "بسيط", "casual", "daily", "everyday", "normal", "simple"],
                "formal": ["رسمي", "فورمال", "formal", "official", "professional"],
                "modern": ["عصري", "حديث", "مودرن", "modern", "contemporary", "trendy"],
            },
            "usage_context": {
                "university": ["جامعة", "للجامعة", "جامعي", "كلية", "university", "college", "campus", "school"],
                "gym": ["جيم", "نادي", "تمرين", "رياضة", "gym", "fitness", "workout", "exercise"],
                "walking": ["مشي", "للمشي", "walking", "walk", "hiking"],
                "gift": ["هدية", "هديه", "للهدية", "هدايا", "gift", "present", "gifting"],
                "work": ["عمل", "للعمل", "شغل", "مكتب", "work", "office", "professional"],
                "travel": ["سفر", "للسفر", "رحلة", "travel", "trip", "vacation"],
                "party": ["حفلة", "مناسبة", "party", "event", "occasion"],
            },
            "luxury_level": {
                "premium": ["فاخر", "فخم", "راقي", "غالي", "أفخم", "premium", "luxury", "high-end", "expensive"],
                "budget": ["أرخص", "ارخص", "رخيص", "اقتصادي", "بسعر", "cheap", "budget", "affordable", "economical"],
                "mid_range": ["متوسط", "مناسب", "معقول", "mid", "mid-range", "moderate", "reasonable"],
            },
            "target_customer": {
                "young": ["شاب", "شباب", "صغير", "young", "teen", "student", "طلاب", "طالب", "youth"],
                "male": ["رجالي", "للرجال", "رجال", "male", "men", "man"],
                "female": ["نسائي", "للنساء", "نساء", "بنات", "female", "women", "woman", "ladies"],
                "kids": ["أطفال", "اطفال", "طفل", "kids", "children", "child"],
            },
        }
        
        # UPGRADE 3: Enhanced buying intent detection (Arabic + English)
        buying_signals = {
            "immediate_buy": ["اشتري", "شراء", "احصل", "اطلب", "buy", "purchase", "order", "get", "احجز", "book"],
            "price_inquiry": ["كم", "سعر", "ثمن", "price", "cost", "how much", "بكام", "بكم"],
            "availability": ["متوفر", "موجود", "عندك", "لديك", "available", "in stock", "وين", "فين"],
        }
        
        # UPGRADE 4: Urgency detection (Arabic + English)
        urgency_signals = {
            "urgent": ["عاجل", "سريع", "اليوم", "الآن", "حالا", "ضروري", "urgent", "asap", "now", "today", "immediately", "quick"],
            "soon": ["قريب", "قريبا", "بسرعة", "soon", "shortly", "quickly"],
        }
        
        # UPGRADE 5: Emotional tone detection (Arabic + English)
        emotion_signals = {
            "happy": ["مبسوط", "سعيد", "فرحان", "ممتاز", "رائع", "happy", "great", "excellent", "perfect", "wonderful"],
            "frustrated": ["زعلان", "تعبت", "مش عارف", "confused", "frustrated", "tired", "difficult"],
            "confused": ["محتار", "مش فاهم", "ما فهمت", "confused", "don't understand", "unclear", "unsure"],
            "excited": ["متحمس", "حماس", "excited", "enthusiastic", "eager"],
        }
        
        # UPGRADE 6: Vague request patterns (Arabic + English)
        vague_patterns = [
            "شيء", "شي", "حاجة", "أي", "something", "anything", "whatever", "any",
            "أنت تختار", "انت اختار", "you choose", "you decide", "surprise me",
            "مش عارف", "ما أدري", "don't know", "not sure", "محتار"
        ]
        
        # Normalize text for pattern matching
        normalized = _normalize_catalog_text(text)
        
        # UPGRADE 7: Apply local pattern matching with enhanced vocabulary
        for field_name, options in local_map.items():
            for value, terms in options.items():
                if any(_normalize_catalog_text(term) in normalized for term in terms):
                    fallback[field_name] = value
        
        # UPGRADE 8: Detect buying intent and stage
        for signal_type, terms in buying_signals.items():
            if any(_normalize_catalog_text(term) in normalized for term in terms):
                if signal_type == "immediate_buy":
                    fallback["intent"] = "buy"
                    fallback["buying_stage"] = "ready_to_buy"
                    fallback["confidence"] = max(fallback["confidence"], 0.75)
                elif signal_type == "price_inquiry":
                    fallback["intent"] = "ask_details"
                    fallback["buying_stage"] = "considering"
                    fallback["confidence"] = max(fallback["confidence"], 0.65)
                elif signal_type == "availability":
                    fallback["buying_stage"] = "considering"
                    fallback["confidence"] = max(fallback["confidence"], 0.60)
        
        # UPGRADE 9: Detect urgency level
        for urgency_type, terms in urgency_signals.items():
            if any(_normalize_catalog_text(term) in normalized for term in terms):
                fallback["urgency"] = urgency_type
                fallback["urgency_level"] = urgency_type
                if urgency_type == "urgent":
                    fallback["buying_stage"] = "ready_to_buy"
                    fallback["confidence"] = max(fallback["confidence"], 0.70)
        
        # UPGRADE 10: Detect emotional state
        for emotion, terms in emotion_signals.items():
            if any(_normalize_catalog_text(term) in normalized for term in terms):
                fallback["emotional_state"] = emotion
                fallback["sentiment"] = "positive" if emotion in ["happy", "excited"] else "negative" if emotion == "frustrated" else "neutral"
        
        # UPGRADE 11: Detect vague requests
        if any(_normalize_catalog_text(pattern) in normalized for pattern in vague_patterns):
            fallback["vague_request"] = True
            # Slight confidence penalty for vague requests
            fallback["confidence"] = max(0.3, fallback["confidence"] - 0.1)
        
        # Existing intent detection (enhanced)
        if any(term in normalized for term in ["قارن", "compare", "الفرق", "difference", "vs"]):
            fallback["intent"] = "compare"
            fallback["confidence"] = max(fallback["confidence"], 0.65)
        if any(term in normalized for term in ["تفاصيل", "details", "معلومات", "info", "specifications", "مواصفات"]):
            fallback["intent"] = "ask_details"
            fallback["confidence"] = max(fallback["confidence"], 0.65)
        
        # UPGRADE 12: Boost confidence when multiple signals align
        if fallback["style"] or fallback["usage_context"] or fallback["luxury_level"] or fallback["target_customer"]:
            if fallback["intent"] == "unknown":
                fallback["intent"] = "recommendation"
            fallback["confidence"] = max(fallback["confidence"], 0.55)
            # Boost confidence if multiple attributes detected
            detected_attrs = sum([
                bool(fallback["style"]),
                bool(fallback["usage_context"]),
                bool(fallback["luxury_level"]),
                bool(fallback["target_customer"])
            ])
            if detected_attrs >= 2:
                fallback["confidence"] = min(0.85, fallback["confidence"] + 0.15)
        
        # Early return if AI client unavailable or message too short
        if not ai_client or len(text) < 2:
            print(f"[SEMANTIC_INTENT] {fallback}")
            return fallback
        
        # UPGRADE 13: Enhanced conversation memory context
        try:
            convo_data = _read_convo_data(active_convo) if active_convo else {}
            profile = _customer_profile(convo_data) if convo_data else {}
            
            conversation_memory = {
                "current_step": active_convo["current_step"] if active_convo else "greeting",
                "current_category": convo_data.get("current_category"),
                "last_intent": convo_data.get("last_intent"),
                "last_category": convo_data.get("last_category"),
                "last_filters": convo_data.get("last_filters", [])[:3],  # Last 3 filters
                # NEW: Enhanced memory context
                "viewed_products": (profile.get("viewed_product_ids") or [])[:5],  # Last 5 viewed
                "rejected_products": (profile.get("rejected_product_ids") or [])[:3],  # Last 3 rejected
                "purchased_history": (profile.get("purchased_product_ids") or [])[:3],  # Last 3 purchases
                "preferred_categories": profile.get("preferred_categories", [])[:3],
                "preferred_styles": profile.get("preferred_styles", [])[:3],
                "interaction_count": len(convo_data.get("shown_product_ids", [])),
                "conversation_depth": "deep" if len(convo_data.get("shown_product_ids", [])) > 5 else "shallow",
            }
        except Exception as e:
            print(f"[MEMORY_CONTEXT_ERROR] {repr(e)}")
            conversation_memory = {
                "current_step": active_convo["current_step"] if active_convo else "greeting",
                "current_category": None,
                "last_intent": None,
            }
        
        # UPGRADE 14: Enhanced AI prompt with new instructions
        try:
            model = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
            completion = ai_client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an advanced semantic commerce intent analyzer with deep Arabic language understanding. "
                            "Analyze customer messages considering conversation history, emotional tone, and cultural context. "
                            "Extract meaning and intent, not just keywords. "
                            "\n\nRETURN VALID JSON with these keys:\n"
                            "- intent: greeting|product_lookup|recommendation|ask_details|buy|reject|compare|main_menu|unknown\n"
                            "- category, style, luxury_level, usage_context, price_preference, target_customer: null or string\n"
                            "- sentiment: positive|negative|neutral\n"
                            "- urgency: normal|soon|urgent\n"
                            "- emotional_state: neutral|happy|frustrated|confused|excited|unsure\n"
                            "- buying_stage: browsing|considering|ready_to_buy\n"
                            "- vague_request: true|false (detect unclear/ambiguous requests)\n"
                            "- urgency_level: normal|soon|urgent\n"
                            "- confidence: 0.0 to 1.0\n"
                            "\n\nARABIC EXPERTISE:\n"
                            "- Understand dialectal variations (Gulf, Egyptian, Levantine, Mauritanian)\n"
                            "- Detect buying urgency: عاجل، الآن، اليوم، سريع، ضروري\n"
                            "- Recognize emotions: مبسوط، زعلان، محتار، متحمس\n"
                            "- Handle vague requests: شيء، حاجة، أي شي، انت اختار\n"
                            "\n\nCONFIDENCE SCORING:\n"
                            "- High (0.8-1.0): Clear intent with specific attributes\n"
                            "- Medium (0.5-0.8): Some context, moderate clarity\n"
                            "- Low (0.2-0.5): Vague or ambiguous\n"
                            "- Very Low (0.0-0.2): Unclear, needs clarification\n"
                            "\n\nUSE CONVERSATION MEMORY to understand context and improve accuracy."
                        )
                    },
                    {
                        "role": "user",
                        "content": json.dumps({
                            "message": message_text,
                            "language": lang,
                            "conversation_memory": conversation_memory,
                        }, ensure_ascii=False)
                    }
                ],
                temperature=0,
                response_format={"type": "json_object"}
            )
            parsed = json.loads(completion.choices[0].message.content or "{}")
            
            # UPGRADE 15: Enhanced fallback merging with new fields
            for key, value in fallback.items():
                parsed.setdefault(key, value)
            
            # UPGRADE 16: Improved confidence calculation
            try:
                ai_confidence = float(parsed.get("confidence") or 0.0)
                local_confidence = fallback["confidence"]
                
                # Multi-factor confidence: take maximum of AI and local pattern matching
                final_confidence = max(ai_confidence, local_confidence)
                
                # Boost confidence if conversation memory shows engagement
                if conversation_memory.get("interaction_count", 0) > 3:
                    final_confidence = min(1.0, final_confidence + 0.05)
                
                # Reduce confidence for vague requests
                if parsed.get("vague_request") or fallback["vague_request"]:
                    final_confidence = max(0.3, final_confidence - 0.1)
                
                parsed["confidence"] = max(0.0, min(1.0, final_confidence))
            except (TypeError, ValueError):
                parsed["confidence"] = fallback["confidence"]
            
            # UPGRADE 17: Preserve strong local matches
            if fallback["confidence"] > parsed.get("confidence", 0):
                for key in ("style", "usage_context", "luxury_level", "target_customer", "buying_stage", "urgency_level"):
                    if fallback.get(key) and fallback[key] != "normal" and fallback[key] != "browsing":
                        parsed[key] = fallback.get(key)
                if parsed.get("intent") in (None, "unknown") and fallback["intent"] != "unknown":
                    parsed["intent"] = fallback["intent"]
                parsed["confidence"] = max(parsed.get("confidence", 0), fallback["confidence"])
            
            print(f"[SEMANTIC_INTENT] {parsed}")
            return parsed
        except Exception as exc:
            print(f"[SEMANTIC_INTENT_ERROR] {repr(exc)}")
            print(f"[SEMANTIC_INTENT] {fallback}")
            return fallback

    def _semantic_rank_products(semantic_intent, limit=3):
        if not _catalog_search_allowed():
            print(json.dumps({"log": "[CATALOG_GUARD_BLOCKED]", "phone": sender_phone, "source": "_semantic_rank_products"}, ensure_ascii=False))
            return []
        if not semantic_intent or semantic_intent.get("intent") not in {"recommendation", "product_lookup"}:
            return []
        data = _read_convo_data(active_convo)
        profile = _customer_profile(data)
        rejected_product_id = data.get("rejected_product_id")
        rejected_product_ids = set(profile.get("rejected_product_ids") or [])
        current_product_id = data.get("current_product") or profile.get("last_product_id")
        viewed = set((profile.get("viewed_products") or []) + (profile.get("viewed_product_ids") or []))
        recommended = set(profile.get("recommended_products") or [])
        purchased = set((profile.get("purchased_products") or []) + (profile.get("purchased_product_ids") or []))
        if current_product_id:
            print(json.dumps({
                "log": "[CUSTOMER_CONTEXT_USED]",
                "phone": sender_phone,
                "reason": "semantic_recommendation",
                "last_product_id": current_product_id,
                "last_category": profile.get("last_category"),
                "last_intent": profile.get("last_intent"),
                "last_filters": profile.get("last_filters"),
            }, ensure_ascii=False))

        def _tokens(value):
            return {
                _normalize_catalog_text(token)
                for token in " ".join(_json_list(value) if isinstance(value, (list, str)) else []).replace("_", " ").replace("-", " ").split()
                if len(_normalize_catalog_text(token)) > 1
            }

        def _metadata_terms(item):
            metadata = _json_object(_field(item, "ai_metadata"))
            terms = []
            for key in ("category", "subcategory", "brand", "identity", "style", "luxury_level", "price_segment"):
                if metadata.get(key):
                    terms.append(str(metadata.get(key)))
            for key in ("intent", "audience", "usage", "semantic_keywords"):
                terms.extend(_json_list(metadata.get(key)))
            return terms

        desired_terms = []
        for key in ("category", "style", "luxury_level", "usage_context", "target_customer", "price_preference"):
            value = semantic_intent.get(key)
            if value:
                desired_terms.extend(_json_list(value) if isinstance(value, list) else [str(value)])
        desired_terms.extend(profile.get("preferred_categories") or [])
        desired_terms.extend(profile.get("preferred_styles") or [])
        if profile.get("preferred_price_level"):
            desired_terms.append(str(profile.get("preferred_price_level")))
        desired_tokens = _tokens(desired_terms + [message_text])
        scored = []
        for item in items:
            if rejected_product_id and item["id"] == rejected_product_id:
                continue
            if item["id"] in rejected_product_ids:
                continue
            if current_product_id and item["id"] == current_product_id and semantic_intent.get("intent") == "recommendation":
                continue
            item_category = _normalize_catalog_text(_field(item, "ai_category") or _field(item, "category"))
            item_style = _normalize_catalog_text(_field(item, "ai_style"))
            item_luxury = _normalize_catalog_text(_field(item, "ai_luxury_level"))
            item_target = _normalize_catalog_text(_field(item, "ai_target_customer"))
            item_metadata = _json_object(_field(item, "ai_metadata"))
            usage_tokens = _tokens(_field(item, "ai_usage_contexts"))
            tag_tokens = _tokens(" ".join([
                _field(item, "ai_semantic_tags"),
                _field(item, "ai_tags"),
                _field(item, "ai_intent"),
            ]))
            search_tokens = _tokens(" ".join([
                _field(item, "ai_search_text"),
                _field(item, "ai_embedding_text"),
            ]))
            metadata_tokens = _tokens(_metadata_terms(item))
            all_tokens = usage_tokens | tag_tokens | search_tokens | metadata_tokens | {
                item_category,
                item_style,
                item_luxury,
                item_target,
                _normalize_catalog_text(_field(item, "ai_subcategory")),
            }
            score = 0.0
            reasons = []
            category = _normalize_catalog_text(str(semantic_intent.get("category") or ""))
            style = _normalize_catalog_text(str(semantic_intent.get("style") or ""))
            usage = _normalize_catalog_text(str(semantic_intent.get("usage_context") or ""))
            luxury = _normalize_catalog_text(str(semantic_intent.get("luxury_level") or ""))
            target = _normalize_catalog_text(str(semantic_intent.get("target_customer") or ""))
            price_preference = _normalize_catalog_text(str(semantic_intent.get("price_preference") or ""))
            if category and (category in item_category or category in all_tokens):
                score += 0.22
                reasons.append("category relevance")
            if style and (style == item_style or style in all_tokens):
                score += 0.24
                reasons.append(f"{style} style")
            if usage and (usage in usage_tokens or usage in all_tokens):
                score += 0.22
                reasons.append(f"{usage} usage")
            if luxury and (luxury == item_luxury or luxury in all_tokens):
                score += 0.14
                reasons.append(f"{luxury} luxury match")
            if target and (target in item_target or target in all_tokens):
                score += 0.10
                reasons.append("target customer match")
            overlap = desired_tokens & all_tokens
            if overlap:
                score += min(len(overlap) * 0.06, 0.24)
                reasons.append("semantic tags overlap")
            if price_preference in {"budget", "cheap", "affordable", "ارخص", "رخيص"}:
                score += 0.08
                reasons.append("price preference")
            if price_preference and price_preference == _normalize_catalog_text(str(item_metadata.get("price_segment") or "")):
                score += 0.10
                reasons.append("metadata price segment")
            if metadata_tokens & desired_tokens:
                score += min(len(metadata_tokens & desired_tokens) * 0.05, 0.20)
                reasons.append("ai_metadata match")
            if _item_category(item) in ((profile.get("favorite_categories") or []) + (profile.get("preferred_categories") or [])):
                score += 0.06
                reasons.append("customer preference memory")
            if item_style and item_style in {_normalize_catalog_text(style) for style in (profile.get("preferred_styles") or [])}:
                score += 0.06
                reasons.append("preferred style memory")
            if item["id"] in purchased:
                score += 0.04
                reasons.append("previous purchase category")
            if item["id"] in viewed or item["id"] in recommended:
                score -= 0.08
                reasons.append("already interacted")
            score = max(0.0, min(1.0, score))
            print(json.dumps({
                "log": "[RANKING_SCORE]",
                "product_id": item["id"],
                "title": item["title"],
                "score": round(score, 3),
                "match_reasons": reasons,
            }, ensure_ascii=False))
            if score >= 0.22 and reasons:
                scored.append({"score": score, "item": item, "match_reasons": reasons})
        scored.sort(key=lambda row: row["score"], reverse=True)
        for row in scored[:limit]:
            print(json.dumps({
                "log": "[PRODUCT_MATCH]",
                "product_id": row["item"]["id"],
                "score": round(row["score"], 3),
                "match_reasons": row["match_reasons"],
            }, ensure_ascii=False))
        return [row["item"] for row in scored[:limit]]

    def _format_recommendations(recommended_items, intro=None, outro=None, kind=None):
        fallback = False
        if isinstance(recommended_items, tuple):
            recommended_items, fallback = recommended_items
        existing_data = _read_convo_data(active_convo)
        existing_ids = existing_data.get("suggested_product_ids") or []
        shown_ids = existing_data.get("shown_product_ids") or []
        rejected_product_id = existing_data.get("rejected_product_id")
        existing_profile = _customer_profile(existing_data)
        rejected_product_ids = set(existing_profile.get("rejected_product_ids") or [])
        if not recommended_items:
            fallback = True
            fallback_pool = [
                item for item in items
                if not rejected_product_id or item["id"] != rejected_product_id
            ]
            fallback_pool = [item for item in fallback_pool if item["id"] not in rejected_product_ids]
            unseen_fallback_pool = [item for item in fallback_pool if item["id"] not in shown_ids]
            if unseen_fallback_pool:
                fallback_pool = unseen_fallback_pool
            recommended_items = random.sample(fallback_pool, 1) if fallback_pool else []
        if not recommended_items:
            return t(lang, "alternative_options")
        new_ids = [item["id"] for item in recommended_items[:2]]
        _state_update(
            "recommendation_flow",
            data={
                "suggested_product_ids": existing_ids + [item_id for item_id in new_ids if item_id not in existing_ids],
                "shown_product_ids": shown_ids + [item_id for item_id in new_ids if item_id not in shown_ids],
                "last_shown_product_ids": new_ids,
                "rejected_product_id": None,
                "last_bot_message_type": "recommendation_options",
                "last_question_type": "recommendation_followup",
                "last_question": "recommendation_followup",
                "last_intent": kind if isinstance(kind, str) else existing_data.get("last_intent"),
                "last_filters": _append_unique(existing_data.get("last_filters") or [], kind, 10) if isinstance(kind, str) else existing_data.get("last_filters"),
                "recommended_product_ids": new_ids,
                "last_category": _item_category(recommended_items[0]) if recommended_items else existing_data.get("last_category"),
                "current_category": existing_data.get("current_category") or (_item_category(recommended_items[0]) if recommended_items else existing_data.get("last_category"))
            },
            force=True
        )
        _profile_update(
            recommended=new_ids,
            favorite_categories=[_item_category(item) for item in recommended_items[:2]],
            last_intent=kind if isinstance(kind, str) else None,
            last_product_id=new_ids[0] if new_ids else None,
            last_category=_item_category(recommended_items[0]) if recommended_items else None,
            last_filters=[kind] if isinstance(kind, str) else None
        )
        messages = []
        if fallback:
            messages.append(t(lang, "category_fallback_options") if existing_data.get("current_category") else t(lang, "alternative_options"))
        if intro:
            messages.append(intro)
        messages.extend([
            _with_product_image(item, _format_item(item))
            for item in recommended_items[:2]
        ])
        if outro:
            messages.append(outro)
        return messages

    def _menu():
        return (
            f"{t(lang, 'welcome').format(business_name=business_name)}\n\n"
            f"{t(lang, 'menu_question')}\n"
            f"{t(lang, 'menu_catalog')}\n"
            f"{t(lang, 'menu_prices')}\n"
            f"{t(lang, 'menu_booking')}\n"
            f"{t(lang, 'menu_support')}\n\n"
            f"{t(lang, 'menu_hint')}"
        )

    def _read_convo_data(convo):
        if not convo:
            return {}
        try:
            return json.loads(convo["collected_data"] or "{}")
        except (TypeError, ValueError, KeyError):
            return {}

    def _default_customer_profile():
        return {
            "preferences": [],
            "budget": None,
            "favorite_categories": [],
            "preferred_categories": [],
            "preferred_styles": [],
            "preferred_price_level": None,
            "rejected_product_ids": [],
            "viewed_product_ids": [],
            "purchased_product_ids": [],
            "viewed_products": [],
            "recommended_products": [],
            "purchased_products": [],
            "last_intent": None,
            "last_product_id": None,
            "last_category": None,
            "last_filters": [],
            "last_recommendation_ids": [],
        }

    def _customer_profile(data=None):
        profile = dict(_default_customer_profile())
        source = data if data is not None else _read_convo_data(active_convo)
        saved_profile = source.get("customer_profile") if isinstance(source, dict) else {}
        if isinstance(saved_profile, dict):
            profile.update(saved_profile)
        profile["preferred_categories"] = list(dict.fromkeys(
            (profile.get("preferred_categories") or []) + (profile.get("favorite_categories") or [])
        ))
        profile["favorite_categories"] = list(dict.fromkeys(
            (profile.get("favorite_categories") or []) + (profile.get("preferred_categories") or [])
        ))
        profile["viewed_product_ids"] = list(dict.fromkeys(
            (profile.get("viewed_product_ids") or []) + (profile.get("viewed_products") or [])
        ))
        profile["viewed_products"] = list(dict.fromkeys(
            (profile.get("viewed_products") or []) + (profile.get("viewed_product_ids") or [])
        ))
        profile["purchased_product_ids"] = list(dict.fromkeys(
            (profile.get("purchased_product_ids") or []) + (profile.get("purchased_products") or [])
        ))
        profile["purchased_products"] = list(dict.fromkeys(
            (profile.get("purchased_products") or []) + (profile.get("purchased_product_ids") or [])
        ))
        if profile.get("preferred_price_level") in (None, "") and profile.get("budget") not in (None, ""):
            profile["preferred_price_level"] = "budget"
        for key in ("preferences", "favorite_categories", "preferred_categories", "preferred_styles", "rejected_product_ids", "viewed_product_ids", "viewed_products", "recommended_products", "purchased_product_ids", "purchased_products", "last_filters", "last_recommendation_ids"):
            if not isinstance(profile.get(key), list):
                profile[key] = []
        print(json.dumps({"log": "[CUSTOMER_MEMORY_LOAD]", "phone": sender_phone, "profile": profile}, ensure_ascii=False))
        return profile

    def _append_unique(values, value, limit=20):
        if value in (None, ""):
            return values[-limit:]
        updated = [existing for existing in values if existing != value]
        updated.append(value)
        return updated[-limit:]

    def _profile_update(preferences=None, budget=None, favorite_categories=None, preferred_styles=None, preferred_price_level=None, rejected=None, viewed=None, recommended=None, purchased=None, last_intent=None, last_product_id=None, last_category=None, last_filters=None):
        data = _read_convo_data(active_convo)
        profile = _customer_profile(data)
        for preference in preferences or []:
            profile["preferences"] = _append_unique(profile["preferences"], preference, 20)
            profile["preferred_styles"] = _append_unique(profile["preferred_styles"], preference, 20)
        for category in favorite_categories or []:
            profile["favorite_categories"] = _append_unique(profile["favorite_categories"], category, 20)
            profile["preferred_categories"] = _append_unique(profile["preferred_categories"], category, 20)
        for style in preferred_styles or []:
            profile["preferred_styles"] = _append_unique(profile["preferred_styles"], style, 20)
        for product_id in rejected or []:
            profile["rejected_product_ids"] = _append_unique(profile["rejected_product_ids"], product_id, 50)
        for product_id in viewed or []:
            profile["viewed_products"] = _append_unique(profile["viewed_products"], product_id, 30)
            profile["viewed_product_ids"] = _append_unique(profile["viewed_product_ids"], product_id, 30)
        for product_id in recommended or []:
            profile["recommended_products"] = _append_unique(profile["recommended_products"], product_id, 40)
        for product_id in purchased or []:
            profile["purchased_products"] = _append_unique(profile["purchased_products"], product_id, 40)
            profile["purchased_product_ids"] = _append_unique(profile["purchased_product_ids"], product_id, 40)
        if budget not in (None, ""):
            profile["budget"] = budget
            profile["preferred_price_level"] = "budget"
        if preferred_price_level:
            profile["preferred_price_level"] = preferred_price_level
        if last_intent:
            profile["last_intent"] = last_intent
        if last_product_id:
            profile["last_product_id"] = last_product_id
        if last_category:
            profile["last_category"] = last_category
        for item_filter in last_filters or []:
            profile["last_filters"] = _append_unique(profile["last_filters"], item_filter, 20)
        if recommended:
            profile["last_recommendation_ids"] = list(recommended)[-5:]
        data["customer_profile"] = profile
        print(json.dumps({"log": "[CUSTOMER_MEMORY_UPDATE]", "phone": sender_phone, "profile": profile}, ensure_ascii=False))
        _state_update(active_convo["current_step"] if active_convo else "greeting", data=data, force=True)

    def _extract_preferences():
        preference_terms = {
            "premium": ["premium", "فاخر", "فخم", "راقي", "luxe", "lujo", "lusso"],
            "cheap": ["cheap", "أرخص", "ارخص", "رخيص", "moins cher", "barato", "economico"],
            "luxury": ["luxury", "فاخر", "فخم", "luxe", "lujo", "lusso"],
            "strong": ["strong", "قوي", "أقوى", "اقوى", "powerful", "puissant", "potente", "forte"],
            "fresh": ["fresh", "منعش", "frais", "fresco", "fresco"],
            "elegant": ["elegant", "راقي", "أنيق", "انيق", "élégant", "elegante"],
            "budget": ["budget", "اقتصادي", "ميزانية", "affordable", "económico", "economico"],
        }
        found = []
        for preference, terms in preference_terms.items():
            if any(term in text for term in terms):
                found.append(preference)
        return found

    def _extract_budget():
        budget_markers = ["budget", "ميزانية", "سعر", "price", "prix", "precio", "prezzo"]
        if not any(marker in text for marker in budget_markers):
            return None
        numbers = []
        for token in text.replace(",", " ").replace(".", " ").split():
            cleaned = "".join(char for char in token if char.isdigit())
            if cleaned:
                try:
                    numbers.append(float(cleaned))
                except ValueError:
                    pass
        return max(numbers) if numbers else None

    def _item_category(item):
        return (_field(item, "category") or _field(item, "type") or "").strip()

    def _catalog_price(item):
        try:
            return float(item["sale_price"] if item["sale_price"] not in (None, "") else item["price"] or 0)
        except (TypeError, ValueError, KeyError):
            return 0.0

    def _query_embedding_text(query_text, semantic=None, profile=None):
        semantic = semantic or {}
        profile = profile or {}
        parts = [
            query_text,
            semantic.get("intent"),
            semantic.get("category"),
            semantic.get("style"),
            semantic.get("luxury_level"),
            semantic.get("usage_context"),
            semantic.get("price_preference"),
            semantic.get("target_customer"),
            profile.get("preferred_price_level"),
        ]
        parts.extend(profile.get("preferred_categories") or [])
        parts.extend(profile.get("preferred_styles") or [])
        parts.extend(profile.get("preferences") or [])
        return " ".join(str(part).strip() for part in parts if str(part or "").strip())

    def _matched_semantic_tags(query_text, item):
        query_tokens = _semantic_tokens(query_text)
        item_tokens = _semantic_tokens(_semantic_profile_text(item))
        return sorted(query_tokens & item_tokens)[:12]

    def _price_relevance(item, semantic=None, profile=None):
        semantic = semantic or {}
        profile = profile or {}
        preference = _normalize_catalog_text(str(semantic.get("price_preference") or profile.get("preferred_price_level") or ""))
        item_price = _catalog_price(item)
        all_prices = [_catalog_price(candidate) for candidate in items if _catalog_price(candidate) > 0]
        if not preference or not item_price or not all_prices:
            return 0.0
        low = min(all_prices)
        high = max(all_prices)
        if high <= low:
            return 0.05
        percentile = (item_price - low) / (high - low)
        if preference in {"budget", "cheap", "affordable", "ارخص", "رخيص", "اقتصادي"}:
            return max(0.0, 1.0 - percentile) * 0.12
        if preference in {"premium", "luxury", "expensive", "فاخر", "فخم", "راقي"}:
            return percentile * 0.12
        return 0.04 if 0.25 <= percentile <= 0.75 else 0.0

    def _vector_hybrid_rank(query_text=None, semantic=None, limit=4):
        query_text = query_text or message_text or ""
        semantic = semantic or {}
        data = _read_convo_data(active_convo)
        profile = _customer_profile(data)
        query_embedding = create_ai_embedding(_query_embedding_text(query_text, semantic, profile))
        if not query_embedding:
            return []
        threshold = float(os.getenv("AI_SEARCH_SIMILARITY_THRESHOLD", "0.22"))
        debug_enabled = os.getenv("AI_SEARCH_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
        rejected_ids = set(profile.get("rejected_product_ids") or [])
        viewed_ids = set((profile.get("viewed_product_ids") or []) + (profile.get("viewed_products") or []))
        recommended_ids = set(profile.get("recommended_products") or [])
        purchased_ids = set((profile.get("purchased_product_ids") or []) + (profile.get("purchased_products") or []))
        current_product_id = data.get("current_product") or profile.get("last_product_id")
        scored = []
        requested_category = _normalize_catalog_text(str(semantic.get("category") or data.get("current_category") or profile.get("last_category") or ""))
        requested_luxury = _normalize_catalog_text(str(semantic.get("luxury_level") or profile.get("preferred_price_level") or ""))
        preferred_categories = {_normalize_catalog_text(value) for value in (profile.get("preferred_categories") or [])}
        preferred_styles = {_normalize_catalog_text(value) for value in (profile.get("preferred_styles") or [])}
        for item in items:
            if item["id"] in rejected_ids:
                continue
            if current_product_id and item["id"] == current_product_id and semantic.get("intent") == "recommendation":
                continue
            item_embedding = _parse_embedding(_field(item, "ai_embedding"))
            if not item_embedding:
                continue
            similarity = max(0.0, cosine_similarity(query_embedding, item_embedding))
            metadata = _json_object(_field(item, "ai_metadata"))
            metadata_terms = " ".join(
                [str(metadata.get(key) or "") for key in ("category", "subcategory", "brand", "identity", "style", "luxury_level", "price_segment")]
                + _json_list(metadata.get("intent"))
                + _json_list(metadata.get("audience"))
                + _json_list(metadata.get("usage"))
                + _json_list(metadata.get("semantic_keywords"))
            )
            item_category = _normalize_catalog_text(metadata.get("category") or _field(item, "ai_category") or _field(item, "category") or _field(item, "type"))
            item_luxury = _normalize_catalog_text(metadata.get("luxury_level") or _field(item, "ai_luxury_level"))
            item_style = _normalize_catalog_text(metadata.get("style") or _field(item, "ai_style"))
            metadata_text = _normalize_catalog_text(metadata_terms)
            category_boost = 0.08 if requested_category and (requested_category in item_category or item_category in requested_category) else 0.0
            metadata_boost = 0.07 if query_text and any(token in metadata_text for token in _semantic_tokens(query_text)) else 0.0
            preference_boost = 0.0
            if item_category in preferred_categories:
                preference_boost += 0.05
            if item_style in preferred_styles:
                preference_boost += 0.05
            luxury_boost = 0.06 if requested_luxury and requested_luxury == item_luxury else 0.0
            price_boost = _price_relevance(item, semantic, profile)
            popularity_boost = 0.03 if item["id"] in purchased_ids else 0.0
            interaction_penalty = 0.06 if item["id"] in viewed_ids or item["id"] in recommended_ids else 0.0
            total_score = (similarity * 0.62) + category_boost + metadata_boost + preference_boost + luxury_boost + price_boost + popularity_boost - interaction_penalty
            matched_tags = _matched_semantic_tags(query_text, item)
            reasons = []
            if similarity:
                reasons.append(f"vector_similarity={similarity:.3f}")
            if category_boost:
                reasons.append("category boost")
            if metadata_boost:
                reasons.append("ai_metadata semantic boost")
            if preference_boost:
                reasons.append("customer preference boost")
            if luxury_boost:
                reasons.append("luxury match")
            if price_boost:
                reasons.append("price relevance")
            if popularity_boost:
                reasons.append("popularity/purchase signal")
            if interaction_penalty:
                reasons.append("recent interaction penalty")
            if debug_enabled:
                print(json.dumps({
                    "log": "[AI_SEARCH_DEBUG]",
                    "product_id": item["id"],
                    "title": item["title"],
                    "similarity": round(similarity, 4),
                    "hybrid_score": round(total_score, 4),
                    "matched_tags": matched_tags,
                    "why_selected": reasons,
                }, ensure_ascii=False))
            if similarity >= threshold:
                scored.append((total_score, similarity, item, reasons, matched_tags))
        scored.sort(key=lambda row: row[0], reverse=True)
        if not scored:
            print(json.dumps({
                "log": "[AI_SEARCH_NO_MATCH]",
                "threshold": threshold,
                "query": query_text,
            }, ensure_ascii=False))
            return []
        return [row[2] for row in scored[:limit]]

    valid_steps = {
        "greeting",
        "browsing_catalog",
        "recommendation_flow",
        "product_selected",
        "awaiting_order_info",
        "awaiting_booking_info",
        "order_confirmed",
        "post_order_engagement",
        "support_requested",
        "conversation_closed",
    }
    allowed_transitions = {
        "greeting": {"greeting", "browsing_catalog", "recommendation_flow", "product_selected", "awaiting_booking_info", "support_requested", "conversation_closed"},
        "browsing_catalog": {"browsing_catalog", "recommendation_flow", "product_selected", "awaiting_booking_info", "support_requested", "conversation_closed"},
        "recommendation_flow": {"recommendation_flow", "product_selected", "browsing_catalog", "awaiting_order_info", "conversation_closed"},
        "product_selected": {"product_selected", "recommendation_flow", "browsing_catalog", "awaiting_order_info", "conversation_closed"},
        "awaiting_order_info": {"awaiting_order_info", "order_confirmed", "greeting", "conversation_closed"},
        "awaiting_booking_info": {"awaiting_booking_info", "greeting", "conversation_closed"},
        "order_confirmed": {"post_order_engagement", "conversation_closed"},
        "post_order_engagement": {"post_order_engagement", "browsing_catalog", "product_selected", "conversation_closed"},
        "support_requested": {"support_requested", "greeting", "conversation_closed"},
        "conversation_closed": {"greeting", "conversation_closed"},
    }
    explicit_menu_requests = {
        "menu", "catalog", "products", "services", "browse", "القائمة", "الكتالوج", "المنتجات", "الخدمات", "تصفح"
    }
    explicit_reset_requests = {
        "restart", "start over", "main menu", "reset", "ابدأ من جديد", "ابدا من جديد", "القائمة الرئيسية", "العودة للبداية", "ارجع للبداية"
    }

    def _explicit_reset_requested():
        return text in explicit_reset_requests or any(phrase in text for phrase in explicit_reset_requests if " " in phrase)

    def _contextual_short_intent():
        short_intents = {
            "yes": ["نعم", "اي", "إي", "تمام", "اوك", "موافق", "اكمل", "أكمل", "تابع", "ok", "yes", "continue", "go on", "oui", "sí", "si"],
            "select_last": ["أريد هذا", "اريد هذا", "ابي هذا", "أبي هذا", "هذا", "اختار هذا", "اختر هذا", "i want this", "this one"],
            "no": ["لا", "no", "non"],
            "price": ["أقل", "اقل", "أرخص", "ارخص", "رخيص", "cheap", "budget", "moins cher", "barato", "economico"],
            "premium": ["فاخر", "فخم", "راقي", "premium", "luxury", "luxe", "lujo", "lusso"],
            "sport": ["رياضي", "رياضة", "sport", "sporty", "sportif", "deportivo", "sportivo"],
            "daily": ["يومي", "عملي", "daily", "everyday", "practical"],
            "strong": ["ثابت", "قوي", "أقوى", "اقوى", "strong", "lasting", "durable", "intense"],
            "fresh": ["منعش", "fresh", "frais", "fresco"],
        }
        for intent_name, terms in short_intents.items():
            if text in terms or any(term == text for term in terms):
                return intent_name
        return None

    def _fetch_active_convo():
        con = get_db_connection()
        try:
            return con.execute("""
                SELECT *
                FROM conversations
                WHERE client_id=? AND phone=?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
            """, (client_id, sender_phone)).fetchone()
        finally:
            con.close()

    def _state_update(step, known_ids=None, data=None, force=False):
        nonlocal active_convo
        def _conversation_state_for(step_name):
            if step_name in {"browsing_catalog", "recommendation_flow"}:
                return "filtering" if step_name == "recommendation_flow" else "browsing"
            if step_name == "product_selected":
                return "selecting_product"
            if step_name == "awaiting_order_info":
                return "confirming_purchase"
            if step_name == "support_requested":
                return "support"
            return "idle"

        if step not in valid_steps:
            step = "greeting"
        current_data = _read_convo_data(active_convo)
        previous_step = active_convo["current_step"] if active_convo else "greeting"
        if not force and step not in allowed_transitions.get(previous_step, {"greeting"}):
            step = previous_step if previous_step in {"recommendation_flow", "product_selected"} else "greeting"
        current_data["previous_step"] = previous_step
        current_data["customer_language"] = lang
        if data:
            current_data.update(data)
        current_data["current_step"] = step
        current_data["conversation_state"] = current_data.get("conversation_state") or _conversation_state_for(step)
        if data and "conversation_state" not in data:
            current_data["conversation_state"] = _conversation_state_for(step)
        known_json = json.dumps(known_ids, ensure_ascii=False) if known_ids is not None else None
        con = get_db_connection()
        try:
            if known_json is None:
                cur = con.execute("""
                    UPDATE conversations
                    SET current_step=?,
                        collected_data=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE client_id=? AND phone=?
                """, (
                    step,
                    json.dumps(current_data, ensure_ascii=False),
                    client_id,
                    sender_phone
                ))
                if cur.rowcount == 0:
                    con.execute("""
                        INSERT INTO conversations
                            (client_id, phone, current_step, collected_data, updated_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        client_id,
                        sender_phone,
                        step,
                        json.dumps(current_data, ensure_ascii=False)
                    ))
            else:
                cur = con.execute("""
                    UPDATE conversations
                    SET current_step=?,
                        known_catalog_ids_json=?,
                        collected_data=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE client_id=? AND phone=?
                """, (
                    step,
                    known_json,
                    json.dumps(current_data, ensure_ascii=False),
                    client_id,
                    sender_phone
                ))
                if cur.rowcount == 0:
                    con.execute("""
                        INSERT INTO conversations
                            (client_id, phone, current_step, known_catalog_ids_json, collected_data, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        client_id,
                        sender_phone,
                        step,
                        known_json,
                        json.dumps(current_data, ensure_ascii=False)
                    ))
            con.commit()
            active_convo = _fetch_active_convo()
            print("[STATE_REFRESHED]", dict(active_convo) if active_convo else None)
            print(f"[STATE_SAVED] {step}")
        finally:
            con.close()

    def _mark_last_message(message_type):
        nonlocal active_convo
        if not active_convo:
            return
        data = _read_convo_data(active_convo)
        data["previous_step"] = data.get("previous_step", active_convo["current_step"])
        data["last_bot_message_type"] = message_type
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE conversations
                SET collected_data=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE client_id=? AND phone=?
            """, (json.dumps(data, ensure_ascii=False), client_id, sender_phone))
            con.commit()
        finally:
            con.close()
        active_convo = _fetch_active_convo()

    def _was_recently_sent(message_type):
        if not active_convo:
            return False

    def _reset_checkout_session_state(reason="greeting"):
        if not active_convo:
            return
        _state_update(
            "greeting",
            known_ids=[],
            data={
                "current_product": None,
                "current_category": None,
                "pending_question": None,
                "last_question": None,
                "last_question_type": None,
                "last_bot_message_type": f"conversation_reset_{reason}",
                "purchase_flow": {},
                "checkout_state": "",
            },
            force=True,
        )
        _set_purchase_state("idle", {"reset_reason": reason, "collected": {}})

    def _normalize_checkout_token(value):
        token = _normalize_catalog_text(str(value or "").strip())
        return token

    def _is_valid_checkout_name(value):
        token = _normalize_checkout_token(value)
        if not token:
            return False
        if len(token) < 4:
            return False
        if token in {"نعم", "لا", "اوكي", "أوكي", "ok", "okay", "nato", "سلام", "مرحبا", "اهلا", "اهلاً", "هلا"}:
            return False
        parts = [part for part in token.split() if part]
        if len(parts) < 2 and len(token) <= 6:
            return False
        if any(term in token for term in ("سلام", "مرحبا", "السلام", "شكرا", "شكرًا")):
            return False
        return True

    def _is_valid_checkout_city(value):
        token = _normalize_checkout_token(value)
        if not token:
            return False
        if len(token) < 3:
            return False
        if token in {"نعم", "لا", "اوكي", "أوكي", "ok", "okay", "nato", "سلام", "مرحبا", "اهلا", "اهلاً", "هلا"}:
            return False
        if token in {"أنا", "انا", "me", "my", "mine"}:
            return False
        return True

    def _is_explicit_purchase_confirmation(message):
        token = _normalize_checkout_token(message)
        if not token:
            return False
        if token in {"نعم", "لا", "اوكي", "أوكي", "ok", "okay", "nato", "سلام", "مرحبا", "السلام عليكم"}:
            return False
        explicit_terms = [
            "اكد", "أكد", "أؤكد", "confirm", "confirm order", "اكمل الطلب", "أكمل الطلب",
            "اشتري", "أشتري", "شراء", "الشراء", "اطلب", "أطلب", "خذ", "خذه", "خذها",
            "اكيد", "أكيد",
        ]
        return any(term in token for term in explicit_terms)

    def _has_real_checkout_context(product=None, checkout_fields=None, explicit_confirmation=False):
        checkout_fields = checkout_fields or {}
        product_id = (product or {}).get("id") if isinstance(product, dict) else None
        customer_name = checkout_fields.get("name")
        city = checkout_fields.get("location")
        return bool(product_id and _is_valid_checkout_name(customer_name) and _is_valid_checkout_city(city) and explicit_confirmation)
        data = _read_convo_data(active_convo)
        return data.get("last_bot_message_type") == message_type

    def _set_message_and_return(message_type, response):
        _mark_last_message(message_type)
        return response

    def _core_session_manager():
        data = _read_convo_data(active_convo)
        profile = _customer_profile(data)
        flow = data.get("purchase_flow") if isinstance(data.get("purchase_flow"), dict) else {}
        session_state = {
            "conversation_state": data.get("conversation_state") or ("checkout" if flow.get("state") in {"ready_to_buy", "collecting_shipping", "collecting_payment"} else "idle"),
            "last_intent": data.get("last_intent") or profile.get("last_intent"),
            "current_product": data.get("current_product") or profile.get("last_product_id"),
            "current_category": data.get("current_category") or data.get("last_category") or profile.get("last_category"),
            "memory_context": profile,
            "last_recommendations": data.get("last_shown_product_ids") or data.get("suggested_product_ids") or profile.get("last_recommendation_ids") or [],
            "current_step": active_convo["current_step"] if active_convo else "greeting",
            "checkout_state": flow.get("state", "browsing"),
        }
        data["core_session"] = session_state
        print(json.dumps({"log": "[CORE_SESSION]", "phone": sender_phone, "session": session_state}, ensure_ascii=False))
        return session_state

    def _product_understanding(item):
        if not item:
            return {}
        metadata = _json_object(_field(item, "ai_metadata"))
        return {
            "id": item["id"],
            "title": _field(item, "title"),
            "category": _field(item, "ai_category") or _field(item, "category") or _field(item, "type"),
            "subcategory": _field(item, "ai_subcategory"),
            "usage": _json_list(_field(item, "ai_usage_contexts")),
            "target_customer": _field(item, "ai_target_customer"),
            "price": _format_price(item),
            "luxury_level": _field(item, "ai_luxury_level"),
            "style": _field(item, "ai_style"),
            "semantic_tags": _json_list(_field(item, "ai_semantic_tags")),
            "ai_metadata": metadata,
            "intent": metadata.get("intent") or _json_list(_field(item, "ai_searchable_intents")),
            "price_segment": metadata.get("price_segment") or _price_segment(_catalog_price(item)),
            "aliases": aliases_by_item.get(item["id"], []),
        }

    def _core_product_retrieval(query=None, semantic=None, limit=4):
        if not _catalog_search_allowed():
            print(json.dumps({"log": "[CATALOG_GUARD_BLOCKED]", "phone": sender_phone, "query": query or message_text}, ensure_ascii=False))
            return []
        query_text = _normalize_catalog_text(query or message_text or "")
        semantic = semantic or {}
        vector_ranked = _vector_hybrid_rank(query or message_text, semantic, limit=limit)
        semantic_ranked = _semantic_rank_products(semantic, limit=limit) if semantic.get("intent") in {"recommendation", "product_lookup"} else []
        smart_matches = [] if vector_ranked else _smart_catalog_matches(limit=limit)
        scored = {}
        def _add(item, score, source):
            if not item:
                return
            existing = scored.get(item["id"], {"score": 0, "sources": [], "item": item})
            existing["score"] += score
            existing["sources"].append(source)
            scored[item["id"]] = existing
        for index, item in enumerate(vector_ranked):
            _add(item, 130 - index * 8, "vector_hybrid")
        for index, item in enumerate(semantic_ranked):
            _add(item, 100 - index * 5, "semantic")
        for index, item in enumerate(smart_matches):
            _add(item, 70 - index * 5, "smart_match")
        if not vector_ranked:
            for item in items:
                searchable = _normalize_catalog_text(" ".join([
                    _field(item, "title"),
                    _field(item, "type"),
                    _field(item, "category"),
                    _field(item, "description"),
                    _field(item, "keywords"),
                    _field(item, "tags"),
                    _field(item, "ai_search_text"),
                    _field(item, "ai_tags"),
                    _field(item, "ai_intent"),
                    _field(item, "ai_embedding_text"),
                    _field(item, "ai_semantic_tags"),
                    " ".join(aliases_by_item.get(item["id"], [])),
                ]))
                if query_text and query_text in searchable:
                    _add(item, 60, "multilingual_alias")
                    continue
                ratio = difflib.SequenceMatcher(None, query_text, searchable[: max(len(query_text) * 3, 80)]).ratio() if query_text else 0
                if ratio >= 0.34:
                    _add(item, int(ratio * 50), "fuzzy")
        ranked = sorted(scored.values(), key=lambda row: row["score"], reverse=True)
        print(json.dumps({"log": "[PRODUCT_RETRIEVAL]", "phone": sender_phone, "query": query_text, "results": [{"id": row["item"]["id"], "score": row["score"], "sources": row["sources"]} for row in ranked[:limit]]}, ensure_ascii=False))
        return [row["item"] for row in ranked[:limit]]

    def _smart_context_injection(session_state, parsed=None, semantic=None):
        data = _read_convo_data(active_convo)
        related_products = []
        product_ids = []
        if session_state.get("current_product"):
            product_ids.append(session_state["current_product"])
        product_ids.extend(session_state.get("last_recommendations") or [])
        for product_id in _append_unique([], *[], limit=1) if False else []:
            pass
        seen = set()
        for product_id in product_ids:
            if product_id in seen:
                continue
            found = next((item for item in items if item["id"] == product_id), None)
            if found:
                related_products.append(_product_understanding(found))
                seen.add(product_id)
        retrieved = _core_product_retrieval(message_text, semantic, limit=3)
        for item in retrieved:
            if item["id"] not in seen:
                related_products.append(_product_understanding(item))
                seen.add(item["id"])
        context = {
            "business": {"name": business_name, "type": business_type, "currency": currency},
            "customer_message": message_text,
            "language": lang,
            "session": session_state,
            "memory": session_state.get("memory_context") or {},
            "conversation": {
                "current_step": session_state.get("current_step"),
                "conversation_state": session_state.get("conversation_state"),
                "last_intent": session_state.get("last_intent"),
                "current_category": session_state.get("current_category"),
                "checkout_state": session_state.get("checkout_state"),
            },
            "parsed_intent": parsed or {},
            "semantic_intent": semantic or {},
            "related_products": related_products[:5],
            "interests": {
                "preferences": (session_state.get("memory_context") or {}).get("preferences", []),
                "preferred_categories": (session_state.get("memory_context") or {}).get("preferred_categories", []),
                "preferred_styles": (session_state.get("memory_context") or {}).get("preferred_styles", []),
                "preferred_price_level": (session_state.get("memory_context") or {}).get("preferred_price_level"),
            },
            "last_messages": data.get("last_messages", [])[-6:],
        }
        context["formatted_products"] = [
            format_product(next((item for item in items if item["id"] == product.get("id")), None))
            for product in related_products[:5]
            if next((item for item in items if item["id"] == product.get("id")), None)
        ]
        print(json.dumps({"log": "[SMART_CONTEXT]", "phone": sender_phone, "products": [item["id"] for item in related_products[:5]], "state": context["conversation"]}, ensure_ascii=False))
        return context

    def _conversation_flow_engine(session_state, parsed, semantic):
        checkout_states = {"ready_to_buy", "collecting_shipping", "collecting_payment"}
        route = "recommendation"
        # CRITICAL: EXPLICIT_BUY_TERMS - Only these trigger checkout route
        # Product interest ("أريد آيفون") is NOT the same as purchase intent ("اشتري")
        explicit_buy_terms_flow = {
            "شراء", "الشراء", "أريد الشراء", "اريد الشراء", "أريد شراء", "اريد شراء",
            "اشتري", "أشتري", "اشتريه", "أشتريه", "اشتريها", "أشتريها",
            "اطلب", "أطلب", "اطلبه", "أطلبه", "اطلبها", "أطلبها", "اطلب الآن", "أطلب الآن",
            "buy", "purchase", "checkout", "confirm order", "order now", "place order",
            "نعم اشتري", "نعم أشتري", "اكمل الطلب", "أكمل الطلب", "تأكيد الطلب",
            "خذه", "خذها", "آخذه", "آخذها", "اخذه", "اخذها",
        }
        normalized_text_flow = _normalize_catalog_text(text)
        has_explicit_buy_flow = any(_normalize_catalog_text(term) in normalized_text_flow for term in explicit_buy_terms_flow)
        
        # Only route to checkout if:
        # 1. Already in checkout state (collecting data), OR
        # 2. Has EXPLICIT buy intent keywords (not just product interest)
        if session_state.get("checkout_state") in checkout_states:
            route = "checkout"
        elif has_explicit_buy_flow and (parsed.get("intent") == "buy" or semantic.get("intent") == "buy"):
            route = "checkout"
        elif parsed.get("intent") == "compare" or semantic.get("intent") == "compare":
            route = "compare"
        elif parsed.get("base_intent") == "price_objection" or parsed.get("intent") == "cheaper":
            route = "pricing"
        elif parsed.get("intent") == "ask_details" or semantic.get("intent") == "ask_details":
            route = "question"
        elif parsed.get("intent") == "greeting":
            route = "greeting"
        elif any(term in text for term in ["دعم", "support", "موظف", "agent"]):
            route = "support"
        elif any(term in text for term in ["طلب", "اوردر", "order status", "status"]):
            route = "order_status"
        elif parsed.get("action") == "recommend" or semantic.get("intent") == "recommendation":
            route = "recommendation"
        hesitant = any(term in _normalize_catalog_text(text) for term in ["محتار", "متردد", "مش عارف", "not sure", "unsure"])
        flow = {
            "route": route,
            "inside_purchase_journey": route == "checkout",
            "is_question": route == "question",
            "is_comparing": route == "compare",
            "wants_payment": any(term in _normalize_catalog_text(text) for term in ["دفع", "payment", "pay", "visa", "كاش"]),
            "is_hesitant": hesitant,
            "wants_recommendation": route == "recommendation",
        }
        print(json.dumps({"log": "[FLOW_ENGINE]", "phone": sender_phone, "flow": flow}, ensure_ascii=False))
        return flow

    def _intent_router(parsed, semantic, flow):
        route = flow.get("route")
        if route == "checkout":
            return "checkout"
        if route == "compare":
            return "compare"
        if route == "pricing":
            return "pricing"
        if route == "support":
            return "support"
        if route == "order_status":
            return "order_status"
        if route == "greeting":
            return "greeting"
        if parsed.get("intent") == "product_lookup" or semantic.get("intent") == "product_lookup":
            return "search"
        if route == "question":
            return "question"
        return "recommendation"

    def _ai_prompt_architecture(context, route):
        return [
            {
                "role": "system",
                "content": (
                    "You are a professional WhatsApp commerce salesperson. "
                    "Never restart the conversation or send a welcome menu when a session is active. "
                    "Guide the customer toward the next useful sales step with minimal friction. "
                    "Use only the provided catalog context. Keep replies concise, natural, and in the customer's language."
                )
            },
            {"role": "system", "content": "Customer memory:\n" + json.dumps(context.get("memory", {}), ensure_ascii=False)},
            {"role": "system", "content": "Catalog context:\n" + "\n\n".join(context.get("formatted_products") or [])},
            {"role": "system", "content": "Sales strategy:\n" + json.dumps({"route": route, "conversation": context.get("conversation"), "interests": context.get("interests")}, ensure_ascii=False)},
            {"role": "system", "content": "Response formatting: short WhatsApp message, no markdown tables, ask one next question maximum. Never expose raw product objects or raw field labels such as category:, type:, price:, sale_price:, description:, ai_metadata:, or keywords:. If mentioning a product, format it as a clean customer message with product name, السعر, and الوصف only when available."},
            {"role": "user", "content": context.get("customer_message", "")},
        ]

    def _core_ai_response(context, route):
        if not ai_client or route in {"checkout", "pricing", "compare", "support", "order_status"}:
            return None
        try:
            completion = ai_client.chat.completions.create(
                model=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
                messages=_ai_prompt_architecture(context, route),
                temperature=0.35,
            )
            content = (completion.choices[0].message.content or "").strip()
            print(json.dumps({"log": "[AI_PIPELINE_RESPONSE]", "phone": sender_phone, "route": route, "used_ai": bool(content)}, ensure_ascii=False))
            return sanitize_customer_reply(content) or None
        except Exception as exc:
            print(f"[AI_PIPELINE_ERROR] {repr(exc)}")
            return None

    def _core_fallback_response(route, context):
        retrieved_items = [
            next((item for item in items if item["id"] == product.get("id")), None)
            for product in context.get("related_products", [])
        ]
        retrieved_items = [item for item in retrieved_items if item]
        if route == "search" and retrieved_items:
            return _with_product_image(retrieved_items[0], _format_product_message(retrieved_items[0]))
        if route == "recommendation" and retrieved_items:
            return _format_recommendations((retrieved_items[:2], False), kind="semantic")
        if route == "greeting" and active_convo and active_convo["current_step"] != "greeting":
            return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}"
        return _business_questions()

    def _send_menu(explicit=False):
        if active_convo:
            data = _read_convo_data(active_convo)
            current_step = active_convo["current_step"] or "greeting"
            if not explicit and current_step == "product_selected":
                return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}\n{t(lang, 'back_catalog')}"
            if not explicit and current_step == "awaiting_order_info":
                return t(lang, "send_name_city")
            if not explicit and current_step == "post_order_engagement":
                return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}\n{t(lang, 'back_catalog')}"
            if not explicit and data.get("conversation_state") not in (None, "", "idle"):
                return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}\n{t(lang, 'back_catalog')}"
            if (
                not explicit
                and current_step in ("greeting", "browsing_catalog")
                and data.get("previous_step") in (None, "greeting", "browsing_catalog", "product_selected", "post_order_engagement")
                and data.get("last_bot_message_type") == "main_menu"
            ):
                return t(lang, "menu_already")
        _state_update("browsing_catalog", data={"last_bot_message_type": "main_menu"})
        print("[DEBUG_MENU_RETURN] returning main menu")
        return _menu()

    def _compare_last_shown_products():
        data = _read_convo_data(active_convo)
        last_ids = data.get("last_shown_product_ids") or data.get("shown_product_ids") or []
        last_ids = last_ids[-3:]
        products = [item for product_id in last_ids for item in items if item["id"] == product_id]
        if len(products) < 2:
            return t(lang, "compare_need_recommendations")

        def _summary(item):
            searchable = " ".join([
                item["title"] or "",
                item["category"] if "category" in item.keys() else "",
                item["description"] or "",
                item["keywords"] if "keywords" in item.keys() else "",
                item["tags"] if "tags" in item.keys() else "",
            ]).lower()
            traits = []
            if any(term in searchable for term in ["رخيص", "اقتصادي", "cheap", "budget", "affordable"]):
                traits.append(t(lang, "trait_budget"))
            if any(term in searchable for term in ["فاخر", "فخم", "راقي", "luxury", "premium"]):
                traits.append(t(lang, "trait_premium"))
            if any(term in searchable for term in ["سريع", "fast", "speed", "quick"]):
                traits.append(t(lang, "trait_fast"))
            if any(term in searchable for term in ["قوي", "strong", "powerful", "durable"]):
                traits.append(t(lang, "trait_strong"))
            if any(term in searchable for term in ["يومي", "daily", "everyday", "casual"]):
                traits.append(t(lang, "trait_daily"))
            if not traits:
                desc = (item["description"] or "").strip()
                return desc[:90].strip() if desc else t(lang, "trait_default")
            return "، ".join(traits)

        lines = [t(lang, "compare_heading")]
        for item in products:
            lines.append(f"{item['title']}: {_summary(item)}.")
        return "\n".join(lines)

    def _parse_message_intent():
        base_intent = detect_intent(text)
        product = _find_product_by_text(text)
        requested_category = _detect_requested_category()
        short_intent = _contextual_short_intent()
        filters = []
        action = None
        intent_name = "unknown"
        if _explicit_reset_requested() or text in explicit_menu_requests:
            intent_name = "main_menu"
            action = "show_menu"
        elif any(term in text for term in ["ما الفرق", "الفرق بينهم", "قارن", "compare"]):
            intent_name = "compare"
            action = "compare"
        elif product:
            intent_name = "product_lookup"
            action = "show_product"
        elif base_intent == "price_objection" or short_intent == "price":
            intent_name = "cheaper"
            filters.append("price")
            action = "recommend"
        elif base_intent == "premium_request" or short_intent == "premium" or any(term in text for term in ["أفضل", "افضل", "شيء أفضل", "شي افضل", "better", "best"]):
            intent_name = "premium"
            filters.append("premium")
            action = "recommend"
        elif any(term in text for term in ["مشابه", "شبيه", "مثل هذا", "نفس", "similar", "like this"]):
            intent_name = "similar"
            filters.append("similar")
            action = "recommend"
        elif any(term in text for term in ["رياضي", "رياضة", "sport", "sports", "sporty", "fitness", "gym", "running"]):
            intent_name = "ask_recommendation"
            filters.append("sport")
            action = "recommend"
        elif base_intent == "reject_product" or short_intent == "no":
            intent_name = "reject"
            action = "recommend"
        elif text in ["شراء", "الشراء", "buy", "purchase"] or short_intent == "select_last":
            intent_name = "buy"
            action = "buy"
        elif text in ["تفاصيل", "عرض التفاصيل", "details", "info"]:
            intent_name = "ask_details"
            action = "details"
        elif _generic_request_detected():
            intent_name = "ask_recommendation"
            action = "recommend"
        elif any(term in text for term in ["مرحبا", "السلام", "hello", "hi", "bonjour", "hola"]):
            intent_name = "greeting"
            action = "greeting"
        return {
            "intent": intent_name,
            "product": product,
            "product_name": product["title"] if product else "",
            "category": requested_category,
            "filters": filters,
            "action": action,
            "language": lang,
            "base_intent": base_intent,
            "short_intent": short_intent,
        }

    def _sales_decision_engine(parsed, semantic, profile, state_data):
        normalized_text = _normalize_catalog_text(text)
        current_product_id = state_data.get("current_product") or profile.get("last_product_id")
        last_ids = state_data.get("last_shown_product_ids") or state_data.get("suggested_product_ids") or profile.get("last_recommendation_ids") or []
        viewed_ids = set((profile.get("viewed_product_ids") or []) + (profile.get("viewed_products") or []))
        rejected_ids = set(profile.get("rejected_product_ids") or [])
        hesitant_terms = ["محتار", "محتاره", "متردد", "متردده", "مش عارف", "مو عارف", "لا اعرف", "لا أعرف", "which", "not sure", "unsure"]
        support_terms = ["دعم", "موظف", "خدمة العملاء", "support", "agent", "human", "representative"]
        # EXPLICIT_BUY_TERMS - Only these trigger push_to_checkout
        # These are EXPLICIT purchase intent keywords, not product interest
        explicit_buy_terms = {
            "شراء", "الشراء", "أريد الشراء", "اريد الشراء", "أريد شراء", "اريد شراء",
            "اشتري", "أشتري", "اشتريه", "أشتريه", "اشتريها", "أشتريها",
            "اطلب", "أطلب", "اطلبه", "أطلبه", "اطلبها", "أطلبها", "اطلب الآن", "أطلب الآن",
            "buy", "purchase", "checkout", "confirm order", "order now", "place order",
            "نعم اشتري", "نعم أشتري", "اكمل الطلب", "أكمل الطلب", "تأكيد الطلب",
            "خذه", "خذها", "آخذه", "آخذها", "اخذه", "اخذها",
        }
        # Product interest terms - these show interest but NOT explicit buy intent
        product_interest_terms = ["أريد", "اريد", "ابي", "أبي", "ابغى", "أبغى", "عندكم", "عندك", "هل يوجد", "موجود"]
        detail_terms = ["تفاصيل", "عرض التفاصيل", "details", "info", "معلومات"]
        has_memory_context = bool(current_product_id or last_ids or profile.get("preferred_categories") or profile.get("preferred_styles"))
        
        # Check if message contains EXPLICIT buy intent (not just product interest)
        has_explicit_buy_intent = any(_normalize_catalog_text(term) in normalized_text for term in explicit_buy_terms)
        has_product_interest = any(_normalize_catalog_text(term) in normalized_text for term in product_interest_terms)
        has_product_in_message = bool(parsed.get("product"))
        
        decision = "ask_clarifying_question"
        reasons = []
        if any(term in normalized_text for term in [_normalize_catalog_text(term) for term in support_terms]):
            decision = "connect_support"
            reasons.append("customer_requested_support")
        elif parsed.get("intent") == "main_menu":
            decision = "ask_clarifying_question"
            reasons.append("explicit_menu_or_reset")
        elif parsed.get("intent") == "compare" or semantic.get("intent") == "compare" or any(term in normalized_text for term in [_normalize_catalog_text(term) for term in hesitant_terms]):
            decision = "compare_products"
            reasons.append("customer_hesitant_or_compare_request")
        # CRITICAL FIX: Only push_to_checkout when EXPLICIT buy keywords are present
        # Messages like "أريد آيفون 12" should show product details, NOT checkout
        elif has_explicit_buy_intent:
            decision = "push_to_checkout" if (current_product_id or last_ids or parsed.get("product")) else "ask_clarifying_question"
            reasons.append("explicit_purchase_intent_detected" if decision == "push_to_checkout" else "purchase_intent_without_product_context")
        # Product interest without explicit buy intent → show product details
        elif (has_product_interest and has_product_in_message) or (parsed.get("intent") == "product_lookup" and parsed.get("product")):
            decision = "show_product_details"
            reasons.append("product_interest_detected_showing_details")
        elif parsed.get("intent") == "ask_details" or semantic.get("intent") == "ask_details" or any(term in normalized_text for term in [_normalize_catalog_text(term) for term in detail_terms]):
            decision = "show_product_details" if (current_product_id or last_ids or parsed.get("product")) else "ask_clarifying_question"
            reasons.append("details_requested" if decision == "show_product_details" else "details_without_product_context")
        elif parsed.get("intent") == "cheaper" or parsed.get("base_intent") == "price_objection" or semantic.get("price_preference") in {"budget", "cheap", "affordable"}:
            decision = "handle_price_objection"
            reasons.append("price_objection_or_budget_preference")
        elif parsed.get("intent") == "reject" or parsed.get("short_intent") == "no":
            decision = "offer_alternative"
            reasons.append("product_rejected")
        elif parsed.get("intent") == "product_lookup" and parsed.get("product"):
            decision = "show_product_details"
            reasons.append("specific_product_requested")
        elif parsed.get("action") == "recommend" or semantic.get("intent") in {"recommendation", "product_lookup"}:
            decision = "recommend_products"
            reasons.append("recommendation_intent_detected")
        elif has_memory_context and normalized_text:
            decision = "recommend_products"
            reasons.append("using_customer_memory_context")
        else:
            reasons.append("insufficient_sales_context")
        print(json.dumps({
            "log": "[SALES_DECISION]",
            "decision": decision,
            "intent": parsed.get("intent"),
            "semantic_intent": semantic.get("intent"),
            "current_step": current_step,
            "last_product_id": current_product_id,
            "rejected_count": len(rejected_ids),
            "viewed_count": len(viewed_ids),
            "price_preference": profile.get("preferred_price_level") or semantic.get("price_preference"),
        }, ensure_ascii=False))
        print(json.dumps({"log": "[DECISION_REASON]", "decision": decision, "reasons": reasons}, ensure_ascii=False))
        return {"decision": decision, "reasons": reasons, "current_product_id": current_product_id, "last_ids": last_ids}

    def _sales_clarifying_question(decision_info):
        return _business_questions()

    def _sales_select_context_product(decision_info, parsed=None):
        if parsed and parsed.get("product"):
            return parsed["product"]
        current_product_id = decision_info.get("current_product_id")
        if current_product_id:
            found = next((item for item in items if item["id"] == current_product_id), None)
            if found:
                return found
        for product_id in decision_info.get("last_ids") or []:
            found = next((item for item in items if item["id"] == product_id), None)
            if found:
                return found
        return None

    def _purchase_flow(data=None):
        source = data if isinstance(data, dict) else _read_convo_data(active_convo)
        flow = source.get("purchase_flow") if isinstance(source, dict) else {}
        if not isinstance(flow, dict):
            flow = {}
        flow.setdefault("state", "browsing")
        flow.setdefault("collected", {})
        flow.setdefault("missing", [])
        flow.setdefault("probability_of_purchase", 0.0)
        flow.setdefault("reminder_sent_at", None)
        flow.setdefault("last_activity_at", None)
        return flow

    def _checkout_log(tag, payload):
        data = {"log": tag, "phone": sender_phone}
        data.update(payload or {})
        print(json.dumps(data, ensure_ascii=False))

    def _set_purchase_state(state, extra=None):
        data = _read_convo_data(active_convo)
        flow = _purchase_flow(data)
        previous_state = flow.get("state")
        flow["state"] = state
        flow["last_activity_at"] = datetime.datetime.now().isoformat()
        if extra:
            for key, value in extra.items():
                if key == "collected" and isinstance(value, dict):
                    flow["collected"].update({k: v for k, v in value.items() if v not in (None, "")})
                else:
                    flow[key] = value
        data["purchase_flow"] = flow
        _checkout_log("[CHECKOUT_STATE]", {"from": previous_state, "to": state, "flow": flow})
        _state_update(active_convo["current_step"] if active_convo else "greeting", data=data, force=True)
        return flow

    def _extract_checkout_fields(message):
        value = (message or "").strip()
        normalized = _normalize_catalog_text(value.lower())
        fields = {}
        purchase_only_terms = ["اريد هذا", "أريد هذا", "ابي هذا", "أبي هذا", "اشتري", "أشتري", "اطلب هذا", "أطلب هذا", "نعم اريده", "نعم أريده", "buy", "purchase", "this one"]
        objection_terms = ["غالي", "ارخص", "أرخص", "قارن", "compare", "expensive", "cheap", "افضل", "أفضل", "better", "best"]
        payment_terms = {
            "cod": ["الدفع عند الاستلام", "عند الاستلام", "cash on delivery", "cod"],
            "cash": ["كاش", "cash", "نقد"],
            "card": ["بطاقة", "فيزا", "visa", "card", "mastercard"],
            "transfer": ["تحويل", "bank transfer", "transfer"],
        }
        for method, terms in payment_terms.items():
            if any(_normalize_catalog_text(term) in normalized for term in terms):
                fields["payment_method"] = method
        color_terms = ["اسود", "أبيض", "ابيض", "احمر", "أحمر", "ازرق", "أزرق", "اخضر", "أخضر", "black", "white", "red", "blue", "green", "beige", "brown"]
        for color in color_terms:
            if _normalize_catalog_text(color) in normalized:
                fields["color"] = color
                break
        size_terms = ["xs", "s", "m", "l", "xl", "xxl", "small", "medium", "large", "صغير", "وسط", "كبير"]
        for size in size_terms:
            if _normalize_catalog_text(size) in normalized.split() or _normalize_catalog_text(size) in normalized:
                fields["size"] = size.upper() if len(size) <= 3 else size
                break
        # Skip name/location extraction if message contains objection or purchase-only terms
        if any(_normalize_catalog_text(term) in normalized for term in purchase_only_terms + objection_terms):
            fields["phone"] = sender_phone
            return fields
        parts = [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]
        # Handle comma-separated: "name, location"
        if len(parts) >= 2:
            fields["name"] = parts[0]
            fields["location"] = ", ".join(parts[1:])
        # Handle space-separated: "name location" (e.g., "ناتو نواكشوط")
        elif " " in value:
            name_parts = value.split()
            if len(name_parts) >= 2:
                fields["name"] = " ".join(name_parts[:-1])
                fields["location"] = name_parts[-1]
            elif len(name_parts) == 1:
                fields["name"] = name_parts[0]
        # Single word - treat as name
        elif parts:
            fields["name"] = parts[0]
        fields["phone"] = sender_phone
        return fields

    def _checkout_required_fields(product=None):
        required = ["name", "phone", "location", "payment_method"]
        searchable = _normalize_catalog_text(" ".join([
            _field(product, "title") if product else "",
            _field(product, "category") if product else "",
            _field(product, "type") if product else "",
            _field(product, "description") if product else "",
        ]))
        if any(term in searchable for term in ["ملابس", "حذاء", "لبس", "clothes", "shoe", "fashion", "shirt", "dress"]):
            required.extend(["size", "color"])
        return required

    def _checkout_missing_fields(flow, product=None):
        collected = flow.get("collected") or {}
        return [field for field in _checkout_required_fields(product) if not collected.get(field)]

    def _checkout_prompt(missing, product=None, flow=None):
        first_missing = missing[0] if missing else None
        persuasion = _checkout_persuasion(product, flow)
        prompts = {
            "name": "تمام 👌 أرسل اسمك لإكمال الطلب.",
            "phone": "تمام، أحتاج رقم الهاتف للتواصل معك.",
            "location": "ممتاز، أرسل المدينة أو الموقع للتوصيل.",
            "size": "ما المقاس المناسب لك؟",
            "color": "أي لون تفضّل؟",
            "payment_method": "ما طريقة الدفع المناسبة؟ كاش، بطاقة، أو عند الاستلام؟",
        }
        return "\n".join([line for line in [persuasion, prompts.get(first_missing, t(lang, "send_name_city"))] if line]).strip()

    def _checkout_persuasion(product=None, flow=None):
        collected = (flow or {}).get("collected") or {}
        if collected.get("payment_method"):
            return "اختيارك مناسب، باقي خطوة بسيطة ونثبت الطلب ✅"
        if customer_profile.get("preferred_price_level") == "budget":
            return "هذا الخيار مناسب لميزانيتك وقريب من احتياجك 👌"
        if product and product["id"] in (customer_profile.get("viewed_product_ids") or []):
            return "واضح أن هذا المنتج لفت انتباهك أكثر من مرة، مناسب نكمل عليه."
        return "هذا خيار مناسب لاحتياجك، ونقدر نثبت الطلب بخطوات بسيطة."

    def _checkout_confidence(parsed, semantic, flow, product=None):
        score = 0.0
        reasons = []
        collected = flow.get("collected") or {}
        product_id = product["id"] if product else flow.get("product_id")
        viewed_count = (customer_profile.get("viewed_product_ids") or []).count(product_id) if product_id else 0
        if parsed.get("intent") == "buy" or semantic.get("intent") == "buy":
            score += 0.35
            reasons.append("purchase_intent")
        if product_id and product_id in (customer_profile.get("viewed_product_ids") or []):
            score += min(0.10 + viewed_count * 0.05, 0.20)
            reasons.append("repeated_product_view")
        if flow.get("details_requested"):
            score += 0.15
            reasons.append("details_requested")
        if flow.get("shipping_asked"):
            score += 0.10
            reasons.append("shipping_question")
        if flow.get("payment_asked") or collected.get("payment_method"):
            score += 0.15
            reasons.append("payment_signal")
        if collected.get("name") and collected.get("location"):
            score += 0.20
            reasons.append("core_checkout_info_collected")
        score = max(0.0, min(1.0, score))
        _checkout_log("[CONFIDENCE_SCORE]", {"probability_of_purchase": round(score, 3), "reasons": reasons})
        return score

    def _checkout_objection_kind():
        normalized = _normalize_catalog_text(text)
        if any(term in normalized for term in ["غالي", "ارخص", "أرخص", "expensive", "cheap"]):
            return "price"
        if any(term in normalized for term in ["غير مناسب", "مش مناسب", "not suitable"]):
            return "fit"
        if any(term in normalized for term in ["قارن", "مقارنه", "مقارنة", "compare"]):
            return "compare"
        if any(term in normalized for term in ["افضل", "أفضل", "better", "best"]):
            return "better"
        return None

    def _checkout_interest_signals():
        normalized = _normalize_catalog_text(text)
        return {
            "details_requested": any(term in normalized for term in ["تفاصيل", "معلومات", "details", "info"]),
            "shipping_asked": any(term in normalized for term in ["شحن", "توصيل", "shipping", "delivery"]),
            "payment_asked": any(term in normalized for term in ["دفع", "الدفع", "payment", "pay", "visa", "card", "cash"]),
        }

    def _maybe_checkout_abandonment(data):
        flow = _purchase_flow(data)
        if flow.get("state") not in {"ready_to_buy", "collecting_shipping", "collecting_payment"}:
            return None
        if flow.get("reminder_sent_at"):
            return None
        try:
            updated_at = datetime.datetime.fromisoformat((active_convo["updated_at"] or "").replace("Z", "+00:00")) if active_convo else datetime.datetime.now()
            if updated_at.tzinfo is not None:
                updated_at = updated_at.replace(tzinfo=None)
        except (TypeError, ValueError):
            updated_at = datetime.datetime.now()
        inactive_for = datetime.datetime.now() - updated_at
        if inactive_for <= datetime.timedelta(minutes=30):
            return None
        flow["reminder_sent_at"] = datetime.datetime.now().isoformat()
        data["purchase_flow"] = flow
        _checkout_log("[ABANDONMENT]", {"state": flow.get("state"), "inactive_minutes": int(inactive_for.total_seconds() // 60), "spam_guard": "first_reminder_only"})
        _state_update(active_convo["current_step"] if active_convo else "greeting", data=data, force=True)
        return "كنت قريب من إتمام الطلب 👌 إذا ما زلت مهتمًا، أرسل البيانات الناقصة فقط وسأكمل لك الطلب بدون إعادة الكتالوج."

    def _is_general_greeting():
        normalized = _normalize_catalog_text(text).strip()
        normalized = " ".join(normalized.replace("!", " ").replace("؟", " ").replace(".", " ").replace(",", " ").split())
        greetings = {
            "سلام",
            "السلام عليكم",
            "السلام عليكم ورحمه الله",
            "السلام عليكم ورحمه الله وبركاته",
            "مرحبا",
            "اهلا",
            "اهلا وسهلا",
            "هلا",
            "هاي",
            "hello",
            "hi",
            "hey",
            "bonjour",
            "bonsoir",
        }
        return normalized in greetings

    def _conversation_state_snapshot(data):
        last_ids = data.get("last_shown_product_ids") or data.get("suggested_product_ids") or data.get("recommended_product_ids") or []
        purchase_flow = data.get("purchase_flow") if isinstance(data.get("purchase_flow"), dict) else {}
        return {
            "current_step": active_convo["current_step"] if active_convo else "greeting",
            "viewing_product": bool(data.get("current_product")),
            "asked_to_choose": bool(last_ids or data.get("pending_question") or data.get("last_question")),
            "rejected": data.get("last_intent") == "reject" or bool(data.get("rejected_product_id")),
            "wants_cheaper": data.get("last_intent") in {"cheaper", "price", "price_objection"},
            "checkout_active": purchase_flow.get("state") in {"ready_to_buy", "collecting_shipping", "collecting_payment"},
            "last_product_id": data.get("current_product") or (last_ids[0] if last_ids else None),
            "last_shown_product_ids": last_ids,
            "last_bot_message_type": data.get("last_bot_message_type"),
        }

    def _classify_conversation_intent(conversation_state):
        normalized = _normalize_catalog_text(text).strip()
        normalized = " ".join(normalized.replace("!", " ").replace("؟", " ").replace(".", " ").replace(",", " ").split())
        result = {
            "intent": "unknown",
            "confidence": 0.0,
            "catalog_allowed": False,
            "reason": "fallback",
        }
        if not normalized:
            return result
        general_terms = {
            "ok", "okay", "تمام", "حسنا", "حسنًا", "thanks", "thank you", "شكرا", "شكرًا",
            "merci", "gracias", "grazie", "bonjour", "bonsoir"
        }
        reject_terms = {
            "لا", "لا شكرا", "لا شكرًا", "لا اريد", "لا اريده", "مش عايز", "مو مناسب",
            "no", "no thanks", "not this", "pas ça", "non", "no quiero", "no gracias", "no grazie"
        }
        accept_terms = {
            "نعم", "اي", "إي", "تمام", "موافق", "yes", "ok", "okay", "d accord", "oui", "si", "sí", "va bene"
        }
        buy_terms = {"اشتري", "شراء", "اريد شراء", "أريد شراء", "buy", "purchase", "comprar", "acheter", "comprare"}
        detail_terms = {"تفاصيل", "عرض التفاصيل", "معلومات", "details", "info", "détails", "detalles", "dettagli"}
        service_terms = {"خدمة", "خدمات", "service", "services", "servicio", "servicios", "servizio", "servizi"}
        product_request_terms = {
            "اريد", "أريد", "ابي", "أبي", "ابغى", "احتاج", "عندكم", "هل يوجد", "أبحث", "ابحث",
            "منتج", "هاتف", "حذاء", "عطر", "ملابس", "want", "need", "looking for", "do you have",
            "product", "phone", "shoe", "perfume", "je veux", "cherche", "produit", "quiero", "busco",
            "producto", "voglio", "cerco", "prodotto"
        }
        if _is_general_greeting():
            return {"intent": "greeting", "confidence": 1.0, "catalog_allowed": False, "reason": "greeting"}
        if normalized.isdigit() and conversation_state.get("asked_to_choose"):
            return {"intent": "menu_selection", "confidence": 0.95, "catalog_allowed": False, "reason": "contextual_selection"}
        if normalized in reject_terms or any(term in normalized for term in reject_terms if len(term) > 3):
            return {"intent": "reject", "confidence": 0.9, "catalog_allowed": False, "reason": "rejection"}
        if normalized in accept_terms and conversation_state.get("asked_to_choose"):
            return {"intent": "accept", "confidence": 0.8, "catalog_allowed": False, "reason": "contextual_acceptance"}
        if any(term in normalized for term in buy_terms):
            return {"intent": "buy", "confidence": 0.85, "catalog_allowed": False, "reason": "purchase_intent"}
        if any(term in normalized for term in detail_terms):
            return {"intent": "details_request", "confidence": 0.85, "catalog_allowed": False, "reason": "details_intent"}
        if any(term in normalized for term in service_terms):
            return {"intent": "service_request", "confidence": 0.82, "catalog_allowed": True, "reason": "service_request"}
        if any(term in normalized for term in product_request_terms):
            return {"intent": "product_request", "confidence": 0.78, "catalog_allowed": True, "reason": "product_request"}
        if normalized in general_terms:
            return {"intent": "general_talk", "confidence": 0.85, "catalog_allowed": False, "reason": "general_talk"}
        if ai_client:
            try:
                completion = ai_client.chat.completions.create(
                    model=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You classify commerce chat intent before catalog search. "
                                "Return JSON only with keys: intent, confidence, catalog_allowed, reason. "
                                "intent must be one of: greeting, product_request, service_request, reject, accept, buy, details_request, menu_selection, general_talk, unknown. "
                                "catalog_allowed must be true only for product_request or service_request. "
                                "Understand Arabic, English, French, Spanish, and Italian by meaning and conversation context. "
                                "Do not classify greetings, thanks, ok, no, or vague short messages as product_request."
                            )
                        },
                        {
                            "role": "user",
                            "content": json.dumps({
                                "message": message_text,
                                "language": lang,
                                "conversation_state": conversation_state,
                            }, ensure_ascii=False)
                        }
                    ],
                    temperature=0,
                    response_format={"type": "json_object"}
                )
                parsed = json.loads(completion.choices[0].message.content or "{}")
                intent = parsed.get("intent") if parsed.get("intent") in {
                    "greeting", "product_request", "service_request", "reject", "accept", "buy",
                    "details_request", "menu_selection", "general_talk", "unknown"
                } else "unknown"
                confidence = max(0.0, min(1.0, float(parsed.get("confidence") or 0.0)))
                catalog_allowed = intent in {"product_request", "service_request"} and bool(parsed.get("catalog_allowed"))
                return {
                    "intent": intent,
                    "confidence": confidence,
                    "catalog_allowed": catalog_allowed,
                    "reason": parsed.get("reason") or "ai_classifier",
                }
            except Exception as exc:
                print(f"[CONVERSATION_INTENT_AI_ERROR] {repr(exc)}")
        return result

    def _smart_fallback_response():
        return "لم أفهم طلبك بالكامل 👌 هل تبحث عن منتج أم خدمة؟"

    active_convo = _fetch_active_convo()
    print("[DEBUG_STATE]", dict(active_convo) if active_convo else None)
    print("[DEBUG_TEXT]", text)
    current_step = active_convo["current_step"] if active_convo else "greeting"
    state_before = _read_convo_data(active_convo)
    print(f"[STATE_DEBUG] current_step={current_step}")
    print(f"[FLOW_DEBUG] current_step={current_step} text={text}")
    
    # ════════════════════════════════════════════════════════════════════════
    # PRIORITY 2: PURCHASE LOCK PROTECTION - CHECKOUT LOCKED STATES
    # ════════════════════════════════════════════════════════════════════════
    # When in locked checkout states, treat ALL short messages as checkout data
    # and DISABLE: objection detection, semantic analyzer, recommendation engine,
    # upsell logic, and alternative product suggestions.
    
    CHECKOUT_LOCKED_STATES = {"awaiting_order_info", "collecting_shipping", "collecting_payment"}
    CHECKOUT_LOCKED_PURCHASE_STATES = {"ready_to_buy", "collecting_shipping", "collecting_payment"}
    
    def _is_checkout_locked():
        """Check if we're in a locked checkout state where all systems should be disabled."""
        if current_step in CHECKOUT_LOCKED_STATES:
            return True
        # Also check purchase_flow state
        purchase_flow_data = state_before.get("purchase_flow") or {}
        if purchase_flow_data.get("state") in CHECKOUT_LOCKED_PURCHASE_STATES:
            return True
        return False
    
    def _is_checkout_data_message(msg_text):
        """
        Check if the message looks like checkout data (name, city, payment method).
        Short messages or simple Arabic names/cities should be treated as checkout data.
        """
        normalized = msg_text.strip().lower()
        
        # Known objection/product-change terms that should NOT be treated as checkout data
        objection_terms = [
            "غالي", "ارخص", "أرخص", "قارن", "compare", "expensive", "cheap",
            "افضل", "أفضل", "better", "best", "مشابه", "similar", "بديل", "alternative",
            "منتج آخر", "منتج اخر", "شيء آخر", "شي اخر", "something else", "other product"
        ]
        
        # If message contains objection terms, it's NOT checkout data
        if any(term in normalized for term in objection_terms):
            return False
        
        # Known cities in Mauritania and common Arab cities
        known_cities = [
            "نواكشوط", "انواكشوط", "nouakchott", "نواذيبو", "كيفة", "كيفه", "نيما",
            "اطار", "روصو", "زويرات", "الرياض", "جدة", "دبي", "الكويت", "الدوحة",
            "مسقط", "عمان", "القاهرة", "الدار البيضاء", "الرباط"
        ]
        
        # If message is a known city, it's checkout data
        if any(city in normalized for city in known_cities):
            print(f"[CHECKOUT_DATA_DETECTED] city match: {normalized!r}")
            return True
        
        # Payment method terms
        payment_terms = ["كاش", "cash", "عند الاستلام", "cod", "فيزا", "visa", "بطاقة", "card", "تحويل", "transfer"]
        if any(term in normalized for term in payment_terms):
            print(f"[CHECKOUT_DATA_DETECTED] payment method: {normalized!r}")
            return True
        
        # Short messages (1-3 words) without special characters are likely names or cities
        words = normalized.split()
        if 1 <= len(words) <= 3:
            # Check if it's mostly Arabic text (likely a name or city)
            arabic_chars = sum(1 for c in normalized if '\u0600' <= c <= '\u06FF')
            total_chars = len(normalized.replace(" ", ""))
            if total_chars > 0 and arabic_chars / total_chars > 0.5:
                print(f"[CHECKOUT_DATA_DETECTED] short Arabic message (likely name/city): {normalized!r}")
                return True
        
        return False
    
    # PURCHASE LOCK: If in checkout state and message looks like checkout data
    if _is_checkout_locked():
        print(f"[PURCHASE_LOCK_ACTIVE] current_step={current_step}")
        
        # Check if message is checkout data
        if _is_checkout_data_message(text):
            print(f"[PURCHASE_LOCK_HANDLING] Treating as checkout data: {text!r}")
            
            # Extract checkout fields directly
            checkout_fields = _extract_checkout_fields(message_text)
            
            # Merge with compound parsed data
            if compound_parsed.get("customer_name") and not checkout_fields.get("name"):
                checkout_fields["name"] = compound_parsed["customer_name"]
            if compound_parsed.get("customer_city") and not checkout_fields.get("location"):
                checkout_fields["location"] = compound_parsed["customer_city"]
            if compound_parsed.get("payment_method") and not checkout_fields.get("payment_method"):
                checkout_fields["payment_method"] = compound_parsed["payment_method"]
            
            # Get current purchase flow
            purchase_flow_data = state_before.get("purchase_flow") or {}
            current_collected = purchase_flow_data.get("collected") or {}
            
            # Merge new fields with existing collected data
            for key, value in checkout_fields.items():
                if value and not current_collected.get(key):
                    current_collected[key] = value
            
            # Get the product context
            product_id = purchase_flow_data.get("product_id") or state_before.get("current_product")
            checkout_product = next((item for item in items if product_id and item["id"] == product_id), None)
            
            # Update purchase state
            merged_flow = _set_purchase_state(
                purchase_flow_data.get("state") or "collecting_shipping",
                {
                    "collected": current_collected,
                    "product_id": product_id,
                }
            )
            
            # Check what fields are still missing
            missing_fields = _checkout_missing_fields(merged_flow, checkout_product)
            
            if missing_fields:
                print(f"[PURCHASE_LOCK_PROMPT] missing_fields={missing_fields}")
                return _checkout_prompt(missing_fields, checkout_product, merged_flow)
            
            # All fields collected - complete the order
            _set_purchase_state("confirmed")
            print(f"[PURCHASE_LOCK_COMPLETED] Order confirmed")
            return t(lang, "order_received")
    
    conversation_state = _conversation_state_snapshot(state_before)
    conversation_intent = _classify_conversation_intent(conversation_state)
    catalog_allowed = conversation_intent.get("catalog_allowed") is True
    print(json.dumps({
        "log": "[CONVERSATION_INTELLIGENCE]",
        "phone": sender_phone,
        "state": conversation_state,
        "intent": conversation_intent,
    }, ensure_ascii=False))
    if not catalog_allowed:
        last_messages = state_before.get("last_messages") if isinstance(state_before.get("last_messages"), list) else []
        last_messages = (last_messages + [{"role": "customer", "text": message_text, "at": datetime.datetime.now().isoformat()}])[-8:]
        _state_update(
            current_step,
            data={
                "conversation_intelligence": conversation_intent,
                "conversation_state_snapshot": conversation_state,
                "last_intent": conversation_intent.get("intent"),
                "last_messages": last_messages,
            },
            force=True
        )
        if conversation_intent.get("intent") == "greeting":
            print("[DECISION] conversation_greeting_no_catalog")
            return "وعليكم السلام 👋 كيف يمكنني مساعدتك؟ يمكنك أن تطلب منتجًا أو خدمة من الكتالوج."
        if conversation_intent.get("intent") == "general_talk":
            print("[DECISION] general_talk_no_catalog")
            return "كيف يمكنني مساعدتك؟ يمكنك أن تطلب منتجًا أو خدمة من الكتالوج."
        if conversation_intent.get("intent") == "reject":
            print("[DECISION] rejection_no_catalog")
            return "تمام، لن أعرض هذا الخيار. هل تبحث عن منتج أو خدمة أخرى؟"
        if conversation_intent.get("intent") == "accept":
            print("[DECISION] acceptance_no_catalog")
            return "تمام 👌 ماذا تريد أن تفعل الآن؟ يمكنك طلب التفاصيل أو إكمال الشراء."
        if conversation_intent.get("intent") == "buy":
            current_product_id = conversation_state.get("last_product_id")
            if current_product_id:
                _state_update(
                    "awaiting_order_info",
                    known_ids=[current_product_id],
                    data={"last_bot_message_type": "order_info_request", "last_intent": "buy"},
                    force=True
                )
                return t(lang, "send_name_city")
            return "أي منتج تريد شراءه؟ اكتب اسم المنتج أو الخدمة التي تريدها."
        if conversation_intent.get("intent") == "details_request":
            current_product_id = conversation_state.get("last_product_id")
            selected_item = next((item for item in items if current_product_id and item["id"] == current_product_id), None)
            if selected_item:
                return _with_product_image(selected_item, _format_product_message(selected_item))
            return "عن أي منتج أو خدمة تريد التفاصيل؟"
        if conversation_intent.get("intent") == "menu_selection":
            try:
                index = int(text) - 1
            except ValueError:
                index = -1
            source_ids = conversation_state.get("last_shown_product_ids") or []
            selected_id = source_ids[index] if 0 <= index < len(source_ids) else None
            selected_item = next((item for item in items if selected_id and item["id"] == selected_id), None)
            if selected_item:
                _state_update(
                    "product_selected",
                    known_ids=[selected_item["id"]],
                    data={
                        "source": "conversation_intelligence_selection",
                        "last_bot_message_type": "product_options",
                        "last_shown_product_ids": [selected_item["id"]],
                        "current_product": selected_item["id"],
                        "current_category": _item_category(selected_item),
                        "last_intent": "menu_selection",
                        "pending_question": "product_followup"
                    },
                    force=True
                )
                return _with_product_image(selected_item, _format_product_message(selected_item))
            return "لم أجد هذا الاختيار. من فضلك اختر رقمًا من الخيارات المعروضة."
        print("[DECISION] unknown_no_catalog")

        if active_convo:
            current_product = active_convo.get("current_product")
            pending_question = active_convo.get("pending_question")

            if current_product and pending_question:
                print("[STATE_RECOVERY] recovering active checkout state")

                selected_item = _find_product_by_id(current_product)

                if selected_item:
                    return _with_product_image(
                        selected_item,
                        _format_product_message(selected_item)
                    )

        return _smart_fallback_response()

    semantic_intent = _semantic_intent_analyzer()
    parsed_intent = _parse_message_intent()
    print(f"[STATE_BEFORE] step={current_step} data={state_before}")
    print(f"[INTENT] {parsed_intent}")
    message_preferences = _extract_preferences()
    message_budget = _extract_budget()
    if message_preferences or message_budget is not None:
        _profile_update(
            preferences=message_preferences,
            budget=message_budget,
            last_intent=detect_intent(text)
        )
        current_step = active_convo["current_step"] if active_convo else current_step

    core_session = _core_session_manager()
    core_flow = _conversation_flow_engine(core_session, parsed_intent, semantic_intent)
    core_route = _intent_router(parsed_intent, semantic_intent, core_flow)
    core_context = _smart_context_injection(core_session, parsed_intent, semantic_intent)
    core_data = _read_convo_data(active_convo)
    last_messages = core_data.get("last_messages") if isinstance(core_data.get("last_messages"), list) else []
    last_messages = (last_messages + [{"role": "customer", "text": message_text, "at": datetime.datetime.now().isoformat()}])[-8:]
    _state_update(
        current_step,
        data={
            "core_session": core_session,
            "last_intent": parsed_intent.get("intent") or semantic_intent.get("intent"),
            "last_messages": last_messages,
        },
        force=True
    )

    customer_profile = _customer_profile(_read_convo_data(active_convo))
    sales_decision = _sales_decision_engine(parsed_intent, semantic_intent, customer_profile, _read_convo_data(active_convo))
    abandonment_message = _maybe_checkout_abandonment(_read_convo_data(active_convo))
    if abandonment_message and sales_decision["decision"] not in {"push_to_checkout", "handle_price_objection", "offer_alternative"}:
        return abandonment_message
    checkout_flow = _purchase_flow(_read_convo_data(active_convo))
    
    # ── Early checkout field extraction to prevent false objection detection ──
    checkout_fields = _extract_checkout_fields(message_text)
    
    # ── COMPOUND PARSER INTEGRATION: Merge compound_parsed data into checkout_fields ──
    if compound_parsed.get("customer_name") and not checkout_fields.get("name"):
        checkout_fields["name"] = compound_parsed["customer_name"]
        print(f"[COMPOUND_MERGE] name from compound_parsed: {compound_parsed['customer_name']!r}")
    if compound_parsed.get("customer_city") and not checkout_fields.get("location"):
        checkout_fields["location"] = compound_parsed["customer_city"]
        print(f"[COMPOUND_MERGE] location from compound_parsed: {compound_parsed['customer_city']!r}")
    if compound_parsed.get("payment_method") and not checkout_fields.get("payment_method"):
        checkout_fields["payment_method"] = compound_parsed["payment_method"]
        print(f"[COMPOUND_MERGE] payment_method from compound_parsed: {compound_parsed['payment_method']!r}")
    
    purchase_state = checkout_flow.get("state") if checkout_flow else None
    is_collecting_order_data = (
        current_step in {"awaiting_order_info", "collecting_shipping", "collecting_payment"}
        or purchase_state in {"ready_to_buy", "collecting_shipping", "collecting_payment"}
    )
    
    # ── COMPOUND PARSER: If checkout intent detected, treat as collecting order data ──
    if compound_parsed.get("has_checkout_intent") and not is_collecting_order_data:
        # Check if there's a product context to continue checkout
        context_product_id = conversation_state.get("last_product_id") or (checkout_flow.get("product_id") if checkout_flow else None)
        if context_product_id or compound_parsed.get("product_reference"):
            print(f"[COMPOUND_CHECKOUT_TRIGGER] has_checkout_intent=True context_product={context_product_id} product_ref={compound_parsed.get('product_reference')!r}")
            is_collecting_order_data = True
    
    if is_collecting_order_data and (
        checkout_fields.get("name") or checkout_fields.get("location") or compound_parsed.get("has_checkout_intent")
    ):
        # Customer is providing checkout info, not making a price objection
        checkout_objection = None
        
        merged_flow = _set_purchase_state(
            purchase_state or "collecting_shipping",
            {
                "collected": checkout_fields,
                "product_id": checkout_flow.get("product_id") if checkout_flow else None,
            }
        )
        
        checkout_product = _sales_select_context_product(sales_decision, parsed_intent)
        missing_fields = _checkout_missing_fields(merged_flow, checkout_product)
        
        if missing_fields:
            return _checkout_prompt(missing_fields, checkout_product, merged_flow)
        
        # STRICT CHECK: Only confirm order if we have all required data
        if current_step in {"awaiting_order_info", "collecting_shipping"} and checkout_fields.get("name") and checkout_fields.get("location") and checkout_product and checkout_product.get("id"):
            _set_purchase_state("confirmed")
            return t(lang, "order_received")
        else:
            # Missing data, ask for it
            return _checkout_prompt(["name", "location"], checkout_product, merged_flow)
    else:
        checkout_objection = _checkout_objection_kind()
    
    checkout_product = _sales_select_context_product(sales_decision, parsed_intent)
    if checkout_flow.get("state") in {"ready_to_buy", "collecting_shipping", "collecting_payment"} and checkout_objection:
        _checkout_log("[OBJECTION]", {"kind": checkout_objection, "state": checkout_flow.get("state")})
        if checkout_objection == "price":
            return _format_recommendations(_recommend_products("price"), kind="price")
        if checkout_objection == "compare":
            _set_purchase_state("comparing")
            return _compare_last_shown_products()
        if checkout_objection == "better":
            return _format_recommendations(_recommend_products("premium"), kind="premium")
        return _format_recommendations(_recommend_products("similar"), kind="alternative")
    if checkout_flow.get("state") in {"ready_to_buy", "collecting_shipping", "collecting_payment"} and sales_decision["decision"] != "push_to_checkout":
        checkout_fields = _extract_checkout_fields(message_text)
        checkout_signals = _checkout_interest_signals()
        merged_flow = _set_purchase_state(
            checkout_flow.get("state"),
            dict(checkout_signals, collected=checkout_fields, product_id=checkout_flow.get("product_id") or (checkout_product["id"] if checkout_product else None))
        )
        missing_fields = _checkout_missing_fields(merged_flow, checkout_product)
        probability = _checkout_confidence(parsed_intent, semantic_intent, merged_flow, checkout_product)
        next_state = "collecting_payment" if missing_fields == ["payment_method"] else "collecting_shipping"
        if not missing_fields:
            _set_purchase_state("confirmed", {"probability_of_purchase": probability})
        else:
            _set_purchase_state(next_state, {"missing": missing_fields, "probability_of_purchase": probability})
            return _checkout_prompt(missing_fields, checkout_product, merged_flow)
    if sales_decision["decision"] == "connect_support":
        _state_update("support_requested", data={"last_bot_message_type": "support_requested"}, force=True)
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return t(lang, "unknown")
    if sales_decision["decision"] == "compare_products":
        response = _compare_last_shown_products()
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return response
    if sales_decision["decision"] == "handle_price_objection":
        response = _format_recommendations(
            _recommend_products("price"),
            intro=random.choice([t(lang, "price_intro_1"), t(lang, "price_intro_2"), t(lang, "price_intro_3")]),
            outro=random.choice([t(lang, "price_outro_1"), t(lang, "price_outro_2"), t(lang, "price_outro_3")]),
            kind="price"
        )
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return response
    if sales_decision["decision"] == "push_to_checkout":
        selected_item = _sales_select_context_product(sales_decision, parsed_intent)
        if selected_item:
            _checkout_log("[PURCHASE_INTENT]", {"product_id": selected_item["id"], "title": selected_item["title"], "source": "sales_decision_engine"})
            checkout_signals = _checkout_interest_signals()
            checkout_flow = _set_purchase_state(
                "ready_to_buy",
                dict(checkout_signals, **{
                    "product_id": selected_item["id"],
                    "collected": _extract_checkout_fields(message_text),
                    "purchase_intent_text": message_text,
                    "reminder_sent_at": None
                })
            )
            missing_fields = _checkout_missing_fields(checkout_flow, selected_item)
            probability = _checkout_confidence(parsed_intent, semantic_intent, checkout_flow, selected_item)
            _state_update(
                "awaiting_order_info",
                known_ids=[selected_item["id"]],
                data={
                    "last_bot_message_type": "order_info_request",
                    "current_product": selected_item["id"],
                    "current_category": _item_category(selected_item),
                    "last_intent": "buy",
                    "purchase_flow": dict(checkout_flow, missing=missing_fields, probability_of_purchase=probability)
                },
                force=True
            )
            print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
            explicit_confirmation = _is_explicit_purchase_confirmation(message_text)
            if missing_fields:
                next_state = "collecting_payment" if missing_fields == ["payment_method"] else "collecting_shipping"
                _set_purchase_state(next_state, {"missing": missing_fields, "probability_of_purchase": probability})
                return _checkout_prompt(missing_fields, selected_item, checkout_flow)
            if _has_real_checkout_context(selected_item, checkout_fields, explicit_confirmation):
                _set_purchase_state("confirmed", {"probability_of_purchase": probability, "explicit_confirmation": True})
                return t(lang, "send_name_city")
            _set_purchase_state("ready_to_buy", {"probability_of_purchase": probability, "collected": checkout_fields})
            return _checkout_prompt(["name", "location"], selected_item, checkout_flow)
    if sales_decision["decision"] == "show_product_details":
        selected_item = _sales_select_context_product(sales_decision, parsed_intent)
        if selected_item:
            _state_update(
                "product_selected",
                known_ids=[selected_item["id"]],
                data={
                    "source": "sales_decision_engine",
                    "last_bot_message_type": "product_options",
                    "last_shown_product_ids": [selected_item["id"]],
                    "suggested_product_ids": [selected_item["id"]],
                    "last_question_type": "product_followup",
                    "last_question": "product_followup",
                    "last_intent": "show_product_details",
                    "last_category": _item_category(selected_item),
                    "current_category": _item_category(selected_item),
                    "current_product": selected_item["id"],
                    "pending_question": "product_followup"
                },
                force=True
            )
            print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
            return _with_product_image(selected_item, _format_product_message(selected_item))
    if sales_decision["decision"] == "offer_alternative":
        response = _format_recommendations(_recommend_products("premium"), kind="alternative")
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return response
    if core_route == "order_status":
        _state_update("support_requested", data={"last_bot_message_type": "order_status_request"}, force=True)
        return "أرسل رقم الطلب أو تفاصيله، وسأساعدك في متابعة حالته."
    if core_route in {"question", "greeting"} and sales_decision["decision"] in {"ask_clarifying_question", "recommend_products"}:
        ai_response = _core_ai_response(core_context, core_route)
        response = ai_response or _core_fallback_response(core_route, core_context)
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return response
    if sales_decision["decision"] == "ask_clarifying_question" and parsed_intent["intent"] == "unknown" and not _generic_request_detected():
        _state_update("browsing_catalog", data={"last_bot_message_type": "sales_clarifying_question"}, force=True)
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return _sales_clarifying_question(sales_decision)

    if parsed_intent["intent"] == "main_menu":
        print("[DECISION] explicit_main_menu")
        _state_update("greeting", known_ids=[], data={"last_bot_message_type": "conversation_reset"}, force=True)
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return _send_menu(explicit=True)

    if parsed_intent["intent"] == "compare":
        print("[DECISION] compare_last_shown_products")
        response = _compare_last_shown_products()
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return response

    if parsed_intent["intent"] == "product_lookup" and parsed_intent["product"]:
        selected_item = parsed_intent["product"]
        print(f"[DECISION] product_lookup id={selected_item['id']} title={selected_item['title']!r}")
        _state_update(
            "product_selected",
            known_ids=[selected_item["id"]],
            data={
                "source": "intent_product_lookup",
                "last_bot_message_type": "product_options",
                "last_shown_product_ids": [selected_item["id"]],
                "suggested_product_ids": [selected_item["id"]],
                "last_question_type": "product_followup",
                "last_question": "product_followup",
                "last_intent": "product_lookup",
                "last_category": _item_category(selected_item),
                "current_category": _item_category(selected_item),
                "current_product": selected_item["id"],
                "pending_question": "product_followup"
            },
            force=True
        )
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return _with_product_image(selected_item, _format_product_message(selected_item))

    if (
        semantic_intent.get("intent") == "recommendation"
        and float(semantic_intent.get("confidence") or 0) >= 0.45
        and parsed_intent["intent"] not in {"main_menu", "compare", "product_lookup"}
    ):
        semantic_products = _semantic_rank_products(semantic_intent, limit=3)
        if semantic_products:
            semantic_ids = [item["id"] for item in semantic_products]
            context_data = _read_convo_data(active_convo)
            semantic_filters = [
                semantic_intent.get(key)
                for key in ("category", "style", "luxury_level", "usage_context", "price_preference", "target_customer")
                if semantic_intent.get(key)
            ]
            _state_update(
                "recommendation_flow",
                data={
                    "source": "semantic_recommendation_engine",
                    "semantic_intent": semantic_intent,
                    "last_intent": "semantic_recommendation",
                    "last_filters": _append_unique(context_data.get("last_filters") or [], "semantic", 10),
                    "semantic_filters": semantic_filters,
                    "current_category": semantic_intent.get("category") or context_data.get("current_category"),
                    "current_product": semantic_ids[0] if semantic_ids else context_data.get("current_product"),
                    "last_category": _item_category(semantic_products[0]) if semantic_products else context_data.get("last_category"),
                    "last_question": "recommendation_followup",
                    "last_question_type": "recommendation_followup",
                    "recommended_product_ids": semantic_ids,
                    "last_shown_product_ids": semantic_ids[:2],
                },
                force=True
            )
            print(f"[DECISION] semantic_recommendation products={semantic_ids}")
            response = _format_recommendations((semantic_products[:2], False), kind="semantic")
            print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
            return response

    if parsed_intent["intent"] in {"premium", "similar", "cheaper", "reject", "ask_recommendation"}:
        intent_kind = {
            "premium": "premium",
            "similar": "similar",
            "cheaper": "price",
            "reject": "premium",
            "ask_recommendation": parsed_intent["filters"][0] if parsed_intent["filters"] else "premium",
        }.get(parsed_intent["intent"], "premium")
        context_data = _read_convo_data(active_convo)
        current_product_id = context_data.get("current_product")
        current_product = next((item for item in items if current_product_id and item["id"] == current_product_id), None)
        update_data = {
            "last_intent": parsed_intent["intent"],
            "last_filters": _append_unique(context_data.get("last_filters") or [], intent_kind, 10),
            "last_question": "recommendation_followup",
            "last_question_type": "recommendation_followup",
        }
        if current_product:
            update_data["current_category"] = context_data.get("current_category") or _item_category(current_product)
            if parsed_intent["intent"] == "reject":
                update_data["rejected_product_id"] = current_product["id"]
                _profile_update(
                    rejected=[current_product["id"]],
                    last_intent="reject",
                    last_product_id=current_product["id"],
                    last_category=_item_category(current_product),
                    last_filters=["reject"]
                )
        elif parsed_intent.get("category"):
            update_data["current_category"] = parsed_intent["category"]
        print(f"[DECISION] contextual_recommendation intent={parsed_intent['intent']} kind={intent_kind} current_product={current_product_id}")
        _state_update("recommendation_flow", data=update_data, force=True)
        response = _format_recommendations(_recommend_products(intent_kind), kind=intent_kind)
        print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
        return response

    if text.isdigit() and active_convo and current_step in {"recommendation_flow", "browsing_catalog"}:
        index = int(text) - 1
        context_data = _read_convo_data(active_convo)
        source_ids = context_data.get("last_shown_product_ids") or context_data.get("suggested_product_ids") or []
        if not source_ids and current_step == "browsing_catalog":
            source_ids = [item["id"] for item in items[:10]]
        selected_id = source_ids[index] if 0 <= index < len(source_ids) else None
        selected_item = next((item for item in items if selected_id and item["id"] == selected_id), None)
        if selected_item:
            print(f"[DECISION] numeric_context_select step={current_step} index={text} product_id={selected_id}")
            _state_update(
                "product_selected",
                known_ids=[selected_item["id"]],
                data={
                    "source": f"{current_step}_number_select",
                    "last_bot_message_type": "product_options",
                    "last_shown_product_ids": [selected_item["id"]],
                    "current_product": selected_item["id"],
                    "current_category": _item_category(selected_item),
                    "last_intent": "product_lookup",
                    "pending_question": "product_followup"
                },
                force=True
            )
            print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
            return _with_product_image(selected_item, _format_product_message(selected_item))

    if parsed_intent["intent"] == "greeting":
        _reset_checkout_session_state("greeting")
        has_context = bool(active_convo and any(state_before.get(key) for key in ("current_product", "current_category", "last_shown_product_ids", "suggested_product_ids")))
        if not has_context:
            print("[DECISION] greeting_empty_session_menu")
            response = _send_menu()
            print(f"[STATE_AFTER] {dict(active_convo) if active_convo else None}")
            return response
        print("[DECISION] greeting_with_context_continue")
        return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}\n{t(lang, 'back_catalog')}"

    intent = detect_intent(text)
    if any(term in text for term in ["ما الفرق", "الفرق بينهم", "قارن", "compare"]):
        return _compare_last_shown_products()

    if _explicit_reset_requested():
        _reset_checkout_session_state("explicit_reset")
        _state_update("greeting", known_ids=[], data={"last_bot_message_type": "conversation_reset"}, force=True)
        return _send_menu(explicit=True)

    convo_data = _read_convo_data(active_convo)
    short_intent = _contextual_short_intent()
    active_state = convo_data.get("conversation_state")
    has_session_context = any(convo_data.get(key) for key in ("current_category", "current_product", "pending_question", "last_question", "last_shown_product_ids", "suggested_product_ids"))
    if active_convo and (current_step in {"recommendation_flow", "product_selected", "browsing_catalog"} or active_state != "idle" or has_session_context) and short_intent:
        if short_intent in {"yes", "select_last"}:
            last_ids = convo_data.get("last_shown_product_ids") or convo_data.get("suggested_product_ids") or []
            selected_item = next((item for item in items if last_ids and item["id"] == last_ids[0]), None)
            if selected_item:
                _state_update(
                    "product_selected",
                    known_ids=[selected_item["id"]],
                    data={
                        "last_bot_message_type": "product_options",
                        "last_question_type": "product_followup",
                        "last_question": "product_followup",
                        "current_product": selected_item["id"],
                        "current_category": convo_data.get("current_category") or _item_category(selected_item),
                        "pending_question": "product_followup"
                    },
                    force=True
                )
                return _with_product_image(selected_item, _format_product_message(selected_item))
            last_kind = convo_data.get("last_intent") or "premium"
            return _format_recommendations(_recommend_products(last_kind), kind=last_kind)
        if short_intent == "no":
            _reset_checkout_session_state("short_no")
            last_ids = convo_data.get("last_shown_product_ids") or convo_data.get("suggested_product_ids") or []
            rejected_id = last_ids[0] if last_ids else convo_data.get("current_product")
            rejected_item = next((item for item in items if rejected_id and item["id"] == rejected_id), None)
            if rejected_item:
                _profile_update(
                    rejected=[rejected_item["id"]],
                    last_intent="reject",
                    last_product_id=rejected_item["id"],
                    last_category=_item_category(rejected_item),
                    last_filters=["reject"]
                )
            _state_update(
                "recommendation_flow",
                data={
                    "last_bot_message_type": "recommendation_refine",
                    "last_question_type": "recommendation_followup",
                    "rejected_product_id": rejected_id
                },
                force=True
            )
            return t(lang, "reject_options")
        kind_map = {
            "price": "price",
            "premium": "premium",
            "sport": "sport",
            "daily": "daily",
            "strong": "strong",
            "fresh": "fresh",
        }
        mapped_kind = kind_map.get(short_intent)
        if mapped_kind:
            _state_update(
                "recommendation_flow",
                data={
                    "last_intent": mapped_kind,
                    "last_filters": _append_unique(convo_data.get("last_filters") or [], mapped_kind, 10),
                    "last_question": "recommendation_followup",
                    "last_question_type": "recommendation_followup"
                },
                force=True
            )
            return _format_recommendations(_recommend_products(mapped_kind), kind=mapped_kind)

    unsure_words = ["لا أعرف", "لا اعرف", "ما أعرف", "ما اعرف", "محتار", "محتارة", "مش عارف", "مو عارف"]
    vector_matches = _vector_hybrid_rank(message_text, semantic_intent, limit=4)
    smart_matches = vector_matches or _smart_catalog_matches(limit=4)
    if smart_matches:
        selected_item = smart_matches[0]
        match_ids = [item["id"] for item in smart_matches]
        _state_update(
            "product_selected",
            known_ids=[selected_item["id"]],
            data={
                "source": "vector_hybrid_match" if vector_matches else "smart_catalog_match",
                "last_bot_message_type": "product_options",
                "last_shown_product_ids": match_ids,
                "suggested_product_ids": match_ids,
                "last_question_type": "product_followup",
                "last_question": "product_followup",
                "last_intent": "vector_hybrid_match" if vector_matches else "smart_catalog_match",
                "last_category": _item_category(selected_item),
                "current_category": _detect_requested_category() or _read_convo_data(active_convo).get("current_category") or _item_category(selected_item),
                "recommended_product_ids": match_ids,
                "current_product": selected_item["id"],
                "pending_question": "product_followup"
            },
            force=True
        )
        _profile_update(
            recommended=match_ids,
            favorite_categories=[_item_category(item) for item in smart_matches],
            last_intent="vector_hybrid_match" if vector_matches else "smart_catalog_match",
            last_product_id=selected_item["id"]
        )
        if len(smart_matches) > 1:
            return _format_recommendations(
                (smart_matches[:3], False),
                intro=t(lang, "similar_products_intro"),
                outro=t(lang, "order_product_prompt")
            )
        return _with_product_image(selected_item, _format_product_message(selected_item))

    if _generic_request_detected():
        _state_update(
            "browsing_catalog",
            data={"last_bot_message_type": "business_questions"},
            force=True
        )
        return _business_questions()

    if any(word in text for word in unsure_words):
        return _unsure_recommendations()

    if intent == "reject_product":
        rejected_product_id = None
        if active_convo:
            try:
                current_ids = json.loads(active_convo["known_catalog_ids_json"] or "[]")
            except (TypeError, ValueError):
                current_ids = []
            rejected_product_id = current_ids[0] if current_ids else None
        _state_update(
            "browsing_catalog",
            known_ids=[],
            data={
                "last_bot_message_type": "product_rejected",
                "rejected_product_id": rejected_product_id
            },
            force=True
        )
        if rejected_product_id:
            rejected_item = next((item for item in items if item["id"] == rejected_product_id), None)
            _profile_update(
                rejected=[rejected_product_id],
                last_intent="reject",
                last_product_id=rejected_product_id,
                last_category=_item_category(rejected_item) if rejected_item else None,
                last_filters=["reject"]
            )

        return t(lang, "reject_options")

    if intent == "price_objection":
        return _format_recommendations(
            _recommend_products("price"),
            intro=random.choice([t(lang, "price_intro_1"), t(lang, "price_intro_2"), t(lang, "price_intro_3")]),
            outro=random.choice([t(lang, "price_outro_1"), t(lang, "price_outro_2"), t(lang, "price_outro_3")]),
            kind="price"
        )

    if intent == "premium_request":
        return _format_recommendations(
            _recommend_products("premium"),
            intro=random.choice([t(lang, "premium_intro_1"), t(lang, "premium_intro_2"), t(lang, "premium_intro_3")]),
            outro=random.choice([t(lang, "premium_outro_1"), t(lang, "premium_outro_2"), t(lang, "premium_outro_3")]),
            kind="premium"
        )

    if active_convo and _read_convo_data(active_convo).get("last_bot_message_type") == "product_rejected":
        if text == "1":
            return _format_recommendations(_recommend_products("premium"))
        if text == "2":
            return _format_recommendations(_recommend_products("strong"))
        if text == "3":
            return _format_recommendations(_recommend_products("price"))
        if text == "4":
            return _send_menu(explicit=True)

    if active_convo and active_convo["current_step"] == "product_selected":
        if text in ["نعم", "لا", "ناتو", "أوكي", "اوكي", "ok", "okay"]:
            _reset_checkout_session_state("generic_short_reply")
            return _send_menu(explicit=False)
        if text == "2":
            _state_update(
                "awaiting_order_info",
                data={"last_bot_message_type": "order_info_request"},
                force=True
            )
            return t(lang, "send_name_city")

        if text == "1":
            return f"{t(lang, 'details_already')}\n\n{t(lang, 'buy_now')}"

        if text == "3":
            _state_update("browsing_catalog", data={"last_bot_message_type": "catalog_list"})

        elif text in ["شراء", "الشراء", "buy", "purchase"]:
            _state_update(
                "awaiting_order_info",
                data={"last_bot_message_type": "order_info_request"},
                force=True
            )
            return t(lang, "send_name_city")

    if active_convo and active_convo["current_step"] == "awaiting_order_info":
        details_text = (message_text or "").strip()
        order_context_data = _read_convo_data(active_convo)
        order_flow = _purchase_flow(order_context_data)
        order_collected = order_flow.get("collected") or {}
        requested_product = _find_product_by_text(text)
        if requested_product:
            _state_update(
                "product_selected",
                known_ids=[requested_product["id"]],
                data={"source": "product_search", "last_bot_message_type": "product_options"},
                force=True
            )
            return _with_product_image(requested_product, _format_product_message(requested_product))

        if not details_text and not order_collected:
            return t(lang, "send_name_city")

        parts = [part.strip() for part in details_text.replace("\n", ",").split(",") if part.strip()]
        customer_name = parts[0] if parts else details_text
        city = ", ".join(parts[1:]) if len(parts) > 1 else ""
        if not city and " " in details_text:
            name_parts = details_text.split()
            if len(name_parts) >= 2:
                customer_name = " ".join(name_parts[:-1])
                city = name_parts[-1]
        customer_name = order_collected.get("name") or customer_name
        city = order_collected.get("location") or city

        explicit_confirmation = _is_explicit_purchase_confirmation(details_text) or _is_explicit_purchase_confirmation(message_text)

        # Do not confirm an order unless we have a real product context and
        # both customer name + city are explicitly collected.
        if not _has_real_checkout_context(None, {"name": customer_name, "location": city}, explicit_confirmation):
            return t(lang, "send_name_city")

        con = get_db_connection()
        []:
        return item_ids = json.loads(active_convo["known_catalog_ids_json"] or "[]")
            selected_item = None
            if item_ids:
                selected_item = con.execute("""
                    SELECT *
                    FROM catalogs
                    WHERE id=? AND client_id=?
                    LIMIT 1
                """, (item_ids[0], client_id)).fetchone()

            if not selected_item:
                return t(lang, "send_name_city")

            price = _item_price(selected_item)
            order_object = {
                "name": customer_name,
                "phone_number": sender_phone,
                "product": selected_item["title"],
                "price": price,
                "city": city
            }
            if order_collected.get("size"):
                order_object["size"] = order_collected.get("size")
            if order_collected.get("color"):
                order_object["color"] = order_collected.get("color")
            if order_collected.get("payment_method"):
                order_object["payment_method"] = order_collected.get("payment_method")

            con.execute("""
                INSERT INTO orders
                    (client_id, phone, name, items, status, amount, intent, customer_phone)
                VALUES (?, ?, ?, ?, 'pending', ?, 'order', ?)
            """, (
                client_id,
                sender_phone,
                customer_name,
                json.dumps([order_object], ensure_ascii=False),
                float(price or 0),
                sender_phone
            ))
            con.commit()
            _state_update(
                "greeting",
                known_ids=[],
                data={"last_bot_message_type": "order_confirmed", "purchase_flow": dict(order_flow, state="confirmed")},
                force=True
            )
            _checkout_log("[CHECKOUT_STATE]", {"from": order_flow.get("state"), "to": "confirmed", "order_created": True})
            _profile_update(
                purchased=[selected_item["id"]],
                favorite_categories=[_item_category(selected_item)],
                last_intent="purchase",
                last_product_id=selected_item["id"],
                last_category=_item_category(selected_item)
            )
            print(f"[ORDER_CREATED] client_id={client_id} phone={sender_phone!r} product={selected_item['title']!r} city={city!r}")
            print("[NEW ORDER]")
            print(f"Name: {customer_name}")
            print(f"Phone: {sender_phone}")
            print(f"Product: {selected_item['title']}")
            print(f"City: {city}")
        finally:
            con.close()

        return t(lang, "order_received")

    if not items:
        return t(lang, "no_items").format(business_name=business_name)

    if active_convo:
        try:
            updated_at = datetime.datetime.fromisoformat((active_convo["updated_at"] or "").replace("Z", "+00:00"))
            if updated_at.tzinfo is not None:
                updated_at = updated_at.replace(tzinfo=None)
            inactive_for = datetime.datetime.now() - updated_at
        except (TypeError, ValueError):
            inactive_for = datetime.timedelta()
        if inactive_for > datetime.timedelta(minutes=30):
            _state_update("greeting", known_ids=[], data={"last_bot_message_type": "timeout_reset"})
            active_convo = None
            current_step = "greeting"

    print("[DEBUG_BEFORE_PRODUCT_SELECTED]")
    print("TEXT =", text)
    if active_convo:
        print("CURRENT_STEP =", active_convo["current_step"])
    else:
        print("NO ACTIVE CONVO")

    if active_convo and active_convo["current_step"] == "product_selected":
        if text in ["2", "شراء", "الشراء", "buy", "purchase"]:
            con = get_db_connection()
            try:
                con.execute("""
                    UPDATE conversations
                    SET current_step='awaiting_order_info',
                        updated_at=CURRENT_TIMESTAMP
                    WHERE client_id=? AND phone=?
                """, (client_id, sender_phone))
                con.commit()
            finally:
                con.close()

            return t(lang, "send_name_city")

        if text in ["1", "تفاصيل", "عرض التفاصيل", "details"]:
            item_ids = []
            try:
                item_ids = json.loads(active_convo["known_catalog_ids_json"] or "[]")
            except (TypeError, ValueError):
                item_ids = []
            selected_item = next((item for item in items if item_ids and item["id"] == item_ids[0]), None)
            if selected_item:
                return _set_message_and_return("product_details", _with_product_image(selected_item, _format_item(selected_item)))
            return f"{t(lang, 'what_next')}\n{t(lang, 'buy_now')}"

        if text in ["نعم", "yes", "ok", "تمام"]:
            return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}"

        if text == "3":
            if _was_recently_sent("catalog_list"):
                return t(lang, "catalog_already")
            heading = t(lang, "catalog_heading").format(business_name=business_name)
            lines = [heading, ""]
            for i, item in enumerate(items[:10], start=1):
                price = _format_price(item)
                if price:
                    lines.append(f"{i}. {item['title']} - {price}")
                else:
                    lines.append(f"{i}. {item['title']}")
            lines.append(t(lang, "send_product_name"))
            _state_update("browsing_catalog", data={"last_bot_message_type": "catalog_list"})
            return "\n".join(lines)
        return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}\n{t(lang, 'back_catalog')}"

    if active_convo and active_convo["current_step"] == "conversation_closed":
        reopen_words = {"hi", "hello", "start", "menu", "catalog", "مرحبا", "السلام", "ابدأ", "القائمة", "الكتالوج"}
        if any(word in text for word in reopen_words):
            _state_update("greeting", known_ids=[], data={"last_bot_message_type": "conversation_reopened"})
            active_convo = None
        else:
            return t(lang, "closed_thanks")

    if active_convo and active_convo["current_step"] == "awaiting_order_details":
        details_text = (message_text or "").strip()
        parts = [part.strip() for part in details_text.replace("\n", ",").split(",") if part.strip()]
        customer_name = parts[0] if parts else details_text
        city = ", ".join(parts[1:]) if len(parts) > 1 else ""
        if not city and " " in details_text:
            name_parts = details_text.split()
            if len(name_parts) >= 2:
                customer_name = " ".join(name_parts[:-1])
                city = name_parts[-1]

        con = get_db_connection()
        try:
            item_ids = json.loads(active_convo["known_catalog_ids_json"] or "[]")
            selected_item = None
            if item_ids:
                selected_item = con.execute("""
                    SELECT *
                    FROM catalogs
                    WHERE id=? AND client_id=?
                    LIMIT 1
                """, (item_ids[0], client_id)).fetchone()

            if not selected_item:
                _state_update("greeting", known_ids=[], data={"last_bot_message_type": "state_reset"}, force=True)
                return _send_menu()

            price = _item_price(selected_item)
            order_object = {
                "name": customer_name,
                "phone_number": sender_phone,
                "product": selected_item["title"],
                "price": price,
                "city": city
            }

            con.execute("""
                INSERT INTO orders
                    (client_id, phone, name, items, status, amount, intent, customer_phone)
                VALUES (?, ?, ?, ?, 'pending', ?, 'order', ?)
            """, (
                client_id,
                sender_phone,
                customer_name,
                json.dumps([order_object], ensure_ascii=False),
                float(price or 0),
                sender_phone
            ))
            con.commit()
            _state_update("order_confirmed", data={"name": customer_name, "city": city, "last_bot_message_type": "order_confirmation"})
            _state_update("conversation_closed", known_ids=[], data={"last_bot_message_type": "order_confirmation"})
            _profile_update(
                purchased=[selected_item["id"]],
                favorite_categories=[_item_category(selected_item)],
                last_intent="purchase",
                last_product_id=selected_item["id"],
                last_category=_item_category(selected_item)
            )
            print(f"[ORDER_CREATED] client_id={client_id} phone={sender_phone!r} product={selected_item['title']!r} city={city!r}")
            print("[NEW ORDER]")
            print(f"Name: {customer_name}")
            print(f"Phone: {sender_phone}")
            print(f"Product: {selected_item['title']}")
            print(f"City: {city}")
        finally:
            con.close()

        return t(lang, "order_received")

    if False and active_convo and active_convo["current_step"] == "post_order_engagement":
        post_order_data = _read_convo_data(active_convo)
        item_ids = []
        try:
            item_ids = json.loads(active_convo["known_catalog_ids_json"] or "[]")
        except (TypeError, ValueError):
            item_ids = []

        selected_item = None
        if item_ids:
            selected_item = next((item for item in items if item["id"] == item_ids[0]), None)

        if text == "1":
            related_items = []
            if selected_item:
                selected_type = (selected_item["type"] or "").lower()
                selected_words = set((selected_item["title"] or "").lower().split())
                for item in items:
                    if item["id"] == selected_item["id"]:
                        continue
                    item_type = (item["type"] or "").lower()
                    item_words = set((item["title"] or "").lower().split())
                    if selected_type and item_type == selected_type:
                        related_items.append(item)
                    elif selected_words and selected_words.intersection(item_words):
                        related_items.append(item)
                    if len(related_items) >= 3:
                        break
            if not related_items:
                related_items = [item for item in items if not selected_item or item["id"] != selected_item["id"]][:3]
            if related_items:
                lines = [t(lang, "similar_products_intro"), ""]
                for i, item in enumerate(related_items, start=1):
                    lines.append(_format_item(item, i))
                    lines.append("")
                lines.append(t(lang, "order_product_prompt"))
                return "\n".join(lines).strip()
            return _send_menu()

        if text == "2":
            discounted_items = [
                item for item in items
                if item["sale_price"] not in (None, "", 0, "0")
            ][:3]
            if discounted_items:
                lines = [t(lang, "special_offers_intro"), ""]
                for i, item in enumerate(discounted_items, start=1):
                    lines.append(_format_item(item, i))
                    lines.append("")
                lines.append(t(lang, "order_product_prompt"))
                return "\n".join(lines).strip()
            return t(lang, "no_special_offers") + "\n\n" + _send_menu()

        if text == "3":
            _state_update("conversation_closed", known_ids=[], data={"last_bot_message_type": "conversation_closed"})
            return t(lang, "closed_thanks")

        if post_order_data.get("last_bot_message_type") == "post_order_options":
            return t(lang, "post_order_options")

        _mark_last_message("post_order_options")
        return t(lang, "post_order_options")

    explicit_purchase_words = {"buy", "purchase", "acheter", "comprar", "comprare", "شراء", "أريد الشراء", "اريد الشراء", "اطلب الآن", "اطلب الان"}
    if text in explicit_purchase_words:
        return t(lang, "send_product_name")

    catalog_words = {
        "1", "catalog", "products", "services", "catalogue", "productos", "servicios", "prodotti", "servizi",
        "الكتالوج", "الخدمات", "المنتجات"
    }
    catalog_search_words = catalog_words | {
        "menu", "price", "prices", "browse", "prix", "precio", "prezzo",
        "القائمة", "الأسعار", "السعر", "تصفح"
    }

    if active_convo and active_convo["current_step"] in ["greeting", "browsing_catalog"]:
        if text == "2":
            return t(lang, "price_request_prompt")
        elif text == "3":
            _state_update(
                "awaiting_booking_info",
                data={"last_bot_message_type": "booking_request"}
            )
            return t(lang, "booking_prompt")
        elif text == "4":
            _state_update(
                "support_requested",
                data={"last_bot_message_type": "support_request"}
            )
            return t(lang, "support_prompt")

    if not text or text in catalog_words or any(word in text for word in catalog_search_words):
        explicit_catalog_request = text in explicit_menu_requests or text in catalog_words or any(word in text for word in explicit_menu_requests)
        if _was_recently_sent("catalog_list") and not explicit_catalog_request:
            return t(lang, "catalog_already")
        heading = t(lang, "catalog_heading").format(business_name=business_name)
        lines = [heading, ""]
        for i, item in enumerate(items[:10], start=1):
            price = _format_price(item)
            if price:
                lines.append(f"{i}. {item['title']} - {price}")
            else:
                lines.append(f"{i}. {item['title']}")
        if len(items) > 10:
            lines.append(t(lang, "search_more"))
        lines.append(t(lang, "send_product_name"))
        _state_update("browsing_catalog", data={"last_bot_message_type": "catalog_list"})
        return "\n".join(lines)

    intent_groups = {
        "luxury": ["luxury", "premium", "fancy", "elegant", "فخم", "فاخر", "راقي"],
        "strong": ["strong", "intense", "bold", "powerful", "قوي", "مركز"],
        "fast": ["fast", "speed", "quick", "rapid", "سريع"],
        "budget": ["cheap", "budget", "affordable", "رخيص", "اقتصادي"],
        "men": ["men", "man's", "mens", "male", "رجالي", "للرجال", "رجل"],
        "women": ["women", "woman", "female", "ladies", "نسائي", "للنساء", "امرأة"],
        "gift": ["gift", "present", "هدية", "هديه"]
    }
    intent_terms = []
    detected_intents = []
    for intent_name, terms in intent_groups.items():
        if any(term in text for term in terms):
            detected_intents.append(intent_name)
            intent_terms.extend(terms)

    if detected_intents:
        scored_items = []
        for item in items:
            searchable = " ".join([
                item["title"] or "",
                item["description"] or "",
                item["type"] or ""
            ]).lower()
            score = 0
            for term in intent_terms:
                if term in searchable:
                    score += 1
            for word in text.replace(",", " ").replace(".", " ").split():
                if len(word) > 2 and word in searchable:
                    score += 1
            if score:
                scored_items.append((score, item))

        scored_items.sort(key=lambda pair: pair[0], reverse=True)
        ai_match = scored_items[0][1] if scored_items else None

        if ai_match:
            _state_update(
                "product_selected",
                known_ids=[ai_match["id"]],
                data={"source": "ai_recommendation", "last_bot_message_type": "product_options"},
                force=True
            )
            print("[PRODUCT_STATE_FORCED] product_selected saved")
            print("[DEBUG_AFTER_PRODUCT_SELECTED]", sender_phone)
            check_convo = _fetch_active_convo()
            if check_convo:
                print("[DEBUG_SAVED_STEP]", check_convo["current_step"])
            else:
                print("[DEBUG_SAVED_STEP] NO CONVERSATION")

            print(f"[AI_MATCH] intent={','.join(detected_intents)} score={scored_items[0][0]} product={ai_match['title']}")
            return _with_product_image(ai_match, _format_product_message(ai_match))

        return t(lang, "unknown")

    tokens = [token for token in text.replace(",", " ").replace(".", " ").split() if len(token) > 1]
    matches = []
    for item in items:
        searchable_parts = [
            item["title"] or "",
            item["type"] or "",
            item["description"] or "",
            " ".join(aliases_by_item.get(item["id"], [])),
        ]
        searchable = " ".join(searchable_parts).lower()
        score = 0
        if text and text in searchable:
            score += 5
        for token in tokens:
            if token in searchable:
                score += 1
        if score:
            matches.append((score, item))

    matches.sort(key=lambda pair: pair[0], reverse=True)
    matched_items = [item for _, item in matches[:5]]

    if matched_items:
        selected_item = matched_items[0]
        _state_update(
            "product_selected",
            known_ids=[selected_item["id"]],
            data={"source": "product_search", "last_bot_message_type": "product_options"},
            force=True
        )
        print("[PRODUCT_STATE_FORCED] product_selected saved")
        print("[DEBUG_AFTER_PRODUCT_SELECTED]", sender_phone)
        check_convo = _fetch_active_convo()
        if check_convo:
            print("[DEBUG_SAVED_STEP]", check_convo["current_step"])
        else:
            print("[DEBUG_SAVED_STEP] NO CONVERSATION")

        return _with_product_image(selected_item, _format_product_message(selected_item))

    description_matches = []
    for item in items:
        description = (item["description"] or "").lower()
        if any(word in description for word in text.split()):
            description_matches.append(item)

    if description_matches:
        response = f"🔥 {t(lang, 'similar_products_intro')}\n\n"
        for i, item in enumerate(description_matches, 1):
            price = _format_price(item)
            if price:
                response += f"{i}. {item['title']} - {price}\n"
            else:
                response += f"{i}. {item['title']}\n"
        return response.strip()

    final_data = _read_convo_data(active_convo)
    if active_convo and final_data.get("conversation_state") not in (None, "", "idle"):
        return f"{t(lang, 'what_next')}\n{t(lang, 'view_details')}\n{t(lang, 'buy_now')}\n{t(lang, 'back_catalog')}"
    return _send_menu()

@app.route("/webhook", methods=["GET", "POST"])
@app.route("/whatsapp", methods=["GET", "POST"])
def whatsapp():
    """Meta WhatsApp Cloud API webhook endpoint.
    
    GET: Verifies webhook subscription
    POST: Processes incoming messages and sends replies
    """
    if request.method == "GET":
        # ── Webhook verification ──────────────────────────────────────────
        mode           = request.args.get("hub.mode")
        challenge      = request.args.get("hub.challenge")
        verify_token   = request.args.get("hub.verify_token")
        
        print(f"[META_WEBHOOK_VERIFICATION] mode={mode!r} token_match={verify_token == '123456'}")
        
        if mode == "subscribe" and verify_token == "123456":
            print(f"[META_WEBHOOK_VERIFIED] challenge accepted")
            return challenge, 200
        
        print(f"[META_WEBHOOK_VERIFICATION_FAILED] mode={mode!r} token_match={verify_token == '123456'}")
        return "Forbidden", 403
    
    # ── POST: Process incoming message ────────────────────────────────────
    print("[META_WEBHOOK_RECEIVED] POST request received")
    
    try:
        payload = request.get_json(force=True, silent=True) or {}
        print(f"[META_WEBHOOK_PAYLOAD] {json.dumps(payload, ensure_ascii=False)}")
        print("Incoming:", payload)
        
        # Extract message data from Meta webhook structure
        # Structure: { "entry": [{ "changes": [{ "value": { "messages": [...] } }] }] }
        entry = (payload.get("entry") or [{}])[0]
        change = (entry.get("changes") or [{}])[0]
        value = change.get("value") or {}
        messages = value.get("messages") or []
        metadata = value.get("metadata") or {}
        phone_number_id = metadata.get("phone_number_id") or META_PHONE_NUMBER_ID
        
        print(f"[META_PARSE] entry={bool(entry)}, change={bool(change)}, value={bool(value)}, messages_count={len(messages)}")
        
        if not messages:
            print("[META_WEBHOOK_RECEIVED] no messages in payload")
            return jsonify({"status": "ok"}), 200
        
        message = messages[0]
        sender_phone = message.get("from")
        message_text = (message.get("text") or {}).get("body") or ""
        message_type = message.get("type", "text")
        
        print(f"[META_INCOMING_MESSAGE] from={sender_phone!r} type={message_type!r} text={message_text!r}")
        
        if not sender_phone or not message_text or message_type != "text":
            print("[META_WEBHOOK_RECEIVED] skipped non-text message")
            return jsonify({"status": "ok"}), 200
        
        # ── Send automatic reply using Meta Cloud API ──────────────────────
        bot_client_id = resolve_whatsapp_client_id(phone_number_id)
        reply_text = generate_bot_reply(bot_client_id, sender_phone, message_text)
        if isinstance(reply_text, list):
            response = None
            for reply_part in reply_text:
                if isinstance(reply_part, dict) and reply_part.get("type") == "image":
                    response = meta_send_image(sender_phone, reply_part.get("image_url"), reply_part.get("caption", ""))
                elif isinstance(reply_part, dict):
                    clean_reply = _format_raw_product_reply(reply_part) or str(reply_part.get("text") or reply_part.get("body") or reply_part.get("caption") or "")
                    response = meta_send_message(sender_phone, clean_reply)
                else:
                    response = meta_send_message(sender_phone, reply_part)
        elif isinstance(reply_text, dict) and reply_text.get("type") == "image":
            response = meta_send_image(sender_phone, reply_text.get("image_url"), reply_text.get("caption", ""))
        elif isinstance(reply_text, dict):
            clean_reply = _format_raw_product_reply(reply_text) or str(reply_text.get("text") or reply_text.get("body") or reply_text.get("caption") or "")
            response = meta_send_message(sender_phone, clean_reply)
        else:
            response = meta_send_message(sender_phone, reply_text)
        
        if response and response.status_code == 200:
            print(f"[META_REPLY_SENT] to={sender_phone!r} status={response.status_code}")
        else:
            status = response.status_code if response else "N/A"
            print(f"[META_REPLY_SENT] to={sender_phone!r} status={status} error=failed to send")
        
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        import traceback
        print(f"[META_WEBHOOK_ERROR] {repr(e)}")
        print(traceback.format_exc())
        return jsonify({"status": "error"}), 500


@app.route("/build-id")
def build_id():
    return "BUILD_ID: META_WHATSAPP_CLOUD_API_001", 200


@app.route("/admin/dashboard")
def admin_dashboard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid    = _session_client_id()
    client = get_client(cid)

    if not (int(client.get("onboarding_step") or 0) >= 5):
        print(f"[ONBOARDING_STEP] client={cid} step={client.get('onboarding_step', 0)} → redirect to onboarding")
        return redirect(url_for("admin_onboarding"))

    con = get_db_connection()
    try:
        total_orders  = con.execute("SELECT COUNT(*) FROM orders WHERE client_id=?", (cid,)).fetchone()[0]
        today_str     = datetime.datetime.now().strftime("%Y-%m-%d")
        today_orders  = con.execute(
            "SELECT COUNT(*) FROM orders WHERE client_id=? AND created_at LIKE ?",
            (cid, today_str + "%")
        ).fetchone()[0]
        catalog_count = con.execute(
            "SELECT COUNT(*) FROM catalogs WHERE client_id=? AND is_active=1", (cid,)
        ).fetchone()[0]
        active_convos = con.execute(
            "SELECT COUNT(*) FROM whatsapp_state WHERE current_step != 'service'"
        ).fetchone()[0]
        recent_orders = [dict(r) for r in con.execute(
            "SELECT * FROM orders WHERE client_id=? ORDER BY id DESC LIMIT 10", (cid,)
        ).fetchall()]
    finally:
        con.close()

    sub = get_client_subscription(cid)
    referral_link = f"{request.host_url.rstrip('/')}signup?ref={client.get('referral_code', '')}"
    stats = dict(total_orders=total_orders, today_orders=today_orders,
                 catalog_count=catalog_count, active_convos=active_convos)
    
    expire_trial_if_needed(cid)
    _fresh_client = get_client(cid)
    trial_info    = get_trial_status(_fresh_client)
    
    _aff_con = get_db_connection()
    try:
        _aff_count = _aff_con.execute(
            "SELECT COUNT(*) FROM users WHERE affiliate_id=?", (cid,)
        ).fetchone()[0]
    finally:
        _aff_con.close()
    affiliate_link = f"{request.host_url.rstrip('/')}signup?aff={_fresh_client.get('affiliate_code', '')}"
    affiliate_info = {
        "enabled":  _fresh_client.get("affiliate_enabled", 1),
        "code":     _fresh_client.get("affiliate_code", ""),
        "earnings": _fresh_client.get("affiliate_earnings") or 0.0,
        "count":    _aff_count,
        "rate":     int((_fresh_client.get("affiliate_rate") or 0.20) * 100),
        "link":     affiliate_link,
    }
    return render_template("admin/dashboard.html", client=client, stats=stats,
                           recent_orders=recent_orders, sub=sub,
                           referral_link=referral_link, active="dashboard",
                           trial_info=trial_info, affiliate_info=affiliate_info)


@app.route("/admin/connect-whatsapp", methods=["GET", "POST"])
def admin_connect_whatsapp():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    cid = _session_client_id()
    client = get_client(cid)
    lang = client.get("default_language") or "en"

    if request.method == "POST" and request.form.get("action") == "disconnect":
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients
                SET whatsapp_connected=0,
                    whatsapp_connection_status='not_connected',
                    business_whatsapp_number=NULL
                WHERE id=?
            """, (cid,))
            con.commit()
        finally:
            con.close()
        flash("WhatsApp disconnected.", "success")
        return redirect(url_for("admin_connect_whatsapp"))

    if META_ACCESS_TOKEN and META_PHONE_NUMBER_ID and not client.get("whatsapp_connected"):
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients
                SET whatsapp_connected=1,
                    whatsapp_connection_status='connected',
                    whatsapp_provider='meta',
                    business_whatsapp_number=COALESCE(business_whatsapp_number, ?)
                WHERE id=?
            """, (META_PHONE_NUMBER_ID, cid))
            con.commit()
        finally:
            con.close()
        client = get_client(cid)

    wa_deeplink = None
    if META_PHONE_NUMBER_ID:
        message = quote_plus("START")
        wa_deeplink = f"https://wa.me/{META_PHONE_NUMBER_ID}?text={message}"

    return render_template("admin/connect_whatsapp.html", client=client,
                           wa_deeplink=wa_deeplink, lang=lang,
                           active="whatsapp")


@app.route("/admin/onboarding", methods=["GET", "POST"])
def admin_onboarding():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    
    cid    = _session_client_id()
    client = get_client(cid)
    _lang  = client.get("default_language") or "en"

    if request.method == "POST":
        action = request.form.get("action", "")
        cur_step = int(client.get("onboarding_step") or 0)

        def _advance(new_step, updates=None):
            con = get_db_connection()
            try:
                if updates:
                    set_clause = ", ".join(f"{k}=?" for k in updates)
                    vals = list(updates.values()) + [cid]
                    con.execute(f"UPDATE clients SET {set_clause} WHERE id=?", vals)
                con.execute("UPDATE clients SET onboarding_step=? WHERE id=?",
                            (max(cur_step, new_step), cid))
                con.commit()
            finally:
                con.close()

        if action == "welcome_done":
            _advance(1)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=0 (welcome)")
            return redirect(url_for("admin_onboarding"))

        elif action == "save_business":
            biz_name = request.form.get("name", "").strip()
            biz_type = request.form.get("business_type", "").strip()
            lang_val  = request.form.get("default_language", "en").strip()
            currency  = request.form.get("currency", "").strip()
            timezone  = request.form.get("timezone", "").strip()
            updates = {}

            if biz_name:
                updates["name"] = biz_name

            if biz_type:
                updates["business_type"] = biz_type

            if lang_val:
                updates["default_language"] = lang_val

            if currency:
                updates["currency"] = currency

            if timezone:
                updates["timezone"] = timezone

            _advance(2, updates if updates else None)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=1 (business_info)")
            return redirect(url_for("admin_onboarding"))

        elif action == "save_ai":
            tone      = request.form.get("assistant_tone", "friendly").strip()
            goal      = request.form.get("assistant_goal", "book_appointments").strip()
            biz_desc  = request.form.get("business_description", "").strip()
            updates = {
                "assistant_tone":       tone,
                "assistant_goal":       goal,
                "business_description": biz_desc,
            }
            _advance(3, updates)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=2 (ai_settings)")
            return redirect(url_for("admin_onboarding"))

        elif action in ("whatsapp_done", "skip_whatsapp"):
            _advance(4)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=3 (whatsapp)")
            return redirect(url_for("admin_onboarding"))

        elif action == "complete":
            _now       = datetime.datetime.now()
            _trial_end = _now + datetime.timedelta(days=3)
            _now_iso   = _now.isoformat(timespec="seconds")
            _end_iso   = _trial_end.isoformat(timespec="seconds")
            con = get_db_connection()
            try:
                _existing = con.execute(
                    "SELECT is_trial, trial_started_at FROM clients WHERE id=?", (cid,)
                ).fetchone()
                if _existing and not _existing["is_trial"]:
                    con.execute("""
                        UPDATE clients
                        SET onboarding_step=5,
                            is_trial=1, is_active=1,
                            trial_started_at=?, trial_ends_at=?
                        WHERE id=?
                    """, (_now_iso, _end_iso, cid))
                    print(f"[TRIAL_STARTED] client={cid}")
                    track_event(cid, "trial_started", {})
                else:
                    con.execute("UPDATE clients SET onboarding_step=5 WHERE id=?", (cid,))
                con.commit()
            finally:
                con.close()
            track_event(cid, "onboarding_completed", {})
            print(f"[ONBOARDING_FINISHED] client={cid}")
            flash("Setup complete! Welcome to Filtrex AI.", "success")
            return redirect(url_for("admin_dashboard"))

        return redirect(url_for("admin_onboarding"))

    client = get_client(cid)
    step = int(client.get("onboarding_step") or 0)

    if step == 0:
        print(f"[ONBOARDING_STARTED] client={cid}")

    if step >= 5:
        return redirect(url_for("admin_dashboard"))

    wa_connected = bool(client.get("whatsapp_connected"))

    return render_template("admin/onboarding.html", client=client, step=step,
                           wa_connected=wa_connected, lang=_lang, active="dashboard")


@app.route("/onboarding", methods=["GET", "POST"])
def onboarding_alias():
    return admin_onboarding()


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        email    = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        con = get_db_connection()
        try:
            row = con.execute(
                "SELECT id, password, client_id, email FROM users WHERE email=? OR username=?",
                (email, email)
            ).fetchone()
        finally:
            con.close()
        if row and check_password_hash(row["password"], password):
            client_id = row["client_id"] or CLIENT_ID
            session.clear()
            session["logged_in"]  = True
            session["user_id"]    = row["id"]
            session["client_id"]  = client_id
            session["user_email"] = row["email"] or email
            print(f"[AUTH_LOGIN] email={email!r} client_id={client_id}")
            return redirect(url_for("admin_dashboard"))
        error = "Invalid email or password."
    return render_template("login.html", error=error)


@app.route("/signup", methods=["GET", "POST"])
def signup():
    error = None
    if request.method == "POST":
        business_name = request.form.get("business_name", "").strip()
        email         = request.form.get("email", "").strip().lower()
        password      = request.form.get("password", "").strip()
        if not business_name or not email or not password:
            error = "All fields are required."
        elif len(password) < 6:
            error = "Password must be at least 6 characters."
        else:
            con = get_db_connection()
            try:
                existing = con.execute(
                    "SELECT id FROM users WHERE email=?", (email,)
                ).fetchone()
                if existing:
                    error = "An account with this email already exists."
                else:
                    cur_c = con.execute("""
                        INSERT INTO clients
                            (name, business_type, default_language,
                             currency, timezone, is_active)
                        VALUES (?, 'other', 'ar', 'MAR', 'Africa/Casablanca', 1)
                    """, (business_name,))
                    new_client_id = cur_c.lastrowid
                    new_ref_code = generate_referral_code(new_client_id)
                    con.execute(
                        "UPDATE clients SET referral_code=? WHERE id=?",
                        (new_ref_code, new_client_id)
                    )
                    new_aff_code = generate_affiliate_code(new_client_id)
                    con.execute(
                        "UPDATE clients SET affiliate_code=? WHERE id=?",
                        (new_aff_code, new_client_id)
                    )
                    _t_now = datetime.datetime.now()
                    _t_end = _t_now + datetime.timedelta(days=3)
                    con.execute("""
                        UPDATE clients
                        SET    is_trial=1,
                               trial_started_at=?,
                               trial_ends_at=?,
                               plan='starter'
                        WHERE  id=?
                    """, (_t_now.isoformat(timespec="seconds"),
                          _t_end.isoformat(timespec="seconds"),
                          new_client_id))
                    cur_u = con.execute("""
                        INSERT INTO users (username, email, password, client_id)
                        VALUES (?, ?, ?, ?)
                    """, (email, email, generate_password_hash(password), new_client_id))
                    new_user_id = cur_u.lastrowid
                    con.commit()
                    print(f"[AUTH_SIGNUP] user_id={new_user_id} client_id={new_client_id}")
                    track_event(new_client_id, "user_registered", {"email": email})
                    
                    session.clear()
                    session["logged_in"]  = True
                    session["user_id"]    = new_user_id
                    session["client_id"]  = new_client_id
                    session["user_email"] = email
                    return redirect(url_for("admin_dashboard"))
            finally:
                con.close()
    return render_template("signup.html", error=error)
@app.route("/admin/catalog", methods=["GET", "POST"])
def admin_catalog():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    cid = _session_client_id()

    if request.method == "POST":
        allowed, sub = check_limit(cid, "catalog_items")
        if not allowed:
            flash("لقد وصلت إلى حد المنتجات في باقتك. قم بترقية الباقة.", "error")
            return redirect(url_for("admin_catalog"))

        title = request.form.get("title", "").strip()
        item_type = request.form.get("type", "service").strip()
        price = request.form.get("price", "0").strip()
        currency = request.form.get("currency", "").strip()
        sale_price = request.form.get("sale_price", "").strip()
        category = request.form.get("category", "").strip()
        description = request.form.get("description", "").strip()
        aliases_text = request.form.get("aliases", "").strip()
        product_hint = request.form.get("product_hint", "").strip()
        image_url = request.form.get("image_url", "").strip()
        uploaded_image_url = save_catalog_image(request.files.get("image_file"))
        if uploaded_image_url:
            image_url = uploaded_image_url
        duration_min = request.form.get("duration_min", "").strip()
        stock_qty = request.form.get("stock_qty", "").strip()

        if not title:
            flash("اسم المنتج أو الخدمة مطلوب.", "error")
            return redirect(url_for("admin_catalog"))

        try:
            price = float(price or 0)
        except ValueError:
            price = 0

        try:
            sale_price = float(sale_price) if sale_price else None
        except ValueError:
            sale_price = None

        try:
            duration_min = int(duration_min) if duration_min else None
        except ValueError:
            duration_min = None

        try:
            stock_qty = int(stock_qty) if stock_qty else None
        except ValueError:
            stock_qty = None

        intelligence, ai_error = analyze_catalog_product_with_ai(
            title=title,
            description=description,
            image_url=image_url,
            price=price,
            category=category,
            keywords=aliases_text,
            item_type=item_type,
            product_hint=product_hint
        )
        if ai_error:
            print(f"[AI_PRODUCT_ANALYZER_SKIP] reason={ai_error}")

        con = get_db_connection()
        try:
            cur = con.execute("""
                INSERT INTO catalogs
                    (client_id, title, type, price, currency, sale_price, category, description, image_url,
                     duration_min, stock_qty, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                cid, title, item_type, price, currency, sale_price,
                category, description, image_url, duration_min, stock_qty
            ))
            save_catalog_aliases(con, cur.lastrowid, aliases_text)
            save_catalog_ai_intelligence(con, cur.lastrowid, intelligence)
            con.commit()
        finally:
            con.close()

        flash("تمت إضافة المنتج/الخدمة بنجاح.", "success")
        return redirect(url_for("admin_catalog"))

    con = get_db_connection()
    try:
        items = [dict(r) for r in con.execute("""
            SELECT *
            FROM catalogs
            WHERE client_id=?
            ORDER BY id DESC
        """, (cid,)).fetchall()]
    finally:
        con.close()

    return render_template(
        "admin/catalog.html",
        items=items,
        active="catalog"
    )


@app.route("/catalog")
def catalog_alias():
    return redirect(url_for("admin_catalog"))


@app.route("/admin/catalog/ai-enrich", methods=["POST"])
def admin_catalog_ai_enrich():
    if not session.get("logged_in"):
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    form_data = request.form if request.form else {}
    title = (payload.get("title") or form_data.get("title") or "").strip()
    price = (payload.get("price") or form_data.get("price") or "").strip()
    currency = (payload.get("currency") or form_data.get("currency") or "").strip()
    item_type = (payload.get("type") or form_data.get("type") or "").strip()
    product_hint = (payload.get("product_hint") or form_data.get("product_hint") or "").strip()
    image_url = (payload.get("image_url") or form_data.get("image_url") or "").strip()
    image_file = request.files.get("image_file")

    if not title:
        return jsonify({"error": "اسم المنتج مطلوب قبل التوليد."}), 400
    detected_type = detect_product_type(title)

    previous_catalog_context = []
    con = get_db_connection()
    try:
        rows = con.execute("""
            SELECT title, type, category, description
            FROM catalogs
            WHERE client_id=?
              AND is_active=1
            ORDER BY id DESC
            LIMIT 12
        """, (_session_client_id(),)).fetchall()
        previous_catalog_context = [
            {
                "title": row["title"] or "",
                "type": row["type"] or "",
                "category": row["category"] or "",
                "description": (row["description"] or "")[:160]
            }
            for row in rows
        ]
    except Exception as exc:
        print(f"[AI_ENRICH_CONTEXT_ERROR] {repr(exc)}")
    finally:
        con.close()

    user_content = [
        {
            "type": "text",
            "text": json.dumps({
                "title": title,
                "detected_product_type": detected_type,
                "price": price,
                "currency": currency,
                "type": item_type,
                "product_hint": product_hint,
                "image_url": image_url,
                "previous_catalog_context": previous_catalog_context,
                "semantic_search_note": "Future embeddings should use previous_catalog_context to compare similar products and categories before enrichment."
            }, ensure_ascii=False)
        }
    ]
    if image_url:
        user_content.append({"type": "image_url", "image_url": {"url": image_url}})
    if image_file and image_file.filename:
        image_bytes = image_file.read()
        image_ext = os.path.splitext(image_file.filename)[1].lower().lstrip(".") or "jpeg"
        if image_ext == "jpg":
            image_ext = "jpeg"
        import base64
        image_data = base64.b64encode(image_bytes).decode("ascii")
        user_content.append({"type": "image_url", "image_url": {"url": f"data:image/{image_ext};base64,{image_data}"}})

    model = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
    try:
        completion = ai_client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an elite ecommerce AI. Analyze products intelligently from the product name even when description or image is missing. "
                        "Understand the real-world meaning of the product name and detected product type. "
                        "Generate a professional description, SEO keywords, buyer intent, use cases, emotional marketing copy, product category, and WhatsApp pitch. "
                        "Do not hallucinate random categories; when uncertain, use broad safe categories and keep confidence moderate. "
                        "Return only valid JSON with keys: category, type, description, keywords, sales_pitch, use_cases, confidence, needs_more_info, question. "
                        "confidence must be a number from 0 to 1 based on evidence quality. "
                        "needs_more_info should be false when the product name provides enough real-world signal. "
                        "If an image is provided, use it only for visible product type, category, broad description, and keywords. Do not invent brand claims, materials, specs, size, ingredients, or benefits not visible/provided. "
                        "type must be product or service. "
                        "Write customer-facing Arabic by default unless the product name/hint is clearly in another language. "
                        "keywords must be comma-separated search terms."
                    )
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        raw_content = completion.choices[0].message.content or "{}"
        enriched = json.loads(raw_content)
    except Exception as exc:
        print(f"[AI_ENRICH_ERROR] {repr(exc)}")
        return jsonify({"error": "تعذر توليد بيانات المنتج بالذكاء الاصطناعي."}), 500

    try:
        confidence = float(enriched.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    needs_more_info = bool(enriched.get("needs_more_info")) and confidence < 0.35

    return jsonify({
        "category": str(enriched.get("category") or ""),
        "type": str(enriched.get("type") or "product"),
        "description": str(enriched.get("description") or ""),
        "keywords": str(enriched.get("keywords") or ""),
        "sales_pitch": str(enriched.get("sales_pitch") or ""),
        "use_cases": str(enriched.get("use_cases") or ""),
        "confidence": confidence,
        "needs_more_info": needs_more_info,
        "question": str(enriched.get("question") or "")
    })


@app.route("/admin/catalog/new", methods=["GET", "POST"])
def admin_catalog_new():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    cid = _session_client_id()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        item_type = request.form.get("type", "service").strip()
        price = request.form.get("price", "0").strip()
        currency = request.form.get("currency", "").strip()
        category = request.form.get("category", "").strip()
        description = request.form.get("description", "").strip()
        aliases_text = request.form.get("aliases", "").strip()
        product_hint = request.form.get("product_hint", "").strip()
        image_url = request.form.get("image_url", "").strip()
        uploaded_image_url = save_catalog_image(request.files.get("image_file"))
        if uploaded_image_url:
            image_url = uploaded_image_url

        if not title:
            flash("اسم العنصر مطلوب", "error")
            return redirect(url_for("admin_catalog_new"))

        try:
            price = float(price or 0)
        except:
            price = 0

        intelligence, ai_error = analyze_catalog_product_with_ai(
            title=title,
            description=description,
            image_url=image_url,
            price=price,
            category=category,
            keywords=aliases_text,
            item_type=item_type,
            product_hint=product_hint
        )
        if ai_error:
            print(f"[AI_PRODUCT_ANALYZER_SKIP] reason={ai_error}")

        con = get_db_connection()
        try:
            cur = con.execute("""
                INSERT INTO catalogs
                (client_id, title, type, price, currency, category, description, image_url, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (cid, title, item_type, price, currency, category, description, image_url))
            save_catalog_aliases(con, cur.lastrowid, aliases_text)
            save_catalog_ai_intelligence(con, cur.lastrowid, intelligence)
            con.commit()
        finally:
            con.close()

        flash("تمت الإضافة بنجاح", "success")
        return redirect(url_for("admin_catalog"))

    return render_template("admin/catalog_form.html", item=None, aliases_str="")


@app.route("/admin/catalog/<int:item_id>/edit", methods=["GET", "POST"])
def admin_catalog_edit(item_id):
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    cid = _session_client_id()
    con = get_db_connection()
    try:
        item = con.execute("""
            SELECT *
            FROM catalogs
            WHERE id=? AND client_id=?
            LIMIT 1
        """, (item_id, cid)).fetchone()

        if not item:
            flash("Product not found.", "error")
            return redirect(url_for("admin_catalog"))

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            item_type = request.form.get("type", "service").strip()
            price = request.form.get("price", "0").strip()
            sale_price = request.form.get("sale_price", "").strip()
            currency = request.form.get("currency", "").strip()
            category = request.form.get("category", "").strip()
            description = request.form.get("description", "").strip()
            aliases_text = request.form.get("aliases", "").strip()
            product_hint = request.form.get("product_hint", "").strip()
            image_url = request.form.get("image_url", "").strip()
            uploaded_image_url = save_catalog_image(request.files.get("image_file"))
            if uploaded_image_url:
                image_url = uploaded_image_url
            duration_min = request.form.get("duration_min", "").strip()
            stock_qty = request.form.get("stock_qty", "").strip()

            try:
                price = float(price or 0)
            except ValueError:
                price = 0

            try:
                sale_price = float(sale_price) if sale_price else None
            except ValueError:
                sale_price = None

            try:
                duration_min = int(duration_min) if duration_min else None
            except ValueError:
                duration_min = None

            try:
                stock_qty = int(stock_qty) if stock_qty else None
            except ValueError:
                stock_qty = None

            intelligence, ai_error = analyze_catalog_product_with_ai(
                title=title,
                description=description,
                image_url=image_url,
                price=price,
                category=category,
                keywords=aliases_text,
                item_type=item_type,
                product_hint=product_hint
            )
            if ai_error:
                print(f"[AI_PRODUCT_ANALYZER_SKIP] reason={ai_error}")

            con.execute("""
                UPDATE catalogs
                SET title=?,
                    type=?,
                    price=?,
                    sale_price=?,
                    currency=?,
                    category=?,
                    description=?,
                    image_url=?,
                    duration_min=?,
                    stock_qty=?
                WHERE id=? AND client_id=?
            """, (
                title,
                item_type,
                price,
                sale_price,
                currency,
                category,
                description,
                image_url,
                duration_min,
                stock_qty,
                item_id,
                cid
            ))
            save_catalog_aliases(con, item_id, aliases_text)
            save_catalog_ai_intelligence(con, item_id, intelligence)
            con.commit()
            flash("تم تحديث المنتج بنجاح.", "success")
            return redirect(url_for("admin_catalog"))
        alias_rows = con.execute(
            "SELECT alias FROM catalog_aliases WHERE catalog_id=? ORDER BY id",
            (item_id,)
        ).fetchall()
        aliases_str = ", ".join(row["alias"] for row in alias_rows)
    finally:
        con.close()

    return render_template("admin/catalog_form.html", item=item, aliases_str=aliases_str)


@app.route("/admin/catalog/<int:item_id>/delete", methods=["POST"])
def admin_catalog_delete(item_id):
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    cid = _session_client_id()

    con = get_db_connection()
    try:
        con.execute("""
            UPDATE catalogs
            SET is_active=0
            WHERE id=? AND client_id=?
        """, (item_id, cid))
        con.commit()
    finally:
        con.close()

    flash("تم تعطيل العنصر.", "success")
    return redirect(url_for("admin_catalog"))


@app.route("/admin/catalog/<int:item_id>/toggle", methods=["POST"])
def admin_catalog_toggle(item_id):
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    cid = _session_client_id()

    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT is_active
            FROM catalogs
            WHERE id=? AND client_id=?
        """, (item_id, cid)).fetchone()

        if row:
            new_status = 0 if row["is_active"] else 1
            con.execute("""
                UPDATE catalogs
                SET is_active=?
                WHERE id=? AND client_id=?
            """, (new_status, item_id, cid))
            con.commit()
    finally:
        con.close()

    return redirect(url_for("admin_catalog"))

@app.route("/admin/settings", methods=["GET", "POST"])
def admin_settings():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients
                SET name=?, business_type=?, default_language=?, currency=?,
                    timezone=?, admin_whatsapp=?
                WHERE id=?
            """, (
                request.form.get("name", "").strip(),
                request.form.get("business_type", "").strip(),
                request.form.get("default_language", "en").strip(),
                request.form.get("currency", "").strip(),
                request.form.get("timezone", "").strip(),
                request.form.get("admin_whatsapp", "").strip(),
                cid,
            ))
            con.commit()
        finally:
            con.close()
        flash("Settings saved.", "success")
        return redirect(url_for("admin_settings"))
    client = get_client(cid)
    return render_template("admin/settings.html", client=client, active="settings",
                           trial_info=get_trial_status(client))


@app.route("/settings")
def settings_alias():
    return redirect(url_for("admin_settings"))


@app.route("/admin/orders")
def admin_orders():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    f_status = request.args.get("status", "").strip()
    f_flow = request.args.get("flow_type", "").strip()
    f_date = request.args.get("date", "").strip()
    f_q = request.args.get("q", "").strip()
    where = ["client_id=?"]
    vals = [cid]
    if f_status:
        where.append("status=?")
        vals.append(f_status)
    if f_flow:
        where.append("COALESCE(intent, 'booking')=?")
        vals.append(f_flow)
    if f_date == "today":
        where.append("created_at LIKE ?")
        vals.append(datetime.datetime.now().strftime("%Y-%m-%d") + "%")
    elif f_date == "this_week":
        where.append("created_at >= ?")
        vals.append((datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(timespec="seconds"))
    if f_q:
        where.append("(COALESCE(name, '') LIKE ? OR COALESCE(phone, '') LIKE ? OR COALESCE(customer_phone, '') LIKE ?)")
        vals.extend([f"%{f_q}%", f"%{f_q}%", f"%{f_q}%"])
    con = get_db_connection()
    try:
        rows = [dict(r) for r in con.execute(f"""
            SELECT * FROM orders
            WHERE {" AND ".join(where)}
            ORDER BY id DESC
        """, vals).fetchall()]
    finally:
        con.close()
    client = get_client(cid)
    currency = client.get("currency") or "MAD"
    orders = []
    for row in rows:
        row["customer_name"] = row.get("name") or ""
        row["flow_type"] = row.get("intent") or "booking"
        row["items_parsed"] = []
        row["items_rich"] = []
        try:
            parsed = json.loads(row.get("items") or "[]")
        except Exception:
            parsed = []
        for item in parsed if isinstance(parsed, list) else []:
            if isinstance(item, dict):
                row["items_rich"].append({
                    "title": item.get("title") or item.get("name") or "",
                    "price": float(item.get("price") or 0),
                    "currency": currency,
                })
            elif item:
                row["items_parsed"].append(str(item))
        row["total_display"] = row.get("amount") or 0
        row["day"] = row.get("appointment_day") or ""
        row["time"] = row.get("appointment_time") or ""
        orders.append(row)
    return render_template("admin/orders.html", orders=orders, f_status=f_status,
                           f_flow=f_flow, f_date=f_date, f_q=f_q, active="orders",
                           client=client, trial_info=get_trial_status(client))


@app.route("/orders")
def orders_alias():
    return redirect(url_for("admin_orders"))


@app.route("/admin/orders/<int:order_id>/status", methods=["POST"])
def admin_order_status(order_id):
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    status = request.form.get("status", "new").strip()
    if status not in {"new", "confirmed", "done", "cancelled"}:
        status = "new"
    con = get_db_connection()
    try:
        con.execute("UPDATE orders SET status=? WHERE id=? AND client_id=?",
                    (status, order_id, _session_client_id()))
        con.commit()
    finally:
        con.close()
    return redirect(url_for("admin_orders"))


@app.route("/admin/billing")
def admin_billing():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    con = get_db_connection()
    try:
        catalog_count = con.execute("SELECT COUNT(*) FROM catalogs WHERE client_id=?", (cid,)).fetchone()[0]
        plans = [dict(r) for r in con.execute("""
            SELECT * FROM subscription_plans WHERE is_active=1 ORDER BY price_monthly ASC
        """).fetchall()]
    finally:
        con.close()
    for plan in plans:
        try:
            plan["features"] = json.loads(plan.get("features_json") or "[]")
        except Exception:
            plan["features"] = []
    client = get_client(cid)
    return render_template("admin/billing.html", sub=get_client_subscription(cid),
                           all_plans=plans, catalog_count=catalog_count,
                           active="billing", client=client,
                           trial_info=get_trial_status(client))


@app.route("/admin/upgrade-click")
def admin_upgrade_click():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    track_event(_session_client_id(), "upgrade_clicked", {"from": request.args.get("from", "")})
    return redirect(url_for("admin_billing"))


@app.route("/admin/invoices")
def admin_invoices():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    client = get_client(_session_client_id())
    return render_template_string("""
{% extends "admin/layout.html" %}
{% block title %}Invoices{% endblock %}
{% block content %}
<div class="page-head">
  <div>
    <div class="page-title">Invoices</div>
    <div class="page-sub">Invoice management will be available here soon.</div>
  </div>
</div>
<div class="card">
  <div class="empty">
    <div class="empty-icon">🧾</div>
    <div class="empty-text">No invoices yet</div>
    <div class="empty-sub">Invoices and payment records will appear here once billing is configured.</div>
  </div>
</div>
{% endblock %}
""", client=client, active="billing", trial_info=get_trial_status(client))


@app.route("/invoices")
def invoices_alias():
    return redirect(url_for("admin_invoices"))


@app.route("/admin/brand")
def admin_brand():
    return redirect(url_for("admin_branding"))


@app.route("/admin/branding", methods=["GET", "POST"])
def admin_branding():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        logo_url = request.form.get("logo_url_field", "").strip()
        logo_file = request.files.get("logo_file")
        if logo_file and logo_file.filename:
            filename = secure_filename(logo_file.filename)
            upload_dir = os.path.join(app.root_path, "static", "uploads")
            os.makedirs(upload_dir, exist_ok=True)
            logo_file.save(os.path.join(upload_dir, filename))
            logo_url = url_for("static", filename=f"uploads/{filename}")
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients
                SET white_label_enabled=?, brand_name=?, primary_color=?,
                    custom_domain=?, logo_url=?
                WHERE id=?
            """, (
                1 if request.form.get("white_label_enabled") else 0,
                request.form.get("brand_name", "").strip(),
                request.form.get("primary_color", "#4f46e5").strip() or "#4f46e5",
                request.form.get("custom_domain", "").strip().lower(),
                logo_url,
                cid,
            ))
            con.commit()
        finally:
            con.close()
        flash("Branding saved.", "success")
        return redirect(url_for("admin_branding"))
    client = get_client(cid)
    return render_template("admin/branding.html", client=client, error=None,
                           active="branding", trial_info=get_trial_status(client))


@app.route("/ai")
def ai_alias():
    return redirect(url_for("admin_ai_brain"))


@app.route("/admin/ai-brain", methods=["GET", "POST"])
def admin_ai_brain():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients
                SET name=?, business_type=?, default_language=?, assistant_tone=?,
                    assistant_goal=?, business_description=?, policies=?, fallback_message=?
                WHERE id=?
            """, (
                request.form.get("name", "").strip(),
                request.form.get("business_type", "").strip(),
                request.form.get("default_language", "ar").strip(),
                request.form.get("assistant_tone", "friendly").strip(),
                request.form.get("assistant_goal", "book_appointments").strip(),
                request.form.get("business_description", "").strip(),
                request.form.get("policies", "").strip(),
                request.form.get("fallback_message", "").strip(),
                cid,
            ))
            con.commit()
        finally:
            con.close()
        flash("AI Brain settings saved.", "success")
        return redirect(url_for("admin_ai_brain"))
    client = get_client(cid)
    return render_template("admin/ai-brain.html", client=client, active="ai-brain",
                           trial_info=get_trial_status(client))


def _get_integration_config(client_id, provider):
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT config_json FROM client_integrations
            WHERE client_id=? AND provider=? AND is_active=1
            ORDER BY id DESC LIMIT 1
        """, (client_id, provider)).fetchone()
    finally:
        con.close()
    if not row:
        return {}
    try:
        return json.loads(row["config_json"] or "{}")
    except Exception:
        return {}


def _save_integration_config(client_id, provider, config):
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT id FROM client_integrations WHERE client_id=? AND provider=?
            ORDER BY id DESC LIMIT 1
        """, (client_id, provider)).fetchone()
        if row:
            con.execute("""
                UPDATE client_integrations
                SET config_json=?, is_active=1, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
            """, (json.dumps(config), row["id"]))
        else:
            con.execute("""
                INSERT INTO client_integrations (client_id, provider, config_json, is_active)
                VALUES (?, ?, ?, 1)
            """, (client_id, provider, json.dumps(config)))
        con.commit()
    finally:
        con.close()


@app.route("/admin/integrations")
def admin_integrations():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    client = get_client(cid)
    return render_template("admin/integrations.html",
                           shopify_cfg=_get_integration_config(cid, "shopify"),
                           stripe_cfg=_get_integration_config(cid, "stripe"),
                           client=client, active="integrations",
                           trial_info=get_trial_status(client))


@app.route("/admin/api-keys", methods=["GET", "POST"])
def admin_api_keys():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        con = get_db_connection()
        try:
            action = request.form.get("action")
            if action == "generate":
                key = "fax_" + _secrets.token_urlsafe(32)
                label = request.form.get("label", "Default").strip() or "Default"
                con.execute("INSERT INTO api_keys (client_id, api_key, label) VALUES (?, ?, ?)", (cid, key, label))
                flash(f"API key generated: {key}", "success")
            elif action == "revoke":
                con.execute("UPDATE api_keys SET is_active=0 WHERE id=? AND client_id=?",
                            (request.form.get("key_id"), cid))
                flash("API key revoked.", "success")
            con.commit()
        finally:
            con.close()
        return redirect(url_for("admin_api_keys"))
    con = get_db_connection()
    try:
        keys = [dict(r) for r in con.execute("SELECT * FROM api_keys WHERE client_id=? ORDER BY id DESC", (cid,)).fetchall()]
    finally:
        con.close()
    client = get_client(cid)
    return render_template("admin/api_keys.html", keys=keys, client=client,
                           active="integrations", trial_info=get_trial_status(client))


@app.route("/admin/webhooks", methods=["GET", "POST"])
def admin_webhooks():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        con = get_db_connection()
        try:
            action = request.form.get("action", "create")
            if action in {"create", "add"}:
                con.execute("""
                    INSERT INTO webhooks (client_id, url, event_type, is_active)
                    VALUES (?, ?, ?, 1)
                """, (cid, request.form.get("url", "").strip(), request.form.get("event_type", "order.created").strip()))
                flash("Webhook saved.", "success")
            elif action in {"delete", "disable"}:
                con.execute("UPDATE webhooks SET is_active=0 WHERE id=? AND client_id=?",
                            (request.form.get("webhook_id"), cid))
                flash("Webhook disabled.", "success")
            con.commit()
        finally:
            con.close()
        return redirect(url_for("admin_webhooks"))
    con = get_db_connection()
    try:
        webhooks = [dict(r) for r in con.execute("SELECT * FROM webhooks WHERE client_id=? ORDER BY id DESC", (cid,)).fetchall()]
    finally:
        con.close()
    client = get_client(cid)
    return render_template("admin/webhooks.html", webhooks=webhooks, client=client,
                           active="integrations", trial_info=get_trial_status(client))


@app.route("/admin/integrations/shopify", methods=["GET", "POST"])
def admin_integration_shopify():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        _save_integration_config(cid, "shopify", {
            "shop_domain": request.form.get("shop_domain", "").strip(),
            "access_token": request.form.get("access_token", "").strip(),
        })
        flash("Shopify integration saved.", "success")
        return redirect(url_for("admin_integration_shopify"))
    client = get_client(cid)
    return render_template("admin/integration_shopify.html",
                           cfg=_get_integration_config(cid, "shopify"),
                           error=None, client=client, active="integrations",
                           trial_info=get_trial_status(client))


@app.route("/admin/integrations/stripe", methods=["GET", "POST"])
def admin_integration_stripe():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = _session_client_id()
    if request.method == "POST":
        _save_integration_config(cid, "stripe", {
            "secret_key": request.form.get("secret_key", "").strip(),
            "publishable_key": request.form.get("publishable_key", "").strip(),
        })
        flash("Stripe integration saved.", "success")
        return redirect(url_for("admin_integration_stripe"))
    client = get_client(cid)
    return render_template("admin/integration_stripe.html",
                           cfg=_get_integration_config(cid, "stripe"),
                           error=None, client=client, active="integrations",
                           trial_info=get_trial_status(client))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=5000, debug=debug)



