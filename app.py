from flask import Flask, request, jsonify, render_template, session, redirect, url_for, flash, g
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import os
import json
import sqlite3
import datetime
import random

ULTRAMSG_INSTANCE         = os.getenv("ULTRAMSG_INSTANCE", "")
ULTRAMSG_TOKEN            = os.getenv("ULTRAMSG_TOKEN", "")
ADMIN_WHATSAPP_NUMBER     = os.getenv("ADMIN_WHATSAPP_NUMBER", "")
PLATFORM_ADMIN_WHATSAPP   = os.getenv("PLATFORM_ADMIN_WHATSAPP", "")
WA_BOT_NUMBER             = os.getenv("WA_BOT_NUMBER", "22230489495")   # UltraMsg bot phone number for deep links
print(f"[STARTUP] ADMIN_WHATSAPP_NUMBER={ADMIN_WHATSAPP_NUMBER!r}")
print(f"[STARTUP] PLATFORM_ADMIN_WHATSAPP={'set' if PLATFORM_ADMIN_WHATSAPP else 'not set'}")
print(f"[STARTUP] WA_BOT_NUMBER={'set' if WA_BOT_NUMBER else 'not set'}")

def ultramsg_send(to, text):
    import traceback as _tb
    if any(kw in (text or "") for kw in ("حجز جديد", "📦", "📥", "هل ترغب")):
        print("[TRACE_DUPLICATE_ADMIN_MESSAGE_SOURCE] ⚠️  suspicious text fired:")
        _tb.print_stack()
    url = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"
    payload = {"token": ULTRAMSG_TOKEN, "to": to, "body": text}
    print(f"[ULTRAMSG] sending to={to!r} body={text!r}")
    try:
        resp = requests.post(url, data=payload, timeout=10)
    except Exception as req_err:
        print(f"[ULTRAMSG_ERROR] request failed: {repr(req_err)}")
        return None
    print(f"[ULTRAMSG] response status={resp.status_code} body={resp.text!r}")
    body_lower = resp.text.lower()
    if "demo" in body_lower and "limit" in body_lower:
        print(
            "[ULTRAMSG_LIMIT] ⚠️  Demo daily sending limit exceeded. "
            "Message was NOT delivered. "
            "Either wait for the daily reset (midnight UTC) or upgrade your UltraMsg plan."
        )
    elif resp.status_code != 200 or '"sent"' not in resp.text.lower():
        print(f"[ULTRAMSG_WARN] message may not have been delivered — status={resp.status_code} body={resp.text!r}")
    return resp


def _ultramsg_send_with_creds(instance_id, token, to, text):
    """Low-level send using explicit instance_id + token (for per-client instances)."""
    url = f"https://api.ultramsg.com/{instance_id}/messages/chat"
    payload = {"token": token, "to": to, "body": text}
    print(f"[WA_SEND_CLIENT_INSTANCE] instance={instance_id!r} to={to!r}")
    try:
        resp = requests.post(url, data=payload, timeout=10)
        print(f"[WA_SEND_CLIENT_INSTANCE] status={resp.status_code} body={resp.text!r}")
        return resp
    except Exception as _e:
        print(f"[WA_SEND_CLIENT_INSTANCE_ERROR] {_e!r}")
        return None


def get_whatsapp_instance(client_id):
    """Return the client's whatsapp_instances row (sqlite3.Row) or None."""
    try:
        _con = sqlite3.connect(DB_FILE, timeout=10)
        _con.row_factory = sqlite3.Row
        row = _con.execute(
            "SELECT * FROM whatsapp_instances WHERE client_id=?", (client_id,)
        ).fetchone()
        _con.close()
        return row
    except Exception as _e:
        print(f"[GET_WA_INSTANCE_ERROR] client={client_id} {_e!r}")
        return None


def send_whatsapp_message(client_id, to, text):
    """Send a WhatsApp message using the client's own instance.
    Falls back to the platform's shared UltraMsg instance if none is configured."""
    inst = get_whatsapp_instance(client_id)
    if inst and inst["instance_id"] and inst["token"] and inst["status"] == "connected":
        print(f"[WA_SEND_CLIENT_INSTANCE] client={client_id} using own instance={inst['instance_id']!r}")
        return _ultramsg_send_with_creds(inst["instance_id"], inst["token"], to, text)
    print(f"[WA_SEND_PLATFORM_FALLBACK] client={client_id} no personal instance → using platform")
    return ultramsg_send(to, text)


def notify_platform_admin_connect_request(client_id, phone):
    """Notify the platform admin when a client submits a WhatsApp connection request.
    Reads PLATFORM_ADMIN_WHATSAPP fresh from env each call (picks up Secrets added after startup).
    Never raises — all failures are logged so the client request is never broken."""
    print("[ADMIN_CONNECT_NOTIFY] START")

    admin_number = os.getenv("PLATFORM_ADMIN_WHATSAPP", "").strip()
    print(f"[ADMIN_CONNECT_NOTIFY] admin_number={admin_number!r}")

    if not admin_number:
        print("[ADMIN_CONNECT_NOTIFY] SKIPPED — PLATFORM_ADMIN_WHATSAPP not set")
        return False

    # get_client and normalize_number are defined later in the file but resolved at call time
    try:
        client      = get_client(client_id)
        client_name = client.get("name") or f"Client #{client_id}"
    except Exception as _ce:
        client_name = f"Client #{client_id}"
        print(f"[ADMIN_CONNECT_NOTIFY] could not fetch client name: {repr(_ce)}")

    msg = (
        "🔔 طلب ربط واتساب جديد\n\n"
        f"العميل: {client_name}\n"
        f"الرقم: {phone}\n"
        "الحالة: pending\n\n"
        "ادخل للوحة التحكم لإكمال الربط."
    )
    print(f"[ADMIN_CONNECT_NOTIFY] message={msg!r}")

    try:
        to   = normalize_number(admin_number)
        resp = ultramsg_send(to, msg)
        status = resp.status_code if resp else "N/A"
        body   = resp.text        if resp else ""
        print(f"[ADMIN_CONNECT_NOTIFY] resp={status} body={body!r}")
        if resp and resp.status_code == 200:
            print("[WHATSAPP_ADMIN_NOTIFIED] alert delivered successfully")
            return True
        print("[ADMIN_CONNECT_NOTIFY] delivery uncertain — check UltraMsg logs above")
        return False
    except Exception as _exc:
        print(f"[ADMIN_CONNECT_NOTIFY_ERROR] {repr(_exc)}")
        return False


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

# ── White-label: resolve branding once per request ────────────────────────────
_SKIP_BRANDING_PREFIXES = ("/static/", "/webhook")

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
        # Nav
        "nav_dashboard":    "Dashboard",
        "nav_catalog":      "Catalog",
        "nav_orders":       "Orders",
        "nav_whatsapp":     "WhatsApp",
        "nav_billing":      "Billing",
        "nav_branding":     "Branding",
        "nav_integrations": "Integrations",
        "nav_settings":     "Settings",
        "nav_logout":       "Logout",
        # Dashboard
        "dashboard_title":        "Dashboard",
        "plan_limit_reached":     "You have reached your current plan limit. Please upgrade to continue.",
        "plan_approaching_limit": "⚠️ You're approaching your plan limit — upgrade to avoid interruptions.",
        "upgrade_plan":           "Upgrade Plan",
        "manage_plan":            "Manage Plan",
        "stat_total_orders":      "Total Orders",
        "stat_today":             "Today",
        "stat_catalog":           "Catalog Items",
        "stat_convos":            "Active Conversations",
        "stat_whatsapp":          "WhatsApp",
        "wa_connected":           "Connected",
        "wa_disconnected":        "Not Connected",
        "invite_earn":            "Invite & Earn",
        "invite_desc":            "Invite colleagues and earn +1000 bonus messages for every 3 referrals.",
        "invite_share_link":      "Share your link to get started.",
        "invite_referred":        "You've referred",
        "invite_referred_suffix": "client(s) so far.",
        "referrals_label":        "referral(s)",
        "more_to_reward":         "more to reward",
        "affiliate_title":        "Affiliate Earnings",
        "affiliate_desc":         "Share your affiliate link. Earn {rate}% commission on every paid subscription.",
        "affiliate_earned":       "Total Earned",
        "affiliate_referrals":    "Paid Referrals",
        "affiliate_copy":         "Copy Link",
        "affiliate_copied":       "Copied!",
        "affiliate_no_earnings":  "No earnings yet — share your link to start earning.",
        "copy":                   "Copy",
        "copied":                 "Copied!",
        "recent_orders":          "Recent Orders",
        "view_all":               "View all",
        "col_date":               "Date",
        "col_name":               "Name",
        "col_items":              "Items",
        "col_scheduled":          "Scheduled",
        "col_status":             "Status",
        "no_orders":              "No orders yet",
        "no_orders_sub":          "Orders will appear here once customers start booking",
        # Connect WhatsApp page
        "wa_page_title":          "Connect WhatsApp",
        "wa_page_sub":            "Connect your WhatsApp in seconds. Instant activation.",
        "wa_status_connected":    "Connected ✅",
        "wa_status_connected_sub":"Your bot is live and receiving messages.",
        "wa_status_pending":      "⏳ Almost Connected",
        "wa_status_pending_sub":  "Send START to the bot — you'll be live instantly.",
        "wa_status_failed":       "Connection Failed",
        "wa_status_failed_sub":   "Something went wrong. Click Connect and try again.",
        "wa_status_not_conn":     "Not Connected",
        "wa_status_not_conn_sub": "Your bot is ready. Connect your number to go live.",
        "wa_form_title":          "Your WhatsApp Business Number",
        "wa_form_label":          "WhatsApp Number",
        "wa_form_placeholder":    "e.g. +966501234567",
        "wa_form_hint":           "Enter the full number with country code.",
        "wa_btn_connect":         "Connect WhatsApp",
        "wa_btn_update":          "Update Number",
        "wa_btn_disconnect":      "Disconnect",
        "wa_disconnect_confirm":  "Are you sure you want to disconnect?",
        "wa_settings_title":      "Connection Settings",
        "wa_settings_number":     "Registered number:",
        "wa_settings_change":     "To change your number, disconnect first.",
        "wa_pending_box":         "Send START from your WhatsApp to the bot number. Your connection activates instantly.",
        "wa_success_msg":         "Number received. Connecting now — send START to activate.",
        "wa_disconnect_msg":      "Disconnected. Your bot is paused.",
        "wa_error_number":        "Please enter a valid WhatsApp number.",
        "wa_connect_title":         "Connect WhatsApp",
        "wa_number_label":          "Your WhatsApp Number",
        "wa_number_hint":           "Enter the number clients will send messages to (include country code).",
        "wa_submit_request":        "إرسال طلب الربط",
        "wa_status_pending":        "⏳ جاري الربط",
        "wa_status_connected":      "✅ تم ربط واتساب بنجاح",
        "wa_status_not_connected":  "غير مربوط",
        "wa_pending_note":          "⏳ جاري الربط",
        "wa_connected_note":        "✅ تم ربط واتساب بنجاح",
        "wa_page_title":            "Connect WhatsApp",
        "wa_page_sub":              "Connect your WhatsApp in seconds. Instant activation.",
        "wa_status_connected_sub":  "Your bot is live and receiving messages.",
        "wa_status_pending_sub":    "Send START to the bot — you'll be live instantly.",
        "wa_status_not_conn":       "Not Connected",
        "wa_status_not_conn_sub":   "Your bot is ready. Connect your number to go live.",
        "wa_status_failed":         "Connection Failed",
        "wa_status_failed_sub":     "Something went wrong. Click Connect and try again.",
        "wa_btn_connect":           "إرسال طلب الربط",
        "wa_btn_update":            "Update Number",
        "wa_btn_disconnect":        "Disconnect",
        "wa_form_title":            "Your WhatsApp Number",
        "wa_form_label":            "WhatsApp Number",
        "wa_form_placeholder":      "+212600000000",
        "wa_form_hint":             "Include country code, e.g. +212600000000",
        # Onboarding (5-step wizard)
        "ob_welcome":            "Welcome to {brand} 🚀",
        "ob_subtitle":           "Let's set up your AI sales engine in 5 quick steps.",
        "ob_step0_label":        "Welcome",
        "ob_step1_label":        "Business Info",
        "ob_step2_label":        "AI Settings",
        "ob_step3_label":        "WhatsApp",
        "ob_step4_label":        "All Done!",
        "ob_step0_title":        "Welcome to Filtrex AI 🚀",
        "ob_step0_desc":         "Set up your AI sales bot in under 2 minutes.",
        "ob_step1_title":        "Your Business",
        "ob_step1_desc":         "Help us personalize your bot.",
        "ob_step2_title":        "AI Personality",
        "ob_step2_desc":         "Pick a tone and goal. Your bot adapts instantly.",
        "ob_step3_title":        "Connect WhatsApp",
        "ob_step3_desc":         "Connect WhatsApp and go live right now.",
        "ob_step4_title":        "You're live! 🎉",
        "ob_step4_desc":         "Your AI bot is active and taking orders. Let's go.",
        "ob_preview_title":      "Conversation Preview",
        "ob_preview_note":       "This is a preview of how your AI bot will talk to customers on WhatsApp.",
        "ob_customer_label":     "Customer",
        "ob_bot_label":          "Bot",
        "ob_already_conn":       "✅ Already connected",
        "ob_done":               "✓ Done",
        "ob_continue":           "Continue →",
        "ob_skip":               "Skip for now",
        "ob_connect_btn":        "Connect Now →",
        "ob_wa_not_conn":        "Skip for now — connect anytime from Settings.",
        "ob_go_dashboard":       "Go to Dashboard →",
        "ob_lbl_biz_name":       "Business Name",
        "ob_lbl_biz_type":       "Business Type",
        "ob_lbl_language":       "Default Language",
        "ob_lbl_currency":       "Currency",
        "ob_lbl_timezone":       "Timezone",
        "ob_lbl_tone":           "AI Tone",
        "ob_lbl_goal":           "Primary Goal",
        "ob_lbl_biz_desc":       "Brief Business Description",
        "ob_tone_friendly":      "Friendly & Warm",
        "ob_tone_professional":  "Professional",
        "ob_tone_casual":        "Casual & Fun",
        "ob_goal_book":          "Book Appointments",
        "ob_goal_sell":          "Sell Products",
        "ob_goal_support":       "Customer Support",
        "ob_goal_leads":         "Generate Leads",
        "ob_biz_desc_hint":      "e.g. Dental clinic offering cleaning, whitening, and check-ups.",
        "ob_get_started":        "Start Setup →",
        "ob_finish":             "Go to Dashboard →",
    },
    "ar": {
        # Nav
        "nav_dashboard":    "لوحة التحكم",
        "nav_catalog":      "الكتالوج",
        "nav_orders":       "الطلبات",
        "nav_whatsapp":     "واتساب",
        "nav_billing":      "الفواتير",
        "nav_branding":     "العلامة التجارية",
        "nav_integrations": "التكاملات",
        "nav_settings":     "الإعدادات",
        "nav_logout":       "تسجيل الخروج",
        # Dashboard
        "dashboard_title":        "لوحة التحكم",
        "plan_limit_reached":     "لقد وصلت إلى حد خطتك الحالية. يرجى الترقية للمتابعة.",
        "plan_approaching_limit": "⚠️ اقتربت من استهلاك باقتك — قم بالترقية لتفادي الانقطاع.",
        "upgrade_plan":           "ترقية الخطة",
        "manage_plan":            "إدارة الخطة",
        "stat_total_orders":      "إجمالي الطلبات",
        "stat_today":             "اليوم",
        "stat_catalog":           "عناصر الكتالوج",
        "stat_convos":            "محادثات نشطة",
        "stat_whatsapp":          "واتساب",
        "wa_connected":           "متصل",
        "wa_disconnected":        "غير متصل",
        "invite_earn":            "ادع واربح",
        "invite_desc":            "ادع زملاءك واحصل على +1000 رسالة مجانية لكل 3 دعوات.",
        "invite_share_link":      "شارك رابطك للبدء.",
        "invite_referred":        "لقد دعوت",
        "invite_referred_suffix": "عميل حتى الآن.",
        "referrals_label":        "دعوة",
        "more_to_reward":         "دعوة أخرى للمكافأة",
        "affiliate_title":        "أرباح الشركاء",
        "affiliate_desc":         "شارك رابطك واكسب {rate}٪ عمولة على كل اشتراك مدفوع.",
        "affiliate_earned":       "إجمالي الأرباح",
        "affiliate_referrals":    "إحالات مدفوعة",
        "affiliate_copy":         "نسخ الرابط",
        "affiliate_copied":       "تم النسخ!",
        "affiliate_no_earnings":  "لا أرباح بعد — شارك رابطك للبدء.",
        "copy":                   "نسخ",
        "copied":                 "تم النسخ!",
        "recent_orders":          "الطلبات الأخيرة",
        "view_all":               "عرض الكل",
        "col_date":               "التاريخ",
        "col_name":               "الاسم",
        "col_items":              "العناصر",
        "col_scheduled":          "المجدول",
        "col_status":             "الحالة",
        "no_orders":              "لا طلبات حتى الآن",
        "no_orders_sub":          "ستظهر الطلبات هنا عندما يبدأ العملاء بالحجز",
        # Connect WhatsApp page
        "wa_page_title":          "ربط واتساب",
        "wa_page_sub":            "اربط واتساب في ثوانٍ. تفعيل فوري.",
        "wa_status_connected":    "متصل ✅",
        "wa_status_connected_sub":"البوت يعمل ويستقبل الرسائل الآن.",
        "wa_status_pending":      "⏳ على وشك الاكتمال",
        "wa_status_pending_sub":  "أرسل START للبوت — سيبدأ عمله فوراً.",
        "wa_status_failed":       "فشل الاتصال",
        "wa_status_failed_sub":   "حدث خطأ. انقر على ربط وحاول مجدداً.",
        "wa_status_not_conn":     "غير متصل",
        "wa_status_not_conn_sub": "البوت جاهز. اربط رقمك للانطلاق.",
        "wa_form_title":          "رقم واتساب الخاص بنشاطك",
        "wa_form_label":          "رقم واتساب",
        "wa_form_placeholder":    "مثال: ‎+966501234567",
        "wa_form_hint":           "أدخل الرقم كاملاً مع رمز الدولة.",
        "wa_btn_connect":         "ربط واتساب",
        "wa_btn_update":          "تحديث الرقم",
        "wa_btn_disconnect":      "قطع الاتصال",
        "wa_disconnect_confirm":  "هل تريد قطع الاتصال؟",
        "wa_settings_title":      "إعدادات الاتصال",
        "wa_settings_number":     "الرقم المسجّل:",
        "wa_settings_change":     "لتغيير الرقم، قطع الاتصال أولاً.",
        "wa_pending_box":         "أرسل START من واتساب إلى رقم البوت. يتم التفعيل فوراً.",
        "wa_success_msg":         "تم استلام الرقم. اربط الآن — أرسل START للتفعيل.",
        "wa_disconnect_msg":      "تم قطع الاتصال. البوت متوقف مؤقتاً.",
        "wa_error_number":        "يرجى إدخال رقم واتساب صحيح.",
        "wa_connect_title":         "ربط واتساب",
        "wa_number_label":          "رقم واتساب الخاص بك",
        "wa_number_hint":           "أدخل الرقم الذي سيرسل إليه العملاء الرسائل (مع رمز الدولة).",
        "wa_submit_request":        "إرسال طلب الربط",
        "wa_status_pending":        "⏳ جاري الربط",
        "wa_status_connected":      "✅ تم ربط واتساب بنجاح",
        "wa_status_not_connected":  "غير مربوط",
        "wa_pending_note":          "⏳ جاري الربط",
        "wa_connected_note":        "✅ تم ربط واتساب بنجاح",
        "wa_page_title":            "ربط واتساب",
        "wa_page_sub":              "اربط واتساب في ثوانٍ. تفعيل فوري.",
        "wa_status_connected_sub":  "البوت يعمل ويستقبل الرسائل الآن.",
        "wa_status_pending_sub":    "أرسل START للبوت — سيبدأ عمله فوراً.",
        "wa_status_not_conn":       "غير متصل",
        "wa_status_not_conn_sub":   "البوت جاهز. اربط رقمك للانطلاق.",
        "wa_status_failed":         "فشل الاتصال",
        "wa_status_failed_sub":     "حدث خطأ. انقر على ربط وحاول مجدداً.",
        "wa_btn_connect":           "إرسال طلب الربط",
        "wa_btn_update":            "تحديث الرقم",
        "wa_btn_disconnect":        "قطع الاتصال",
        "wa_form_title":            "رقم واتساب الخاص بك",
        "wa_form_label":            "رقم واتساب",
        "wa_form_placeholder":      "+212600000000",
        "wa_form_hint":             "أدخل الرقم مع رمز الدولة، مثال: +212600000000",
        # Onboarding (5-step wizard)
        "ob_welcome":            "مرحباً بك في {brand} 🚀",
        "ob_subtitle":           "دعنا نُعدّ محرك مبيعاتك الذكي في 5 خطوات سريعة.",
        "ob_step0_label":        "الترحيب",
        "ob_step1_label":        "معلومات النشاط",
        "ob_step2_label":        "إعدادات الذكاء",
        "ob_step3_label":        "واتساب",
        "ob_step4_label":        "تمّ!",
        "ob_step0_title":        "مرحباً بك في Filtrex AI 🚀",
        "ob_step0_desc":         "أعِدّ بوت المبيعات الذكي في أقل من دقيقتين.",
        "ob_step1_title":        "نشاطك التجاري",
        "ob_step1_desc":         "ساعدنا في تخصيص البوت.",
        "ob_step2_title":        "شخصية الذكاء الاصطناعي",
        "ob_step2_desc":         "اختر الأسلوب والهدف. البوت يتكيّف فوراً.",
        "ob_step3_title":        "ربط واتساب",
        "ob_step3_desc":         "اربط واتساب وابدأ الآن.",
        "ob_step4_title":        "أنت الآن مباشر! 🎉",
        "ob_step4_desc":         "البوت يعمل ويستقبل الطلبات. انطلق!",
        "ob_preview_title":      "معاينة المحادثة",
        "ob_preview_note":       "هذه معاينة لطريقة تواصل البوت مع عملائك عبر واتساب.",
        "ob_customer_label":     "العميل",
        "ob_bot_label":          "البوت",
        "ob_already_conn":       "✅ مرتبط بالفعل",
        "ob_done":               "✓ تم",
        "ob_continue":           "متابعة ←",
        "ob_skip":               "تخطي الآن",
        "ob_connect_btn":        "ربط الآن ←",
        "ob_wa_not_conn":        "تخطَّ الآن — الربط متاح في أي وقت من الإعدادات.",
        "ob_go_dashboard":       "الذهاب للوحة التحكم ←",
        "ob_lbl_biz_name":       "اسم النشاط التجاري",
        "ob_lbl_biz_type":       "نوع النشاط",
        "ob_lbl_language":       "اللغة الافتراضية",
        "ob_lbl_currency":       "العملة",
        "ob_lbl_timezone":       "المنطقة الزمنية",
        "ob_lbl_tone":           "أسلوب الذكاء",
        "ob_lbl_goal":           "الهدف الرئيسي",
        "ob_lbl_biz_desc":       "وصف مختصر للنشاط",
        "ob_tone_friendly":      "ودود ودافئ",
        "ob_tone_professional":  "احترافي",
        "ob_tone_casual":        "غير رسمي ومرح",
        "ob_goal_book":          "حجز المواعيد",
        "ob_goal_sell":          "بيع المنتجات",
        "ob_goal_support":       "دعم العملاء",
        "ob_goal_leads":         "توليد العملاء المحتملين",
        "ob_biz_desc_hint":      "مثال: عيادة أسنان للتنظيف والتبييض.",
        "ob_get_started":        "ابدأ الإعداد ←",
        "ob_finish":             "الذهاب للوحة التحكم ←",
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
                business_type     TEXT NOT NULL DEFAULT 'clinic',
                default_language  TEXT DEFAULT 'ar',
                currency          TEXT DEFAULT 'SAR',
                timezone          TEXT DEFAULT 'Africa/Nouakchott',
                admin_whatsapp    TEXT,
                ultramsg_instance TEXT,
                ultramsg_token    TEXT,
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
                sale_price   REAL,
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
                current_step          TEXT DEFAULT 'service',
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
        # upsells: old schema had trigger_item_id/upsell_item_id/is_active
        _upsell_cols = [r[1] for r in con.execute("PRAGMA table_info(upsells)").fetchall()]
        if "source_catalog_id" not in _upsell_cols:
            con.execute("ALTER TABLE upsells ADD COLUMN source_catalog_id INTEGER DEFAULT 0")
            con.execute("ALTER TABLE upsells ADD COLUMN target_catalog_id INTEGER DEFAULT 0")
            con.execute("ALTER TABLE upsells ADD COLUMN priority INTEGER DEFAULT 1")
            if "trigger_item_id" in _upsell_cols:
                con.execute("UPDATE upsells SET source_catalog_id = trigger_item_id")
                con.execute("UPDATE upsells SET target_catalog_id = upsell_item_id")
            con.commit()
            print("[SAAS] migrated upsells → source_catalog_id/target_catalog_id/priority")

        # catalogs: add missing columns (category, currency, updated_at)
        _cat_cols = [r[1] for r in con.execute("PRAGMA table_info(catalogs)").fetchall()]
        if "category" not in _cat_cols:
            con.execute("ALTER TABLE catalogs ADD COLUMN category TEXT DEFAULT ''")
            con.commit()
            print("[SAAS] migrated catalogs → added category")
        if "currency" not in _cat_cols:
            con.execute("ALTER TABLE catalogs ADD COLUMN currency TEXT DEFAULT ''")
            con.commit()
            print("[SAAS] migrated catalogs → added currency")
        if "updated_at" not in _cat_cols:
            con.execute("ALTER TABLE catalogs ADD COLUMN updated_at TEXT DEFAULT ''")
            con.commit()
            print("[SAAS] migrated catalogs → added updated_at")

        # catalog_options: old schema had option_key/option_val
        _opt_cols = [r[1] for r in con.execute("PRAGMA table_info(catalog_options)").fetchall()]
        if "option_type" not in _opt_cols:
            con.execute("ALTER TABLE catalog_options ADD COLUMN option_type  TEXT DEFAULT ''")
            con.execute("ALTER TABLE catalog_options ADD COLUMN option_value TEXT DEFAULT ''")
            con.execute("ALTER TABLE catalog_options ADD COLUMN extra_price  REAL DEFAULT 0")
            con.commit()
            print("[SAAS] migrated catalog_options → option_type/option_value/extra_price")

        # catalog_aliases: old schema had (catalog_id, alias, lang) — add lang index if needed
        _alias_cols = [r[1] for r in con.execute("PRAGMA table_info(catalog_aliases)").fetchall()]
        if "lang" not in _alias_cols:
            con.execute("ALTER TABLE catalog_aliases ADD COLUMN lang TEXT DEFAULT 'ar'")
            con.commit()
            print("[SAAS] migrated catalog_aliases → added lang column")

        # clients: add whatsapp_connected + onboarding_step columns if missing
        _cli_cols = [r[1] for r in con.execute("PRAGMA table_info(clients)").fetchall()]
        if "whatsapp_connected" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_connected INTEGER DEFAULT 0")
            con.commit()
            print("[SAAS] migrated clients → added whatsapp_connected")
        if "onboarding_step" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN onboarding_step INTEGER DEFAULT 0")
            # Existing clients (id=1) have already been configured — mark them done (5-step wizard)
            con.execute("UPDATE clients SET onboarding_step=5 WHERE id=1")
            con.commit()
            print("[SAAS] migrated clients → added onboarding_step, existing client=1 marked done (step=5)")
        else:
            # Bump legacy step=3 clients (old 3-step flow) to step=5 (new 5-step flow)
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
            # Generate referral codes for existing clients
            _no_code = con.execute("SELECT id FROM clients WHERE referral_code IS NULL").fetchall()
            for _r in _no_code:
                _code = f"REF{_r['id']}{random.randint(1000, 9999)}"
                con.execute("UPDATE clients SET referral_code=? WHERE id=?", (_code, _r["id"]))
            if _no_code:
                con.commit()
            print(f"[REFERRAL_CREATED] migrated clients → referral columns, generated {len(_no_code)} code(s)")

        # clients: WhatsApp connection UX v2 — number + status + provider
        if "business_whatsapp_number" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN business_whatsapp_number TEXT")
            con.commit()
            print("[SAAS] migrated clients → added business_whatsapp_number")
        if "whatsapp_connection_status" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_connection_status TEXT DEFAULT 'not_connected'")
            # Migrate existing connected clients so they show as connected
            con.execute("""
                UPDATE clients
                SET whatsapp_connection_status = CASE
                    WHEN whatsapp_connected = 1 THEN 'connected'
                    ELSE 'not_connected'
                END
            """)
            con.commit()
            print("[SAAS] migrated clients → added whatsapp_connection_status, backfilled existing")
        if "whatsapp_provider" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN whatsapp_provider TEXT DEFAULT 'ultramsg'")
            con.commit()
            print("[SAAS] migrated clients → added whatsapp_provider")

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
            print("[TRIAL] migrated clients → added is_trial, trial_started_at, trial_ends_at")
        if "trial_reminder_day" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN trial_reminder_day  INTEGER DEFAULT 0")
            con.commit()
            print("[TRIAL] migrated clients → added trial_reminder_day")

        # ── conversations: add collected_data column ──────────────────────────
        _conv_cols = [r[1] for r in con.execute("PRAGMA table_info(conversations)").fetchall()]
        if "collected_data" not in _conv_cols:
            con.execute("ALTER TABLE conversations ADD COLUMN collected_data TEXT DEFAULT '{}'")
            con.commit()
            print("[FLOW] migrated conversations → added collected_data")

        # ── orders: add intent + customer_phone + payment columns ───────────────
        _ord_cols = [r[1] for r in con.execute("PRAGMA table_info(orders)").fetchall()]
        if "intent" not in _ord_cols:
            con.execute("ALTER TABLE orders ADD COLUMN intent          TEXT DEFAULT 'unknown'")
            con.execute("ALTER TABLE orders ADD COLUMN customer_phone  TEXT DEFAULT ''")
            con.commit()
            print("[INTENT] migrated orders → added intent, customer_phone")
        if "amount" not in _ord_cols:
            con.execute("ALTER TABLE orders ADD COLUMN amount           REAL DEFAULT 0")
            con.execute("ALTER TABLE orders ADD COLUMN payment_status   TEXT DEFAULT 'pending'")
            con.execute("ALTER TABLE orders ADD COLUMN payment_link     TEXT DEFAULT ''")
            con.execute("ALTER TABLE orders ADD COLUMN payment_provider TEXT DEFAULT 'paypal'")
            con.commit()
            print("[PAYMENT] migrated orders → added amount, payment_status, payment_link, payment_provider")

        # ── AI Brain columns ──────────────────────────────────────────────────
        if "assistant_tone" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN assistant_tone        TEXT DEFAULT 'friendly'")
            con.execute("ALTER TABLE clients ADD COLUMN assistant_goal        TEXT DEFAULT 'book_appointments'")
            con.execute("ALTER TABLE clients ADD COLUMN business_description  TEXT DEFAULT ''")
            con.execute("ALTER TABLE clients ADD COLUMN policies              TEXT DEFAULT ''")
            con.execute("ALTER TABLE clients ADD COLUMN fallback_message      TEXT DEFAULT ''")
            con.commit()
            print("[AI_BRAIN] migrated clients → assistant_tone, assistant_goal, business_description, policies, fallback_message")

        # users: add email + client_id columns for multi-tenant auth
        _usr_cols = [r[1] for r in con.execute("PRAGMA table_info(users)").fetchall()]
        if "affiliate_id" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN affiliate_id INTEGER")
            con.commit()
            print("[AFFILIATE] migrated users → added affiliate_id")
        if "email" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN email TEXT")
            con.commit()
            print("[SAAS] migrated users → added email")
        if "client_id" not in _usr_cols:
            con.execute("ALTER TABLE users ADD COLUMN client_id INTEGER")
            # link existing users to client 1 (the only client in single-tenant MVP)
            con.execute("UPDATE users SET client_id=1 WHERE client_id IS NULL")
            con.commit()
            print("[SAAS] migrated users → added client_id, linked existing users → 1")

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
            print("[REFERRAL_CREATED] migrated client_subscriptions → added bonus_messages")
        if "paypal_subscription_id" not in _sub_cols:
            con.execute("ALTER TABLE client_subscriptions ADD COLUMN paypal_subscription_id TEXT")
            con.commit()
            print("[BILLING] migrated client_subscriptions → added paypal_subscription_id")

        # clients: plan shortcut + raw subscription_id for quick lookups
        if "plan" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN plan TEXT DEFAULT 'free'")
            con.commit()
            print("[BILLING] migrated clients → added plan")
        if "subscription_id" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN subscription_id TEXT")
            con.commit()
            print("[BILLING] migrated clients → added subscription_id")
        if "subscription_status" not in _cli_cols:
            con.execute("ALTER TABLE clients ADD COLUMN subscription_status TEXT DEFAULT 'inactive'")
            # backfill: clients that already have a plan are active
            con.execute("""
                UPDATE clients
                SET subscription_status = 'active'
                WHERE plan IS NOT NULL AND plan != 'free' AND plan != ''
            """)
            con.commit()
            print("[BILLING] migrated clients → added subscription_status")

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

        # ── STEP 7i: whatsapp_instances — per-client WA credentials ───────
        con.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_instances (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id    INTEGER NOT NULL UNIQUE,
                provider     TEXT    DEFAULT 'ultramsg',
                instance_id  TEXT,
                token        TEXT,
                phone_number TEXT,
                status       TEXT    DEFAULT 'pending',
                qr_url       TEXT,
                created_at   TEXT    DEFAULT CURRENT_TIMESTAMP,
                updated_at   TEXT    DEFAULT CURRENT_TIMESTAMP
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
            print("[BILLING_PLAN] seeded 4 default plans: Free/Starter/Pro/Business")

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
                print(f"[BILLING_PLAN] assigned Free plan to {len(unsubscribed)} existing client(s)")

        # ── STEP 8: Seed demo client ──────────────────────────────────────
        exists = con.execute("SELECT id FROM clients WHERE id = 1").fetchone()
        if not exists:
            con.execute("""
                INSERT INTO clients (id, name, business_type, default_language,
                    currency, timezone, admin_whatsapp, is_active)
                VALUES (1, 'Veltrix Dental Clinic', 'clinic', 'ar',
                    'SAR', 'Africa/Nouakchott', ?, 1)
            """, (ADMIN_WHATSAPP_NUMBER,))
            con.commit()
            print("[SAAS] seeded client id=1")

        # ── STEP 8–9: Seed catalog items + multilingual aliases ───────────
        cat_count = con.execute("SELECT COUNT(*) FROM catalogs WHERE client_id=1").fetchone()[0]
        if cat_count == 0:
            _seed = [
                ("تنظيف أسنان", "service", 100, None,
                 "تنظيف احترافي للأسنان يزيل الجير واللويحات الجرثومية", 30, None),
                ("تبييض الأسنان", "service", 250, None,
                 "تبييض متقدم بتقنية LED لابتسامة أكثر إشراقاً", 60, None),
                ("فحص الأسنان", "service", 50, None,
                 "فحص شامل مع تقرير صحة الأسنان", 20, None),
            ]
            cat_ids = []
            for title, typ, price, sale, desc, dur, stock in _seed:
                cur = con.execute("""
                    INSERT INTO catalogs (client_id, title, type, price, sale_price,
                        description, duration_min, stock_qty)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                """, (title, typ, price, sale, desc, dur, stock))
                cat_ids.append(cur.lastrowid)
            con.commit()

            # STEP 9 — multilingual aliases per spec (catalog_id, lang, alias)
            _aliases = [
                (cat_ids[0], "ar", "تنظيف"),
                (cat_ids[0], "ar", "تنظيف أسنان"),
                (cat_ids[0], "en", "cleaning"),
                (cat_ids[0], "en", "teeth cleaning"),
                (cat_ids[0], "fr", "nettoyage"),
                (cat_ids[0], "fr", "nettoyage des dents"),

                (cat_ids[1], "ar", "تبييض"),
                (cat_ids[1], "ar", "تبييض أسنان"),
                (cat_ids[1], "ar", "تبييض الأسنان"),
                (cat_ids[1], "en", "whitening"),
                (cat_ids[1], "en", "teeth whitening"),
                (cat_ids[1], "fr", "blanchiment"),
                (cat_ids[1], "fr", "blanchiment des dents"),

                (cat_ids[2], "ar", "فحص"),
                (cat_ids[2], "ar", "فحص أسنان"),
                (cat_ids[2], "ar", "فحص الأسنان"),
                (cat_ids[2], "en", "checkup"),
                (cat_ids[2], "en", "dental checkup"),
                (cat_ids[2], "fr", "controle"),
                (cat_ids[2], "fr", "consultation"),
            ]
            con.executemany(
                "INSERT INTO catalog_aliases (catalog_id, lang, alias) VALUES (?, ?, ?)",
                _aliases,
            )

            # upsell: cleaning → whitening, checkup → cleaning
            con.executemany(
                "INSERT INTO upsells (client_id, source_catalog_id, target_catalog_id, priority) VALUES (?,?,?,?)",
                [
                    (1, cat_ids[0], cat_ids[1], 1),
                    (1, cat_ids[2], cat_ids[0], 1),
                ]
            )
            con.commit()
            print(f"[SAAS] seeded catalog ids={cat_ids} + aliases + upsells")

    finally:
        con.close()

_migrate_saas()

# ── SAAS HELPERS ──────────────────────────────────────────────────────────────

CLIENT_ID = 1   # WhatsApp webhook default; admin routes use _session_client_id()

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
    """
    Check whether client_id is within their plan limits.
    limit_type: 'messages' | 'catalog_items' | 'orders'
    Returns (allowed: bool, sub: dict|None)
    Logs [BILLING_LIMIT_CHECK] and [BILLING_BLOCKED].
    """
    sub = get_client_subscription(client_id)
    if not sub:
        # No subscription row → allow (should not happen after migration)
        print(f"[BILLING_LIMIT_CHECK] client={client_id} type={limit_type} NO_SUB → allowed")
        return True, None

    plan_name = sub.get("plan_name", "?")

    if limit_type == "messages":
        limit = sub.get("max_messages", 100) + sub.get("bonus_messages", 0)
        used  = sub.get("messages_used", 0)
    elif limit_type == "catalog_items":
        limit = sub.get("max_catalog_items", 5)
        # live count straight from DB (most accurate)
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

    # -1 means unlimited
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
# Single source of truth for feature gates and per-plan limits.
# None  = unlimited.  -1 in DB = also unlimited (handled by check_usage_limit).
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
    """
    Return the client's active plan name as a lowercase string
    (e.g. 'free', 'starter', 'pro', 'business').
    Reads from clients.plan; defaults to 'free'.
    Logs [PLAN_CHECK].
    """
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
    """
    Return True if the client's current plan includes 'feature'.
    feature: 'whatsapp_bot' | 'multilingual' | 'upsell' | 'analytics' | 'white_label'
    Logs [PLAN_CHECK] (via get_client_plan) and [FEATURE_BLOCKED] when denied.
    """
    plan    = get_client_plan(client_id)
    allowed = PLANS.get(plan, PLANS["free"])["features"].get(feature, False)
    if not allowed:
        print(f"[FEATURE_BLOCKED] client={client_id} plan={plan!r} feature={feature!r} → blocked")
    return allowed


def check_limit(client_id, limit_type):
    """
    Return (allowed: bool, sub: dict|None).
    limit_type: 'messages' | 'catalog_items' | 'orders'

    Checks the PLANS dict for unlimited overrides, then delegates to
    check_usage_limit() for live DB counter comparison.
    Logs [LIMIT_CHECK] and [LIMIT_EXCEEDED].
    """
    plan     = get_client_plan(client_id)
    plan_cfg = PLANS.get(plan, PLANS["free"])
    _key_map = {
        "messages":      "max_messages",
        "catalog_items": "max_catalog_items",
        "orders":        "max_orders",
    }
    static_limit = plan_cfg.get(_key_map.get(limit_type, ""), 0)

    # None = unlimited in PLANS config
    if static_limit is None:
        print(f"[LIMIT_CHECK] client={client_id} plan={plan!r} type={limit_type} → unlimited ✓")
        return True, None

    # Delegate to DB-aware checker (handles messages_used, orders_used, live catalog count)
    allowed, sub = check_usage_limit(client_id, limit_type)
    status = "allowed" if allowed else "EXCEEDED"
    print(f"[LIMIT_CHECK] client={client_id} plan={plan!r} type={limit_type} "
          f"static_limit={static_limit} → {status}")
    if not allowed:
        print(f"[LIMIT_EXCEEDED] client={client_id} plan={plan!r} "
              f"type={limit_type} limit={static_limit}")
    return allowed, sub


def check_plan_limit(client_id, limit_name):
    """
    Public alias for check_limit() using the user-facing limit names:
    'messages' | 'catalog_items' | 'orders'
    Returns (allowed: bool, sub: dict|None).
    """
    return check_limit(client_id, limit_name)


def increment_usage(client_id, usage_type):
    """
    Increment a usage counter for the client's active subscription.
    usage_type: 'messages_used' | 'orders_used'
    Logs [USAGE_INCREMENTED].
    """
    _billing_increment(client_id, usage_type)
    print(f"[USAGE_INCREMENTED] client={client_id} type={usage_type}")


def get_trial_status(client):
    """
    Return a dict describing the client's free-trial state.

    Keys:
      is_trial   bool  — client was ever on a trial
      active     bool  — trial is currently running
      expired    bool  — trial started but now over
      days/hours/minutes int — remaining time (0 when expired)
      warning    bool  — True when < 24 h remain
      ends_at    str   — ISO timestamp of trial end
    """
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
    warning = remaining < 86400   # < 24 h

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
    """
    Downgrade a client to the free plan if their trial has ended.
    Must be called before plan-limit checks at each enforcement point.
    Returns True if the trial was expired and the plan was downgraded.
    Logs [TRIAL_EXPIRED].
    """
    client = get_client(client_id)
    if not client.get("is_trial"):
        return False

    trial = get_trial_status(client)
    if not trial.get("expired"):
        return False

    # Downgrade: clear trial flag, reset plan to free
    con = get_db_connection()
    try:
        con.execute("""
            UPDATE clients
            SET    is_trial=0, plan='free', subscription_status='expired'
            WHERE  id=? AND is_trial=1
        """, (client_id,))
        # Also mark any pending/active trial subscription as cancelled
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


# ── Analytics event tracker ───────────────────────────────────────────────────
import json as _json_mod

def track_event(client_id, event_name, metadata=None):
    """Insert one row into analytics_events and log [EVENT_TRACKED].

    Args:
        client_id  : int or None (platform-level events like user_registered)
        event_name : str  e.g. 'order_created', 'trial_started'
        metadata   : dict|None  any extra key-value pairs (JSON-serialised)
    """
    _meta = _json_mod.dumps(metadata or {})
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
    """
    Central paywall handler — call when a plan limit is hit.
    Logs [PAYWALL_TRIGGERED] and returns a structured dict with
    bilingual messages and the upgrade URL.

    limit_type: 'messages' | 'catalog_items' | 'orders'
    """
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


_APP_DEFAULT_BRAND = {
    "brand_name":          "Filtrex AI",
    "logo_url":            None,
    "primary_color":       "#4f46e5",
    "white_label_enabled": 0,
}


def _default_branding():
    return dict(_APP_DEFAULT_BRAND)


def _build_branding(row):
    """Return a branding dict from a clients row."""
    brand = _default_branding()
    brand["brand_name"]          = row["brand_name"]  or brand["brand_name"]
    brand["logo_url"]            = row["logo_url"]     or None
    brand["primary_color"]       = row["primary_color"] or brand["primary_color"]
    brand["white_label_enabled"] = row["white_label_enabled"] or 0
    return brand


# ═══════════════════════════════════════════════════════════════
# API PLATFORM HELPERS
# ═══════════════════════════════════════════════════════════════

import secrets as _api_secrets
import threading as _threading


def _generate_api_key():
    """Return a crypto-random API key prefixed with 'fax_'."""
    return "fax_" + _api_secrets.token_hex(24)


def _api_guard():
    """
    Validate Authorization: Bearer <key> header.
    Returns (client_id, None) on success or (None, JSON error response) on failure.
    Logs [API_REQUEST].
    """
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None, (jsonify({"error": "Missing or invalid Authorization header. "
                               "Use: Authorization: Bearer <api_key>"}), 401)
    key = auth[7:].strip()
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT client_id FROM api_keys WHERE api_key=? AND is_active=1",
            (key,)
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None, (jsonify({"error": "Invalid or revoked API key."}), 401)
    cid = row["client_id"]
    print(f"[API_REQUEST] key=...{key[-6:]!r} client_id={cid} "
          f"method={request.method} path={request.path}")
    return cid, None


def fire_webhook(client_id, event_type, payload):
    """
    Fire all active webhooks for (client_id, event_type) asynchronously.
    Logs [WEBHOOK_SENT].
    """
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT url FROM webhooks WHERE client_id=? AND event_type=? AND is_active=1",
            (client_id, event_type)
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return

    data = {"event": event_type, "client_id": client_id, "data": payload}

    def _send(url, body):
        try:
            resp = requests.post(url, json=body, timeout=10,
                                 headers={"Content-Type": "application/json",
                                          "X-Filtrex-Event": event_type})
            print(f"[WEBHOOK_SENT] event={event_type!r} url={url!r} "
                  f"status={resp.status_code}")
        except Exception as exc:
            print(f"[WEBHOOK_SENT] event={event_type!r} url={url!r} error={exc!r}")

    for row in rows:
        _threading.Thread(target=_send, args=(row["url"], data), daemon=True).start()


def _get_integration(client_id, provider):
    """Return config dict for a provider or {} if not configured."""
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT config_json FROM client_integrations WHERE client_id=? AND provider=?",
            (client_id, provider)
        ).fetchone()
    finally:
        con.close()
    if not row:
        return {}
    try:
        return json.loads(row["config_json"] or "{}")
    except Exception:
        return {}


def _save_integration(client_id, provider, config):
    """Upsert integration config for a provider."""
    con = get_db_connection()
    try:
        existing = con.execute(
            "SELECT id FROM client_integrations WHERE client_id=? AND provider=?",
            (client_id, provider)
        ).fetchone()
        cfg_str = json.dumps(config)
        now = datetime.datetime.utcnow().isoformat()
        if existing:
            con.execute(
                "UPDATE client_integrations SET config_json=?, updated_at=? WHERE id=?",
                (cfg_str, now, existing["id"])
            )
        else:
            con.execute(
                "INSERT INTO client_integrations (client_id, provider, config_json, updated_at) VALUES (?,?,?,?)",
                (client_id, provider, cfg_str, now)
            )
        con.commit()
    finally:
        con.close()
    print(f"[INTEGRATION_TRIGGER] client={client_id} provider={provider!r} saved")


PLAN_PRICES = {
    "starter":  9.0,
    "pro":     29.0,
    "business": 79.0,
    "free":     0.0,
}


def _apply_affiliate_commission(affiliate_client_id, plan_name):
    """
    Credit commission to an affiliate when a referred user's subscription activates.
    commission = plan_price * affiliate_rate (default 20%)
    Logs [AFFILIATE_COMMISSION].
    """
    con = get_db_connection()
    try:
        aff = con.execute(
            "SELECT affiliate_enabled, affiliate_rate FROM clients WHERE id=?",
            (affiliate_client_id,)
        ).fetchone()
        if not aff or not aff["affiliate_enabled"]:
            return

        plan_price = PLAN_PRICES.get(plan_name, 0.0)
        if plan_price <= 0:
            return

        rate       = float(aff["affiliate_rate"] or 0.20)
        commission = round(plan_price * rate, 2)

        con.execute("""
            UPDATE clients
            SET    affiliate_earnings = affiliate_earnings + ?
            WHERE  id = ?
        """, (commission, affiliate_client_id))
        con.commit()
        print(
            f"[AFFILIATE_COMMISSION] affiliate={affiliate_client_id} "
            f"plan={plan_name!r} price=${plan_price} rate={rate:.0%} "
            f"commission=${commission}"
        )
    finally:
        con.close()


def _apply_referral_reward(referrer_id, new_count):
    """
    Grant 1 000 bonus messages for every 3 successful referrals.
    Logs [REFERRAL_REWARD_GRANTED].
    """
    if new_count > 0 and new_count % 3 == 0:
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE client_subscriptions
                SET    bonus_messages = bonus_messages + 1000
                WHERE  client_id = ? AND status = 'active'
            """, (referrer_id,))
            con.commit()
        finally:
            con.close()
        print(f"[REFERRAL_REWARD_GRANTED] client={referrer_id} referral_count={new_count} → +1000 bonus messages")


def _check_activation(client_id):
    """
    If client has ≥1 catalog item AND ≥1 order, ensure is_active=1.
    Advances onboarding_step to 3 (done) on first activation.
    Logs [ACTIVATION].
    """
    con = get_db_connection()
    try:
        cat_count = con.execute(
            "SELECT COUNT(*) FROM catalogs WHERE client_id=? AND is_active=1", (client_id,)
        ).fetchone()[0]
        ord_count = con.execute(
            "SELECT COUNT(*) FROM bookings_or_orders WHERE client_id=?", (client_id,)
        ).fetchone()[0]
        if cat_count >= 1 and ord_count >= 1:
            con.execute(
                "UPDATE clients SET is_active=1, onboarding_step=3 WHERE id=?",
                (client_id,)
            )
            con.commit()
            print(f"[ACTIVATION] client={client_id} activated — catalogs={cat_count} orders={ord_count}")
        else:
            print(f"[ACTIVATION] client={client_id} not yet active — catalogs={cat_count} orders={ord_count}")
    finally:
        con.close()


def _onboarding_complete(client):
    """Return True if client has finished (or skipped) all onboarding steps."""
    return int(client.get("onboarding_step") or 0) >= 5


def find_catalog_match(client_id, msg, lang="ar"):
    """
    STEP 10 — Matching engine:
    1. Query catalog items for this client filtered by lang
    2. For each item, fetch ALL its aliases (any lang) and check against msg
    3. Return first match as dict, or None
    """
    if not msg:
        return None
    text = (msg or "").lower()
    con = get_db_connection()
    try:
        # Get distinct catalog items that have at least one alias in the detected lang
        cur = con.execute("""
            SELECT DISTINCT c.id, c.title, c.price, c.sale_price, c.type,
                            c.description, c.duration_min, c.is_active, c.client_id
            FROM catalogs c
            JOIN catalog_aliases a ON a.catalog_id = c.id
            WHERE c.client_id = ? AND a.lang = ? AND c.is_active = 1
        """, (client_id, lang))
        rows = cur.fetchall()
        for row in rows:
            cid = row["id"]
            # Fetch ALL aliases for this catalog item (any lang) — longest first
            alias_rows = con.execute(
                "SELECT alias FROM catalog_aliases WHERE catalog_id = ? ORDER BY LENGTH(alias) DESC",
                (cid,)
            ).fetchall()
            aliases = [r["alias"].lower() for r in alias_rows]
            for alias in aliases:
                if alias and alias in text:
                    result = dict(row)
                    print(f"[CATALOG_MATCH] lang={lang!r} alias={alias!r} → id={cid} title={row['title']!r}")
                    return result
    finally:
        con.close()
    # Fallback: try without lang filter (catches cross-language messages)
    con = get_db_connection()
    try:
        all_rows = con.execute("""
            SELECT DISTINCT c.id, c.title, c.price, c.sale_price, c.type,
                            c.description, c.duration_min, c.is_active, c.client_id
            FROM catalogs c
            JOIN catalog_aliases a ON a.catalog_id = c.id
            WHERE c.client_id = ? AND c.is_active = 1
            ORDER BY LENGTH(a.alias) DESC
        """, (client_id,)).fetchall()
        for row in all_rows:
            alias_rows = con.execute(
                "SELECT alias FROM catalog_aliases WHERE catalog_id = ? ORDER BY LENGTH(alias) DESC",
                (row["id"],)
            ).fetchall()
            for ar in alias_rows:
                if ar["alias"].lower() in text:
                    print(f"[CATALOG_MATCH] fallback alias={ar['alias']!r} → id={row['id']}")
                    return dict(row)
    finally:
        con.close()
    print(f"[CATALOG_MATCH] no match for msg={msg!r} lang={lang!r}")
    return None

def get_catalog_item(catalog_id):
    con = get_db_connection()
    try:
        row = con.execute("SELECT * FROM catalogs WHERE id=?", (catalog_id,)).fetchone()
    finally:
        con.close()
    return dict(row) if row else {}

def get_catalog_items(client_id, ids):
    """Return list of active catalog row dicts for given IDs belonging to client_id."""
    if not ids:
        return []
    con = get_db_connection()
    try:
        placeholders = ",".join("?" * len(ids))
        rows = con.execute(
            f"SELECT * FROM catalogs WHERE id IN ({placeholders}) AND client_id=? AND is_active=1",
            (*ids, client_id)
        ).fetchall()
    finally:
        con.close()
    return [dict(r) for r in rows]

def calculate_total(client_id, ids):
    """Sum sale_price (if set and > 0) or price for each catalog item."""
    total = 0.0
    for item in get_catalog_items(client_id, ids):
        p = item.get("sale_price") or item.get("price") or 0
        total += float(p)
    return total

def determine_flow_type(items):
    """Return 'booking', 'order', or 'mixed' based on catalog item types.
    'service' items → booking flow (day/time/name required, slot check applies)
    'product' items → order flow (quantity/address/name required, no slot check)
    mixed → both sets of fields required"""
    if not items:
        return "booking"          # safe default
    types = {(it.get("type") or "service").lower() for it in items}
    if types <= {"service"}:
        return "booking"
    if types <= {"product"}:
        return "order"
    return "mixed"

def get_required_fields(flow_type, items=None):
    """Return ordered list of state keys required to complete the flow."""
    if flow_type == "booking":
        return ["known_name", "known_day", "known_time"]
    if flow_type == "order":
        return ["known_name", "quantity", "address"]
    if flow_type == "mixed":
        return ["known_name", "known_day", "known_time", "quantity", "address"]
    return ["known_name", "known_day", "known_time"]   # safe default

def get_missing_fields(state, required_fields):
    """Return list of required fields not yet present in state."""
    return [f for f in required_fields if not state.get(f)]

# Maps state field name → step name the bot advances to when asking for it
_FIELD_TO_STEP = {
    "known_day":  "day",
    "known_time": "time",
    "known_name": "name",
    "quantity":   "quantity",
    "address":    "address",
}

def get_upsell_for_item(client_id, catalog_id):
    """Return upsell catalog row dict using spec columns (source/target), or None."""
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT c.*
            FROM upsells u
            JOIN catalogs c ON c.id = u.target_catalog_id
            WHERE u.client_id=? AND u.source_catalog_id=? AND c.is_active=1
            ORDER BY u.priority ASC
            LIMIT 1
        """, (client_id, catalog_id)).fetchone()
    finally:
        con.close()
    return dict(row) if row else None

def save_booking_or_order(client_id, phone, name, catalog_ids, day, time, total_price=0):
    """Write to bookings_or_orders (spec table) and legacy orders in one call."""
    # Resolve catalog titles from IDs
    items = []
    for cid in (catalog_ids or []):
        item = get_catalog_item(cid)
        if item:
            items.append(item.get("title", str(cid)))
    items_json = json.dumps(items, ensure_ascii=False)
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO bookings_or_orders
                (client_id, phone, customer_name, items_json, day, time, total_price, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmed')
        """, (client_id, phone, name, items_json, day, time, total_price))
        con.execute("""
            INSERT INTO orders (client_id, phone, name, items, scheduled, status)
            VALUES (?, ?, ?, ?, ?, 'confirmed')
        """, (client_id, phone, name, items_json, f"{day or ''} {time or ''}".strip()))
        con.commit()
    finally:
        con.close()
    print(f"[ORDER_SAVED] bookings_or_orders + orders client={client_id} name={name!r} items={items_json!r}")
    _wh_payload = {"name": name, "phone": phone, "items": items_json,
                   "scheduled": f"{day or ''} {time or ''}".strip(), "status": "confirmed"}
    fire_webhook(client_id, "order_created",   _wh_payload)
    fire_webhook(client_id, "booking_created", _wh_payload)

def save_order(client_id, phone, name, items, scheduled, status="confirmed"):
    items_json = json.dumps(items, ensure_ascii=False) if isinstance(items, list) else items
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO orders (client_id, phone, name, items, scheduled, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (client_id, phone, name, items_json, scheduled, status))
        con.commit()
    finally:
        con.close()
    print(f"[ORDER_SAVED] client={client_id} phone={phone!r} name={name!r} items={items_json!r}")
    fire_webhook(client_id, "order_created",
                 {"name": name, "phone": phone, "items": items_json,
                  "scheduled": scheduled, "status": status})

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
        return redirect(url_for("admin_dashboard"))
    return redirect(url_for("login"))

@app.route("/assistant")
def assistant():
    return render_template("index.html")

WHATSAPP_USER_ID = 1

def wa_load(phone):
    con = get_db_connection()
    try:
        row = con.execute(
            """SELECT known_service, known_day, known_time, known_name,
                      current_step, lang, upsell_offered, upsell_rejected, completed
               FROM whatsapp_state WHERE phone = ?""",
            (phone,)
        ).fetchone()
    finally:
        con.close()
    if row:
        _svc_raw = row["known_service"]
        if _svc_raw:
            try:
                _parsed = json.loads(_svc_raw)
                _svc_val = _parsed if isinstance(_parsed, list) else [_svc_raw]
            except Exception:
                _svc_val = [_svc_raw]
        else:
            _svc_val = []
        state = {
            "known_service":          _svc_val,
            "known_catalog_ids_json": "[]",     # loaded from conversations below
            "known_day":              row["known_day"],
            "known_time":             row["known_time"],
            "known_name":             row["known_name"],
            "current_step":           row["current_step"] or "service",
            "lang":                   row["lang"] or "",
            "upsell_offered":         bool(row["upsell_offered"]),
            "upsell_rejected":        bool(row["upsell_rejected"]),
            "completed":              bool(row["completed"]),
            "msg_intent":             row["msg_intent"] or "",
        }
    else:
        state = {
            "known_service": [], "known_catalog_ids_json": "[]",
            "known_day": None, "known_time": None, "known_name": None,
            "current_step": "service", "lang": "",
            "upsell_offered": False, "upsell_rejected": False,
            "completed": False, "msg_intent": "",
        }
    # ── Load known_catalog_ids_json from conversations table ──────────────
    con2 = get_db_connection()
    try:
        conv = con2.execute(
            "SELECT known_catalog_ids_json FROM conversations WHERE client_id=? AND phone=?",
            (CLIENT_ID, phone)
        ).fetchone()
        if conv and conv["known_catalog_ids_json"]:
            state["known_catalog_ids_json"] = conv["known_catalog_ids_json"]
    finally:
        con2.close()
    print(f"[STATE_LOAD] sender={phone} state={state}")
    return state

def wa_save(phone, state):
    print(f"[STATE_SAVE] sender={phone} state={state}")
    con = get_db_connection()
    try:
        _svc_to_save = state.get("known_service")
        if isinstance(_svc_to_save, list):
            _svc_to_save = json.dumps(_svc_to_save, ensure_ascii=False) if _svc_to_save else None
        # ── whatsapp_state ────────────────────────────────────────────────
        con.execute(
            """INSERT INTO whatsapp_state (phone, known_service, known_day, known_time, known_name, current_step, lang, upsell_offered, upsell_rejected, completed, msg_intent)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   known_service   = excluded.known_service,
                   known_day       = excluded.known_day,
                   known_time      = excluded.known_time,
                   known_name      = excluded.known_name,
                   current_step    = excluded.current_step,
                   lang            = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE whatsapp_state.lang END,
                   upsell_offered  = excluded.upsell_offered,
                   upsell_rejected = excluded.upsell_rejected,
                   completed       = excluded.completed,
                   msg_intent      = CASE WHEN excluded.msg_intent != '' THEN excluded.msg_intent ELSE whatsapp_state.msg_intent END""",
            (phone,
             _svc_to_save,
             state.get("known_day"),
             state.get("known_time"),
             state.get("known_name"),
             state.get("current_step", "service"),
             state.get("lang", ""),
             1 if state.get("upsell_offered") else 0,
             1 if state.get("upsell_rejected") else 0,
             1 if state.get("completed") else 0,
             state.get("msg_intent") or "")
        )
        # ── conversations (spec table) ────────────────────────────────────
        _cat_ids_json = state.get("known_catalog_ids_json", "[]")
        con.execute(
            """INSERT INTO conversations
                   (client_id, phone, lang, current_step, known_catalog_ids_json,
                    known_day, known_time, known_name, upsell_offered, upsell_rejected)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(client_id, phone) DO UPDATE SET
                   lang                   = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE conversations.lang END,
                   current_step           = CASE
                       WHEN conversations.current_step IN ('ask_day','ask_time','ask_name','confirm')
                       THEN conversations.current_step
                       ELSE excluded.current_step
                   END,
                   known_catalog_ids_json = excluded.known_catalog_ids_json,
                   known_day              = excluded.known_day,
                   known_time             = excluded.known_time,
                   known_name             = excluded.known_name,
                   upsell_offered         = excluded.upsell_offered,
                   upsell_rejected        = excluded.upsell_rejected,
                   updated_at             = CURRENT_TIMESTAMP""",
            (CLIENT_ID, phone,
             state.get("lang", ""),
             state.get("current_step", "service"),
             _cat_ids_json,
             state.get("known_day"),
             state.get("known_time"),
             state.get("known_name"),
             1 if state.get("upsell_offered") else 0,
             1 if state.get("upsell_rejected") else 0)
        )
        con.commit()
        print(f"[STATE_SAVE] committed lang={state.get('lang')!r} catalog_ids={_cat_ids_json!r}")
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

def load_catalog_for_ai(client_id):
    """Load all active catalog items for the client.
    Returns list of dicts with all fields needed for AI context.
    Logs [CATALOG_LOADED]."""
    try:
        con = get_db_connection()
        try:
            rows = con.execute("""
                SELECT title, description, price, sale_price, currency, category, type
                FROM   catalogs
                WHERE  client_id = ? AND is_active = 1
                ORDER  BY id ASC
            """, (client_id,)).fetchall()
            items = [dict(r) for r in rows]
        finally:
            con.close()
        print(f"[CATALOG_LOADED] client={client_id} count={len(items)} titles={[i['title'] for i in items]}")
        return items
    except Exception as _e:
        print(f"[CATALOG_LOADED] ERROR client={client_id} err={_e!r}")
        return []


def _build_catalog_context(catalog_items):
    """Build a structured, generic catalog block for the AI system prompt.
    Works for any business type — no hardcoded industry terms.
    Returns a formatted string or empty string if no items."""
    if not catalog_items:
        return ""
    lines = ["Available catalog:"]
    for it in catalog_items:
        title    = (it.get("title")       or "").strip()
        itype    = (it.get("type")        or "").strip()
        category = (it.get("category")    or "").strip()
        desc     = (it.get("description") or "").strip()
        price    = it.get("price")    or 0
        sale     = it.get("sale_price")
        cur      = (it.get("currency") or "").strip()
        block = [f"- {title}"]
        if itype:
            block.append(f"  type: {itype}")
        if category:
            block.append(f"  category: {category}")
        if desc:
            block.append(f"  description: {desc}")
        block.append(f"  price: {price} {cur}".strip())
        if sale and float(sale) > 0:
            block.append(f"  sale_price: {sale} {cur}".strip())
        lines.extend(block)
    return "\n".join(lines)


# ── Keywords that signal a catalog-intent query ────────────────────────────────
_CATALOG_TRIGGER_WORDS = [
    # Arabic availability
    "هل لديكم", "هل عندكم", "هل عندك", "هل توفر", "هل يوجد", "هل توجد",
    "يوجد لديكم", "لديكم", "عندكم", "عندك", "توفر", "متوفر",
    # Arabic price
    "سعر", "سعره", "سعرها", "بكم", "بكم هي", "كم سعر", "كم ثمن",
    "ثمن", "التكلفة", "كم تكلفة", "تكلفة",
    # English
    "do you have", "is there", "do you sell",
    "price", "cost", "how much", "what is the price",
]


def _catalog_match_by_keywords(catalog_items, msg):
    """Fuzzy-match catalog items against a customer message.

    Algorithm:
    1. Tokenise the message into words ≥ 3 chars (Arabic or Latin).
    2. For each token, check if it appears as a sub-string of any item's
       title, description, or category (case-insensitive).
    3. Return all items that had at least one token match, sorted by
       number of matches descending.
    """
    import re as _re
    msg_clean = msg.lower().strip()
    # Tokenise — words of 3+ chars
    tokens = [w for w in _re.split(r'[\s،,؟?!.]+', msg_clean) if len(w) >= 3]
    if not tokens:
        return []

    scored = []
    for item in catalog_items:
        haystack = " ".join([
            (item.get("title")       or ""),
            (item.get("description") or ""),
            (item.get("category")    or ""),
        ]).lower()
        hits = sum(1 for tok in tokens if tok in haystack)
        if hits > 0:
            scored.append((hits, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored]


def _format_catalog_reply(matched_items, all_items, lang, msg):
    """Build a rich WhatsApp reply from catalog match results.

    matched_items  — items whose title/desc matched the query
    all_items      — full catalog (used for 'not found' guard)
    """
    not_found = {
        "ar": "عذراً، هذه الخدمة أو المنتج غير متوفر حالياً في الكتالوج. 😊\nهل يمكنني مساعدتك في شيء آخر؟",
        "en": "Sorry, this service or product is not currently in our catalog. 😊\nCan I help you with something else?",
        "fr": "Désolé, ce service ou produit n'est pas disponible actuellement. 😊\nPuis-je vous aider avec autre chose?",
    }
    yes_hdr = {
        "ar": "نعم، لدينا ما يلي:",
        "en": "Yes, we have the following:",
        "fr": "Oui, nous avons:",
    }
    order_cta = {
        "ar": "هل ترغب في الطلب أو الحجز؟ 😊",
        "en": "Would you like to order or book? 😊",
        "fr": "Voulez-vous commander ou réserver? 😊",
    }
    _lang = lang if lang in not_found else "ar"

    if not matched_items:
        return not_found[_lang]

    lines = [yes_hdr[_lang]]
    for it in matched_items:
        regular  = it.get("price") or 0
        sale     = it.get("sale_price")
        cur      = (it.get("currency") or "").strip()
        title    = it.get("title", "")
        desc     = (it.get("description") or "").strip()

        if sale and float(sale) > 0 and float(sale) < float(regular):
            price_str = f"💎 *{title}*: ~~{regular} {cur}~~ → *{sale} {cur}*"
        else:
            price_str = f"💎 *{title}*: *{regular} {cur}*"

        lines.append(price_str)
        if desc:
            lines.append(f"   _{desc}_")

    lines.append("")
    lines.append(order_cta[_lang])
    return "\n".join(lines)


def build_ai_prompt(client, lang="ar", catalog_items=None):
    """Build a dynamic system prompt from client AI Brain + live catalog.
    Logs [AI_BRAIN_LOADED] and [AI_PROMPT_BUILT].
    """
    biz_name    = (client or {}).get("name")             or "Business"
    biz_type    = (client or {}).get("business_type")    or "business"
    tone        = (client or {}).get("assistant_tone")   or "friendly"
    goal        = (client or {}).get("assistant_goal")   or "book_appointments"
    description = (client or {}).get("business_description") or ""
    policies    = (client or {}).get("policies")         or ""
    fallback    = (client or {}).get("fallback_message") or ""

    print(f"[AI_BRAIN_LOADED] biz={biz_name!r} type={biz_type!r} tone={tone!r} goal={goal!r} lang={lang!r}")

    # ── Tone mapping ──────────────────────────────────────────────────────────
    _tone_map = {
        "friendly":    "Be warm, helpful, and conversational.",
        "professional":"Be formal, precise, and courteous.",
        "luxury":      "Be refined, elegant, and exclusive. Use premium vocabulary.",
        "short":       "Be very concise — 1 to 2 lines per reply maximum.",
        "persuasive":  "Be confident and sales-driven. Highlight value and urgency.",
    }
    tone_instruction = _tone_map.get(tone, _tone_map["friendly"])

    # ── Goal mapping ──────────────────────────────────────────────────────────
    _goal_map = {
        "sell_products":     "Your main goal is to help customers find and purchase products.",
        "book_appointments": "Your main goal is to guide customers to book an appointment. Follow this flow: service → day → time → name → confirm.",
        "customer_support":  "Your main goal is to resolve customer questions and issues efficiently.",
        "collect_leads":     "Your main goal is to collect the customer's name, phone number, and interest.",
        "mixed":             "Your goal is to sell, support, and book as needed based on the customer's request.",
    }
    goal_instruction = _goal_map.get(goal, _goal_map["book_appointments"])

    # ── Catalog section — generic, works for any business type ───────────────
    has_catalog     = bool(catalog_items)
    _catalog_block  = _build_catalog_context(catalog_items) if has_catalog else ""
    catalog_section = f"\n\n{_catalog_block}" if _catalog_block else ""

    # Generic catalog enforcement rules — no hardcoded industry terms
    if has_catalog:
        catalog_rule = (
            "\n- CATALOG RULES (apply to every message):"
            "\n  1. When the customer asks about availability, price, products, services,"
            " booking, or ordering — answer ONLY from the catalog above."
            "\n  2. Match the customer's request to catalog items dynamically."
            " Do not hardcode any product or service name."
            "\n  3. If the item is in the catalog: show its name, price, sale price (if any),"
            " and ask if the customer wants to proceed."
            "\n  4. If the item is NOT in the catalog: say it is not currently available."
            " Do not invent products, services, prices, or discounts."
            "\n  5. Always reply in the customer's language."
        )
    else:
        catalog_rule = (
            "\n- No catalog items are currently available for this business."
        )

    # ── Policies section ──────────────────────────────────────────────────────
    policies_section = f"\n\nPOLICIES:\n{policies.strip()}" if policies.strip() else ""

    # ── Description section ───────────────────────────────────────────────────
    desc_section = f"\n\nABOUT THE BUSINESS:\n{description.strip()}" if description.strip() else ""

    # ── Fallback ──────────────────────────────────────────────────────────────
    fallback_section = (
        f"\n\nFALLBACK: If you cannot answer, say: \"{fallback.strip()}\""
        if fallback.strip() else ""
    )

    prompt = (
        f"You are a business assistant for {biz_name} ({biz_type}).\n"
        f"Use ONLY the catalog below when answering product/service/price/availability questions.\n"
        f"Do not invent products, services, prices, or discounts.\n\n"
        f"TONE: {tone_instruction}\n\n"
        f"GOAL: {goal_instruction}"
        f"{desc_section}"
        f"{catalog_section}"
        f"{policies_section}"
        f"{fallback_section}\n\n"
        f"RULES:\n"
        f"- Reply language: {lang}. Always reply in the customer's language.\n"
        f"- Never restart the conversation.\n"
        f"- Always continue from the customer's last message.\n"
        f"- Keep replies short (2-3 lines max) unless detail is needed.\n"
        f"- Do not repeat greetings or ask unnecessary questions."
        f"{catalog_rule}"
    )
    _cat_count = len(catalog_items) if catalog_items else 0
    if _cat_count > 0:
        print(f"[CATALOG_SEPARATE_FROM_BRAIN] catalog merged into prompt — {_cat_count} items (NOT from brain fields)")
    print(f"[AI_PROMPT_BUILT] length={len(prompt)} chars catalog_items={_cat_count}")
    return prompt

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


def detect_customer_language(message):
    """Confident language detection — returns ar/fr/en/es/it or None if unsure.
    Unlike detect_lang, this never forces a default so callers can fall back
    to client.default_language instead.
    """
    import re as _dlre
    msg_lower = message.lower().strip()

    # Arabic script characters — high confidence
    if _dlre.search(r'[\u0600-\u06FF]', message):
        print("[CUSTOMER_LANGUAGE_DETECTED] script=arabic → ar")
        return "ar"

    # English
    if any(w in msg_lower for w in [
        "hello", "hi ", "hey", "good morning", "good evening",
        "how are you", "i want", "i need", "please", "thank", "yes", "no"
    ]):
        print("[CUSTOMER_LANGUAGE_DETECTED] rule=en")
        return "en"

    # French
    if any(w in msg_lower for w in [
        "bonjour", "salut", "bonsoir", "merci", "je veux",
        "je voudrais", "s'il vous", "excusez"
    ]):
        print("[CUSTOMER_LANGUAGE_DETECTED] rule=fr")
        return "fr"

    # Spanish
    if any(w in msg_lower for w in [
        "hola", "buenos", "gracias", "quiero", "necesito",
        "por favor", "buenas"
    ]):
        print("[CUSTOMER_LANGUAGE_DETECTED] rule=es")
        return "es"

    # Italian
    if any(w in msg_lower for w in [
        "ciao", "buongiorno", "buonasera", "grazie", "voglio",
        "vorrei", "salve", "prego"
    ]):
        print("[CUSTOMER_LANGUAGE_DETECTED] rule=it")
        return "it"

    print("[CUSTOMER_LANGUAGE_DETECTED] not confident → None")
    return None


def get_reply_language(client, incoming_msg):
    """Determine the language to reply in.
    1. Use detected customer language if confident.
    2. Otherwise fall back to client.default_language.
    3. Final fallback: 'ar'.
    """
    client_default = (client or {}).get("default_language") or "ar"
    print(f"[CLIENT_DEFAULT_LANGUAGE] {client_default!r}")
    detected = detect_customer_language(incoming_msg)
    reply_lang = detected or client_default
    print(f"[REPLY_LANGUAGE] detected={detected!r} client_default={client_default!r} → using={reply_lang!r}")
    return reply_lang

def openai_chat(user_message, lang="ar", client_obj=None, catalog_items=None):
    _cat_count = len(catalog_items) if catalog_items else 0
    print(f"[AI_CALLED] msg={user_message!r} lang={lang!r} catalog_items={_cat_count}")
    system_prompt = build_ai_prompt(client_obj, lang=lang, catalog_items=catalog_items)
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_message}
                ]
            },
            timeout=20
        )
        print(f"[OPENAI] response status={resp.status_code} body={resp.text[:300]!r}")
        if resp.status_code == 200:
            _reply = resp.json()["choices"][0]["message"]["content"].strip()
            if _reply:
                print(f"[AI_RESPONSE_SENT] reply_preview={_reply[:120]!r}")
                return _reply
            # GPT returned 200 but empty content
            print(f"[EMPTY_REPLY_GUARD] AI returned empty content — using fallback lang={lang!r}")
    except Exception as _oe:
        print(f"[AI_CALLED] ERROR — {_oe!r}")
    # Fallback — use client's configured fallback first, never a greeting
    _fallback_msg = ((client_obj or {}).get("fallback_message") or "").strip()
    if _fallback_msg:
        print(f"[EMPTY_REPLY_GUARD] using client fallback_message")
        return _fallback_msg
    _err = {
        "ar": "عذراً، لم أتمكن من الإجابة الآن. يرجى المحاولة مجدداً أو التواصل معنا مباشرة.",
        "en": "Sorry, I couldn't respond right now. Please try again or contact us directly.",
        "fr": "Désolé, je n'ai pas pu répondre. Veuillez réessayer ou nous contacter directement.",
    }
    _safe = _err.get(lang, _err["ar"])
    print(f"[EMPTY_REPLY_GUARD] using generic error fallback lang={lang!r}")
    return _safe

def normalize_number(raw):
    """Return a WhatsApp number in the canonical form  DIGITS@c.us.
    Handles every known malformed variant:
      • "22923289"            → "22923289@c.us"
      • "+22923289"           → "22923289@c.us"
      • "22923289@c.us"       → "22923289@c.us"   (already correct)
      • "c.us@22923289"       → "22923289@c.us"   (reversed)
      • "whatsapp:22923289"   → "22923289@c.us"
      • "  22923289  "        → "22923289@c.us"
    """
    import re as _re
    s = str(raw).strip()
    # Remove known text prefixes
    s = s.replace("whatsapp:", "").replace("@c.us", "").replace("c.us@", "")
    # Strip non-digit characters (handles +, spaces, dashes, dots)
    digits = _re.sub(r"\D", "", s)
    if not digits:
        # Fallback: return raw with @c.us so the caller can log it
        return str(raw).strip() + "@c.us"
    return digits + "@c.us"

def get_latest_pending_client():
    """Return the most-recent client that is pending WhatsApp connection with no number yet.
    Used by the auto-connect webhook when no token is present."""
    _con = get_db_connection()
    try:
        row = _con.execute("""
            SELECT id, name FROM clients
            WHERE whatsapp_connection_status='pending'
              AND (business_whatsapp_number IS NULL OR business_whatsapp_number='')
            ORDER BY id DESC LIMIT 1
        """).fetchone()
        return dict(row) if row else None
    finally:
        _con.close()


def get_latest_pending_whatsapp_client():
    """Return the most-recent pending client including all debug fields."""
    con = get_db_connection()
    try:
        row = con.execute("""
            SELECT id, name, business_whatsapp_number, whatsapp_connection_status
            FROM clients
            WHERE whatsapp_connection_status = 'pending'
            ORDER BY id DESC
            LIMIT 1
        """).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def mark_client_whatsapp_connected(client_id, phone):
    """Set a client's WhatsApp number and mark them connected."""
    con = get_db_connection()
    try:
        con.execute("""
            UPDATE clients
            SET business_whatsapp_number      = ?,
                whatsapp_connected            = 1,
                whatsapp_connection_status    = 'connected',
                whatsapp_provider             = 'manual_ultramsg'
            WHERE id = ?
        """, (normalize_number(phone), client_id))
        con.commit()
    finally:
        con.close()
    track_event(client_id, "whatsapp_connected", {"phone": phone})


def wa_reply(to, text, client_id=None):
    """Send a message to the CUSTOMER only. Never call this with admin content.
    Uses the client's own WhatsApp instance if connected, else platform fallback."""
    _cid = client_id or getattr(g, 'wa_client_id', CLIENT_ID)
    to   = normalize_number(to)
    print(f"[SEND_CUSTOMER] client={_cid} to={to!r}")
    print(f"[SEND_CUSTOMER] body={text!r}")
    resp = send_whatsapp_message(_cid, to, text)
    print(f"[SEND_CUSTOMER] status={resp.status_code if resp else 'N/A'}")
    return "", 200


def _fire_first_reply_nudge(sender, client_id):
    """After the first real AI reply, send a value nudge to the customer (async, 2.5s delay)."""
    import threading
    def _send():
        try:
            _c = get_db_connection()
            try:
                _row = _c.execute(
                    "SELECT id FROM analytics_events WHERE client_id=? AND event_name='first_reply_nudge' LIMIT 1",
                    (client_id,)
                ).fetchone()
                if _row:
                    return
                _c.execute(
                    "INSERT INTO analytics_events(client_id, event_name, event_data, created_at) VALUES(?,?,?,?)",
                    (client_id, "first_reply_nudge", "{}", datetime.datetime.utcnow().isoformat())
                )
                _c.commit()
            finally:
                _c.close()
            nudge_msg = (
                "🔥 يمكنك الآن:\n"
                "✔ استقبال الطلبات\n"
                "✔ حجز المواعيد\n\n"
                "جرب إرسال: (أريد حجز)"
            )
            ultramsg_send(normalize_number(sender), nudge_msg)
            print(f"[USER_NUDGED] first_reply_nudge sent to={sender!r} client={client_id}")
        except Exception as _nudge_err:
            print(f"[NUDGE_ERROR] {_nudge_err}")
    _t = threading.Timer(2.5, _send)
    _t.daemon = True
    _t.start()


# wa_send_admin() REMOVED — use send_booking_messages() as the single send point

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
        "ar": "خيار ممتاز ✨ {svc} {benefit}.\nالسعر {price} فقط.",
        "en": "Great choice ✨ {svc} {benefit}.\nOnly {price}.",
        "fr": "Excellent choix ✨ {svc} {benefit}.\nSeulement {price}.",
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

def _bot_str(key, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    return _STRINGS.get(key, {}).get(lang) or _STRINGS.get(key, {}).get("ar", "")

def build_ask_service(client_id, lang):
    """Build 'choose a service' prompt dynamically from the catalog."""
    l = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT title, price, sale_price FROM catalogs WHERE client_id=? AND is_active=1 ORDER BY id",
            (client_id,)
        ).fetchall()
    finally:
        con.close()
    cur = get_client(client_id).get("currency", "SAR")
    if not rows:
        return _bot_str("ask_service", l)
    bullets = ""
    for r in rows:
        p = r["sale_price"] or r["price"] or 0
        bullets += f"\n• {r['title']} — {int(p)} {cur}"
    _headers = {
        "ar": f"أهلاً! 😊 كيف يمكنني مساعدتك؟ يمكنك الاختيار من:{bullets}",
        "en": f"Hello! 😊 How can I help you? Choose from:{bullets}",
        "fr": f"Bonjour! 😊 Comment puis-je vous aider? Choisissez parmi:{bullets}",
    }
    return _headers[l]

def build_price_list(client_id, lang):
    """Build price list dynamically from the catalog."""
    l = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT title, price, sale_price FROM catalogs WHERE client_id=? AND is_active=1 ORDER BY id",
            (client_id,)
        ).fetchall()
    finally:
        con.close()
    cur = get_client(client_id).get("currency", "SAR")
    if not rows:
        return _bot_str("price_list", l)
    bullets = ""
    for r in rows:
        p = r["sale_price"] or r["price"] or 0
        bullets += f"\n• {r['title']} — {int(p)} {cur}"
    _headers = {
        "ar": f"يسعدنا خدمتك! 😊 أسعارنا:{bullets}\nأي خدمة تناسبك؟",
        "en": f"Happy to help! 😊 Our prices:{bullets}\nWhich service suits you?",
        "fr": f"Avec plaisir! 😊 Nos tarifs:{bullets}\nQuel service vous convient?",
    }
    return _headers[l]

_SVC_DISPLAY = {
    "تنظيف أسنان": {
        "ar": "تنظيف أسنان",
        "en": "Teeth Cleaning",
        "fr": "Nettoyage des dents",
    },
    "تبييض الأسنان": {
        "ar": "تبييض الأسنان",
        "en": "Teeth Whitening",
        "fr": "Blanchiment des dents",
    },
    "فحص الأسنان": {
        "ar": "فحص الأسنان",
        "en": "Dental Checkup",
        "fr": "Contrôle dentaire",
    },
}

_PRICE_DISPLAY = {
    "تنظيف أسنان": {
        "ar": "100 ريال",
        "en": "100 SAR",
        "fr": "100 SAR",
    },
    "تبييض الأسنان": {
        "ar": "250 ريال",
        "en": "250 SAR",
        "fr": "250 SAR",
    },
    "فحص الأسنان": {
        "ar": "50 ريال",
        "en": "50 SAR",
        "fr": "50 SAR",
    },
}

def svc_name(canonical, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    return _SVC_DISPLAY.get(canonical, {}).get(lang, canonical)

def svc_price(canonical, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT price, sale_price FROM catalogs WHERE client_id=? AND title=? AND is_active=1",
            (CLIENT_ID, canonical)
        ).fetchone()
    finally:
        con.close()
    if row:
        p = row["sale_price"] or row["price"]
        _cur = get_client(CLIENT_ID).get("currency", "MAD")
        return f"{int(p)} {_cur}"
    return _PRICE_DISPLAY.get(canonical, {}).get(lang, _WA_PRICES.get(canonical, ""))

_SVC_BENEFITS = {
    "تنظيف أسنان": {
        "ar": "يساعد على صحة اللثة ويمنحك إحساسًا بالنظافة والانتعاش",
        "en": "helps improve gum health and leaves your teeth feeling fresh",
        "fr": "aide à garder des gencives saines et une sensation de fraîcheur",
    },
    "تبييض الأسنان": {
        "ar": "يحسّن بياض الابتسامة ويمنحك مظهرًا أكثر إشراقًا",
        "en": "brightens your smile and boosts your appearance and confidence",
        "fr": "illumine votre sourire et améliore votre apparence et confiance",
    },
    "فحص الأسنان": {
        "ar": "يكشف المشاكل مبكرًا ويريحك من القلق على صحة أسنانك",
        "en": "detects issues early and gives you peace of mind about your dental health",
        "fr": "détecte les problèmes tôt et vous rassure sur votre santé dentaire",
    },
}

_RECOMMENDATION = {
    "ar": (
        "أنصحك بـ {svc} كبداية ✨\n"
        "{benefit}.\n"
        "السعر {price} فقط — هل تفضل اليوم أو غدًا؟"
    ),
    "en": (
        "I'd recommend starting with {svc} ✨\n"
        "It {benefit}.\n"
        "Only {price} — would you prefer today or tomorrow?"
    ),
    "fr": (
        "Je vous recommande de commencer par {svc} ✨\n"
        "Cela {benefit}.\n"
        "Seulement {price} — aujourd'hui ou demain?"
    ),
}

_RECOMMEND_KEYWORDS = [
    "recommend", "suggest", "best", "what do you offer", "what should i",
    "which service", "not sure", "don't know", "what's good",
    "ماذا تنصح", "ماذا تقترح", "ايش تنصح", "ما الأفضل", "ما هو الأفضل",
    "مش عارف", "مو عارف", "ما أدري", "شو تنصح",
    "que recommandez", "que conseillez", "quoi choisir", "pas sûr",
]

def svc_benefit(canonical, lang):
    lang = lang if lang in ("ar", "en", "fr") else "ar"
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT description FROM catalogs WHERE client_id=? AND title=? AND is_active=1",
            (CLIENT_ID, canonical)
        ).fetchone()
    finally:
        con.close()
    if row and row["description"]:
        return row["description"]
    return _SVC_BENEFITS.get(canonical, {}).get(lang, "")

def is_recommendation_request(msg):
    msg_lower = msg.lower()
    return any(kw in msg_lower for kw in _RECOMMEND_KEYWORDS)

def is_affirmation(msg):
    msg = (msg or "").strip().lower()
    return msg in {"yes", "oui", "نعم", "ok", "okay", "يعم", "ايه", "اوك"}

_ADD_INTENT_KEYWORDS = ["أضيف", "اضف", "أضف", "add", "ajoute", "ajouter"]

def is_add_intent(msg):
    text = (msg or "").lower()
    return any(kw in text for kw in _ADD_INTENT_KEYWORDS)

def ensure_svc_list(val):
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]

def format_services(services, lang="ar"):
    if not services:
        return ""
    if isinstance(services, str):
        services = [services]
    _lang  = lang if lang in ("ar", "en", "fr") else "ar"
    _label = {"ar": "الخدمات", "en": "Services", "fr": "Services"}
    names  = [svc_name(s, lang) for s in services]
    lines  = "\n".join(f"• {n}" for n in names)
    return f"{_label[_lang]}:\n{lines}"

def format_svcs(svcs, lang):
    return format_services(svcs, lang)

_NOISE_MESSAGES = {
    "سلام", "السلام", "السلام عليكم", "وعليكم السلام",
    "hi", "hello", "hey",
    "bonjour", "bonsoir", "salut",
}

def is_noise_message(msg):
    return (msg or "").strip().lower() in _NOISE_MESSAGES

_CANONICAL_SERVICE_MAP = {
    "teeth_cleaning":  "تنظيف أسنان",
    "teeth_whitening": "تبييض الأسنان",
    "dental_checkup":  "فحص الأسنان",
}

def extract_entities(msg):
    import re
    text    = (msg or "").lower().strip()
    service = None
    day     = None
    time    = None
    _svc_keywords = {
        "teeth_cleaning":  ["تنظيف", "cleaning", "nettoyage"],
        "teeth_whitening": ["تبييض", "whitening", "blanchiment"],
        "dental_checkup":  ["فحص", "checkup", "consultation", "contrôle", "controle"],
    }
    for _canon, _kws in _svc_keywords.items():
        if any(kw in text for kw in _kws):
            service = _canon
            break
    if "اليوم" in text or "today" in text or "aujourd'hui" in text:
        day = "today"
    elif any(w in text for w in ["غد", "غدا", "غدًا", "tomorrow", "demain"]):
        day = "tomorrow"
    m = re.search(r"(?<!\d)\d{1,2}:\d{2}(?!\d)", text)
    if m:
        time = normalize_time_input(m.group())
    else:
        m = re.search(r"(?<!\d)\d{1,2}(am|pm)(?!\w)", text)
        if m:
            time = normalize_time_input(m.group())
        else:
            m = re.search(r"(?<!\d)\d{1,2}(?!\d)", text)
            if m:
                candidate = normalize_time_input(m.group())
                if is_valid_time(candidate):
                    time = candidate
    print(f"[ENTITY_EXTRACT] service={service!r} day={day!r} time={time!r}")
    return service, day, time

_PARSE_SYSTEM_PROMPT = (
    "You are a universal business assistant message parser. "
    "Parse the user message and return ONLY a valid JSON object with exactly these keys:\n"
    "  intent        — one of: book_service | add_service | cancel | query | affirm | reject | other\n"
    "  service       — the service/product the user wants (exact text from message) or null\n"
    "  add_on_service— an additional service mentioned with add-intent words (أضيف/add/ajoute) or null\n"
    "  day           — one of: today | tomorrow | null\n"
    "  time          — 24-hour string HH:MM or null\n"
    "  name          — person name string (1-2 words) or null\n"
    "  affirmation   — true if message means yes/ok/confirm, else false\n"
    "  rejection     — true if message means no/refuse, else false\n\n"
    "Rules:\n"
    "- service = the main product or service the user wants to book or order\n"
    "- add_on_service = extra service mentioned with add-intent words; if set, service=null\n"
    "- day: اليوم/today/aujourd'hui → today; غدا/غداً/tomorrow/demain → tomorrow\n"
    "- time: normalize to 24-hour HH:MM; 'الساعة 5' or '5 مساء' → '17:00' (assume PM for 1-9)\n"
    "- name: only a person's first name, never a service or sentence\n"
    "- affirmation: نعم/yes/oui/ok/تمام/أكيد/بالتأكيد\n"
    "- rejection: لا/no/non/لأ/ما أبي/ما أريد\n"
    "- Return ONLY the JSON object. No markdown, no explanation."
)

def parse_user_message(msg, lang="ar"):
    _empty = {
        "intent": "other", "service": None, "add_on_service": None,
        "day": None, "time": None, "name": None,
        "affirmation": False, "rejection": False,
    }
    print(f"[PARSE] raw={msg!r}")
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": _PARSE_SYSTEM_PROMPT},
                    {"role": "user",   "content": msg},
                ],
                "temperature": 0,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"[PARSE] OpenAI error status={resp.status_code}")
            return _empty
        import json as _json
        raw_content = resp.json()["choices"][0]["message"]["content"].strip()
        parsed = _json.loads(raw_content)
        for k, v in _empty.items():
            if k not in parsed:
                parsed[k] = v
        print(f"[PARSE] result={parsed}")
        return parsed
    except Exception as _pe:
        print(f"[PARSE] failed={repr(_pe)} — falling back to regex")
        return _empty


# ── Intent Detection ──────────────────────────────────────────────────────────
_INTENT_LABELS = {"book_appointment", "place_order", "ask_price", "ask_info", "greeting"}

def detect_message_intent(msg, lang="ar"):
    """Single-call AI intent classifier.
    Returns one of: book_appointment | place_order | ask_price | ask_info | greeting
    Falls back to 'ask_info' on any error.
    """
    prompt = (
        "حدد نية هذه الرسالة بكلمة واحدة فقط من القائمة التالية:\n"
        "book_appointment\n"
        "place_order\n"
        "ask_price\n"
        "ask_info\n"
        "greeting\n\n"
        "قواعد التصنيف:\n"
        "- greeting: فقط إذا كانت الرسالة تحية بحتة مثل (مرحبا، السلام عليكم، hello) بدون أي سؤال\n"
        "- ask_info: أي سؤال عن منتج أو خدمة مثل (هل لديكم X، عندكم X، هل يوجد X)\n"
        "- ask_price: أي سؤال عن السعر مثل (كم سعر X، بكم X)\n"
        "- book_appointment: طلب حجز موعد\n"
        "- place_order: طلب شراء منتج\n"
        "مهم: الأسئلة مثل 'هل لديكم عطور' هي ask_info وليست greeting\n\n"
        f"الرسالة:\n{msg}"
    )
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "max_tokens": 10,
            },
            timeout=8,
        )
        if resp.status_code == 200:
            intent = resp.json()["choices"][0]["message"]["content"].strip().lower()
            # Keep only the first word in case the model adds extra text
            intent = intent.split()[0] if intent else "ask_info"
            if intent not in _INTENT_LABELS:
                intent = "ask_info"
            print(f"[INTENT_DETECTED] intent={intent!r} msg={msg[:60]!r}")
            return intent  # track_event called by caller with client_id context
    except Exception as _ie:
        print(f"[INTENT_DETECT_ERROR] {_ie}")
    return "ask_info"


def create_intent_order(client_id, phone, intent, service=None):
    """Create a row in the orders table to track an intent-driven conversation.
    Returns the new order id.
    """
    con = get_db_connection()
    try:
        cur = con.execute("""
            INSERT INTO orders (client_id, customer_phone, intent, name, items, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
        """, (
            client_id,
            normalize_number(phone),
            intent,
            "",
            service or "",
            datetime.datetime.utcnow().isoformat(),
        ))
        con.commit()
        order_id = cur.lastrowid
        print(f"[ORDER_CREATED] order_id={order_id} client={client_id} phone={phone!r} intent={intent!r}")
        track_event(client_id, "order_created", {"order_id": order_id, "phone": phone, "intent": intent})
        return order_id
    finally:
        con.close()


# ── Conversation Flow Engine ──────────────────────────────────────────────────

_FLOW_STEPS = ("ask_day", "ask_time", "ask_name", "confirm", "done")

_FLOW_STRINGS = {
    "ask_day": {
        "ar": "ما اليوم المناسب لك؟ 🗓️",
        "en": "What day works for you? 🗓️",
        "fr": "Quel jour vous convient? 🗓️",
        "es": "¿Qué día te conviene? 🗓️",
        "it": "Quale giorno ti va bene? 🗓️",
    },
    "ask_time": {
        "ar": "ما الوقت المناسب؟ 🕐",
        "en": "What time works for you? 🕐",
        "fr": "À quelle heure? 🕐",
        "es": "¿A qué hora? 🕐",
        "it": "A che ora? 🕐",
    },
    "ask_name": {
        "ar": "ما اسمك؟ 😊",
        "en": "What is your name? 😊",
        "fr": "Quel est votre nom? 😊",
        "es": "¿Cuál es tu nombre? 😊",
        "it": "Come ti chiami? 😊",
    },
    "confirm_tmpl": {
        "ar": "حجزك يوم {day} الساعة {time} باسم {name}. هل تؤكد؟ ✅",
        "en": "Your booking: {day} at {time}, name: {name}. Confirm? ✅",
        "fr": "Votre réservation: {day} à {time}, nom: {name}. Confirmez? ✅",
        "es": "Tu reserva: {day} a las {time}, nombre: {name}. ¿Confirmas? ✅",
        "it": "La tua prenotazione: {day} alle {time}, nome: {name}. Confermi? ✅",
    },
    "confirmed": {
        "ar": "تم تأكيد حجزك! ✅ نحن بانتظارك 🌟",
        "en": "Booking confirmed! ✅ We look forward to seeing you 🌟",
        "fr": "Réservation confirmée! ✅ Nous avons hâte de vous accueillir 🌟",
        "es": "¡Reserva confirmada! ✅ Te esperamos 🌟",
        "it": "Prenotazione confermata! ✅ Non vediamo l'ora di vederti 🌟",
    },
    "cancelled": {
        "ar": "تم إلغاء الحجز. يمكنك البدء من جديد في أي وقت.",
        "en": "Booking cancelled. You can start again anytime.",
        "fr": "Réservation annulée. Vous pouvez recommencer à tout moment.",
        "es": "Reserva cancelada. Puedes volver a empezar en cualquier momento.",
        "it": "Prenotazione annullata. Puoi ricominciare in qualsiasi momento.",
    },
    "need_day": {
        "ar": "لم أفهم اليوم. اكتب مثلاً: اليوم أو غدًا.",
        "en": "I didn't catch the day. Try: today or tomorrow.",
        "fr": "Je n'ai pas compris le jour. Essayez: aujourd'hui ou demain.",
        "es": "No entendí el día. Prueba: hoy o mañana.",
        "it": "Non ho capito il giorno. Prova: oggi o domani.",
    },
    "need_time": {
        "ar": "لم أفهم الوقت. اكتب مثلاً: 10:00 أو الساعة 3.",
        "en": "I didn't catch the time. Try: 10:00 or 3pm.",
        "fr": "Je n'ai pas compris l'heure. Essayez: 10h00 ou 15h00.",
        "es": "No entendí la hora. Prueba: 10:00 o las 3.",
        "it": "Non ho capito l'orario. Prova: 10:00 o le 3.",
    },
}

def _fs(key, lang):
    """Get a flow string in the right language, falling back to Arabic."""
    return _FLOW_STRINGS.get(key, {}).get(lang) or _FLOW_STRINGS.get(key, {}).get("ar", "")


_FLOW_DAY_MAP = {
    "اليوم": "اليوم", "today": "اليوم", "aujourd'hui": "اليوم", "hoy": "اليوم", "oggi": "اليوم",
    "غدا": "غدًا",   "غدًا": "غدًا",   "tomorrow": "غدًا", "demain": "غدًا", "mañana": "غدًا",  "domani": "غدًا",
}

def _flow_detect_day(msg):
    """Return normalised Arabic day string or None."""
    t = msg.lower().strip()
    for kw, val in _FLOW_DAY_MAP.items():
        if kw.lower() in t:
            return val
    return None

def _flow_detect_time(msg):
    """Return HH:MM string or None using existing helpers."""
    try:
        _, _, t = extract_entities(msg)
        if t:
            return t
    except Exception:
        pass
    import re as _re
    m = _re.search(r'\b(\d{1,2}):(\d{2})\b', msg)
    if m:
        candidate = f"{int(m.group(1)):02d}:{m.group(2)}"
        if is_valid_time(candidate):
            return candidate
    m2 = _re.search(r'\b(\d{1,2})\b', msg)
    if m2:
        h = int(m2.group(1))
        if 1 <= h <= 12:
            h += 12 if h < 8 else 0
        candidate = f"{h:02d}:00"
        if is_valid_time(candidate):
            return candidate
    return None

def _flow_detect_name(msg):
    """Best-effort first-word name extraction (1-2 words, not a digit)."""
    import re as _re
    words = [w for w in _re.split(r"[\s,،]+", msg.strip()) if w and not w.isdigit()]
    if words:
        return " ".join(words[:2])
    return None

_AFFIRM_KW = {"نعم","اه","أه","آه","ايه","أيه","اوك","ok","yes","oui","sí","si","sì","تمام","أكيد","اكيد","صح","بالتأكيد","موافق"}
_REJECT_KW  = {"لا","لأ","no","non","لا أريد","ما أريد","إلغاء","الغاء","cancel","annuler","cancelar","cancella"}

def _flow_is_affirm(msg):
    t = msg.lower().strip()
    return any(k in t for k in _AFFIRM_KW)

def _flow_is_reject(msg):
    t = msg.lower().strip()
    return any(k in t for k in _REJECT_KW)


def flow_load(client_id, phone):
    """Load current flow state from conversations table.
    Returns dict with current_step and collected_data (dict) or None if no flow active.
    Phone is stored raw (same format wa_save uses).
    """
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT current_step, collected_data, lang FROM conversations WHERE client_id=? AND phone=?",
            (client_id, phone)
        ).fetchone()
    finally:
        con.close()
    if not row:
        return None
    step = row["current_step"] or ""
    if step not in _FLOW_STEPS:
        return None
    try:
        data = json.loads(row["collected_data"] or "{}")
    except Exception:
        data = {}
    return {"current_step": step, "collected_data": data, "lang": row["lang"] or "ar"}


def flow_save(client_id, phone, step, collected_data, lang=""):
    """Upsert flow state into conversations table.
    Phone is stored raw (same format wa_save uses).
    """
    data_json = json.dumps(collected_data, ensure_ascii=False)
    now_iso   = datetime.datetime.utcnow().isoformat()
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO conversations (client_id, phone, current_step, collected_data, lang, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_id, phone) DO UPDATE SET
                current_step   = excluded.current_step,
                collected_data = excluded.collected_data,
                lang           = CASE WHEN excluded.lang != '' THEN excluded.lang ELSE conversations.lang END,
                updated_at     = excluded.updated_at
        """, (client_id, phone, step, data_json,
              lang if lang else "", now_iso))
        con.commit()
        print(f"[FLOW_STEP] client={client_id} phone={phone!r} step={step!r} data={collected_data}")
    finally:
        con.close()


def flow_reset(client_id, phone):
    """Reset the flow by clearing step back to empty (conversation over)."""
    con = get_db_connection()
    try:
        con.execute("""
            UPDATE conversations
            SET current_step='', collected_data='{}', updated_at=?
            WHERE client_id=? AND phone=?
        """, (datetime.datetime.utcnow().isoformat(), client_id, phone))
        con.commit()
    finally:
        con.close()


def run_booking_flow(sender, incoming_msg, client_id, lang, flow):
    """Execute one turn of the booking conversation flow.
    flow = result of flow_load() — guaranteed non-None and step in _FLOW_STEPS.
    Returns a Flask response via wa_reply().
    """
    step = flow["current_step"]
    data = dict(flow["collected_data"])   # mutable copy

    # ── ask_day ───────────────────────────────────────────────────────────────
    if step == "ask_day":
        day = _flow_detect_day(incoming_msg)
        if not day:
            print(f"[FLOW_STEP] ask_day — could not parse day from {incoming_msg!r}")
            return wa_reply(sender, _fs("need_day", lang))
        data["day"] = day
        flow_save(client_id, sender, "ask_time", data, lang=lang)
        return wa_reply(sender, _fs("ask_time", lang))

    # ── ask_time ──────────────────────────────────────────────────────────────
    elif step == "ask_time":
        time_val = _flow_detect_time(incoming_msg)
        if not time_val:
            print(f"[FLOW_STEP] ask_time — could not parse time from {incoming_msg!r}")
            return wa_reply(sender, _fs("need_time", lang))
        data["time"] = time_val
        flow_save(client_id, sender, "ask_name", data, lang=lang)
        return wa_reply(sender, _fs("ask_name", lang))

    # ── ask_name ──────────────────────────────────────────────────────────────
    elif step == "ask_name":
        name = _flow_detect_name(incoming_msg)
        if not name:
            name = incoming_msg.strip()[:30]    # last resort: use raw text
        data["name"] = name
        flow_save(client_id, sender, "confirm", data, lang=lang)
        confirm_tmpl = _fs("confirm_tmpl", lang)
        summary = confirm_tmpl.format(
            day  = data.get("day",  "—"),
            time = data.get("time", "—"),
            name = data.get("name", "—"),
        )
        return wa_reply(sender, summary)

    # ── confirm ───────────────────────────────────────────────────────────────
    elif step == "confirm":
        if _flow_is_affirm(incoming_msg):
            # 1. Create pending order
            _data_json = json.dumps(data, ensure_ascii=False)
            _norm_phone = normalize_number(sender)
            con = get_db_connection()
            try:
                cur = con.execute("""
                    INSERT INTO orders
                        (client_id, customer_phone, intent, name, items,
                         status, payment_status, payment_provider, created_at)
                    VALUES (?, ?, 'book_appointment', ?, ?, 'pending', 'pending', 'paypal', ?)
                """, (
                    client_id,
                    _norm_phone,
                    data.get("name", ""),
                    _data_json,
                    datetime.datetime.utcnow().isoformat(),
                ))
                con.commit()
                _oid = cur.lastrowid

                # 2. Build full payment URL and save it
                _pay_url = request.url_root.rstrip("/") + f"/pay/{_oid}"
                con.execute(
                    "UPDATE orders SET payment_link=? WHERE id=?",
                    (_pay_url, _oid)
                )
                con.commit()
                print(f"[PAYMENT_LINK_CREATED] order_id={_oid} url={_pay_url!r}")
                track_event(client_id, "payment_started", {"order_id": _oid, "url": _pay_url})
            finally:
                con.close()

            # 3. Reset flow, send payment link via WhatsApp
            flow_reset(client_id, sender)
            _pay_msgs = {
                "ar": f"💳 لإتمام طلبك، يرجى الدفع من هنا:\n{_pay_url}",
                "en": f"💳 To complete your order, please pay here:\n{_pay_url}",
                "fr": f"💳 Pour finaliser votre commande, veuillez payer ici:\n{_pay_url}",
                "es": f"💳 Para completar tu pedido, paga aquí:\n{_pay_url}",
                "it": f"💳 Per completare il tuo ordine, paga qui:\n{_pay_url}",
            }
            return wa_reply(sender, _pay_msgs.get(lang, _pay_msgs["ar"]))

        elif _flow_is_reject(incoming_msg):
            flow_reset(client_id, sender)
            print(f"[FLOW_STEP] confirm — rejected by user")
            return wa_reply(sender, _fs("cancelled", lang))

        else:
            # Re-show summary — not a clear yes or no
            confirm_tmpl = _fs("confirm_tmpl", lang)
            summary = confirm_tmpl.format(
                day  = data.get("day",  "—"),
                time = data.get("time", "—"),
                name = data.get("name", "—"),
            )
            return wa_reply(sender, summary)

    # Fallback — should not reach here
    flow_reset(client_id, sender)
    return wa_reply(sender, _fs("ask_day", lang))


_FULL_INTENT_PROMPT = (
    "You are a booking intent extractor for any business type.\n"
    "The user may mention multiple services, a day, a time, and their name all in ONE message.\n\n"
    "Return ONLY a single valid JSON object — no markdown, no explanation, no extra keys:\n"
    "{\n"
    '  "services": ["<service text 1>", "<service text 2>"],\n'
    '  "day": "today" | "tomorrow" | null,\n'
    '  "time": "HH:MM" | null,\n'
    '  "name": "<first name>" | null\n'
    "}\n\n"
    "Rules:\n"
    "- services: list every service the user mentions (book, add, request). Return exact phrasing. Empty list [] if none.\n"
    "- day: اليوم/today/aujourd'hui → \"today\"; غدا/غدً/غداً/tomorrow/demain → \"tomorrow\"; null if absent.\n"
    "- time: normalize any format to 24-hour HH:MM string.\n"
    "  Examples: '5 مساء' → '17:00', '5pm' → '17:00', '9 صباح' → '09:00', '14:30' → '14:30'.\n"
    "  Assume PM (add 12) for 1–9 without explicit AM/morning indicator. null if absent.\n"
    "- name: extract only after words like اسمي/اسم/my name is/je m'appelle/أنا. null if absent.\n"
    "  Must be 1-3 words, a real person name, NOT a service or sentence.\n"
    "- Return ONLY the JSON object. No markdown fences, no explanations."
)

def extract_full_intent(message):
    """
    Use OpenAI to extract structured data from a single message.
    Returns dict: {services: [...], day: str|None, time: str|None, name: str|None}
    Never raises — returns empty structure on any error.
    """
    _empty = {"services": [], "day": None, "time": None, "name": None}
    if not message or len(message.strip()) < 3:
        return _empty
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":    "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": _FULL_INTENT_PROMPT},
                    {"role": "user",   "content": message},
                ],
                "temperature": 0,
            },
            timeout=12,
        )
        if resp.status_code != 200:
            print(f"[INTENT] OpenAI error status={resp.status_code}")
            return _empty
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if model wraps in them
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        out = {
            "services": result.get("services") or [],
            "day":      result.get("day"),
            "time":     result.get("time"),
            "name":     result.get("name"),
        }
        print(f"[INTENT] {out}")
        return out
    except Exception as _e:
        print(f"[INTENT] failed={repr(_e)}")
        return _empty

def is_valid_time(text):
    import re
    text = (text or "").strip().lower()
    if "الساعة" in text:
        return True
    if re.match(r"^\d{1,2}(:\d{2})?$", text):
        return True
    if any(x in text for x in ["am", "pm"]):
        return True
    return False

def is_valid_day(text):
    text = (text or "").strip().lower()
    valid_days = [
        "اليوم", "غدا", "غدًا",
        "today", "tomorrow",
        "aujourd'hui", "demain",
    ]
    return text in valid_days

def is_valid_name(text):
    text = (text or "").strip().lower()
    bad_words = [
        "je veux", "i want", "bonjour", "hello", "salam",
        "تنظيف", "تبييض", "فحص", "اريد", "أريد",
        "service", "nettoyage", "cleaning", "whitening", "checkup",
        "today", "tomorrow", "demain", "اليوم", "غدا", "غدًا",
    ]
    if any(w in text for w in bad_words):
        return False
    if len(text.split()) > 4:
        return False
    if len(text.strip()) < 2:
        return False
    return True

def sanitize_booking_field(text, max_len=40):
    if not text:
        return ""
    text = str(text).strip()
    return text[:max_len]

def build_confirmation(state, items, flow_type, client_config, lang, name):
    """Unified Confirmation Engine — returns formatted confirmation string.
    Fully dynamic: no hardcoded service/product names.
    Branches on flow_type: 'booking' | 'order' | 'mixed'.
    Supports per-item options (size/color) and quantity."""
    l   = lang if lang in ("ar", "en", "fr") else "ar"
    cur = client_config.get("currency", "SAR")
    _ids = [it["id"] for it in items] if items else []

    print(f"[CONFIRM_FLOW_TYPE] {flow_type!r}")
    print(f"[CONFIRM_ITEMS] {[it.get('title') for it in items]}")

    # ── Per-item quantity and options from state ──────────────────────────
    qty_global  = str(state.get("quantity") or state.get("known_quantity") or "").strip()
    known_opts  = state.get("known_options") or {}   # {str(catalog_id): {key: val}}

    def _fmt_item(it):
        title   = it.get("title", "?")
        price   = int(it.get("sale_price") or it.get("price") or 0)
        it_type = (it.get("type") or "service").lower()
        opts    = known_opts.get(str(it.get("id", ""))) or {}
        if opts:
            opts_str = " / ".join(f"{v}" for v in opts.values())
            label = f"• {title} ({opts_str}) — {price} {cur}"
        else:
            label = f"• {title} — {price} {cur}"
        if it_type == "product" and qty_global and qty_global not in ("", "1"):
            label += f" × {qty_global}"
        return label

    svc_items  = [it for it in items if (it.get("type") or "service").lower() == "service"]
    prod_items = [it for it in items if (it.get("type") or "service").lower() == "product"]
    fallback   = items or []

    # ── Total price ──────────────────────────────────────────────────────
    total     = calculate_total(CLIENT_ID, _ids) if _ids else 0.0
    total_str = f"{int(total)} {cur}"
    print(f"[CONFIRM_TOTAL] {total_str}")

    # ── i18n labels — 100% dynamic, nothing hardcoded outside this dict ──
    _L = {
        "hdr_booking": {"ar": "✅ تم تأكيد حجزك",          "en": "✅ Booking confirmed",                       "fr": "✅ Réservation confirmée"},
        "hdr_order":   {"ar": "✅ تم تأكيد طلبك",          "en": "✅ Order confirmed",                         "fr": "✅ Commande confirmée"},
        "hdr_mixed":   {"ar": "✅ تم تأكيد طلبك",          "en": "✅ Order confirmed",                         "fr": "✅ Commande confirmée"},
        "services":    {"ar": "الخدمات:",                  "en": "Services:",                                 "fr": "Services:"},
        "products":    {"ar": "المنتجات:",                 "en": "Products:",                                 "fr": "Produits:"},
        "total":       {"ar": "الإجمالي:",                 "en": "Total:",                                    "fr": "Total:"},
        "appointment": {"ar": "الموعد:",                   "en": "Appointment:",                              "fr": "Rendez-vous:"},
        "address":     {"ar": "عنوان التوصيل:",            "en": "Delivery address:",                         "fr": "Adresse de livraison:"},
        "name":        {"ar": "الاسم:",                    "en": "Name:",                                     "fr": "Nom:"},
        "close_bk":    {"ar": "نحن بانتظارك ⭐",           "en": "We look forward to seeing you ⭐",           "fr": "Nous avons hâte de vous accueillir ⭐"},
        "close_ord":   {"ar": "شكرًا على طلبك 🚀",         "en": "Thank you for your order 🚀",               "fr": "Merci pour votre commande 🚀"},
        "close_mix":   {"ar": "شكرًا! سنتواصل معك قريباً 🚀", "en": "Thanks! We'll be in touch soon 🚀",     "fr": "Merci! Nous vous contacterons bientôt 🚀"},
    }
    def lbl(key): return _L[key].get(l, _L[key]["ar"])

    day     = sanitize_booking_field(state.get("known_day"))
    time    = sanitize_booking_field(state.get("known_time"))
    address = (state.get("known_address") or state.get("address") or "").strip()
    parts   = []

    if flow_type == "booking":
        parts += [lbl("hdr_booking"), ""]
        parts += [lbl("services")] + [_fmt_item(it) for it in (svc_items or fallback)]
        if total > 0:
            parts += ["", f"{lbl('total')} {total_str}"]
        if day or time:
            parts.append(f"{lbl('appointment')} {day} {time}".strip())
        parts.append(f"{lbl('name')} {name}")
        parts += ["", lbl("close_bk")]

    elif flow_type == "order":
        parts += [lbl("hdr_order"), ""]
        parts += [lbl("products")] + [_fmt_item(it) for it in (prod_items or fallback)]
        parts += ["", f"{lbl('total')} {total_str}"]
        if address:
            parts.append(f"{lbl('address')} {address}")
        parts.append(f"{lbl('name')} {name}")
        parts += ["", lbl("close_ord")]

    else:   # mixed
        parts += [lbl("hdr_mixed"), ""]
        if svc_items:
            parts += [lbl("services")] + [_fmt_item(it) for it in svc_items]
        if prod_items:
            parts += [lbl("products")] + [_fmt_item(it) for it in prod_items]
        if not svc_items and not prod_items:
            parts += [_fmt_item(it) for it in fallback]
        parts += ["", f"{lbl('total')} {total_str}"]
        if day or time:
            parts.append(f"{lbl('appointment')} {day} {time}".strip())
        if address:
            parts.append(f"{lbl('address')} {address}")
        parts.append(f"{lbl('name')} {name}")
        parts += ["", lbl("close_mix")]

    return "\n".join(parts)


def confirmation_message(state, name, lang, phone=None):
    """Thin wrapper — resolves items/flow/client then delegates to build_confirmation()."""
    _ids         = json.loads(state.get("known_catalog_ids_json") or "[]")
    items        = get_catalog_items(CLIENT_ID, _ids)
    flow         = determine_flow_type(items)
    client_cfg   = get_client(CLIENT_ID)
    l            = lang if lang in ("ar", "en", "fr") else "ar"
    # Fallback item list from known_service if catalog IDs resolve nothing
    if not items:
        _svcs = ensure_svc_list(state.get("known_service"))
        items = [{"id": None, "title": s, "type": "service", "price": 0, "sale_price": None}
                 for s in _svcs]
        flow  = "booking"
    return build_confirmation(state, items, flow, client_cfg, l, name)

_RECOMMENDED_SERVICE = "تنظيف أسنان"

_UPSELL_MAP = {
    "تنظيف أسنان":   "تبييض الأسنان",
    "فحص الأسنان":   "تنظيف أسنان",
    "تبييض الأسنان": "فحص الأسنان",
}

def build_times_hint(svc, lang, day_offset=0, day=None):
    _day = (day or "اليوم").strip()
    _day_label = {"اليوم": {"ar": "اليوم", "en": "today", "fr": "aujourd'hui"},
                  "غدا":   {"ar": "غداً",  "en": "tomorrow", "fr": "demain"}}
    _dl = _day_label.get(_day, {}).get(lang, _day)

    priority = get_time_priority(_day)          # [(time, count), ...]
    available = [(t, c) for t, c in priority]  # all priority slots with counts

    urgency = len([t for t, c in available if c == 0]) == 1  # exactly 1 free slot

    if not available:
        _fallback = _ALL_TIMES[-2:]
        t1, t2 = _fallback[0], _fallback[1]
        _hints = {
            "ar": f"لدينا مواعيد {_dl} الساعة {t1} أو {t2}، أيهما يناسبك؟",
            "en": f"We have slots {_dl} at {t1} or {t2}. Which works best?",
            "fr": f"Nous avons des créneaux {_dl} à {t1} ou {t2}. Lequel vous convient?",
        }
        return _hints.get(lang, _hints["ar"])

    t1, c1 = available[0]
    if urgency and c1 == 0:
        _urgent = {
            "ar": f"بقي موعد أخير {_dl} الساعة {t1} 🔥\nأيهم يناسبك؟ 😊",
            "en": f"Only one slot left {_dl} at {t1} 🔥\nDoes that work for you? 😊",
            "fr": f"Il ne reste qu'un créneau {_dl} à {t1} 🔥\nCela vous convient? 😊",
        }
        return _urgent.get(lang, _urgent["ar"])

    if len(available) >= 2:
        t2, _ = available[1]
        _hints = {
            "ar": (f"لدينا موعد متاح {_dl} الساعة {t1} ⭐\n"
                   f"ويوجد أيضًا {t2} إذا رغبت\n"
                   f"أيهم يناسبك؟ 😊"),
            "en": (f"We have a slot {_dl} at {t1} ⭐\n"
                   f"Also available: {t2}\n"
                   f"Which works best? 😊"),
            "fr": (f"Nous avons un créneau {_dl} à {t1} ⭐\n"
                   f"Aussi disponible: {t2}\n"
                   f"Lequel vous convient? 😊"),
        }
    else:
        _hints = {
            "ar": f"لدينا موعد متاح {_dl} الساعة {t1} ⭐\nأيهم يناسبك؟ 😊",
            "en": f"We have a slot {_dl} at {t1} ⭐\nDoes that work? 😊",
            "fr": f"Nous avons un créneau {_dl} à {t1} ⭐\nCela vous convient? 😊",
        }
    return _hints.get(lang, _hints["ar"])

def _catalog_id_for_title(title):
    """Return catalog id for an exact title match, or None."""
    if not title:
        return None
    con = get_db_connection()
    try:
        row = con.execute(
            "SELECT id FROM catalogs WHERE client_id=? AND title=? AND is_active=1",
            (CLIENT_ID, title)
        ).fetchone()
    finally:
        con.close()
    return row["id"] if row else None

def build_upsell(svc, lang):
    _lang = lang if lang in ("ar", "en", "fr") else "ar"
    # ── DB-first upsell lookup ────────────────────────────────────────────
    cat_id = _catalog_id_for_title(svc)
    if cat_id:
        upsell_item = get_upsell_for_item(CLIENT_ID, cat_id)
        if upsell_item:
            uname = upsell_item["title"]
            print(f"[UPSELL] DB suggested={uname!r} for svc={svc!r}")
            _upsell = {
                "ar": f"وإذا رغبت، يمكن إضافة {uname} بعد ذلك لنتيجة أجمل 🌟",
                "en": f"If you'd like, you can also add {uname} afterwards for an even better result 🌟",
                "fr": f"Si vous le souhaitez, vous pouvez aussi ajouter {uname} après pour un résultat encore meilleur 🌟",
            }
            return _upsell[_lang]
    # ── Hardcoded fallback ────────────────────────────────────────────────
    upsell_svc = _UPSELL_MAP.get(svc)
    if not upsell_svc:
        return ""
    uname = svc_name(upsell_svc, lang)
    print(f"[UPSELL] hardcoded suggested={upsell_svc!r} for svc={svc!r}")
    _upsell = {
        "ar": f"وإذا رغبت، يمكن إضافة {uname} بعد ذلك لنتيجة أجمل 🌟",
        "en": f"If you'd like, you can also add {uname} afterwards for an even better result 🌟",
        "fr": f"Si vous le souhaitez, vous pouvez aussi ajouter {uname} après pour un résultat encore meilleur 🌟",
    }
    return _upsell[_lang]

_UPSELL_CANONICAL_MAP = {
    "تنظيف أسنان":   "تبييض الأسنان",
    "فحص الأسنان":   "تنظيف أسنان",
    "تبييض الأسنان": "فحص الأسنان",
}

def can_show_upsell(state):
    # ── PLAN ENFORCE: upsell is a pro / business feature ───────────────────
    print(f"[PLAN_ENFORCE] checking feature=upsell — client={CLIENT_ID}")
    if not has_feature(CLIENT_ID, "upsell"):
        print(f"[FEATURE_BLOCKED] upsell — client={CLIENT_ID} → upgrade required")
        return False

    step          = state.get("current_step", "service")
    svcs          = ensure_svc_list(state.get("known_service"))
    offered       = state.get("upsell_offered", False)
    rejected      = state.get("upsell_rejected", False)

    if offered:
        print("[UPSELL_CHECK] allowed=False (already offered)")
        return False
    if rejected:
        print("[UPSELL_CHECK] allowed=False (user rejected)")
        return False
    if step not in ("service", "day"):
        print(f"[UPSELL_CHECK] allowed=False (step={step!r} too late)")
        return False
    if not svcs:
        print("[UPSELL_CHECK] allowed=False (no service yet)")
        return False
    primary     = svcs[-1]
    upsell_svc  = _UPSELL_CANONICAL_MAP.get(primary)
    if not upsell_svc:
        print("[UPSELL_CHECK] allowed=False (no upsell mapping)")
        return False
    if upsell_svc in svcs:
        print("[UPSELL_CHECK] allowed=False (upsell already in cart)")
        return False
    print(f"[UPSELL_CHECK] allowed=True primary={primary!r} suggested={upsell_svc!r}")
    return True

_REJECTION_WORDS = {"لا", "no", "non", "لأ", "la", "nope", "ما أبي", "ما أريد", "not interested"}

def is_rejection(msg):
    return (msg or "").strip().lower() in _REJECTION_WORDS

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

def is_greeting(msg):
    msg = (msg or "").lower().strip()
    greetings = [
        "hello", "hi", "hey",
        "bonjour", "salut",
        "السلام عليكم", "السلام", "مرحبا", "أهلا", "اهلا",
    ]
    return any(g in msg for g in greetings)

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

def send_booking_messages(sender, state, name, lang):
    """SINGLE SEND POINT for all post-completion messages (WhatsApp Sales Engine).
    Sends exactly ONE message to the customer and ONE to the admin.
    This is the ONLY function allowed to send completion messages."""

    print(f"[TRACE] sending from send_booking_messages ONLY")
    print(f"[DEBUG_SEND_CHECK] sender={sender!r} name={name!r}")

    # ── Determine flow type from cart ────────────────────────────────────────
    _ids   = json.loads(state.get("known_catalog_ids_json") or "[]")
    items  = get_catalog_items(CLIENT_ID, _ids)
    flow   = determine_flow_type(items)
    print(f"[FLOW_TYPE] send_booking_messages flow={flow!r}")

    # ── 1. Build customer confirmation (flow-aware) ──────────────────────────
    customer_message = confirmation_message(state, name, lang, phone=None)

    # ── 2. Send to customer (sender) — ONLY the confirmation ────────────────
    print(f"[SEND_CUSTOMER] to={normalize_number(sender)!r}")
    print(f"[SEND_CUSTOMER] body={customer_message!r}")
    wa_reply(sender, customer_message)

    # ── 3. Build admin notification (flow-aware label) ───────────────────────
    _cur      = get_client(CLIENT_ID).get("currency", "SAR")
    if items:
        item_lines = "\n".join(
            f"  • {it['title']} — {int(it.get('sale_price') or it.get('price') or 0)} {_cur}"
            for it in items
        )
        total_str = f"{int(calculate_total(CLIENT_ID, _ids))} {_cur}"
    else:
        _svcs = ensure_svc_list(state.get("known_service"))
        item_lines = "\n".join(f"  • {s}" for s in _svcs) if _svcs else "  غير محدد"
        total_str = "-"

    if flow == "order":
        _admin_label = "📦 طلب جديد"
        _extra = f"العنوان: {state.get('known_address', '-')}"
    elif flow == "mixed":
        _admin_label = "🛒 طلب مختلط (خدمات + منتجات)"
        _extra = f"الموعد: {state.get('known_day', '')} {state.get('known_time', '')}".strip()
    else:
        _admin_label = "📥 حجز جديد"
        _extra = f"الموعد: {state.get('known_day', '')} {state.get('known_time', '')}".strip()

    admin_message = (
        f"{_admin_label}\n"
        f"الاسم: {name}\n"
        f"الرقم: {sender}\n"
        f"العناصر:\n{item_lines}\n"
        f"الإجمالي: {total_str}\n"
        f"{_extra}"
    ).strip()

    # ── 4. Send to admin — ONLY if admin number is different from sender ─────
    if not ADMIN_WHATSAPP_NUMBER or not ADMIN_WHATSAPP_NUMBER.strip():
        print("[SEND_ADMIN] SKIPPED — ADMIN_WHATSAPP_NUMBER not configured")
        return

    print(f"[ADMIN_RAW] {ADMIN_WHATSAPP_NUMBER!r}")
    _admin_to   = normalize_number(ADMIN_WHATSAPP_NUMBER.strip())
    _customer_n = normalize_number(sender)
    print(f"[ADMIN_FINAL] {_admin_to!r}")
    print(f"[CUSTOMER] {_customer_n!r}")

    if _admin_to == _customer_n:
        print(f"[SEND_ADMIN] SKIPPED — admin == customer ({_admin_to!r})")
        return

    # resp = ultramsg_send(_admin_to, admin_message)   # DISABLED FOR TEST
    print("[SEND_ADMIN] DISABLED FOR TEST")

_ALL_TIMES = [
    "09:00", "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00", "16:00",
    "17:00", "18:00", "19:00",
]

def get_top_times(times, limit=3):
    return times[:limit]

def normalize_time_input(msg):
    msg = msg.strip()
    mapping = {
        "الصباح": "09:00",
        "بدري":   "10:00",
        "الظهر":  "12:00",
        "العصر":  "16:00",
        "المغرب": "18:00",
        "المساء": "19:00",
        "الليل":  "20:00",
        "الساعة 9":  "09:00",
        "الساعة 10": "10:00",
        "الساعة 11": "11:00",
        "الساعة 12": "12:00",
        "الساعة 1":  "13:00",
        "الساعة 2":  "14:00",
        "الساعة 3":  "15:00",
        "الساعة 4":  "16:00",
        "الساعة 5":  "17:00",
        "الساعة 6":  "18:00",
        "الساعة 7":  "19:00",
        "9am": "09:00", "10am": "10:00", "11am": "11:00",
        "12pm": "12:00", "1pm": "13:00", "2pm": "14:00",
        "3pm": "15:00", "4pm": "16:00", "5pm": "17:00",
        "6pm": "18:00", "7pm": "19:00",
        "9h": "09:00", "10h": "10:00", "11h": "11:00",
        "12h": "12:00", "13h": "13:00", "14h": "14:00",
        "15h": "15:00", "16h": "16:00", "17h": "17:00",
        "18h": "18:00", "19h": "19:00",
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

_PRIORITY_TIMES = ["16:00", "17:00", "18:00"]

def get_time_priority(day):
    """Return _PRIORITY_TIMES sorted by booking count ascending (least booked first)."""
    day = (day or "اليوم").strip()
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT time FROM bookings WHERE time LIKE ?",
            (f"{day}%",)
        ).fetchall()
    finally:
        con.close()
    counts = {t: 0 for t in _PRIORITY_TIMES}
    for row in rows:
        slot = normalize_slot_text(row["time"])
        for pt in _PRIORITY_TIMES:
            if pt in slot:
                counts[pt] += 1
                break
    sorted_times = sorted(counts.items(), key=lambda x: x[1])
    print(f"[TIME_PRIORITY] day={day!r} counts={counts} sorted={[t for t,_ in sorted_times]}")
    return sorted_times   # list of (time, count)

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
    print("[SAVE_BOOKING] START")
    print(f"[SAVE_BOOKING] client_id={CLIENT_ID}")
    print(f"[SAVE_BOOKING] phone={phone}")
    print(f"[SAVE_BOOKING] name={name}")
    print(f"[SAVE_BOOKING] state={state}")

    # ── TRIAL CHECK ────────────────────────────────────────────────────────
    if expire_trial_if_needed(CLIENT_ID):
        print(f"[TRIAL_EXPIRED] client={CLIENT_ID} — booking aborted")
        return  # abort silently; WA already told user in message handler

    # ── PLAN ENFORCE: order limit ──────────────────────────────────────────
    print(f"[PLAN_ENFORCE] checking orders limit — client={CLIENT_ID}")
    _ord_ok, _ord_sub = check_plan_limit(CLIENT_ID, "orders")
    if not _ord_ok:
        _ord_plan = (_ord_sub or {}).get("plan_name", "Free")
        _ord_lim  = (_ord_sub or {}).get("max_orders", 10)
        print(f"[LIMIT_BLOCKED] orders — client={CLIENT_ID} plan={_ord_plan!r} limit={_ord_lim}")
        handle_limit_exceeded(CLIENT_ID, "orders")
        return  # abort save silently
    increment_usage(CLIENT_ID, "orders_used")

    day  = state.get("known_day")  or ""
    time = state.get("known_time") or ""

    # ── Resolve catalog items + total ─────────────────────────────────────
    _ids   = json.loads(state.get("known_catalog_ids_json") or "[]")
    _items = get_catalog_items(CLIENT_ID, _ids)
    _total = calculate_total(CLIENT_ID, _ids) if _ids else 0.0

    # Build items list — titles from catalog, fallback to known_service
    if _items:
        item_titles = [it.get("title", str(it.get("id", "?"))) for it in _items]
    else:
        item_titles = ensure_svc_list(state.get("known_service")) or []
    items_json = json.dumps(item_titles, ensure_ascii=False)

    print(f"[SAVE_BOOKING] ids={_ids} items={item_titles} total={_total} day={day!r} time={time!r}")

    # ── 1. INSERT into bookings_or_orders (primary admin table) ───────────
    con = get_db_connection()
    try:
        con.execute("""
            INSERT INTO bookings_or_orders
                (client_id, phone, customer_name, items_json,
                 day, time, total_price, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?)
        """, (
            CLIENT_ID,
            phone,
            name,
            items_json,
            day,
            time,
            _total,
            datetime.datetime.now().isoformat(),
        ))
        con.commit()
        print("[SAVE_BOOKING] SUCCESS — bookings_or_orders row committed")
    except Exception as e:
        print(f"[SAVE_BOOKING] ERROR bookings_or_orders: {repr(e)}")
        import traceback as _tb; _tb.print_exc()
    finally:
        con.close()

    # ── 2. Legacy bookings table (kept for backwards compat) ──────────────
    svc_str = " / ".join(item_titles) if item_titles else "غير محدد"
    slot    = f"{day} {time}".strip()
    con2 = get_db_connection()
    try:
        con2.execute(
            "INSERT INTO bookings (user_id, name, service, time, timestamp) VALUES (?, ?, ?, ?, ?)",
            (str(WHATSAPP_USER_ID), name, svc_str, slot, datetime.datetime.now().isoformat())
        )
        con2.commit()
        print("[SAVE_BOOKING] bookings (legacy) row committed")
    except Exception as e:
        print(f"[SAVE_BOOKING] ERROR bookings (legacy): {repr(e)}")
    finally:
        con2.close()

    # ── 3. orders table (secondary, independent transaction) ──────────────
    con3 = get_db_connection()
    try:
        con3.execute(
            "INSERT INTO orders (client_id, phone, name, items, scheduled, status) VALUES (?, ?, ?, ?, ?, 'confirmed')",
            (CLIENT_ID, phone, name, items_json, slot)
        )
        con3.commit()
        print("[SAVE_BOOKING] orders row committed")
    except Exception as e:
        print(f"[SAVE_BOOKING] ERROR orders: {repr(e)}")
    finally:
        con3.close()

    # ── Activation check (order saved → may trigger is_active=1) ──────────
    _check_activation(CLIENT_ID)

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


@app.route("/debug/auto-connect-status")
def debug_auto_connect_status():
    """Diagnostic endpoint — shows all state needed to debug the auto-connect flow."""
    import os as _os
    _con = get_db_connection()
    try:
        _pending_rows = _con.execute("""
            SELECT id, name, business_whatsapp_number, whatsapp_connection_status
            FROM clients
            WHERE whatsapp_connection_status = 'pending'
            ORDER BY id DESC
        """).fetchall()
        _pending_count  = len(_pending_rows)
        _latest_pending = dict(_pending_rows[0]) if _pending_rows else None
    finally:
        _con.close()

    _instance = _os.getenv("ULTRAMSG_INSTANCE", "")
    _token    = _os.getenv("ULTRAMSG_TOKEN", "")
    _bot_num  = _os.getenv("WA_BOT_NUMBER", "")

    return jsonify({
        "ultramsg_instance":    bool(_instance),
        "ultramsg_token":       bool(_token),
        "wa_bot_number_set":    bool(_bot_num),
        "webhook_url_expected": "https://filtrex-agent-1.replit.app/whatsapp",
        "pending_clients":      _pending_count,
        "latest_pending_client": _latest_pending,
    })


@app.route("/webhook/whatsapp", methods=["POST"])
def whatsapp_webhook():
    """Dedicated UltraMsg webhook endpoint.
    Configure in UltraMsg dashboard:
        https://filtrex-agent-1.replit.app/webhook/whatsapp
    Handles the START auto-connect flow only; all other chat traffic
    is forwarded internally to the main whatsapp() handler via the
    same logic.
    """
    import re as _wh_re
    data = request.get_json(force=True, silent=True) or {}
    print("[WEBHOOK RECEIVED]", data)

    try:
        # UltraMsg wraps payload inside a "data" key
        msg_data     = data.get("data", data)   # tolerate both flat and nested
        sender       = (msg_data.get("from") or msg_data.get("sender") or "").strip()
        message      = (msg_data.get("body") or msg_data.get("text")   or "").strip()
        msg_type     = msg_data.get("type", "chat")

        _from_me_raw = msg_data.get("fromMe", False)
        from_me      = _from_me_raw in (True, 1, "true", "1", "True")

        print(f"[WEBHOOK] sender={sender!r} msg={message!r} type={msg_type!r} fromMe={from_me!r}")

        if from_me:
            print("[WEBHOOK] ignored — outbound message")
            return "ignored", 200

        if not sender or not message:
            print("[WEBHOOK] ignored — empty sender or body")
            return "ignored", 200

        msg_upper = message.strip().upper()

        # ── AUTO-CONNECT ──────────────────────────────────────────────────────
        if msg_upper == "START" or msg_upper.startswith("START_"):
            _sender_norm   = normalize_number(sender)
            _sender_digits = _wh_re.sub(r'\D', '', sender)

            # Extract optional token
            _token = None
            if "_" in message:
                _token = message.strip().split("_", 1)[1].lower()
            print(f"[WEBHOOK AUTO-CONNECT] norm={_sender_norm!r} token={_token!r}")

            _con = get_db_connection()
            try:
                # Already linked?
                _existing = _con.execute(
                    "SELECT id FROM clients WHERE business_whatsapp_number=?",
                    (_sender_norm,)
                ).fetchone()
                if _existing:
                    print(f"[WEBHOOK AUTO-CONNECT] already linked to client={_existing['id']} — ignored")
                    return "ok", 200

                # Resolve target client via token or fallback
                _target_cid = None
                if _token:
                    _now_iso = datetime.datetime.utcnow().isoformat()
                    _tok_row = _con.execute("""
                        SELECT client_id FROM wa_connect_tokens
                        WHERE token=? AND used=0 AND expires_at > ?
                    """, (_token, _now_iso)).fetchone()
                    if _tok_row:
                        _target_cid = _tok_row["client_id"]
                        print(f"[WEBHOOK AUTO-CONNECT] token valid → client_id={_target_cid}")
                    else:
                        print(f"[WEBHOOK AUTO-CONNECT] token {_token!r} invalid or expired")

                if not _target_cid:
                    _pending = get_latest_pending_client()
                    if _pending:
                        _target_cid = _pending["id"]
                        print(f"[WEBHOOK AUTO-CONNECT] fallback to pending client_id={_target_cid}")

                if not _target_cid:
                    print("[WEBHOOK AUTO-CONNECT] no matching client — ignored")
                    return "ok", 200

                # Link number
                _con.execute("""
                    UPDATE clients
                    SET business_whatsapp_number=?,
                        whatsapp_connected=1,
                        whatsapp_connection_status='connected',
                        whatsapp_provider='ultramsg'
                    WHERE id=?
                """, (_sender_digits, _target_cid))

                # Mark token used
                if _token:
                    _con.execute(
                        "UPDATE wa_connect_tokens SET used=1 WHERE token=?",
                        (_token,)
                    )
                _con.commit()
                print(f"[AUTO CONNECT SUCCESS] client={_target_cid} number={_sender_digits!r}")

            finally:
                _con.close()

            # Confirmation + first-action nudge
            ultramsg_send(_sender_norm, (
                "✅ تم ربط واتساب بنجاح!\n\n"
                "📱 أرسل رسالة الآن إلى واتسابك وجرّب البوت فورًا."
            ))
            print(f"[USER_FIRST_ACTION] client={_target_cid} wa={_sender_norm!r}")
            return "ok", 200

        # Non-START messages: pass through to main handler
        return whatsapp()

    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
        return "error", 500


@app.route("/whatsapp/instance/<instance_id>", methods=["POST"])
def whatsapp_by_instance(instance_id):
    """Per-client webhook endpoint. Configure each client's UltraMsg instance
    to POST to /whatsapp/instance/<their_instance_id>."""
    _con = get_db_connection()
    try:
        _row = _con.execute(
            "SELECT client_id FROM whatsapp_instances WHERE instance_id=?", (instance_id,)
        ).fetchone()
    finally:
        _con.close()
    if _row:
        g.wa_client_id = _row["client_id"]
        print(f"[WA_INSTANCE_ROUTE] instance={instance_id!r} → client={g.wa_client_id}")
    else:
        print(f"[WA_INSTANCE_ROUTE] unknown instance={instance_id!r} → fallback client=1")
        g.wa_client_id = 1
    return whatsapp()


@app.route("/whatsapp", methods=["POST"])
def whatsapp():
    print("🔥 WHATSAPP ROUTE HIT")
    try:
        # Per-request client ID: set by whatsapp_by_instance() or defaults to global CLIENT_ID
        _WH_CID    = getattr(g, 'wa_client_id', CLIENT_ID)
        _wh_client = get_client(_WH_CID)   # loaded early — used across all intent branches
        # Load active catalog once per request — passed to every openai_chat call
        _ai_catalog = load_catalog_for_ai(_WH_CID)
        data = request.get_json(force=True, silent=True) or {}
        print(f"[TRACE_PAYLOAD] {data}")          # full dump — reveals UltraMsg echo payloads
        msg_data     = data.get("data", {})
        sender       = msg_data.get("from", "").strip()
        incoming_msg = msg_data.get("body", "").strip()
        msg_type     = msg_data.get("type", "")

        # ── Harden fromMe: UltraMsg may send True, 1, "true", "1" ────────────
        _from_me_raw = msg_data.get("fromMe", False)
        from_me = _from_me_raw in (True, 1, "true", "1", "True")

        print(f"[WHATSAPP] sender={sender!r} message={incoming_msg!r} type={msg_type!r} fromMe_raw={_from_me_raw!r} fromMe={from_me!r}")
        print("[AUTO_CONNECT_DEBUG] route=/whatsapp hit")
        print(f"[AUTO_CONNECT_DEBUG] sender={sender!r}")
        print(f"[AUTO_CONNECT_DEBUG] incoming_msg={incoming_msg!r}")
        print(f"[AUTO_CONNECT_DEBUG] fromMe={from_me!r}")

        # ── Ignore outbound messages the bot itself sent ─────────────────────
        if from_me:
            print(f"[WHATSAPP] IGNORED outbound (fromMe={_from_me_raw!r}) — body={incoming_msg!r}")
            return "", 200

        if msg_type != "chat" or not sender or not incoming_msg:
            print("[WHATSAPP] ignored non-chat or empty message")
            return "", 200

        # Track every valid inbound message
        track_event(_WH_CID, "message_received", {"sender": sender, "len": len(incoming_msg)})
        print(f"[WHATSAPP_MESSAGE_RECEIVED] client={_WH_CID} sender={sender!r} msg={incoming_msg!r}")

        # ── AUTO-CONNECT: handle "START" or "START_<token>" ──────────────
        _msg_upper = incoming_msg.strip().upper()
        if _msg_upper == "START" or _msg_upper.startswith("START_"):
            print("[AUTO_CONNECT_START_RECEIVED]")

            pending = get_latest_pending_whatsapp_client()
            print(f"[AUTO_CONNECT_PENDING_CLIENT] {pending}")

            if not pending:
                wa_reply(sender, "لم نجد طلب ربط قيد الانتظار. افتح لوحة التحكم واضغط ربط واتساب أولاً.")
                return "", 200

            mark_client_whatsapp_connected(client_id=pending["id"], phone=sender)
            print(f"[AUTO_CONNECT_SUCCESS] client_id={pending['id']} phone={sender!r}")

            wa_reply(sender, (
                "✅ تم ربط واتساب بنجاح!\n\n"
                "📱 أرسل رسالة الآن إلى واتسابك وجرّب البوت فورًا."
            ))
            print(f"[USER_FIRST_ACTION] client={pending['id']} wa={sender!r}")
            return "", 200

        # ── FIRST ENGAGEMENT — log + queue nudge for first real message ────
        _eg_con = get_db_connection()
        try:
            _eg_row = _eg_con.execute(
                "SELECT id FROM analytics_events WHERE client_id=? AND event_name='first_reply_nudge' LIMIT 1",
                (_WH_CID,)
            ).fetchone()
            if not _eg_row:
                print(f"[USER_ENGAGED] client={_WH_CID} first real message from={sender!r}")
                _fire_first_reply_nudge(sender, _WH_CID)
        finally:
            _eg_con.close()

        # ── TRIAL CHECK ────────────────────────────────────────────────────
        if expire_trial_if_needed(_WH_CID):
            print(f"[TRIAL_EXPIRED] client={_WH_CID} blocked WA reply from sender={sender!r}")
            _trial_msg = (
                "🚫 انتهت التجربة المجانية — اشترك الآن للاستمرار 👇\n"
                "https://filtrex.ai/pay/subscription\n\n"
                "🚫 Free trial expired — subscribe now to continue 👇"
            )
            return wa_reply(sender, _trial_msg)

        # ── PLAN ENFORCE: message limit ────────────────────────────────────
        print(f"[PLAN_ENFORCE] checking messages limit — client={_WH_CID}")
        _msg_allowed, _msg_sub = check_plan_limit(_WH_CID, "messages")
        if not _msg_allowed:
            _plan_n = (_msg_sub or {}).get("plan_name", "Free")
            _used   = (_msg_sub or {}).get("messages_used", 0)
            _limit  = (_msg_sub or {}).get("max_messages", 100)
            print(f"[LIMIT_BLOCKED] messages — client={_WH_CID} plan={_plan_n!r} used={_used}/{_limit}")
            _pw = handle_limit_exceeded(_WH_CID, "messages")
            _paywall_msg = (
                f"{_pw['message_ar']} قم بالترقية للاستمرار 👇\n"
                f"https://filtrex.ai/pay/subscription\n\n"
                f"{_pw['message_en']} Upgrade to continue 👇"
            )
            return wa_reply(sender, _paywall_msg)
        increment_usage(_WH_CID, "messages_used")

        state = wa_load(sender)
        _step_early = state.get("current_step", "service")

        print(f"[FINAL STATE] {state}")
        print(f"[COMPLETED] step={_step_early!r} is_done={_step_early == 'done'}")

        # ── COMPLETED LOCK — booking already done, offer new booking ─────────
        if state.get("completed") or _step_early == "done":
            _cl = state.get("lang") or "ar"
            _new_booking_q = {
                "ar": "هل ترغب في حجز جديد؟",
                "en": "Would you like to make a new booking?",
                "fr": "Souhaitez-vous faire une nouvelle réservation?",
            }
            print(f"[COMPLETED] completed={state.get('completed')} step={_step_early!r} — offering new booking")
            return wa_reply(sender, _new_booking_q.get(_cl, _new_booking_q["ar"]))

        if is_noise_message(incoming_msg) and _step_early != "service":
            print(f"[NOISE] ignored mid-booking greeting at step={_step_early!r}")
            return "", 200

        # ── Language detection (shared across all branches below) ────────────
        _early_lang = state.get("lang") or detect_lang(incoming_msg) or "ar"

        # ── CATALOG QUERY HANDLER — generic, AI-driven, no hardcoded terms ──────
        # Triggers on universal availability/price/inquiry keywords.
        # AI receives the full structured catalog and decides what matches and
        # how to answer — works for any business type.
        _msg_lower_ck = incoming_msg.lower()
        _catalog_triggered = any(kw in _msg_lower_ck for kw in _CATALOG_TRIGGER_WORDS)
        if _catalog_triggered:
            print(f"[CATALOG_LOADED] query detected — client={_WH_CID} catalog_size={len(_ai_catalog)}")

            # Persist intent so this session is never re-classified as greeting
            if not state.get("msg_intent"):
                state["msg_intent"] = "ask_catalog"
                wa_save(sender, state)

            # Empty catalog guard
            if not _ai_catalog:
                _empty_msgs = {
                    "ar": "لا توجد منتجات أو خدمات مضافة حاليًا في الكتالوج.",
                    "en": "No products or services are currently available in our catalog.",
                    "fr": "Aucun produit ou service n'est actuellement disponible dans notre catalogue.",
                }
                _empty_reply = _empty_msgs.get(_early_lang, _empty_msgs["ar"])
                print(f"[EMPTY_REPLY_GUARD] empty catalog — client={_WH_CID}")
                print(f"[CATALOG_RESPONSE_SENT] empty-catalog guard — client={_WH_CID}")
                return wa_reply(sender, _empty_reply)

            # AI decides: which item matches, what price to show, how to reply
            # No niche-specific matching — catalog context is fully generic
            print(f"[GENERIC_CATALOG_AI_MATCH] routing to AI — client={_WH_CID} items={len(_ai_catalog)}")
            _cat_reply = openai_chat(
                incoming_msg, lang=_early_lang,
                client_obj=_wh_client, catalog_items=_ai_catalog
            )

            # Empty-response guard — never return blank
            if not (_cat_reply or "").strip():
                _fb = (((_wh_client or {}).get("fallback_message")) or "").strip()
                _cat_reply = _fb or _empty_msgs.get(_early_lang, _empty_msgs["ar"])
                print(f"[EMPTY_REPLY_GUARD] AI returned empty — using fallback — client={_WH_CID}")

            print(f"[CATALOG_RESPONSE_SENT] client={_WH_CID} reply_preview={(_cat_reply or '')[:100]!r}")
            return wa_reply(sender, _cat_reply)

        # ── INTENT DETECTION — only on fresh conversation start ───────────────
        if _step_early == "service" and not state.get("msg_intent"):
            _msg_intent = detect_message_intent(incoming_msg, lang=_early_lang)
            state["msg_intent"] = _msg_intent
            track_event(_WH_CID, "intent_detected", {"intent": _msg_intent, "lang": _early_lang})

            _greeting_map = {
                "ar": "أهلاً! كيف يمكنني مساعدتك اليوم؟",
                "en": "Hello! How can I help you today?",
                "fr": "Bonjour! Comment puis-je vous aider aujourd'hui?",
                "es": "¡Hola! ¿Cómo puedo ayudarte hoy?",
                "it": "Ciao! Come posso aiutarti oggi?",
            }

            wa_save(sender, state)   # persist msg_intent before any early return

            if _msg_intent == "greeting":
                # Only send the greeting once — if already greeted, process with AI instead
                if state.get("has_greeted"):
                    print(f"[INTENT_FLOW] greeting repeated — routing to AI (already greeted)")
                    print(f"[AI_CALLED] repeated greeting → AI fallback client={_WH_CID}")
                    _g_ai = openai_chat(incoming_msg, lang=_early_lang,
                                        client_obj=_wh_client, catalog_items=_ai_catalog)
                    print(f"[AI_RESPONSE_SENT] client={_WH_CID}")
                    return wa_reply(sender, _g_ai)
                print(f"[INTENT_FLOW] greeting → send welcome (first time)")
                state["has_greeted"] = True
                wa_save(sender, state)
                _wc = (_wh_client or {}).get("default_language") or _early_lang
                _gr = _greeting_map.get(_wc, _greeting_map["ar"])
                print(f"[AI_RESPONSE_SENT] greeting client={_WH_CID}")
                return wa_reply(sender, _gr)

            elif _msg_intent == "ask_price":
                print(f"[INTENT_FLOW] ask_price → catalog")
                print(f"[CATALOG_LOADED] ask_price branch client={_WH_CID} items={len(_ai_catalog)}")
                if _ai_catalog:
                    _lines = []
                    for _pr in _ai_catalog:
                        _sp = _pr.get("sale_price")
                        _pp = _pr.get("price") or 0
                        _cu = (_pr.get("currency") or "").strip()
                        if _sp and float(_sp) > 0:
                            _lines.append(f"• {_pr['title']}: {_pp} {_cu} (عرض: {_sp} {_cu})")
                        else:
                            _lines.append(f"• {_pr['title']}: {_pp} {_cu}")
                    _price_hdr = {
                        "ar": "أسعارنا 💎", "en": "Our prices 💎",
                        "fr": "Nos prix 💎", "es": "Nuestros precios 💎",
                    }
                    _price_msg = _price_hdr.get(_early_lang, _price_hdr["ar"]) + "\n" + "\n".join(_lines)
                    print(f"[AI_RESPONSE_SENT] ask_price catalog client={_WH_CID} items={len(_ai_catalog)}")
                else:
                    print(f"[AI_CALLED] ask_price no catalog → AI client={_WH_CID}")
                    _price_msg = openai_chat(incoming_msg, lang=_early_lang,
                                             client_obj=_wh_client, catalog_items=_ai_catalog)
                    print(f"[AI_RESPONSE_SENT] ask_price AI client={_WH_CID}")
                return wa_reply(sender, _price_msg)

            elif _msg_intent == "ask_info":
                print(f"[INTENT_FLOW] ask_info → AI with catalog client={_WH_CID} items={len(_ai_catalog)}")
                print(f"[AI_CALLED] ask_info client={_WH_CID}")
                _info_reply = openai_chat(incoming_msg, lang=_early_lang,
                                          client_obj=_wh_client, catalog_items=_ai_catalog)
                print(f"[AI_RESPONSE_SENT] ask_info client={_WH_CID}")
                return wa_reply(sender, _info_reply)

            elif _msg_intent in ("book_appointment", "place_order"):
                create_intent_order(_WH_CID, sender, _msg_intent)
                print(f"[FLOW_STARTED] client={_WH_CID} phone={sender!r} intent={_msg_intent!r}")
                flow_save(_WH_CID, sender, "ask_day", {}, lang=_early_lang)
                return wa_reply(sender, _fs("ask_day", _early_lang))

        # ── CONVERSATION FLOW ENGINE — highest priority ───────────────────────
        # If the customer is already inside a booking flow, route there immediately.
        _active_flow = flow_load(_WH_CID, sender)
        if _active_flow:
            print(f"[FLOW_STEP] active flow detected step={_active_flow['current_step']!r}")
            _flow_lang = _active_flow.get("lang") or _early_lang
            return run_booking_flow(sender, incoming_msg, _WH_CID, _flow_lang, _active_flow)

        # ── STEP 11: CATALOG MATCH → known_catalog_ids_json ───────────────
        # _early_lang already set above
        _cat_match = find_catalog_match(_WH_CID, incoming_msg, lang=_early_lang)
        if _cat_match:
            print(f"[CATALOG_MATCH] {_cat_match}")
            _ids = json.loads(state.get("known_catalog_ids_json") or "[]")
            if _cat_match["id"] not in _ids:
                _ids.append(_cat_match["id"])
                state["known_catalog_ids_json"] = json.dumps(_ids)
                wa_save(sender, state)
                print(f"[CATALOG_IDS] updated ids={_ids}")

        # ── FULL INTENT EXTRACTION (multi-field, any language) ─────────────
        # Runs on every message — merges services, day, time, name into state
        # regardless of current step. Enables one-shot booking.
        _DAY_NORM = {"today": "اليوم", "tomorrow": "غدا"}
        state["known_service"] = ensure_svc_list(state.get("known_service"))
        _intent = extract_full_intent(incoming_msg)
        _intent_changed = False

        # Merge services: match each extracted phrase against catalog
        for _svc_phrase in (_intent.get("services") or []):
            _svc_match = find_catalog_match(_WH_CID, _svc_phrase, lang=_early_lang)
            if not _svc_match:
                # Retry with full message to handle phrase variation
                _svc_match = find_catalog_match(_WH_CID, incoming_msg, lang=_early_lang)
            if not _svc_match:
                # Last resort: reverse LIKE — find alias that CONTAINS any word from phrase
                _con_r = get_db_connection()
                try:
                    for _word in sorted(_svc_phrase.split(), key=len, reverse=True):
                        if len(_word) >= 3:
                            _row_r = _con_r.execute("""
                                SELECT DISTINCT c.* FROM catalogs c
                                JOIN catalog_aliases a ON a.catalog_id=c.id
                                WHERE c.client_id=? AND a.lang=? AND c.is_active=1
                                  AND a.alias LIKE ?
                                ORDER BY LENGTH(a.alias) DESC LIMIT 1
                            """, (_WH_CID, _early_lang, f"%{_word}%")).fetchone()
                            if _row_r:
                                _svc_match = dict(_row_r)
                                print(f"[INTENT_MERGE] reverse-LIKE matched {_word!r} → {_svc_match['title']!r}")
                                break
                finally:
                    _con_r.close()
            if _svc_match:
                _svc_title = _svc_match["title"]
                if _svc_title not in state["known_service"]:
                    state["known_service"].append(_svc_title)
                    _intent_changed = True
                    print(f"[INTENT_MERGE] service added={_svc_title!r}")
                _i_ids = json.loads(state.get("known_catalog_ids_json") or "[]")
                if _svc_match["id"] not in _i_ids:
                    _i_ids.append(_svc_match["id"])
                    state["known_catalog_ids_json"] = json.dumps(_i_ids)
                    _intent_changed = True

        # Merge day
        _i_day = _intent.get("day")
        if _i_day and _i_day in _DAY_NORM and not state.get("known_day"):
            state["known_day"] = _DAY_NORM[_i_day]
            _intent_changed = True
            print(f"[INTENT_MERGE] day={state['known_day']!r}")

        # Merge time
        _i_time = _intent.get("time")
        if _i_time and not state.get("known_time"):
            _i_time_norm = normalize_time_input(str(_i_time))
            if is_valid_time(_i_time_norm):
                state["known_time"] = _i_time_norm
                _intent_changed = True
                print(f"[INTENT_MERGE] time={_i_time_norm!r}")

        # Merge name — unconditional (no step restriction)
        _i_name = (_intent.get("name") or "").strip()
        if _i_name and not state.get("known_name") and is_valid_name(_i_name):
            state["known_name"] = _i_name
            _intent_changed = True
            print(f"[INTENT_MERGE] name={_i_name!r}")

        if _intent_changed:
            wa_save(sender, state)
        print(f"[STATE AFTER MERGE] step={state.get('current_step')!r} "
              f"services={state.get('known_service')} day={state.get('known_day')!r} "
              f"time={state.get('known_time')!r} name={state.get('known_name')!r} "
              f"ids={state.get('known_catalog_ids_json')}")

        # ── LLM PARSE ─────────────────────────────────────────────────────
        _parsed = parse_user_message(incoming_msg, lang=state.get("lang") or "ar")
        _DAY_NORM = {"today": "اليوم", "tomorrow": "غدا"}
        state["known_service"] = ensure_svc_list(state.get("known_service"))
        _changed = False

        _p_svc   = _parsed.get("service")
        _p_addon = _parsed.get("add_on_service")
        _p_day   = _parsed.get("day")
        _p_time  = _parsed.get("time")
        _p_name  = _parsed.get("name")
        _parsed_affirmation = bool(_parsed.get("affirmation"))
        _parsed_rejection   = bool(_parsed.get("rejection"))

        # Resolve service from catalog (generic — any business type)
        if _p_svc:
            _p_svc_match = find_catalog_match(_WH_CID, _p_svc, lang=_early_lang)
            _resolved_svc = _p_svc_match["title"] if _p_svc_match else _CANONICAL_SERVICE_MAP.get(_p_svc, _p_svc)
            if not state["known_service"]:
                state["known_service"] = [_resolved_svc]
                _changed = True
                print(f"[STATE_MERGE] set service={_resolved_svc!r}")

        if _p_addon:
            _p_addon_match = find_catalog_match(_WH_CID, _p_addon, lang=_early_lang)
            _resolved_addon = _p_addon_match["title"] if _p_addon_match else _CANONICAL_SERVICE_MAP.get(_p_addon, _p_addon)
            if _resolved_addon not in state["known_service"]:
                state["known_service"].append(_resolved_addon)
                _changed = True
                print(f"[STATE_MERGE] appended add_on={_resolved_addon!r}")

        if _p_day in ("today", "tomorrow") and not state.get("known_day"):
            state["known_day"] = _DAY_NORM[_p_day]
            _changed = True
            print(f"[STATE_MERGE] set day={state['known_day']!r}")

        if _p_time and not state.get("known_time"):
            _norm_t = normalize_time_input(_p_time)
            if is_valid_time(_norm_t):
                state["known_time"] = _norm_t
                _changed = True
                print(f"[STATE_MERGE] set time={_norm_t!r}")

        if _p_name and not state.get("known_name"):
            if is_valid_name(_p_name):
                state["known_name"] = _p_name
                _changed = True
                print(f"[STATE_MERGE] set name={_p_name!r}")

        if _changed:
            wa_save(sender, state)
            print(f"[STATE_MERGE] updated_state={state}")

        # ── REGEX + CATALOG FALLBACK (only when LLM found nothing) ───────
        _parser_found = any([_p_svc, _p_addon, _p_day, _p_time])
        if not _parser_found:
            _e_svc, _e_day, _e_time = extract_entities(incoming_msg)
            _re_changed = False
            # Catalog alias match as extra fallback for service
            if not _e_svc:
                _cat_match = find_catalog_match(_WH_CID, incoming_msg,
                                                lang=state.get("lang") or "ar")
                if _cat_match:
                    _e_svc = _cat_match["title"]
            if _e_svc:
                _arabic_svc = _CANONICAL_SERVICE_MAP.get(_e_svc, _e_svc)
                _cur_svcs   = state["known_service"]
                if is_add_intent(incoming_msg):
                    if _arabic_svc not in _cur_svcs:
                        _cur_svcs.append(_arabic_svc)
                        state["known_service"] = _cur_svcs
                        _re_changed = True
                        print(f"[ENTITY_EXTRACT] appended svc={_arabic_svc!r} list={_cur_svcs!r}")
                elif not _cur_svcs:
                    state["known_service"] = [_arabic_svc]
                    _re_changed = True
                    print(f"[ENTITY_EXTRACT] set svc={_arabic_svc!r}")
            if _e_day and not state.get("known_day"):
                state["known_day"] = _DAY_NORM.get(_e_day, _e_day)
                _re_changed = True
            if _e_time and not state.get("known_time"):
                state["known_time"] = _e_time
                _re_changed = True
            if _re_changed:
                wa_save(sender, state)
                print(f"[ENTITY_EXTRACT] merged day={_e_day!r} time={_e_time!r}")

        step  = state["current_step"]

        old_lang     = state.get("lang") or ""
        _wh_client   = get_client(_WH_CID)
        new_lang     = detect_customer_language(incoming_msg)   # None if not confident
        print(f"[LANG_DETECT] detected={new_lang!r} stored={old_lang!r}")

        if new_lang and new_lang != old_lang:
            print(f"[LANG_SWITCH] old={old_lang!r} new={new_lang!r} sender={sender!r}")
            state["lang"] = new_lang
            wa_save(sender, state)

        lang = state.get("lang") or new_lang or _wh_client.get("default_language") or "ar"
        print(f"[CLIENT_DEFAULT_LANGUAGE] {_wh_client.get('default_language')!r}")
        print(f"[REPLY_LANGUAGE] detected={new_lang!r} client_default={_wh_client.get('default_language')!r} → using={lang!r}")
        print(f"[LANG_FINAL] using={lang!r}")
        print(f"[WHATSAPP] step={step!r} lang={lang!r}")

        print(f"[FLOW] current_step={step!r}")

        # ── GREETING — only act on pure greetings that haven't been greeted yet ──
        # Guard: if user was already greeted, never send welcome again
        if is_greeting(incoming_msg) and not state.get("has_greeted"):
            _state_has_data = bool(
                ensure_svc_list(state.get("known_service")) or
                state.get("known_day") or state.get("known_time") or state.get("known_name")
            )
            _intent_has_data = bool(
                _intent.get("services") or _intent.get("day") or
                _intent.get("time")     or _intent.get("name")
            )
            if _state_has_data:
                print(f"[GREETING] skipping reset — state has existing data (guard)")
            elif _intent_has_data:
                print(f"[GREETING] skipping reset — intent has data={_intent}")
            elif step == "service":
                print(f"[GREETING] pure greeting — resetting state for sender={sender!r}")
                state["has_greeted"] = True
                wa_save(sender, state)
                wa_clear(sender)
                print(f"[AI_RESPONSE_SENT] greeting/service-ask client={_WH_CID}")
                return wa_reply(sender, build_ask_service(_WH_CID, lang))
            else:
                _ask_map = {
                    "day":     "Ask the user for the appointment day (today or tomorrow only).",
                    "time":    "Ask the user for the appointment time (example: 16:00).",
                    "name":    "Ask the user for their name to complete the booking.",
                    "confirm": "Ask the user to confirm their booking (yes or no).",
                }
                _ask = _ask_map.get(step, "Ask the user what service they need.")
                print(f"[FLOW] asking_for={step!r} (after mid-booking greeting)")
                print(f"[AI_CALLED] mid-booking greeting re-prompt client={_WH_CID}")
                _g2 = openai_chat(_ask, lang=lang, client_obj=_wh_client, catalog_items=_ai_catalog)
                print(f"[AI_RESPONSE_SENT] client={_WH_CID}")
                return wa_reply(sender, _g2)

        # ── UPSELL REJECTION DETECTION ────────────────────────────────────
        if state.get("upsell_offered") and not state.get("upsell_rejected") and (is_rejection(incoming_msg) or _parsed_rejection):
            state["upsell_rejected"] = True
            wa_save(sender, state)
            print(f"[UPSELL_REJECTED] sender={sender!r}")
            _ask_map = {
                "day":     "Ask the user for the appointment day (today or tomorrow only).",
                "time":    "Ask the user for the appointment time (example: 16:00).",
                "name":    "Ask the user for their name to complete the booking.",
                "confirm": "Ask the user to confirm their booking (yes or no).",
            }
            _ask = _ask_map.get(step, "Ask the user what service they need.")
            return wa_reply(sender, openai_chat(_ask, lang=lang, client_obj=_wh_client, catalog_items=_ai_catalog))

        # ── ONE-SHOT SHORTCUT — all fields present → go directly to confirmation
        _sc_svcs = ensure_svc_list(state.get("known_service"))
        _sc_day  = state.get("known_day")
        _sc_time = state.get("known_time")
        _sc_name = state.get("known_name")
        # Resolve current cart items + flow type for shortcut decision
        _sc_ids_pre  = json.loads(state.get("known_catalog_ids_json") or "[]")
        _sc_items    = get_catalog_items(_WH_CID, _sc_ids_pre) if _sc_ids_pre else []
        _sc_flow     = determine_flow_type(_sc_items)
        print(f"[FLOW_TYPE] shortcut check flow={_sc_flow!r}")
        # Services/mixed require day+time; products require only name
        _sc_needs_appt = _sc_flow in ("booking", "mixed")
        _sc_ready = bool(
            _sc_svcs and _sc_name and
            (not _sc_needs_appt or (_sc_day and _sc_time))
        )
        if _sc_ready:
            print(f"[SHORTCUT] all required fields complete — skipping step flow")
            print(f"[SHORTCUT] flow={_sc_flow!r} services={_sc_svcs} day={_sc_day!r} time={_sc_time!r} name={_sc_name!r}")
            # Ensure catalog IDs are merged for all items in cart
            _sc_ids = json.loads(state.get("known_catalog_ids_json") or "[]")
            for _sv in _sc_svcs:
                _sv_id = _catalog_id_for_title(_sv)
                if _sv_id and _sv_id not in _sc_ids:
                    _sc_ids.append(_sv_id)
            state["known_catalog_ids_json"] = json.dumps(_sc_ids)
            state["current_step"] = "done"
            state["completed"]    = True
            wa_save_booking(sender, state, _sc_name)
            wa_save(sender, state)
            print(f"[STATE_COMPLETED] True")
            print(f"[FINAL STATE] {state}")
            send_booking_messages(sender, state, _sc_name, lang)
            return "", 200

        # ── STEP: service ─────────────────────────────────────────────────
        if step == "service":
            # Prefer catalog match from STEP 11 over hardcoded detect_wa_service
            _cat_item = _cat_match  # set by STEP 11 block above (or None)
            svc = _cat_item["title"] if _cat_item else detect_wa_service(incoming_msg)

            if svc:
                _cur_svcs = ensure_svc_list(state.get("known_service"))
                if is_add_intent(incoming_msg):
                    if svc not in _cur_svcs:
                        _cur_svcs.append(svc)
                else:
                    _cur_svcs = [svc]
                state["known_service"] = _cur_svcs
                print(f"[ENTITY_EXTRACT] service list={_cur_svcs!r} (from_catalog={bool(_cat_item)})")

                # ── Merge catalog IDs from all svcs in cart ────────────────
                _ids_set = json.loads(state.get("known_catalog_ids_json") or "[]")
                for _sv in _cur_svcs:
                    _sv_id = _catalog_id_for_title(_sv)
                    if _sv_id and _sv_id not in _ids_set:
                        _ids_set.append(_sv_id)
                state["known_catalog_ids_json"] = json.dumps(_ids_set)

                # ── Required Fields Engine ────────────────────────────────
                _svc_items_now = get_catalog_items(_WH_CID, _ids_set)
                _svc_flow      = determine_flow_type(_svc_items_now)
                _req_fields    = get_required_fields(_svc_flow, _svc_items_now)
                _miss_fields   = get_missing_fields(state, _req_fields)
                _next_field    = _miss_fields[0] if _miss_fields else None
                _next_step     = _FIELD_TO_STEP.get(_next_field, "name") if _next_field else "done"
                state["current_step"] = _next_step
                print(f"[FLOW_TYPE]       {_svc_flow!r}")
                print(f"[REQUIRED_FIELDS] {_req_fields}")
                print(f"[MISSING_FIELDS]  {_miss_fields}")
                print(f"[ASKING_FOR]      {_next_field!r} → step={_next_step!r}")
                wa_save(sender, state)

                # ── Build item-confirmed reply using catalog data ──────────
                _primary = _cur_svcs[-1]
                _cur     = get_client(_WH_CID).get("currency", "SAR")
                if _cat_item:
                    _p_raw   = _cat_item.get("sale_price") or _cat_item.get("price") or 0
                    _price   = f"{int(_p_raw)} {_cur}"
                    _benefit = _cat_item.get("description") or svc_benefit(_primary, lang)
                else:
                    _price   = svc_price(_primary, lang)
                    _benefit = svc_benefit(_primary, lang)

                # Multi-item: show all items + total when cart has >1 item
                if len(_cur_svcs) > 1:
                    _all_items = get_catalog_items(_WH_CID, _ids_set)
                    if _all_items:
                        _list_lines = "\n".join(
                            f"• {it['title']} — {int(it.get('sale_price') or it.get('price') or 0)} {_cur}"
                            for it in _all_items
                        )
                        _total = calculate_total(_WH_CID, _ids_set)
                        _cart_hdrs = {
                            "ar": f"تم إضافة {svc} ✨\nسلة طلباتك:\n{_list_lines}\n\nالإجمالي: {int(_total)} {_cur}",
                            "en": f"Added {svc} ✨\nYour cart:\n{_list_lines}\n\nTotal: {int(_total)} {_cur}",
                            "fr": f"{svc} ajouté ✨\nVotre panier:\n{_list_lines}\n\nTotal: {int(_total)} {_cur}",
                        }
                        reply = _cart_hdrs[lang if lang in ("ar","en","fr") else "ar"]
                    else:
                        reply = _bot_str("service_confirmed", lang).format(
                            svc=format_svcs(_cur_svcs, lang),
                            price=_price,
                            benefit=_benefit,
                        )
                else:
                    reply = _bot_str("service_confirmed", lang).format(
                        svc=svc,
                        price=_price,
                        benefit=_benefit,
                    )

                # Append times hint only when service/mixed flow needs day next
                if _next_field == "known_day" and _svc_flow in ("booking", "mixed"):
                    reply += "\n" + build_times_hint(_primary, lang, day=state.get("known_day"))

                # ── Upsell from catalog (DB-first, no hardcoded map) ───────
                if can_show_upsell(state):
                    _pid     = _cat_item["id"] if _cat_item else _catalog_id_for_title(_primary)
                    _upsell_item = get_upsell_for_item(_WH_CID, _pid) if _pid else None
                    if _upsell_item:
                        _uname    = _upsell_item["title"]
                        _up_price = _upsell_item.get("sale_price") or _upsell_item.get("price") or 0
                        _up_hdrs  = {
                            "ar": f"وإذا رغبت، يمكن إضافة {_uname} ({int(_up_price)} {_cur}) لنتيجة أجمل 🌟",
                            "en": f"If you'd like, you can add {_uname} ({int(_up_price)} {_cur}) for an even better result 🌟",
                            "fr": f"Si vous le souhaitez, ajoutez {_uname} ({int(_up_price)} {_cur}) pour un résultat encore meilleur 🌟",
                        }
                        upsell = _up_hdrs[lang if lang in ("ar","en","fr") else "ar"]
                        reply += "\n" + upsell
                        state["upsell_offered"] = True
                        wa_save(sender, state)
                        print(f"[UPSELL_OFFER] catalog source={_pid} target={_upsell_item['id']} ({_uname!r})")

            elif is_price_question(incoming_msg):
                reply = build_price_list(_WH_CID, lang)

            elif is_recommendation_request(incoming_msg):
                # Recommend first active catalog item dynamically
                _rec_con = get_db_connection()
                try:
                    _rec_ids = [r["id"] for r in _rec_con.execute(
                        "SELECT id FROM catalogs WHERE client_id=? AND is_active=1 ORDER BY id LIMIT 1",
                        (_WH_CID,)
                    ).fetchall()]
                finally:
                    _rec_con.close()
                _all_cat = get_catalog_items(_WH_CID, _rec_ids)
                if _all_cat:
                    _rec     = _all_cat[0]
                    _cur     = get_client(_WH_CID).get("currency", "SAR")
                    _rp      = _rec.get("sale_price") or _rec.get("price") or 0
                    _rec_lang = lang if lang in ("ar", "en", "fr") else "ar"
                    reply = _RECOMMENDATION[_rec_lang].format(
                        svc=_rec["title"],
                        benefit=_rec.get("description") or svc_benefit(_rec["title"], lang),
                        price=f"{int(_rp)} {_cur}",
                    )
                else:
                    reply = openai_chat(incoming_msg, lang=lang, client_obj=_wh_client, catalog_items=_ai_catalog)

            else:
                reply = openai_chat(incoming_msg, lang=lang, client_obj=_wh_client, catalog_items=_ai_catalog)

        # ── STEP: day ─────────────────────────────────────────────────────
        elif step == "day":
            if not is_valid_day(incoming_msg):
                print(f"[DAY_INVALID] rejected={incoming_msg!r}")
                return wa_reply(sender, openai_chat(
                    "Ask the user to choose a valid day like today or tomorrow only.",
                    lang=lang, client_obj=_wh_client,
                ))
            svc = detect_wa_service(incoming_msg)
            if svc and not ensure_svc_list(state.get("known_service")):
                state["known_service"] = [svc]
            state["known_day"] = incoming_msg.strip()
            if state.get("known_time"):
                state["current_step"] = "name"
                print(f"[FLOW] asking_for='name' (time already known)")
                wa_save(sender, state)
                return wa_reply(sender, openai_chat(
                    "Ask the user for their name to complete the booking.",
                    lang=lang, client_obj=_wh_client,
                ))
            else:
                state["current_step"] = "time"
                print(f"[FLOW] asking_for='time'")
                wa_save(sender, state)
                return wa_reply(sender, openai_chat(
                    "Ask the user for the exact time.",
                    lang=lang, client_obj=_wh_client,
                ))

        # ── STEP: time ────────────────────────────────────────────────────
        elif step == "time":
            if is_affirmation(incoming_msg) or _parsed_affirmation:
                svc_tmp = state.get("known_service") or ""
                day_tmp = state.get("known_day") or ""
                avail   = get_available_times(svc_tmp, day_tmp)
                top     = get_top_times(avail, 2)
                if top:
                    slots_str = " / ".join(top)
                    return wa_reply(sender, openai_chat(
                        f"Ask the user to choose one of these available times: {slots_str}",
                        lang=lang, client_obj=_wh_client,
                    ))
                else:
                    return wa_reply(sender, openai_chat(
                        "Ask the user for the exact time.",
                        lang=lang, client_obj=_wh_client,
                    ))
            else:
                time_val = normalize_time_input(incoming_msg)
                if not is_valid_time(time_val):
                    print(f"[TIME_INVALID] rejected={incoming_msg!r} normalized={time_val!r}")
                    return wa_reply(sender, openai_chat(
                        "Ask the user to provide a valid time (example: 16:00).",
                        lang=lang, client_obj=_wh_client,
                    ))
                _svcs_t = ensure_svc_list(state.get("known_service"))
                svc = _svcs_t[0] if _svcs_t else ""
                day = state.get("known_day") or ""
                # Resolve flow type — slot check applies ONLY to service bookings
                _t_ids   = json.loads(state.get("known_catalog_ids_json") or "[]")
                _t_items = get_catalog_items(_WH_CID, _t_ids) if _t_ids else []
                _t_flow  = determine_flow_type(_t_items)
                print(f"[FLOW_TYPE] time-step slot check flow={_t_flow!r}")
                if _t_flow != "order" and is_time_slot_taken(svc, day, time_val):
                    available = get_available_times(svc, day)
                    print(f"[SMART_SUGGEST] full={available}")
                    top = get_top_times(available)
                    print(f"[SMART_SUGGEST] top={top}")
                    if top:
                        slots = "\n".join(f"- {slot}" for slot in top)
                        reply = _bot_str("slot_taken_header", lang) + slots + _bot_str("slot_taken_footer", lang)
                    else:
                        reply = _bot_str("no_slots", lang)
                else:
                    state["known_time"]   = time_val
                    state["current_step"] = "name"
                    wa_save(sender, state)
                    reply = _bot_str("ask_name", lang)

        # ── STEP: name → confirm + save ───────────────────────────────────
        elif step == "name":
            name = (_p_name or (incoming_msg or "")).strip()
            print(f"[DEBUG] validating name={name!r} (parser={_p_name!r})")

            if not is_valid_name(name):
                print("[DEBUG] invalid name detected — rejecting")
                return wa_reply(sender, openai_chat(
                    "Ask the user politely to provide their name only (one or two words). Do not accept sentences.",
                    lang=lang, client_obj=_wh_client,
                ))

            print("[DEBUG] name accepted — saving booking")

            state["current_step"] = "done"
            state["completed"]    = True
            wa_save_booking(sender, state, name)
            wa_save(sender, state)
            print(f"[STATE_COMPLETED] True")
            print(f"[FINAL STATE] {state}")
            send_booking_messages(sender, state, name, lang)
            return "", 200

        else:
            _reprompts = {
                "day":     "Ask the user for the appointment day (today or tomorrow only).",
                "time":    "Ask the user for the appointment time (example: 16:00).",
                "name":    "Ask the user for their name to complete the booking.",
                "confirm": "Ask the user to confirm their booking (yes or no).",
            }
            _prompt = _reprompts.get(step, "Ask the user what service they need.")
            reply = openai_chat(_prompt, lang=lang, client_obj=_wh_client, catalog_items=_ai_catalog)

        print(f"[WHATSAPP] reply={reply!r}")
        return wa_reply(sender, reply)

    except Exception as e:
        import traceback
        print(f"[WHATSAPP] EXCEPTION: {repr(e)}")
        print(traceback.format_exc())
        return "", 200

def _admin_guard():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    cid = session.get("client_id")
    if cid:
        print(f"[AUTH_CLIENT_ID] client_id={cid} path={request.path}")
    return None

# ── /admin/dashboard ──────────────────────────────────────────────────────────
@app.route("/admin/dashboard")
def admin_dashboard():
    guard = _admin_guard()
    if guard:
        return guard
    cid    = _session_client_id()
    client = get_client(cid)

    # ── Onboarding redirect — new clients go through setup first ─────────
    if not _onboarding_complete(client):
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
    # ── Trial status (expire if needed, then compute display state) ────────
    expire_trial_if_needed(cid)
    _fresh_client = get_client(cid)
    trial_info    = get_trial_status(_fresh_client)
    # ── Affiliate stats ────────────────────────────────────────────────────
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

# ── /admin/onboarding ─────────────────────────────────────────────────────────
@app.route("/admin/onboarding", methods=["GET", "POST"])
def admin_onboarding():
    guard = _admin_guard()
    if guard:
        return guard
    cid    = _session_client_id()
    client = get_client(cid)

    _lang = client.get("default_language") or "en"

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

        # ── Step 0 → 1: Welcome acknowledged ──────────────────────────────
        if action == "welcome_done":
            _advance(1)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=0 (welcome)")
            return redirect(url_for("admin_onboarding"))

        # ── Step 1 → 2: Business Info saved ───────────────────────────────
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

        # ── Step 2 → 3: AI Settings saved ─────────────────────────────────
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
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=2 (ai_settings) tone={tone!r} goal={goal!r}")
            return redirect(url_for("admin_onboarding"))

        # ── Step 3 → 4: WhatsApp step done (or skipped) ───────────────────
        elif action in ("whatsapp_done", "skip_whatsapp"):
            _advance(4)
            print(f"[ONBOARDING_STEP_COMPLETED] client={cid} step=3 (whatsapp) action={action!r}")
            return redirect(url_for("admin_onboarding"))

        # ── Step 4 → 5: Final completion + trial start ────────────────────
        elif action == "complete":
            _now       = datetime.datetime.now()
            _trial_end = _now + datetime.timedelta(days=3)
            _now_iso   = _now.isoformat(timespec="seconds")
            _end_iso   = _trial_end.isoformat(timespec="seconds")
            con = get_db_connection()
            try:
                # Only start trial if not already on one (idempotent)
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
                    print(f"[TRIAL_STARTED] client={cid} ends_at={_end_iso}")
                    track_event(cid, "trial_started", {"ends_at": _end_iso})
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

    # ── GET: Refresh client and render wizard ──────────────────────────────────
    client = get_client(cid)
    step = int(client.get("onboarding_step") or 0)

    if step == 0:
        print(f"[ONBOARDING_STARTED] client={cid} lang={_lang!r}")

    # If already finished, go to dashboard
    if step >= 5:
        return redirect(url_for("admin_dashboard"))

    wa_connected = bool(client.get("whatsapp_connected"))

    return render_template(
        "admin/onboarding.html",
        client=client,
        step=step,
        wa_connected=wa_connected,
        lang=_lang,
        active="dashboard"
    )


# ── /onboarding  (short alias for guided sign-up flow) ────────────────────────
@app.route("/onboarding", methods=["GET", "POST"])
def onboarding_alias():
    return admin_onboarding()


# ── /admin/catalog ────────────────────────────────────────────────────────────
@app.route("/admin/catalog")
def admin_catalog():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    client = get_client(cid)
    con = get_db_connection()
    try:
        items = [dict(r) for r in con.execute(
            "SELECT * FROM catalogs WHERE client_id=? ORDER BY id ASC", (cid,)
        ).fetchall()]
    finally:
        con.close()
    return render_template("admin/catalog.html", items=items,
                           currency=client.get("currency", "MAD"), active="catalog")

# ── /admin/catalog/new ────────────────────────────────────────────────────────
@app.route("/admin/catalog/new", methods=["GET", "POST"])
def admin_catalog_new():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    client = get_client(cid)
    if request.method == "POST":
        title       = request.form.get("title", "").strip()
        typ         = request.form.get("type", "service")
        price       = float(request.form.get("price") or 0)
        sale_price  = request.form.get("sale_price") or None
        description = request.form.get("description", "").strip()
        category    = request.form.get("category", "").strip()
        item_cur    = request.form.get("currency", "").strip() or client.get("currency", "MAD")
        duration    = request.form.get("duration_min") or None
        stock       = request.form.get("stock_qty") or None
        is_active   = int(request.form.get("is_active", 1))
        aliases_raw = request.form.get("aliases", "")
        if not title:
            flash("Title is required.", "error")
        else:
            print(f"[CATALOG_SAVE_START] client={cid} title={title!r}")
            # ── TRIAL CHECK ───────────────────────────────────────────────
            if expire_trial_if_needed(cid):
                flash("انتهت التجربة المجانية — يرجى الاشتراك للاستمرار.", "error")
                return redirect(url_for("admin_catalog"))

            # ── PLAN ENFORCE: catalog item limit ──────────────────────────
            print(f"[PLAN_ENFORCE] checking catalog_items limit — client={cid}")
            _cat_ok, _cat_sub = check_plan_limit(cid, "catalog_items")
            if not _cat_ok:
                _cat_plan = (_cat_sub or {}).get("plan_name", "Free")
                _cat_lim  = (_cat_sub or {}).get("max_catalog_items", 5)
                print(f"[LIMIT_BLOCKED] catalog_items — client={cid} plan={_cat_plan!r} limit={_cat_lim}")
                _pw = handle_limit_exceeded(cid, "catalog_items")
                flash(
                    f'{_pw["message_ar"]} — <a href="/admin/upgrade-click?from=catalog" '
                    f'style="color:#1d4ed8;font-weight:700">ترقية الآن</a>',
                    "error"
                )
                return redirect(url_for("admin_catalog"))
            try:
                _now = datetime.datetime.utcnow().isoformat()
                con  = get_db_connection()
                try:
                    cur = con.execute("""
                        INSERT INTO catalogs
                            (client_id, title, type, price, sale_price, description,
                             category, currency, duration_min, stock_qty, is_active,
                             created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (cid, title, typ, price,
                          float(sale_price) if sale_price else None,
                          description, category, item_cur,
                          int(duration) if duration else None,
                          int(stock) if stock else None,
                          is_active, _now, _now))
                    cat_id = cur.lastrowid
                    for alias in [a.strip() for a in aliases_raw.split(",") if a.strip()]:
                        con.execute(
                            "INSERT INTO catalog_aliases (catalog_id, alias, lang) VALUES (?,?,?)",
                            (cat_id, alias.lower(), "ar")
                        )
                    con.commit()
                finally:
                    con.close()
                print(f"[CATALOG_SAVED] client={cid} id={cat_id} title={title!r}")
            except Exception as _ce:
                print(f"[CATALOG_SAVE_ERROR] client={cid} title={title!r} err={_ce!r}")
                flash("حدث خطأ أثناء الحفظ. يرجى المحاولة مجدداً.", "error")
                return redirect(url_for("admin_catalog"))
            # Activation check (first catalog item may complete activation)
            _check_activation(cid)
            flash("تم حفظ العنصر في الكتالوج بنجاح.", "success")
            return redirect(url_for("admin_catalog"))
    return render_template("admin/catalog_form.html", item=None, aliases_str="",
                           currency=client.get("currency", "MAD"), active="catalog")

# ── /admin/catalog/<id>/edit ──────────────────────────────────────────────────
@app.route("/admin/catalog/<int:cat_id>/edit", methods=["GET", "POST"])
def admin_catalog_edit(cat_id):
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    client = get_client(cid)
    con = get_db_connection()
    try:
        item_row = con.execute(
            "SELECT * FROM catalogs WHERE id=? AND client_id=?", (cat_id, cid)
        ).fetchone()
        if not item_row:
            flash("Item not found.", "error")
            return redirect(url_for("admin_catalog"))
        item = dict(item_row)
        aliases_list = [r["alias"] for r in con.execute(
            "SELECT alias FROM catalog_aliases WHERE catalog_id=?", (cat_id,)
        ).fetchall()]
    finally:
        con.close()
    aliases_str = ", ".join(aliases_list)
    if request.method == "POST":
        title       = request.form.get("title", "").strip()
        typ         = request.form.get("type", "service")
        price       = float(request.form.get("price") or 0)
        sale_price  = request.form.get("sale_price") or None
        description = request.form.get("description", "").strip()
        category    = request.form.get("category", "").strip()
        item_cur    = request.form.get("currency", "").strip() or client.get("currency", "MAD")
        duration    = request.form.get("duration_min") or None
        stock       = request.form.get("stock_qty") or None
        is_active   = int(request.form.get("is_active", 1))
        aliases_raw = request.form.get("aliases", "")
        print(f"[CATALOG_SAVE_START] client={cid} id={cat_id} title={title!r}")
        try:
            _now = datetime.datetime.utcnow().isoformat()
            con  = get_db_connection()
            try:
                con.execute("""
                    UPDATE catalogs
                    SET title=?, type=?, price=?, sale_price=?, description=?,
                        category=?, currency=?, duration_min=?, stock_qty=?,
                        is_active=?, updated_at=?
                    WHERE id=? AND client_id=?
                """, (title, typ, price,
                      float(sale_price) if sale_price else None,
                      description, category, item_cur,
                      int(duration) if duration else None,
                      int(stock) if stock else None,
                      is_active, _now, cat_id, cid))
                con.execute("DELETE FROM catalog_aliases WHERE catalog_id=?", (cat_id,))
                for alias in [a.strip() for a in aliases_raw.split(",") if a.strip()]:
                    con.execute(
                        "INSERT INTO catalog_aliases (catalog_id, alias, lang) VALUES (?,?,?)",
                        (cat_id, alias.lower(), "ar")
                    )
                con.commit()
            finally:
                con.close()
            print(f"[CATALOG_SAVED] client={cid} id={cat_id} title={title!r}")
        except Exception as _ue:
            print(f"[CATALOG_SAVE_ERROR] client={cid} id={cat_id} err={_ue!r}")
            flash("حدث خطأ أثناء الحفظ. يرجى المحاولة مجدداً.", "error")
            return redirect(url_for("admin_catalog"))
        flash("تم حفظ التعديلات بنجاح.", "success")
        return redirect(url_for("admin_catalog"))
    return render_template("admin/catalog_form.html", item=item, aliases_str=aliases_str,
                           currency=client.get("currency", "MAD"), active="catalog")

# ── /admin/catalog/<id>/delete ────────────────────────────────────────────────
@app.route("/admin/catalog/<int:cat_id>/delete", methods=["POST"])
def admin_catalog_delete(cat_id):
    """Soft-delete: sets is_active=0 so item disappears from AI and UI
    but remains in the DB for audit purposes."""
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    _now = datetime.datetime.utcnow().isoformat()
    con  = get_db_connection()
    try:
        con.execute(
            "UPDATE catalogs SET is_active=0, updated_at=? WHERE id=? AND client_id=?",
            (_now, cat_id, cid)
        )
        con.commit()
    finally:
        con.close()
    print(f"[CATALOG_SAVE_START] client={cid} id={cat_id} (soft-delete)")
    print(f"[CATALOG_SAVED] client={cid} id={cat_id} is_active=0 (soft-deleted)")
    flash("تم إيقاف تفعيل العنصر (soft delete).", "success")
    return redirect(url_for("admin_catalog"))

# ── /admin/orders ─────────────────────────────────────────────────────────────
@app.route("/admin/orders")
def admin_orders():
    guard = _admin_guard()
    if guard:
        return guard

    # ── read filter params ────────────────────────────────────────────────
    f_status   = (request.args.get("status")    or "").strip().lower()
    f_flow     = (request.args.get("flow_type") or "").strip().lower()
    f_date     = (request.args.get("date")      or "").strip().lower()
    f_q        = (request.args.get("q")         or "").strip()

    print(f"[ORDERS_FILTERS] status={f_status!r} flow={f_flow!r} date={f_date!r} q={f_q!r}")

    cid = _session_client_id()
    # ── build SQL WHERE (status / name / phone / date handled in DB) ──────
    where_clauses = ["client_id = ?"]
    params        = [cid]

    if f_status in ("new", "confirmed", "done", "cancelled"):
        where_clauses.append("status = ?")
        params.append(f_status)

    if f_q:
        like = f"%{f_q}%"
        where_clauses.append("(customer_name LIKE ? OR phone LIKE ?)")
        params.extend([like, like])

    if f_date == "today":
        where_clauses.append("date(created_at) = date('now')")
    elif f_date == "this_week":
        where_clauses.append("date(created_at) >= date('now', '-6 days')")

    sql = ("SELECT * FROM bookings_or_orders WHERE "
           + " AND ".join(where_clauses)
           + " ORDER BY id DESC")

    con = get_db_connection()
    try:
        raw  = con.execute(sql, params).fetchall()
        rows = [{k: r[k] for k in r.keys()} for r in raw]
    finally:
        con.close()

    # ── per-row enrichment ────────────────────────────────────────────────
    enriched = []
    for row in rows:
        # parse stored title list
        try:
            titles = json.loads(row.get("items_json") or "[]")
        except Exception:
            titles = []
        row["items_parsed"] = titles

        # resolve catalog rows by title → type + price
        catalog_items = []
        if titles:
            cat_con = get_db_connection()
            try:
                for title in titles:
                    r = cat_con.execute(
                        "SELECT * FROM catalogs WHERE title=? AND client_id=? LIMIT 1",
                        (title, cid)
                    ).fetchone()
                    if r:
                        catalog_items.append({k: r[k] for k in r.keys()})
                    else:
                        catalog_items.append({"title": title, "type": "service",
                                              "price": 0, "sale_price": None})
            finally:
                cat_con.close()

        # flow type (computed, used for Python-side filtering)
        flow = determine_flow_type(catalog_items)
        row["flow_type"] = flow
        print(f"[ADMIN_RENDER_ITEMS] id={row['id']} titles={titles} flow={flow}")

        # apply flow_type filter in Python (can't do in SQL)
        if f_flow in ("booking", "order", "mixed") and flow != f_flow:
            continue

        # rich item list
        row["items_rich"] = [
            {
                "title":    it.get("title", "?"),
                "price":    float(it.get("sale_price") or it.get("price") or 0),
                "currency": "MAD",
            }
            for it in catalog_items
        ]

        # total from stored value
        row["total_display"] = float(row.get("total_price") or 0)
        print(f"[ADMIN_RENDER_TOTAL] id={row['id']} total={row['total_display']}")

        enriched.append(row)

    print(f"[ORDERS_COUNT] returned={len(enriched)} (pre-filter SQL rows={len(rows)})")

    return render_template(
        "admin/orders.html",
        orders=enriched,
        active="orders",
        f_status=f_status,
        f_flow=f_flow,
        f_date=f_date,
        f_q=f_q,
    )

# ── /admin/orders/<id>/status ──────────────────────────────────────────────────
@app.route("/admin/orders/<int:order_id>/status", methods=["POST"])
def admin_order_status(order_id):
    guard = _admin_guard()
    if guard:
        return guard
    ALLOWED = {"new", "confirmed", "done", "cancelled"}
    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ALLOWED:
        flash(f"Invalid status: {new_status!r}", "error")
        return redirect(url_for("admin_orders"))
    cid = _session_client_id()
    con = get_db_connection()
    try:
        con.execute(
            "UPDATE bookings_or_orders SET status=? WHERE id=? AND client_id=?",
            (new_status, order_id, cid)
        )
        con.commit()
    finally:
        con.close()
    flash(f"Order #{order_id} marked as {new_status}.", "success")
    return redirect(url_for("admin_orders"))

# ── /admin/connect-whatsapp ───────────────────────────────────────────────────
@app.route("/admin/connect-whatsapp", methods=["GET", "POST"])
def admin_connect_whatsapp():
    guard = _admin_guard()
    if guard:
        return guard
    cid    = _session_client_id()
    client = get_client(cid)
    _lang  = client.get("default_language") or "en"

    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "disconnect":
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
            print(f"[WHATSAPP_CONNECT_REQUEST] client={cid} action=disconnect")
            flash(t("wa_disconnect_msg", _lang), "success")
        return redirect(url_for("admin_connect_whatsapp"))

    # ── GET: generate a one-time connect token valid for 15 minutes ──────────
    _bot_raw  = os.getenv("WA_BOT_NUMBER", WA_BOT_NUMBER).strip()
    _now      = datetime.datetime.utcnow()
    _expires  = _now + datetime.timedelta(minutes=15)
    _token    = _secrets.token_hex(4)           # e.g. "a3f91c2b" — 8 chars

    con = get_db_connection()
    try:
        # Expire any old unused tokens for this client
        con.execute("""
            UPDATE wa_connect_tokens SET used=1
            WHERE client_id=? AND used=0 AND expires_at < ?
        """, (cid, _now.isoformat()))
        con.execute("""
            INSERT INTO wa_connect_tokens (token, client_id, created_at, expires_at, used)
            VALUES (?, ?, ?, ?, 0)
        """, (_token, cid, _now.isoformat(), _expires.isoformat()))
        con.commit()
    finally:
        con.close()

    print(f"[WA_AUTO_CONNECT_START] client={cid} token={_token!r} bot={_bot_raw!r}")

    _wa_deeplink = ""
    if _bot_raw:
        import re as _re
        _bot_digits = _re.sub(r'\D', '', _bot_raw)
        _wa_deeplink = f"https://wa.me/{_bot_digits}?text=START"
        print(f"[WA_LINK_GENERATED] {_wa_deeplink}")

    return render_template(
        "admin/connect_whatsapp.html",
        client=client,
        active="whatsapp",
        wa_deeplink=_wa_deeplink,
        bot_configured=bool(_bot_raw),
    )


# ── /admin/connect-whatsapp/qr  (WhatsApp Web-style QR scan page) ─────────────
@app.route("/admin/connect-whatsapp/qr")
def admin_connect_whatsapp_qr():
    guard = _admin_guard()
    if guard:
        return guard
    cid    = _session_client_id()
    client = get_client(cid)
    inst   = get_whatsapp_instance(cid)
    return render_template(
        "admin/connect_whatsapp_qr.html",
        client=client,
        active="whatsapp",
        has_instance=bool(inst),
        instance_status=(inst["status"] if inst else None),
    )


def _fetch_qr_from_ultramsg(instance_id, token):
    """Fetch QR bytes from UltraMsg.
    Tries /instance/qr (raw image) then /instance/qrCode (JSON+base64).
    Returns (bytes, mime) or (None, None)."""
    import base64 as _b64
    try:
        r1 = requests.get(
            f"https://api.ultramsg.com/{instance_id}/instance/qr",
            params={"token": token}, timeout=10
        )
        ct = r1.headers.get("content-type", "")
        if r1.status_code == 200 and ("image" in ct or len(r1.content) > 500):
            return r1.content, ct or "image/png"
    except Exception:
        pass
    try:
        r2 = requests.get(
            f"https://api.ultramsg.com/{instance_id}/instance/qrCode",
            params={"token": token}, timeout=10
        )
        data   = r2.json()
        b64val = data.get("qrCode") or data.get("qr") or ""
        if b64val:
            if "," in b64val:
                b64val = b64val.split(",", 1)[1]
            return _b64.b64decode(b64val), "image/png"
    except Exception:
        pass
    return None, None


def _qr_url_for(instance_id, token):
    """Build the UltraMsg QR image URL. Token embedded in URL per spec."""
    return f"https://api.ultramsg.com/{instance_id}/instance/qr?token={token}"


# ── /api/whatsapp/create-instance ─────────────────────────────────────────────
@app.route("/api/whatsapp/create-instance", methods=["POST"])
def api_whatsapp_create_instance():
    """
    Fully automated per-client WhatsApp instance creation.
    1. Calls UltraMsg POST /instance/create with platform master token.
    2. Saves unique instance_id + token per client in DB.
    3. Returns only {status, qr_url} — token never appears as a bare field.
    """
    guard = _admin_guard()
    if guard:
        return jsonify({"error": "unauthorized"}), 403
    cid          = _session_client_id()
    master_token = os.getenv("ULTRAMSG_TOKEN", "")
    if not master_token:
        return jsonify({"error": "not_configured",
                        "message": "Platform WhatsApp credentials not set."}), 503

    _now = datetime.datetime.utcnow().isoformat()
    con  = get_db_connection()
    try:
        existing = con.execute(
            "SELECT instance_id, token, status FROM whatsapp_instances WHERE client_id=?",
            (cid,)
        ).fetchone()

        if existing and existing["status"] == "connected":
            # Already live — return QR URL for the current instance (for re-display)
            qr_url = _qr_url_for(existing["instance_id"], existing["token"])
            return jsonify({"status": "connected", "qr_url": qr_url})

        if existing:
            # Instance exists but not yet connected — reuse it, reset to pending
            _inst_id = existing["instance_id"]
            _token   = existing["token"]
            con.execute(
                "UPDATE whatsapp_instances SET status='pending', updated_at=? WHERE client_id=?",
                (_now, cid)
            )
            con.commit()
            print(f"[AUTO_INSTANCE_CREATED] client={cid} instance={_inst_id!r} (reused)")
        else:
            # ── Call UltraMsg to create a brand-new per-client instance ──────────
            try:
                create_resp = requests.post(
                    "https://api.ultramsg.com/instance/create",
                    json={"token": master_token},
                    timeout=15
                )
                create_data = create_resp.json()
                _inst_id = (
                    create_data.get("instance_id")
                    or create_data.get("instanceId")
                    or create_data.get("id")
                    or os.getenv("ULTRAMSG_INSTANCE", "")
                )
                _token = (
                    create_data.get("token")
                    or create_data.get("instanceToken")
                    or master_token
                )
            except Exception as _ce:
                # If create API unavailable, fall back to platform-level instance
                print(f"[INSTANCE_CREATE_FALLBACK] {_ce!r}")
                _inst_id = os.getenv("ULTRAMSG_INSTANCE", "")
                _token   = master_token

            if not _inst_id:
                return jsonify({"error": "instance_unavailable"}), 503

            con.execute("""
                INSERT INTO whatsapp_instances
                    (client_id, provider, instance_id, token, status, created_at, updated_at)
                VALUES (?, 'ultramsg', ?, ?, 'pending', ?, ?)
            """, (cid, _inst_id, _token, _now, _now))
            con.commit()
            print(f"[AUTO_INSTANCE_CREATED] client={cid} instance={_inst_id!r}")
    finally:
        con.close()

    qr_url = _qr_url_for(_inst_id, _token)
    print(f"[QR_GENERATED] client={cid} instance={_inst_id!r}")
    return jsonify({"status": "pending", "qr_url": qr_url})


# ── /api/whatsapp/status ───────────────────────────────────────────────────────
@app.route("/api/whatsapp/status")
def api_whatsapp_status():
    """
    Server-side status check — credentials looked up from DB, never sent to browser.
    Logs: [STATUS_CHECK] on every poll, [CONNECTED] on first connection.
    """
    guard = _admin_guard()
    if guard:
        return jsonify({"error": "unauthorized"}), 403
    cid  = _session_client_id()
    inst = get_whatsapp_instance(cid)
    if not inst:
        return jsonify({"status": "not_configured"})

    _inst_id = inst["instance_id"] or os.getenv("ULTRAMSG_INSTANCE", "")
    _token   = inst["token"]       or os.getenv("ULTRAMSG_TOKEN", "")
    if not _inst_id or not _token:
        return jsonify({"status": "not_configured"})

    print(f"[STATUS_CHECK] client={cid} instance={_inst_id!r}")
    try:
        resp = requests.get(
            f"https://api.ultramsg.com/{_inst_id}/instance/status",
            params={"token": _token}, timeout=8
        )
        data = resp.json()
        raw  = (data.get("status") or data.get("instanceStatus") or "").lower()

        if raw in ("connected", "authenticated"):
            _phone = data.get("phone") or data.get("number") or None
            _now   = datetime.datetime.utcnow().isoformat()
            con    = get_db_connection()
            try:
                con.execute("""
                    UPDATE whatsapp_instances
                    SET status='connected', phone_number=COALESCE(?,phone_number), updated_at=?
                    WHERE client_id=?
                """, (_phone, _now, cid))
                con.execute("""
                    UPDATE clients
                    SET whatsapp_connected=1, whatsapp_connection_status='connected'
                    WHERE id=?
                """, (cid,))
                con.commit()
            finally:
                con.close()
            print(f"[CONNECTED] client={cid} phone={_phone!r}")
            return jsonify({"status": "connected"})

        if raw in ("qr", "loading", "init", "initializing"):
            return jsonify({"status": "qr_pending"})
        return jsonify({"status": "disconnected", "raw": raw})
    except Exception as _e:
        print(f"[STATUS_CHECK_ERROR] client={cid} err={_e!r}")
        return jsonify({"status": "error"})


# ── Legacy admin QR routes (kept for backward compatibility) ───────────────────
@app.route("/admin/connect-whatsapp/create-instance", methods=["POST"])
def admin_create_whatsapp_instance():
    return api_whatsapp_create_instance()


@app.route("/admin/connect-whatsapp/qr/image")
def admin_qr_image():
    """Server-side QR image proxy — never exposes token to browser."""
    from flask import make_response
    guard = _admin_guard()
    if guard:
        return "", 403
    cid  = _session_client_id()
    inst = get_whatsapp_instance(cid)
    _instance = (inst["instance_id"] if inst else None) or os.getenv("ULTRAMSG_INSTANCE", "")
    _token    = (inst["token"]       if inst else None) or os.getenv("ULTRAMSG_TOKEN", "")
    if not _instance or not _token:
        return "", 404
    img_bytes, mime = _fetch_qr_from_ultramsg(_instance, _token)
    if not img_bytes:
        return "", 204
    resp = make_response(img_bytes)
    resp.headers["Content-Type"]  = mime
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"]        = "no-cache"
    return resp


@app.route("/admin/connect-whatsapp/qr/status")
def admin_qr_status():
    return api_whatsapp_status()


@app.route("/admin/connect-whatsapp/qr/code")
def admin_qr_code():
    """JSON — returns QR as base64 data-URI for legacy callers."""
    guard = _admin_guard()
    if guard:
        return jsonify({"error": "unauthorized"}), 403
    cid  = _session_client_id()
    inst = get_whatsapp_instance(cid)
    _instance = (inst["instance_id"] if inst else None) or os.getenv("ULTRAMSG_INSTANCE", "")
    _token    = (inst["token"]       if inst else None) or os.getenv("ULTRAMSG_TOKEN", "")
    if not _instance or not _token:
        return jsonify({"qr": None, "status": "not_configured"})
    import base64 as _b64
    img_bytes, mime = _fetch_qr_from_ultramsg(_instance, _token)
    if img_bytes:
        return jsonify({"qr": "data:" + mime + ";base64," + _b64.b64encode(img_bytes).decode()})
    return jsonify({"qr": None, "status": "no_qr"})


# ── /admin/whatsapp-requests  (platform owner: client_id == 1 only) ───────────
@app.route("/admin/whatsapp-requests")
def admin_whatsapp_requests():
    guard = _admin_guard()
    if guard:
        return guard
    if _session_client_id() != 1:
        return "Forbidden", 403

    con = get_db_connection()
    try:
        rows = con.execute("""
            SELECT id, name,
                   business_whatsapp_number,
                   whatsapp_connection_status,
                   created_at
            FROM   clients
            ORDER  BY
                   CASE whatsapp_connection_status
                       WHEN 'pending'   THEN 0
                       WHEN 'connected' THEN 1
                       ELSE 2
                   END,
                   id DESC
        """).fetchall()
        clients_list = [dict(r) for r in rows]
    finally:
        con.close()

    return render_template(
        "admin/whatsapp_requests.html",
        clients=clients_list,
        active="whatsapp_requests",
    )


@app.route("/admin/whatsapp-requests/<int:target_client_id>/complete", methods=["POST"])
@app.route("/admin/whatsapp-requests/<int:target_client_id>/mark-connected", methods=["POST"])
def admin_whatsapp_mark_connected(target_client_id):
    guard = _admin_guard()
    if guard:
        return guard
    if _session_client_id() != 1:
        return "Forbidden", 403

    con = get_db_connection()
    try:
        con.execute("""
            UPDATE clients
            SET    whatsapp_connected=1,
                   whatsapp_connection_status='connected'
            WHERE  id=?
        """, (target_client_id,))
        con.commit()
    finally:
        con.close()

    print(f"[WHATSAPP_CONNECTED_MANUAL] admin marked client={target_client_id} as connected")
    return redirect(url_for("admin_whatsapp_requests"))


# ── /admin/test-admin-notify  (temporary diagnostic route) ────────────────────
@app.route("/admin/test-admin-notify")
def test_admin_notify():
    admin_number = os.getenv("PLATFORM_ADMIN_WHATSAPP", "").strip()
    print(f"[TEST_ADMIN_NOTIFY] admin_number={admin_number!r}")

    if not admin_number:
        print("[TEST_ADMIN_NOTIFY] FAILED — PLATFORM_ADMIN_WHATSAPP not set")
        return {"error": "PLATFORM_ADMIN_WHATSAPP missing"}, 500

    msg = "✅ اختبار إشعار الأدمن من المنصة"
    to  = normalize_number(admin_number)
    print(f"[TEST_ADMIN_NOTIFY] normalized_to={to!r}")
    print(f"[TEST_ADMIN_NOTIFY] message={msg!r}")

    resp = ultramsg_send(to, msg)

    status = resp.status_code if resp else None
    body   = resp.text        if resp else None
    print(f"[TEST_ADMIN_NOTIFY] resp={status}")
    print(f"[TEST_ADMIN_NOTIFY] body={body!r}")

    return {
        "admin_number":   admin_number,
        "normalized_to":  to,
        "status":         status,
        "body":           body,
    }


# ── /admin/settings ───────────────────────────────────────────────────────────
@app.route("/admin/settings", methods=["GET", "POST"])
def admin_settings():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    client = get_client(cid)

    # ── PLAN ENFORCE: white_label fields require business plan ─────────────
    if request.method == "POST" and request.form.get("white_label_enabled"):
        print(f"[PLAN_ENFORCE] checking feature=white_label — client={cid}")
        if not has_feature(cid, "white_label"):
            print(f"[FEATURE_BLOCKED] white_label — client={cid} → upgrade required")
            flash("لقد وصلت إلى حد باقتك الحالية. يرجى الترقية للاستمرار.", "error")
            return redirect(url_for("admin_billing"))

    if request.method == "POST":
        name             = request.form.get("name", "").strip()
        business_type    = request.form.get("business_type", "clinic")
        default_language = request.form.get("default_language", "ar")
        currency         = request.form.get("currency", "MAD").strip()
        timezone         = request.form.get("timezone", "Africa/Casablanca").strip()
        admin_whatsapp   = request.form.get("admin_whatsapp", "").strip()
        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients SET name=?,business_type=?,default_language=?,
                    currency=?,timezone=?,admin_whatsapp=?
                WHERE id=?
            """, (name, business_type, default_language, currency, timezone,
                  admin_whatsapp, cid))
            con.commit()
        finally:
            con.close()
        flash("Settings saved.", "success")
        return redirect(url_for("admin_settings"))
    return render_template("admin/settings.html", client=client, active="settings")


# ── /admin/ai-brain ────────────────────────────────────────────────────────────
@app.route("/admin/ai-brain", methods=["GET", "POST"])
def admin_ai_brain():
    guard = _admin_guard()
    if guard:
        return guard
    cid    = _session_client_id()
    client = get_client(cid)

    if request.method == "POST":
        business_name       = request.form.get("name", "").strip()
        business_type       = request.form.get("business_type", "clinic").strip()
        default_language    = request.form.get("default_language", "ar").strip()
        assistant_tone      = request.form.get("assistant_tone", "friendly").strip()
        assistant_goal      = request.form.get("assistant_goal", "book_appointments").strip()
        business_description= request.form.get("business_description", "").strip()
        policies            = request.form.get("policies", "").strip()
        fallback_message    = request.form.get("fallback_message", "").strip()

        con = get_db_connection()
        try:
            con.execute("""
                UPDATE clients
                SET name=?, business_type=?, default_language=?,
                    assistant_tone=?, assistant_goal=?,
                    business_description=?, policies=?, fallback_message=?
                WHERE id=?
            """, (business_name, business_type, default_language,
                  assistant_tone, assistant_goal,
                  business_description, policies, fallback_message, cid))
            con.commit()
        finally:
            con.close()
        print(f"[AI_BRAIN_SAVED] client={cid} tone={assistant_tone!r} goal={assistant_goal!r} lang={default_language!r}")
        print(f"[CATALOG_SEPARATE_FROM_BRAIN] products/prices NOT stored in brain — client={cid}")
        flash("تم حفظ إعدادات AI Brain بنجاح.", "success")
        return redirect(url_for("admin_ai_brain"))

    print(f"[CATALOG_SEPARATE_FROM_BRAIN] AI Brain loaded without catalog — client={cid}")
    return render_template("admin/ai-brain.html", client=client, active="ai-brain")


# ── /admin/billing + PayPal routes ───────────────────────────────────────────
# ── PayPal plan-ID → internal plan name map ──────────────────────────────────
PAYPAL_PLAN_MAP = {
    "P-2U68430732155245WNHV6LMA": "starter",   # live Starter plan
    "P-38W13773TC442671ENHWAFNY":  "pro",       # live Pro plan
    "P-97J09954NN198664JNHWAZ6Y":  "business",  # live Business plan
    "P-STARTER":                   "starter",   # test alias
    "P-PRO":                       "pro",       # test alias
    "P-BUSINESS":                  "business",  # test alias
}


def get_plan_from_paypal(plan_id):
    """Return internal plan name for a PayPal plan_id, defaulting to 'free'."""
    return PAYPAL_PLAN_MAP.get(plan_id, "free")


# ── /paypal/webhook ───────────────────────────────────────────────────────────
@app.route("/paypal/webhook", methods=["POST"])
def paypal_webhook():
    import json as _json

    payload    = request.get_json(silent=True) or {}
    event_type = payload.get("event_type", "UNKNOWN")
    resource   = payload.get("resource", {})

    print(f"[PAYPAL_WEBHOOK_RECEIVED] event_type={event_type!r}")

    def _resolve_client_by_sub(con, subscription_id, email=""):
        """
        Return client_id for a given PayPal subscription_id.
        Primary:  match clients.subscription_id (set during /api/paypal/subscribe pending phase).
        Fallback: match users.email from subscriber block.
        """
        if subscription_id:
            row = con.execute(
                "SELECT id FROM clients WHERE subscription_id=?", (subscription_id,)
            ).fetchone()
            if row:
                return row["id"]
        if email:
            row = con.execute(
                "SELECT client_id FROM users WHERE LOWER(email)=?", (email.strip().lower(),)
            ).fetchone()
            if row:
                return row["client_id"]
        return None

    # ── BILLING.SUBSCRIPTION.ACTIVATED ───────────────────────────────────────
    if event_type == "BILLING.SUBSCRIPTION.ACTIVATED":
        subscription_id = resource.get("id")
        plan_id         = resource.get("plan_id")
        subscriber      = resource.get("subscriber") or {}
        email           = subscriber.get("email_address", "")
        plan_name       = get_plan_from_paypal(plan_id)
        now             = datetime.now().isoformat(timespec="seconds")

        print(f"[PAYPAL_SUB_ACTIVATED] sub_id={subscription_id!r} "
              f"plan_id={plan_id!r} → plan={plan_name!r} email={email!r}")

        con = get_db_connection()
        try:
            client_id = _resolve_client_by_sub(con, subscription_id, email)

            if client_id:
                # 1. Activate on clients table
                con.execute("""
                    UPDATE clients
                    SET    plan=?, subscription_id=?, subscription_status='active'
                    WHERE  id=?
                """, (plan_name, subscription_id, client_id))
                con.commit()

                # 2. Sync client_subscriptions (sets started_at = activated_at)
                upgrade_client_plan(client_id, plan_name, subscription_id)

                # 3. Affiliate commission
                _aff_row = con.execute(
                    "SELECT affiliate_id FROM users WHERE client_id=? LIMIT 1",
                    (client_id,)
                ).fetchone()
                if _aff_row and _aff_row["affiliate_id"]:
                    _apply_affiliate_commission(_aff_row["affiliate_id"], plan_name)

                print(f"[PAYPAL_SUB_ACTIVATED] client={client_id} "
                      f"plan={plan_name!r} sub={subscription_id!r} → active")
            else:
                print(f"[PAYPAL_SUB_NOT_FOUND] sub_id={subscription_id!r} "
                      f"email={email!r} — no matching client in DB")
        finally:
            con.close()

    # ── BILLING.SUBSCRIPTION.CANCELLED ───────────────────────────────────────
    elif event_type == "BILLING.SUBSCRIPTION.CANCELLED":
        subscription_id = resource.get("id")
        subscriber      = resource.get("subscriber") or {}
        email           = subscriber.get("email_address", "")

        print(f"[PAYPAL_SUB_CANCELLED] sub_id={subscription_id!r} email={email!r}")

        con = get_db_connection()
        try:
            client_id = _resolve_client_by_sub(con, subscription_id, email)

            if client_id:
                # Downgrade clients to free + mark cancelled
                con.execute("""
                    UPDATE clients
                    SET    plan='free', subscription_status='cancelled'
                    WHERE  id=?
                """, (client_id,))

                # Mark client_subscriptions as cancelled
                con.execute("""
                    UPDATE client_subscriptions
                    SET    status='cancelled'
                    WHERE  client_id=? AND paypal_subscription_id=?
                """, (client_id, subscription_id))

                con.commit()
                print(f"[PAYPAL_SUB_CANCELLED] client={client_id} "
                      f"downgraded to free → cancelled")
            else:
                print(f"[PAYPAL_SUB_NOT_FOUND] sub_id={subscription_id!r} "
                      f"email={email!r} — no matching client to cancel")
        finally:
            con.close()

    # ── PAYMENT.SALE.COMPLETED ────────────────────────────────────────────────
    elif event_type == "PAYMENT.SALE.COMPLETED":
        subscription_id = resource.get("billing_agreement_id")
        sale_id         = resource.get("id")
        amount_obj      = resource.get("amount") or {}
        amount          = amount_obj.get("total")
        currency        = amount_obj.get("currency", "USD")

        print(f"[PAYPAL_WEBHOOK_RECEIVED] PAYMENT.SALE.COMPLETED "
              f"sub_id={subscription_id!r} amount={amount!r} {currency}")

        con = get_db_connection()
        try:
            client_id = _resolve_client_by_sub(con, subscription_id)
            now = datetime.now().isoformat(timespec="seconds")
            con.execute("""
                INSERT OR IGNORE INTO paypal_payments
                    (client_id, subscription_id, sale_id, amount,
                     currency, event_type, raw_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (client_id, subscription_id, sale_id,
                  float(amount) if amount else None,
                  currency, event_type, _json.dumps(payload), now))
            con.commit()
            print(f"[PAYPAL_WEBHOOK_RECEIVED] payment logged: "
                  f"client={client_id} amount={amount} {currency}")
        except Exception as _pe:
            print(f"[PAYPAL_WEBHOOK_RECEIVED] payment insert error: {repr(_pe)}")
        finally:
            con.close()

    else:
        print(f"[PAYPAL_WEBHOOK_RECEIVED] unhandled event={event_type!r} — ignored")

    # Always return 200 — PayPal retries on non-2xx
    return {"ok": True}, 200


def upgrade_client_plan(client_id, plan_name, subscription_id=None):
    """
    Activate a named subscription plan for a client.
    Looks up the plan by name, creates/updates client_subscriptions,
    and stores the external subscription_id for later webhook verification.
    """
    con = get_db_connection()
    try:
        plan = con.execute(
            "SELECT * FROM subscription_plans WHERE LOWER(name)=LOWER(?) AND is_active=1",
            (plan_name,)
        ).fetchone()
        if not plan:
            print(f"[UPGRADE] plan not found: {plan_name!r}")
            return False

        plan_id = plan["id"]
        now = datetime.now().isoformat(timespec="seconds")

        existing = con.execute(
            "SELECT id FROM client_subscriptions WHERE client_id=?",
            (client_id,)
        ).fetchone()

        if existing:
            con.execute("""
                UPDATE client_subscriptions
                SET plan_id=?, status='active', started_at=?,
                    expires_at=NULL, paypal_subscription_id=?
                WHERE client_id=?
            """, (plan_id, now, subscription_id, client_id))
        else:
            con.execute("""
                INSERT INTO client_subscriptions
                    (client_id, plan_id, status, started_at, paypal_subscription_id)
                VALUES (?, ?, 'active', ?, ?)
            """, (client_id, plan_id, now, subscription_id))

        # Clear trial flag → client is now a paying subscriber
        con.execute("""
            UPDATE clients
            SET is_trial=0, is_active=1, plan=?, subscription_status='active'
            WHERE id=?
        """, (plan_name, client_id))
        con.commit()
        print(f"[UPGRADE] client={client_id} → plan={plan_name!r} sub_id={subscription_id!r}")
        print(f"[USER_CONVERTED] client={client_id} plan={plan_name!r}")
        track_event(client_id, "payment_success", {"plan": plan_name, "subscription_id": subscription_id or ""})
        return True
    except Exception as _e:
        print(f"[UPGRADE] ERROR: {repr(_e)}")
        return False
    finally:
        con.close()


@app.route("/paypal/subscription-success", methods=["POST"])
def paypal_subscription_success():
    """Legacy route — kept for backwards compatibility. Delegates to the API handler."""
    return api_paypal_subscribe()


@app.route("/api/paypal/subscribe", methods=["POST"])
def api_paypal_subscribe():
    """
    Save a PayPal subscription as PENDING only.

    Accepts JSON: { subscriptionID, plan }
    Saves to DB:  clients.subscription_id, clients.subscription_status='pending'
                  client_subscriptions row with status='pending'

    Does NOT activate the plan. Final activation is done exclusively by
    the PayPal webhook (BILLING.SUBSCRIPTION.ACTIVATED) to prevent spoofing.

    Returns: { "success": true, "status": "pending" }
    """
    data            = request.get_json() or {}
    subscription_id = data.get("subscriptionID") or data.get("subscription_id")
    plan            = data.get("plan", "").strip().lower()

    client_id = session.get("client_id")
    if not client_id:
        print("[PAYPAL_SUBSCRIBE] no session → 401")
        return {"success": False, "error": "not_logged_in"}, 401

    if not plan or not subscription_id:
        print(f"[PAYPAL_SUBSCRIBE] missing fields — plan={plan!r} sub={subscription_id!r}")
        return {"success": False, "error": "missing_fields"}, 400

    con = get_db_connection()
    try:
        # ── 1. Store subscription_id + mark pending on clients ─────────────────
        # Do NOT change clients.plan yet — webhook will do that upon verification.
        con.execute("""
            UPDATE clients
            SET    subscription_id=?, subscription_status='pending'
            WHERE  id=?
        """, (subscription_id, client_id))

        # ── 2. Upsert client_subscriptions as pending ──────────────────────────
        plan_row = con.execute(
            "SELECT id FROM subscription_plans WHERE LOWER(name)=LOWER(?) AND is_active=1",
            (plan,)
        ).fetchone()

        if plan_row:
            now      = datetime.now().isoformat(timespec="seconds")
            existing = con.execute(
                "SELECT id FROM client_subscriptions WHERE client_id=?", (client_id,)
            ).fetchone()
            if existing:
                con.execute("""
                    UPDATE client_subscriptions
                    SET    plan_id=?, status='pending',
                           paypal_subscription_id=?, started_at=?
                    WHERE  client_id=?
                """, (plan_row["id"], subscription_id, now, client_id))
            else:
                con.execute("""
                    INSERT INTO client_subscriptions
                        (client_id, plan_id, status, started_at, paypal_subscription_id)
                    VALUES (?, ?, 'pending', ?, ?)
                """, (client_id, plan_row["id"], now, subscription_id))

        con.commit()
    finally:
        con.close()

    print(f"[PAYPAL_SUBSCRIBE] client={client_id} plan={plan!r} "
          f"sub={subscription_id!r} → pending (awaiting webhook)")
    return {"success": True, "status": "pending"}


@app.route("/admin/upgrade-click")
def admin_upgrade_click():
    """Log upgrade intent and redirect to billing page."""
    source = request.args.get("from", "unknown")
    cid    = _session_client_id()
    print(f"[UPGRADE_CLICKED] client={cid} from={source!r}")
    return redirect(url_for("admin_billing"))


# ── /pay/subscription — upgrade shortlink (used in WA messages + onboarding) ──
@app.route("/pay/subscription")
def pay_subscription():
    """Short upgrade link — redirect to billing page."""
    cid = session.get("client_id")
    print(f"[UPGRADE_CLICKED] client={cid} from=pay_subscription_link")
    if session.get("logged_in"):
        return redirect(url_for("admin_billing"))
    return redirect(url_for("login"))


# ── /api/cron/trial-reminders — call once per day (e.g. via cron or ping) ─────
@app.route("/api/cron/trial-reminders", methods=["GET", "POST"])
def cron_trial_reminders():
    """
    Sends timed WhatsApp reminder messages to trial clients.
    Day 1: Engagement nudge
    Day 2: Feature highlight
    Day 3: Expiry warning

    Tracks which day was last sent in clients.trial_reminder_day.
    Logs [TRIAL_REMINDER_SENT] for each message sent.
    """
    _secret = request.args.get("secret") or request.headers.get("X-Cron-Secret", "")
    _expected = os.getenv("CRON_SECRET", "filtrex-cron")
    if _secret != _expected:
        return {"error": "unauthorized"}, 401

    con = get_db_connection()
    try:
        _trial_clients = con.execute("""
            SELECT id, name, admin_whatsapp, default_language,
                   trial_started_at, trial_ends_at, trial_reminder_day
            FROM clients
            WHERE is_trial=1 AND trial_started_at IS NOT NULL
        """).fetchall()
    finally:
        con.close()

    _now    = datetime.datetime.now()
    _sent   = 0
    _skipped = 0

    _msgs = {
        1: {
            "ar": "👋 مرحباً! هل جربت البوت حتى الآن؟\nأرسل رسالة على رقم واتساب نشاطك وشاهد الذكاء الاصطناعي يرد تلقائياً 🚀",
            "en": "👋 Hi! Have you tried your AI bot yet?\nSend a message to your WhatsApp number and watch the AI reply automatically 🚀",
        },
        2: {
            "ar": "🔥 هل تعلمت أن البوت يمكنه تحويل المحادثات إلى طلبات تلقائياً؟\nأضف خدمات أو منتجات من لوحة التحكم وابدأ البيع الآن 📦",
            "en": "🔥 Did you know your bot can automatically convert conversations into orders?\nAdd services or products from your dashboard and start selling now 📦",
        },
        3: {
            "ar": "⏳ تجربتك المجانية تنتهي قريباً!\nاشترك الآن للاستمرار في استخدام الذكاء الاصطناعي لمبيعاتك 👇\nhttps://filtrex.ai/pay/subscription",
            "en": "⏳ Your free trial is ending soon!\nSubscribe now to keep your AI sales engine running 👇\nhttps://filtrex.ai/pay/subscription",
        },
    }

    for _row in _trial_clients:
        _cid        = _row["id"]
        _phone      = (_row["admin_whatsapp"] or "").strip()
        _lang       = _row["default_language"] or "ar"
        _started_str= _row["trial_started_at"]
        _last_day   = int(_row["trial_reminder_day"] or 0)

        if not _phone or not _started_str:
            _skipped += 1
            continue

        try:
            _started = datetime.datetime.fromisoformat(_started_str)
        except (ValueError, TypeError):
            _skipped += 1
            continue

        _elapsed_days = (_now - _started).total_seconds() / 86400
        _target_day   = min(3, int(_elapsed_days) + 1)   # which day reminder to send

        if _target_day <= _last_day:
            _skipped += 1
            continue

        _msg_template = _msgs.get(_target_day, {})
        _msg = _msg_template.get(_lang) or _msg_template.get("ar", "")
        if not _msg:
            _skipped += 1
            continue

        # Send via platform UltraMsg
        _resp = ultramsg_send(_phone, _msg)
        _status = getattr(_resp, "status_code", "N/A")

        # Update reminder day tracker
        _ucon = get_db_connection()
        try:
            _ucon.execute(
                "UPDATE clients SET trial_reminder_day=? WHERE id=?",
                (_target_day, _cid)
            )
            _ucon.commit()
        finally:
            _ucon.close()

        print(f"[TRIAL_REMINDER_SENT] client={_cid} day={_target_day} "
              f"phone={_phone!r} lang={_lang!r} status={_status}")
        _sent += 1

    print(f"[TRIAL_REMINDERS_RUN] sent={_sent} skipped={_skipped} total={len(_trial_clients)}")
    return {"ok": True, "sent": _sent, "skipped": _skipped}, 200


@app.route("/admin/billing")
def admin_billing():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    sub = get_client_subscription(cid)

    # catalog item count (live)
    con = get_db_connection()
    try:
        catalog_count = con.execute(
            "SELECT COUNT(*) FROM catalogs WHERE client_id=?", (cid,)
        ).fetchone()[0]
        all_plans = con.execute(
            "SELECT * FROM subscription_plans WHERE is_active=1 ORDER BY price_monthly ASC"
        ).fetchall()
    finally:
        con.close()

    plans_list = []
    for p in all_plans:
        pd = dict(p)
        try:
            pd["features"] = json.loads(pd.get("features_json") or "[]")
        except Exception:
            pd["features"] = []
        plans_list.append(pd)

    _plan_display = sub.get("plan_name") if sub else "none"
    print(f"[BILLING_PLAN] admin_billing client={cid} plan={_plan_display!r}")
    return render_template(
        "admin/billing.html",
        sub=sub,
        catalog_count=catalog_count,
        all_plans=plans_list,
        active="billing"
    )


# ── /admin/analytics ──────────────────────────────────────────────────────────
@app.route("/admin/analytics")
def admin_analytics():
    guard = _admin_guard()
    if guard:
        return guard
    cid    = _session_client_id()
    client = get_client(cid)

    # ── PLAN ENFORCE: analytics requires pro or business ──────────────────
    print(f"[PLAN_ENFORCE] checking feature=analytics — client={cid}")
    if not has_feature(cid, "analytics"):
        plan_now = get_client_plan(cid)
        print(f"[FEATURE_BLOCKED] analytics — client={cid} plan={plan_now!r} → upgrade required")
        flash("لقد وصلت إلى حد باقتك الحالية. يرجى الترقية للاستمرار.", "error")
        return redirect(url_for("admin_billing"))

    # ── Gather analytics data ─────────────────────────────────────────────
    import json as _j
    con = get_db_connection()
    try:
        # ── Core order / catalog stats ───────────────────────────────────
        total_orders   = con.execute(
            "SELECT COUNT(*) FROM orders WHERE client_id=?", (cid,)
        ).fetchone()[0]
        catalog_count  = con.execute(
            "SELECT COUNT(*) FROM catalogs WHERE client_id=?", (cid,)
        ).fetchone()[0]
        recent_orders  = [dict(r) for r in con.execute(
            "SELECT * FROM orders WHERE client_id=? ORDER BY id DESC LIMIT 20", (cid,)
        ).fetchall()]
        payments_total = con.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM paypal_payments WHERE client_id=?", (cid,)
        ).fetchone()[0]

        # ── analytics_events based metrics ───────────────────────────────
        total_messages = con.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE client_id=? AND event_name='message_received'",
            (cid,)
        ).fetchone()[0]

        total_ev_orders = con.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE client_id=? AND event_name='order_created'",
            (cid,)
        ).fetchone()[0]

        # Unique customers (unique sender phones from message_received events)
        total_customers = con.execute(
            """SELECT COUNT(DISTINCT json_extract(metadata, '$.sender'))
               FROM analytics_events
               WHERE client_id=? AND event_name='message_received'""",
            (cid,)
        ).fetchone()[0]

        # Active customers — sent a message in the last 7 days
        active_customers = con.execute(
            """SELECT COUNT(DISTINCT json_extract(metadata, '$.sender'))
               FROM analytics_events
               WHERE client_id=? AND event_name='message_received'
                 AND created_at >= DATE('now', '-7 days')""",
            (cid,)
        ).fetchone()[0]

        # WhatsApp connected flag from clients table
        _cli_row = con.execute(
            "SELECT whatsapp_connected FROM clients WHERE id=?", (cid,)
        ).fetchone()
        wa_connected_pct = 100 if (_cli_row and _cli_row["whatsapp_connected"]) else 0

        # Conversion rate: trial_started vs payment_success events
        _trial_started_cnt = con.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE client_id=? AND event_name='trial_started'",
            (cid,)
        ).fetchone()[0]
        _paid_cnt = con.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE client_id=? AND event_name='payment_success'",
            (cid,)
        ).fetchone()[0]
        conversion_rate = (
            round((_paid_cnt / _trial_started_cnt) * 100, 1)
            if _trial_started_cnt > 0 else 0
        )

        # ── Chart: messages over time (last 14 days) ─────────────────────
        daily_messages = [dict(r) for r in con.execute(
            """SELECT DATE(created_at) AS day, COUNT(*) AS cnt
               FROM analytics_events
               WHERE client_id=? AND event_name='message_received'
                 AND created_at >= DATE('now', '-14 days')
               GROUP BY day ORDER BY day ASC""",
            (cid,)
        ).fetchall()]

        # ── Chart: revenue over time (last 14 days from paypal_payments) ─
        daily_revenue = [dict(r) for r in con.execute(
            """SELECT DATE(created_at) AS day,
                      ROUND(SUM(amount), 2) AS total
               FROM paypal_payments
               WHERE client_id=? AND created_at >= DATE('now', '-14 days')
               GROUP BY day ORDER BY day ASC""",
            (cid,)
        ).fetchall()]

        # ── Chart: orders over time (last 14 days) ───────────────────────
        daily_orders = [dict(r) for r in con.execute(
            """SELECT DATE(created_at) AS day, COUNT(*) AS cnt
               FROM orders
               WHERE client_id=? AND created_at >= DATE('now', '-14 days')
               GROUP BY day ORDER BY day ASC""",
            (cid,)
        ).fetchall()]

        # ── Intent breakdown ──────────────────────────────────────────────
        intent_rows = [dict(r) for r in con.execute(
            """SELECT json_extract(metadata, '$.intent') AS intent, COUNT(*) AS cnt
               FROM analytics_events
               WHERE client_id=? AND event_name='intent_detected'
               GROUP BY intent ORDER BY cnt DESC LIMIT 8""",
            (cid,)
        ).fetchall()]

    finally:
        con.close()

    sub = get_client_subscription(cid)
    messages_used = sub.get("messages_used", 0) if sub else 0

    print(f"[PLAN_CHECK] admin_analytics client={cid} plan={get_client_plan(cid)!r} → analytics allowed")
    return render_template(
        "admin/analytics.html",
        client=client,
        sub=sub,
        # KPI cards
        total_customers=total_customers,
        active_customers=active_customers,
        wa_connected_pct=wa_connected_pct,
        total_messages=total_messages,
        total_orders=total_orders,
        payments_total=payments_total,
        conversion_rate=conversion_rate,
        # legacy
        catalog_count=catalog_count,
        messages_used=messages_used,
        total_ev_orders=total_ev_orders,
        # charts
        daily_messages=daily_messages,
        daily_revenue=daily_revenue,
        daily_orders=daily_orders,
        intent_rows=intent_rows,
        # orders table
        recent_orders=recent_orders,
        active="analytics"
    )


# ── /admin/branding ───────────────────────────────────────────────────────────
ALLOWED_LOGO_EXTS = {"png", "jpg", "jpeg", "gif", "webp", "svg"}

@app.route("/admin/branding", methods=["GET", "POST"])
def admin_branding():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    con = get_db_connection()
    try:
        client = con.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    finally:
        con.close()

    error = None
    success = None

    if request.method == "POST":
        brand_name    = request.form.get("brand_name",    "").strip()
        primary_color = request.form.get("primary_color", "#4f46e5").strip()
        custom_domain = request.form.get("custom_domain", "").strip().lower().lstrip("https://").lstrip("http://").strip("/")
        wl_enabled    = 1 if request.form.get("white_label_enabled") else 0
        logo_url      = client["logo_url"] if client else None

        # ── File upload ───────────────────────────────────────────────────
        upload = request.files.get("logo_file")
        if upload and upload.filename:
            ext = upload.filename.rsplit(".", 1)[-1].lower()
            if ext not in ALLOWED_LOGO_EXTS:
                error = f"Unsupported file type .{ext}. Allowed: {', '.join(sorted(ALLOWED_LOGO_EXTS))}"
            else:
                save_dir  = os.path.join("static", "uploads", "logos")
                os.makedirs(save_dir, exist_ok=True)
                filename  = f"client_{cid}.{ext}"
                save_path = os.path.join(save_dir, filename)
                upload.save(save_path)
                logo_url  = f"/static/uploads/logos/{filename}"
                print(f"[WHITE_LABEL] client={cid} logo saved → {save_path!r}")
        elif request.form.get("logo_url_field", "").strip():
            logo_url = request.form.get("logo_url_field", "").strip()

        # ── Domain uniqueness check ───────────────────────────────────────
        if not error and custom_domain:
            con = get_db_connection()
            try:
                conflict = con.execute(
                    "SELECT id FROM clients WHERE custom_domain=? AND id!=?",
                    (custom_domain, cid)
                ).fetchone()
            finally:
                con.close()
            if conflict:
                error = "That custom domain is already registered by another account."

        if not error:
            con = get_db_connection()
            try:
                con.execute("""
                    UPDATE clients
                    SET brand_name=?, logo_url=?, primary_color=?,
                        custom_domain=?, white_label_enabled=?
                    WHERE id=?
                """, (brand_name or None, logo_url, primary_color,
                      custom_domain or None, wl_enabled, cid))
                con.commit()
            finally:
                con.close()
            print(f"[WHITE_LABEL_APPLIED] client={cid} brand={brand_name!r} domain={custom_domain!r} enabled={wl_enabled}")
            flash("Branding saved.", "success")
            return redirect(url_for("admin_branding"))

    con = get_db_connection()
    try:
        client = con.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    finally:
        con.close()
    return render_template("admin/branding.html", client=client,
                           error=error, active="branding")


# ═══════════════════════════════════════════════════════════════
# PUBLIC REST API  (/api/*)
# ═══════════════════════════════════════════════════════════════

def _order_row_to_dict(r):
    return {
        "id":         r["id"],
        "client_id":  r["client_id"],
        "name":       r["name"],
        "phone":      r["phone"],
        "items":      r["items"],
        "scheduled":  r["scheduled"],
        "status":     r["status"],
        "created_at": r["created_at"],
    }

def _catalog_row_to_dict(r):
    return {
        "id":          r["id"],
        "title":       r["title"],
        "type":        r["type"],
        "price":       r["price"],
        "sale_price":  r["sale_price"],
        "description": r["description"],
        "stock_qty":   r["stock_qty"],
        "is_active":   r["is_active"],
    }


@app.route("/api/orders", methods=["GET"])
def api_get_orders():
    cid, err = _api_guard()
    if err:
        return err
    status_filter = request.args.get("status")
    limit  = min(int(request.args.get("limit",  50)), 200)
    offset = int(request.args.get("offset", 0))
    con = get_db_connection()
    try:
        if status_filter:
            rows = con.execute(
                "SELECT * FROM orders WHERE client_id=? AND status=? "
                "ORDER BY id DESC LIMIT ? OFFSET ?",
                (cid, status_filter, limit, offset)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM orders WHERE client_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
                (cid, limit, offset)
            ).fetchall()
    finally:
        con.close()
    return jsonify({"orders": [_order_row_to_dict(r) for r in rows],
                    "count": len(rows), "limit": limit, "offset": offset})


@app.route("/api/orders", methods=["POST"])
def api_post_orders():
    cid, err = _api_guard()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    name      = str(body.get("name",      "")).strip()
    phone     = str(body.get("phone",     "")).strip()
    items     = str(body.get("items",     "")).strip()
    scheduled = str(body.get("scheduled", "")).strip()
    status    = body.get("status", "pending")
    if not phone or not items:
        return jsonify({"error": "phone and items are required"}), 400

    # ── TRIAL CHECK ────────────────────────────────────────────────────────
    if expire_trial_if_needed(cid):
        return jsonify({
            "error": "trial_expired",
            "message_ar": "انتهت التجربة المجانية — يرجى الاشتراك للاستمرار.",
            "message_en": "Free trial expired — please subscribe to continue.",
            "upgrade_url": "/admin/billing",
        }), 402

    # ── PLAN ENFORCE: order limit ──────────────────────────────────────────
    print(f"[PLAN_ENFORCE] checking orders limit — client={cid}")
    _ord_ok, _ord_sub = check_plan_limit(cid, "orders")
    if not _ord_ok:
        _ord_plan = (_ord_sub or {}).get("plan_name", "Free")
        _ord_lim  = (_ord_sub or {}).get("max_orders", 10)
        print(f"[LIMIT_BLOCKED] orders — client={cid} plan={_ord_plan!r} limit={_ord_lim}")
        _pw = handle_limit_exceeded(cid, "orders")
        return jsonify(_pw | {"plan": _ord_plan, "limit": _ord_lim}), 429

    con = get_db_connection()
    try:
        cur = con.execute(
            "INSERT INTO orders (client_id, name, phone, items, scheduled, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, name, phone, items, scheduled, status)
        )
        new_id = cur.lastrowid
        row = con.execute("SELECT * FROM orders WHERE id=?", (new_id,)).fetchone()
        con.commit()
    finally:
        con.close()
    payload = _order_row_to_dict(row)
    fire_webhook(cid, "order_created", payload)
    return jsonify({"order": payload}), 201


@app.route("/api/catalog", methods=["GET"])
def api_get_catalog():
    cid, err = _api_guard()
    if err:
        return err
    limit  = min(int(request.args.get("limit",  50)), 200)
    offset = int(request.args.get("offset", 0))
    con = get_db_connection()
    try:
        rows = con.execute(
            "SELECT * FROM catalogs WHERE client_id=? AND is_active=1 "
            "ORDER BY id DESC LIMIT ? OFFSET ?",
            (cid, limit, offset)
        ).fetchall()
    finally:
        con.close()
    return jsonify({"catalog": [_catalog_row_to_dict(r) for r in rows],
                    "count": len(rows), "limit": limit, "offset": offset})


@app.route("/api/catalog", methods=["POST"])
def api_post_catalog():
    cid, err = _api_guard()
    if err:
        return err
    # Check plan limit
    allowed, msg = check_usage_limit(cid, "catalog_items")
    if not allowed:
        return jsonify({"error": msg}), 402
    body = request.get_json(force=True, silent=True) or {}
    title = str(body.get("title", "")).strip()
    if not title:
        return jsonify({"error": "title is required"}), 400
    item_type = body.get("type",        "product")
    price     = float(body.get("price", 0))
    sale_p    = body.get("sale_price")
    desc      = str(body.get("description", "")).strip()
    stock     = body.get("stock_qty")
    con = get_db_connection()
    try:
        cur = con.execute(
            "INSERT INTO catalogs (client_id, title, type, price, sale_price, description, stock_qty) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (cid, title, item_type, price, sale_p, desc, stock)
        )
        new_id = cur.lastrowid
        row = con.execute("SELECT * FROM catalogs WHERE id=?", (new_id,)).fetchone()
        con.commit()
    finally:
        con.close()
    return jsonify({"item": _catalog_row_to_dict(row)}), 201


# ═══════════════════════════════════════════════════════════════
# ADMIN: API KEYS  (/admin/api-keys)
# ═══════════════════════════════════════════════════════════════

@app.route("/admin/api-keys", methods=["GET", "POST"])
def admin_api_keys():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()

    if request.method == "POST":
        action = request.form.get("action")
        con = get_db_connection()
        try:
            if action == "generate":
                label   = request.form.get("label", "Default").strip() or "Default"
                new_key = _generate_api_key()
                con.execute(
                    "INSERT INTO api_keys (client_id, api_key, label) VALUES (?, ?, ?)",
                    (cid, new_key, label)
                )
                con.commit()
                flash(f"API key generated: {new_key}", "success")
            elif action == "revoke":
                key_id = int(request.form.get("key_id", 0))
                # security: only revoke keys owned by this client
                con.execute(
                    "UPDATE api_keys SET is_active=0 WHERE id=? AND client_id=?",
                    (key_id, cid)
                )
                con.commit()
                flash("API key revoked.", "success")
        finally:
            con.close()
        return redirect(url_for("admin_api_keys"))

    con = get_db_connection()
    try:
        keys = con.execute(
            "SELECT * FROM api_keys WHERE client_id=? ORDER BY id DESC",
            (cid,)
        ).fetchall()
    finally:
        con.close()
    return render_template("admin/api_keys.html", keys=keys, active="integrations")


# ═══════════════════════════════════════════════════════════════
# ADMIN: WEBHOOKS  (/admin/webhooks)
# ═══════════════════════════════════════════════════════════════

_VALID_EVENTS = ["order_created", "booking_created"]

@app.route("/admin/webhooks", methods=["GET", "POST"])
def admin_webhooks():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()

    if request.method == "POST":
        action = request.form.get("action")
        con = get_db_connection()
        try:
            if action == "add":
                url        = request.form.get("url", "").strip()
                event_type = request.form.get("event_type", "").strip()
                if not url.startswith("http"):
                    flash("URL must start with http:// or https://", "error")
                elif event_type not in _VALID_EVENTS:
                    flash(f"Unknown event type. Choose from: {', '.join(_VALID_EVENTS)}", "error")
                else:
                    con.execute(
                        "INSERT INTO webhooks (client_id, url, event_type) VALUES (?, ?, ?)",
                        (cid, url, event_type)
                    )
                    con.commit()
                    flash("Webhook registered.", "success")
            elif action == "delete":
                wh_id = int(request.form.get("wh_id", 0))
                con.execute(
                    "DELETE FROM webhooks WHERE id=? AND client_id=?",
                    (wh_id, cid)
                )
                con.commit()
                flash("Webhook removed.", "success")
        finally:
            con.close()
        return redirect(url_for("admin_webhooks"))

    con = get_db_connection()
    try:
        whs = con.execute(
            "SELECT * FROM webhooks WHERE client_id=? ORDER BY id DESC",
            (cid,)
        ).fetchall()
    finally:
        con.close()
    return render_template("admin/webhooks.html", webhooks=whs,
                           valid_events=_VALID_EVENTS, active="integrations")


# ═══════════════════════════════════════════════════════════════
# ADMIN: INTEGRATIONS HUB  (/admin/integrations)
# ═══════════════════════════════════════════════════════════════

@app.route("/admin/integrations")
def admin_integrations():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    shopify_cfg = _get_integration(cid, "shopify")
    stripe_cfg  = _get_integration(cid, "stripe")
    return render_template("admin/integrations.html",
                           shopify_cfg=shopify_cfg, stripe_cfg=stripe_cfg,
                           active="integrations")


# ═══════════════════════════════════════════════════════════════
# ADMIN: SHOPIFY INTEGRATION
# ═══════════════════════════════════════════════════════════════

@app.route("/admin/integrations/shopify", methods=["GET", "POST"])
def admin_integration_shopify():
    guard = _admin_guard()
    if guard:
        return guard
    cid = _session_client_id()
    cfg   = _get_integration(cid, "shopify")
    error = None
    sync_result = None

    if request.method == "POST":
        action = request.form.get("action", "save")

        if action == "save":
            shop_domain    = request.form.get("shop_domain", "").strip().strip("/")
            access_token   = request.form.get("access_token", "").strip()
            if not shop_domain or not access_token:
                error = "Shop domain and access token are required."
            else:
                cfg = {"shop_domain": shop_domain, "access_token": access_token}
                _save_integration(cid, "shopify", cfg)
                flash("Shopify credentials saved.", "success")
                return redirect(url_for("admin_integration_shopify"))

        elif action == "sync":
            cfg = _get_integration(cid, "shopify")
            shop_domain  = cfg.get("shop_domain", "")
            access_token = cfg.get("access_token", "")
            if not shop_domain or not access_token:
                error = "Save your Shopify credentials before syncing."
            else:
                try:
                    api_url = f"https://{shop_domain}/admin/api/2024-01/products.json?limit=50"
                    resp = requests.get(api_url,
                                        headers={"X-Shopify-Access-Token": access_token},
                                        timeout=15)
                    if resp.status_code != 200:
                        error = f"Shopify API error {resp.status_code}: {resp.text[:200]}"
                    else:
                        products = resp.json().get("products", [])
                        imported = 0
                        con = get_db_connection()
                        try:
                            for p in products:
                                title  = p.get("title", "")
                                desc   = p.get("body_html", "") or ""
                                variant = (p.get("variants") or [{}])[0]
                                price  = float(variant.get("price") or 0)
                                stock  = variant.get("inventory_quantity")
                                # Skip if already exists (same title same client)
                                exists = con.execute(
                                    "SELECT id FROM catalogs WHERE client_id=? AND title=?",
                                    (cid, title)
                                ).fetchone()
                                if not exists:
                                    con.execute(
                                        "INSERT INTO catalogs "
                                        "(client_id, title, type, price, description, stock_qty) "
                                        "VALUES (?, ?, 'product', ?, ?, ?)",
                                        (cid, title, price, desc[:500], stock)
                                    )
                                    imported += 1
                            con.commit()
                        finally:
                            con.close()
                        print(f"[INTEGRATION_TRIGGER] client={cid} shopify sync "
                              f"fetched={len(products)} imported={imported}")
                        sync_result = f"Synced {len(products)} products — {imported} new items added to catalog."
                except Exception as exc:
                    error = f"Sync failed: {exc!r}"

    cfg = _get_integration(cid, "shopify")
    return render_template("admin/integration_shopify.html",
                           cfg=cfg, error=error, sync_result=sync_result,
                           active="integrations")


# ═══════════════════════════════════════════════════════════════
# ADMIN: STRIPE INTEGRATION
# ═══════════════════════════════════════════════════════════════

@app.route("/admin/integrations/stripe", methods=["GET", "POST"])
def admin_integration_stripe():
    guard = _admin_guard()
    if guard:
        return guard
    cid   = _session_client_id()
    cfg   = _get_integration(cid, "stripe")
    error = None
    payment_link = None

    if request.method == "POST":
        action = request.form.get("action", "save")

        if action == "save":
            secret_key = request.form.get("secret_key", "").strip()
            if not secret_key.startswith("sk_"):
                error = "Stripe secret key must start with sk_live_ or sk_test_"
            else:
                _save_integration(cid, "stripe", {"secret_key": secret_key})
                flash("Stripe credentials saved.", "success")
                return redirect(url_for("admin_integration_stripe"))

        elif action == "create_link":
            cfg        = _get_integration(cid, "stripe")
            secret_key = cfg.get("secret_key", "")
            amount     = request.form.get("amount", "").strip()
            currency   = request.form.get("currency", "usd").strip().lower()
            name       = request.form.get("name", "Order Payment").strip()
            if not secret_key:
                error = "Save your Stripe secret key first."
            elif not amount or not amount.replace(".", "").isdigit():
                error = "Enter a valid amount."
            else:
                try:
                    amount_cents = int(float(amount) * 100)
                    # Create a Price then a Payment Link
                    price_resp = requests.post(
                        "https://api.stripe.com/v1/prices",
                        auth=(secret_key, ""),
                        data={
                            "unit_amount": amount_cents,
                            "currency":    currency,
                            "product_data[name]": name,
                        },
                        timeout=15
                    )
                    if price_resp.status_code != 200:
                        error = f"Stripe error: {price_resp.json().get('error', {}).get('message', price_resp.text[:200])}"
                    else:
                        price_id = price_resp.json()["id"]
                        link_resp = requests.post(
                            "https://api.stripe.com/v1/payment_links",
                            auth=(secret_key, ""),
                            data={"line_items[0][price]": price_id,
                                  "line_items[0][quantity]": 1},
                            timeout=15
                        )
                        if link_resp.status_code != 200:
                            error = f"Stripe error: {link_resp.json().get('error', {}).get('message', link_resp.text[:200])}"
                        else:
                            payment_link = link_resp.json().get("url", "")
                            print(f"[INTEGRATION_TRIGGER] client={cid} stripe "
                                  f"payment_link={payment_link!r} amount={amount} {currency}")
                except Exception as exc:
                    error = f"Stripe request failed: {exc!r}"

    cfg = _get_integration(cid, "stripe")
    return render_template("admin/integration_stripe.html",
                           cfg=cfg, error=error, payment_link=payment_link,
                           active="integrations")


# ── /admin/bookings ──────────────────────────────────────────────────────────── (legacy)
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
                    ref_code = (request.form.get("ref_code") or "").strip().upper()
                    # Create client first
                    cur_c = con.execute("""
                        INSERT INTO clients
                            (name, business_type, default_language,
                             currency, timezone, is_active)
                        VALUES (?, 'other', 'ar', 'MAD', 'Africa/Casablanca', 1)
                    """, (business_name,))
                    new_client_id = cur_c.lastrowid
                    # Generate and save referral code
                    new_ref_code = generate_referral_code(new_client_id)
                    con.execute(
                        "UPDATE clients SET referral_code=? WHERE id=?",
                        (new_ref_code, new_client_id)
                    )
                    # ── Generate affiliate code ───────────────────────────
                    new_aff_code = generate_affiliate_code(new_client_id)
                    con.execute(
                        "UPDATE clients SET affiliate_code=? WHERE id=?",
                        (new_aff_code, new_client_id)
                    )
                    print(f"[AFFILIATE_CREATED] client={new_client_id} affiliate_code={new_aff_code!r}")
                    # ── Start 3-day free trial ────────────────────────────
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
                    print(f"[TRIAL_STARTED] client={new_client_id} ends_at={_t_end.isoformat(timespec='seconds')!r}")
                    # Create user linked to that client
                    cur_u = con.execute("""
                        INSERT INTO users (username, email, password, client_id)
                        VALUES (?, ?, ?, ?)
                    """, (email, email,
                          generate_password_hash(password), new_client_id))
                    new_user_id = cur_u.lastrowid
                    con.commit()
                    print(f"[AUTH_SIGNUP] user_id={new_user_id} client_id={new_client_id} email={email!r} referral_code={new_ref_code!r}")
                    track_event(new_client_id, "user_registered", {"email": email})
                    track_event(new_client_id, "trial_started", {"ends_at": _t_end.isoformat(timespec="seconds")})

                    # ── Affiliate tracking ────────────────────────────────
                    aff_code = (request.form.get("aff_code") or "").strip().upper()
                    if aff_code:
                        aff_client = con.execute(
                            "SELECT id FROM clients WHERE affiliate_code=?", (aff_code,)
                        ).fetchone()
                        if aff_client and aff_client["id"] != new_client_id:
                            con.execute(
                                "UPDATE users SET affiliate_id=? WHERE id=?",
                                (aff_client["id"], new_user_id)
                            )
                            con.commit()
                            print(f"[AFFILIATE_REFERRAL] new_client={new_client_id} "
                                  f"user={new_user_id} affiliate_client={aff_client['id']} "
                                  f"code={aff_code!r}")
                        else:
                            print(f"[AFFILIATE_REFERRAL] aff_code={aff_code!r} "
                                  f"not found or self-referral — ignored")

                    # ── Referral tracking ─────────────────────────────────
                    if ref_code:
                        referrer = con.execute(
                            "SELECT id, referral_count FROM clients WHERE referral_code=?",
                            (ref_code,)
                        ).fetchone()
                        if referrer and referrer["id"] != new_client_id:
                            referrer_id    = referrer["id"]
                            new_ref_count  = (referrer["referral_count"] or 0) + 1
                            con.execute(
                                "UPDATE clients SET referred_by=? WHERE id=?",
                                (referrer_id, new_client_id)
                            )
                            con.execute(
                                "UPDATE clients SET referral_count=? WHERE id=?",
                                (new_ref_count, referrer_id)
                            )
                            con.commit()
                            print(f"[REFERRAL_USED] new_client={new_client_id} referred_by={referrer_id} ref_code={ref_code!r} referrer_count={new_ref_count}")
                            _apply_referral_reward(referrer_id, new_ref_count)
                        else:
                            print(f"[REFERRAL_USED] ref_code={ref_code!r} not found or self-referral — ignored")

                    # Auto-login after signup
                    session.clear()
                    session["logged_in"]  = True
                    session["user_id"]    = new_user_id
                    session["client_id"]  = new_client_id
                    session["user_email"] = email
                    print(f"[AUTH_LOGIN] email={email!r} client_id={new_client_id}")
                    print(f"[AUTH_CLIENT_ID] client_id={new_client_id} path=/signup")
                    return redirect(url_for("admin_dashboard"))
            finally:
                con.close()
    return render_template("signup.html", error=error)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        email    = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        con = get_db_connection()
        try:
            # Support login by email OR username (backward compat for existing accounts)
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
            print(f"[AUTH_CLIENT_ID] client_id={client_id} path=/login")
            return redirect(url_for("admin_dashboard"))
        error = "Invalid email or password."
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

# ── /saas/whatsapp-requests  (SaaS-operator only: client_id == 1) ─────────────
def _saas_guard():
    """Only client_id=1 (the SaaS owner) may access /saas/* routes."""
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    if _session_client_id() != 1:
        return "Forbidden", 403
    return None


@app.route("/saas/whatsapp-requests")
def saas_whatsapp_requests():
    guard = _saas_guard()
    if guard:
        return guard

    con = get_db_connection()
    try:
        rows = con.execute("""
            SELECT id, name,
                   business_whatsapp_number,
                   whatsapp_connection_status,
                   created_at
            FROM   clients
            WHERE  whatsapp_connection_status IN ('pending', 'connected', 'not_connected')
            ORDER  BY
                   CASE whatsapp_connection_status
                       WHEN 'pending'   THEN 0
                       WHEN 'connected' THEN 1
                       ELSE 2
                   END,
                   id DESC
        """).fetchall()
        clients_list = [dict(r) for r in rows]
    finally:
        con.close()

    return render_template("saas/whatsapp_requests.html", clients=clients_list)


@app.route("/saas/whatsapp-approve", methods=["POST"])
def saas_whatsapp_approve():
    guard = _saas_guard()
    if guard:
        return guard

    client_id = request.form.get("client_id", type=int)
    action    = request.form.get("action", "connect")   # connect | disconnect

    if not client_id:
        return redirect(url_for("saas_whatsapp_requests"))

    con = get_db_connection()
    try:
        if action == "disconnect":
            con.execute("""
                UPDATE clients
                SET    whatsapp_connected=0,
                       whatsapp_connection_status='not_connected'
                WHERE  id=?
            """, (client_id,))
            con.commit()
            print(f"[WHATSAPP_CONNECTED_MANUAL] operator disconnected client={client_id}")
        else:
            con.execute("""
                UPDATE clients
                SET    whatsapp_connected=1,
                       whatsapp_connection_status='connected'
                WHERE  id=?
            """, (client_id,))
            con.commit()
            print(f"[WHATSAPP_CONNECTED_MANUAL] operator activated client={client_id}")
    finally:
        con.close()

    return redirect(url_for("saas_whatsapp_requests"))


# ── Payment Routes ────────────────────────────────────────────────────────────

@app.route("/pay/<int:order_id>", methods=["GET"])
def pay_order(order_id):
    """Public payment page — shows order summary and PayPal button."""
    con = get_db_connection()
    try:
        order = con.execute(
            "SELECT * FROM orders WHERE id=?", (order_id,)
        ).fetchone()
    finally:
        con.close()
    if not order:
        return "Order not found", 404
    order = dict(order)
    # Parse collected_data JSON stored in items
    try:
        order["data"] = json.loads(order.get("items") or "{}")
    except Exception:
        order["data"] = {}
    paypal_client_id = os.getenv("PAYPAL_CLIENT_ID", "")
    return render_template("pay.html", order=order, paypal_client_id=paypal_client_id)


@app.route("/pay/<int:order_id>/success", methods=["POST"])
def pay_order_success(order_id):
    """Called by PayPal JS SDK after successful payment approval."""
    payload   = request.get_json(silent=True) or {}
    paypal_id = payload.get("orderID") or payload.get("id") or ""
    print(f"[PAYMENT_SUCCESS] order_id={order_id} paypal_id={paypal_id!r}")

    con = get_db_connection()
    try:
        order = con.execute(
            "SELECT * FROM orders WHERE id=?", (order_id,)
        ).fetchone()
        if not order:
            return jsonify({"ok": False, "error": "not found"}), 404
        order = dict(order)

        # Update order status
        con.execute("""
            UPDATE orders
            SET payment_status='paid', status='confirmed'
            WHERE id=?
        """, (order_id,))
        con.commit()
        print(f"[ORDER_CONFIRMED] order_id={order_id} client={order.get('client_id')}")
    finally:
        con.close()

    # Send WhatsApp confirmation to customer
    _cust_phone = order.get("customer_phone") or order.get("phone") or ""
    if _cust_phone:
        _confirm_msg = "✅ تم تأكيد طلبك بنجاح! نراك قريبًا 🎉"
        ultramsg_send(_cust_phone, _confirm_msg)
        print(f"[PAYMENT_SUCCESS] WA confirmation sent to {_cust_phone!r}")

    return jsonify({"ok": True, "order_id": order_id})


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=5000, debug=debug)
