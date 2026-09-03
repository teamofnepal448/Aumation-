import os
import re
import time
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone

from quart import Quart, request, jsonify, render_template_string
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

# ----------------------------------------------------------------------------
# Logging: ring buffer + stdout so Render captures the same stream the UI shows
# ----------------------------------------------------------------------------

LOG_BUFFER = deque(maxlen=500)
LOG_SEQ = [0]

class BufferLogHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_SEQ[0] += 1
            LOG_BUFFER.append({
                "id": LOG_SEQ[0],
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "source": record.name,
                "message": record.getMessage(),
            })
        except Exception:
            self.handleError(record)

logger = logging.getLogger("tgrelay")
logger.setLevel(logging.DEBUG)
logger.propagate = False
logger.addHandler(BufferLogHandler())
_stdout = logging.StreamHandler()
_stdout.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
logger.addHandler(_stdout)

# ----------------------------------------------------------------------------
# App + in-memory state (per Hypercorn worker)
# ----------------------------------------------------------------------------

app = Quart(__name__)

PENDING = {}  # phone -> {"client","phone_code_hash","api_id","api_hash","ts"}
ACTIVE = {}   # phone -> {"client","account","connected_at"}
PENDING_TTL = 600.0  # seconds an OTP attempt stays valid in memory
STATE_LOCK = asyncio.Lock()

FWD = {
    "running": False,
    "handler": None,
    "builder": None,
    "client": None,
    "phone": None,
    "source": None,
    "targets": [],
    "branding": "",
    "forwarded": 0,
    "failed": 0,
    "started_at": None,
}

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def mask_phone(phone):
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) <= 4:
        return ""
    return "+" + digits[:3] + "***" + digits[-2:]

def parse_ident(raw):
    """Normalize -100 ids, @usernames, t.me/ and t.me/c/ links."""
    s = str(raw).strip()
    if not s:
        raise ValueError("empty identifier")
    s = s.split("?")[0].rstrip("/")
    m = re.match(r"^(?:https?://)?t.me/c/(\d+)(?:/\d+)?$", s)
    if m:
        return int("-100" + m.group(1))
    m = re.match(r"^(?:https?://)?t.me/([\w]+)$", s)
    if m:
        return m.group(1)
    if s.startswith("@"):
        return s[1:]
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    return s

def entity_title(entity):
    return (getattr(entity, "title", None) or getattr(entity, "username", None) or getattr(entity, "first_name", None) or str(getattr(entity, "id", "?")))

async def safe_disconnect(client):
    try:
        if client and client.is_connected():
            await client.disconnect()
    except Exception as exc:
        logger.debug("Disconnect notice: %s", exc)

async def drop_pending(phone):
    entry = PENDING.pop(phone, None)
    if entry:
        await safe_disconnect(entry["client"])

def active_account_or_sole(phone):
    if phone and phone in ACTIVE:
        return phone, ACTIVE[phone]
    if not phone and len(ACTIVE) == 1:
        key = next(iter(ACTIVE))
        return key, ACTIVE[key]
    return None, None

def rpc_error_payload(exc):
    name = type(exc).__name__
    if isinstance(exc, errors.FloodWaitError):
        return 429, "FloodWait: retry after %s seconds" % exc.seconds
    if isinstance(exc, errors.PhoneNumberInvalidError):
        return 400, "Phone number is invalid on Telegram."
    if isinstance(exc, errors.PhoneNumberBannedError):
        return 403, "This phone number is banned from Telegram."
    if isinstance(exc, errors.PhoneCodeInvalidError):
        return 400, "OTP code is invalid. Check and retry."
    if isinstance(exc, errors.PhoneCodeExpiredError):
        return 410, "OTP code expired. Request a fresh code."
    if isinstance(exc, errors.PasswordHashInvalidError):
        return 400, "Two-factor password is incorrect."
    if isinstance(exc, errors.ApiIdInvalidError):
        return 400, "API_ID / API_HASH pair is invalid."
    return 500, "%s: %s" % (name, exc)

# ----------------------------------------------------------------------------
# Lifecycle
# ----------------------------------------------------------------------------

async def pending_sweeper():
    while True:
        await asyncio.sleep(120)
        now = time.time()
        stale = [p for p, e in list(PENDING.items()) if now - e["ts"] > PENDING_TTL]
        for phone in stale:
            logger.info("Sweeper: expiring stale OTP attempt for %s", mask_phone(phone))
            await drop_pending(phone)

@app.before_serving
async def boot():
    logger.info("Hypercorn worker online — Quart app booted, PID %s", os.getpid())
    logger.info("Routes armed: /api/send-otp /api/verify-otp /api/login-session /api/start-forwarder /api/stop-forwarder /api/logs /api/status /api/health")
    app.add_background_task(pending_sweeper)

@app.after_serving
async def shutdown():
    await stop_forwarder_internal()
    for phone in list(PENDING):
        await drop_pending(phone)
    for phone, entry in list(ACTIVE.items()):
        await safe_disconnect(entry["client"])
    ACTIVE.clear()
    logger.info("Shutdown complete — all Telethon clients disconnected.")

# ----------------------------------------------------------------------------
# CORS (drive the API from any dashboard origin)
# ----------------------------------------------------------------------------

@app.before_request
async def cors_preflight():
    if request.method == "OPTIONS":
        return "", 204

@app.after_request
async def cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

# ----------------------------------------------------------------------------
# API: OTP
# ----------------------------------------------------------------------------

@app.route("/api/send-otp", methods=["POST"])
async def api_send_otp():
    data = await request.get_json(force=True, silent=True) or {}
    try:
        api_id = int(str(data.get("api_id", "")).strip())
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "api_id must be an integer."}), 400
    api_hash = str(data.get("api_hash", "")).strip()
    phone = str(data.get("phone", "")).strip()
    if not api_hash or not phone:
        return jsonify({"ok": False, "error": "api_hash and phone are required."}), 400
    if phone in ACTIVE:
        acct = ACTIVE[phone]["account"]
        return jsonify({"ok": True, "already_connected": True, "account": acct, "message": "Session already authorized."})
    try:
        async with STATE_LOCK:
            await drop_pending(phone)
            client = TelegramClient(StringSession(), api_id, api_hash, device_model="TG Relay Web", system_version="Hypercorn/Quart", app_version="1.1.0")
            await asyncio.wait_for(client.connect(), timeout=25)
            logger.info("send-otp: MTProto connection up for %s", mask_phone(phone))
            sent = await client.send_code_request(phone)
            PENDING[phone] = {
                "client": client,
                "phone_code_hash": sent.phone_code_hash,
                "api_id": api_id,
                "api_hash": api_hash,
                "ts": time.time(),
            }
            code_type = type(sent.type).__name__
            logger.info("send-otp: code dispatched to %s via %s", mask_phone(phone), code_type)
            return jsonify({"ok": True, "message": "OTP dispatched via Telegram.", "code_type": code_type})
    except asyncio.TimeoutError:
        await drop_pending(phone)
        return jsonify({"ok": False, "error": "Timed out connecting to Telegram DC."}), 504
    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)
        logger.warning("send-otp failed for %s: %s", mask_phone(phone), msg)
        await drop_pending(phone)
        return jsonify({"ok": False, "error": msg}), status
    except Exception as exc:
        logger.exception("send-otp unexpected failure")
        await drop_pending(phone)
        return jsonify({"ok": False, "error": "Unexpected: %s" % exc}), 500

@app.route("/api/verify-otp", methods=["POST"])
async def api_verify_otp():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone", "")).strip()
    code = str(data.get("otp_code", "")).strip().replace(" ", "")
    password = str(data.get("password", "")).strip()
    if not phone or not code:
        return jsonify({"ok": False, "error": "phone and otp_code are required."}), 400
    entry = PENDING.get(phone)
    if not entry:
        return jsonify({"ok": False, "error": "No pending login for this phone. Request a new OTP first."}), 400
    if time.time() - entry["ts"] > PENDING_TTL:
        await drop_pending(phone)
        return jsonify({"ok": False, "error": "OTP attempt expired (10 min). Request a new code."}), 410
    client = entry["client"]
    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=25)
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=entry["phone_code_hash"])
        except errors.SessionPasswordNeededError:
            logger.info("verify: 2FA cloud password required for %s", mask_phone(phone))
            if not password:
                return jsonify({"ok": False, "need_password": True, "message": "Two-step verification is ON — provide the cloud password and submit again."})
            await client.sign_in(password=password)
        me = await client.get_me()
        string_session = client.session.save()
        account = {
            "id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": phone,
        }
        ACTIVE[phone] = {"client": client, "account": account, "connected_at": utcnow()}
        PENDING.pop(phone, None)
        logger.info("verify: CONNECTED %s as @%s (id=%s)", mask_phone(phone), me.username or "-", me.id)
        return jsonify({"ok": True, "connected": True, "account": account, "string_session": string_session, "message": "Account connected. Store the string session safely."})
    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)
        logger.warning("verify failed for %s: %s", mask_phone(phone), msg)
        if status == 410:
            await drop_pending(phone)
        return jsonify({"ok": False, "error": msg}), status
    except Exception as exc:
        logger.exception("verify unexpected failure")
        return jsonify({"ok": False, "error": "Unexpected: %s" % exc}), 500

# ----------------------------------------------------------------------------
# API: Session ID login (StringSession) — NEW in v1.1
# ----------------------------------------------------------------------------

@app.route("/api/login-session", methods=["POST"])
async def api_login_session():
    data = await request.get_json(force=True, silent=True) or {}
    try:
        api_id = int(str(data.get("api_id", "")).strip())
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "api_id must be an integer."}), 400
    api_hash = str(data.get("api_hash", "")).strip()
    session_string = str(data.get("session_string", "")).strip()
    if not api_hash or not session_string:
        return jsonify({"ok": False, "error": "api_hash and session_string are required."}), 400
    logger.info("session-login: verifying %s-char StringSession (blob redacted)", len(session_string))
    try:
        parsed = StringSession(session_string)
    except Exception:
        logger.warning("session-login: rejected — blob is not a decodable StringSession")
        return jsonify({"ok": False, "error": "Session string is malformed. Paste the exact output of StringSession.save() (starts with '1BQ…')."}), 400
    client = TelegramClient(parsed, api_id, api_hash, device_model="TG Relay Web", system_version="Hypercorn/Quart", app_version="1.1.0")
    try:
        await asyncio.wait_for(client.connect(), timeout=25)
        try:
            authorized = await client.is_user_authorized()
        except errors.AuthKeyUnregisteredError:
            authorized = False
        if not authorized:
            await safe_disconnect(client)
            logger.warning("session-login: auth key rejected by Telegram (expired/revoked)")
            return jsonify({"ok": False, "error": "Session is invalid or expired — Telegram rejected the auth key. Generate a fresh StringSession."}), 401
        me = await client.get_me()
        phone = "+" + me.phone if me.phone else "session-%s" % me.id
        if phone in ACTIVE:
            await safe_disconnect(client)
            logger.info("session-login: %s already active as @%s", mask_phone(phone), me.username or me.id)
            return jsonify({"ok": True, "already_connected": True, "account": ACTIVE[phone]["account"], "message": "Account already active."})
        account = {
            "id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": phone,
        }
        ACTIVE[phone] = {"client": client, "account": account, "connected_at": utcnow()}
        logger.info("session-login: CONNECTED %s as @%s (id=%s)", mask_phone(phone), me.username or "-", me.id)
        return jsonify({"ok": True, "connected": True, "account": account, "message": "Session verified and account connected."})
    except asyncio.TimeoutError:
        await safe_disconnect(client)
        return jsonify({"ok": False, "error": "Timed out connecting to Telegram DC."}), 504
    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)
        logger.warning("session-login RPC failure: %s", msg)
        await safe_disconnect(client)
        return jsonify({"ok": False, "error": msg}), status
    except Exception as exc:
        logger.exception("session-login unexpected failure")
        await safe_disconnect(client)
        return jsonify({"ok": False, "error": "Unexpected: %s" % exc}), 500

# ----------------------------------------------------------------------------
# API: Forwarder
# ----------------------------------------------------------------------------

async def stop_forwarder_internal():
    if FWD["handler"] and FWD["client"]:
        try:
            FWD["client"].remove_event_handler(FWD["handler"], FWD["builder"])
        except (ValueError, KeyError):
            pass
    was = FWD["running"]
    FWD.update({"running": False, "handler": None, "builder": None, "client": None, "phone": None, "source": None, "targets": [], "branding": "", "started_at": None})
    return was

@app.route("/api/start-forwarder", methods=["POST"])
async def api_start_forwarder():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone", "")).strip()
    branding = str(data.get("custom_branding_text", "")).strip()
    src_raw = str(data.get("source_channel_id", "")).strip()
    targets_raw = [t.strip() for t in str(data.get("target_channels", "")).split(",")]
    if not src_raw or not any(targets_raw):
        return jsonify({"ok": False, "error": "source_channel_id and at least one target channel are required."}), 400
    used_phone, entry = active_account_or_sole(phone)
    if not entry:
        return jsonify({"ok": False, "error": "No connected account. Complete OTP verification first."}), 409
    client = entry["client"]
    if not client.is_connected():
        await asyncio.wait_for(client.connect(), timeout=25)
    try:
        src_ident = parse_ident(src_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "source_channel_id is not parseable. Use -100… id, @username or t.me link."}), 422
    ident_list = []
    for raw in targets_raw:
        if not raw:
            continue
        try:
            ident_list.append(parse_ident(raw))
        except ValueError:
            return jsonify({"ok": False, "error": "Unparseable target: %s" % raw}), 422
    if not ident_list:
        return jsonify({"ok": False, "error": "No usable target channels supplied."}), 400
    try:
        source_entity = await client.get_entity(src_ident)
    except Exception:
        return jsonify({"ok": False, "error": "Source not visible to this account. Join it first (and be admin where needed)."}), 404
    resolved = []
    for ident in ident_list:
        try:
            ent = await client.get_entity(ident)
            resolved.append({"entity": ent, "title": entity_title(ent)})
        except Exception:
            logger.warning("start: target %s not resolvable by account", ident)
    if not resolved:
        return jsonify({"ok": False, "error": "None of the targets resolve. The account must be a member of each target."}), 404
    src_title = entity_title(source_entity)
    async with STATE_LOCK:
        if await stop_forwarder_internal():
            logger.info("start: detached previous forwarder binding (restart).")
        builder = events.NewMessage(chats=source_entity)
        async def relay(event):
            msg = event.message
            if getattr(msg, "action", None):
                return  # skip service messages
            body = (msg.message or "").strip()
            branded = (body + "\n\n" + branding).strip() if branding else body
            for target in FWD["targets"]:
                ent = target["entity"]
                try:
                    if msg.media is not None:
                        await client.send_file(ent, msg.media, caption=(branded or None))
                    else:
                        if not branded:
                            continue
                        await client.send_message(ent, branded, link_preview=False)
                    FWD["forwarded"] += 1
                    logger.info("relay: msg %s → %s", msg.id, target["title"])
                except errors.FloodWaitError as fw:
                    logger.warning("relay: FloodWait %ss on %s", fw.seconds, target["title"])
                except Exception as exc:
                    FWD["failed"] += 1
                    logger.error("relay: send to %s failed: %s", target["title"], exc)
        client.add_event_handler(relay, builder)
        FWD.update({
            "running": True,
            "handler": relay,
            "builder": builder,
            "client": client,
            "phone": used_phone,
            "source": {"title": src_title, "id": getattr(source_entity, "id", None)},
            "targets": resolved,
            "branding": branding,
            "forwarded": 0,
            "failed": 0,
            "started_at": utcnow(),
        })
        logger.info("start: FORWARDER LIVE %s → %s", src_title, ", ".join(t["title"] for t in resolved))
        return jsonify({
            "ok": True,
            "message": "Forwarder bound. Listening for new messages.",
            "source": {"title": src_title, "id": getattr(source_entity, "id", None)},
            "targets": [{"title": t["title"]} for t in resolved],
            "branding": branding,
        })

@app.route("/api/stop-forwarder", methods=["POST"])
async def api_stop_forwarder():
    async with STATE_LOCK:
        was = await stop_forwarder_internal()
        if was:
            logger.info("stop: forwarder stopped (forwarded=%s failed=%s)", FWD["forwarded"], FWD["failed"])
            return jsonify({"ok": True, "message": "Forwarder stopped. Handler detached."})
        return jsonify({"ok": False, "error": "Forwarder is not running."}), 409

# ----------------------------------------------------------------------------
# API: logs / status / health
# ----------------------------------------------------------------------------

@app.route("/api/logs", methods=["GET"])
async def api_logs():
    try:
        since = int(request.args.get("since", "0"))
    except ValueError:
        since = 0
    fresh = [row for row in LOG_BUFFER if row["id"] > since]
    return jsonify({"ok": True, "logs": fresh, "latest": LOG_SEQ[0]})

@app.route("/api/status", methods=["GET"])
async def api_status():
    accounts = []
    for phone, entry in ACTIVE.items():
        a = dict(entry["account"])
        a["phone"] = mask_phone(a.get("phone", phone))
        a["connected_at"] = entry["connected_at"]
        accounts.append(a)
    return jsonify({
        "ok": True,
        "server_time": utcnow(),
        "accounts": accounts,
        "pending_logins": len(PENDING),
        "forwarder": {
            "running": FWD["running"],
            "phone": mask_phone(FWD["phone"]) if FWD["phone"] else None,
            "source": FWD["source"],
            "targets": [t["title"] for t in FWD["targets"]],
            "branding": FWD["branding"],
            "forwarded": FWD["forwarded"],
            "failed": FWD["failed"],
            "started_at": FWD["started_at"],
        },
    })

@app.route("/api/health", methods=["GET"])
async def api_health():
    return jsonify({"ok": True, "ts": utcnow()})

# ----------------------------------------------------------------------------
# Embedded dashboard
# ----------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>TG Relay Control Deck</title>
</head>
<body>
    <h1>TG Relay Control Deck</h1>
    <p>Dashboard template placeholder.</p>
</body>
</html>"""

@app.route("/", methods=["GET"])
async def dashboard():
    return await render_template_string(DASHBOARD_HTML)

# ----------------------------------------------------------------------------
# Entrypoint (local dev). On Render: hypercorn main:app --bind 0.0.0.0:$PORT
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
