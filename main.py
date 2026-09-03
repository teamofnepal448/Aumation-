# ============================================================================
# DEVIL ENGINE v6.0
# Telegram multi-account automation + dual-panel paid content platform
# Quart + Telethon + Hypercorn | single-file ASGI app (UI embedded)
#
# Deploy on Render:
#   build:  pip install -r requirements.txt
#   start:  hypercorn main:app --bind 0.0.0.0:$PORT
#
# Environment (optional):
#   ADMIN_USERNAME (default "admin")
#   ADMIN_PASSWORD (default "devil@5000")
# ============================================================================

import os
import re
import json
import time
import hmac
import secrets
import inspect
import asyncio
import logging
import traceback
from functools import wraps
from collections import deque
from datetime import datetime, timezone

from quart import Quart, request, jsonify, render_template_string
from telethon import TelegramClient, events, errors, utils, types
from telethon.sessions import StringSession

# ----------------------------------------------------------------------------
# Logging: tagged ring buffer + stdout (Render captures the same stream as UI)
# ----------------------------------------------------------------------------

LOG_BUFFER = deque(maxlen=600)
LOG_SEQ = [0]


class BufferLogHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_SEQ[0] += 1
            LOG_BUFFER.append({
                "id": LOG_SEQ[0],
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "tag": getattr(record, "tag", "core"),
                "message": record.getMessage(),
            })
        except Exception:
            self.handleError(record)


logger = logging.getLogger("devil")
logger.setLevel(logging.DEBUG)
logger.propagate = False
logger.addHandler(BufferLogHandler())
_stdout = logging.StreamHandler()
_stdout.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
logger.addHandler(_stdout)


def emit(level, message, *args, **kwargs):
    tag = kwargs.pop("tag", "core")
    getattr(logger, level.lower(), logger.info)(message, *args, extra={"tag": tag})


# ----------------------------------------------------------------------------
# App + state
# ----------------------------------------------------------------------------

app = Quart(__name__)
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024   # 8 MB (QR uploads)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_FILE = os.path.join(BASE_DIR, "sessions.json")
TEMPLATES_FILE = os.path.join(BASE_DIR, "templates.json")
PREDICTIONS_FILE = os.path.join(BASE_DIR, "predictions.json")
PAYMENTS_FILE = os.path.join(BASE_DIR, "payments.json")
ORDERS_FILE = os.path.join(BASE_DIR, "orders.json")

ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "devil@5000")
ADMIN_TOKENS = {}                 # token -> expiry epoch
ADMIN_TOKEN_TTL = 12 * 3600

PENDING = {}                      # phone -> pending OTP attempt
ACCOUNTS = {}                     # phone -> account record (the ACTIVE registry)
RUNNING = {}                      # phone -> running script task record
PENDING_TTL = 600.0
STATE_LOCK = asyncio.Lock()

CATEGORIES = ("session", "match", "toss", "combo")
CATEGORY_LABELS = {
    "session": "Session",
    "match": "Match",
    "toss": "Toss",
    "combo": "All-in Combo",
}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def mask_phone(phone):
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) <= 4:
        return "***"
    return "+" + digits[:3] + "******" + digits[-2:]


def read_json_file(path, fallback=None):
    default = {} if fallback is None else fallback
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, type(default)):
                return data
            return default
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, OSError) as exc:
        emit("WARNING", "Broken JSON store %s (%s) — starting empty", path, exc)
        return default


def write_json_file(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)
    os.replace(tmp, path)


def new_id(prefix):
    return "%s_%s" % (prefix, secrets.token_hex(5))


# ----------------------------------------------------------------------------
# Telethon helpers
# ----------------------------------------------------------------------------

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


async def safe_disconnect(client):
    try:
        if client and client.is_connected():
            await client.disconnect()
    except Exception as exc:
        logger.debug("disconnect notice: %s", exc)


async def drop_pending(phone):
    entry = PENDING.pop(phone, None)
    if entry:
        await safe_disconnect(entry["client"])


def persist_account(phone):
    entry = ACCOUNTS[phone]
    store = read_json_file(SESSIONS_FILE)
    store[phone] = {
        "session": entry["session_string"],
        "api_id": entry["api_id"],
        "api_hash": entry["api_hash"],
        "saved_at": utcnow(),
    }
    write_json_file(SESSIONS_FILE, store)
    emit("INFO", "sessions.json: persisted %s", mask_phone(phone), tag=phone)


def unpersist_account(phone):
    store = read_json_file(SESSIONS_FILE)
    if phone in store:
        store.pop(phone)
        write_json_file(SESSIONS_FILE, store)


# ----------------------------------------------------------------------------
# Script execution engine
# ----------------------------------------------------------------------------

def build_script_env(phone):
    acct = ACCOUNTS[phone]

    def tprint(*args, **_kwargs):
        emit("INFO", "%s", " ".join(str(a) for a in args), tag=phone)

    def tlog(msg, level="INFO"):
        emit(str(level).upper(), "%s", str(msg), tag=phone)

    return {
        "asyncio": asyncio,
        "time": time,
        "re": re,
        "json": json,
        "datetime": datetime,
        "client": acct["client"],
        "SESSION_STRING": acct["session_string"],
        "PHONE": phone,
        "events": events,
        "errors": errors,
        "utils": utils,
        "types": types,
        "TelegramClient": TelegramClient,
        "StringSession": StringSession,
        "print": tprint,
        "log": tlog,
    }


def handlers_snapshot(client):
    return set((id(cb), id(builder)) for cb, builder in client.list_event_handlers())


def purge_script_handlers(client, snapshot, phone):
    removed = 0
    for cb, builder in client.list_event_handlers():
        if (id(cb), id(builder)) not in snapshot:
            try:
                client.remove_event_handler(cb, builder)
                removed += 1
            except (ValueError, KeyError):
                pass
    if removed:
        emit("INFO", "Purged %s script-registered handler(s)", removed, tag=phone)


async def script_engine(phone, name, code, token):
    acct = ACCOUNTS[phone]
    client = acct["client"]
    snapshot = handlers_snapshot(client)
    emit("INFO", "Engine online: '%s' compiling…", name, tag=phone)
    try:
        compiled = compile(code, "<%s>" % (name or "script"), "exec")
        env = build_script_env(phone)
        exec(compiled, env, env)
        emit("INFO", "'%s' module-level executed", name, tag=phone)

        main_fn = env.get("main")
        main_ran = False
        if inspect.iscoroutinefunction(main_fn):
            emit("INFO", "'%s': async main() detected — awaiting", name, tag=phone)
            await main_fn()
            main_ran = True
            emit("INFO", "'%s': main() returned", name, tag=phone)

        live = handlers_snapshot(client) - snapshot
        if live:
            emit("INFO", "'%s': %s handler(s) armed — task idles until stopped",
                 name, len(live), tag=phone)
            while True:
                await asyncio.sleep(3600)
        elif main_ran:
            emit("INFO", "'%s': work complete, no handlers — exiting", name, tag=phone)
        else:
            emit("WARNING", "'%s' registered nothing and has no main() — exiting",
                 name, tag=phone)
    except asyncio.CancelledError:
        emit("WARNING", "'%s' cancelled by operator", name, tag=phone)
        raise
    except SyntaxError as se:
        emit("ERROR", "'%s' SyntaxError line %s: %s", name, se.lineno, se.msg, tag=phone)
    except Exception:
        trace = " | ".join(traceback.format_exc(limit=5).splitlines()[-4:])
        emit("ERROR", "'%s' crashed: %s", name, trace, tag=phone)
    finally:
        purge_script_handlers(client, snapshot, phone)
        cur = RUNNING.get(phone)
        if cur and cur.get("token") == token:
            RUNNING.pop(phone, None)
        emit("INFO", "Engine halted: '%s'", name, tag=phone)


def on_task_done(phone, name):
    def _cb(task):
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            emit("ERROR", "Task '%s' died outside envelope: %s", name, exc, tag=phone)
    return _cb


async def stop_account_task(phone, reason="operator"):
    entry = RUNNING.pop(phone, None)
    if not entry:
        return False
    entry["task"].cancel()
    try:
        await asyncio.wait_for(entry["task"], timeout=4)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    except Exception as exc:
        emit("WARNING", "Teardown raised during %s stop: %s", reason, exc, tag=phone)
    return True


# ----------------------------------------------------------------------------
# Lifecycle
# ----------------------------------------------------------------------------

async def restore_sessions():
    store = read_json_file(SESSIONS_FILE)
    if not store:
        emit("INFO", "No saved sessions — waiting for first authorization")
        return
    for phone, row in list(store.items()):
        try:
            client = TelegramClient(
                StringSession(row["session"]), int(row["api_id"]), row["api_hash"],
                device_model="Devil Engine", system_version="Hypercorn/Quart",
                app_version="6.0.0")
            await asyncio.wait_for(client.connect(), timeout=25)
            if not await client.is_user_authorized():
                raise ValueError("session dead (auth key unregistered)")
            me = await client.get_me()
            ACCOUNTS[phone] = {
                "client": client,
                "account": {"id": me.id, "username": me.username,
                            "first_name": me.first_name, "phone": phone},
                "session_string": row["session"],
                "api_id": int(row["api_id"]),
                "api_hash": row["api_hash"],
                "connected_at": utcnow(),
            }
            emit("INFO", "Session restored: %s as @%s", mask_phone(phone),
                 me.username or me.id, tag=phone)
        except Exception as exc:
            emit("WARNING", "Restore failed for %s: %s — dropping",
                 mask_phone(phone), exc)
            unpersist_account(phone)


async def pending_sweeper():
    while True:
        await asyncio.sleep(120)
        now = time.time()
        for phone in [p for p, e in list(PENDING.items())
                      if now - e["ts"] > PENDING_TTL]:
            emit("INFO", "Sweeper: expiring stale OTP attempt for %s", mask_phone(phone))
            await drop_pending(phone)
        for tok in [t for t, exp in list(ADMIN_TOKENS.items()) if exp < now]:
            ADMIN_TOKENS.pop(tok, None)


@app.before_serving
async def boot():
    emit("INFO", "DEVIL ENGINE v6.0 — Hypercorn worker online, PID %s", os.getpid())
    emit("INFO", "Panels: /  (engine + admin + customer)   Admin user: %s", ADMIN_USERNAME)
    app.add_background_task(restore_sessions)
    app.add_background_task(pending_sweeper)


@app.after_serving
async def shutdown():
    for phone in list(RUNNING):
        await stop_account_task(phone, reason="shutdown")
    for phone in list(PENDING):
        await drop_pending(phone)
    for phone, entry in list(ACCOUNTS.items()):
        await safe_disconnect(entry["client"])
        ACCOUNTS.pop(phone, None)
    emit("INFO", "Shutdown complete — tasks cancelled, clients disconnected.")


@app.before_request
async def cors_preflight():
    if request.method == "OPTIONS":
        return "", 204


@app.after_request
async def cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Admin-Token"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


# ----------------------------------------------------------------------------
# API: authentication (OTP)
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

    if phone in ACCOUNTS:
        return jsonify({"ok": True, "already_connected": True,
                        "account": ACCOUNTS[phone]["account"],
                        "message": "Account already authorized."})
    try:
        async with STATE_LOCK:
            await drop_pending(phone)
            client = TelegramClient(StringSession(), api_id, api_hash,
                                    device_model="Devil Engine",
                                    system_version="Hypercorn/Quart",
                                    app_version="6.0.0")
            await asyncio.wait_for(client.connect(), timeout=25)
            emit("INFO", "send-otp: MTProto up for %s", mask_phone(phone), tag=phone)
            sent = await client.send_code_request(phone)
            PENDING[phone] = {"client": client,
                              "phone_code_hash": sent.phone_code_hash,
                              "api_id": api_id, "api_hash": api_hash,
                              "ts": time.time()}
        emit("INFO", "send-otp: code dispatched via %s",
             type(sent.type).__name__, tag=phone)
        return jsonify({"ok": True, "message": "OTP dispatched via Telegram.",
                        "code_type": type(sent.type).__name__})
    except asyncio.TimeoutError:
        await drop_pending(phone)
        return jsonify({"ok": False, "error": "Timed out connecting to Telegram DC."}), 504
    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)
        emit("WARNING", "send-otp failed: %s", msg, tag=phone)
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
        return jsonify({"ok": False, "error": "No pending login. Request a new OTP."}), 400
    if time.time() - entry["ts"] > PENDING_TTL:
        await drop_pending(phone)
        return jsonify({"ok": False, "error": "OTP attempt expired. Request a new code."}), 410

    client = entry["client"]
    try:
        if not client.is_connected():
            await asyncio.wait_for(client.connect(), timeout=25)
        try:
            await client.sign_in(phone=phone, code=code,
                                 phone_code_hash=entry["phone_code_hash"])
        except errors.SessionPasswordNeededError:
            emit("INFO", "verify: 2FA cloud password required", tag=phone)
            if not password:
                return jsonify({"ok": False, "need_password": True,
                                "message": "Two-step verification is ON — provide the "
                                           "cloud password and submit again."})
            await client.sign_in(password=password)

        me = await client.get_me()
        session_string = client.session.save()
        ACCOUNTS[phone] = {
            "client": client,
            "account": {"id": me.id, "username": me.username,
                        "first_name": me.first_name, "phone": phone},
            "session_string": session_string,
            "api_id": entry["api_id"], "api_hash": entry["api_hash"],
            "connected_at": utcnow(),
        }
        PENDING.pop(phone, None)
        persist_account(phone)
        emit("INFO", "ACCOUNT SAVED: @%s (id=%s)", me.username or "-", me.id, tag=phone)
        return jsonify({"ok": True, "connected": True,
                        "account": ACCOUNTS[phone]["account"],
                        "message": "Session saved. Account ready for scripting."})
    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)
        emit("WARNING", "verify failed: %s", msg, tag=phone)
        if status == 410:
            await drop_pending(phone)
        return jsonify({"ok": False, "error": msg}), status
    except Exception as exc:
        logger.exception("verify unexpected failure")
        return jsonify({"ok": False, "error": "Unexpected: %s" % exc}), 500


# ----------------------------------------------------------------------------
# API: Session ID login (StringSession)
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
        return jsonify({"ok": False,
                        "error": "api_hash and session_string are required."}), 400

    # SECURITY: never log the blob itself — length only.
    emit("INFO", "session-login: verifying %s-char StringSession (blob redacted)",
         len(session_string))
    try:
        parsed = StringSession(session_string)
    except Exception:
        emit("WARNING", "session-login: blob is not a de
