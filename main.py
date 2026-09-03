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
        emit("WARNING", "session-login: blob is not a decodable StringSession")
        return jsonify({"ok": False,
                        "error": "Session string is malformed. Paste the exact output "
                                 "of StringSession.save()."}), 400

    client = TelegramClient(parsed, api_id, api_hash,
                            device_model="Devil Engine",
                            system_version="Hypercorn/Quart",
                            app_version="6.0.0")
    try:
        await asyncio.wait_for(client.connect(), timeout=25)
        try:
            authorized = await client.is_user_authorized()
        except errors.AuthKeyUnregisteredError:
            authorized = False
        if not authorized:
            await safe_disconnect(client)
            emit("WARNING", "session-login: auth key rejected (expired/revoked)")
            return jsonify({"ok": False,
                            "error": "Session is invalid or expired — Telegram rejected "
                                     "the auth key. Generate a fresh StringSession."}), 401

        me = await client.get_me()
        phone = "+" + me.phone if me.phone else "session-%s" % me.id
        if phone in ACCOUNTS:
            await safe_disconnect(client)
            emit("INFO", "session-login: %s already active", mask_phone(phone), tag=phone)
            return jsonify({"ok": True, "already_connected": True,
                            "account": ACCOUNTS[phone]["account"],
                            "message": "Account already active."})

        ACCOUNTS[phone] = {
            "client": client,
            "account": {"id": me.id, "username": me.username,
                        "first_name": me.first_name, "phone": phone},
            "session_string": session_string,
            "api_id": api_id, "api_hash": api_hash,
            "connected_at": utcnow(),
        }
        persist_account(phone)
        emit("INFO", "SESSION LOGIN OK: @%s (id=%s)", me.username or "-", me.id, tag=phone)
        # SECURITY: response contains the account record only — no session string.
        return jsonify({"ok": True, "connected": True,
                        "account": ACCOUNTS[phone]["account"],
                        "message": "Session verified and account saved."})
    except asyncio.TimeoutError:
        await safe_disconnect(client)
        return jsonify({"ok": False, "error": "Timed out connecting to Telegram DC."}), 504
    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)
        emit("WARNING", "session-login RPC failure: %s", msg)
        await safe_disconnect(client)
        return jsonify({"ok": False, "error": msg}), status
    except Exception as exc:
        logger.exception("session-login unexpected failure")
        await safe_disconnect(client)
        return jsonify({"ok": False, "error": "Unexpected: %s" % exc}), 500


@app.route("/api/disconnect-account", methods=["POST"])
async def api_disconnect_account():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone", "")).strip()
    entry = ACCOUNTS.get(phone)
    if not entry:
        return jsonify({"ok": False, "error": "No such account."}), 404
    await stop_account_task(phone, reason="disconnect")
    await safe_disconnect(entry["client"])
    emit("INFO", "Account disconnected (session file kept): %s",
         mask_phone(phone), tag=phone)
    return jsonify({"ok": True, "message": "Client disconnected. Session file retained."})


@app.route("/api/delete-account", methods=["POST"])
async def api_delete_account():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone", "")).strip()
    stopped = await stop_account_task(phone, reason="account-delete")
    entry = ACCOUNTS.pop(phone, None)
    if not entry:
        return jsonify({"ok": False, "error": "No such account."}), 404
    await safe_disconnect(entry["client"])
    unpersist_account(phone)
    emit("INFO", "Account deleted: %s", mask_phone(phone), tag=phone)
    return jsonify({"ok": True, "message": "Account removed and session purged.",
                    "stopped_script": stopped})


# ----------------------------------------------------------------------------
# API: script engine
# ----------------------------------------------------------------------------

@app.route("/api/run-script", methods=["POST"])
async def api_run_script():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone_number", "") or data.get("phone", "")).strip()
    code = str(data.get("script_code", ""))
    name = str(data.get("script_name", "")).strip() or "unnamed-script"

    if not phone:
        return jsonify({"ok": False, "error": "phone_number is required."}), 400
    if not code.strip():
        return jsonify({"ok": False, "error": "script_code is empty."}), 400
    acct = ACCOUNTS.get(phone)
    if not acct:
        return jsonify({"ok": False, "error": "Account not authorized."}), 409

    try:
        compile(code, "<precheck>", "exec")
    except SyntaxError as se:
        return jsonify({"ok": False,
                        "error": "SyntaxError line %s: %s" % (se.lineno, se.msg)}), 400

    client = acct["client"]
    if not client.is_connected():
        await asyncio.wait_for(client.connect(), timeout=25)

    async with STATE_LOCK:
        if phone in RUNNING:
            emit("WARNING", "Replacing running script '%s' with '%s'",
                 RUNNING[phone]["name"], name, tag=phone)
            await stop_account_task(phone, reason="replace")
        token = "%s-%s" % (int(time.time() * 1000), secrets.token_hex(3))
        task = asyncio.create_task(script_engine(phone, name, code, token),
                                   name="engine:%s" % phone)
        task.add_done_callback(on_task_done(phone, name))
        RUNNING[phone] = {"task": task, "token": token, "name": name,
                          "started_at": utcnow()}
    emit("INFO", "Task registered: '%s'", name, tag=phone)
    return jsonify({"ok": True, "message": "Script installed and running.",
                    "script_name": name, "phone": phone,
                    "started_at": RUNNING[phone]["started_at"]})


@app.route("/api/stop-script", methods=["POST"])
async def api_stop_script():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone_number", "") or data.get("phone", "")).strip()
    if not phone:
        return jsonify({"ok": False, "error": "phone_number is required."}), 400
    async with STATE_LOCK:
        stopped = await stop_account_task(phone, reason="operator")
    if not stopped:
        return jsonify({"ok": False, "error": "No running script on this account."}), 409
    return jsonify({"ok": True, "message": "Task cancelled — handlers purged."})


@app.route("/api/save-template", methods=["POST"])
async def api_save_template():
    data = await request.get_json(force=True, silent=True) or {}
    name = str(data.get("name", "")).strip()
    code = str(data.get("code", ""))
    if not name or not code.strip():
        return jsonify({"ok": False, "error": "name and code are required."}), 400
    store = read_json_file(TEMPLATES_FILE)
    store[name] = {"code": code, "saved_at": utcnow()}
    write_json_file(TEMPLATES_FILE, store)
    emit("INFO", "Template installed: '%s' (%s bytes)", name, len(code))
    return jsonify({"ok": True, "message": "Template '%s' saved." % name})


@app.route("/api/templates", methods=["GET"])
async def api_templates():
    store = read_json_file(TEMPLATES_FILE)
    return jsonify({"ok": True,
                    "templates": dict((k, v.get("code", "")) for k, v in store.items())})


@app.route("/api/delete-template", methods=["POST"])
async def api_delete_template():
    data = await request.get_json(force=True, silent=True) or {}
    name = str(data.get("name", "")).strip()
    store = read_json_file(TEMPLATES_FILE)
    if name not in store:
        return jsonify({"ok": False, "error": "No such template."}), 404
    store.pop(name)
    write_json_file(TEMPLATES_FILE, store)
    return jsonify({"ok": True, "message": "Template deleted."})


# ----------------------------------------------------------------------------
# Admin authentication
# ----------------------------------------------------------------------------

def issue_admin_token():
    token = secrets.token_urlsafe(32)
    ADMIN_TOKENS[token] = time.time() + ADMIN_TOKEN_TTL
    return token


def valid_admin_token(token):
    exp = ADMIN_TOKENS.get(token)
    if not exp:
        return False
    if exp < time.time():
        ADMIN_TOKENS.pop(token, None)
        return False
    return True


def admin_required(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        token = request.headers.get("X-Admin-Token", "")
        if not valid_admin_token(token):
            return jsonify({"ok": False,
                            "error": "Admin authentication required."}), 401
        return await fn(*args, **kwargs)
    return wrapper


@app.route("/api/admin/login", methods=["POST"])
async def api_admin_login():
    data = await request.get_json(force=True, silent=True) or {}
    username = str(data.get("username", ""))
    password = str(data.get("password", ""))
    ok_user = hmac.compare_digest(username, ADMIN_USERNAME)
    ok_pass = hmac.compare_digest(password, ADMIN_PASSWORD)
    if not (ok_user and ok_pass):
        emit("WARNING", "admin: failed login attempt for '%s'", username[:24])
        return jsonify({"ok": False, "error": "Invalid admin credentials."}), 401
    token = issue_admin_token()
    emit("INFO", "admin: '%s' authenticated", ADMIN_USERNAME)
    return jsonify({"ok": True, "token": token, "expires_in": ADMIN_TOKEN_TTL,
                    "message": "Admin session started."})


@app.route("/api/admin/logout", methods=["POST"])
async def api_admin_logout():
    ADMIN_TOKENS.pop(request.headers.get("X-Admin-Token", ""), None)
    return jsonify({"ok": True, "message": "Admin session closed."})


# ----------------------------------------------------------------------------
# Admin: paid content manager
# ----------------------------------------------------------------------------

def public_prediction(row):
    return {
        "id": row.get("id"),
        "match_name": row.get("match_name"),
        "category": row.get("category"),
        "category_label": CATEGORY_LABELS.get(row.get("category"), "Match"),
        "match_datetime": row.get("match_datetime"),
        "description": row.get("description"),
        "price": row.get("price"),
        "status": row.get("status", "open"),
        "created_at": row.get("created_at"),
    }


@app.route("/api/admin/predictions", methods=["GET"])
@admin_required
async def api_admin_predictions():
    store = read_json_file(PREDICTIONS_FILE)
    rows = sorted(store.values(), key=lambda r: r.get("created_at", ""), reverse=True)
    return jsonify({"ok": True, "predictions": rows})


@app.route("/api/admin/save-prediction", methods=["POST"])
@admin_required
async def api_admin_save_prediction():
    data = await request.get_json(force=True, silent=True) or {}
    match_name = str(data.get("match_name", "")).strip()
    category = str(data.get("category", "match")).strip().lower()
    if not match_name:
        return jsonify({"ok": False, "error": "match_name is required."}), 400
    if category not in CATEGORIES:
        return jsonify({"ok": False,
                        "error": "category must be one of %s." % ", ".join(CATEGORIES)}), 400
    try:
        price = int(float(str(data.get("price", 0)).strip() or 0))
    except ValueError:
        return jsonify({"ok": False, "error": "price must be numeric."}), 400

    store = read_json_file(PREDICTIONS_FILE)
    pid = str(data.get("id", "")).strip() or new_id("mtc")
    existing = store.get(pid, {})
    locked = data.get("locked_content") or {}
    row = {
        "id": pid,
        "match_name": match_name,
        "category": category,
        "match_datetime": str(data.get("match_datetime", "")).strip(),
        "description": str(data.get("description", "")).strip(),
        "price": price,
        "status": str(data.get("status", existing.get("status", "open"))).strip(),
        "locked_content": {
            "winner": str(locked.get("winner", "")).strip(),
            "toss": str(locked.get("toss", "")).strip(),
            "session": str(locked.get("session", "")).strip(),
            "notes": str(locked.get("notes", "")).strip(),
        },
        "created_at": existing.get("created_at", utcnow()),
        "updated_at": utcnow(),
    }
    store[pid] = row
    write_json_file(PREDICTIONS_FILE, store)
    emit("INFO", "admin: prediction saved '%s' [%s] ₹%s", match_name, category, price)
    return jsonify({"ok": True, "prediction": row,
                    "message": "Prediction saved and published."})


@app.route("/api/admin/delete-prediction", methods=["POST"])
@admin_required
async def api_admin_delete_prediction():
    data = await request.get_json(force=True, silent=True) or {}
    pid = str(data.get("id", "")).strip()
    store = read_json_file(PREDICTIONS_FILE)
    if pid not in store:
        return jsonify({"ok": False, "error": "No such prediction."}), 404
    removed = store.pop(pid)
    write_json_file(PREDICTIONS_FILE, store)
    emit("INFO", "admin: prediction deleted '%s'", removed.get("match_name"))
    return jsonify({"ok": True, "message": "Prediction deleted."})


DEFAULT_PAYMENT = {
    "upi_id": "",
    "payee_name": "",
    "default_price": 499,
    "instructions": "Pay the exact amount, then submit your UTR. "
                    "Access unlocks after admin verification.",
    "qr_data_url": "",
}


@app.route("/api/admin/payment-config", methods=["GET"])
@admin_required
async def api_admin_payment_get():
    cfg = dict(DEFAULT_PAYMENT)
    cfg.update(read_json_file(PAYMENTS_FILE))
    return jsonify({"ok": True, "config": cfg})


@app.route("/api/admin/payment-config", methods=["POST"])
@admin_required
async def api_admin_payment_set():
    data = await request.get_json(force=True, silent=True) or {}
    cfg = dict(DEFAULT_PAYMENT)
    cfg.update(read_json_file(PAYMENTS_FILE))
    try:
        price = int(float(str(data.get("default_price", cfg["default_price"])) or 0))
    except ValueError:
        return jsonify({"ok": False, "error": "default_price must be numeric."}), 400
    qr = str(data.get("qr_data_url", cfg.get("qr_data_url", "")))
    if qr and not qr.startswith("data:image/"):
        return jsonify({"ok": False, "error": "qr_data_url must be an image data URL."}), 400
    if len(qr) > 3_000_000:
        return jsonify({"ok": False, "error": "QR image too large (max ~3 MB)."}), 413
    cfg.update({
        "upi_id": str(data.get("upi_id", cfg["upi_id"])).strip(),
        "payee_name": str(data.get("payee_name", cfg["payee_name"])).strip(),
        "default_price": price,
        "instructions": str(data.get("instructions", cfg["instructions"])).strip(),
        "qr_data_url": qr,
        "updated_at": utcnow(),
    })
    write_json_file(PAYMENTS_FILE, cfg)
    emit("INFO", "admin: payment config updated (upi=%s, qr=%s)",
         cfg["upi_id"] or "-", "yes" if qr else "no")
    return jsonify({"ok": True, "config": cfg, "message": "Payment settings saved."})


@app.route("/api/admin/orders", methods=["GET"])
@admin_required
async def api_admin_orders():
    store = read_json_file(ORDERS_FILE)
    rows = sorted(store.values(), key=lambda r: r.get("created_at", ""), reverse=True)
    pending = len([r for r in rows if r.get("status") == "pending"])
    return jsonify({"ok": True, "orders": rows, "pending": pending})


@app.route("/api/admin/review-order", methods=["POST"])
@admin_required
async def api_admin_review_order():
    data = await request.get_json(force=True, silent=True) or {}
    oid = str(data.get("id", "")).strip()
    action = str(data.get("action", "")).strip().lower()
    if action not in ("approve", "reject"):
        return jsonify({"ok": False, "error": "action must be approve or reject."}), 400
    store = read_json_file(ORDERS_FILE)
    row = store.get(oid)
    if not row:
        return jsonify({"ok": False, "error": "No such order."}), 404
    row["status"] = "approved" if action == "approve" else "rejected"
    row["reviewed_at"] = utcnow()
    store[oid] = row
    write_json_file(ORDERS_FILE, store)
    emit("INFO", "admin: order %s %s for %s", oid, row["status"], row.get("match_name"))
    return jsonify({"ok": True, "order": row,
                    "message": "Order %s." % row["status"]})


# ----------------------------------------------------------------------------
# Customer panel API
# ----------------------------------------------------------------------------

@app.route("/api/customer/matches", methods=["GET"])
async def api_customer_matches():
    store = read_json_file(PREDICTIONS_FILE)
    rows = [public_prediction(r) for r in store.values()]
    rows.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    cfg = dict(DEFAULT_PAYMENT)
    cfg.update(read_json_file(PAYMENTS_FILE))
    payment = {
        "upi_id": cfg.get("upi_id", ""),
        "payee_name": cfg.get("payee_name", ""),
        "default_price": cfg.get("default_price", 0),
        "instructions": cfg.get("instructions", ""),
        "qr_data_url": cfg.get("qr_data_url", ""),
    }
    # SECURITY: locked_content is never included in this response.
    return jsonify({"ok": True, "matches": rows, "payment": payment})


@app.route("/api/customer/purchase", methods=["POST"])
async def api_customer_purchase():
    data = await request.get_json(force=True, silent=True) or {}
    match_id = str(data.get("match_id", "")).strip()
    name = str(data.get("customer_name", "")).strip()
    contact = str(data.get("contact", "")).strip()
    utr = str(data.get("utr", "")).strip()
    if not (match_id and name and contact and utr):
        return jsonify({"ok": False,
                        "error": "match_id, customer_name, contact and utr are required."}), 400

    predictions = read_json_file(PREDICTIONS_FILE)
    match = predictions.get(match_id)
    if not match:
        return jsonify({"ok": False, "error": "Match not found."}), 404
    if match.get("status") != "open":
        return jsonify({"ok": False, "error": "This match is closed for purchase."}), 409

    orders = read_json_file(ORDERS_FILE)
    for row in orders.values():
        if (row.get("match_id") == match_id
                and row.get("contact", "").lower() == contact.lower()
                and row.get("status") in ("pending", "approved")):
            return jsonify({"ok": True, "duplicate": True, "order": row,
                            "access_code": row.get("access_code"),
                            "message": "You already have a %s order for this match."
                                       % row.get("status")})

    oid = new_id("ord")
    order = {
        "id": oid,
        "match_id": match_id,
        "match_name": match.get("match_name"),
        "category": match.get("category"),
        "customer_name": name,
        "contact": contact,
        "utr": utr,
        "amount": match.get("price", 0),
        "status": "pending",
        "access_code": secrets.token_hex(3).upper(),
        "created_at": utcnow(),
        "reviewed_at": None,
    }
    orders[oid] = order
    write_json_file(ORDERS_FILE, orders)
    emit("INFO", "customer: payment proof submitted for '%s' by %s (UTR %s…)",
         match.get("match_name"), name, utr[:4])
    return jsonify({"ok": True, "order": order, "access_code": order["access_code"],
                    "message": "Payment proof submitted. Awaiting admin verification."})


@app.route("/api/customer/unlock", methods=["POST"])
async def api_customer_unlock():
    data = await request.get_json(force=True, silent=True) or {}
    lookup = str(data.get("lookup", "")).strip()
    if not lookup:
        return jsonify({"ok": False, "error": "Provide your contact or access code."}), 400

    orders = read_json_file(ORDERS_FILE)
    predictions = read_json_file(PREDICTIONS_FILE)
    needle = lookup.lower()
    mine = [r for r in orders.values()
            if r.get("contact", "").lower() == needle
            or r.get("access_code", "").lower() == needle]
    if not mine:
        return jsonify({"ok": True, "unlocked": [], "pending": [],
                        "message": "No orders found for that contact or code."})

    unlocked, pending = [], []
    for row in sorted(mine, key=lambda r: r.get("created_at", ""), reverse=True):
        match = predictions.get(row.get("match_id"))
        card = {
            "order_id": row.get("id"),
            "match_name": row.get("match_name"),
            "category": row.get("category"),
            "category_label": CATEGORY_LABELS.get(row.get("category"), "Match"),
            "amount": row.get("amount"),
            "status": row.get("status"),
            "access_code": row.get("access_code"),
            "created_at": row.get("created_at"),
        }
        if row.get("status") == "approved" and match:
            card["match_datetime"] = match.get("match_datetime")
            card["description"] = match.get("description")
            # Locked boxes are released ONLY for approved orders.
            card["locked_content"] = match.get("locked_content", {})
            unlocked.append(card)
        else:
            pending.append(card)
    emit("INFO", "customer: unlock check — %s approved, %s pending",
         len(unlocked), len(pending))
    return jsonify({"ok": True, "unlocked": unlocked, "pending": pending,
                    "message": "%s unlocked, %s awaiting approval."
                               % (len(unlocked), len(pending))})


# ----------------------------------------------------------------------------
# Telemetry
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
    for phone, entry in ACCOUNTS.items():
        a = dict(entry["account"])
        a["connected_at"] = entry["connected_at"]
        a["connected"] = entry["client"].is_connected()
        a["running_script"] = RUNNING[phone]["name"] if phone in RUNNING else None
        accounts.append(a)
    processes = [{"phone": phone, "script_name": row["name"],
                  "started_at": row["started_at"], "token_tail": row["token"][-6:]}
                 for phone, row in RUNNING.items()]
    orders = read_json_file(ORDERS_FILE)
    return jsonify({
        "ok": True, "server_time": utcnow(),
        "accounts": accounts, "processes": processes,
        "pending_logins": len(PENDING),
        "predictions": len(read_json_file(PREDICTIONS_FILE)),
        "orders_pending": len([r for r in orders.values()
                               if r.get("status") == "pending"]),
    })


@app.route("/api/health", methods=["GET"])
async def api_health():
    return jsonify({"ok": True, "ts": utcnow()})


# ----------------------------------------------------------------------------
# Embedded dashboard (Engine + Admin + Customer)
# ----------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DEVIL ENGINE v6.0 — Control Deck</title>
<script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
<script src="https://unpkg.com/lucide@latest"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
  body { font-family:'Inter',system-ui,sans-serif; background:#050509; color:#e2e8f0; min-height:100vh; }
  .mono { font-family:'JetBrains Mono',monospace; }
  .card { background:linear-gradient(160deg,rgba(17,18,34,.85),rgba(5,5,10,.93)); border:1px solid rgba(148,163,184,.12); border-radius:.9rem; }
  .field { width:100%; background:rgba(4,4,10,.8); border:1px solid rgba(148,163,184,.15); border-radius:.55rem; padding:.55rem .75rem; font-family:'JetBrains Mono',monospace; font-size:.75rem; color:#e2e8f0; outline:none; transition:border-color .25s, box-shadow .25s; }
  .field:focus { border-color:rgba(217,70,239,.55); box-shadow:0 0 0 3px rgba(217,70,239,.12); }
  .field::placeholder { color:#475569; }
  select.field option { background:#0a0a14; }
  .flab { font-family:'JetBrains Mono',monospace; font-size:.57rem; letter-spacing:.16em; color:#8b93a7; display:block; margin-bottom:.3rem; }
  .btn { transition:all .2s ease; }
  .btn:active { transform:scale(.97); }
  .btn:disabled { opacity:.5; pointer-events:none; }
  .chip { display:inline-flex; align-items:center; gap:.35rem; font-family:'JetBrains Mono',monospace; font-size:.58rem; letter-spacing:.12em; padding:.26rem .58rem; border-radius:9999px; border:1px solid rgba(148,163,184,.18); color:#94a3b8; background:rgba(17,18,34,.6); white-space:nowrap; }
  .chip-on { border-color:rgba(16,185,129,.45); color:#34d399; background:rgba(16,185,129,.08); }
  .chip-live { border-color:rgba(217,70,239,.5); color:#e879f9; background:rgba(217,70,239,.1); }
  .chip-cyan { border-color:rgba(6,182,212,.5); color:#22d3ee; background:rgba(6,182,212,.08); }
  .chip-err { border-color:rgba(251,113,133,.5); color:#fb7185; background:rgba(244,63,94,.08); }
  .chip-warn { border-color:rgba(245,158,11,.45); color:#fbbf24; background:rgba(245,158,11,.08); }
  .chip-off { border-color:rgba(148,163,184,.2); color:#64748b; }
  .dot { width:7px; height:7px; border-radius:9999px; }
  table { border-collapse:collapse; width:100%; }
  th, td { padding:.45rem .5rem; text-align:left; }
  tbody tr { border-top:1px solid rgba(148,163,184,.08); }
  .ed-wrap { display:grid; grid-template-columns:44px 1fr; background:#07070e; border:1px solid rgba(148,163,184,.12); border-radius:.6rem; overflow:hidden; }
  .ed-gutter { margin:0; padding:.7rem 0; font-family:'JetBrains Mono',monospace; font-size:.68rem; line-height:1.55; text-align:right; color:#3b4252; background:rgba(10,10,20,.7); overflow:hidden; user-select:none; white-space:pre; }
  .ed-code { padding:.7rem .85rem; font-family:'JetBrains Mono',monospace; font-size:.68rem; line-height:1.55; color:#c8d3e8; background:transparent; border:none; outline:none; resize:none; white-space:pre; overflow:auto; caret-color:#e879f9; min-height:300px; max-height:400px; }
  .logline { display:grid; grid-template-columns:62px 50px 86px 1fr; gap:.5rem; padding:.1rem 0; font-family:'JetBrains Mono',monospace; font-size:.67rem; line-height:1.5; word-break:break-word; }
  ::-webkit-scrollbar { width:8px; height:8px; }
  ::-webkit-scrollbar-track { background:#0a0a14; }
  ::-webkit-scrollbar-thumb { background:#701a75; border-radius:8px; }
  .toast { padding:.6rem .85rem; border-radius:.65rem; font-size:.74rem; border:1px solid; margin-bottom:.5rem; opacity:0; transform:translateY(8px); transition:all .3s ease; }
  .toast.show { opacity:1; transform:none; }
  .toast-ok { border-color:rgba(16,185,129,.4); background:rgba(16,185,129,.12); color:#6ee7b7; }
  .toast-err { border-color:rgba(251,113,133,.45); background:rgba(244,63,94,.12); color:#fda4af; }
  .toast-warn { border-color:rgba(245,158,11,.4); background:rgba(245,158,11,.1); color:#fcd34d; }
  .ptab { border-bottom:2px solid transparent; transition:all .2s ease; }
  .ptab.on { border-color:#d946ef; color:#f0abfc; background:rgba(217,70,239,.08); }
</style>
</head>
<body class="antialiased">
<div class="fixed inset-0 pointer-events-none">
  <div class="absolute w-96 h-96 rounded-full bg-fuchsia-600/15 blur-3xl -top-20 -left-20"></div>
  <div class="absolute w-96 h-96 rounded-full bg-cyan-500/10 blur-3xl bottom-0 right-0"></div>
</div>

<header class="relative border-b border-slate-800/70 bg-slate-950/80 backdrop-blur sticky top-0 z-30">
  <div class="max-w-7xl mx-auto px-4 h-14 flex items-center gap-3">
    <span class="w-8 h-8 rounded-lg bg-gradient-to-br from-fuchsia-600 to-cyan-500 flex items-center justify-center">
      <i data-lucide="flame" class="w-4 h-4 text-slate-950"></i>
    </span>
    <div>
      <div class="font-bold text-sm leading-tight">DEVIL ENGINE <span class="text-fuchsia-400">v6.0</span></div>
      <div class="mono text-[8px] text-slate-500 tracking-[.22em]">AUTOMATION + PAID CONTENT PLATFORM</div>
    </div>
    <div class="ml-auto flex items-center gap-2">
      <span id="sv-pulse" class="mono text-[10px] flex items-center gap-1.5 text-rose-300"><span id="sv-dot" class="dot bg-rose-400"></span><span id="sv-text">OFFLINE</span></span>
      <span id="sv-procs" class="mono text-[10px] px-2.5 py-1 rounded-full border border-cyan-500/40 text-cyan-300 bg-cyan-500/10">0 TASKS</span>
    </div>
  </div>
  <div class="max-w-7xl mx-auto px-4 flex overflow-x-auto">
    <button data-panel="engine" class="ptab on mono text-[11px] tracking-widest px-5 py-2.5 text-slate-400 flex items-center gap-2 whitespace-nowrap"><i data-lucide="terminal-square" class="w-3.5 h-3.5"></i>ENGINE</button>
    <button data-panel="admin" class="ptab mono text-[11px] tracking-widest px-5 py-2.5 text-slate-500 flex items-center gap-2 whitespace-nowrap"><i data-lucide="shield" class="w-3.5 h-3.5"></i>ADMIN PANEL</button>
    <button data-panel="customer" class="ptab mono text-[11px] tracking-widest px-5 py-2.5 text-slate-500 flex items-center gap-2 whitespace-nowrap"><i data-lucide="store" class="w-3.5 h-3.5"></i>CUSTOMER PANEL</button>
  </div>
</header>

<main class="relative max-w-7xl mx-auto px-4 py-5">

<!-- ENGINE -->
<div id="panel-engine" class="grid lg:grid-cols-12 gap-4">
  <div class="lg:col-span-5 space-y-4">
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="key-round" class="w-4 h-4 text-fuchsia-400"></i>OTP Authorization</h2>
      <div class="grid grid-cols-2 gap-2.5">
        <div><label class="flab" for="in-api-id">API_ID</label><input id="in-api-id" class="field" placeholder="204xxxxx" inputmode="numeric"></div>
        <div><label class="flab" for="in-api-hash">API_HASH</label><input id="in-api-hash" type="password" class="field" placeholder="0123abc…"></div>
      </div>
      <div class="mt-2.5"><label class="flab" for="in-phone">PHONE NUMBER</label><input id="in-phone" class="field" placeholder="+1 555 000 1122"></div>
      <button id="btn-otp" class="btn mt-3 w-full py-2.5 rounded-lg bg-fuchsia-500 text-slate-950 font-bold text-sm hover:bg-fuchsia-400 flex items-center justify-center gap-2"><i data-lucide="send" class="w-4 h-4"></i><span data-label>Send OTP</span></button>
      <div id="otp-block" class="hidden mt-4 pt-3 border-t border-dashed border-slate-700/70">
        <div class="mono text-[9px] tracking-[.2em] text-emerald-300 mb-2.5">OTP DISPATCHED — CHECK TELEGRAM</div>
        <div class="grid grid-cols-2 gap-2.5">
          <div><label class="flab" for="in-otp">OTP CODE</label><input id="in-otp" class="field" placeholder="12345" autocomplete="one-time-code"></div>
          <div><label class="flab" for="in-2fa">2FA (OPTIONAL)</label><input id="in-2fa" type="password" class="field" placeholder="cloud password"></div>
        </div>
        <button id="btn-verify" class="btn mt-3 w-full py-2.5 rounded-lg bg-emerald-500 text-slate-950 font-bold text-sm hover:bg-emerald-400 flex items-center justify-center gap-2"><i data-lucide="shield-check" class="w-4 h-4"></i><span data-label>Verify &amp; Save Session</span></button>
      </div>
    </section>

    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="fingerprint" class="w-4 h-4 text-cyan-400"></i>Session ID Login</h2>
      <div class="grid grid-cols-2 gap-2.5">
        <div><label class="flab" for="s-api-id">API_ID</label><input id="s-api-id" class="field" placeholder="204xxxxx" inputmode="numeric"></div>
        <div><label class="flab" for="s-api-hash">API_HASH</label><input id="s-api-hash" type="password" class="field" placeholder="0123abc…"></div>
      </div>
      <div class="mt-2.5"><label class="flab" for="s-session">TELEGRAM STRING SESSION</label><textarea id="s-session" class="field h-14 !text-[10px] resize-none" placeholder="1BQAAAA… StringSession.save() output"></textarea></div>
      <button id="btn-session" class="btn mt-3 w-full py-2.5 rounded-lg bg-cyan-500 text-slate-950 font-bold text-sm hover:bg-cyan-400 flex items-center justify-center gap-2"><i data-lucide="log-in" class="w-4 h-4"></i><span data-label>Login With Session</span></button>
      <p class="mono text-[9px] text-slate-600 mt-2">is_user_authorized() + get_me(). Blob never logged or returned.</p>
    </section>

    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-2"><i data-lucide="users" class="w-4 h-4 text-emerald-400"></i>Active Accounts</h2>
      <div class="overflow-x-auto mono text-[11px]">
        <table>
          <thead><tr class="text-slate-600 text-[9px] tracking-widest"><th>PHONE</th><th>STATUS</th><th>SCRIPT</th><th class="text-right">ACTIONS</th></tr></thead>
          <tbody id="accounts-body"><tr><td colspan="4" class="py-4 text-center text-slate-600">No accounts authorized.</td></tr></tbody>
        </table>
      </div>
    </section>

    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-2"><i data-lucide="activity" class="w-4 h-4 text-fuchsia-400"></i>Running Processes</h2>
      <div id="procs-body" class="space-y-2"><p class="mono text-[11px] text-slate-600 text-center py-5">No engines running.</p></div>
    </section>
  </div>

  <div class="lg:col-span-7 space-y-4">
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="code-2" class="w-4 h-4 text-fuchsia-400"></i>Custom Script Installer</h2>
      <div class="grid sm:grid-cols-2 gap-2.5 mb-2.5">
        <div><label class="flab" for="run-account">RUN ON ACCOUNT</label><select id="run-account" class="field"><option value="">— no accounts —</option></select></div>
        <div><label class="flab" for="script-name">SCRIPT NAME</label><input id="script-name" class="field" placeholder="Cleaner Bot Account 1"></div>
      </div>
      <div class="flex items-center gap-1.5 mb-2">
        <select id="templates" class="field !py-1 !px-2 !text-[10px] !w-40"><option value="">templates…</option></select>
        <button id="btn-load-tpl" class="btn mono text-[10px] px-2.5 py-1.5 rounded border border-slate-700 text-slate-400 hover:border-slate-500">Load</button>
        <span class="ml-auto mono text-[9px] text-slate-600">script.py — telethon context</span>
      </div>
      <div class="ed-wrap"><pre id="gutter" class="ed-gutter">1</pre><textarea id="code" class="ed-code" spellcheck="false" wrap="off"></textarea></div>
      <div class="flex flex-wrap gap-1.5 mt-2.5 mono text-[9px]">
        <span class="px-2 py-1 rounded-full border border-emerald-500/40 text-emerald-300 bg-emerald-500/10">client</span>
        <span class="px-2 py-1 rounded-full border border-emerald-500/40 text-emerald-300 bg-emerald-500/10">SESSION_STRING</span>
        <span class="px-2 py-1 rounded-full border border-cyan-500/40 text-cyan-300 bg-cyan-500/10">PHONE · events · errors · utils · types</span>
        <span class="px-2 py-1 rounded-full border border-slate-700 text-slate-400">log() · print → terminal</span>
      </div>
      <div class="grid sm:grid-cols-2 gap-2.5 mt-3">
        <button id="btn-run" class="btn py-2.5 rounded-lg bg-gradient-to-r from-fuchsia-500 to-cyan-500 text-slate-950 font-bold text-sm hover:opacity-95 flex items-center justify-center gap-2"><i data-lucide="play" class="w-4 h-4"></i><span data-label>Install &amp; Run Script</span></button>
        <button id="btn-save-tpl" class="btn py-2.5 rounded-lg border border-cyan-500/40 text-cyan-300 font-bold text-sm hover:bg-cyan-500/10 flex items-center justify-center gap-2"><i data-lucide="save" class="w-4 h-4"></i><span data-label>Save Template</span></button>
      </div>
    </section>

    <section class="card overflow-hidden">
      <div class="flex items-center gap-2 px-4 py-2.5 border-b border-slate-800 bg-slate-950/60">
        <span class="w-2.5 h-2.5 rounded-full bg-rose-500/70"></span><span class="w-2.5 h-2.5 rounded-full bg-amber-500/70"></span><span class="w-2.5 h-2.5 rounded-full bg-emerald-500/70"></span>
        <span class="mono text-[10px] text-slate-400 ml-1">devil://live-terminal</span>
        <div class="ml-auto flex items-center gap-1.5">
          <input id="in-filter" class="field !py-1 !px-2 !text-[10px] !w-28" placeholder="filter…">
          <button id="btn-pause" class="btn mono text-[10px] px-2 py-1 rounded border border-slate-700 text-slate-400"><span data-label>Pause</span></button>
          <button id="btn-clear" class="btn mono text-[10px] px-2 py-1 rounded border border-slate-700 text-slate-400">Clear</button>
        </div>
      </div>
      <div id="term-body" class="h-[400px] overflow-y-auto px-4 py-3">
        <div class="logline"><span class="text-slate-600">--:--:--</span><span class="text-slate-500 font-semibold">SYS</span><span class="text-slate-500">[core]</span><span class="text-slate-500">Terminal attached. Streaming GET /api/logs…</span></div>
      </div>
    </section>
  </div>
</div>

<!-- ADMIN -->
<div id="panel-admin" class="hidden">
  <div id="admin-gate" class="max-w-md mx-auto py-10">
    <div class="card p-6">
      <div class="w-11 h-11 rounded-xl bg-cyan-500/15 border border-cyan-500/30 flex items-center justify-center mb-4"><i data-lucide="shield" class="w-5 h-5 text-cyan-300"></i></div>
      <h2 class="font-bold text-lg mb-1">Admin Access</h2>
      <p class="text-slate-500 text-sm mb-4">Credential gated. Set ADMIN_USERNAME / ADMIN_PASSWORD env vars in production.</p>
      <div><label class="flab" for="adm-user">USERNAME</label><input id="adm-user" class="field" placeholder="admin"></div>
      <div class="mt-2.5"><label class="flab" for="adm-pass">PASSWORD</label><input id="adm-pass" type="password" class="field" placeholder="••••••••"></div>
      <button id="btn-admin-login" class="btn mt-4 w-full py-2.5 rounded-lg bg-cyan-500 text-slate-950 font-bold text-sm hover:bg-cyan-400 flex items-center justify-center gap-2"><i data-lucide="log-in" class="w-4 h-4"></i><span data-label>Login to Admin</span></button>
    </div>
  </div>

  <div id="admin-body" class="hidden grid lg:grid-cols-12 gap-4">
    <div class="lg:col-span-7 space-y-4">
      <section class="card p-4">
        <div class="flex items-center justify-between mb-3">
          <h2 class="font-bold text-sm flex items-center gap-2"><i data-lucide="trophy" class="w-4 h-4 text-fuchsia-400"></i>Paid Content Manager</h2>
          <button id="btn-admin-logout" class="mono text-[10px] px-2.5 py-1 rounded border border-slate-700 text-slate-400 hover:border-rose-500/50 hover:text-rose-300">LOGOUT</button>
        </div>
        <input type="hidden" id="pred-id">
        <div class="grid sm:grid-cols-2 gap-2.5">
          <div><label class="flab" for="pred-name">MATCH NAME</label><input id="pred-name" class="field" placeholder="India vs Australia — 3rd ODI"></div>
          <div><label class="flab" for="pred-cat">CONTENT CATEGORY</label>
            <select id="pred-cat" class="field"><option value="session">Session</option><option value="match">Match</option><option value="toss">Toss</option><option value="combo">All-in Combo</option></select>
          </div>
          <div><label class="flab" for="pred-dt">MATCH DATE &amp; TIME</label><input id="pred-dt" type="datetime-local" class="field"></div>
          <div><label class="flab" for="pred-price">PRICE (₹)</label><input id="pred-price" class="field" inputmode="numeric" placeholder="499"></div>
        </div>
        <div class="mt-2.5"><label class="flab" for="pred-desc">DESCRIPTION (PUBLIC)</label><textarea id="pred-desc" class="field h-14 resize-none" placeholder="Premium session + match call with entry range."></textarea></div>
        <div class="mt-3 rounded-lg border border-fuchsia-500/25 bg-fuchsia-500/5 p-3">
          <div class="mono text-[9px] tracking-[.2em] text-fuchsia-300 mb-2 flex items-center gap-1.5"><i data-lucide="lock" class="w-3 h-3"></i>LOCKED PREDICTION BOXES</div>
          <div class="grid sm:grid-cols-2 gap-2.5">
            <div><label class="flab" for="lock-winner">MATCH WINNER</label><input id="lock-winner" class="field" placeholder="India — 82% model edge"></div>
            <div><label class="flab" for="lock-toss">TOSS CALL</label><input id="lock-toss" class="field" placeholder="Australia to bat first"></div>
            <div><label class="flab" for="lock-session">SESSION CALL</label><input id="lock-session" class="field" placeholder="1st 6 ov over 52.5"></div>
            <div><label class="flab" for="lock-notes">ANALYST NOTES</label><input id="lock-notes" class="field" placeholder="Dew factor after 30 ov"></div>
          </div>
        </div>
        <div class="grid sm:grid-cols-2 gap-2.5 mt-3">
          <button id="btn-pred-save" class="btn py-2.5 rounded-lg bg-fuchsia-500 text-slate-950 font-bold text-sm hover:bg-fuchsia-400 flex items-center justify-center gap-2"><i data-lucide="plus" class="w-4 h-4"></i><span data-label>Save Prediction</span></button>
          <button id="btn-pred-reset" class="btn py-2.5 rounded-lg border border-slate-700 text-slate-300 font-bold text-sm hover:border-slate-500">Clear Form</button>
        </div>
      </section>

      <section class="card p-4">
        <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="list" class="w-4 h-4 text-cyan-400"></i>Managed Matches</h2>
        <div id="pred-list" class="space-y-2"><p class="mono text-[11px] text-slate-600 text-center py-5">No predictions yet.</p></div>
      </section>
    </div>

    <div class="lg:col-span-5 space-y-4">
      <section class="card p-4">
        <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="indian-rupee" class="w-4 h-4 text-emerald-400"></i>Payment Configuration</h2>
        <div><label class="flab" for="pay-upi">UPI ID</label><input id="pay-upi" class="field" placeholder="devilengine@okaxis"></div>
        <div class="mt-2.5"><label class="flab" for="pay-name">PAYEE NAME</label><input id="pay-name" class="field" placeholder="Devil Analytics"></div>
        <div class="mt-2.5"><label class="flab" for="pay-price">DEFAULT PRICE (₹)</label><input id="pay-price" class="field" inputmode="numeric" placeholder="499"></div>
        <div class="mt-2.5"><label class="flab" for="pay-note">PAYMENT INSTRUCTIONS</label><textarea id="pay-note" class="field h-14 resize-none"></textarea></div>
        <div class="mt-2.5"><label class="flab" for="pay-qr">UPLOAD QR CODE</label><input id="pay-qr" type="file" accept="image/*" class="field !py-1.5"></div>
        <div id="qr-preview" class="hidden mt-3 flex items-center gap-3">
          <img id="qr-img" alt="QR" class="w-20 h-20 rounded-lg border border-slate-700 bg-white object-contain p-1">
          <span class="mono text-[10px] text-slate-500">QR attached.</span>
        </div>
        <button id="btn-pay-save" class="btn mt-3 w-full py-2.5 rounded-lg bg-emerald-500 text-slate-950 font-bold text-sm hover:bg-emerald-400 flex items-center justify-center gap-2"><i data-lucide="save" class="w-4 h-4"></i><span data-label>Save Payment Config</span></button>
      </section>

      <section class="card p-4">
        <div class="flex items-center justify-between mb-3">
          <h2 class="font-bold text-sm flex items-center gap-2"><i data-lucide="receipt" class="w-4 h-4 text-amber-400"></i>Payment Verifications</h2>
          <span id="order-count" class="chip chip-warn">0 PENDING</span>
        </div>
        <div id="orders-list" class="space-y-2"><p class="mono text-[11px] text-slate-600 text-center py-5">No orders submitted.</p></div>
      </section>
    </div>
  </div>
</div>

<!-- CUSTOMER -->
<div id="panel-customer" class="hidden grid lg:grid-cols-12 gap-4">
  <div class="lg:col-span-8 space-y-4">
    <section class="card p-4">
      <div class="flex items-center justify-between mb-3">
        <h2 class="font-bold text-sm flex items-center gap-2"><i data-lucide="ticket" class="w-4 h-4 text-fuchsia-400"></i>Available Matches</h2>
        <button id="btn-refresh-matches" class="mono text-[10px] px-2.5 py-1 rounded border border-slate-700 text-slate-400 hover:border-slate-500">REFRESH</button>
      </div>
      <div id="matches" class="grid sm:grid-cols-2 gap-3"><p class="mono text-[11px] text-slate-600 col-span-2 text-center py-6">Loading matches…</p></div>
    </section>
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="unlock" class="w-4 h-4 text-emerald-400"></i>My Unlocked Insights</h2>
      <div id="unlocked" class="space-y-3"><p class="mono text-[11px] text-slate-600 text-center py-5">Check access with your contact or access code.</p></div>
    </section>
  </div>
  <div class="lg:col-span-4 space-y-4">
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="credit-card" class="w-4 h-4 text-cyan-400"></i>Payment &amp; Verification</h2>
      <div class="rounded-lg border border-cyan-500/25 bg-cyan-500/5 p-3 mb-3">
        <div class="mono text-[10px] text-slate-400">Selected: <span id="sel-match" class="text-cyan-300">none — pick a match</span></div>
        <div class="mono text-[10px] text-slate-400 mt-1">Amount: <span id="sel-price" class="text-emerald-300">₹0</span></div>
        <div class="mono text-[10px] text-slate-400 mt-1">UPI: <span id="sel-upi" class="text-cyan-300">—</span></div>
        <img id="cust-qr" alt="QR" class="hidden mt-2 w-28 h-28 rounded-lg border border-slate-700 bg-white object-contain p-1">
        <p id="pay-note-view" class="text-[11px] text-slate-500 mt-2"></p>
      </div>
      <div><label class="flab" for="cu-name">YOUR NAME</label><input id="cu-name" class="field" placeholder="Rahul S."></div>
      <div class="mt-2.5"><label class="flab" for="cu-contact">CONTACT (PHONE / TG)</label><input id="cu-contact" class="field" placeholder="+91 90000 00000"></div>
      <div class="mt-2.5"><label class="flab" for="cu-utr">UTR / TXN ID</label><input id="cu-utr" class="field" placeholder="4198xxxxxx"></div>
      <button id="btn-purchase" class="btn mt-3 w-full py-2.5 rounded-lg bg-gradient-to-r from-cyan-500 to-emerald-500 text-slate-950 font-bold text-sm hover:opacity-95 flex items-center justify-center gap-2"><i data-lucide="send" class="w-4 h-4"></i><span data-label>Submit Payment Proof</span></button>
    </section>
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="key" class="w-4 h-4 text-emerald-400"></i>Check My Access</h2>
      <div><label class="flab" for="cu-lookup">CONTACT OR ACCESS CODE</label><input id="cu-lookup" class="field" placeholder="+91 90000 00000 / A1B2C3"></div>
      <button id="btn-unlock" class="btn mt-3 w-full py-2.5 rounded-lg bg-emerald-500 text-slate-950 font-bold text-sm hover:bg-emerald-400 flex items-center justify-center gap-2"><i data-lucide="unlock" class="w-4 h-4"></i><span data-label>Unlock My Predictions</span></button>
      <p class="mono text-[9px] text-slate-600 mt-2">Locked boxes stay server-side until admin approves.</p>
    </section>
  </div>
</div>

</main>
<div id="toasts" class="fixed bottom-4 right-4 z-50 w-80"></div>

<script>
(function () {
  var S = { since: 0, paused: false, filter: "", adminToken: "", selMatch: null, payment: {} };
  var TPL = {};

  var DEFAULT_SCRIPT = [
    "# Injected globals: client, SESSION_STRING, PHONE, events, errors,",
    "# utils, types, asyncio, log(msg), print(...) -> live terminal",
    "",
    "HITS = {\"n\": 0}",
    "",
    "@client.on(events.NewMessage(pattern=r\"^/status$\"))",
    "async def status_cmd(event):",
    "    HITS[\"n\"] += 1",
    "    me = await client.get_me()",
    "    await event.reply(\"Engine alive as @\" + str(me.username or me.id) + \" | hits: \" + str(HITS[\"n\"]))",
    "",
    "async def main():",
    "    me = await client.get_me()",
    "    print(\"Booted as\", me.username or me.id)",
    "    print(\"Send /status in Saved Messages to test.\")",
    ""
  ].join("\n");

  function $(id) { return document.getElementById(id); }
  function val(id) { var el = $(id); return el ? el.value.trim() : ""; }
  function esc(s) { return String(s === null || s === undefined ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  function toast(msg, kind) {
    var d = document.createElement("div");
    d.className = "toast " + (kind === "error" ? "toast-err" : (kind === "warn" ? "toast-warn" : "toast-ok"));
    d.textContent = msg;
    $("toasts").appendChild(d);
    setTimeout(function () { d.classList.add("show"); }, 10);
    setTimeout(function () { d.classList.remove("show"); setTimeout(function () { d.remove(); }, 300); }, 4600);
  }

  function busy(id, on, txt) {
    var b = $(id); if (!b) return;
    b.disabled = on;
    var l = b.querySelector("[data-label]");
    if (l) { if (!l.dataset.orig) l.dataset.orig = l.textContent; l.textContent = on ? (txt || "Working…") : l.dataset.orig; }
  }

  async function post(path, payload, admin) {
    var headers = { "Content-Type": "application/json" };
    if (admin && S.adminToken) headers["X-Admin-Token"] = S.adminToken;
    var res = await fetch(path, { method: "POST", headers: headers, body: JSON.stringify(payload || {}) });
    var data = null;
    try { data = await res.json(); } catch (e) { data = { ok: false, error: "HTTP " + res.status }; }
    return { status: res.status, data: data };
  }
  async function get(path, admin) {
    var headers = {};
    if (admin && S.adminToken) headers["X-Admin-Token"] = S.adminToken;
    var res = await fetch(path, { headers: headers });
    var data = null;
    try { data = await res.json(); } catch (e) { data = { ok: false, error: "HTTP " + res.status }; }
    return { status: res.status, data: data };
  }

  // ---------- panels ----------
  function showPanel(name) {
    ["engine", "admin", "customer"].forEach(function (p) {
      $("panel-" + p).classList.toggle("hidden", p !== name);
    });
    document.querySelectorAll("[data-panel]").forEach(function (b) {
      b.classList.toggle("on", b.getAttribute("data-panel") === name);
    });
    if (name === "customer") loadMatches();
    if (name === "admin" && S.adminToken) { loadPredictions(); loadOrders(); }
  }
  document.querySelectorAll("[data-panel]").forEach(function (b) {
    b.addEventListener("click", function () { showPanel(b.getAttribute("data-panel")); });
  });

  // ---------- engine: auth ----------
  async function sendOtp() {
    var apiId = parseInt(val("in-api-id"), 10);
    var p = { api_id: apiId, api_hash: val("in-api-hash"), phone: val("in-phone") };
    if (!apiId || !p.api_hash || !p.phone) { toast("Fill API_ID, API_HASH and phone.", "warn"); return; }
    busy("btn-otp", true, "Dispatching…");
    try {
      var r = await post("/api/send-otp", p);
      if (r.data.already_connected) { toast("Account already authorized.", "ok"); refreshStatus(); return; }
      if (!r.data.ok) { toast(r.data.error || "send-otp failed", "error"); return; }
      $("otp-block").classList.remove("hidden");
      toast("OTP dispatched (" + (r.data.code_type || "app") + ").", "ok");
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-otp", false); }
  }

  async function verifyOtp() {
    var p = { phone: val("in-phone"), otp_code: val("in-otp"), password: val("in-2fa") };
    if (!p.phone || !p.otp_code) { toast("Phone and OTP required.", "warn"); return; }
    busy("btn-verify", true, "Verifying…");
    try {
      var r = await post("/api/verify-otp", p);
      if (r.data.need_password) { toast(r.data.message, "warn"); $("in-2fa").focus(); return; }
      if (!r.data.ok) { toast(r.data.error || "verify failed", "error"); return; }
      toast("Saved: @" + (r.data.account.username || r.data.account.id), "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-verify", false); }
  }

  async function sessionLogin() {
    var p = {
      api_id: parseInt(val("s-api-id") || val("in-api-id"), 10),
      api_hash: val("s-api-hash") || val("in-api-hash"),
      session_string: val("s-session")
    };
    if (!p.api_id || !p.api_hash || !p.session_string) { toast("API_ID, API_HASH and session required.", "warn"); return; }
    busy("btn-session", true, "Verifying…");
    try {
      var r = await post("/api/login-session", p);
      if (!r.data.ok) { toast(r.data.error || "session login failed", "error"); return; }
      if (!r.data.already_connected) $("s-session").value = "";
      toast((r.data.already_connected ? "Already active: @" : "Session verified: @") + (r.data.account.username || r.data.account.id), "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-session", false); }
  }

  async function accountAction(phone, kind) {
    if (kind === "delete" && !window.confirm("Delete " + phone + " and purge its session?")) return;
    var r = await post(kind === "delete" ? "/api/delete-account" : "/api/disconnect-account", { phone: phone });
    if (!r.data.ok) { toast(r.data.error || "action failed", "error"); return; }
    toast(r.data.message, "ok");
    refreshStatus();
  }

  // ---------- engine: scripts ----------
  async function runScript() {
    var phone = val("run-account");
    var p = { phone_number: phone, script_code: $("code").value, script_name: val("script-name") };
    if (!phone) { toast("Select an account.", "warn"); return; }
    if (!p.script_code.trim()) { toast("Editor is empty.", "warn"); return; }
    busy("btn-run", true, "Launching…");
    try {
      var r = await post("/api/run-script", p);
      if (!r.data.ok) { toast(r.data.error || "run failed", "error"); return; }
      toast("Engine launched: " + r.data.script_name, "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-run", false); }
  }

  async function stopScript(phone) {
    var r = await post("/api/stop-script", { phone_number: phone });
    if (!r.data.ok) { toast(r.data.error || "stop failed", "error"); return; }
    toast("Task cancelled.", "ok");
    refreshStatus();
  }

  async function saveTemplate() {
    var p = { name: val("script-name") || "untitled-" + Date.now(), code: $("code").value };
    if (!p.code.trim()) { toast("Editor is empty.", "warn"); return; }
    busy("btn-save-tpl", true, "Saving…");
    try {
      var r = await post("/api/save-template", p);
      if (!r.data.ok) { toast(r.data.error || "save failed", "error"); return; }
      toast(r.data.message, "ok");
      loadTemplates();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-save-tpl", false); }
  }

  async function loadTemplates() {
    try {
      var r = await get("/api/templates");
      TPL = r.data.templates || {};
      var sel = $("templates");
      sel.innerHTML = '<option value="">templates…</option>';
      Object.keys(TPL).forEach(function (k) {
        var o = document.createElement("option"); o.value = k; o.textContent = k; sel.appendChild(o);
      });
    } catch (e) { /* offline */ }
  }

  // ---------- engine: render ----------
  function renderAccounts(accounts) {
    var body = $("accounts-body");
    if (!accounts.length) {
      body.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-600">No accounts authorized.</td></tr>';
    } else {
      body.innerHTML = accounts.map(function (a) {
        var st = a.connected
          ? '<span class="inline-flex items-center gap-1.5 text-emerald-300"><span class="dot bg-emerald-400"></span>ONLINE</span>'
          : '<span class="inline-flex items-center gap-1.5 text-slate-500"><span class="dot bg-slate-500"></span>IDLE</span>';
        var sc = a.running_script ? '<span class="text-fuchsia-300">' + esc(a.running_script) + "</span>" : '<span class="text-slate-600">—</span>';
        return "<tr><td class='mono'>" + esc(a.phone) + "</td><td>" + st + "</td><td>" + sc + "</td>" +
          "<td class='text-right whitespace-nowrap'>" +
          "<button data-dc='" + esc(a.phone) + "' class='mono text-[9px] px-2 py-1 rounded border border-slate-700 text-slate-400 hover:border-amber-500/50 hover:text-amber-300 mr-1'>DISCONNECT</button>" +
          "<button data-del='" + esc(a.phone) + "' class='mono text-[9px] px-2 py-1 rounded border border-rose-500/50 text-rose-300 hover:bg-rose-500/10'>DELETE</button></td></tr>";
      }).join("");
      body.querySelectorAll("[data-del]").forEach(function (b) { b.addEventListener("click", function () { accountAction(b.getAttribute("data-del"), "delete"); }); });
      body.querySelectorAll("[data-dc]").forEach(function (b) { b.addEventListener("click", function () { accountAction(b.getAttribute("data-dc"), "disconnect"); }); });
    }
    var sel = $("run-account"); var keep = sel.value;
    sel.innerHTML = accounts.length ? "" : '<option value="">— no accounts —</option>';
    accounts.forEach(function (a) {
      var o = document.createElement("option"); o.value = a.phone;
      o.textContent = a.phone + (a.username ? " (@" + a.username + ")" : "");
      sel.appendChild(o);
    });
    if (keep) sel.value = keep;
  }

  function fmtUp(ms) {
    var s = Math.floor(ms / 1000);
    var h = Math.floor(s / 3600); s -= h * 3600;
    var m = Math.floor(s / 60); s -= m * 60;
    return (h ? h + "h " : "") + (m ? m + "m " : "") + s + "s";
  }

  function renderProcs(procs) {
    $("sv-procs").textContent = procs.length + (procs.length === 1 ? " TASK" : " TASKS");
    var host = $("procs-body");
    if (!procs.length) { host.innerHTML = '<p class="mono text-[11px] text-slate-600 text-center py-5">No engines running.</p>'; return; }
    host.innerHTML = procs.map(function (p) {
      return '<div class="rounded-lg border border-slate-800 bg-slate-950/60 p-3 flex items-center gap-3">' +
        '<span class="w-2 h-2 rounded-full bg-fuchsia-400 shrink-0"></span>' +
        '<div class="min-w-0 flex-1"><div class="mono text-[11px] font-semibold text-fuchsia-200 truncate">' + esc(p.script_name) + "</div>" +
        '<div class="mono text-[9px] text-slate-500 truncate">' + esc(p.phone) + ' · up <span data-up="' + esc(p.started_at) + '">…</span></div></div>' +
        "<button data-stop='" + esc(p.phone) + "' class='mono text-[10px] px-2.5 py-1.5 rounded border border-rose-500/50 text-rose-300 hover:bg-rose-500/10 shrink-0'>STOP</button></div>";
    }).join("");
    host.querySelectorAll("[data-stop]").forEach(function (b) { b.addEventListener("click", function () { stopScript(b.getAttribute("data-stop")); }); });
    tickUp();
  }

  function tickUp() {
    document.querySelectorAll("[data-up]").forEach(function (el) {
      var t = Date.parse(el.getAttribute("data-up"));
      el.textContent = isNaN(t) ? "—" : fmtUp(Math.max(0, Date.now() - t));
    });
  }
  setInterval(tickUp, 1000);

  function tagColor(tag) {
    var pal = ["text-fuchsia-300", "text-cyan-300", "text-emerald-300", "text-amber-300", "text-violet-300", "text-pink-300"];
    if (tag === "core") return "text-slate-500";
    var h = 0;
    for (var i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) >>> 0;
    return pal[h % pal.length];
  }

  function appendLog(row) {
    if (S.filter) {
      var hay = (row.level + " " + row.tag + " " + row.message).toLowerCase();
      if (hay.indexOf(S.filter) === -1) return;
    }
    var host = $("term-body");
    var lvl = String(row.level || "INFO").toUpperCase();
    var cls = (lvl === "ERROR" || lvl === "CRITICAL") ? "text-rose-400" : (lvl === "WARNING" ? "text-amber-400" : (lvl === "DEBUG" ? "text-slate-500" : "text-cyan-300"));
    var t = String(row.ts || "").split("T")[1] || "";
    t = t.replace("Z", "").split(".")[0].split("+")[0] || "--:--:--";
    var d = document.createElement("div");
    d.className = "logline";
    d.innerHTML = '<span class="text-slate-600">' + esc(t) + '</span><span class="' + cls + ' font-semibold">' + esc(lvl.slice(0, 4)) + '</span>' +
      '<span class="' + tagColor(String(row.tag || "core")) + ' truncate">[' + esc(String(row.tag || "core")) + "]</span>" +
      '<span class="text-slate-300">' + esc(row.message) + "</span>";
    host.appendChild(d);
    while (host.children.length > 260) host.removeChild(host.firstChild);
    if (!S.paused && host.scrollHeight - host.scrollTop - host.clientHeight < 60) host.scrollTop = host.scrollHeight;
  }

  // ---------- admin ----------
  async function adminLogin() {
    var p = { username: val("adm-user"), password: val("adm-pass") };
    if (!p.username || !p.password) { toast("Username and password required.", "warn"); return; }
    busy("btn-admin-login", true, "Authenticating…");
    try {
      var r = await post("/api/admin/login", p);
      if (!r.data.ok) { toast(r.data.error || "login failed", "error"); return; }
      S.adminToken = r.data.token;
      try { sessionStorage.setItem("devil_admin", S.adminToken); } catch (e) {}
      $("admin-gate").classList.add("hidden");
      $("admin-body").classList.remove("hidden");
      $("adm-pass").value = "";
      toast("Admin session started.", "ok");
      loadPredictions(); loadPaymentConfig(); loadOrders();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-admin-login", false); }
  }

  async function adminLogout() {
    await post("/api/admin/logout", {}, true).catch(function () {});
    S.adminToken = "";
    try { sessionStorage.removeItem("devil_admin"); } catch (e) {}
    $("admin-body").classList.add("hidden");
    $("admin-gate").classList.remove("hidden");
    toast("Admin session closed.", "ok");
  }

  function catLabel(c) {
    return { session: "Session", match: "Match", toss: "Toss", combo: "All-in Combo" }[c] || "Match";
  }

  async function loadPredictions() {
    var r = await get("/api/admin/predictions", true);
    if (r.status === 401) { adminExpired(); return; }
    var rows = r.data.predictions || [];
    var host = $("pred-list");
    if (!rows.length) { host.innerHTML = '<p class="mono text-[11px] text-slate-600 text-center py-5">No predictions yet.</p>'; return; }
    host.innerHTML = rows.map(function (p) {
      return '<div class="rounded-lg border border-slate-800 bg-slate-950/60 p-3">' +
        '<div class="flex items-start gap-2">' +
        '<div class="min-w-0 flex-1"><div class="font-semibold text-sm truncate">' + esc(p.match_name) + "</div>" +
        '<div class="mono text-[9px] text-slate-500 mt-1">' + esc(p.match_datetime || "TBA") + " · ₹" + esc(p.price) + "</div></div>" +
        '<span class="chip chip-cyan">' + esc(catLabel(p.category)) + "</span></div>" +
        '<p class="text-[11px] text-slate-500 mt-2 line-clamp-2">' + esc(p.description || "") + "</p>" +
        '<div class="flex gap-1.5 mt-2">' +
        "<button data-edit='" + esc(p.id) + "' class='mono text-[9px] px-2 py-1 rounded border border-cyan-500/40 text-cyan-300 hover:bg-cyan-500/10'>EDIT</button>" +
        "<button data-delp='" + esc(p.id) + "' class='mono text-[9px] px-2 py-1 rounded border border-rose-500/50 text-rose-300 hover:bg-rose-500/10'>DELETE</button></div></div>";
    }).join("");
    host.querySelectorAll("[data-edit]").forEach(function (b) {
      b.addEventListener("click", function () {
        var row = rows.filter(function (x) { return x.id === b.getAttribute("data-edit"); })[0];
        if (!row) return;
        $("pred-id").value = row.id;
        $("pred-name").value = row.match_name || "";
        $("pred-cat").value = row.category || "match";
        $("pred-dt").value = row.match_datetime || "";
        $("pred-price").value = row.price || "";
        $("pred-desc").value = row.description || "";
        var lc = row.locked_content || {};
        $("lock-winner").value = lc.winner || "";
        $("lock-toss").value = lc.toss || "";
        $("lock-session").value = lc.session || "";
        $("lock-notes").value = lc.notes || "";
        window.scrollTo({ top: 0, behavior: "smooth" });
        toast("Editing '" + row.match_name + "'", "ok");
      });
    });
    host.querySelectorAll("[data-delp]").forEach(function (b) {
      b.addEventListener("click", async function () {
        if (!window.confirm("Delete this prediction?")) return;
        var rr = await post("/api/admin/delete-prediction", { id: b.getAttribute("data-delp") }, true);
        if (!rr.data.ok) { toast(rr.data.error || "delete failed", "error"); return; }
        toast("Prediction deleted.", "ok"); loadPredictions();
      });
    });
  }

  function resetPredForm() {
    ["pred-id", "pred-name", "pred-dt", "pred-price", "pred-desc", "lock-winner", "lock-toss", "lock-session", "lock-notes"].forEach(function (k) { $(k).value = ""; });
    $("pred-cat").value = "session";
  }

  async function savePrediction() {
    var p = {
      id: val("pred-id"),
      match_name: val("pred-name"),
      category: val("pred-cat"),
      match_datetime: val("pred-dt"),
      price: val("pred-price") || 0,
      description: val("pred-desc"),
      locked_content: { winner: val("lock-winner"), toss: val("lock-toss"), session: val("lock-session"), notes: val("lock-notes") }
    };
    if (!p.match_name) { toast("Match name is required.", "warn"); return; }
    busy("btn-pred-save", true, "Saving…");
    try {
      var r = await post("/api/admin/save-prediction", p, true);
      if (r.status === 401) { adminExpired(); return; }
      if (!r.data.ok) { toast(r.data.error || "save failed", "error"); return; }
      toast("Prediction published.", "ok");
      resetPredForm(); loadPredictions();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-pred-save", false); }
  }

  async function loadPaymentConfig() {
    var r = await get("/api/admin/payment-config", true);
    if (r.status === 401) { adminExpired(); return; }
    var c = r.data.config || {};
    $("pay-upi").value = c.upi_id || "";
    $("pay-name").value = c.payee_name || "";
    $("pay-price").value = c.default_price || "";
    $("pay-note").value = c.instructions || "";
    if (c.qr_data_url) { $("qr-img").src = c.qr_data_url; $("qr-preview").classList.remove("hidden"); }
  }

  var QR_DATA = null;
  function bindQrUpload() {
    $("pay-qr").addEventListener("change", function () {
      var f = this.files && this.files[0];
      if (!f) return;
      if (f.size > 2500000) { toast("QR too large (max 2.5 MB).", "warn"); return; }
      var fr = new FileReader();
      fr.onload = function () {
        QR_DATA = fr.result;
        $("qr-img").src = QR_DATA;
        $("qr-preview").classList.remove("hidden");
        toast("QR ready — click Save Payment Config.", "ok");
      };
      fr.readAsDataURL(f);
    });
  }

  async function savePayment() {
    var p = {
      upi_id: val("pay-upi"), payee_name: val("pay-name"),
      default_price: val("pay-price") || 0, instructions: val("pay-note")
    };
    if (QR_DATA) p.qr_data_url = QR_DATA;
    busy("btn-pay-save", true, "Saving…");
    try {
      var r = await post("/api/admin/payment-config", p, true);
      if (r.status === 401) { adminExpired(); return; }
      if (!r.data.ok) { toast(r.data.error || "save failed", "error"); return; }
      toast("Payment settings saved.", "ok");
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-pay-save", false); }
  }

  async function loadOrders() {
    var r = await get("/api/admin/orders", true);
    if (r.status === 401) { adminExpired(); return; }
    var rows = r.data.orders || [];
    $("order-count").textContent = (r.data.pending || 0) + " PENDING";
    var host = $("orders-list");
    if (!rows.length) { host.innerHTML = '<p class="mono text-[11px] text-slate-600 text-center py-5">No orders submitted.</p>'; return; }
    host.innerHTML = rows.map(function (o) {
      var chip = o.status === "approved" ? "chip chip-on" : (o.status === "rejected" ? "chip chip-err" : "chip chip-warn");
      var act = o.status === "pending"
        ? "<button data-ap='" + esc(o.id) + "' class='mono text-[9px] px-2 py-1 rounded border border-emerald-500/50 text-emerald-300 hover:bg-emerald-500/10 mr-1'>APPROVE</button>" +
          "<button data-rj='" + esc(o.id) + "' class='mono text-[9px] px-2 py-1 rounded border border-rose-500/50 text-rose-300 hover:bg-rose-500/10'>REJECT</button>"
        : "<span class='mono text-[9px] text-slate-600'>" + esc(o.reviewed_at || "").slice(0, 16).replace("T", " ") + "</span>";
      return '<div class="rounded-lg border border-slate-800 bg-slate-950/60 p-3">' +
        '<div class="flex items-start gap-2"><div class="min-w-0 flex-1">' +
        '<div class="font-semibold text-[13px] truncate">' + esc(o.match_name) + "</div>" +
        '<div class="mono text-[9px] text-slate-500 mt-1">' + esc(o.customer_name) + " · " + esc(o.contact) + "</div>" +
        '<div class="mono text-[9px] text-slate-500">UTR ' + esc(o.utr) + " · ₹" + esc(o.amount) + " · code " + esc(o.access_code) + "</div>" +
        '</div><span class="' + chip + '">' + esc(String(o.status).toUpperCase()) + "</span></div>" +
        '<div class="mt-2">' + act + "</div></div>";
    }).join("");
    host.querySelectorAll("[data-ap]").forEach(function (b) { b.addEventListener("click", function () { reviewOrder(b.getAttribute("data-ap"), "approve"); }); });
    host.querySelectorAll("[data-rj]").forEach(function (b) { b.addEventListener("click", function () { reviewOrder(b.getAttribute("data-rj"), "reject"); }); });
  }

  async function reviewOrder(id, action) {
    var r = await post("/api/admin/review-order", { id: id, action: action }, true);
    if (!r.data.ok) { toast(r.data.error || "review failed", "error"); return; }
    toast(r.data.message, "ok");
    loadOrders();
  }

  function adminExpired() {
    S.adminToken = "";
    try { sessionStorage.removeItem("devil_admin"); } catch (e) {}
    $("admin-body").classList.add("hidden");
    $("admin-gate").classList.remove("hidden");
    toast("Admin session expired — log in again.", "warn");
  }

  // ---------- customer ----------
  async function loadMatches() {
    var r = await get("/api/customer/matches");
    var rows = (r.data && r.data.matches) || [];
    S.payment = (r.data && r.data.payment) || {};
    $("sel-upi").textContent = S.payment.upi_id || "—";
    $("pay-note-view").textContent = S.payment.instructions || "";
    if (S.payment.qr_data_url) { $("cust-qr").src = S.payment.qr_data_url; $("cust-qr").classList.remove("hidden"); }
    var host = $("matches");
    if (!rows.length) { host.innerHTML = '<p class="mono text-[11px] text-slate-600 col-span-2 text-center py-6">No matches published yet.</p>'; return; }
    host.innerHTML = rows.map(function (m) {
      return '<div class="rounded-xl border border-slate-800 bg-slate-950/60 p-3 flex flex-col">' +
        '<div class="flex items-start gap-2 mb-2"><div class="min-w-0 flex-1">' +
        '<div class="font-semibold text-sm truncate">' + esc(m.match_name) + "</div>" +
        '<div class="mono text-[9px] text-slate-500 mt-0.5">' + esc(m.match_datetime || "TBA") + "</div></div>" +
        '<span class="chip chip-cyan">' + esc(m.category_label) + "</span></div>" +
        '<p class="text-[11px] text-slate-500 flex-1">' + esc(m.description || "") + "</p>" +
        '<div class="mt-2 rounded-lg border border-fuchsia-500/25 bg-fuchsia-500/5 p-2 flex items-center gap-2">' +
        '<i data-lucide="lock" class="w-3.5 h-3.5 text-fuchsia-400"></i>' +
        '<span class="mono text-[9px] text-fuchsia-300">WINNER · TOSS · SESSION LOCKED</span></div>' +
        '<div class="flex items-center justify-between mt-2">' +
        '<span class="mono text-sm font-bold text-emerald-300">₹' + esc(m.price) + "</span>" +
        "<button data-buy='" + esc(m.id) + "' data-nm='" + esc(m.match_name) + "' data-pr='" + esc(m.price) + "' class='mono text-[10px] px-3 py-1.5 rounded bg-fuchsia-500 text-slate-950 font-bold hover:bg-fuchsia-400'>UNLOCK</button>" +
        "</div></div>";
    }).join("");
    host.querySelectorAll("[data-buy]").forEach(function (b) {
      b.addEventListener("click", function () {
        S.selMatch = { id: b.getAttribute("data-buy"), name: b.getAttribute("data-nm"), price: b.getAttribute("data-pr") };
        $("sel-match").textContent = S.selMatch.name;
        $("sel-price").textContent = "₹" + S.selMatch.price;
        toast("Selected — pay then submit UTR.", "ok");
      });
    });
    if (window.lucide) lucide.createIcons();
  }

  async function purchase() {
    if (!S.selMatch) { toast("Pick a match first.", "warn"); return; }
    var p = {
      match_id: S.selMatch.id, customer_name: val("cu-name"),
      contact: val("cu-contact"), utr: val("cu-utr")
    };
    if (!p.customer_name || !p.contact || !p.utr) { toast("Name, contact and UTR required.", "warn"); return; }
    busy("btn-purchase", true, "Submitting…");
    try {
      var r = await post("/api/customer/purchase", p);
      if (!r.data.ok) { toast(r.data.error || "submit failed", "error"); return; }
      $("cu-lookup").value = p.contact;
      toast("Submitted. Access code: " + r.data.access_code, "ok");
      $("cu-utr").value = "";
      checkUnlock();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-purchase", false); }
  }

  async function checkUnlock() {
    var lookup = val("cu-lookup");
    if (!lookup) { toast("Enter your contact or access code.", "warn"); return; }
    busy("btn-unlock", true, "Checking…");
    try {
      var r = await post("/api/customer/unlock", { lookup: lookup });
      var host = $("unlocked");
      var un = (r.data && r.data.unlocked) || [];
      var pd = (r.data && r.data.pending) || [];
      if (!un.length && !pd.length) {
        host.innerHTML = '<p class="mono text-[11px] text-slate-600 text-center py-5">No orders found for that contact or code.</p>';
        return;
      }
      var html = un.map(function (u) {
        var lc = u.locked_content || {};
        return '<div class="rounded-xl border border-emerald-500/30 bg-emerald-500/5 p-3">' +
          '<div class="flex items-center gap-2 mb-2"><i data-lucide="unlock" class="w-4 h-4 text-emerald-400"></i>' +
          '<span class="font-semibold text-sm">' + esc(u.match_name) + "</span>" +
          '<span class="chip chip-on ml-auto">UNLOCKED</span></div>' +
          '<div class="grid sm:grid-cols-2 gap-2 mono text-[10px]">' +
          '<div class="rounded border border-slate-800 bg-slate-950/60 p-2"><div class="text-slate-500">MATCH WINNER</div><div class="text-emerald-300 mt-0.5">' + esc(lc.winner || "—") + "</div></div>" +
          '<div class="rounded border border-slate-800 bg-slate-950/60 p-2"><div class="text-slate-500">TOSS</div><div class="text-emerald-300 mt-0.5">' + esc(lc.toss || "—") + "</div></div>" +
          '<div class="rounded border border-slate-800 bg-slate-950/60 p-2"><div class="text-slate-500">SESSION</div><div class="text-emerald-300 mt-0.5">' + esc(lc.session || "—") + "</div></div>" +
          '<div class="rounded border border-slate-800 bg-slate-950/60 p-2"><div class="text-slate-500">NOTES</div><div class="text-emerald-300 mt-0.5">' + esc(lc.notes || "—") + "</div></div>" +
          "</div></div>";
      }).join("");
      html += pd.map(function (p) {
        return '<div class="rounded-xl border border-amber-500/30 bg-amber-500/5 p-3 flex items-center gap-2">' +
          '<i data-lucide="clock" class="w-4 h-4 text-amber-400"></i>' +
          '<div class="min-w-0 flex-1"><div class="font-semibold text-sm truncate">' + esc(p.match_name) + "</div>" +
          '<div class="mono text-[9px] text-slate-500">code ' + esc(p.access_code) + " · ₹" + esc(p.amount) + "</div></div>" +
          '<span class="chip chip-warn">' + esc(String(p.status).toUpperCase()) + "</span></div>";
      }).join("");
      host.innerHTML = html;
      if (window.lucide) lucide.createIcons();
      toast(r.data.message, "ok");
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { busy("btn-unlock", false); }
  }

  // ---------- polling ----------
  function setServer(on) {
    $("sv-text").textContent = on ? "ONLINE" : "OFFLINE";
    $("sv-dot").className = "dot " + (on ? "bg-emerald-400" : "bg-rose-400");
    $("sv-pulse").className = "mono text-[10px] flex items-center gap-1.5 " + (on ? "text-emerald-300" : "text-rose-300");
  }

  async function pollLogs() {
    try {
      var res = await fetch("/api/logs?since=" + S.since);
      var j = await res.json();
      (j.logs || []).forEach(appendLog);
      if (typeof j.latest === "number") S.since = j.latest;
      setServer(true);
      setTimeout(pollLogs, 1500);
    } catch (e) { setServer(false); setTimeout(pollLogs, 4000); }
  }

  async function refreshStatus() {
    try {
      var res = await fetch("/api/status");
      var j = await res.json();
      renderAccounts(j.accounts || []);
      renderProcs(j.processes || []);
      setServer(true);
    } catch (e) { setServer(false); }
  }
  async function statusLoop() { await refreshStatus(); setTimeout(statusLoop, 4000); }

  // ---------- editor ----------
  function syncGutter() {
    var n = $("code").value.split("\n").length, buf = [];
    for (var i = 1; i <= n; i++) buf.push(i);
    $("gutter").textContent = buf.join("\n");
  }

  document.addEventListener("DOMContentLoaded", function () {
    $("btn-otp").addEventListener("click", sendOtp);
    $("btn-verify").addEventListener("click", verifyOtp);
    $("btn-session").addEventListener("click", sessionLogin);
    $("btn-run").addEventListener("click", runScript);
    $("btn-save-tpl").addEventListener("click", saveTemplate);
    $("btn-load-tpl").addEventListener("click", function () {
      var k = val("templates");
      if (!k || !TPL[k]) { toast("Pick a template.", "warn"); return; }
      $("code").value = TPL[k]; $("script-name").value = k; syncGutter();
      toast("Template '" + k + "' loaded.", "ok");
    });
    $("btn-clear").addEventListener("click", function () { $("term-body").innerHTML = ""; });
    $("in-filter").addEventListener("input", function () { S.filter = this.value.trim().toLowerCase(); });
    $("btn-pause").addEventListener("click", function () {
      S.paused = !S.paused;
      var l = this.querySelector("[data-label]"); if (l) l.textContent = S.paused ? "Resume" : "Pause";
    });
    $("btn-admin-login").addEventListener("click", adminLogin);
    $("btn-admin-logout").addEventListener("click", adminLogout);
    $("btn-pred-save").addEventListener("click", savePrediction);
    $("btn-pred-reset").addEventListener("click", resetPredForm);
    $("btn-pay-save").addEventListener("click", savePayment);
    $("btn-refresh-matches").addEventListener("click", loadMatches);
    $("btn-purchase").addEventListener("click", purchase);
    $("btn-unlock").addEventListener("click", checkUnlock);
    bindQrUpload();

    var code = $("code");
    code.value = DEFAULT_SCRIPT;
    syncGutter();
    code.addEventListener("input", syncGutter);
    code.addEventListener("scroll", function () { $("gutter").scrollTop = code.scrollTop; });
    code.addEventListener("keydown", function (e) {
      if (e.key === "Tab") {
        e.preventDefault();
        var s = this.selectionStart, en = this.selectionEnd;
        this.value = this.value.slice(0, s) + "    " + this.value.slice(en);
        this.selectionStart = this.selectionEnd = s + 4;
        syncGutter();
      }
    });

    try {
      var saved = sessionStorage.getItem("devil_admin");
      if (saved) {
        S.adminToken = saved;
        $("admin-gate").classList.add("hidden");
        $("admin-body").classList.remove("hidden");
        loadPredictions(); loadPaymentConfig(); loadOrders();
      }
    } catch (e) {}

    if (window.lucide) lucide.createIcons();
    loadTemplates();
    pollLogs();
    statusLoop();
  });
})();
</script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
async def dashboard():
    return await render_template_string(DASHBOARD_HTML)


# ----------------------------------------------------------------------------
# Entrypoint (local dev). On Render: hypercorn main:app --bind 0.0.0.0:$PORT
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
