
# ============================================================================
# DEVIL MULTI-PROJECT MANAGER v5.0
# Multi-account Telegram custom-script execution dashboard
# Quart + Telethon + Hypercorn  |  single-file application (UI embedded)
#
# Deploy on Render:
#   build:  pip install -r requirements.txt
#   start:  hypercorn main:app --bind 0.0.0.0:$PORT
# ============================================================================

import os
import re
import json
import time
import inspect
import asyncio
import logging
import traceback
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


def emit(level, message, tag="core"):
    extra = {"tag": tag}
    getattr(logger, level.lower(), logger.info)(message, extra=extra)


# ----------------------------------------------------------------------------
# App + in-memory state (per Hypercorn worker)
# ----------------------------------------------------------------------------

app = Quart(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_FILE = os.path.join(BASE_DIR, "sessions.json")
TEMPLATES_FILE = os.path.join(BASE_DIR, "templates.json")

PENDING = {}          # phone -> {"client","phone_code_hash","api_id","api_hash","ts"}
ACCOUNTS = {}         # phone -> {"client","account","session_string","connected_at"}
RUNNING = {}          # phone -> {"task","token","name","started_at"}
PENDING_TTL = 600.0
STATE_LOCK = asyncio.Lock()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def mask_phone(phone):
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) <= 4:
        return "***"
    return "+" + digits[:3] + "******" + digits[-2:]


def read_json_file(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as exc:
        emit("WARNING", "Broken JSON store %s (%s) — starting empty", path, exc)
        return {}


def write_json_file(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    os.replace(tmp, path)


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


# ----------------------------------------------------------------------------
# Script execution engine
# ----------------------------------------------------------------------------

def build_script_env(phone):
    acct = ACCOUNTS[phone]

    def tprint(*args, **_kwargs):
        emit("INFO", " ".join(str(a) for a in args), tag=phone)

    def tlog(msg, level="INFO"):
        emit(str(level).upper(), str(msg), tag=phone)

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
    return {(id(cb), id(builder)) for cb, builder in client.list_event_handlers()}


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
        emit("INFO", "Purged %s script-registered handler(s) from client", removed, tag=phone)


async def script_engine(phone, name, code, token):
    acct = ACCOUNTS[phone]
    client = acct["client"]
    snapshot = handlers_snapshot(client)
    emit("INFO", "Engine online: '%s' compiling…", name, tag=phone)
    try:
        compiled = compile(code, "<%s>" % (name or "script"), "exec")
        env = build_script_env(phone)
        exec(compiled, env, env)
        emit("INFO", "'%s' module-level executed on %s", name,
             acct["account"].get("username") or acct["account"].get("id"), tag=phone)

        main_fn = env.get("main")
        main_ran = False
        if inspect.iscoroutinefunction(main_fn):
            emit("INFO", "'%s': async main() detected — awaiting", name, tag=phone)
            await main_fn()
            main_ran = True
            emit("INFO", "'%s': main() returned", name, tag=phone)

        live = handlers_snapshot(client) - snapshot
        if live:
            emit("INFO", "'%s': %s event handler(s) armed — task idles until stopped",
                 name, len(live), tag=phone)
            while True:
                await asyncio.sleep(3600)
        elif main_ran:
            emit("INFO", "'%s': work complete, no handlers — engine exiting", name, tag=phone)
        else:
            emit("WARNING", "'%s' registered nothing and defines no main() — engine exiting",
                 name, tag=phone)
    except asyncio.CancelledError:
        emit("WARNING", "'%s' cancelled by operator (Stop Task)", name, tag=phone)
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
            emit("ERROR", "Engine task for '%s' died outside envelope: %s", name, exc, tag=phone)
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
        emit("WARNING", "Task teardown raised during %s stop: %s", reason, exc, tag=phone)
    return True


# ----------------------------------------------------------------------------
# Lifecycle: restore saved sessions, sweep stale OTP attempts
# ----------------------------------------------------------------------------

async def restore_sessions():
    store = read_json_file(SESSIONS_FILE)
    if not store:
        emit("INFO", "No saved sessions — waiting for first OTP authorization")
        return
    for phone, row in list(store.items()):
        try:
            client = TelegramClient(
                StringSession(row["session"]),
                int(row["api_id"]), row["api_hash"],
                device_model="DEVIL Engine",
                system_version="Hypercorn/Quart",
                app_version="5.0.0")
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
            emit("WARNING", "Session restore failed for %s: %s — dropping",
                 mask_phone(phone), exc)
            unpersist_account(phone)


async def pending_sweeper():
    while True:
        await asyncio.sleep(120)
        now = time.time()
        stale = [p for p, e in list(PENDING.items()) if now - e["ts"] > PENDING_TTL]
        for phone in stale:
            emit("INFO", "Sweeper: expiring stale OTP attempt for %s", mask_phone(phone))
            await drop_pending(phone)


@app.before_serving
async def boot():
    emit("INFO", "DEVIL ENGINE v5.0 — Hypercorn worker online, PID %s", os.getpid())
    emit("INFO", "Routes armed: /api/send-otp /api/verify-otp /api/run-script "
                 "/api/stop-script /api/delete-account /api/save-template /api/templates "
                 "/api/logs /api/status /api/health")
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
    emit("INFO", "Shutdown complete — all tasks cancelled, clients disconnected.")


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
# API: authentication
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
                                    device_model="DEVIL Engine",
                                    system_version="Hypercorn/Quart",
                                    app_version="5.0.0")
            await asyncio.wait_for(client.connect(), timeout=25)
            emit("INFO", "send-otp: MTProto connection up for %s", mask_phone(phone), tag=phone)
            sent = await client.send_code_request(phone)
            PENDING[phone] = {"client": client, "phone_code_hash": sent.phone_code_hash,
                              "api_id": api_id, "api_hash": api_hash, "ts": time.time()}
        emit("INFO", "send-otp: code dispatched via %s", type(sent.type).__name__, tag=phone)
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
        return jsonify({"ok": False, "error": "No pending login for this phone. "
                                              "Request a new OTP first."}), 400
    if time.time() - entry["ts"] > PENDING_TTL:
        await drop_pending(phone)
        return jsonify({"ok": False, "error": "OTP attempt expired (10 min). "
                                              "Request a new code."}), 410

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
            "api_id": entry["api_id"],
            "api_hash": entry["api_hash"],
            "connected_at": utcnow(),
        }
        PENDING.pop(phone, None)
        persist_account(phone)
        emit("INFO", "ACCOUNT SAVED: @%s (id=%s)", me.username or "-", me.id, tag=phone)
        return jsonify({"ok": True, "connected": True,
                        "account": ACCOUNTS[phone]["account"],
                        "string_session": session_string,
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


@app.route("/api/delete-account", methods=["POST"])
async def api_delete_account():
    data = await request.get_json(force=True, silent=True) or {}
    phone = str(data.get("phone", "")).strip()
    if not phone:
        return jsonify({"ok": False, "error": "phone is required."}), 400
    stopped = await stop_account_task(phone, reason="account-delete")
    entry = ACCOUNTS.pop(phone, None)
    if not entry:
        return jsonify({"ok": False, "error": "No such account."}), 404
    await safe_disconnect(entry["client"])
    unpersist_account(phone)
    emit("INFO", "Account deleted (%s script stopped): %s",
         "and its" if stopped else "no", mask_phone(phone), tag=phone)
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
        return jsonify({"ok": False, "error": "Account not authorized. "
                                              "Complete OTP verification first."}), 409

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
            old_name = RUNNING[phone]["name"]
            emit("WARNING", "Replacing running script '%s' with '%s'", old_name, name, tag=phone)
            await stop_account_task(phone, reason="replace")
        token = "%s-%s" % (int(time.time() * 1000), os.getpid())
        task = asyncio.create_task(script_engine(phone, name, code, token),
                                   name="engine:%s" % phone)
        task.add_done_callback(on_task_done(phone, name))
        RUNNING[phone] = {"task": task, "token": token, "name": name,
                          "started_at": utcnow()}
    emit("INFO", "Task registered: '%s' on %s", name, mask_phone(phone), tag=phone)
    return jsonify({"ok": True, "message": "Script launched in isolated asyncio task.",
                    "script_name": name, "phone": phone, "started_at": RUNNING[phone]["started_at"]})


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
    return jsonify({"ok": True, "message": "Task cancelled cleanly — handlers purged."})


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
    emit("INFO", "Template saved: '%s' (%s bytes)", name, len(code))
    return jsonify({"ok": True, "message": "Template '%s' saved to templates.json." % name})


@app.route("/api/templates", methods=["GET"])
async def api_templates():
    store = read_json_file(TEMPLATES_FILE)
    return jsonify({"ok": True,
                    "templates": {k: v.get("code", "") for k, v in store.items()}})


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
    for phone, entry in ACCOUNTS.items():
        a = dict(entry["account"])
        a["connected_at"] = entry["connected_at"]
        a["connected"] = entry["client"].is_connected()
        a["running_script"] = RUNNING[phone]["name"] if phone in RUNNING else None
        accounts.append(a)
    processes = []
    for phone, row in RUNNING.items():
        processes.append({"phone": phone, "script_name": row["name"],
                          "started_at": row["started_at"],
                          "token_tail": row["token"][-6:]})
    return jsonify({"ok": True, "server_time": utcnow(), "accounts": accounts,
                    "processes": processes, "pending_logins": len(PENDING)})


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
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DEVIL MULTI-PROJECT MANAGER v5.0</title>
<script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
<script src="https://unpkg.com/lucide@latest"></script>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
  body { font-family: 'Space Grotesk', system-ui, sans-serif; background: #050508; color: #e2e8f0; min-height: 100vh; }
  .mono { font-family: 'JetBrains Mono', monospace; }
  .card { background: linear-gradient(160deg, rgba(15,18,32,.85), rgba(5,5,10,.92)); border: 1px solid rgba(148,163,184,.12); border-radius: .9rem; }
  .field { width: 100%; background: rgba(4,4,10,.8); border: 1px solid rgba(148,163,184,.15); border-radius: .55rem; padding: .55rem .75rem; font-family: 'JetBrains Mono', monospace; font-size: .76rem; color: #e2e8f0; outline: none; transition: border-color .25s, box-shadow .25s; }
  .field:focus { border-color: rgba(244,63,94,.55); box-shadow: 0 0 0 3px rgba(244,63,94,.12); }
  .field::placeholder { color: #475569; }
  select.field option { background: #0a0a14; }
  .flab { font-family: 'JetBrains Mono', monospace; font-size: .58rem; letter-spacing: .16em; color: #8b93a7; display: block; margin-bottom: .3rem; }
  .btn { transition: all .2s ease; }
  .btn:active { transform: scale(.97); }
  .btn:disabled { opacity: .55; pointer-events: none; }
  table { border-collapse: collapse; width: 100%; }
  th, td { padding: .45rem .5rem; text-align: left; }
  tbody tr { border-top: 1px solid rgba(148,163,184,.08); }
  .ed-wrap { display: grid; grid-template-columns: 44px 1fr; }
  .ed-gutter { margin: 0; padding: .7rem 0; font-family: 'JetBrains Mono', monospace; font-size: .7rem; line-height: 1.55; text-align: right; color: #3b4252; background: rgba(10,10,18,.7); overflow: hidden; user-select: none; white-space: pre; border-right: 1px solid rgba(148,163,184,.08); }
  .ed-code { padding: .7rem .85rem; font-family: 'JetBrains Mono', monospace; font-size: .7rem; line-height: 1.55; color: #c8d3e8; background: transparent; border: none; outline: none; resize: none; white-space: pre; overflow: auto; caret-color: #fb7185; }
  .logline { display: grid; grid-template-columns: 66px 54px 92px 1fr; gap: .5rem; padding: .1rem 0; font-family: 'JetBrains Mono', monospace; font-size: .68rem; line-height: 1.5; word-break: break-word; }
  ::-webkit-scrollbar { width: 8px; height: 8px; }
  ::-webkit-scrollbar-track { background: #0a0a12; }
  ::-webkit-scrollbar-thumb { background: #7f1d3a; border-radius: 8px; }
  .toast { padding: .6rem .85rem; border-radius: .65rem; font-size: .74rem; border: 1px solid; margin-bottom: .5rem; transition: opacity .3s ease, transform .3s ease; opacity: 0; transform: translateY(8px); }
  .toast.show { opacity: 1; transform: none; }
  .toast-ok { border-color: rgba(16,185,129,.4); background: rgba(16,185,129,.1); color: #6ee7b7; }
  .toast-err { border-color: rgba(251,113,133,.45); background: rgba(244,63,94,.12); color: #fda4af; }
  .toast-warn { border-color: rgba(245,158,11,.4); background: rgba(245,158,11,.1); color: #fcd34d; }
  .dot { width: 7px; height: 7px; border-radius: 9999px; }
</style>
</head>
<body class="antialiased">
<div class="fixed inset-0 pointer-events-none">
  <div class="absolute w-96 h-96 rounded-full bg-rose-600/15 blur-3xl -top-20 -left-20"></div>
  <div class="absolute w-96 h-96 rounded-full bg-cyan-500/10 blur-3xl bottom-0 right-0"></div>
</div>

<header class="relative border-b border-slate-800/70 bg-slate-950/80 backdrop-blur sticky top-0 z-30">
  <div class="max-w-7xl mx-auto px-4 h-14 flex items-center gap-3">
    <span class="w-8 h-8 rounded-lg bg-gradient-to-br from-rose-600 to-rose-400 flex items-center justify-center">
      <i data-lucide="flame" class="w-4 h-4 text-slate-950"></i>
    </span>
    <div>
      <div class="font-bold text-sm leading-tight">DEVIL MULTI-PROJECT MANAGER <span class="text-rose-400">v5.0</span></div>
      <div class="mono text-[8px] text-slate-500 tracking-[.22em]">MULTI-ACCOUNT TELETHON SCRIPT ENGINE</div>
    </div>
    <div class="ml-auto flex items-center gap-2">
      <span id="sv-pulse" class="mono text-[10px] flex items-center gap-1.5 text-rose-300"><span id="sv-dot" class="dot bg-rose-400"></span><span id="sv-text">OFFLINE</span></span>
      <span id="sv-procs" class="mono text-[10px] px-2.5 py-1 rounded-full border border-cyan-500/40 text-cyan-300 bg-cyan-500/10">0 PROCESSES</span>
    </div>
  </div>
</header>

<main class="relative max-w-7xl mx-auto px-4 py-5 grid lg:grid-cols-12 gap-4">
  <div class="lg:col-span-5 space-y-4">
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="key-round" class="w-4 h-4 text-rose-400"></i>Account Authorization</h2>
      <div class="grid grid-cols-2 gap-2.5">
        <div><label class="flab" for="in-api-id">API_ID</label><input id="in-api-id" class="field" placeholder="204xxxxx" inputmode="numeric"></div>
        <div><label class="flab" for="in-api-hash">API_HASH</label><input id="in-api-hash" type="password" class="field" placeholder="0123abc…"></div>
      </div>
      <div class="mt-2.5"><label class="flab" for="in-phone">PHONE NUMBER</label><input id="in-phone" class="field" placeholder="+1 555 000 1122"></div>
      <button id="btn-otp" class="btn mt-3 w-full py-2.5 rounded-lg bg-rose-500 text-slate-950 font-bold text-sm hover:bg-rose-400 flex items-center justify-center gap-2">
        <i data-lucide="send" class="w-4 h-4"></i><span data-label>Send OTP</span>
      </button>
      <div id="otp-block" class="hidden mt-4 pt-3 border-t border-dashed border-slate-700/70">
        <div class="mono text-[9px] tracking-[.2em] text-emerald-300 mb-2.5">OTP DISPATCHED — CHECK TELEGRAM</div>
        <div class="grid grid-cols-2 gap-2.5">
          <div><label class="flab" for="in-otp">OTP CODE</label><input id="in-otp" class="field" placeholder="12345" autocomplete="one-time-code"></div>
          <div><label class="flab" for="in-2fa">2FA (OPTIONAL)</label><input id="in-2fa" type="password" class="field" placeholder="cloud password"></div>
        </div>
        <button id="btn-verify" class="btn mt-3 w-full py-2.5 rounded-lg bg-emerald-500 text-slate-950 font-bold text-sm hover:bg-emerald-400 flex items-center justify-center gap-2">
          <i data-lucide="shield-check" class="w-4 h-4"></i><span data-label>Verify &amp; Save Session</span>
        </button>
        <div id="session-wrap" class="hidden mt-3">
          <label class="flab" for="out-session">STRING SESSION — SAVED</label>
          <div class="relative">
            <textarea id="out-session" readonly class="field h-16 !text-[10px] resize-none pr-8"></textarea>
            <button id="btn-copy-session" class="absolute top-2 right-2 text-slate-500 hover:text-rose-300"><i data-lucide="copy" class="w-4 h-4"></i></button>
          </div>
        </div>
      </div>
    </section>

    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-2"><i data-lucide="users" class="w-4 h-4 text-cyan-400"></i>Active Accounts</h2>
      <div class="overflow-x-auto mono text-[11px]">
        <table>
          <thead><tr class="text-slate-600 text-[9px] tracking-widest"><th>PHONE</th><th>STATUS</th><th>RUNNING SCRIPT</th><th class="text-right">ACTION</th></tr></thead>
          <tbody id="accounts-body"><tr><td colspan="4" class="py-4 text-center text-slate-600">No accounts authorized yet.</td></tr></tbody>
        </table>
      </div>
    </section>

    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-2"><i data-lucide="activity" class="w-4 h-4 text-emerald-400"></i>Running Processes</h2>
      <div id="procs-body" class="space-y-2">
        <p class="mono text-[11px] text-slate-600 text-center py-5">No engines running.</p>
      </div>
    </section>
  </div>

  <div class="lg:col-span-7 space-y-4">
    <section class="card p-4">
      <h2 class="font-bold text-sm flex items-center gap-2 mb-3"><i data-lucide="terminal-square" class="w-4 h-4 text-rose-400"></i>Dynamic Script Runner</h2>
      <div class="grid sm:grid-cols-2 gap-2.5 mb-2.5">
        <div>
          <label class="flab" for="run-account">RUN ON ACCOUNT</label>
          <select id="run-account" class="field"><option value="">— no accounts —</option></select>
        </div>
        <div>
          <label class="flab" for="script-name">SCRIPT NAME</label>
          <input id="script-name" class="field" placeholder="Cleaner Bot Account 1">
        </div>
      </div>
      <div class="rounded-lg border border-slate-800 overflow-hidden bg-[#07070d]">
        <div class="flex items-center gap-1.5 px-3 py-2 border-b border-slate-800 bg-slate-950/60">
          <span class="w-2.5 h-2.5 rounded-full bg-rose-500/70"></span>
          <span class="w-2.5 h-2.5 rounded-full bg-amber-500/70"></span>
          <span class="w-2.5 h-2.5 rounded-full bg-emerald-500/70"></span>
          <span class="mono text-[9px] text-slate-500 ml-1">script.py — telethon context</span>
          <div class="ml-auto flex items-center gap-1.5">
            <select id="templates" class="field !py-1 !px-2 !text-[10px] !w-32"><option value="">templates…</option></select>
            <button id="btn-load-tpl" class="btn mono text-[10px] px-2.5 py-1 rounded border border-slate-700 text-slate-400 hover:border-slate-500">Load</button>
          </div>
        </div>
        <div class="ed-wrap">
          <pre id="gutter" class="ed-gutter">1</pre>
          <textarea id="code" class="ed-code" style="min-height:330px;max-height:440px" spellcheck="false" wrap="off"></textarea>
        </div>
      </div>
      <div class="flex flex-wrap gap-1.5 mt-2.5 mono text-[9px]">
        <span class="px-2 py-1 rounded-full border border-emerald-500/40 text-emerald-300 bg-emerald-500/10">client</span>
        <span class="px-2 py-1 rounded-full border border-emerald-500/40 text-emerald-300 bg-emerald-500/10">SESSION_STRING</span>
        <span class="px-2 py-1 rounded-full border border-cyan-500/40 text-cyan-300 bg-cyan-500/10">PHONE · events · errors · utils · types</span>
        <span class="px-2 py-1 rounded-full border border-slate-700 text-slate-400">log("msg") · print → terminal</span>
      </div>
      <div class="grid sm:grid-cols-2 gap-2.5 mt-3">
        <button id="btn-run" class="btn py-2.5 rounded-lg bg-gradient-to-r from-rose-500 to-rose-400 text-slate-950 font-bold text-sm hover:opacity-95 flex items-center justify-center gap-2">
          <i data-lucide="play" class="w-4 h-4"></i><span data-label>Run Script on Selected Account</span>
        </button>
        <button id="btn-save-tpl" class="btn py-2.5 rounded-lg border border-cyan-500/40 text-cyan-300 font-bold text-sm hover:bg-cyan-500/10 flex items-center justify-center gap-2">
          <i data-lucide="save" class="w-4 h-4"></i><span data-label>Save Code Template</span>
        </button>
      </div>
    </section>

    <section class="card overflow-hidden">
      <div class="flex items-center gap-2 px-4 py-2.5 border-b border-slate-800 bg-slate-950/60">
        <span class="w-2.5 h-2.5 rounded-full bg-rose-500/70"></span>
        <span class="w-2.5 h-2.5 rounded-full bg-amber-500/70"></span>
        <span class="w-2.5 h-2.5 rounded-full bg-emerald-500/70"></span>
        <span class="mono text-[10px] text-slate-400 ml-1">devil://live-terminal</span>
        <div class="ml-auto flex items-center gap-1.5">
          <input id="in-filter" class="field !py-1 !px-2 !text-[10px] !w-28" placeholder="filter…">
          <button id="btn-pause" class="btn mono text-[10px] px-2 py-1 rounded border border-slate-700 text-slate-400 hover:border-slate-500"><span data-label>Pause</span></button>
          <button id="btn-clear" class="btn mono text-[10px] px-2 py-1 rounded border border-slate-700 text-slate-400 hover:border-slate-500">Clear</button>
        </div>
      </div>
      <div id="term-body" class="h-[340px] overflow-y-auto px-4 py-3">
        <div class="logline"><span class="text-slate-600">--:--:--</span><span class="text-slate-500 font-semibold">SYS   </span><span class="text-slate-500">[core]</span><span class="text-slate-500">Terminal attached. Streaming GET /api/logs…</span></div>
      </div>
    </section>
  </div>
</main>

<div id="toasts" class="fixed bottom-4 right-4 z-50 w-80"></div>

<script>
(function () {
  var state = { since: 0, paused: false, filter: "", accounts: [], selectedPhone: "" };

  var DEFAULT_SCRIPT = [
    "# Sample engine — /status command bot + incoming audit",
    "# Injected globals: client, SESSION_STRING, PHONE, events, errors,",
    "# utils, types, asyncio, log(msg), print(...) -> live terminal",
    "",
    "COUNTER = {\"n\": 0}",
    "",
    "@client.on(events.NewMessage(pattern=r\"^/status$\"))",
    "async def status_cmd(event):",
    "    COUNTER[\"n\"] += 1",
    "    me = await client.get_me()",
    "    await event.reply(\"Engine alive as @\" + str(me.username or me.id) + \" | hits: \" + str(COUNTER[\"n\"]))",
    "",
    "@client.on(events.NewMessage(incoming=True))",
    "async def audit(event):",
    "    log(\"incoming msg %s in chat %s\" % (event.id, event.chat_id))",
    "",
    "async def main():",
    "    me = await client.get_me()",
    "    print(\"Booted as\", me.username or me.id)",
    "    print(\"Send /status in Saved Messages to test the bot.\")",
    ""
  ].join("\n");

  function $(id) { return document.getElementById(id); }
  function val(id) { var el = $(id); return el ? el.value.trim() : ""; }
  function esc(s) { return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  function toast(msg, kind) {
    var div = document.createElement("div");
    div.className = "toast " + (kind === "error" ? "toast-err" : (kind === "warn" ? "toast-warn" : "toast-ok"));
    div.textContent = msg;
    $("toasts").appendChild(div);
    setTimeout(function () { div.classList.add("show"); }, 10);
    setTimeout(function () { div.classList.remove("show"); setTimeout(function () { div.remove(); }, 300); }, 4600);
  }

  function setBusy(id, busy, busyText) {
    var b = $(id);
    if (!b) return;
    b.disabled = busy;
    var label = b.querySelector("[data-label]");
    if (label) {
      if (!label.dataset.orig) label.dataset.orig = label.textContent;
      label.textContent = busy ? (busyText || "Working…") : label.dataset.orig;
    }
  }

  async function api(path, opts) {
    var res = await fetch(path, opts || {});
    return res;
  }

  async function post(path, payload) {
    var res = await api(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    var data = null;
    try { data = await res.json(); } catch (e) { data = { ok: false, error: "HTTP " + res.status }; }
    return { status: res.status, data: data };
  }

  // ---------- auth ----------
  async function sendOtp() {
    var apiId = parseInt(val("in-api-id"), 10);
    var payload = { api_id: apiId, api_hash: val("in-api-hash"), phone: val("in-phone") };
    if (!apiId || !payload.api_hash || !payload.phone) { toast("Fill API_ID, API_HASH and phone.", "warn"); return; }
    setBusy("btn-otp", true, "Dispatching…");
    try {
      var r = await post("/api/send-otp", payload);
      if (r.data.already_connected) { toast("Account already authorized.", "ok"); refreshStatus(); return; }
      if (!r.data.ok) { toast(r.data.error || "send-otp failed (" + r.status + ")", "error"); return; }
      $("otp-block").classList.remove("hidden");
      toast("OTP dispatched (" + (r.data.code_type || "app") + ").", "ok");
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { setBusy("btn-otp", false); }
  }

  async function verifyOtp() {
    var payload = { phone: val("in-phone"), otp_code: val("in-otp"), password: val("in-2fa") };
    if (!payload.phone || !payload.otp_code) { toast("Phone and OTP code required.", "warn"); return; }
    setBusy("btn-verify", true, "Verifying…");
    try {
      var r = await post("/api/verify-otp", payload);
      if (r.data.need_password) { toast(r.data.message || "2FA password required.", "warn"); $("in-2fa").focus(); return; }
      if (!r.data.ok) { toast(r.data.error || "verify failed (" + r.status + ")", "error"); return; }
      if (r.data.string_session) {
        $("session-wrap").classList.remove("hidden");
        $("out-session").value = r.data.string_session;
      }
      toast("Saved: @" + (r.data.account.username || r.data.account.id), "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { setBusy("btn-verify", false); }
  }

  async function deleteAccount(phone) {
    if (!window.confirm("Delete account " + phone + " and stop its script?")) return;
    try {
      var r = await post("/api/delete-account", { phone: phone });
      if (!r.data.ok) { toast(r.data.error || "delete failed", "error"); return; }
      toast("Account removed.", "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
  }

  // ---------- script engine ----------
  async function runScript() {
    var phone = val("run-account");
    var payload = { phone_number: phone, script_code: $("code").value, script_name: val("script-name") };
    if (!phone) { toast("Select an account first.", "warn"); return; }
    if (!payload.script_code.trim()) { toast("Editor is empty.", "warn"); return; }
    setBusy("btn-run", true, "Launching…");
    try {
      var r = await post("/api/run-script", payload);
      if (!r.data.ok) { toast(r.data.error || "run failed (" + r.status + ")", "error"); return; }
      toast("Engine launched: " + r.data.script_name, "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { setBusy("btn-run", false); }
  }

  async function stopScript(phone) {
    try {
      var r = await post("/api/stop-script", { phone_number: phone });
      if (!r.data.ok) { toast(r.data.error || "stop failed", "error"); return; }
      toast("Task cancelled.", "ok");
      refreshStatus();
    } catch (e) { toast("Network error: " + e.message, "error"); }
  }

  async function saveTemplate() {
    var payload = { name: val("script-name") || "untitled-" + Date.now(), code: $("code").value };
    if (!payload.code.trim()) { toast("Editor is empty.", "warn"); return; }
    setBusy("btn-save-tpl", true, "Saving…");
    try {
      var r = await post("/api/save-template", payload);
      if (!r.data.ok) { toast(r.data.error || "save failed", "error"); return; }
      toast(r.data.message || "Template saved.", "ok");
      loadTemplateList();
    } catch (e) { toast("Network error: " + e.message, "error"); }
    finally { setBusy("btn-save-tpl", false); }
  }

  var TEMPLATE_CACHE = {};
  async function loadTemplateList() {
    try {
      var res = await api("/api/templates");
      var j = await res.json();
      TEMPLATE_CACHE = j.templates || {};
      var sel = $("templates");
      sel.innerHTML = '<option value="">templates…</option>';
      Object.keys(TEMPLATE_CACHE).forEach(function (k) {
        var o = document.createElement("option");
        o.value = k; o.textContent = k;
        sel.appendChild(o);
      });
    } catch (e) { /* offline */ }
  }

  // ---------- rendering ----------
  function renderAccounts(accounts) {
    var body = $("accounts-body");
    if (!accounts.length) {
      body.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-600">No accounts authorized yet.</td></tr>';
      return;
    }
    body.innerHTML = accounts.map(function (a) {
      var statusDot = a.connected
        ? '<span class="inline-flex items-center gap-1.5 text-emerald-300"><span class="dot bg-emerald-400"></span>ONLINE</span>'
        : '<span class="inline-flex items-center gap-1.5 text-slate-500"><span class="dot bg-slate-500"></span>IDLE</span>';
      var script = a.running_script
        ? '<span class="text-rose-300">' + esc(a.running_script) + "</span>"
        : '<span class="text-slate-600">—</span>';
      return "<tr>" +
        "<td class='mono'>" + esc(a.phone) + "</td>" +
        "<td>" + statusDot + "</td>" +
        "<td>" + script + "</td>" +
        "<td class='text-right'><button data-del='" + esc(a.phone) + "' class='btn mono text-[10px] px-2 py-1 rounded border border-rose-500/50 text-rose-300 hover:bg-rose-500/10'>DELETE</button></td>" +
      "</tr>";
    }).join("");
    Array.prototype.forEach.call(body.querySelectorAll("[data-del]"), function (btn) {
      btn.addEventListener("click", function () { deleteAccount(btn.getAttribute("data-del")); });
    });

    var sel = $("run-account");
    var keep = sel.value;
    sel.innerHTML = accounts.length ? "" : '<option value="">— no accounts —</option>';
    accounts.forEach(function (a) {
      var o = document.createElement("option");
      o.value = a.phone;
      o.textContent = a.phone + (a.username ? " (@" + a.username + ")" : "");
      sel.appendChild(o);
    });
    if (keep) sel.value = keep;
  }

  function renderProcs(processes, serverTime) {
    var host = $("procs-body");
    var chip = $("sv-procs");
    chip.textContent = processes.length + (processes.length === 1 ? " PROCESS" : " PROCESSES");
    if (!processes.length) {
      host.innerHTML = '<p class="mono text-[11px] text-slate-600 text-center py-5">No engines running.</p>';
      return;
    }
    host.innerHTML = processes.map(function (p) {
      var started = Date.parse(p.started_at);
      var up = isNaN(started) ? "—" : fmtUptime(Math.max(0, Date.now() - started));
      return '<div class="card p-3 flex items-center gap-3 proc-card">' +
        '<span class="dot bg-rose-400 shrink-0"></span>' +
        '<div class="min-w-0 flex-1">' +
          '<div class="mono text-[11px] font-semibold text-rose-200 truncate">' + esc(p.script_name) + "</div>" +
          '<div class="mono text-[9px] text-slate-500 truncate">' + esc(p.phone) + " · up " + up + "</div>" +
        "</div>" +
        "<button data-stop='" + esc(p.phone) + "' class='btn mono text-[10px] px-2.5 py-1.5 rounded border border-rose-500/50 text-rose-300 hover:bg-rose-500/10 shrink-0'>STOP</button>" +
      "</div>";
    }).join("");
    Array.prototype.forEach.call(host.querySelectorAll("[data-stop]"), function (btn) {
      btn.addEventListener("click", function () { stopScript(btn.getAttribute("data-stop")); });
    });
  }

  function fmtUptime(ms) {
    var s = Math.floor(ms / 1000);
    var h = Math.floor(s / 3600); s -= h * 3600;
    var m = Math.floor(s / 60); s -= m * 60;
    return (h ? h + "h " : "") + (m ? m + "m " : "") + s + "s";
  }

  function tagColor(tag) {
    var palette = ["text-rose-300", "text-cyan-300", "text-emerald-300", "text-amber-300", "text-violet-300", "text-pink-300"];
    if (tag === "core") return "text-slate-500";
    var h = 0;
    for (var i = 0; i < tag.length; i++) h = (h * 31 + tag.charCodeAt(i)) >>> 0;
    return palette[h % palette.length];
  }

  function appendLog(row) {
    if (state.filter) {
      var hay = (row.level + " " + row.tag + " " + row.message).toLowerCase();
      if (hay.indexOf(state.filter) === -1) return;
    }
    var host = $("term-body");
    var lvl = String(row.level || "INFO").toUpperCase();
    var lvlCls = (lvl === "ERROR" || lvl === "CRITICAL") ? "text-rose-400" : (lvl === "WARNING" ? "text-amber-400" : (lvl === "DEBUG" ? "text-slate-500" : "text-cyan-300"));
    var time = String(row.ts || "").split("T")[1] || "";
    time = time.replace("Z", "").split(".")[0].split("+")[0] || "--:--:--";
    var div = document.createElement("div");
    div.className = "logline";
    div.innerHTML = '<span class="text-slate-600">' + esc(time) + '</span>' +
      '<span class="' + lvlCls + ' font-semibold">' + esc((lvl + "      ").slice(0, 6)) + '</span>' +
      '<span class="' + tagColor(String(row.tag || "core")) + ' truncate">[' + esc(String(row.tag || "core")) + "]</span>" +
      '<span class="text-slate-300">' + esc(row.message) + "</span>";
    host.appendChild(div);
    while (host.children.length > 260) host.removeChild(host.firstChild);
    var nearBottom = host.scrollHeight - host.scrollTop - host.clientHeight < 48;
    if (nearBottom && !state.paused) host.scrollTop = host.scrollHeight;
  }

  // ---------- polling ----------
  var online = false;
  function setServer(on) {
    online = on;
    $("sv-text").textContent = on ? "ONLINE" : "OFFLINE";
    $("sv-dot").className = "dot " + (on ? "bg-emerald-400" : "bg-rose-400");
    $("sv-pulse").className = "mono text-[10px] flex items-center gap-1.5 " + (on ? "text-emerald-300" : "text-rose-300");
  }

  async function pollLogs() {
    try {
      var res = await api("/api/logs?since=" + state.since);
      var j = await res.json();
      (j.logs || []).forEach(appendLog);
      if (typeof j.latest === "number") state.since = j.latest;
      setServer(true);
      setTimeout(pollLogs, 1500);
    } catch (e) {
      setServer(false);
      setTimeout(pollLogs, 4000);
    }
  }

  async function refreshStatus() {
    try {
      var res = await api("/api/status");
      var j = await res.json();
      renderAccounts(j.accounts || []);
      renderProcs(j.processes || [], j.server_time);
      setServer(true);
    } catch (e) { setServer(false); }
    setTimeout(refreshStatus, 4000);
  }

  // ---------- editor ----------
  function syncGutter() {
    var lines = $("code").value.split("\n").length;
    var buf = [];
    for (var i = 1; i <= lines; i++) buf.push(i);
    $("gutter").textContent = buf.join("\n");
  }

  function initEditor() {
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
  }

  document.addEventListener("DOMContentLoaded", function () {
    $("btn-otp").addEventListener("click", sendOtp);
    $("btn-verify").addEventListener("click", verifyOtp);
    $("btn-run").addEventListener("click", runScript);
    $("btn-save-tpl").addEventListener("click", saveTemplate);
    $("btn-copy-session").addEventListener("click", function () {
      var el = $("out-session");
      var done = function () { toast("Session copied — guard it like a password.", "ok"); };
      el.select();
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(el.value).then(done, function () { document.execCommand("copy"); done(); });
      } else { document.execCommand("copy"); done(); }
    });
    $("btn-load-tpl").addEventListener("click", function () {
      var k = val("templates");
      if (!k || !TEMPLATE_CACHE[k]) { toast("Pick a template first.", "warn"); return; }
      $("code").value = TEMPLATE_CACHE[k];
      $("script-name").value = k;
      syncGutter();
      toast("Template '" + k + "' loaded.", "ok");
    });
    $("btn-clear").addEventListener("click", function () { $("term-body").innerHTML = ""; });
    $("in-filter").addEventListener("input", function () { state.filter = this.value.trim().toLowerCase(); });
    $("btn-pause").addEventListener("click", function () {
      state.paused = !state.paused;
      var l = this.querySelector("[data-label]");
      if (l) l.textContent = state.paused ? "Resume" : "Pause";
    });
    if (window.lucide) lucide.createIcons();
    initEditor();
    loadTemplateList();
    pollLogs();
    refreshStatus();
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
