# main.py
# TG RELAY - Production Telegram Channel Forwarder
# Quart + Telethon + Hypercorn
# v1.1 — OTP + StringSession login

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
# Logging
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
_stdout.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
)
logger.addHandler(_stdout)

# ----------------------------------------------------------------------------
# App
# ----------------------------------------------------------------------------

app = Quart(__name__)

PENDING = {}
ACTIVE = {}
PENDING_TTL = 600.0
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
        return "***"
    return "+" + digits[:3] + "******" + digits[-2:]


def parse_ident(raw):
    s = str(raw).strip()

    if not s:
        raise ValueError("empty identifier")

    s = s.split("?")[0].rstrip("/")

    m = re.match(
        r"^(?:https?://)?t\.me/c/(\d+)(?:/\d+)?$",
        s
    )

    if m:
        return int("-100" + m.group(1))

    m = re.match(
        r"^(?:https?://)?t\.me/([\w]+)$",
        s
    )

    if m:
        return m.group(1)

    if s.startswith("@"):
        return s[1:]

    if re.fullmatch(r"-?\d+", s):
        return int(s)

    return s


def entity_title(entity):
    return (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or getattr(entity, "first_name", None)
        or str(getattr(entity, "id", "?"))
    )


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

        stale = [
            p for p, e in list(PENDING.items())
            if now - e["ts"] > PENDING_TTL
        ]

        for phone in stale:
            logger.info(
                "Sweeper: expiring stale OTP attempt for %s",
                mask_phone(phone)
            )
            await drop_pending(phone)


@app.before_serving
async def boot():
    logger.info(
        "Hypercorn worker online — Quart app booted, PID %s",
        os.getpid()
    )

    logger.info(
        "Routes armed: /api/send-otp /api/verify-otp "
        "/api/login-session /api/start-forwarder "
        "/api/stop-forwarder /api/logs /api/status /api/health"
    )

    app.add_background_task(pending_sweeper)


@app.after_serving
async def shutdown():
    await stop_forwarder_internal()

    for phone in list(PENDING):
        await drop_pending(phone)

    for phone, entry in list(ACTIVE.items()):
        await safe_disconnect(entry["client"])
        ACTIVE.pop(phone, None)

    logger.info(
        "Shutdown complete — all Telethon clients disconnected."
    )


# ----------------------------------------------------------------------------
# CORS
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
# OTP LOGIN
# ----------------------------------------------------------------------------

@app.route("/api/send-otp", methods=["POST"])
async def api_send_otp():
    data = await request.get_json(force=True, silent=True) or {}

    try:
        api_id = int(str(data.get("api_id", "")).strip())
    except (TypeError, ValueError):
        return jsonify({
            "ok": False,
            "error": "api_id must be an integer."
        }), 400

    api_hash = str(data.get("api_hash", "")).strip()
    phone = str(data.get("phone", "")).strip()

    if not api_hash or not phone:
        return jsonify({
            "ok": False,
            "error": "api_hash and phone are required."
        }), 400

    if phone in ACTIVE:
        acct = ACTIVE[phone]["account"]

        return jsonify({
            "ok": True,
            "already_connected": True,
            "account": acct,
            "message": "Session already authorized."
        })

    try:
        async with STATE_LOCK:
            await drop_pending(phone)

            client = TelegramClient(
                StringSession(),
                api_id,
                api_hash,
                device_model="TG Relay Web",
                system_version="Hypercorn/Quart",
                app_version="1.1.0"
            )

            await asyncio.wait_for(
                client.connect(),
                timeout=25
            )

            logger.info(
                "send-otp: MTProto connection up for %s",
                mask_phone(phone)
            )

            sent = await client.send_code_request(phone)

            PENDING[phone] = {
                "client": client,
                "phone_code_hash": sent.phone_code_hash,
                "api_id": api_id,
                "api_hash": api_hash,
                "ts": time.time(),
            }

        code_type = type(sent.type).__name__

        logger.info(
            "send-otp: code dispatched to %s via %s",
            mask_phone(phone),
            code_type
        )

        return jsonify({
            "ok": True,
            "message": "OTP dispatched via Telegram.",
            "code_type": code_type
        })

    except asyncio.TimeoutError:
        await drop_pending(phone)

        return jsonify({
            "ok": False,
            "error": "Timed out connecting to Telegram DC."
        }), 504

    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)

        logger.warning(
            "send-otp failed for %s: %s",
            mask_phone(phone),
            msg
        )

        await drop_pending(phone)

        return jsonify({
            "ok": False,
            "error": msg
        }), status

    except Exception as exc:
        logger.exception("send-otp unexpected failure")

        await drop_pending(phone)

        return jsonify({
            "ok": False,
            "error": "Unexpected: %s" % exc
        }), 500


@app.route("/api/verify-otp", methods=["POST"])
async def api_verify_otp():
    data = await request.get_json(force=True, silent=True) or {}

    phone = str(data.get("phone", "")).strip()
    code = str(
        data.get("otp_code", "")
    ).strip().replace(" ", "")

    password = str(
        data.get("password", "")
    ).strip()

    if not phone or not code:
        return jsonify({
            "ok": False,
            "error": "phone and otp_code are required."
        }), 400

    entry = PENDING.get(phone)

    if not entry:
        return jsonify({
            "ok": False,
            "error": (
                "No pending login for this phone. "
                "Request a new OTP first."
            )
        }), 400

    if time.time() - entry["ts"] > PENDING_TTL:
        await drop_pending(phone)

        return jsonify({
            "ok": False,
            "error": (
                "OTP attempt expired (10 min). "
                "Request a new code."
            )
        }), 410

    client = entry["client"]

    try:
        if not client.is_connected():
            await asyncio.wait_for(
                client.connect(),
                timeout=25
            )

        try:
            await client.sign_in(
                phone=phone,
                code=code,
                phone_code_hash=entry["phone_code_hash"]
            )

        except errors.SessionPasswordNeededError:
            logger.info(
                "verify: 2FA cloud password required for %s",
                mask_phone(phone)
            )

            if not password:
                return jsonify({
                    "ok": False,
                    "need_password": True,
                    "message": (
                        "Two-step verification is ON — "
                        "provide the cloud password and submit again."
                    )
                })

            await client.sign_in(password=password)

        me = await client.get_me()

        string_session = client.session.save()

        account = {
            "id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": phone,
        }

        ACTIVE[phone] = {
            "client": client,
            "account": account,
            "connected_at": utcnow()
        }

        PENDING.pop(phone, None)

        logger.info(
            "verify: CONNECTED %s as @%s (id=%s)",
            mask_phone(phone),
            me.username or "-",
            me.id
        )

        return jsonify({
            "ok": True,
            "connected": True,
            "account": account,
            "string_session": string_session,
            "message": (
                "Account connected. "
                "Store the string session safely."
            )
        })

    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)

        logger.warning(
            "verify failed for %s: %s",
            mask_phone(phone),
            msg
        )

        if status == 410:
            await drop_pending(phone)

        return jsonify({
            "ok": False,
            "error": msg
        }), status

    except Exception as exc:
        logger.exception("verify unexpected failure")

        return jsonify({
            "ok": False,
            "error": "Unexpected: %s" % exc
        }), 500


# ----------------------------------------------------------------------------
# STRING SESSION LOGIN
# ----------------------------------------------------------------------------

@app.route("/api/login-session", methods=["POST"])
async def api_login_session():
    data = await request.get_json(force=True, silent=True) or {}

    try:
        api_id = int(
            str(data.get("api_id", "")).strip()
        )
    except (TypeError, ValueError):
        return jsonify({
            "ok": False,
            "error": "api_id must be an integer."
        }), 400

    api_hash = str(
        data.get("api_hash", "")
    ).strip()

    session_string = str(
        data.get("session_string", "")
    ).strip()

    if not api_hash or not session_string:
        return jsonify({
            "ok": False,
            "error": (
                "api_hash and session_string are required."
            )
        }), 400

    logger.info(
        "session-login: verifying %s-char StringSession (blob redacted)",
        len(session_string)
    )

    try:
        parsed = StringSession(session_string)

    except Exception:
        logger.warning(
            "session-login: rejected — invalid StringSession"
        )

        return jsonify({
            "ok": False,
            "error": "Session string is malformed."
        }), 400

    client = TelegramClient(
        parsed,
        api_id,
        api_hash,
        device_model="TG Relay Web",
        system_version="Hypercorn/Quart",
        app_version="1.1.0"
    )

    try:
        await asyncio.wait_for(
            client.connect(),
            timeout=25
        )

        try:
            authorized = await client.is_user_authorized()

        except errors.AuthKeyUnregisteredError:
            authorized = False

        if not authorized:
            await safe_disconnect(client)

            logger.warning(
                "session-login: auth key rejected by Telegram"
            )

            return jsonify({
                "ok": False,
                "error": (
                    "Session is invalid or expired — "
                    "Telegram rejected the auth key."
                )
            }), 401

        me = await client.get_me()

        phone = (
            "+" + me.phone
            if me.phone
            else "session-%s" % me.id
        )

        if phone in ACTIVE:
            await safe_disconnect(client)

            logger.info(
                "session-login: %s already active as @%s",
                mask_phone(phone),
                me.username or me.id
            )

            return jsonify({
                "ok": True,
                "already_connected": True,
                "account": ACTIVE[phone]["account"],
                "message": "Account already active."
            })

        account = {
            "id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": phone,
        }

        ACTIVE[phone] = {
            "client": client,
            "account": account,
            "connected_at": utcnow()
        }

        logger.info(
            "session-login: CONNECTED %s as @%s (id=%s)",
            mask_phone(phone),
            me.username or "-",
            me.id
        )

        return jsonify({
            "ok": True,
            "connected": True,
            "account": account,
            "message": (
                "Session verified and account connected."
            )
        })

    except asyncio.TimeoutError:
        await safe_disconnect(client)

        return jsonify({
            "ok": False,
            "error": "Timed out connecting to Telegram DC."
        }), 504

    except errors.RPCError as exc:
        status, msg = rpc_error_payload(exc)

        logger.warning(
            "session-login RPC failure: %s",
            msg
        )

        await safe_disconnect(client)

        return jsonify({
            "ok": False,
            "error": msg
        }), status

    except Exception as exc:
        logger.exception(
            "session-login unexpected failure"
        )

        await safe_disconnect(client)

        return jsonify({
            "ok": False,
            "error": "Unexpected: %s" % exc
        }), 500


# ----------------------------------------------------------------------------
# FORWARDER
# ----------------------------------------------------------------------------

async def stop_forwarder_internal():
    if FWD["handler"] and FWD["client"]:
        try:
            FWD["client"].remove_event_handler(
                FWD["handler"],
                FWD["builder"]
            )
        except (ValueError, KeyError):
            pass

    was = FWD["running"]

    FWD.update({
        "running": False,
        "handler": None,
        "builder": None,
        "client": None,
        "phone": None,
        "source": None,
        "targets": [],
        "branding": "",
        "started_at": None,
    })

    return was


@app.route("/api/start-forwarder", methods=["POST"])
async def api_start_forwarder():
    data = await request.get_json(
        force=True,
        silent=True
    ) or {}

    phone = str(
        data.get("phone", "")
    ).strip()

    branding = str(
        data.get("custom_branding_text", "")
    ).strip()

    src_raw = str(
        data.get("source_channel_id", "")
    ).strip()

    targets_raw = [
        t.strip()
        for t in str(
            data.get("target_channels", "")
        ).split(",")
    ]

    if not src_raw or not any(targets_raw):
        return jsonify({
            "ok": False,
            "error": (
                "source_channel_id and at least one "
                "target channel are required."
            )
        }), 400

    used_phone, entry = active_account_or_sole(phone)

    if not entry:
        return jsonify({
            "ok": False,
            "error": (
                "No connected account. "
                "Complete authentication first."
            )
        }), 409

    client = entry["client"]

    if not client.is_connected():
        await asyncio.wait_for(
            client.connect(),
            timeout=25
        )

    try:
        src_ident = parse_ident(src_raw)

    except ValueError:
        return jsonify({
            "ok": False,
            "error": (
                "source_channel_id is not parseable. "
                "Use -100… id, @username or t.me link."
            )
        }), 422

    ident_list = []

    for raw in targets_raw:
        if not raw:
            continue

        try:
            ident_list.append(
                parse_ident(raw)
            )

        except ValueError:
            return jsonify({
                "ok": False,
                "error": "Unparseable target: %s" % raw
            }), 422

    if not ident_list:
        return jsonify({
            "ok": False,
            "error": (
                "No usable target channels supplied."
            )
        }), 400

    try:
        source_entity = await client.get_entity(
            src_ident
        )

    except Exception:
        return jsonify({
            "ok": False,
            "error": (
                "Source not visible to this account. "
                "Join it first where appropriate."
            )
        }), 404

    resolved = []

    for ident in ident_list:
        try:
            ent = await client.get_entity(ident)

            resolved.append({
                "entity": ent,
                "title": entity_title(ent)
            })

        except Exception:
            logger.warning(
                "start: target %s not resolvable by account",
                ident
            )

    if not resolved:
        return jsonify({
            "ok": False,
            "error": (
                "None of the targets resolve."
            )
        }), 404

    src_title = entity_title(source_entity)

    async with STATE_LOCK:
        if await stop_forwarder_internal():
            logger.info(
                "start: detached previous forwarder binding."
            )

        builder = events.NewMessage(
            chats=source_entity
        )

        async def relay(event):
            msg = event.message

            if getattr(msg, "action", None):
                return

            body = (
                msg.message or ""
            ).strip()

            branded = (
                body + "\n\n" + branding
            ).strip() if branding else body

            for target in FWD["targets"]:
                ent = target["entity"]

                try:
                    if msg.media is not None:
                        await client.send_file(
                            ent,
                            msg.media,
                            caption=(branded or None)
                        )

                    else:
                        if not branded:
                            continue

                        await client.send_message(
                            ent,
                            branded,
                            link_preview=False
                        )

                    FWD["forwarded"] += 1

                    logger.info(
                        "relay: msg %s → %s",
                        msg.id,
                        target["title"]
                    )

                except errors.FloodWaitError as fw:
                    logger.warning(
                        "relay: FloodWait %ss on %s",
                        fw.seconds,
                        target["title"]
                    )

                except Exception as exc:
                    FWD["failed"] += 1

                    logger.error(
                        "relay: send to %s failed: %s",
                        target["title"],
                        exc
                    )

        client.add_event_handler(
            relay,
            builder
        )

        FWD.update({
            "running": True,
            "handler": relay,
            "builder": builder,
            "client": client,
            "phone": used_phone,
            "source": {
                "title": src_title,
                "id": getattr(
                    source_entity,
                    "id",
                    None
                )
            },
            "targets": resolved,
            "branding": branding,
            "forwarded": 0,
            "failed": 0,
            "started_at": utcnow(),
        })

    logger.info(
        "start: FORWARDER LIVE %s → %s",
        src_title,
        ", ".join(
            t["title"]
            for t in resolved
        )
    )

    return jsonify({
        "ok": True,
        "message": (
            "Forwarder bound. "
            "Listening for new messages."
        ),
        "source": {
            "title": src_title,
            "id": getattr(
                source_entity,
                "id",
                None
            )
        },
        "targets": [
            {"title": t["title"]}
            for t in resolved
        ],
        "branding": branding,
    })


@app.route("/api/stop-forwarder", methods=["POST"])
async def api_stop_forwarder():
    async with STATE_LOCK:
        was = await stop_forwarder_internal()

    if was:
        logger.info(
            "stop: forwarder stopped "
            "(forwarded=%s failed=%s)",
            FWD["forwarded"],
            FWD["failed"]
        )

        return jsonify({
            "ok": True,
            "message": (
                "Forwarder stopped. "
                "Handler detached."
            )
        })

    return jsonify({
        "ok": False,
        "error": "Forwarder is not running."
    }), 409


# ----------------------------------------------------------------------------
# LOGS / STATUS / HEALTH
# ----------------------------------------------------------------------------

@app.route("/api/logs", methods=["GET"])
async def api_logs():
    try:
        since = int(
            request.args.get(
                "since",
                "0"
            )
        )
    except ValueError:
        since = 0

    fresh = [
        row for row in LOG_BUFFER
        if row["id"] > since
    ]

    return jsonify({
        "ok": True,
        "logs": fresh,
        "latest": LOG_SEQ[0]
    })


@app.route("/api/status", methods=["GET"])
async def api_status():
    accounts = []

    for phone, entry in ACTIVE.items():
        a = dict(entry["account"])

        a["phone"] = mask_phone(
            a.get("phone", phone)
        )

        a["connected_at"] = entry[
            "connected_at"
        ]

        accounts.append(a)

    return jsonify({
        "ok": True,
        "server_time": utcnow(),
        "accounts": accounts,
        "pending_logins": len(PENDING),
        "forwarder": {
            "running": FWD["running"],
            "phone": (
                mask_phone(FWD["phone"])
                if FWD["phone"]
                else None
            ),
            "source": FWD["source"],
            "targets": [
                t["title"]
                for t in FWD["targets"]
            ],
            "branding": FWD["branding"],
            "forwarded": FWD["forwarded"],
            "failed": FWD["failed"],
            "started_at": FWD["started_at"],
        },
    })


@app.route("/api/health", methods=["GET"])
async def api_health():
    return jsonify({
        "ok": True,
        "ts": utcnow()
    })


# ----------------------------------------------------------------------------
# DASHBOARD
# ----------------------------------------------------------------------------

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>TG RELAY — Control Deck</title>

<script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
<script src="https://unpkg.com/lucide@latest"></script>

<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@400;600&display=swap"
      rel="stylesheet">

<style>
body {
  font-family: 'Space Grotesk', system-ui, sans-serif;
  background: #04060c;
  color: #e2e8f0;
  min-height: 100vh;
}

.mono {
  font-family: 'JetBrains Mono', monospace;
}

.glass {
  background: linear-gradient(
    160deg,
    rgba(15,23,42,.85),
    rgba(2,6,23,.92)
  );
  border: 1px solid rgba(148,163,184,.12);
  backdrop-filter: blur(12px);
}

.field {
  width: 100%;
  background: rgba(2,6,23,.75);
  border: 1px solid rgba(148,163,184,.15);
  border-radius: .6rem;
  padding: .6rem .8rem;
  font-family: 'JetBrains Mono', monospace;
  font-size: .8rem;
  color: #e2e8f0;
  outline: none;
  transition: border-color .25s, box-shadow .25s;
}

.field:focus {
  border-color: rgba(6,182,212,.6);
  box-shadow: 0 0 0 3px rgba(6,182,212,.12);
}

.field::placeholder {
  color: #475569;
}

.flab {
  font-family: 'JetBrains Mono', monospace;
  font-size: .6rem;
  letter-spacing: .16em;
  color: #7d8ca3;
  display: block;
  margin-bottom: .35rem;
}

.chip {
  display: inline-flex;
  align-items: center;
  gap: .35rem;
  font-family: 'JetBrains Mono', monospace;
  font-size: .6rem;
  letter-spacing: .12em;
  padding: .28rem .6rem;
  border-radius: 9999px;
  border: 1px solid rgba(148,163,184,.18);
  color: #94a3b8;
  background: rgba(15,23,42,.6);
  white-space: nowrap;
}

.chip-on {
  border-color: rgba(16,185,129,.45);
  color: #34d399;
  background: rgba(16,185,129,.08);
}

.chip-live {
  border-color: rgba(6,182,212,.5);
  color: #22d3ee;
  background: rgba(6,182,212,.1);
}

.chip-err {
  border-color: rgba(244,63,94,.45);
  color: #fb7185;
  background: rgba(244,63,94,.08);
}

.chip-off {
  border-color: rgba(148,163,184,.2);
  color: #64748b;
}

.dot {
  width: 6px;
  height: 6px;
  border-radius: 9999px;
  background: currentColor;
}

.btn {
  transition: all .2s ease;
}

.btn:active {
  transform: scale(.97);
}

.logline {
  display: grid;
  grid-template-columns: 74px 62px 1fr;
  gap: .5rem;
  padding: .1rem 0;
  font-family: 'JetBrains Mono', monospace;
  font-size: .7rem;
  line-height: 1.5;
  word-break: break-word;
}

::-webkit-scrollbar {
  width: 8px;
}

::-webkit-scrollbar-track {
  background: #0a0f1a;
}

::-webkit-scrollbar-thumb {
  background: #164e63;
  border-radius: 8px;
}

.toast {
  padding: .65rem .9rem;
  border-radius: .7rem;
  font-size: .78rem;
  border: 1px solid;
  margin-bottom: .5rem;
  opacity: 0;
  transform: translateY(8px);
  transition: all .3s ease;
}

.toast.show {
  opacity: 1;
  transform: none;
}

.toast-ok {
  border-color: rgba(16,185,129,.4);
  background: rgba(16,185,129,.1);
  color: #6ee7b7;
}

.toast-err {
  border-color: rgba(244,63,94,.4);
  background: rgba(244,63,94,.1);
  color: #fda4af;
}

.toast-warn {
  border-color: rgba(245,158,11,.4);
  background: rgba(245,158,11,.1);
  color: #fcd34d;
}
</style>
</head>

<body class="antialiased">

<header class="relative border-b border-slate-800/70
               bg-slate-950/70 backdrop-blur">

<div class="max-w-6xl mx-auto px-5 h-16
            flex items-center justify-between">

<div class="flex items-center gap-3">

<span class="w-9 h-9 rounded-xl
             bg-gradient-to-br from-cyan-500 to-emerald-500
             flex items-center justify-center">

<i data-lucide="zap"
   class="w-5 h-5 text-slate-950"></i>

</span>

<div>

<span class="font-bold">
TG<span class="text-cyan-400">RELAY</span>
</span>

<span class="mono text-[9px] text-slate-500
             tracking-[.25em] block">
QUART + TELETHON LIVE DECK
</span>

</div>
</div>

<div class="flex items-center gap-2">

<span id="chip-backend"
      class="chip chip-err">
<span class="dot"></span>OFFLINE
</span>

<span id="chip-account"
      class="chip chip-off">
<span class="dot"></span>NO ACCOUNT
</span>

</div>
</div>
</header>


<main class="relative max-w-6xl mx-auto px-5 py-8
             grid lg:grid-cols-5 gap-6">

<div class="lg:col-span-2 space-y-6">

<section class="glass rounded-2xl p-5">

<h2 class="font-bold flex items-center gap-2 mb-4">
<i data-lucide="key-round"
   class="w-4 h-4 text-cyan-400"></i>
Account Session
</h2>

<div class="grid grid-cols-2 gap-3">

<div>
<label class="flab">API_ID</label>
<input id="in-api-id"
       class="field"
       placeholder="204xxxxx"
       inputmode="numeric">
</div>

<div>
<label class="flab">API_HASH</label>
<input id="in-api-hash"
       type="password"
       class="field"
       placeholder="0123abc…">
</div>

</div>

<div class="mt-3">
<label class="flab">PHONE_NUMBER</label>
<input id="in-phone"
       class="field"
       placeholder="+1 555 000 1122">
</div>

<button id="btn-otp"
        class="btn mt-4 w-full py-2.5 rounded-xl
               bg-cyan-500 text-slate-950 font-bold
               text-sm">

<i data-lucide="send"
   class="w-4 h-4"></i>

<span data-label>Request OTP</span>

</button>


<div id="otp-block"
     class="hidden mt-5 pt-4
            border-t border-dashed border-slate-700/70">

<div class="mono text-[9px]
            tracking-[.2em]
            text-emerald-300 mb-3">

OTP BLOCK — CHECK TELEGRAM

</div>

<div class="grid grid-cols-2 gap-3">

<div>
<label class="flab">OTP_CODE</label>
<input id="in-otp"
       class="field"
       placeholder="12345"
       autocomplete="one-time-code">
</div>

<div>
<label class="flab">2FA</label>
<input id="in-2fa"
       type="password"
       class="field"
       placeholder="cloud password">
</div>

</div>

<button id="btn-verify"
        class="btn mt-4 w-full py-2.5
               rounded-xl bg-emerald-500
               text-slate-950 font-bold text-sm">

<i data-lucide="shield-check"
   class="w-4 h-4"></i>

<span data-label>
Verify &amp; Connect
</span>

</button>

<div id="session-wrap"
     class="hidden mt-3">

<label class="flab">
STRING SESSION — SECRET
</label>

<div class="relative">

<textarea id="out-session"
          readonly
          class="field h-20
                 !text-[10px]
                 resize-none"></textarea>

<button id="btn-copy-session"
        class="absolute top-2 right-2">

<i data-lucide="copy"
   class="w-4 h-4"></i>

</button>

</div>
</div>

</div>
</section>


<section class="glass rounded-2xl p-5">

<h2 class="font-bold flex items-center gap-2 mb-4">

<i data-lucide="fingerprint"
   class="w-4 h-4 text-cyan-400"></i>

Session ID Login

</h2>

<div class="grid grid-cols-2 gap-3">

<div>
<label class="flab">API_ID</label>
<input id="s-api-id"
       class="field"
       placeholder="204xxxxx"
       inputmode="numeric">
</div>

<div>
<label class="flab">API_HASH</label>
<input id="s-api-hash"
       type="password"
       class="field"
       placeholder="0123abc…">
</div>

</div>

<div class="mt-3">

<label class="flab">
TELEGRAM STRING SESSION
</label>

<textarea id="s-session"
          class="field h-16 !text-[10px]
                 resize-none leading-relaxed"
          placeholder="Paste StringSession.save() output">
</textarea>

</div>

<button id="btn-session"
        class="btn mt-4 w-full py-2.5
               rounded-xl bg-emerald-500
               text-slate-950 font-bold text-sm">

<i data-lucide="log-in"
   class="w-4 h-4"></i>

<span data-label>
Login With Session
</span>

</button>

<p class="mono text-[9px] text-slate-600 mt-2">
Session blob is treated as a secret and is never logged.
</p>

</section>


<section class="glass rounded-2xl p-5">

<div class="flex items-center
            justify-between mb-4">

<h2 class="font-bold flex items-center gap-2">

<i data-lucide="repeat-2"
   class="w-4 h-4 text-emerald-400"></i>

Channel Forwarder

</h2>

<span id="chip-relay"
      class="chip chip-off">

<span class="dot"></span>IDLE

</span>

</div>


<label class="flab">
SOURCE_CHANNEL_ID
</label>

<input id="in-source"
       class="field"
       placeholder="-100… / @user / t.me/c/…">


<div class="mt-3">

<label class="flab">
TARGET_CHANNELS
</label>

<input id="in-targets"
       class="field"
       placeholder="-100…, @mirror1">

</div>


<div class="mt-3">

<label class="flab">
CUSTOM_BRANDING_TEXT
</label>

<input id="in-branding"
       class="field"
       placeholder="via @YourBrand">

</div>


<div class="mt-4 grid grid-cols-2 gap-3">

<button id="btn-start"
        class="btn py-2.5 rounded-xl
               bg-gradient-to-r
               from-cyan-500 to-emerald-500
               text-slate-950 font-bold text-sm">

<i data-lucide="play"
   class="w-4 h-4"></i>

<span data-label>
Start Forwarder
</span>

</button>


<button id="btn-stop"
        class="btn hidden py-2.5 rounded-xl
               border border-rose-500/50
               text-rose-300 font-bold text-sm">

<i data-lucide="square"
   class="w-4 h-4"></i>

<span data-label>
Stop
</span>

</button>

</div>


<p id="meta-relay"
   class="mono text-[9px]
          text-slate-600 mt-3 truncate">
no binding yet
</p>

<p class="mono text-[9px] text-slate-600 mt-1">
<span id="relay-count">0</span> relayed
</p>

</section>

</div>


<section class="lg:col-span-3
                glass rounded-2xl overflow-hidden
                flex flex-col">

<div class="flex items-center gap-2
            px-4 py-3
            border-b border-slate-800/70">

<span class="w-2.5 h-2.5 rounded-full
             bg-rose-500/70"></span>

<span class="w-2.5 h-2.5 rounded-full
             bg-amber-500/70"></span>

<span class="w-2.5 h-2.5 rounded-full
             bg-emerald-500/70"></span>

<span class="mono text-[10px]
             text-slate-400 ml-2">
relay://live-logs
</span>

</div>


<div id="console-body"
     class="flex-1 min-h-[560px]
            max-h-[640px]
            overflow-y-auto px-4 py-3">

<div class="logline">

<span class="text-slate-600">
--:--:--
</span>

<span class="text-slate-500 font-semibold">
SYSTEM
</span>

<span class="text-slate-500">
Console attached.
</span>

</div>

</div>

</section>

</main>


<div id="toasts"
     class="fixed bottom-4 right-4
            z-50 w-80"></div>


<script>

(function () {

var state = {
  phone: "",
  since: 0
};

function $(id) {
  return document.getElementById(id);
}

function val(id) {
  var el = $(id);
  return el ? el.value.trim() : "";
}

function toast(msg, kind) {

  var div = document.createElement("div");

  div.className =
    "toast " +
    (
      kind === "error"
      ? "toast-err"
      : kind === "warn"
      ? "toast-warn"
      : "toast-ok"
    );

  div.textContent = msg;

  $("toasts").appendChild(div);

  setTimeout(function () {
    div.classList.add("show");
  }, 10);

  setTimeout(function () {
    div.classList.remove("show");

    setTimeout(function () {
      div.remove();
    }, 300);

  }, 4600);
}


async function post(path, payload) {

  var res = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  var data;

  try {
    data = await res.json();
  }

  catch (e) {
    data = {
      ok: false,
      error: "Non-JSON response (" + res.status + ")"
    };
  }

  return {
    status: res.status,
    data: data
  };
}


async function requestOtp() {

  var apiId =
    parseInt(val("in-api-id"), 10);

  var payload = {
    api_id: apiId,
    api_hash: val("in-api-hash"),
    phone: val("in-phone")
  };

  if (
    !apiId ||
    !payload.api_hash ||
    !payload.phone
  ) {
    toast(
      "Fill API_ID, API_HASH and phone.",
      "warn"
    );
    return;
  }

  try {

    var r = await post(
      "/api/send-otp",
      payload
    );

    if (r.data.already_connected) {

      markConnected(r.data);

      toast(
        "Already authorized.",
        "ok"
      );

      return;
    }

    if (!r.data.ok) {

      toast(
        r.data.error ||
        "OTP failed (" + r.status + ")",
        "error"
      );

      return;
    }

    state.phone =
      payload.phone;

    $("otp-block")
      .classList
      .remove("hidden");

    toast(
      "OTP dispatched via Telegram.",
      "ok"
    );

  }

  catch (e) {

    toast(
      "Network error: " + e.message,
      "error"
    );

  }
}


async function verifyOtp() {

  var payload = {
    phone:
      val("in-phone") ||
      state.phone,

    otp_code:
      val("in-otp"),

    password:
      val("in-2fa")
  };

  if (
    !payload.phone ||
    !payload.otp_code
  ) {

    toast(
      "Phone and OTP code required.",
      "warn"
    );

    return;
  }

  try {

    var r = await post(
      "/api/verify-otp",
      payload
    );

    if (r.data.need_password) {

      toast(
        r.data.message ||
        "2FA password required.",
        "warn"
      );

      $("in-2fa").focus();

      return;
    }

    if (!r.data.ok) {

      toast(
        r.data.error ||
        "Verify failed.",
        "error"
      );

      return;
    }

    markConnected(
      r.data,
      true
    );

    toast(
      "Telegram account connected.",
      "ok"
    );

  }

  catch (e) {

    toast(
      "Network error: " + e.message,
      "error"
    );

  }
}


async function sessionLogin() {

  var payload = {

    api_id:
      parseInt(
        val("s-api-id") ||
        val("in-api-id"),
        10
      ),

    api_hash:
      val("s-api-hash") ||
      val("in-api-hash"),

    session_string:
      val("s-session")
  };

  if (
    !payload.api_id ||
    !payload.api_hash ||
    !payload.session_string
  ) {

    toast(
      "API_ID, API_HASH and String Session are required.",
      "warn"
    );

    return;
  }

  try {

    var r = await post(
      "/api/login-session",
      payload
    );

    if (!r.data.ok) {

      toast(
        r.data.error ||
        "Session login failed.",
        "error"
      );

      return;
    }

    $("s-session").value = "";

    markConnected(
      r.data
    );

    toast(
      "Session verified.",
      "ok"
    );

  }

  catch (e) {

    toast(
      "Network error: " + e.message,
      "error"
    );

  }
}


function markConnected(
  data,
  withSession
) {

  if (
    data.account &&
    data.account.phone
  ) {
    state.phone =
      data.account.phone;
  }

  var chip =
    $("chip-account");

  chip.textContent =
    "@" +
    (
      data.account.username ||
      data.account.id
    );

  chip.className =
    "chip chip-on";

  if (
    withSession &&
    data.string_session
  ) {

    $("session-wrap")
      .classList
      .remove("hidden");

    $("out-session").value =
      data.string_session;
  }
}


function copySession() {

  var el =
    $("out-session");

  navigator.clipboard
    .writeText(el.value)
    .then(function () {

      toast(
        "String session copied. Keep it secret.",
        "ok"
      );

    });

}


async function startForwarder() {

  var payload = {

    phone:
      state.phone ||
      val("in-phone"),

    source_channel_id:
      val("in-source"),

    target_channels:
      val("in-targets"),

    custom_branding_text:
      val("in-branding")
  };

  if (
    !payload.source_channel_id ||
    !payload.target_channels
  ) {

    toast(
      "Source and targets required.",
      "warn"
    );

    return;
  }

  try {

    var r = await post(
      "/api/start-forwarder",
      payload
    );

    if (!r.data.ok) {

      toast(
        r.data.error ||
        "Start failed.",
        "error"
      );

      return;
    }

    $("chip-relay").textContent =
      "RELAY LIVE";

    $("chip-relay").className =
      "chip chip-live";

    $("btn-start")
      .classList
      .add("hidden");

    $("btn-stop")
      .classList
      .remove("hidden");

    $("meta-relay").textContent =
      r.data.source.title +
      " → " +
      r.data.targets
        .map(function (t) {
          return t.title;
        })
        .join(", ");

    toast(
      "Forwarder started.",
      "ok"
    );

  }

  catch (e) {

    toast(
      "Network error: " + e.message,
      "error"
    );

  }
}


async function stopForwarder() {

  try {

    var r = await post(
      "/api/stop-forwarder",
      {
        phone:
          state.phone ||
          val("in-phone")
      }
    );

    if (!r.data.ok) {

      toast(
        r.data.error ||
        "Stop failed.",
        "error"
      );

      return;
    }

    $("chip-relay").textContent =
      "IDLE";

    $("chip-relay").className =
      "chip chip-off";

    $("btn-start")
      .classList
      .remove("hidden");

    $("btn-stop")
      .classList
      .add("hidden");

    toast(
      "Forwarder stopped.",
      "ok"
    );

  }

  catch (e) {

    toast(
      "Network error: " + e.message,
      "error"
    );

  }
}


function appendLog(row) {

  var host =
    $("console-body");

  var div =
    document.createElement("div");

  div.className =
    "logline";

  var ts =
    String(row.ts || "")
      .split("T")[1] || "";

  ts =
    ts
      .replace("Z", "")
      .split(".")[0]
      .split("+")[0];

  div.innerHTML =
    "<span class='text-slate-600'>" +
    ts +
    "</span>" +

    "<span class='text-cyan-300 font-semibold'>" +
    String(row.level || "INFO") +
    "</span>" +

    "<span class='text-slate-300'>" +
    String(row.message || "") +
    "</span>";

  host.appendChild(div);

  while (
    host.children.length > 240
  ) {
    host.removeChild(
      host.firstChild
    );
  }

  host.scrollTop =
    host.scrollHeight;
}


async function pollLogs() {

  try {

    var res =
      await fetch(
        "/api/logs?since=" +
        state.since
      );

    var data =
      await res.json();

    if (data.logs) {

      for (
        var i = 0;
        i < data.logs.length;
        i++
      ) {
        appendLog(
          data.logs[i]
        );
      }
    }

    if (
      typeof data.latest ===
      "number"
    ) {
      state.since =
        data.latest;
    }

    $("chip-backend")
      .textContent =
      "ONLINE";

    $("chip-backend")
      .className =
      "chip chip-on";

    setTimeout(
      pollLogs,
      1500
    );

  }

  catch (e) {

    $("chip-backend")
      .textContent =
      "OFFLINE";

    $("chip-backend")
      .className =
      "chip chip-err";

    setTimeout(
      pollLogs,
      4000
    );
  }
}


async function pollStatus() {

  try {

    var res =
      await fetch(
        "/api/status"
      );

    var s =
      await res.json();

    if (
      s.accounts &&
      s.accounts.length
    ) {

      var c =
        $("chip-account");

      c.textContent =
        "@" +
        (
          s.accounts[0].username ||
          s.accounts[0].id
        );

      c.className =
        "chip chip-on";

      if (!state.phone) {
        state.phone =
          s.accounts[0].phone;
      }
    }

    if (
      s.forwarder &&
      s.forwarder.running
    ) {

      $("chip-relay")
        .textContent =
        "RELAY LIVE";

      $("chip-relay")
        .className =
        "chip chip-live";

      $("btn-start")
        .classList
        .add("hidden");

      $("btn-stop")
        .classList
        .remove("hidden");
    }

  }

  catch (e) {}

  setTimeout(
    pollStatus,
    5000
  );
}


document.addEventListener(
  "DOMContentLoaded",
  function () {

    $("btn-otp")
      .addEventListener(
        "click",
        requestOtp
      );

    $("btn-verify")
      .addEventListener(
        "click",
        verifyOtp
      );

    $("btn-session")
      .addEventListener(
        "click",
        sessionLogin
      );

    $("btn-start")
      .addEventListener(
        "click",
        startForwarder
      );

    $("btn-stop")
      .addEventListener(
        "click",
        stopForwarder
      );

    $("btn-copy-session")
      .addEventListener(
        "click",
        copySession
      );

    if (window.lucide) {
      lucide.createIcons();
    }

    pollLogs();
    pollStatus();

  }
);

})();

</script>

</body>
</html>
"""


@app.route("/", methods=["GET"])
async def dashboard():
    return await render_template_string(
        DASHBOARD_HTML
    )


# ----------------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                5000
            )
        )
    )
