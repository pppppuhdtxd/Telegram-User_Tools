# click_farm.py
# Click Farm backend for a private Telegram mini-game assistant.
# Runtime dependency in your environment: Telethon.

import asyncio
import json
import logging
import os
import random
import re
import threading
import time
import uuid
from contextlib import suppress
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "click_farm_data.json"
HTML_FILE = BASE_DIR / "dashboard.html"

HOST = os.getenv("CLICK_FARM_HOST", "127.0.0.1")
PORT = int(os.getenv("CLICK_FARM_PORT", "8321"))

MIN_INTERVAL_MINUTES = 1.0
RESPONSE_TIMEOUT_SECONDS = 90.0
SECOND_CLICK_DELAY_SECONDS = 60.0

MAX_LOGS = 1000
HTTP_TIMEOUT = 180
LOOP_WAIT_SECONDS = 10.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ClickFarm")
logging.getLogger("telethon").setLevel(logging.WARNING)

STATE_LOCK = threading.RLock()

LOOP = None
CLIENTS = {}
CLIENT_LOCKS = {}
TASKS = {}
RUN_INFO = {}
LOGIN_FLOWS = {}
ACCOUNT_FLOOD_UNTIL = {}


def utcnow():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def future_timestamp(seconds):
    try:
        seconds = max(0.0, float(seconds))
    except (TypeError, ValueError):
        seconds = 0.0
    return datetime.fromtimestamp(time.time() + seconds).strftime("%Y-%m-%d %H:%M:%S")


class State:
    def __init__(self):
        self.data = {"accounts": [], "configs": [], "settings": {}}
        self.logs = []
        self.load()

    def load(self):
        if DATA_FILE.exists():
            try:
                raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    self.data = raw
            except Exception as exc:
                log.warning("Could not load state file: %s", exc)

        if not isinstance(self.data.get("accounts"), list):
            self.data["accounts"] = []
        if not isinstance(self.data.get("configs"), list):
            self.data["configs"] = []
        if not isinstance(self.data.get("settings"), dict):
            self.data["settings"] = {}

    def save(self):
        with STATE_LOCK:
            try:
                tmp = DATA_FILE.with_suffix(".tmp")
                tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
                tmp.replace(DATA_FILE)
            except Exception as exc:
                log.error("Could not save state file: %s", exc)

    def accounts(self):
        return self.data["accounts"]

    def configs(self):
        return self.data["configs"]

    def settings(self):
        return self.data["settings"]

    def get_account(self, account_id):
        with STATE_LOCK:
            for item in self.data["accounts"]:
                if item.get("id") == account_id:
                    return item
            return None

    def get_config(self, config_id):
        with STATE_LOCK:
            for item in self.data["configs"]:
                if item.get("id") == config_id:
                    return item
            return None

    def delete_config(self, config_id):
        with STATE_LOCK:
            self.data["configs"] = [c for c in self.data["configs"] if c.get("id") != config_id]
            self.save()

    def add_log(self, level, message, account_id=None, config_id=None):
        entry = {
            "ts": utcnow(),
            "level": level,
            "message": message,
            "account_id": account_id,
            "config_id": config_id,
        }
        with STATE_LOCK:
            self.logs.insert(0, entry)
            if len(self.logs) > MAX_LOGS:
                self.logs = self.logs[:MAX_LOGS]
        getattr(log, str(level).lower(), log.info)(message)


state = State()


def clean_phone(value):
    value = str(value or "").strip()
    if not value:
        return ""
    value = re.sub(r"[^\d+]", "", value)
    if not value.startswith("+"):
        digits = re.sub(r"\D", "", value)
        value = "+" + digits
    return value


def is_task_running(config_id):
    task = TASKS.get(config_id)
    return bool(task and not task.done())


def public_account(account):
    if not account:
        return None
    return {
        "id": account.get("id"),
        "name": account.get("name"),
        "phone": account.get("phone"),
        "status": account.get("status", "unknown"),
        "error": account.get("error"),
        "api_id": account.get("api_id"),
        "has_session": bool(account.get("session")),
        "created_at": account.get("created_at"),
        "updated_at": account.get("updated_at"),
        "last_activity": account.get("last_activity"),
    }


def account_view(account):
    if not account:
        return None
    with STATE_LOCK:
        item = public_account(account)
        account_id = item.get("id")
        account_configs = [c for c in state.configs() if c.get("account_id") == account_id]
        item["configs_count"] = len(account_configs)
        item["running_count"] = sum(1 for c in account_configs if is_task_running(c.get("id")))
        return item


def config_view(cfg):
    if not cfg:
        return None
    with STATE_LOCK:
        item = dict(cfg)
        config_id = item.get("id")
        info = dict(RUN_INFO.get(config_id, {}))
        account = state.get_account(item.get("account_id"))

        item["account_name"] = account.get("name") if account else None
        item["task_running"] = is_task_running(config_id)

        if info.get("state"):
            item["task_state"] = info.get("state")
        elif item.get("task_running"):
            item["task_state"] = "running"
        elif item.get("running"):
            item["task_state"] = "starting"
        else:
            item["task_state"] = "stopped"

        item["next_run"] = info.get("next_run") or item.get("next_run")
        item["last_error"] = info.get("last_error")
        return item


def public_state():
    with STATE_LOCK:
        accounts = [account_view(a) for a in state.accounts()]
        configs = [config_view(c) for c in state.configs()]
        logs = list(state.logs)

    return {
        "ok": True,
        "accounts": accounts,
        "configs": configs,
        "logs": logs,
        "server_time": utcnow(),
    }


def normalize_config(cfg):
    cfg.setdefault("id", str(uuid.uuid4()))
    cfg.setdefault("running", False)
    cfg.setdefault("enabled", True)
    cfg.setdefault("created_at", utcnow())
    cfg.setdefault("updated_at", utcnow())
    cfg.setdefault("success_count", 0)
    cfg.setdefault("warning_count", 0)
    cfg.setdefault("error_count", 0)
    cfg.setdefault("last_status", None)
    cfg.setdefault("last_run", None)
    cfg.setdefault("next_run", None)

    for key in ["name", "account_id", "target_chat", "bot_id", "command", "action_keyword"]:
        cfg[key] = str(cfg.get(key) or "").strip()

    if not cfg.get("name"):
        cfg["name"] = "Game task"

    try:
        cfg["interval_minutes"] = float(cfg.get("interval_minutes", 30))
    except (TypeError, ValueError):
        cfg["interval_minutes"] = 30.0

    if cfg["interval_minutes"] < MIN_INTERVAL_MINUTES:
        cfg["interval_minutes"] = MIN_INTERVAL_MINUTES

    for field, default in (
        ("response_timeout_seconds", RESPONSE_TIMEOUT_SECONDS),
        ("second_click_delay_seconds", SECOND_CLICK_DELAY_SECONDS),
    ):
        try:
            cfg[field] = float(cfg.get(field, default))
        except (TypeError, ValueError):
            cfg[field] = default

    for field in ["success_count", "warning_count", "error_count"]:
        try:
            cfg[field] = int(cfg.get(field, 0))
        except (TypeError, ValueError):
            cfg[field] = 0

    cfg["enabled"] = bool(cfg.get("enabled", True))
    return cfg


def save_config_payload(payload, config_id=None):
    if not isinstance(payload, dict):
        raise ValueError("Invalid config payload")

    with STATE_LOCK:
        if config_id:
            cfg = state.get_config(config_id)
            if not cfg:
                raise ValueError("Config not found")
            candidate = dict(cfg)
            is_new = False
        else:
            candidate = {
                "id": str(uuid.uuid4()),
                "running": False,
                "created_at": utcnow(),
            }
            is_new = True

        for field in ["name", "account_id", "target_chat", "bot_id", "command", "action_keyword"]:
            if field in payload:
                candidate[field] = payload.get(field)

        if "interval_minutes" in payload:
            candidate["interval_minutes"] = payload.get("interval_minutes")

        for field in ["response_timeout_seconds", "second_click_delay_seconds", "enabled"]:
            if field in payload:
                candidate[field] = payload.get(field)

        candidate_account_id = str(candidate.get("account_id") or "").strip()
        if candidate_account_id and not state.get_account(candidate_account_id):
            raise ValueError("Account not found")

        candidate["updated_at"] = utcnow()
        normalize_config(candidate)

        if is_new:
            state.configs().append(candidate)
        else:
            cfg.clear()
            cfg.update(candidate)

        state.save()
        return dict(candidate)


def duplicate_config(config_id, account_id=None):
    with STATE_LOCK:
        src = state.get_config(config_id)
        if not src:
            raise ValueError("Config not found")

        target_account_id = str(account_id or src.get("account_id") or "").strip()
        if target_account_id and not state.get_account(target_account_id):
            raise ValueError("Account not found")

        new_cfg = dict(src)
        new_cfg["id"] = str(uuid.uuid4())
        new_cfg["name"] = f"{src.get('name') or 'Game task'} copy"
        new_cfg["running"] = False
        new_cfg["created_at"] = utcnow()
        new_cfg["updated_at"] = utcnow()
        new_cfg["success_count"] = 0
        new_cfg["warning_count"] = 0
        new_cfg["error_count"] = 0
        new_cfg["last_status"] = None
        new_cfg["last_run"] = None
        new_cfg["next_run"] = None

        if target_account_id:
            new_cfg["account_id"] = target_account_id

        normalize_config(new_cfg)
        state.configs().append(new_cfg)
        state.save()
        return dict(new_cfg)


def update_account_payload(account_id, payload):
    if not isinstance(payload, dict):
        raise ValueError("Invalid account payload")

    with STATE_LOCK:
        account = state.get_account(account_id)
        if not account:
            raise ValueError("Account not found")

        if "name" in payload:
            name = str(payload.get("name") or "").strip()
            account["name"] = name or account.get("phone")

        account["updated_at"] = utcnow()
        state.save()
        return dict(account)


def import_configs(payload):
    if isinstance(payload, list):
        items = payload
        target_account_id = None
    elif isinstance(payload, dict):
        items = payload.get("configs") or payload.get("items") or []
        target_account_id = payload.get("account_id")
    else:
        raise ValueError("Invalid import payload")

    if not isinstance(items, list):
        raise ValueError("Import payload must contain a list of configs")

    target_account_id = str(target_account_id or "").strip()
    if target_account_id and not state.get_account(target_account_id):
        raise ValueError("Target account not found")

    imported = []
    allowed_fields = [
        "name",
        "account_id",
        "target_chat",
        "bot_id",
        "command",
        "action_keyword",
        "interval_minutes",
        "response_timeout_seconds",
        "second_click_delay_seconds",
        "enabled",
    ]

    with STATE_LOCK:
        for item in items:
            if not isinstance(item, dict):
                continue

            cfg = {
                "id": str(uuid.uuid4()),
                "running": False,
                "created_at": utcnow(),
                "updated_at": utcnow(),
            }

            for field in allowed_fields:
                if field in item:
                    cfg[field] = item.get(field)

            if target_account_id:
                cfg["account_id"] = target_account_id

            normalize_config(cfg)
            cfg["running"] = False
            state.configs().append(cfg)
            imported.append(cfg)

        state.save()

    return imported


def export_configs():
    with STATE_LOCK:
        return [dict(c) for c in state.configs()]


def run_async(coro, timeout=HTTP_TIMEOUT):
    global LOOP

    deadline = time.time() + LOOP_WAIT_SECONDS
    while LOOP is None and time.time() < deadline:
        time.sleep(0.05)

    if LOOP is None:
        raise RuntimeError("Event loop is not ready")

    future = asyncio.run_coroutine_threadsafe(coro, LOOP)
    return future.result(timeout=timeout)


async def ensure_client(account):
    account_id = account.get("id")
    lock = CLIENT_LOCKS.setdefault(account_id, asyncio.Lock())

    async with lock:
        client = CLIENTS.get(account_id)

        if client is None:
            try:
                api_id = int(account.get("api_id") or 0)
            except (TypeError, ValueError):
                api_id = 0

            api_hash = str(account.get("api_hash") or "").strip()

            if not api_id or not api_hash:
                raise ValueError("api_id and api_hash are required")

            session = StringSession(account.get("session") or "")
            client = TelegramClient(session, api_id, api_hash)
            CLIENTS[account_id] = client

        if not client.is_connected():
            await client.connect()

        return client


def set_account_cooldown(account_id, seconds, reason="FloodWait"):
    if not account_id:
        return
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = 60
    ACCOUNT_FLOOD_UNTIL[account_id] = time.monotonic() + max(1, seconds)
    state.add_log("warning", f"{reason}: pausing account tasks for {seconds}s", account_id=account_id)


async def wait_account_cooldown(account_id):
    if not account_id:
        return
    until = ACCOUNT_FLOOD_UNTIL.get(account_id)
    if not until:
        return

    now = time.monotonic()
    if until > now:
        await asyncio.sleep(until - now + random.uniform(0.3, 1.5))

    if time.monotonic() >= ACCOUNT_FLOOD_UNTIL.get(account_id, 0):
        ACCOUNT_FLOOD_UNTIL.pop(account_id, None)


async def startup_async():
    state.add_log("info", "Backend starting")

    for account in list(state.accounts()):
        if account.get("session"):
            try:
                client = await ensure_client(account)
                if await client.is_user_authorized():
                    account["status"] = "authorized"
                    account["error"] = None
                    account["session"] = client.session.save()
                    account["last_activity"] = utcnow()
                else:
                    account["status"] = "disconnected"
                    account["error"] = None
            except Exception as exc:
                account["status"] = "error"
                account["error"] = str(exc)
        else:
            account["status"] = "disconnected"
            account["error"] = None

    with STATE_LOCK:
        for cfg in state.configs():
            cfg["running"] = False
            cfg["next_run"] = None
        state.save()

    TASKS.clear()
    RUN_INFO.clear()

    state.add_log("info", "Click Farm backend ready. Add accounts, create tasks, then start them.")


async def add_account_async(payload):
    if not isinstance(payload, dict):
        raise ValueError("Invalid account payload")

    name = str(payload.get("name") or "").strip()
    phone = clean_phone(payload.get("phone"))
    api_hash = str(payload.get("api_hash") or "").strip()

    try:
        api_id = int(str(payload.get("api_id") or 0))
    except (TypeError, ValueError):
        raise ValueError("api_id must be numeric")

    if not phone:
        raise ValueError("phone is required")

    if not api_id or not api_hash:
        raise ValueError("api_id and api_hash are required")

    account = {
        "id": str(uuid.uuid4()),
        "name": name or phone,
        "phone": phone,
        "api_id": api_id,
        "api_hash": api_hash,
        "session": "",
        "status": "connecting",
        "error": None,
        "created_at": utcnow(),
        "updated_at": utcnow(),
        "last_activity": None,
    }

    with STATE_LOCK:
        state.accounts().append(account)
        state.save()

    try:
        client = await ensure_client(account)

        if await client.is_user_authorized():
            account["status"] = "authorized"
            account["session"] = client.session.save()
            account["error"] = None
            account["last_activity"] = utcnow()
            account["updated_at"] = utcnow()
            state.save()
            state.add_log("info", f"Account '{account['name']}' is already authorized", account_id=account["id"])
            return account

        sent = await client.send_code_request(phone)
        LOGIN_FLOWS[account["id"]] = {
            "phone": phone,
            "phone_code_hash": sent.phone_code_hash,
        }

        account["status"] = "awaiting_code"
        account["error"] = None
        account["updated_at"] = utcnow()
        state.save()
        state.add_log("info", f"Login code request sent for '{account['name']}'", account_id=account["id"])
        return account

    except errors.FloodWaitError as exc:
        account["status"] = "error"
        account["error"] = f"Flood wait: retry in {exc.seconds}s"
        account["updated_at"] = utcnow()
        state.save()
        raise

    except Exception as exc:
        account["status"] = "error"
        account["error"] = str(exc)
        account["updated_at"] = utcnow()
        state.save()
        raise


async def import_account_async(payload):
    """
    Register an account from an already-existing Telethon StringSession —
    no phone/code/2FA step at all. Verifies the session is actually
    authorized before saving it, and auto-fills phone/name from get_me().
    """
    if not isinstance(payload, dict):
        raise ValueError("Invalid account payload")

    name = str(payload.get("name") or "").strip()
    api_hash = str(payload.get("api_hash") or "").strip()
    session_string = str(payload.get("session_string") or "").strip()

    try:
        api_id = int(str(payload.get("api_id") or 0))
    except (TypeError, ValueError):
        raise ValueError("api_id must be numeric")

    if not api_id or not api_hash:
        raise ValueError("api_id and api_hash are required")
    if not session_string:
        raise ValueError("session_string is required")

    session = StringSession(session_string)
    client = TelegramClient(session, api_id, api_hash)

    try:
        await client.connect()
    except ValueError:
        raise ValueError("Invalid string session")

    if not await client.is_user_authorized():
        with suppress(Exception):
            await client.disconnect()
        raise ValueError("This string session is not authorized (expired or logged out)")

    phone = ""
    with suppress(Exception):
        me = await client.get_me()
        if me and me.phone:
            phone = "+" + str(me.phone)

    account = {
        "id": str(uuid.uuid4()),
        "name": name or phone or "Imported account",
        "phone": phone,
        "api_id": api_id,
        "api_hash": api_hash,
        "session": client.session.save(),
        "status": "authorized",
        "error": None,
        "created_at": utcnow(),
        "updated_at": utcnow(),
        "last_activity": utcnow(),
    }

    with STATE_LOCK:
        state.accounts().append(account)
        state.save()

    CLIENTS[account["id"]] = client
    state.add_log("info", f"Account '{account['name']}' imported via string session", account_id=account["id"])
    return account


async def verify_code_async(account_id, code):
    account = state.get_account(account_id)
    if not account:
        raise ValueError("Account not found")

    client = await ensure_client(account)
    flow = LOGIN_FLOWS.get(account_id)

    if not flow:
        if await client.is_user_authorized():
            account["status"] = "authorized"
            account["session"] = client.session.save()
            account["error"] = None
            account["last_activity"] = utcnow()
            account["updated_at"] = utcnow()
            state.save()
            return account
        raise ValueError("No active login flow for this account")

    try:
        await client.sign_in(phone=flow["phone"], code=code, phone_code_hash=flow["phone_code_hash"])
    except errors.SessionPasswordNeededError:
        account["status"] = "awaiting_password"
        account["error"] = "Two-step verification password required"
        account["updated_at"] = utcnow()
        state.save()
        return account
    except errors.FloodWaitError as exc:
        account["error"] = f"Flood wait: {exc.seconds}s"
        account["updated_at"] = utcnow()
        state.save()
        raise

    account["status"] = "authorized"
    account["session"] = client.session.save()
    account["error"] = None
    account["last_activity"] = utcnow()
    account["updated_at"] = utcnow()

    LOGIN_FLOWS.pop(account_id, None)
    state.save()
    state.add_log("info", f"Account '{account['name']}' authorized", account_id=account_id)
    return account


async def verify_password_async(account_id, password):
    account = state.get_account(account_id)
    if not account:
        raise ValueError("Account not found")

    client = await ensure_client(account)

    try:
        await client.sign_in(password=password)
    except errors.FloodWaitError as exc:
        account["error"] = f"Flood wait: {exc.seconds}s"
        account["updated_at"] = utcnow()
        state.save()
        raise

    account["status"] = "authorized"
    account["session"] = client.session.save()
    account["error"] = None
    account["last_activity"] = utcnow()
    account["updated_at"] = utcnow()

    LOGIN_FLOWS.pop(account_id, None)
    state.save()
    state.add_log("info", f"Account '{account['name']}' authorized with password", account_id=account_id)
    return account


async def refresh_account_async(account_id):
    account = state.get_account(account_id)
    if not account:
        raise ValueError("Account not found")

    try:
        client = await ensure_client(account)
        authorized = await client.is_user_authorized()

        account["status"] = "authorized" if authorized else "disconnected"
        account["error"] = None
        account["updated_at"] = utcnow()

        if authorized:
            account["session"] = client.session.save()
            account["last_activity"] = utcnow()

    except Exception as exc:
        account["status"] = "error"
        account["error"] = str(exc)
        account["updated_at"] = utcnow()

    state.save()
    return account


async def remove_account_async(account_id):
    account = state.get_account(account_id)
    if not account:
        return True

    account_configs = [c for c in state.configs() if c.get("account_id") == account_id]
    for cfg in account_configs:
        with suppress(Exception):
            await stop_config_async(cfg.get("id"))

    client = CLIENTS.pop(account_id, None)
    if client:
        with suppress(Exception):
            await client.disconnect()

    LOGIN_FLOWS.pop(account_id, None)
    CLIENT_LOCKS.pop(account_id, None)
    ACCOUNT_FLOOD_UNTIL.pop(account_id, None)

    with STATE_LOCK:
        state.data["configs"] = [c for c in state.configs() if c.get("account_id") != account_id]
        state.data["accounts"] = [a for a in state.accounts() if a.get("id") != account_id]
        state.save()

    state.add_log("info", f"Account '{account.get('name')}' removed", account_id=account_id)
    return True


def validate_config_minimal(cfg):
    if not cfg:
        raise ValueError("Config not found")

    if not cfg.get("account_id"):
        raise ValueError("Choose an account for this task")

    if not cfg.get("target_chat") or not cfg.get("command") or not cfg.get("action_keyword"):
        raise ValueError("Target chat, game command, and action keyword are required")


def _start_config_task(config_id):
    cfg_snapshot = None
    create_new_task = False

    with STATE_LOCK:
        cfg = state.get_config(config_id)
        if not cfg:
            return None

        existing = TASKS.get(config_id)
        if existing and not existing.done():
            cfg["running"] = True
            cfg["updated_at"] = utcnow()
            state.save()
            return cfg

        cfg["running"] = True
        cfg["updated_at"] = utcnow()
        state.save()

        RUN_INFO[config_id] = {
            "state": "starting",
            "started_at": utcnow(),
            "next_run": None,
            "last_error": None,
        }

        cfg_snapshot = dict(cfg)
        create_new_task = True

    # Create the asyncio task outside STATE_LOCK to avoid potential deadlock
    if create_new_task:
        task = asyncio.get_event_loop().create_task(config_loop(config_id))
        with STATE_LOCK:
            TASKS[config_id] = task

    state.add_log("info", f"Task started: {cfg_snapshot.get('name')}", account_id=cfg_snapshot.get("account_id"), config_id=config_id)
    return cfg_snapshot


async def start_config_async(config_id):
    with STATE_LOCK:
        cfg = state.get_config(config_id)

    validate_config_minimal(cfg)

    account = state.get_account(cfg.get("account_id"))
    if not account:
        raise ValueError("Account not found")

    try:
        client = await ensure_client(account)
        authorized = await client.is_user_authorized()

        if not authorized:
            with STATE_LOCK:
                account["status"] = "disconnected"
                account["error"] = "Not authorized"
                account["updated_at"] = utcnow()
                state.save()
            raise ValueError("Account is not authorized")

        with STATE_LOCK:
            account["status"] = "authorized"
            account["error"] = None
            account["last_activity"] = utcnow()
            account["updated_at"] = utcnow()
            state.save()

    except errors.FloodWaitError as exc:
        set_account_cooldown(account.get("id"), exc.seconds)
        with STATE_LOCK:
            account["status"] = "error"
            account["error"] = f"Flood wait: {exc.seconds}s"
            account["updated_at"] = utcnow()
            state.save()
        raise

    except Exception as exc:
        with STATE_LOCK:
            account["status"] = "error"
            account["error"] = str(exc)
            account["updated_at"] = utcnow()
            state.save()
        raise

    return _start_config_task(config_id)


async def stop_config_async(config_id):
    with STATE_LOCK:
        cfg = state.get_config(config_id)
        if cfg:
            cfg["running"] = False
            cfg["next_run"] = None
            cfg["updated_at"] = utcnow()
            state.save()

        info = RUN_INFO.get(config_id, {})
        RUN_INFO[config_id] = {
            "state": "stopping",
            "last_error": info.get("last_error"),
            "next_run": None,
        }

    task = TASKS.get(config_id)
    if task and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task

    TASKS.pop(config_id, None)

    with STATE_LOCK:
        if cfg:
            cfg["running"] = False
            cfg["next_run"] = None
            cfg["updated_at"] = utcnow()
            state.save()

        old_info = RUN_INFO.get(config_id, {})
        RUN_INFO[config_id] = {
            "state": "stopped",
            "stopped_at": utcnow(),
            "last_error": old_info.get("last_error"),
            "next_run": None,
        }

        name = cfg.get("name") if cfg else config_id
        account_id = cfg.get("account_id") if cfg else None

    state.add_log("info", f"Task stopped: {name}", account_id=account_id, config_id=config_id)
    return cfg


async def delete_config_async(config_id):
    await stop_config_async(config_id)
    state.delete_config(config_id)
    return True


async def start_all_async(account_id=None):
    account_id = str(account_id or "").strip() or None

    with STATE_LOCK:
        configs = list(state.configs())

    started = 0
    skipped = 0

    for cfg in configs:
        if account_id and cfg.get("account_id") != account_id:
            continue

        if cfg.get("enabled") is False:
            skipped += 1
            continue

        try:
            validate_config_minimal(cfg)
            if not state.get_account(cfg.get("account_id")):
                raise ValueError("Account not found")

            result = _start_config_task(cfg.get("id"))
            if result:
                started += 1
            else:
                skipped += 1

        except Exception as exc:
            skipped += 1
            state.add_log(
                "warning",
                f"Could not start task '{cfg.get('name') or cfg.get('id')}': {exc}",
                account_id=cfg.get("account_id"),
                config_id=cfg.get("id"),
            )

    return {"started": started, "skipped": skipped}


async def stop_all_async(account_id=None):
    account_id = str(account_id or "").strip() or None

    with STATE_LOCK:
        config_ids = [
            c.get("id")
            for c in state.configs()
            if not account_id or c.get("account_id") == account_id
        ]

    if not config_ids:
        return {"stopped": 0, "total": 0}

    results = await asyncio.gather(*(stop_config_async(cid) for cid in config_ids), return_exceptions=True)
    stopped = sum(1 for item in results if not isinstance(item, Exception))
    return {"stopped": stopped, "total": len(config_ids)}


async def config_loop(config_id):
    try:
        while True:
            with STATE_LOCK:
                cfg = state.get_config(config_id)
                if not cfg or not cfg.get("running"):
                    break

                cfg_snapshot = dict(cfg)
                account_id = cfg_snapshot.get("account_id")

                try:
                    interval = max(float(cfg_snapshot.get("interval_minutes", 30)), MIN_INTERVAL_MINUTES) * 60.0
                except (TypeError, ValueError):
                    interval = 30.0 * 60.0

            sleep_seconds = max(60.0, interval * 0.25)

            with STATE_LOCK:
                RUN_INFO.setdefault(config_id, {}).update({
                    "state": "running",
                    "last_error": None,
                })
                live_cfg = state.get_config(config_id)
                if live_cfg:
                    live_cfg["last_status"] = "running"
                    state.save()

            try:
                if not account_id:
                    raise ValueError("Account missing")

                await wait_account_cooldown(account_id)
                result = await execute_config(cfg_snapshot)

                status = str(result.get("status") or "success")
                message = result.get("message")

                with STATE_LOCK:
                    live_cfg = state.get_config(config_id)
                    if live_cfg:
                        live_cfg["last_run"] = utcnow()
                        live_cfg["last_status"] = status

                        if status == "success":
                            live_cfg["success_count"] = int(live_cfg.get("success_count", 0)) + 1
                        elif status == "warning":
                            live_cfg["warning_count"] = int(live_cfg.get("warning_count", 0)) + 1
                        else:
                            live_cfg["error_count"] = int(live_cfg.get("error_count", 0)) + 1

                        state.save()

                    RUN_INFO.setdefault(config_id, {}).update({
                        "state": "idle",
                        "last_error": None if status == "success" else message,
                    })

                sleep_seconds = interval + random.uniform(5, max(15, interval * 0.05))

            except asyncio.CancelledError:
                raise

            except errors.FloodWaitError as exc:
                set_account_cooldown(account_id, exc.seconds)

                with STATE_LOCK:
                    live_cfg = state.get_config(config_id)
                    if live_cfg:
                        live_cfg["last_run"] = utcnow()
                        live_cfg["last_status"] = "flood_wait"
                        live_cfg["error_count"] = int(live_cfg.get("error_count", 0)) + 1
                        state.save()

                    RUN_INFO.setdefault(config_id, {}).update({
                        "state": "flood_wait",
                        "last_error": f"FloodWait {exc.seconds}s",
                    })

                sleep_seconds = float(exc.seconds) + random.uniform(10, 30)

            except Exception as exc:
                with STATE_LOCK:
                    live_cfg = state.get_config(config_id)
                    if live_cfg:
                        live_cfg["last_run"] = utcnow()
                        live_cfg["last_status"] = "error"
                        live_cfg["error_count"] = int(live_cfg.get("error_count", 0)) + 1
                        state.save()

                    RUN_INFO.setdefault(config_id, {}).update({
                        "state": "error",
                        "last_error": str(exc),
                    })

                state.add_log("error", f"Task error: {exc}", account_id=account_id, config_id=config_id)
                sleep_seconds = max(60.0, interval * 0.25)

            with STATE_LOCK:
                next_run = future_timestamp(sleep_seconds)

                live_cfg = state.get_config(config_id)
                if live_cfg:
                    live_cfg["next_run"] = next_run
                    state.save()

                RUN_INFO.setdefault(config_id, {})["next_run"] = next_run

            await asyncio.sleep(sleep_seconds)

    except asyncio.CancelledError:
        pass

    finally:
        with STATE_LOCK:
            cfg = state.get_config(config_id)
            if cfg:
                cfg["running"] = False
                cfg["next_run"] = None
                state.save()

            old_info = RUN_INFO.get(config_id, {})
            RUN_INFO[config_id] = {
                "state": "stopped",
                "stopped_at": utcnow(),
                "last_error": old_info.get("last_error"),
                "next_run": None,
            }

        TASKS.pop(config_id, None)


async def resolve_entity(client, value, field_name="entity"):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field_name} is empty")

    if re.fullmatch(r"-?\d+", raw):
        try:
            return await client.get_entity(int(raw))
        except Exception:
            pass

    try:
        return await client.get_entity(raw)
    except Exception as exc:
        raise ValueError(
            f"Could not resolve {field_name} '{raw}'. "
            f"Use a @username or an ID already known to this account. {exc}"
        )


async def wait_for_bot_response(client, target_entity, bot_id, last_message_id, timeout_seconds=None):
    try:
        timeout_seconds = float(timeout_seconds or RESPONSE_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        timeout_seconds = RESPONSE_TIMEOUT_SECONDS

    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        try:
            async for message in client.iter_messages(target_entity, limit=50, min_id=last_message_id):
                if bot_id and getattr(message, "sender_id", None) != bot_id:
                    continue
                if getattr(message, "reply_markup", None):
                    return message
        except errors.FloodWaitError as exc:
            await asyncio.sleep(exc.seconds + random.uniform(1, 3))
            continue
        except Exception:
            pass

        await asyncio.sleep(random.uniform(2.0, 4.0))

    return None


def find_button(message, keyword):
    keyword = str(keyword or "").strip().lower()
    if not keyword:
        return None

    markup = getattr(message, "reply_markup", None)
    rows = getattr(markup, "rows", None) or []

    for row_index, row in enumerate(rows):
        buttons = getattr(row, "buttons", None) or []
        for col_index, button in enumerate(buttons):
            text = str(getattr(button, "text", "") or "")
            if keyword in text.lower():
                return row_index, col_index, button

    return None


async def send_callback_answer(client, peer, msg_id, data, game):
    kwargs = {"peer": peer, "msg_id": msg_id, "data": data}
    if game:
        kwargs["game"] = True

    try:
        await client(GetBotCallbackAnswerRequest(**kwargs))
    except TypeError:
        kwargs.pop("game", None)
        await client(GetBotCallbackAnswerRequest(**kwargs))


async def click_button(client, target_entity, message, row_index, col_index, button):
    button_text = getattr(button, "text", "") or "button"

    try:
        await message.click(row_index, col_index)
        return True
    except errors.FloodWaitError:
        raise
    except Exception as exc:
        state.add_log("warning", f"message.click failed for '{button_text}': {exc}; trying raw callback")

    data = getattr(button, "data", None)
    if isinstance(data, str):
        data = data.encode()

    button_type = type(button).__name__.lower()
    is_game = button_type.endswith("game")

    if data is None and not is_game:
        return False

    try:
        peer = await client.get_input_entity(target_entity)
        await send_callback_answer(client, peer, message.id, data, is_game)
        return True
    except errors.FloodWaitError:
        raise
    except Exception as exc:
        state.add_log("error", f"Raw callback failed: {exc}")

    return False


async def execute_config(cfg):
    config_id = cfg.get("id")
    account_id = cfg.get("account_id")
    task_name = cfg.get("name") or "Game task"

    account = state.get_account(account_id)
    if not account:
        raise ValueError("Account missing")

    try:
        client = await ensure_client(account)
        authorized = await client.is_user_authorized()
    except errors.FloodWaitError:
        raise
    except Exception as exc:
        with STATE_LOCK:
            live_account = state.get_account(account_id)
            if live_account:
                live_account["status"] = "error"
                live_account["error"] = str(exc)
                live_account["updated_at"] = utcnow()
                state.save()
        raise

    if not authorized:
        with STATE_LOCK:
            live_account = state.get_account(account_id)
            if live_account:
                live_account["status"] = "disconnected"
                live_account["error"] = "Not authorized"
                live_account["updated_at"] = utcnow()
                state.save()
        raise ValueError("Account is not authorized")

    with STATE_LOCK:
        live_account = state.get_account(account_id)
        if live_account:
            live_account["status"] = "authorized"
            live_account["error"] = None
            live_account["last_activity"] = utcnow()
            live_account["updated_at"] = utcnow()
            state.save()

    await wait_account_cooldown(account_id)
    state.add_log("info", f"[{task_name}] Starting cycle", account_id=account_id, config_id=config_id)

    try:
        target_entity = await resolve_entity(client, cfg.get("target_chat"), "target chat")

        bot_entity = None
        if cfg.get("bot_id"):
            bot_entity = await resolve_entity(client, cfg.get("bot_id"), "game bot")

        bot_id = bot_entity.id if bot_entity else None

        await asyncio.sleep(random.uniform(0.7, 2.2))

        history = await client.get_messages(target_entity, limit=1)
        last_message_id = history[0].id if history else 0

        await asyncio.sleep(random.uniform(0.4, 1.6))

        await client.send_message(target_entity, cfg.get("command"))
        state.add_log("info", f"[{task_name}] Sent game command", account_id=account_id, config_id=config_id)

        message = await wait_for_bot_response(
            client,
            target_entity,
            bot_id,
            last_message_id,
            cfg.get("response_timeout_seconds"),
        )

        if not message:
            state.add_log(
                "warning",
                f"[{task_name}] No bot response with inline keyboard found",
                account_id=account_id,
                config_id=config_id,
            )
            return {"status": "warning", "message": "No bot response with inline keyboard found"}

        found = find_button(message, cfg.get("action_keyword"))
        if not found:
            state.add_log(
                "warning",
                f"[{task_name}] No button containing the action keyword",
                account_id=account_id,
                config_id=config_id,
            )
            return {"status": "warning", "message": "No button containing the action keyword"}

        row_index, col_index, button = found
        clicked = await click_button(client, target_entity, message, row_index, col_index, button)

        if not clicked:
            state.add_log(
                "error",
                f"[{task_name}] Could not click matching button",
                account_id=account_id,
                config_id=config_id,
            )
            return {"status": "error", "message": "Could not click matching button"}

        state.add_log(
            "info",
            f"[{task_name}] Clicked '{getattr(button, 'text', '')}'",
            account_id=account_id,
            config_id=config_id,
        )

        try:
            second_click_delay = float(cfg.get("second_click_delay_seconds", SECOND_CLICK_DELAY_SECONDS))
        except (TypeError, ValueError):
            second_click_delay = SECOND_CLICK_DELAY_SECONDS

        if second_click_delay > 0:
            await asyncio.sleep(second_click_delay + random.uniform(0, 5))

            try:
                refreshed_message = await client.get_messages(target_entity, ids=message.id)
            except Exception:
                refreshed_message = None

            if refreshed_message:
                found_again = find_button(refreshed_message, cfg.get("action_keyword"))
                if found_again:
                    row_index2, col_index2, button2 = found_again
                    clicked_again = await click_button(
                        client,
                        target_entity,
                        refreshed_message,
                        row_index2,
                        col_index2,
                        button2,
                    )

                    if clicked_again:
                        state.add_log(
                            "info",
                            f"[{task_name}] Final click executed",
                            account_id=account_id,
                            config_id=config_id,
                        )
                    else:
                        state.add_log(
                            "warning",
                            f"[{task_name}] Final click failed",
                            account_id=account_id,
                            config_id=config_id,
                        )
                else:
                    state.add_log(
                        "info",
                        f"[{task_name}] Keyboard no longer present",
                        account_id=account_id,
                        config_id=config_id,
                    )

        state.add_log("info", f"[{task_name}] Cycle finished", account_id=account_id, config_id=config_id)
        return {"status": "success", "message": "Cycle finished"}

    except errors.FloodWaitError as exc:
        set_account_cooldown(account_id, exc.seconds)
        raise

    except Exception:
        raise


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "ClickFarm/1.0"

    def log_message(self, fmt, *args):
        return

    def _send(self, body, status=200, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send(body, status=status, content_type="application/json")

    def _html(self, content, status=200):
        body = content.encode("utf-8")
        self._send(body, status=status, content_type="text/html; charset=utf-8")

    def _text(self, content, status=404):
        body = content.encode("utf-8")
        self._send(body, status=status, content_type="text/plain; charset=utf-8")

    def _body_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}

        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            raise ValueError("Invalid JSON body")

    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/") or "/"
        parts = [p for p in path.split("/") if p]

        try:
            if path in ("/", "/index.html", "/dashboard.html"):
                if HTML_FILE.exists():
                    self._html(HTML_FILE.read_text(encoding="utf-8"))
                else:
                    self._text("dashboard.html not found", 404)
                return

            if path == "/api/state":
                self._json(public_state())
                return

            if path == "/api/accounts":
                with STATE_LOCK:
                    accounts = [account_view(a) for a in state.accounts()]
                self._json({"ok": True, "accounts": accounts})
                return

            if path == "/api/configs":
                with STATE_LOCK:
                    configs = [config_view(c) for c in state.configs()]
                self._json({"ok": True, "configs": configs})
                return

            if path == "/api/logs":
                with STATE_LOCK:
                    logs = list(state.logs)
                self._json({"ok": True, "logs": logs})
                return

            if path == "/api/export":
                self._json({"ok": True, "configs": export_configs()})
                return

            if len(parts) == 3 and parts[0] == "api" and parts[1] == "accounts":
                account = state.get_account(parts[2])
                if not account:
                    self._json({"ok": False, "error": "Account not found"}, 404)
                    return
                self._json({"ok": True, "account": account_view(account)})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "accounts" and parts[3] == "configs":
                with STATE_LOCK:
                    configs = [config_view(c) for c in state.configs() if c.get("account_id") == parts[2]]
                self._json({"ok": True, "configs": configs})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "accounts" and parts[3] == "logs":
                with STATE_LOCK:
                    logs = [entry for entry in state.logs if entry.get("account_id") == parts[2]]
                self._json({"ok": True, "logs": logs})
                return

            if len(parts) == 3 and parts[0] == "api" and parts[1] == "configs":
                cfg = state.get_config(parts[2])
                if not cfg:
                    self._json({"ok": False, "error": "Config not found"}, 404)
                    return
                self._json({"ok": True, "config": config_view(cfg)})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "configs" and parts[3] == "logs":
                with STATE_LOCK:
                    logs = [entry for entry in state.logs if entry.get("config_id") == parts[2]]
                self._json({"ok": True, "logs": logs})
                return

            self._json({"ok": False, "error": "Not found"}, 404)

        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 400)

    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")
        parts = [p for p in path.split("/") if p]

        try:
            payload = self._body_json()
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 400)
            return

        try:
            if path == "/api/accounts":
                account = run_async(add_account_async(payload))
                self._json({"ok": True, "account": account_view(account)})
                return

            if path == "/api/accounts/import":
                account = run_async(import_account_async(payload))
                self._json({"ok": True, "account": account_view(account)})
                return

            if path == "/api/configs":
                cfg = save_config_payload(payload)
                self._json({"ok": True, "config": config_view(cfg)})
                return

            if path == "/api/start-all":
                result = run_async(start_all_async(payload.get("account_id") or None))
                self._json({"ok": True, **result})
                return

            if path == "/api/stop-all":
                result = run_async(stop_all_async(payload.get("account_id") or None))
                self._json({"ok": True, **result})
                return

            if path == "/api/import":
                imported = import_configs(payload)
                self._json({
                    "ok": True,
                    "imported": len(imported),
                    "configs": [config_view(c) for c in imported],
                })
                return

            if path == "/api/logs/clear":
                with STATE_LOCK:
                    state.logs = []
                self._json({"ok": True})
                return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "accounts":
                account_id = parts[2]
                action = parts[3]

                if action == "code":
                    account = run_async(verify_code_async(account_id, str(payload.get("code") or "").strip()))
                    self._json({"ok": True, "account": account_view(account)})
                    return

                if action == "password":
                    account = run_async(verify_password_async(account_id, str(payload.get("password") or "")))
                    self._json({"ok": True, "account": account_view(account)})
                    return

                if action == "refresh":
                    account = run_async(refresh_account_async(account_id))
                    self._json({"ok": True, "account": account_view(account)})
                    return

                if action == "remove":
                    run_async(remove_account_async(account_id))
                    self._json({"ok": True})
                    return

                if action == "start-all":
                    result = run_async(start_all_async(account_id))
                    self._json({"ok": True, **result})
                    return

                if action == "stop-all":
                    result = run_async(stop_all_async(account_id))
                    self._json({"ok": True, **result})
                    return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "configs":
                config_id = parts[2]
                action = parts[3]

                if action == "start":
                    cfg = run_async(start_config_async(config_id))
                    self._json({"ok": True, "config": config_view(cfg)})
                    return

                if action == "stop":
                    cfg = run_async(stop_config_async(config_id))
                    self._json({"ok": True, "config": config_view(cfg)})
                    return

                if action == "duplicate":
                    cfg = duplicate_config(config_id, payload.get("account_id"))
                    self._json({"ok": True, "config": config_view(cfg)})
                    return

            self._json({"ok": False, "error": "Not found"}, 404)

        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 400)

    def do_PUT(self):
        path = urlparse(self.path).path.rstrip("/")
        parts = [p for p in path.split("/") if p]

        try:
            payload = self._body_json()
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 400)
            return

        try:
            if len(parts) == 3 and parts[0] == "api" and parts[1] == "configs":
                cfg = save_config_payload(payload, parts[2])
                self._json({"ok": True, "config": config_view(cfg)})
                return

            if len(parts) == 3 and parts[0] == "api" and parts[1] == "accounts":
                account = update_account_payload(parts[2], payload)
                self._json({"ok": True, "account": account_view(account)})
                return

            self._json({"ok": False, "error": "Not found"}, 404)

        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 400)

    def do_DELETE(self):
        path = urlparse(self.path).path.rstrip("/")
        parts = [p for p in path.split("/") if p]

        try:
            if len(parts) == 3 and parts[0] == "api" and parts[1] == "accounts":
                run_async(remove_account_async(parts[2]))
                self._json({"ok": True})
                return

            if len(parts) == 3 and parts[0] == "api" and parts[1] == "configs":
                run_async(delete_config_async(parts[2]))
                self._json({"ok": True})
                return

            self._json({"ok": False, "error": "Not found"}, 404)

        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 400)


def main():
    global LOOP

    state.add_log("info", f"Starting local server on http://{HOST}:{PORT}")

    def run_loop():
        global LOOP
        LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(LOOP)
        LOOP.create_task(startup_async())
        LOOP.run_forever()

    threading.Thread(target=run_loop, daemon=True).start()

    deadline = time.time() + LOOP_WAIT_SECONDS
    while LOOP is None and time.time() < deadline:
        time.sleep(0.05)

    ThreadingHTTPServer.daemon_threads = True

    try:
        httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    except OSError as exc:
        log.error("Could not start local web server: %s", exc)
        return

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        state.add_log("info", "Shutting down")
    finally:
        if LOOP:
            LOOP.call_soon_threadsafe(LOOP.stop)
        httpd.server_close()


if __name__ == "__main__":
    main()