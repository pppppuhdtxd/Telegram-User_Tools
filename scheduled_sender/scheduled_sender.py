"""
Telegram Userbot Scheduled Message Sender
Multi-account, multi-config, with an extensible mode system.

HOW TO ADD A NEW MODE
─────────────────────
1. Create a subclass of BaseMode below.
2. Define:
   - MODE_ID: int          — unique integer key
   - LABEL: str            — human-readable name shown in the UI
   - FIELDS: list[dict]    — field descriptors sent to the frontend
   - validate(cfg) -> None — raise ValueError on bad input
   - build_config(body) -> dict — parse raw POST body into a clean config dict
   - refill(client, cfg, peer, fetch_ids, delete_ids) -> (bool, str)
   - schedule_count_key(cfg) -> str | None  — key for counting scheduled msgs
3. Register it: MODE_REGISTRY[YourMode.MODE_ID] = YourMode()

That's it — no other file needs to change.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError, AuthKeyError
from telethon.tl.functions.messages import DeleteScheduledMessagesRequest, GetScheduledHistoryRequest

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("scheduled_sender")
logger.setLevel(logging.INFO)
logging.getLogger("aiohttp.access").setLevel(logging.CRITICAL)
logging.getLogger("aiohttp.server").setLevel(logging.ERROR)
logging.getLogger("telethon").setLevel(logging.ERROR)

# ─── Constants ────────────────────────────────────────────────────────────────

DATA_FILE     = "accounts.json"
_LEGACY_FILE  = "ss_data.json"   # pre-v-string-session filename, migrated on first load
WEB_HOST      = "0.0.0.0"
WEB_PORT      = 8081

# ─── Storage ──────────────────────────────────────────────────────────────────
#
# Every account credential (api_id, api_hash, phone, and — as of this
# version — session_string, the Telethon StringSession for that account)
# lives in this single accounts.json, alongside that account's configs.
# No .session files are created or read anymore, in any code path: not on
# a fresh full login, and not when importing an already-existing string
# session. Login/session state is 100% self-contained in accounts.json.

def load_data() -> dict:
    if Path(DATA_FILE).exists():
        try:
            with open(DATA_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    # One-time migration from the old filename, if present.
    if Path(_LEGACY_FILE).exists():
        try:
            with open(_LEGACY_FILE) as f:
                legacy = json.load(f)
            logger.info(f"Migrated {_LEGACY_FILE} → {DATA_FILE}")
            save_data(legacy)
            return legacy
        except Exception as e:
            logger.error(f"Failed to migrate {_LEGACY_FILE}: {e}")
    return {"accounts": {}, "active_account": None}


def save_data(data: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ─── Global State ─────────────────────────────────────────────────────────────

data: dict = load_data()
clients: dict[str, TelegramClient] = {}
pending_auth: dict[str, dict] = {}
# mode3 running tasks: (account_name, config_id) -> asyncio.Task
mode3_tasks: dict[tuple, asyncio.Task] = {}

# ─── MTProto Helpers ──────────────────────────────────────────────────────────

async def fetch_all_scheduled_ids(client: TelegramClient, peer) -> list[int]:
    """Bypass cache: raw MTProto call with hash=0 forces a full server response."""
    result = await client(GetScheduledHistoryRequest(peer=peer, hash=0))
    return [m.id for m in result.messages]


async def delete_scheduled_ids(client: TelegramClient, peer, ids: list[int]) -> None:
    """Delete scheduled messages in chunks of 100 (Telegram limit per request)."""
    for i in range(0, len(ids), 100):
        await client(DeleteScheduledMessagesRequest(peer=peer, id=ids[i:i + 100]))


async def clear_and_verify(client: TelegramClient, peer) -> tuple[bool, str]:
    """
    Fetch, delete, then re-verify the scheduled queue is empty.
    Returns (True, "") on success or (False, error_message) on failure.
    """
    try:
        ids = await fetch_all_scheduled_ids(client, peer)
    except Exception as e:
        return False, f"Could not fetch scheduled messages: {e}"

    if ids:
        try:
            await delete_scheduled_ids(client, peer, ids)
        except Exception as e:
            return False, f"Deletion failed ({len(ids)} messages): {e}"

        try:
            remaining = await fetch_all_scheduled_ids(client, peer)
        except Exception as e:
            return False, f"Could not verify deletion: {e}"

        if remaining:
            return False, (
                f"Deletion incomplete — {len(remaining)} messages still remain. "
                "Aborting to avoid the 100-message limit."
            )
        logger.info(f"Deleted {len(ids)} scheduled messages.")

    return True, ""


async def schedule_batch(client: TelegramClient, chat_id: int, message: str, timestamps: list[int]) -> int:
    """Schedule messages concurrently. Returns count of successfully sent messages."""
    async def send_one(ts: int) -> bool:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        try:
            await client.send_message(chat_id, message, schedule=dt)
            return True
        except FloodWaitError as e:
            logger.warning(f"FloodWait {e.seconds}s — waiting...")
            await asyncio.sleep(e.seconds + 1)
            try:
                await client.send_message(chat_id, message, schedule=dt)
                return True
            except Exception as ex:
                logger.error(f"Failed after FloodWait: {ex}")
                return False
        except Exception as ex:
            logger.error(f"Failed to schedule: {ex}")
            return False

    results = await asyncio.gather(*[send_one(ts) for ts in timestamps])
    return sum(1 for r in results if r)


# ═══════════════════════════════════════════════════════════════════════════════
# MODE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

class BaseMode(ABC):
    """
    Abstract base for all scheduling modes.

    Subclass this, set the class attributes, implement the abstract methods,
    and register the instance in MODE_REGISTRY.
    """

    MODE_ID: int = 0
    LABEL: str = "Unnamed Mode"

    # FIELDS describes the configuration inputs for this mode.
    # Each dict: { name, label, type ("number"|"text"|"textarea"), default, hint? }
    FIELDS: list[dict] = []

    @abstractmethod
    def validate(self, cfg: dict) -> None:
        """Raise ValueError with a clear message if cfg is invalid."""

    @abstractmethod
    def build_config(self, body: dict) -> dict:
        """Parse a raw API request body into the clean config dict to store."""

    @abstractmethod
    async def refill(
        self,
        client: TelegramClient,
        cfg: dict,
        peer,
    ) -> tuple[bool, str]:
        """
        Execute a refill (clear + reschedule).
        Must clear old messages before scheduling new ones.
        Returns (success, message).
        """

    def supports_refill(self) -> bool:
        """Return False for modes that use start/stop instead of refill."""
        return True

    def supports_start_stop(self) -> bool:
        """Return True for modes that use start/stop commands."""
        return False

    async def start(self, client: TelegramClient, cfg: dict, peer, account_name: str, config_id: str) -> tuple[bool, str]:
        return False, f"Mode '{self.LABEL}' does not support start/stop."

    async def stop(self, account_name: str, config_id: str) -> tuple[bool, str]:
        return False, f"Mode '{self.LABEL}' does not support start/stop."

    def scheduled_count_uses_telegram_queue(self) -> bool:
        """True if this mode uses Telegram's native scheduled message queue."""
        return True


# ─── Mode 1: Single Interval ──────────────────────────────────────────────────

class SingleIntervalMode(BaseMode):
    """
    Schedules messages at a fixed, repeating interval.
    e.g. every 5 minutes: msg1 at +5m, msg2 at +10m, msg3 at +15m …
    """

    MODE_ID = 1
    LABEL   = "Single Interval"
    FIELDS  = [
        {"name": "chat_id",   "label": "Chat ID",             "type": "text",     "default": "",  "hint": "Negative for groups/channels (e.g. -1001234567890)"},
        {"name": "message",   "label": "Message",             "type": "textarea", "default": ""},
        {"name": "interval",  "label": "Interval (minutes)",  "type": "number",   "default": 60},
        {"name": "batch_size","label": "Batch Size",          "type": "number",   "default": 10,  "hint": "Messages per refill"},
    ]

    def validate(self, cfg: dict) -> None:
        if not cfg.get("chat_id"):
            raise ValueError("chat_id is required")
        if not cfg.get("message", "").strip():
            raise ValueError("message is required")
        if cfg.get("interval", 0) < 1:
            raise ValueError("interval must be ≥ 1 minute")
        if cfg.get("batch_size", 0) < 1:
            raise ValueError("batch_size must be ≥ 1")

    def build_config(self, body: dict) -> dict:
        cfg = {
            "mode":       self.MODE_ID,
            "chat_id":    int(str(body["chat_id"]).strip()),
            "message":    body["message"].strip(),
            "interval":   int(body.get("interval", 60)),
            "batch_size": int(body.get("batch_size", 10)),
        }
        self.validate(cfg)
        return cfg

    async def refill(self, client, cfg, peer) -> tuple[bool, str]:
        ok, err = await clear_and_verify(client, peer)
        if not ok:
            return False, err

        interval   = cfg["interval"]
        batch_size = cfg["batch_size"]
        now        = int(datetime.now(timezone.utc).timestamp())
        timestamps = [now + interval * 60 * (i + 1) for i in range(batch_size)]

        sent = await schedule_batch(client, cfg["chat_id"], cfg["message"], timestamps)
        return True, f"Scheduled {sent}/{batch_size} messages (every {interval}m)"


# ─── Mode 2: Dual Interval ────────────────────────────────────────────────────

class DualIntervalMode(BaseMode):
    """
    Alternates between two intervals.
    e.g. interval1=5m, interval2=8m → +5m, +13m, +18m, +26m …
    """

    MODE_ID = 2
    LABEL   = "Dual Interval"
    FIELDS  = [
        {"name": "chat_id",    "label": "Chat ID",              "type": "text",     "default": "",  "hint": "Negative for groups/channels"},
        {"name": "message",    "label": "Message",              "type": "textarea", "default": ""},
        {"name": "interval1",  "label": "Interval 1 (minutes)", "type": "number",   "default": 60},
        {"name": "interval2",  "label": "Interval 2 (minutes)", "type": "number",   "default": 60,  "hint": "Alternates with Interval 1"},
        {"name": "batch_size", "label": "Batch Size",           "type": "number",   "default": 10,  "hint": "Messages per refill"},
    ]

    def validate(self, cfg: dict) -> None:
        if not cfg.get("chat_id"):
            raise ValueError("chat_id is required")
        if not cfg.get("message", "").strip():
            raise ValueError("message is required")
        if cfg.get("interval1", 0) < 1:
            raise ValueError("interval1 must be ≥ 1 minute")
        if cfg.get("interval2", 0) < 1:
            raise ValueError("interval2 must be ≥ 1 minute")
        if cfg.get("batch_size", 0) < 1:
            raise ValueError("batch_size must be ≥ 1")

    def build_config(self, body: dict) -> dict:
        cfg = {
            "mode":       self.MODE_ID,
            "chat_id":    int(str(body["chat_id"]).strip()),
            "message":    body["message"].strip(),
            "interval1":  int(body.get("interval1", 60)),
            "interval2":  int(body.get("interval2", 60)),
            "batch_size": int(body.get("batch_size", 10)),
        }
        self.validate(cfg)
        return cfg

    async def refill(self, client, cfg, peer) -> tuple[bool, str]:
        ok, err = await clear_and_verify(client, peer)
        if not ok:
            return False, err

        interval1  = cfg["interval1"]
        interval2  = cfg["interval2"]
        batch_size = cfg["batch_size"]
        now        = int(datetime.now(timezone.utc).timestamp())
        timestamps = []
        current    = now
        for i in range(batch_size):
            current += (interval1 if i % 2 == 0 else interval2) * 60
            timestamps.append(current)

        sent = await schedule_batch(client, cfg["chat_id"], cfg["message"], timestamps)
        return True, f"Scheduled {sent}/{batch_size} messages ({interval1}m / {interval2}m alternating)"


# ─── Mode 3: Auto-Delete ──────────────────────────────────────────────────────

class AutoDeleteMode(BaseMode):
    """
    Sends a message, waits send_interval, then deletes it after delete_delay.
    Runs as a continuous background loop until stopped.
    Uses start/stop commands instead of refill.
    """

    MODE_ID = 3
    LABEL   = "Auto-Delete"
    FIELDS  = [
        {"name": "chat_id",       "label": "Chat ID",                 "type": "text",   "default": "",  "hint": "Negative for groups/channels"},
        {"name": "message",       "label": "Message",                 "type": "textarea","default": ""},
        {"name": "send_interval", "label": "Send Interval (seconds)", "type": "number", "default": 300, "hint": "Seconds between each message"},
        {"name": "delete_delay",  "label": "Delete Delay (seconds)",  "type": "number", "default": 60,  "hint": "Seconds before deleting the sent message"},
        {"name": "batch_size",    "label": "Batch Size (cycles)",     "type": "number", "default": 10,  "hint": "How many send-delete cycles to run before stopping (0 = unlimited)"},
    ]

    def validate(self, cfg: dict) -> None:
        if not cfg.get("chat_id"):
            raise ValueError("chat_id is required")
        if not cfg.get("message", "").strip():
            raise ValueError("message is required")
        if cfg.get("send_interval", 0) < 1:
            raise ValueError("send_interval must be ≥ 1 second")
        if cfg.get("delete_delay", 0) < 1:
            raise ValueError("delete_delay must be ≥ 1 second")

    def build_config(self, body: dict) -> dict:
        cfg = {
            "mode":          self.MODE_ID,
            "chat_id":       int(str(body["chat_id"]).strip()),
            "message":       body["message"].strip(),
            "send_interval": int(body.get("send_interval", 300)),
            "delete_delay":  int(body.get("delete_delay", 60)),
            "batch_size":    int(body.get("batch_size", 10)),
        }
        self.validate(cfg)
        return cfg

    def supports_refill(self) -> bool:
        return False

    def supports_start_stop(self) -> bool:
        return True

    def scheduled_count_uses_telegram_queue(self) -> bool:
        return False  # uses real-time send, not the scheduled queue

    async def start(self, client, cfg, peer, account_name, config_id) -> tuple[bool, str]:
        key = (account_name, config_id)
        if key in mode3_tasks and not mode3_tasks[key].done():
            return False, "Auto-Delete loop is already running for this config."

        task = asyncio.create_task(
            self._run_loop(client, cfg, account_name, config_id),
            name=f"mode3-{account_name}-{config_id}",
        )
        mode3_tasks[key] = task
        return True, "Auto-Delete loop started."

    async def refill(self, client, cfg, peer) -> tuple[bool, str]:
        return False, "Auto-Delete mode does not support refill. Use Start/Stop instead."

    async def stop(self, account_name, config_id) -> tuple[bool, str]:
        key = (account_name, config_id)
        task = mode3_tasks.get(key)
        if not task or task.done():
            return False, "No running Auto-Delete loop for this config."
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        mode3_tasks.pop(key, None)
        return True, "Auto-Delete loop stopped."

    async def _run_loop(self, client: TelegramClient, cfg: dict, account_name: str, config_id: str):
        chat_id       = cfg["chat_id"]
        message       = cfg["message"]
        send_interval = cfg["send_interval"]
        delete_delay  = cfg["delete_delay"]
        max_cycles    = cfg.get("batch_size", 0)  # 0 = unlimited
        cycle         = 0

        logger.info(f"[Mode3] Starting loop for {account_name}/{config_id} — "
                    f"send every {send_interval}s, delete after {delete_delay}s")
        try:
            while True:
                if max_cycles > 0 and cycle >= max_cycles:
                    logger.info(f"[Mode3] Reached {max_cycles} cycles, stopping.")
                    break

                # Send message
                try:
                    sent_msg = await client.send_message(chat_id, message)
                except FloodWaitError as e:
                    logger.warning(f"[Mode3] FloodWait {e.seconds}s")
                    await asyncio.sleep(e.seconds + 1)
                    sent_msg = await client.send_message(chat_id, message)
                except Exception as ex:
                    logger.error(f"[Mode3] Failed to send: {ex}")
                    await asyncio.sleep(send_interval)
                    continue

                cycle += 1
                msg_id = sent_msg.id

                # Wait delete_delay, then delete
                await asyncio.sleep(delete_delay)
                try:
                    await client.delete_messages(chat_id, [msg_id])
                except Exception as ex:
                    logger.warning(f"[Mode3] Failed to delete message {msg_id}: {ex}")

                # Wait remaining interval before next send
                remaining = send_interval - delete_delay
                if remaining > 0:
                    await asyncio.sleep(remaining)

        except asyncio.CancelledError:
            logger.info(f"[Mode3] Loop cancelled for {account_name}/{config_id}")
            raise
        finally:
            mode3_tasks.pop((account_name, config_id), None)


# ─── Mode Registry ────────────────────────────────────────────────────────────
# To add a new mode: instantiate it and add it here. Nothing else needs editing.

MODE_REGISTRY: dict[int, BaseMode] = {
    SingleIntervalMode.MODE_ID: SingleIntervalMode(),
    DualIntervalMode.MODE_ID:   DualIntervalMode(),
    AutoDeleteMode.MODE_ID:     AutoDeleteMode(),
}

DEFAULT_MODE_ID = SingleIntervalMode.MODE_ID


def get_mode(cfg: dict) -> BaseMode:
    """Return the mode instance for a config, falling back to default."""
    mode_id = cfg.get("mode", DEFAULT_MODE_ID)
    return MODE_REGISTRY.get(mode_id, MODE_REGISTRY[DEFAULT_MODE_ID])


def migrate_config(cfg: dict) -> dict:
    """
    Migrate pre-mode configs (those without a 'mode' key) to Mode 2 (Dual Interval)
    since they have interval1/interval2 fields, which is the dual-interval shape.
    """
    if "mode" not in cfg:
        if "interval1" in cfg and "interval2" in cfg:
            cfg["mode"] = DualIntervalMode.MODE_ID
        elif "interval" in cfg:
            cfg["mode"] = SingleIntervalMode.MODE_ID
        else:
            cfg["mode"] = DEFAULT_MODE_ID
    return cfg


# ─── Data Helpers ─────────────────────────────────────────────────────────────

def get_active_name() -> Optional[str]:
    return data.get("active_account")


def next_config_id(account: dict) -> int:
    configs = account.get("configs", {})
    return max((int(k) for k in configs), default=0) + 1


# ─── Refill Entry Point ───────────────────────────────────────────────────────

async def do_refill(client: TelegramClient, account_name: str, config_id: str) -> tuple[bool, str]:
    account = data["accounts"].get(account_name)
    if not account:
        return False, "Account not found"
    cfg = migrate_config(account.get("configs", {}).get(str(config_id), {}))
    if not cfg:
        return False, "Config not found"

    mode = get_mode(cfg)
    if not mode.supports_refill():
        return False, f"Mode '{mode.LABEL}' does not use refill. Use Start/Stop instead."

    try:
        peer = await client.get_input_entity(cfg["chat_id"])
    except Exception as e:
        return False, f"Could not resolve chat {cfg['chat_id']}: {e}"

    return await mode.refill(client, cfg, peer)


# ─── Client Management ────────────────────────────────────────────────────────

def _build_client(account: dict, name: str) -> TelegramClient:
    """
    Build a TelegramClient purely from account["session_string"].

    No .session file is ever created or read here. If the account has no
    session_string yet (brand-new, not-yet-authorized entry), an empty
    StringSession() is used, which behaves like a fresh unauthenticated
    client — is_user_authorized() will simply return False until a login
    (full or string-import) fills in session_string.
    """
    session = StringSession(account.get("session_string", "") or None)
    return TelegramClient(session, account["api_id"], account["api_hash"])


async def _migrate_legacy_file_session(name: str, account: dict) -> None:
    """
    One-time upgrade path for accounts created by older versions of this
    script, which stored a SQLite session at ./session_{name}.session.

    If such a file exists and this account has no session_string yet,
    open it, derive the equivalent StringSession, save it into
    accounts.json, and delete the old file — after this, the account
    behaves exactly like one that was always string-session-only.
    Silently does nothing if there's no legacy file to migrate.
    """
    legacy_path = Path(f"session_{name}.session")
    if account.get("session_string") or not legacy_path.exists():
        return

    legacy_client = TelegramClient(str(legacy_path.with_suffix("")), account["api_id"], account["api_hash"])
    try:
        await legacy_client.connect()
        if not await legacy_client.is_user_authorized():
            logger.warning(f"Legacy session file for '{name}' exists but isn't authorized — leaving it as-is.")
            return
        account["session_string"] = StringSession.save(legacy_client.session)
        save_data(data)
        logger.info(f"✓ Migrated '{name}' from legacy session file to session_string.")
    except Exception as e:
        logger.error(f"Could not migrate legacy session file for '{name}': {e}")
        return
    finally:
        try:
            await legacy_client.disconnect()
        except Exception:
            pass

    # Only remove the old files once the string session was saved successfully.
    for suffix in ("", "-journal", "-shm", "-wal"):
        p = Path(f"session_{name}.session{suffix}")
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


async def start_client(name: str) -> Optional[TelegramClient]:
    account = data["accounts"].get(name)
    if not account:
        return None

    await _migrate_legacy_file_session(name, account)

    client = _build_client(account, name)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.warning(f"Account '{name}' not authorized yet.")
        else:
            register_cli_handlers(client, name)
            logger.info(f"✓ Account '{name}' connected.")
        clients[name] = client
        return client
    except Exception as e:
        logger.error(f"Failed to start client '{name}': {e}")
        return None


async def start_all_clients():
    for name in data["accounts"]:
        if name not in clients:
            await start_client(name)


# ─── CLI Handlers ─────────────────────────────────────────────────────────────

def register_cli_handlers(client: TelegramClient, account_name: str):

    @client.on(events.NewMessage(pattern=r"^\.ssaccount\s+list$", outgoing=True))
    async def cmd_account_list(event):
        accounts = data["accounts"]
        active = get_active_name()
        if not accounts:
            await event.edit("No accounts configured.")
            return
        lines = ["**Accounts:**"]
        for n in accounts:
            marker = " ◀ active" if n == active else ""
            lines.append(f"• `{n}`{marker}")
        await event.edit("\n".join(lines))

    @client.on(events.NewMessage(pattern=r"^\.ssaccount\s+switch\s+(\S+)$", outgoing=True))
    async def cmd_account_switch(event):
        name = event.pattern_match.group(1)
        if name not in data["accounts"]:
            await event.edit(f"Account `{name}` not found.")
            return
        data["active_account"] = name
        save_data(data)
        await event.edit(f"Switched to account `{name}`.")

    @client.on(events.NewMessage(pattern=r"^\.ssaccount\s+delete\s+(\S+)$", outgoing=True))
    async def cmd_account_delete(event):
        name = event.pattern_match.group(1)
        if name not in data["accounts"]:
            await event.edit(f"Account `{name}` not found.")
            return
        del data["accounts"][name]
        if data["active_account"] == name:
            remaining = list(data["accounts"].keys())
            data["active_account"] = remaining[0] if remaining else None
        save_data(data)
        c = clients.pop(name, None)
        if c:
            await c.disconnect()
        await event.edit(f"Deleted account `{name}`.")

    @client.on(events.NewMessage(pattern=r"^\.ssconfig$", outgoing=True))
    async def cmd_ssconfig(event):
        active = get_active_name()
        if not active:
            await event.edit("No active account.")
            return
        account = data["accounts"][active]
        configs = account.get("configs", {})
        if not configs:
            await event.edit(f"No configs for `{active}`.")
            return
        lines = [f"**Configs for `{active}`:**"]
        for cid, cfg in configs.items():
            cfg = migrate_config(cfg)
            mode = get_mode(cfg)
            preview = cfg.get("message", "")[:40]
            ellipsis = "..." if len(cfg.get("message", "")) > 40 else ""
            lines.append(
                f"\n**#{cid}** [{mode.LABEL}] — Chat: `{cfg['chat_id']}`\n"
                f"  Msg: `{preview}{ellipsis}`"
            )
        await event.edit("\n".join(lines))

    @client.on(events.NewMessage(pattern=r"^\.sslist$", outgoing=True))
    async def cmd_sslist(event):
        active = get_active_name()
        if not active:
            await event.edit("No active account.")
            return
        account = data["accounts"][active]
        configs = account.get("configs", {})
        if not configs:
            await event.edit(f"No configs for `{active}`.")
            return
        c = clients.get(active)
        lines = [f"**Scheduled messages for `{active}`:**"]
        for cid, cfg in configs.items():
            cfg = migrate_config(cfg)
            mode = get_mode(cfg)
            if mode.scheduled_count_uses_telegram_queue():
                try:
                    peer = await c.get_input_entity(cfg["chat_id"])
                    ids = await fetch_all_scheduled_ids(c, peer)
                    count_str = f"{len(ids)} scheduled"
                except Exception as e:
                    count_str = f"error: {e}"
            else:
                key = (active, cid)
                running = key in mode3_tasks and not mode3_tasks[key].done()
                count_str = "running" if running else "stopped"
            lines.append(f"\nConfig #{cid} [{mode.LABEL}] (chat `{cfg['chat_id']}`): {count_str}")
        await event.edit("\n".join(lines))

    @client.on(events.NewMessage(pattern=r"^\.ssdelete\s+(\d+)$", outgoing=True))
    async def cmd_ssdelete(event):
        active = get_active_name()
        if not active:
            await event.edit("No active account.")
            return
        config_id = event.pattern_match.group(1)
        account = data["accounts"][active]
        if config_id not in account.get("configs", {}):
            await event.edit(f"Config #{config_id} not found.")
            return
        # Stop mode3 if running
        key = (active, config_id)
        task = mode3_tasks.get(key)
        if task and not task.done():
            task.cancel()
        del account["configs"][config_id]
        save_data(data)
        await event.edit(f"Deleted config #{config_id}.")

    @client.on(events.NewMessage(pattern=r"^\.ssrefill(?:\s+(\d+))?$", outgoing=True))
    async def cmd_ssrefill(event):
        active = get_active_name()
        if not active:
            await event.edit("No active account.")
            return
        account = data["accounts"][active]
        configs = account.get("configs", {})
        if not configs:
            await event.edit("No configs found.")
            return
        config_id_arg = event.pattern_match.group(1)
        c = clients.get(active)
        if not c:
            await event.edit("Client not connected.")
            return
        cids = [config_id_arg] if config_id_arg else list(configs.keys())
        results = []
        for cid in cids:
            if cid not in configs:
                results.append(f"Config #{cid}: not found")
                continue
            await event.edit(f"Refilling config #{cid}...")
            ok, msg = await do_refill(c, active, cid)
            results.append(f"Config #{cid}: {msg}")
        await event.edit("\n".join(results))

    @client.on(events.NewMessage(pattern=r"^\.ssstart\s+(\d+)$", outgoing=True))
    async def cmd_ssstart(event):
        active = get_active_name()
        if not active:
            await event.edit("No active account.")
            return
        config_id = event.pattern_match.group(1)
        account = data["accounts"][active]
        cfg = migrate_config(account.get("configs", {}).get(config_id, {}))
        if not cfg:
            await event.edit(f"Config #{config_id} not found.")
            return
        mode = get_mode(cfg)
        if not mode.supports_start_stop():
            await event.edit(f"Config #{config_id} uses mode '{mode.LABEL}' which does not support start/stop. Use `.ssrefill {config_id}` instead.")
            return
        c = clients.get(active)
        try:
            peer = await c.get_input_entity(cfg["chat_id"])
        except Exception as e:
            await event.edit(f"Could not resolve chat: {e}")
            return
        ok, msg = await mode.start(c, cfg, peer, active, config_id)
        await event.edit(f"Config #{config_id}: {msg}")

    @client.on(events.NewMessage(pattern=r"^\.ssstop\s+(\d+)$", outgoing=True))
    async def cmd_ssstop(event):
        active = get_active_name()
        if not active:
            await event.edit("No active account.")
            return
        config_id = event.pattern_match.group(1)
        account = data["accounts"][active]
        cfg = migrate_config(account.get("configs", {}).get(config_id, {}))
        if not cfg:
            await event.edit(f"Config #{config_id} not found.")
            return
        mode = get_mode(cfg)
        if not mode.supports_start_stop():
            await event.edit(f"Mode '{mode.LABEL}' does not support start/stop.")
            return
        ok, msg = await mode.stop(active, config_id)
        await event.edit(f"Config #{config_id}: {msg}")

    @client.on(events.NewMessage(pattern=r"^\.sshelp$", outgoing=True))
    async def cmd_sshelp(event):
        modes_list = "\n".join(f"  {mid}: {m.LABEL}" for mid, m in MODE_REGISTRY.items())
        await event.edit(
            f"**Scheduled Sender Commands:**\n\n"
            f"`.ssaccount list` — List accounts\n"
            f"`.ssaccount switch <name>` — Switch active account\n"
            f"`.ssaccount delete <name>` — Delete account\n"
            f"`.ssconfig` — Show configs\n"
            f"`.sslist` — Show scheduled counts / status\n"
            f"`.ssdelete <id>` — Delete a config\n"
            f"`.ssrefill [id]` — Refill (Modes 1 & 2)\n"
            f"`.ssstart <id>` — Start loop (Mode 3)\n"
            f"`.ssstop <id>` — Stop loop (Mode 3)\n"
            f"`.sshelp` — This help\n\n"
            f"**Available Modes:**\n{modes_list}\n\n"
            f"Web UI: http://localhost:{WEB_PORT}"
        )


# ─── Web API ──────────────────────────────────────────────────────────────────

routes = web.RouteTableDef()


@routes.get("/api/modes")
async def api_modes(request):
    """Return all registered modes and their field descriptors."""
    return web.json_response([
        {
            "id":                m.MODE_ID,
            "label":             m.LABEL,
            "fields":            m.FIELDS,
            "supports_refill":   m.supports_refill(),
            "supports_start_stop": m.supports_start_stop(),
        }
        for m in MODE_REGISTRY.values()
    ])


@routes.get("/api/status")
async def api_status(request):
    active = get_active_name()
    accounts_info = {}
    for name, acc in data["accounts"].items():
        c = clients.get(name)
        connected, me = False, None
        if c:
            try:
                connected = await c.is_user_authorized()
                if connected:
                    user = await c.get_me()
                    me = f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username
            except Exception:
                pass
        accounts_info[name] = {
            "connected":    connected,
            "display_name": me,
            "config_count": len(acc.get("configs", {})),
        }
    return web.json_response({"active_account": active, "accounts": accounts_info})


@routes.get("/api/accounts")
async def api_accounts(request):
    result = []
    active = get_active_name()
    for name, acc in data["accounts"].items():
        c = clients.get(name)
        connected, me = False, None
        if c:
            try:
                connected = await c.is_user_authorized()
                if connected:
                    user = await c.get_me()
                    me = f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username
            except Exception:
                pass
        result.append({
            "name": name, "phone": acc.get("phone", ""),
            "connected": connected, "display_name": me,
            "is_active": name == active,
            "config_count": len(acc.get("configs", {})),
        })
    return web.json_response(result)


@routes.post("/api/accounts/send_code")
async def api_send_code(request):
    try:
        body = await request.json()
        name     = body.get("name", "").strip()
        api_id   = int(body.get("api_id", 0))
        api_hash = body.get("api_hash", "").strip()
        phone    = body.get("phone", "").strip()
        if not all([name, api_id, api_hash, phone]):
            return web.json_response({"error": "All fields required"}, status=400)
        if name in data["accounts"]:
            return web.json_response({"error": f"Account '{name}' already exists"}, status=400)
        # In-memory StringSession only — no .session file is written to disk
        # at any point in this login flow, not even a temporary one.
        client = TelegramClient(StringSession(), api_id, api_hash)
        await client.connect()
        result = await client.send_code_request(phone)
        pending_auth[phone] = {
            "client": client, "phone_code_hash": result.phone_code_hash,
            "name": name, "api_id": api_id, "api_hash": api_hash, "phone": phone,
        }
        return web.json_response({"ok": True, "message": "Code sent to your Telegram app"})
    except FloodWaitError as e:
        return web.json_response({"error": f"FloodWait: try again in {e.seconds}s"}, status=429)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


@routes.post("/api/accounts/verify_code")
async def api_verify_code(request):
    try:
        body     = await request.json()
        phone    = body.get("phone", "").strip()
        code     = body.get("code", "").strip()
        password = body.get("password", "").strip()
        if phone not in pending_auth:
            return web.json_response({"error": "No pending auth for this phone"}, status=400)
        auth   = pending_auth[phone]
        client = auth["client"]
        try:
            await client.sign_in(phone, code, phone_code_hash=auth["phone_code_hash"])
        except SessionPasswordNeededError:
            if not password:
                return web.json_response({"error": "2FA_REQUIRED"}, status=401)
            await client.sign_in(password=password)
        except PhoneCodeInvalidError:
            return web.json_response({"error": "Invalid code"}, status=400)
        name = auth["name"]
        data["accounts"][name] = {
            "api_id": auth["api_id"], "api_hash": auth["api_hash"],
            "phone": auth["phone"], "session_string": StringSession.save(client.session),
            "configs": {},
        }
        if not data.get("active_account"):
            data["active_account"] = name
        save_data(data)
        del pending_auth[phone]
        register_cli_handlers(client, name)
        clients[name] = client
        logger.info(f"✓ Account '{name}' authenticated.")
        return web.json_response({"ok": True, "message": f"Account '{name}' added successfully"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


@routes.post("/api/accounts/import_string")
async def api_import_string_session(request):
    """
    Register an account from an already-existing Telethon StringSession —
    no phone/code/2FA step at all. Verifies the session is actually
    authorized before saving it, and auto-fills the phone number from
    Telegram itself so the user doesn't have to type it.
    """
    try:
        body           = await request.json()
        name           = body.get("name", "").strip()
        api_id         = int(body.get("api_id", 0))
        api_hash       = body.get("api_hash", "").strip()
        session_string = body.get("session_string", "").strip()
        if not all([name, api_id, api_hash, session_string]):
            return web.json_response({"error": "All fields required"}, status=400)
        if name in data["accounts"]:
            return web.json_response({"error": f"Account '{name}' already exists"}, status=400)

        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        try:
            await client.connect()
        except AuthKeyError:
            return web.json_response({"error": "Invalid string session (bad auth key)"}, status=400)

        if not await client.is_user_authorized():
            await client.disconnect()
            return web.json_response({"error": "This string session is not authorized (expired or logged out)"}, status=400)

        try:
            me = await client.get_me()
            phone = f"+{me.phone}" if me and me.phone else ""
        except Exception:
            phone = ""

        data["accounts"][name] = {
            "api_id": api_id, "api_hash": api_hash,
            "phone": phone, "session_string": session_string,
            "configs": {},
        }
        if not data.get("active_account"):
            data["active_account"] = name
        save_data(data)
        register_cli_handlers(client, name)
        clients[name] = client
        logger.info(f"✓ Account '{name}' imported via string session.")
        return web.json_response({"ok": True, "message": f"Account '{name}' imported successfully", "phone": phone})
    except FloodWaitError as e:
        return web.json_response({"error": f"FloodWait: try again in {e.seconds}s"}, status=429)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


@routes.post("/api/accounts/{name}/switch")
async def api_switch_account(request):
    name = request.match_info["name"]
    if name not in data["accounts"]:
        return web.json_response({"error": "Account not found"}, status=404)
    data["active_account"] = name
    save_data(data)
    return web.json_response({"ok": True})


@routes.delete("/api/accounts/{name}")
async def api_delete_account(request):
    name = request.match_info["name"]
    if name not in data["accounts"]:
        return web.json_response({"error": "Account not found"}, status=404)
    del data["accounts"][name]
    if data.get("active_account") == name:
        remaining = list(data["accounts"].keys())
        data["active_account"] = remaining[0] if remaining else None
    save_data(data)
    c = clients.pop(name, None)
    if c:
        try:
            await c.disconnect()
        except Exception:
            pass
    return web.json_response({"ok": True})


@routes.get("/api/accounts/{name}/configs")
async def api_get_configs(request):
    name = request.match_info["name"]
    account = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    configs = account.get("configs", {})
    result = []
    for cid, cfg in configs.items():
        cfg = migrate_config(cfg)
        mode = get_mode(cfg)
        key = (name, cid)
        running = key in mode3_tasks and not mode3_tasks[key].done()
        result.append({"id": cid, **cfg, "mode_label": mode.LABEL,
                        "supports_refill": mode.supports_refill(),
                        "supports_start_stop": mode.supports_start_stop(),
                        "is_running": running})
    return web.json_response(result)


@routes.post("/api/accounts/{name}/configs")
async def api_add_config(request):
    name = request.match_info["name"]
    account = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    try:
        body    = await request.json()
        mode_id = int(body.get("mode", DEFAULT_MODE_ID))
        mode    = MODE_REGISTRY.get(mode_id)
        if not mode:
            return web.json_response({"error": f"Unknown mode: {mode_id}"}, status=400)
        cfg = mode.build_config(body)
        configs = account.setdefault("configs", {})
        new_id  = str(next_config_id(account))
        configs[new_id] = cfg
        save_data(data)
        return web.json_response({"ok": True, "id": new_id})
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


@routes.put("/api/accounts/{name}/configs/{config_id}")
async def api_update_config(request):
    name      = request.match_info["name"]
    config_id = request.match_info["config_id"]
    account   = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    configs = account.get("configs", {})
    if config_id not in configs:
        return web.json_response({"error": "Config not found"}, status=404)
    try:
        body    = await request.json()
        mode_id = int(body.get("mode", DEFAULT_MODE_ID))
        mode    = MODE_REGISTRY.get(mode_id)
        if not mode:
            return web.json_response({"error": f"Unknown mode: {mode_id}"}, status=400)
        cfg = mode.build_config(body)
        # Stop mode3 if mode changed or config updated
        key = (name, config_id)
        task = mode3_tasks.get(key)
        if task and not task.done():
            task.cancel()
        configs[config_id] = cfg
        save_data(data)
        return web.json_response({"ok": True})
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


@routes.delete("/api/accounts/{name}/configs/{config_id}")
async def api_delete_config(request):
    name      = request.match_info["name"]
    config_id = request.match_info["config_id"]
    account   = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    configs = account.get("configs", {})
    if config_id not in configs:
        return web.json_response({"error": "Config not found"}, status=404)
    key = (name, config_id)
    task = mode3_tasks.get(key)
    if task and not task.done():
        task.cancel()
    del configs[config_id]
    save_data(data)
    return web.json_response({"ok": True})


@routes.post("/api/accounts/{name}/configs/{config_id}/refill")
async def api_refill_config(request):
    name      = request.match_info["name"]
    config_id = request.match_info["config_id"]
    c = clients.get(name)
    if not c:
        return web.json_response({"error": "Client not connected"}, status=400)
    try:
        if not await c.is_user_authorized():
            return web.json_response({"error": "Account not authorized"}, status=400)
    except Exception:
        return web.json_response({"error": "Client error"}, status=500)
    ok, msg = await do_refill(c, name, config_id)
    if ok:
        return web.json_response({"ok": True, "message": msg})
    return web.json_response({"error": msg}, status=400)


@routes.post("/api/accounts/{name}/configs/{config_id}/start")
async def api_start_config(request):
    name      = request.match_info["name"]
    config_id = request.match_info["config_id"]
    account   = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    cfg  = migrate_config(account.get("configs", {}).get(config_id, {}))
    if not cfg:
        return web.json_response({"error": "Config not found"}, status=404)
    mode = get_mode(cfg)
    if not mode.supports_start_stop():
        return web.json_response({"error": f"Mode '{mode.LABEL}' does not support start/stop"}, status=400)
    c = clients.get(name)
    if not c:
        return web.json_response({"error": "Client not connected"}, status=400)
    try:
        peer = await c.get_input_entity(cfg["chat_id"])
    except Exception as e:
        return web.json_response({"error": f"Could not resolve chat: {e}"}, status=400)
    ok, msg = await mode.start(c, cfg, peer, name, config_id)
    if ok:
        return web.json_response({"ok": True, "message": msg})
    return web.json_response({"error": msg}, status=400)


@routes.post("/api/accounts/{name}/configs/{config_id}/stop")
async def api_stop_config(request):
    name      = request.match_info["name"]
    config_id = request.match_info["config_id"]
    account   = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    cfg  = migrate_config(account.get("configs", {}).get(config_id, {}))
    if not cfg:
        return web.json_response({"error": "Config not found"}, status=404)
    mode = get_mode(cfg)
    if not mode.supports_start_stop():
        return web.json_response({"error": f"Mode '{mode.LABEL}' does not support start/stop"}, status=400)
    ok, msg = await mode.stop(name, config_id)
    if ok:
        return web.json_response({"ok": True, "message": msg})
    return web.json_response({"error": msg}, status=400)


@routes.get("/api/accounts/{name}/configs/{config_id}/scheduled_count")
async def api_scheduled_count(request):
    name      = request.match_info["name"]
    config_id = request.match_info["config_id"]
    account   = data["accounts"].get(name)
    if not account:
        return web.json_response({"error": "Account not found"}, status=404)
    cfg = migrate_config(account.get("configs", {}).get(config_id, {}))
    if not cfg:
        return web.json_response({"error": "Config not found"}, status=404)
    mode = get_mode(cfg)
    c = clients.get(name)
    if not c:
        return web.json_response({"count": 0, "authorized": False})

    if not mode.scheduled_count_uses_telegram_queue():
        key     = (name, config_id)
        running = key in mode3_tasks and not mode3_tasks[key].done()
        return web.json_response({"count": None, "authorized": True, "running": running})

    try:
        if not await c.is_user_authorized():
            return web.json_response({"count": 0, "authorized": False})
        peer = await c.get_input_entity(cfg["chat_id"])
        ids  = await fetch_all_scheduled_ids(c, peer)
        return web.json_response({"count": len(ids), "authorized": True})
    except Exception as e:
        return web.json_response({"count": 0, "authorized": False, "error": str(e)})


@routes.get("/")
async def serve_ui(request):
    ui_path = Path("web_interface.html")
    if not ui_path.exists():
        return web.Response(text="web_interface.html not found", status=404)
    return web.FileResponse(ui_path)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 50)
    logger.info("  Telegram Scheduled Sender")
    logger.info(f"  Web UI → http://localhost:{WEB_PORT}")
    logger.info("=" * 50)

    await start_all_clients()

    @web.middleware
    async def cors_middleware(request, handler):
        if request.method == "OPTIONS":
            response = web.Response()
        else:
            try:
                response = await handler(request)
            except web.HTTPException as ex:
                response = web.json_response({"error": ex.reason}, status=ex.status)
        response.headers["Access-Control-Allow-Origin"]  = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response

    app = web.Application(middlewares=[cors_middleware])
    app.add_routes(routes)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    logger.info(f"✓ Web server running on http://localhost:{WEB_PORT}")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down...")
    finally:
        for task in mode3_tasks.values():
            if not task.done():
                task.cancel()
        await runner.cleanup()
        for c in clients.values():
            try:
                await c.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())