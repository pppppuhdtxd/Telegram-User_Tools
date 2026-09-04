"""
auto_clicker.py

Telegram Userbot inline-button auto-clicker with full web management.

Architecture (high-throughput, parallel, edit-aware):
- Per-session job queue + configurable worker pool for concurrent processing.
- Synchronous pre-filter in the Telethon handler (no await) drops irrelevant
  messages instantly: only button messages from target bots are enqueued.
- Rules are indexed per session and pre-compiled (numeric ID sets, regex,
  normalized keywords) so matching is O(candidates), not O(all rules).
- Button-signature tracking: when a button changes or disappears and then
  returns, a FRESH click task is spawned with a RESET timeout. Stale tasks
  are stopped immediately.
- MessageEdited is a re-evaluator: button edits are re-matched so a returning
  button is clicked again without pause.
- All-chats mode monitors ONLY groups (basic groups + supergroups), excluding
  broadcast channels and private chats, with a two-layer fast filter.
- Self-targeting filter (mention / reply / my-id-in-button-data) prevents
  clicking buttons meant for other users in shared groups.
- The task reconciliation block is fully synchronous (no await) so it is
  atomic under asyncio's single-threaded model — no race conditions, no
  duplicate tasks, no locks required.

Features:
- Full account management from Web UI
- Telethon login wizard via REST API
- Rule management via REST API
- Multiple target bots / chats per rule, "all chats" (groups only) mode
- Keyword button matching: contains / exact / regex (emoji-safe)
- Ultra-low-latency click pipeline:
    - reaction delay, burst mode, configurable refresh rate
    - skip-first-refresh uses the event message directly
- Global settings: workers_per_session, quiet_mode, click concurrency cap
- Per-session click statistics
- Live logs via SSE

Dependencies:
    pip install telethon aiohttp
"""

import asyncio
import json
import logging
import os
import re
import sys
import tempfile
import time
import unicodedata
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    AuthKeyError,
    FloodWaitError,
    MessageIdInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.tl.types import Chat, Channel

from aiohttp import web


# =============================================================================
# CONSTANTS
# =============================================================================

WEB_HOST = "127.0.0.1"
WEB_PORT = 8082

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "auto_clicker_config.json"
HTML_FILE = BASE_DIR / "index.html"
SESSIONS_DIR = BASE_DIR / "sessions"

DEFAULT_TIMEOUT = 10.0
DEFAULT_CPS = 2.0
MAX_CPS = 20.0
MIN_CPS = 0.1
MAX_TIMEOUT = 86400.0

DEFAULT_REACTION_DELAY_MS = 0.0
DEFAULT_SKIP_FIRST_REFRESH = True
DEFAULT_REFRESH_EVERY_N_CLICKS = 5
DEFAULT_BURST_COUNT = 0

MAX_REACTION_DELAY_MS = 60000.0
MAX_REFRESH_EVERY_N_CLICKS = 1000
MAX_BURST_COUNT = 50

PENDING_LOGIN_TTL = 600
SESSION_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

MATCH_MODES = ("contains", "exact", "regex")
DEFAULT_MATCH_MODE = "contains"

CHAT_MODE_ALL = "all"
CHAT_MODE_LIST = "list"
ALL_CHATS_TOKEN = "*"

DEFAULT_SETTINGS: Dict[str, Any] = {
    "workers_per_session": 16,
    "quiet_mode": False,
    "max_concurrent_clicks_per_session": 0,
}

MIN_WORKERS = 1
MAX_WORKERS = 128
MAX_CONCURRENT_CLICKS_CAP = 1000


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger("auto_clicker")

logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("aiohttp.server").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)


LOG_BUFFER: deque = deque(maxlen=500)
SSE_SUBSCRIBERS: List[asyncio.Queue] = []


class UILogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            LOG_BUFFER.append(line)
            for queue in SSE_SUBSCRIBERS:
                try:
                    queue.put_nowait(line)
                except asyncio.QueueFull:
                    pass
        except Exception:
            pass


_ui_handler = UILogHandler()
_ui_handler.setFormatter(
    logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S")
)
logging.getLogger().addHandler(_ui_handler)


def _log_info(msg: str, *args: Any) -> None:
    """Info-level log that downgrades to debug when quiet_mode is enabled."""
    if _settings.get("quiet_mode"):
        log.debug(msg, *args)
    else:
        log.info(msg, *args)


# =============================================================================
# JSON / GENERAL HELPERS
# =============================================================================

def _json_ok(**data: Any) -> web.Response:
    payload: Dict[str, Any] = {"ok": True}
    payload.update(data)
    return web.json_response(payload)


def _json_error(message: str, status: int = 400, **data: Any) -> web.Response:
    payload: Dict[str, Any] = {"ok": False, "error": message}
    payload.update(data)
    return web.json_response(payload, status=status)


async def _read_json(request: web.Request) -> Optional[Dict[str, Any]]:
    try:
        data = await request.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _to_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on", "enabled"):
        return True
    if text in ("0", "false", "no", "off", "disabled"):
        return False
    return default


def _rpc_message(exc: Exception) -> str:
    message = getattr(exc, "message", None) or str(exc)
    return str(message).upper()


def _clean_str_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    return [str(item).strip() for item in items if str(item).strip()]


def _is_valid_session_name(name: str) -> bool:
    return bool(SESSION_NAME_RE.match(name))


def _sanitize_session_name(raw_name: Any) -> str:
    name = str(raw_name or "").strip().replace(" ", "_")
    return name if SESSION_NAME_RE.match(name) else ""


# =============================================================================
# REGEX CACHE
# =============================================================================

_regex_cache: Dict[Tuple[str, bool], Optional[re.Pattern]] = {}


def _get_compiled_regex(pattern: str, case_sensitive: bool) -> Optional[re.Pattern]:
    key = (pattern, case_sensitive)
    if key not in _regex_cache:
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            _regex_cache[key] = re.compile(pattern, flags)
        except re.error:
            _regex_cache[key] = None
    return _regex_cache[key]


# =============================================================================
# SETTINGS
# =============================================================================

def _normalize_settings(raw: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(DEFAULT_SETTINGS)
    try:
        out["workers_per_session"] = max(
            MIN_WORKERS, min(MAX_WORKERS, int(raw.get("workers_per_session", DEFAULT_SETTINGS["workers_per_session"])))
        )
    except (TypeError, ValueError):
        pass
    out["quiet_mode"] = _to_bool(raw.get("quiet_mode"), False)
    try:
        out["max_concurrent_clicks_per_session"] = max(
            0, min(MAX_CONCURRENT_CLICKS_CAP, int(raw.get("max_concurrent_clicks_per_session", 0)))
        )
    except (TypeError, ValueError):
        pass
    return out


# =============================================================================
# RULE NORMALIZATION / VALIDATION
# =============================================================================

def _normalize_rule(rule: Dict[str, Any], now: Optional[int] = None) -> Dict[str, Any]:
    if now is None:
        now = int(time.time())

    rule_id = str(rule.get("id") or uuid.uuid4().hex[:12])
    session = str(rule.get("session", "")).strip()

    # Bots
    bot_ids = _clean_str_list(rule.get("bot_ids"))
    if not bot_ids:
        legacy_bot = str(rule.get("bot_id", "")).strip()
        if legacy_bot:
            bot_ids = _clean_str_list(legacy_bot)

    # Chats
    chat_ids = _clean_str_list(rule.get("chat_ids"))
    legacy_chat = str(rule.get("chat_id", "")).strip()
    if not chat_ids and legacy_chat:
        chat_ids = _clean_str_list(legacy_chat)

    raw_chat_mode = str(rule.get("chat_mode", "")).strip().lower()
    if raw_chat_mode in (CHAT_MODE_ALL, CHAT_MODE_LIST):
        chat_mode = raw_chat_mode
    elif ALL_CHATS_TOKEN in chat_ids:
        chat_mode = CHAT_MODE_ALL
    else:
        chat_mode = CHAT_MODE_LIST

    if chat_mode == CHAT_MODE_ALL:
        chat_ids = [item for item in chat_ids if item != ALL_CHATS_TOKEN]

    # Button keyword / match mode
    button_keyword = str(rule.get("button_keyword", "")).strip()
    migrated_exact = False
    if not button_keyword:
        legacy_button_text = str(rule.get("button_text", "")).strip()
        if legacy_button_text:
            button_keyword = legacy_button_text
            migrated_exact = True

    raw_match_mode = str(rule.get("match_mode", "")).strip().lower()
    if raw_match_mode in MATCH_MODES:
        match_mode = raw_match_mode
    elif migrated_exact:
        match_mode = "exact"
    else:
        match_mode = DEFAULT_MATCH_MODE

    case_sensitive = _to_bool(rule.get("case_sensitive"), False)

    # Numeric fields
    try:
        cps = float(rule.get("clicks_per_second", DEFAULT_CPS))
    except (TypeError, ValueError):
        cps = DEFAULT_CPS
    try:
        timeout = float(rule.get("timeout", DEFAULT_TIMEOUT))
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT
    cps = max(MIN_CPS, min(cps, MAX_CPS))
    timeout = max(1.0, min(timeout, MAX_TIMEOUT))

    # Performance fields
    try:
        reaction_delay_ms = float(rule.get("reaction_delay_ms", DEFAULT_REACTION_DELAY_MS))
    except (TypeError, ValueError):
        reaction_delay_ms = DEFAULT_REACTION_DELAY_MS
    reaction_delay_ms = max(0.0, min(reaction_delay_ms, MAX_REACTION_DELAY_MS))

    skip_first_refresh = _to_bool(rule.get("skip_first_refresh"), DEFAULT_SKIP_FIRST_REFRESH)

    try:
        refresh_every_n_clicks = int(rule.get("refresh_every_n_clicks", DEFAULT_REFRESH_EVERY_N_CLICKS))
    except (TypeError, ValueError):
        refresh_every_n_clicks = DEFAULT_REFRESH_EVERY_N_CLICKS
    refresh_every_n_clicks = max(0, min(refresh_every_n_clicks, MAX_REFRESH_EVERY_N_CLICKS))

    try:
        burst_count = int(rule.get("burst_count", DEFAULT_BURST_COUNT))
    except (TypeError, ValueError):
        burst_count = DEFAULT_BURST_COUNT
    burst_count = max(0, min(burst_count, MAX_BURST_COUNT))

    # Self-targeting fields
    require_mention = _to_bool(rule.get("require_mention"), False)
    require_reply = _to_bool(rule.get("require_reply"), False)
    require_my_id_in_data = _to_bool(rule.get("require_my_id_in_data"), False)

    raw_logic = str(rule.get("self_filter_logic", "and")).strip().lower()
    self_filter_logic = raw_logic if raw_logic in ("and", "or") else "and"

    click_all_matches = _to_bool(rule.get("click_all_matches"), False)

    try:
        created_at = int(rule.get("created_at") or now)
    except (TypeError, ValueError):
        created_at = now

    return {
        "id": rule_id,
        "session": session,
        "bot_ids": bot_ids,
        "chat_mode": chat_mode,
        "chat_ids": chat_ids,
        "button_keyword": button_keyword,
        "match_mode": match_mode,
        "case_sensitive": case_sensitive,
        "clicks_per_second": cps,
        "timeout": timeout,
        "reaction_delay_ms": reaction_delay_ms,
        "skip_first_refresh": skip_first_refresh,
        "refresh_every_n_clicks": refresh_every_n_clicks,
        "burst_count": burst_count,
        "require_mention": require_mention,
        "require_reply": require_reply,
        "require_my_id_in_data": require_my_id_in_data,
        "self_filter_logic": self_filter_logic,
        "click_all_matches": click_all_matches,
        "enabled": _to_bool(rule.get("enabled"), True),
        "created_at": created_at,
        "updated_at": now,
    }


def _validate_rule(rule: Dict[str, Any]) -> Tuple[bool, str]:
    if not _is_valid_session_name(rule.get("session", "")):
        return False, "invalid_session"
    if not rule.get("bot_ids"):
        return False, "bot_ids_required"
    if rule.get("chat_mode") == CHAT_MODE_LIST and not rule.get("chat_ids"):
        return False, "chat_ids_required"
    if not rule.get("button_keyword"):
        return False, "button_keyword_required"

    match_mode = rule.get("match_mode")
    if match_mode not in MATCH_MODES:
        return False, "invalid_match_mode"

    if match_mode == "regex":
        flags = 0 if rule.get("case_sensitive") else re.IGNORECASE
        try:
            re.compile(rule["button_keyword"], flags)
        except re.error as exc:
            return False, f"invalid_regex ({exc})"

    return True, ""


# =============================================================================
# MATCHING HELPERS
# =============================================================================

def _normalize_match_text(value: Any, case_sensitive: bool) -> str:
    text = unicodedata.normalize("NFC", str(value or ""))
    text = text.replace("\ufe0f", "")
    if not case_sensitive:
        text = text.casefold()
    return text


def _button_matches(button_text: Any, keyword: str, match_mode: str, case_sensitive: bool) -> bool:
    text = str(button_text or "")
    key = str(keyword or "")
    if not key:
        return False
    if match_mode == "exact":
        return _normalize_match_text(text, case_sensitive) == _normalize_match_text(key, case_sensitive)
    if match_mode == "contains":
        return _normalize_match_text(key, case_sensitive) in _normalize_match_text(text, case_sensitive)
    if match_mode == "regex":
        compiled = _get_compiled_regex(key, case_sensitive)
        if compiled is None:
            return False
        return compiled.search(text) is not None
    return False


def _extract_raw(btn: Any) -> Any:
    """Return the raw TL button from a MessageButton wrapper, or the button itself."""
    raw = getattr(btn, "button", None)
    return raw if raw is not None else btn


def _button_signature(raw_btn: Any) -> str:
    """
    Compute a stable signature for a button based on its callback data.
    Used to detect when a button changes or returns after being removed.
    """
    data = getattr(raw_btn, "data", None)
    if isinstance(data, (bytes, bytearray)) and data:
        return bytes(data).hex()
    if bool(getattr(raw_btn, "game", False)):
        return "game:" + str(getattr(raw_btn, "text", ""))
    return "text:" + str(getattr(raw_btn, "text", ""))


def _find_buttons_by_keyword(
    buttons: Any,
    keyword: str,
    match_mode: str,
    case_sensitive: bool,
    click_all: bool = False,
) -> List[Any]:
    """
    Walk a 2-D button grid (raw TL buttons or MessageButton wrappers) and
    return matching buttons. Works with both because both expose `.text`.
    """
    found: List[Any] = []
    if not buttons:
        return found
    for row in buttons:
        if not row:
            continue
        for btn in row:
            text = getattr(btn, "text", "")
            if _button_matches(text, keyword, match_mode, case_sensitive):
                found.append(btn)
                if not click_all:
                    return found
    return found


def _find_matches(buttons: Any, rule: Dict[str, Any]) -> List[Tuple[Any, str]]:
    """
    Find matching buttons for a rule and return (raw_button, signature) pairs.
    Respects click_all_matches.
    """
    keyword = rule.get("button_keyword", "")
    match_mode = rule.get("match_mode", DEFAULT_MATCH_MODE)
    case_sensitive = bool(rule.get("case_sensitive", False))
    click_all = bool(rule.get("click_all_matches", False))

    found: List[Tuple[Any, str]] = []
    if not buttons:
        return found

    for row in buttons:
        if not row:
            continue
        for btn in row:
            text = getattr(btn, "text", "")
            if _button_matches(text, keyword, match_mode, case_sensitive):
                raw = _extract_raw(btn)
                found.append((raw, _button_signature(raw)))
                if not click_all:
                    return found
    return found


def _sender_matches_one(bot_id_raw: str, sender: Any) -> bool:
    raw = str(bot_id_raw).strip()
    if not raw:
        return False
    sender_id = getattr(sender, "id", None)
    if raw.lstrip("-").isdigit():
        try:
            return sender_id == int(raw.lstrip("-"))
        except (TypeError, ValueError):
            return False
    sender_username = (getattr(sender, "username", None) or "").lower().lstrip("@")
    target_username = raw.lower().lstrip("@")
    return bool(sender_username) and sender_username == target_username


def _sender_matches_any(bot_ids: List[str], sender: Any) -> bool:
    return any(_sender_matches_one(bot_id, sender) for bot_id in bot_ids)


def _chat_id_candidates(raw: str) -> Set[int]:
    candidates: Set[int] = set()
    raw = str(raw).strip()
    if not raw.lstrip("-").isdigit():
        return candidates
    digits = raw.lstrip("-")
    try:
        candidates.add(int(digits))
    except ValueError:
        pass
    if raw.startswith("-100") and len(digits) > 3:
        try:
            candidates.add(int(digits[3:]))
        except ValueError:
            pass
    elif digits.startswith("100") and len(digits) > 10:
        try:
            candidates.add(int(digits[3:]))
        except ValueError:
            pass
    return candidates


def _chat_matches_one(chat_id_raw: str, chat: Any) -> bool:
    raw = str(chat_id_raw).strip()
    if not raw:
        return False
    chat_id = getattr(chat, "id", None)
    if raw.lstrip("-").isdigit():
        return chat_id in _chat_id_candidates(raw)
    chat_username = (getattr(chat, "username", None) or "").lower().lstrip("@")
    target_username = raw.lower().lstrip("@")
    return bool(chat_username) and chat_username == target_username


def _chat_matches_rule(rule: Dict[str, Any], chat: Any) -> bool:
    chat_mode = str(rule.get("chat_mode", CHAT_MODE_LIST)).strip().lower()
    if chat_mode == CHAT_MODE_ALL:
        return _is_group_entity(chat)
    return any(_chat_matches_one(cid, chat) for cid in (rule.get("chat_ids") or []))


def _is_group_entity(chat: Any) -> bool:
    """
    True only for groups: basic groups (Chat) and supergroups (megagroup Channel).
    Excludes broadcast channels and private chats.
    """
    if chat is None:
        return False
    if isinstance(chat, Chat):
        return True
    if isinstance(chat, Channel):
        return bool(getattr(chat, "megagroup", False))
    return False


def _all_numeric(entries: Any) -> bool:
    if not entries:
        return False
    return all(str(e).lstrip("-").isdigit() for e in entries)


def _sender_numeric_set(bot_ids: List[str]) -> Set[int]:
    result: Set[int] = set()
    for x in bot_ids:
        s = str(x).lstrip("-")
        if s.isdigit():
            result.add(int(s))
    return result


def _chat_numeric_set(chat_ids: List[str]) -> Set[int]:
    result: Set[int] = set()
    for x in chat_ids:
        result.update(_chat_id_candidates(str(x)))
    return result


def _my_id_in_buttons(buttons: List[List[Any]], my_id: Optional[int]) -> bool:
    """Check if the account's user ID appears in any button's callback data."""
    if my_id is None:
        return False
    needle = str(my_id).encode("utf-8")
    for row in buttons:
        for btn in row:
            data = getattr(btn, "data", None)
            if isinstance(data, (bytes, bytearray)) and needle in bytes(data):
                return True
    return False


def _self_filter_passes(rule: Dict[str, Any], job: "Job", my_id: Optional[int]) -> bool:
    """
    Evaluate the self-targeting filter. If no flag is enabled, passes always.
    """
    flags: List[bool] = []
    if rule.get("require_mention"):
        flags.append(bool(job.mentioned))
    if rule.get("require_reply"):
        flags.append(bool(job.has_reply))
    if rule.get("require_my_id_in_data"):
        flags.append(_my_id_in_buttons(job.buttons, my_id))

    if not flags:
        return True

    logic = str(rule.get("self_filter_logic", "and")).lower()
    return any(flags) if logic == "or" else all(flags)


# =============================================================================
# COMPILED RULES & JOB SNAPSHOT
# =============================================================================

@dataclass
class CompiledRule:
    rule: Dict[str, Any]
    session: str
    bot_numeric: Optional[Set[int]]
    bot_has_username: bool
    chat_mode: str
    chat_numeric: Optional[Set[int]]
    chat_has_username: bool
    regex: Optional[re.Pattern]
    keyword_norm_cs: str
    keyword_norm_ci: str


@dataclass
class Job:
    """Lightweight, self-contained snapshot of a message for worker processing."""
    session: str
    chat_id: int
    message_id: int
    sender_id: Optional[int]
    is_private: bool
    is_group: Optional[bool]
    mentioned: bool
    has_reply: bool
    buttons: List[List[Any]]  # raw TL buttons


def _compile_rule(rule: Dict[str, Any]) -> CompiledRule:
    bot_ids = rule.get("bot_ids") or []
    bot_numeric = _sender_numeric_set(bot_ids) if _all_numeric(bot_ids) else None
    bot_has_username = not _all_numeric(bot_ids)

    chat_mode = rule.get("chat_mode", CHAT_MODE_LIST)
    chat_ids = rule.get("chat_ids") or []
    chat_numeric = _chat_numeric_set(chat_ids) if _all_numeric(chat_ids) else None
    chat_has_username = (chat_mode == CHAT_MODE_LIST) and not _all_numeric(chat_ids)

    regex = None
    if rule.get("match_mode") == "regex":
        regex = _get_compiled_regex(rule.get("button_keyword", ""), bool(rule.get("case_sensitive", False)))

    keyword = rule.get("button_keyword", "")

    return CompiledRule(
        rule=rule,
        session=rule.get("session", ""),
        bot_numeric=bot_numeric,
        bot_has_username=bot_has_username,
        chat_mode=chat_mode,
        chat_numeric=chat_numeric,
        chat_has_username=chat_has_username,
        regex=regex,
        keyword_norm_cs=_normalize_match_text(keyword, True),
        keyword_norm_ci=_normalize_match_text(keyword, False),
    )


# =============================================================================
# GLOBAL STATE
# =============================================================================

_config_lock = asyncio.Lock()
_config_cache: Dict[str, Any] = {"rules": []}
_settings: Dict[str, Any] = dict(DEFAULT_SETTINGS)

_rule_index: Dict[str, List[CompiledRule]] = {}
_session_meta: Dict[str, Dict[str, Any]] = {}

_clients: Dict[str, TelegramClient] = {}
_account_meta: Dict[str, Dict[str, Any]] = {}

# task_key -> list of (button_signature, stop_event, task)
_active_tasks: Dict[Tuple[str, int, int], List[Tuple[str, asyncio.Event, asyncio.Task]]] = {}

_job_queues: Dict[str, asyncio.Queue] = {}
_workers: Dict[str, List[asyncio.Task]] = {}
_session_stats: Dict[str, Dict[str, int]] = {}
_click_semaphores: Dict[str, asyncio.Semaphore] = {}

_account_lock = asyncio.Lock()


@dataclass
class PendingLogin:
    session_name: str
    api_id: int
    api_hash: str
    phone: str
    client: TelegramClient
    phone_code_hash: str = ""
    state: str = "code_required"
    save_env: bool = True
    last_code: str = ""
    created_at: float = field(default_factory=time.time)


_pending_logins: Dict[str, PendingLogin] = {}


# =============================================================================
# RULE INDEX & SESSION META
# =============================================================================

def _rebuild_rule_index() -> None:
    """Rebuild the per-session rule index and fast-filter metadata."""
    global _rule_index, _session_meta

    rules = _config_cache.get("rules", [])
    index: Dict[str, List[CompiledRule]] = {}

    for rule in rules:
        if not rule.get("enabled", True):
            continue
        session = rule.get("session", "")
        if not session:
            continue
        compiled = _compile_rule(rule)
        index.setdefault(session, []).append(compiled)

    meta: Dict[str, Dict[str, Any]] = {}
    for session, compiled_rules in index.items():
        bot_set: Set[int] = set()
        has_username = False
        for c in compiled_rules:
            if c.bot_has_username:
                has_username = True
            elif c.bot_numeric:
                bot_set |= c.bot_numeric

        all_only = all(c.chat_mode == CHAT_MODE_ALL for c in compiled_rules)

        meta[session] = {
            "bot_set": bot_set,
            "has_username_bot": has_username,
            "all_only": all_only,
            "has_rules": True,
        }

    _rule_index = index
    _session_meta = meta


# =============================================================================
# CONFIG LAYER
# =============================================================================

DEFAULT_CONFIG: Dict[str, Any] = {"rules": []}


def _save_config_sync(data: Dict[str, Any]) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=CONFIG_FILE.parent, prefix=".auto_clicker_tmp_")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
        os.replace(tmp_path, CONFIG_FILE)
        log.debug("Config saved atomically to %s", CONFIG_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _load_config_sync() -> Dict[str, Any]:
    global _settings

    if not CONFIG_FILE.exists():
        log.info("No config file found. Creating default config.")
        _save_config_sync(DEFAULT_CONFIG)
        _settings = dict(DEFAULT_SETTINGS)
        return json.loads(json.dumps(DEFAULT_CONFIG))

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Config file is corrupted or unreadable (%s). Using defaults.", exc)
        _settings = dict(DEFAULT_SETTINGS)
        return json.loads(json.dumps(DEFAULT_CONFIG))

    if not isinstance(data, dict):
        log.error("Config root is not a JSON object. Using defaults.")
        _settings = dict(DEFAULT_SETTINGS)
        return json.loads(json.dumps(DEFAULT_CONFIG))

    if not isinstance(data.get("rules"), list):
        log.error("Config 'rules' is not a list. Using defaults.")
        _settings = dict(DEFAULT_SETTINGS)
        return json.loads(json.dumps(DEFAULT_CONFIG))

    settings_raw = data.get("settings")
    if isinstance(settings_raw, dict):
        _settings = _normalize_settings(settings_raw)
    else:
        _settings = dict(DEFAULT_SETTINGS)

    now = int(time.time())
    normalized_rules: List[Dict[str, Any]] = []

    for index, rule in enumerate(data["rules"]):
        if not isinstance(rule, dict):
            log.warning("Rule #%d is not an object and was skipped.", index + 1)
            continue
        normalized = _normalize_rule(rule, now)
        ok, error = _validate_rule(normalized)
        if not ok:
            log.warning("Rule #%d failed validation (%s) and was kept disabled.", index + 1, error)
            normalized["enabled"] = False
        normalized_rules.append(normalized)

    data["rules"] = normalized_rules
    data["settings"] = _settings

    log.info("Loaded config with %d rule(s).", len(normalized_rules))
    return data


async def load_config() -> Dict[str, Any]:
    return _config_cache


async def save_config(data: Dict[str, Any]) -> None:
    global _config_cache
    async with _config_lock:
        data["settings"] = _settings
        _save_config_sync(data)
        _config_cache = data
        _rebuild_rule_index()


# =============================================================================
# ACCOUNT STORE (accounts.json — replaces the old per-session .session/.env files)
# =============================================================================
#
# Every account's credentials (api_id, api_hash, phone, and its Telethon
# StringSession) live in one JSON file, keyed by session_name — the same
# identifier rules/_clients/_workers already use everywhere else in this
# file. No .session or .env files are read or written by any code path
# below, including a brand-new full login: TelegramClient is always built
# from StringSession(...), and the resulting string is written straight
# into ACCOUNTS_FILE right after a successful login.
#
# accounts.json shape:
#   { "session_name": {"api_id": int, "api_hash": str, "phone": str,
#                       "session_string": str}, ... }

ACCOUNTS_FILE = BASE_DIR / "accounts.json"

_accounts_cache: Dict[str, Dict[str, Any]] = {}


def _load_accounts_sync() -> Dict[str, Dict[str, Any]]:
    if not ACCOUNTS_FILE.exists():
        return {}
    try:
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return raw if isinstance(raw, dict) else {}
    except (json.JSONDecodeError, OSError) as exc:
        log.error("accounts.json is corrupted or unreadable (%s). Starting with no accounts.", exc)
        return {}


def _save_accounts_sync() -> None:
    ACCOUNTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=ACCOUNTS_FILE.parent, prefix=".accounts_tmp_")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            json.dump(_accounts_cache, fh, indent=2, ensure_ascii=False)
        os.replace(tmp_path, ACCOUNTS_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _account_record(session_name: str) -> Optional[Dict[str, Any]]:
    return _accounts_cache.get(session_name)


def _upsert_account_record(
    session_name: str,
    api_id: int,
    api_hash: str,
    phone: str,
    session_string: str,
) -> None:
    _accounts_cache[session_name] = {
        "api_id": api_id,
        "api_hash": api_hash,
        "phone": phone,
        "session_string": session_string,
    }
    _save_accounts_sync()


def _delete_account_record(session_name: str) -> None:
    if session_name in _accounts_cache:
        del _accounts_cache[session_name]
        _save_accounts_sync()


def _legacy_session_path(session_name: str) -> Path:
    return SESSIONS_DIR / f"{session_name}.session"


def _legacy_env_path(session_name: str) -> Path:
    return SESSIONS_DIR / f"{session_name}.env"


def _read_legacy_env_file(path: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not path.exists():
        return result
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            result[key.strip()] = value.strip().strip('"').strip("'")
    except OSError as exc:
        log.warning("Could not read %s: %s", path, exc)
    return result


def _delete_legacy_files(session_name: str) -> None:
    base = SESSIONS_DIR / session_name
    paths = [
        base.with_suffix(".session"),
        base.with_suffix(".env"),
        base.with_suffix(".session-journal"),
        base.with_suffix(".session-wal"),
        base.with_suffix(".session-shm"),
    ]
    for path in paths:
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            log.warning("Could not delete %s: %s", path, exc)


async def _migrate_legacy_sessions() -> None:
    """
    One-time upgrade path for accounts created by older versions of this
    script, which stored a SQLite session + .env credentials pair per
    account under sessions/. For each legacy .session file that has no
    matching accounts.json entry yet, connect with it, derive the
    equivalent StringSession, save it into accounts.json, and delete the
    old files. Safe to call on every startup — a no-op once migrated.
    """
    if not SESSIONS_DIR.is_dir():
        return
    for session_file in sorted(SESSIONS_DIR.glob("*.session")):
        session_name = session_file.stem
        if session_name in _accounts_cache:
            continue

        env_data = _read_legacy_env_file(_legacy_env_path(session_name))
        raw_api_id = env_data.get("TG_API_ID")
        raw_api_hash = env_data.get("TG_API_HASH")
        if not raw_api_id or not raw_api_hash:
            log.warning(
                "Legacy session '%s' has no matching .env credentials — skipping migration.",
                session_name,
            )
            continue
        try:
            api_id = int(str(raw_api_id).strip())
        except (TypeError, ValueError):
            log.warning("Legacy session '%s' has an invalid TG_API_ID — skipping migration.", session_name)
            continue
        api_hash = str(raw_api_hash).strip()

        legacy_client = TelegramClient(str(session_file.with_suffix("")), api_id, api_hash)
        try:
            await legacy_client.connect()
            if not await legacy_client.is_user_authorized():
                log.warning("Legacy session '%s' isn't authorized — leaving it as-is.", session_name)
                continue
            me = await legacy_client.get_me()
            phone = f"+{me.phone}" if me and me.phone else ""
            session_string = StringSession.save(legacy_client.session)
            _upsert_account_record(session_name, api_id, api_hash, phone, session_string)
            log.info("✓ Migrated '%s' from legacy .session/.env files to accounts.json.", session_name)
        except Exception as exc:
            log.error("Could not migrate legacy session '%s': %s", session_name, exc)
            continue
        finally:
            try:
                await legacy_client.disconnect()
            except Exception:
                pass

        _delete_legacy_files(session_name)


def _build_client_for_account(record: Dict[str, Any]) -> TelegramClient:
    """
    Build a TelegramClient purely from an accounts.json record — always
    via StringSession, never a file. An empty/missing session_string
    (e.g. a brand-new record created mid-login) yields a fresh,
    unauthenticated in-memory session, exactly like an empty StringSession().
    """
    session = StringSession(record.get("session_string") or None)
    return TelegramClient(
        session,
        record["api_id"], record["api_hash"],
        connection_retries=5, retry_delay=3, flood_sleep_threshold=60,
    )


def _account_names() -> List[str]:
    names: Set[str] = set()
    names.update(_accounts_cache.keys())
    names.update(_clients.keys())
    names.update(_pending_logins.keys())
    return sorted(names)


def _accounts_snapshot() -> List[Dict[str, Any]]:
    accounts: List[Dict[str, Any]] = []
    for name in _account_names():
        client = _clients.get(name)
        meta = _account_meta.get(name, {})
        pending = _pending_logins.get(name)
        record = _account_record(name)
        connected = False
        if client is not None:
            try:
                connected = client.is_connected()
            except Exception:
                connected = False
        authorized = bool(meta.get("authorized")) or connected
        accounts.append({
            "name": name,
            "session_exists": bool(record and record.get("session_string")),
            "env_exists": bool(record and record.get("api_id") and record.get("api_hash")),
            "connected": connected,
            "authorized": authorized,
            "pending_state": pending.state if pending else None,
            "user": meta.get("user"),
            "phone": meta.get("phone") or (record.get("phone") if record else None),
        })

    return accounts


# =============================================================================
# STATS & CONCURRENCY HELPERS
# =============================================================================

def _bump_stat(session_name: str, key: str, amount: int = 1) -> None:
    stats = _session_stats.setdefault(session_name, {
        "matches": 0,
        "clicks_ok": 0,
        "clicks_fail": 0,
        "floodwaits": 0,
    })
    stats[key] = stats.get(key, 0) + amount


def _get_click_semaphore(session_name: str) -> Optional[asyncio.Semaphore]:
    max_concurrent = int(_settings.get("max_concurrent_clicks_per_session", 0))
    if max_concurrent <= 0:
        return None
    sem = _click_semaphores.get(session_name)
    if sem is None:
        sem = asyncio.Semaphore(max_concurrent)
        _click_semaphores[session_name] = sem
    return sem


# =============================================================================
# JOB BUILDING & DISPATCH (synchronous — runs inside Telethon handlers)
# =============================================================================

def _build_job(event: Any, session_name: str) -> Job:
    """Build a lightweight, self-contained job snapshot from an event."""
    msg = event.message
    raw_rows: List[List[Any]] = []
    reply_markup = getattr(msg, "reply_markup", None)
    if reply_markup is not None:
        for row in getattr(reply_markup, "rows", None) or []:
            raw_rows.append(list(getattr(row, "buttons", None) or []))

    return Job(
        session=session_name,
        chat_id=event.chat_id,
        message_id=msg.id,
        sender_id=event.sender_id,
        is_private=bool(event.is_private),
        is_group=event.is_group,  # may be None if entity not cached
        mentioned=bool(getattr(msg, "mentioned", False)),
        has_reply=getattr(msg, "reply_to", None) is not None,
        buttons=raw_rows,
    )


def _quick_dispatch(event: Any, session_name: str) -> None:
    """
    Synchronous pre-filter for NEW messages. No await — returns instantly.
    Only relevant messages (from target bots, in groups for all-only sessions)
    are enqueued for worker processing.
    """
    meta = _session_meta.get(session_name)
    if not meta or not meta.get("has_rules"):
        return

    msg = getattr(event, "message", None)
    if msg is None or not msg.buttons:
        return

    # Fast bot pre-filter (O(1) set lookup) — drops the vast majority of messages.
    sender_id = event.sender_id
    if not meta["has_username_bot"]:
        if sender_id is None or sender_id not in meta["bot_set"]:
            return

    # Group-only fast filter for sessions where every rule uses chat_mode "all".
    if meta["all_only"]:
        if event.is_private:
            return
        if event.is_group is False:  # broadcast channel
            return

    job = _build_job(event, session_name)
    queue = _job_queues.get(session_name)
    if queue is not None:
        try:
            queue.put_nowait(job)
        except asyncio.QueueFull:
            log.warning("[%s] Job queue full — dropping message %d.", session_name, msg.id)


def _quick_dispatch_edit(event: Any, session_name: str) -> None:
    """
    Synchronous handler for EDITED messages.
    - If buttons were removed → stop active loops immediately (fast path).
    - If buttons are present → enqueue for re-matching (button may have
      changed or returned, requiring a fresh click task with reset timeout).
    """
    meta = _session_meta.get(session_name)
    if not meta or not meta.get("has_rules"):
        return

    msg = getattr(event, "message", None)
    if msg is None:
        return

    chat_id = event.chat_id
    message_id = msg.id
    if chat_id is None or message_id is None:
        return

    task_key = (session_name, chat_id, message_id)

    # Fast path: buttons removed → stop any active loops for this message.
    if not msg.buttons:
        for sig, evt, task in _active_tasks.get(task_key, []):
            if not task.done():
                evt.set()
        return

    # Has buttons → apply the same pre-filters, then enqueue for re-matching.
    sender_id = event.sender_id
    if not meta["has_username_bot"]:
        if sender_id is None or sender_id not in meta["bot_set"]:
            return

    if meta["all_only"]:
        if event.is_private:
            return
        if event.is_group is False:
            return

    job = _build_job(event, session_name)
    queue = _job_queues.get(session_name)
    if queue is not None:
        try:
            queue.put_nowait(job)
        except asyncio.QueueFull:
            log.warning("[%s] Job queue full — dropping edited message %d.", session_name, message_id)


# =============================================================================
# WORKER SYSTEM (parallel per-session processing)
# =============================================================================

async def _session_worker(session_name: str, queue: asyncio.Queue) -> None:
    """Worker coroutine: dequeue jobs and process them concurrently."""
    while True:
        try:
            job = await queue.get()
        except asyncio.CancelledError:
            break
        try:
            await _process_job(session_name, job)
        except Exception:
            log.exception("[%s] Worker error processing message %d.", session_name, job.message_id)
        finally:
            queue.task_done()


async def _resolve_sender(client: TelegramClient, job: Job) -> Any:
    if job.sender_id is None:
        return None
    try:
        return await client.get_entity(job.sender_id)
    except Exception:
        return None


async def _resolve_chat(client: TelegramClient, job: Job) -> Any:
    try:
        return await client.get_entity(job.chat_id)
    except Exception:
        return None


async def _resolve_is_group(client: TelegramClient, job: Job) -> bool:
    """Determine if the job's chat is a group (basic or supergroup)."""
    if job.is_group is True:
        return True
    if job.is_group is False:
        return False
    # None → resolve entity (usually a cache hit, no network).
    try:
        chat = await client.get_entity(job.chat_id)
    except Exception:
        return False
    return _is_group_entity(chat)


async def _process_job(session_name: str, job: Job) -> None:
    """
    Full matching logic executed by a worker.

    Phase 1 (may await): resolve entities, evaluate rules, collect matches.
    Phase 2 (SYNC, no await): reconcile active tasks by button signature —
        stop stale tasks, spawn fresh tasks for new/returning buttons.
        Because this block has no await, it is atomic under asyncio's
        single-threaded model — no race conditions, no duplicate tasks.
    """
    rules = _rule_index.get(session_name)
    if not rules:
        return

    client = _clients.get(session_name)
    if client is None:
        return

    my_id = (_account_meta.get(session_name, {}).get("user") or {}).get("id")

    # ── Phase 1: collect matches (may await for entity resolution) ─────────
    current_matches: Dict[str, Tuple[Any, Dict[str, Any]]] = {}
    sender_entity = None
    chat_entity = None
    group_ok: Optional[bool] = None

    for compiled in rules:
        rule = compiled.rule
        if not rule.get("enabled", True):
            continue

        # Sender match
        if compiled.bot_has_username:
            if sender_entity is None:
                sender_entity = await _resolve_sender(client, job)
            if not _sender_matches_any(rule.get("bot_ids") or [], sender_entity):
                continue
        else:
            if job.sender_id is None or job.sender_id not in (compiled.bot_numeric or set()):
                continue

        # Chat match
        if compiled.chat_mode == CHAT_MODE_ALL:
            if group_ok is None:
                group_ok = await _resolve_is_group(client, job)
            if not group_ok:
                continue
        elif compiled.chat_has_username:
            if chat_entity is None:
                chat_entity = await _resolve_chat(client, job)
            if not _chat_matches_rule(rule, chat_entity):
                continue
        else:
            if job.chat_id not in (compiled.chat_numeric or set()):
                continue

        # Self-targeting filter
        if not _self_filter_passes(rule, job, my_id):
            continue

        # Button matches — first rule to claim a signature wins.
        for raw_btn, sig in _find_matches(job.buttons, rule):
            if sig not in current_matches:
                current_matches[sig] = (raw_btn, rule)

    # ── Phase 2: SYNC reconciliation (atomic — no await below) ─────────────
    task_key = (session_name, job.chat_id, job.message_id)

    # Keep only tasks that are still running.
    entries = [e for e in _active_tasks.get(task_key, []) if not e[2].done()]
    current_sigs = set(current_matches.keys())

    # Stop tasks whose button signature is no longer present (button changed
    # or removed). This makes the loop exit promptly so a returning button
    # can start a fresh task with a reset timeout.
    for sig, evt, task in entries:
        if sig not in current_sigs:
            evt.set()

    # Spawn fresh tasks for signatures not currently running. A signature
    # that was just stopped (stale) is NOT in running_sigs, so a returning
    # button gets a brand-new task with a fresh deadline.
    running_sigs = {sig for sig, evt, task in entries if not evt.is_set()}

    for sig, (raw_btn, rule) in current_matches.items():
        if sig in running_sigs:
            continue  # already clicking this exact button

        try:
            cps = float(rule.get("clicks_per_second", DEFAULT_CPS))
            timeout = float(rule.get("timeout", DEFAULT_TIMEOUT))
            reaction_delay_ms = float(rule.get("reaction_delay_ms", DEFAULT_REACTION_DELAY_MS))
            refresh_every_n_clicks = int(rule.get("refresh_every_n_clicks", DEFAULT_REFRESH_EVERY_N_CLICKS))
            burst_count = int(rule.get("burst_count", DEFAULT_BURST_COUNT))
        except (TypeError, ValueError):
            continue

        cps = max(MIN_CPS, min(cps, MAX_CPS))
        timeout = max(1.0, min(timeout, MAX_TIMEOUT))
        skip_first_refresh = _to_bool(rule.get("skip_first_refresh"), DEFAULT_SKIP_FIRST_REFRESH)

        evt = asyncio.Event()
        task = asyncio.create_task(
            _click_loop(
                client=client,
                session_name=session_name,
                chat_id=job.chat_id,
                message_id=job.message_id,
                raw_button=raw_btn,
                button_signature=sig,
                button_keyword=rule.get("button_keyword", ""),
                match_mode=rule.get("match_mode", DEFAULT_MATCH_MODE),
                case_sensitive=bool(rule.get("case_sensitive", False)),
                cps=cps,
                timeout=timeout,
                stop_event=evt,
                task_key=task_key,
                reaction_delay_ms=reaction_delay_ms,
                skip_first_refresh=skip_first_refresh,
                refresh_every_n_clicks=refresh_every_n_clicks,
                burst_count=burst_count,
            ),
            name=f"click_{session_name}_{job.message_id}_{sig[:8]}",
        )
        entries.append((sig, evt, task))
        _bump_stat(session_name, "matches")
        _log_info(
            "[%s] Matched rule: bots=%s chat_mode=%s keyword='%s' mode=%s "
            "cps=%.1f timeout=%.0fs sig=%s",
            session_name,
            ",".join(rule.get("bot_ids") or []),
            rule.get("chat_mode"),
            rule.get("button_keyword"),
            rule.get("match_mode"),
            cps,
            timeout,
            sig[:12],
        )

    if entries:
        _active_tasks[task_key] = entries
    else:
        _active_tasks.pop(task_key, None)


async def _ensure_workers(session_name: str) -> None:
    """Start or adjust the worker pool for a session based on settings."""
    count = int(_settings.get("workers_per_session", DEFAULT_SETTINGS["workers_per_session"]))

    queue = _job_queues.get(session_name)
    if queue is None:
        queue = asyncio.Queue(maxsize=0)  # unbounded
        _job_queues[session_name] = queue

    workers = _workers.get(session_name, [])

    while len(workers) < count:
        idx = len(workers)
        task = asyncio.create_task(
            _session_worker(session_name, queue),
            name=f"worker_{session_name}_{idx}",
        )
        workers.append(task)

    while len(workers) > count:
        task = workers.pop()
        task.cancel()

    _workers[session_name] = workers


async def _stop_workers(session_name: str) -> None:
    """Cancel all workers for a session."""
    workers = _workers.pop(session_name, [])
    for task in workers:
        task.cancel()
    if workers:
        await asyncio.gather(*workers, return_exceptions=True)
    _job_queues.pop(session_name, None)
    _click_semaphores.pop(session_name, None)


# =============================================================================
# TELETHON CLIENT MANAGEMENT
# =============================================================================

async def _safe_disconnect(client: Optional[TelegramClient]) -> None:
    if client is None:
        return
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception:
        pass


async def _adopt_client(
    session_name: str,
    client: TelegramClient,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
    save_env: bool = False,
    phone: Optional[str] = None,
) -> Dict[str, Any]:
    old_client = _clients.get(session_name)
    if old_client is not None and old_client is not client:
        await _safe_disconnect(old_client)

    me = await client.get_me()
    user_info = {
        "id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name,
        "username": me.username,
    }

    _clients[session_name] = client
    _account_meta[session_name] = {
        "authorized": True,
        "user": user_info,
        "phone": phone,
        "connected_at": int(time.time()),
    }

    _register_handlers(client, session_name)
    await _ensure_workers(session_name)

    if save_env and api_id is not None and api_hash:
        # Persist (or refresh) this account's full record — including its
        # current StringSession — into accounts.json. This does not create
        # a new Telegram login/auth key; it just serializes the auth key
        # `client.session` already holds, so accounts.json alone stays
        # enough to reconnect later without repeating phone+code+2FA.
        try:
            session_string = StringSession.save(client.session)
            _upsert_account_record(session_name, api_id, api_hash, phone or "", session_string)
            log.info("Saved credentials for session '%s' to accounts.json.", session_name)
        except Exception as exc:
            log.warning("Could not save session_string for '%s' (non-fatal): %s", session_name, exc)

    log.info(
        "Session '%s' connected as %s (id=%s, username=%s).",
        session_name,
        me.first_name,
        me.id,
        f"@{me.username}" if me.username else "n/a",
    )

    return _account_meta[session_name]


async def _connect_session(
    session_name: str,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
) -> Tuple[bool, str]:
    if not _is_valid_session_name(session_name):
        return False, "invalid_session_name"

    async with _account_lock:
        old_client = _clients.pop(session_name, None)
        if old_client is not None:
            await _safe_disconnect(old_client)

        record = _account_record(session_name)
        if record is None:
            return False, "account_not_found"

        resolved_api_id = api_id or record.get("api_id")
        resolved_api_hash = api_hash or record.get("api_hash")
        if not resolved_api_id or not resolved_api_hash:
            return False, "credentials_not_set"
        if not record.get("session_string"):
            return False, "session_unauthorized"

        client = _build_client_for_account(record)

        try:
            await client.connect()
            if not await client.is_user_authorized():
                await _safe_disconnect(client)
                return False, "session_unauthorized"
            await _adopt_client(
                session_name, client,
                api_id=resolved_api_id, api_hash=resolved_api_hash,
                save_env=True, phone=record.get("phone"),
            )
            return True, "connected"
        except AuthKeyError:
            await _safe_disconnect(client)
            return False, "auth_key_invalid"
        except FloodWaitError as exc:
            await _safe_disconnect(client)
            return False, f"flood_wait_{exc.seconds}"
        except RPCError as exc:
            await _safe_disconnect(client)
            return False, _rpc_message(exc).lower()
        except Exception as exc:
            await _safe_disconnect(client)
            return False, str(exc)


async def boot_clients() -> None:
    await _migrate_legacy_sessions()
    if not _accounts_cache:
        log.warning("No accounts found in %s.", ACCOUNTS_FILE)
        return

    for session_name in sorted(_accounts_cache.keys()):
        ok, message = await _connect_session(session_name)
        if not ok:
            log.error("Session '%s' not connected: %s", session_name, message)


async def _cleanup_pending_loop() -> None:
    while True:
        try:
            await asyncio.sleep(30)
            now = time.time()
            async with _account_lock:
                expired = [
                    name for name, pending in _pending_logins.items()
                    if now - pending.created_at > PENDING_LOGIN_TTL
                ]
                for name in expired:
                    pending = _pending_logins.pop(name, None)
                    if pending is None:
                        continue
                    log.warning("Pending login for session '%s' expired.", name)
                    await _safe_disconnect(pending.client)
        except asyncio.CancelledError:
            break
        except Exception:
            log.exception("Error in pending-login cleanup loop.")


# =============================================================================
# EVENT HANDLERS
# =============================================================================

def _register_handlers(client: TelegramClient, session_name: str) -> None:
    """
    Attach optimized event handlers.

    NewMessage:
        - incoming only
        - func filter: only messages with inline buttons
        - handler body is synchronous (_quick_dispatch) — never blocks
    MessageEdited:
        - incoming only
        - re-evaluates button state: stops loops when buttons are removed,
          re-matches when buttons change or return
    """

    @client.on(events.NewMessage(
        incoming=True,
        outgoing=False,
        func=lambda e: e.message is not None and bool(e.message.buttons),
    ))
    async def on_new_message(event: events.NewMessage.Event) -> None:
        try:
            _quick_dispatch(event, session_name)
        except Exception:
            log.exception("Error in NewMessage handler (session=%s).", session_name)

    @client.on(events.MessageEdited(incoming=True, outgoing=False))
    async def on_message_edited(event: events.MessageEdited.Event) -> None:
        try:
            _quick_dispatch_edit(event, session_name)
        except Exception:
            log.exception("Error in MessageEdited handler (session=%s).", session_name)


# =============================================================================
# CLICK LOOP
# =============================================================================

async def _click_loop(
    client: TelegramClient,
    session_name: str,
    chat_id: int,
    message_id: int,
    raw_button: Any,
    button_signature: str,
    button_keyword: str,
    match_mode: str,
    case_sensitive: bool,
    cps: float,
    timeout: float,
    stop_event: asyncio.Event,
    task_key: Tuple[str, int, int],
    reaction_delay_ms: float = DEFAULT_REACTION_DELAY_MS,
    skip_first_refresh: bool = DEFAULT_SKIP_FIRST_REFRESH,
    refresh_every_n_clicks: int = DEFAULT_REFRESH_EVERY_N_CLICKS,
    burst_count: int = DEFAULT_BURST_COUNT,
) -> None:
    """
    Click loop for a single button (identified by its signature).

    - Uses the original event button for the first click when
      skip_first_refresh is True (no extra API call → minimum latency).
    - Re-fetches the message every `refresh_every_n_clicks` clicks to verify
      the button still exists with the SAME signature. If the signature is
      gone, exits so a returning button can start a fresh task.
    - Each task owns its own stop_event; cleanup removes only this task's
      entry from _active_tasks.
    """
    loop = asyncio.get_running_loop()

    _log_info(
        "[%s] Click loop started: message=%s keyword='%s' mode=%s cps=%.1f "
        "timeout=%.0fs reaction_delay=%.0fms skip_first_refresh=%s refresh_every=%d burst=%d sig=%s",
        session_name, message_id, button_keyword, match_mode, cps, timeout,
        reaction_delay_ms, skip_first_refresh, refresh_every_n_clicks, burst_count,
        button_signature[:12],
    )

    deadline = loop.time() + timeout
    interval = 1.0 / max(cps, MIN_CPS)
    click_count = 0
    exit_reason = "unknown"
    last_buttons: Any = None
    sem = _get_click_semaphore(session_name)

    try:
        # Optional reaction delay (interruptible).
        if reaction_delay_ms > 0:
            await _interruptible_sleep(reaction_delay_ms / 1000.0, stop_event, deadline)

        while True:
            now = loop.time()

            if now >= deadline:
                exit_reason = f"timeout ({timeout:.0f}s)"
                break

            if stop_event.is_set():
                exit_reason = "button changed or removed"
                break

            if not client.is_connected():
                exit_reason = "client disconnected"
                break

            # ── Determine target button ───────────────────────────────────
            if click_count == 0 and skip_first_refresh and raw_button is not None:
                # Fastest path: click the button from the original event.
                target_raw = raw_button
            else:
                need_refresh = (
                    last_buttons is None
                    or (
                        refresh_every_n_clicks > 0
                        and (click_count == 0 or click_count % refresh_every_n_clicks == 0)
                    )
                )

                if need_refresh:
                    try:
                        fresh_msg = await client.get_messages(chat_id, ids=message_id)
                    except MessageIdInvalidError:
                        exit_reason = "message deleted"
                        break
                    except FloodWaitError as exc:
                        wait_secs = exc.seconds + 2
                        _bump_stat(session_name, "floodwaits")
                        log.warning("[%s] FloodWait %ds during message fetch.", session_name, wait_secs)
                        await _interruptible_sleep(wait_secs, stop_event, deadline)
                        continue
                    except Exception as exc:
                        log.debug("[%s] get_messages failed: %s. Retrying.", session_name, exc)
                        await asyncio.sleep(1.0)
                        continue

                    if isinstance(fresh_msg, list):
                        fresh_msg = fresh_msg[0] if fresh_msg else None

                    if fresh_msg is None:
                        exit_reason = "message not found"
                        break

                    last_buttons = fresh_msg.buttons

                # Find the button with our exact signature.
                found = _find_buttons_by_keyword(
                    last_buttons, button_keyword, match_mode, case_sensitive, click_all=True
                )
                target_raw = None
                for btn in found:
                    raw = _extract_raw(btn)
                    if _button_signature(raw) == button_signature:
                        target_raw = raw
                        break

                if target_raw is None:
                    # Button signature gone → exit so a returning button can
                    # start a fresh task with a reset timeout.
                    stop_event.set()
                    exit_reason = "button no longer present"
                    break

            # ── Extract callback payload ──────────────────────────────────
            data = getattr(target_raw, "data", None)
            game = bool(getattr(target_raw, "game", False))

            if data is None and not game:
                exit_reason = "button has no callback data"
                break

            # ── Click ─────────────────────────────────────────────────────
            if sem is not None:
                await sem.acquire()

            tick_start = loop.time()

            try:
                await client(
                    GetBotCallbackAnswerRequest(
                        peer=chat_id,
                        msg_id=message_id,
                        data=data or b"",
                        game=game,
                        password=None,
                    )
                )

                click_count += 1
                _bump_stat(session_name, "clicks_ok")

                if click_count % 10 == 0:
                    _log_info(
                        "[%s] Click #%d on keyword '%s' (message=%s).",
                        session_name, click_count, button_keyword, message_id,
                    )
                else:
                    log.debug(
                        "[%s] Click #%d on keyword '%s' (message=%s).",
                        session_name, click_count, button_keyword, message_id,
                    )

            except FloodWaitError as exc:
                wait_secs = exc.seconds + 2
                _bump_stat(session_name, "floodwaits")
                log.warning("[%s] FloodWait %ds. Pausing click loop.", session_name, wait_secs)
                await _interruptible_sleep(wait_secs, stop_event, deadline)
                continue

            except Exception as exc:
                _bump_stat(session_name, "clicks_fail")
                err = str(exc).upper()

                if any(
                    keyword in err
                    for keyword in (
                        "BUTTON", "BOT_RESPONSE_TIMEOUT", "MESSAGE_ID_INVALID",
                        "MSG_ID_INVALID", "PEER_ID_INVALID",
                    )
                ):
                    log.warning(
                        "[%s] Telegram rejected click (%s). Button likely invalidated.",
                        session_name, exc,
                    )
                    exit_reason = "button invalidated by Telegram"
                    break

                log.error("[%s] Unexpected click error: %s", session_name, exc)
                await asyncio.sleep(interval)
                continue

            finally:
                if sem is not None:
                    sem.release()

            # ── Pacing ────────────────────────────────────────────────────
            if click_count < burst_count:
                sleep_for = 0.0
            else:
                elapsed = loop.time() - tick_start
                sleep_for = max(0.0, interval - elapsed)

            await asyncio.sleep(sleep_for)

    except asyncio.CancelledError:
        exit_reason = "task cancelled"
    except Exception:
        exit_reason = "unexpected exception"
        log.exception("[%s] Click loop crashed for message=%s.", session_name, message_id)
    finally:
        # Remove only THIS task's entry (matched by stop_event identity).
        entries = _active_tasks.get(task_key)
        if entries is not None:
            _active_tasks[task_key] = [e for e in entries if e[1] is not stop_event]
            if not _active_tasks[task_key]:
                _active_tasks.pop(task_key, None)

        log.info(
            "[%s] Click loop ended: message=%s clicks=%d reason='%s'.",
            session_name, message_id, click_count, exit_reason,
        )


async def _interruptible_sleep(seconds: float, stop_event: asyncio.Event, deadline: float) -> None:
    loop = asyncio.get_running_loop()
    slept = 0.0
    while slept < seconds:
        if stop_event.is_set() or loop.time() >= deadline:
            break
        chunk = min(1.0, seconds - slept)
        await asyncio.sleep(chunk)
        slept += chunk


# =============================================================================
# ACCOUNT LOGIN API
# =============================================================================

async def _api_login_start(request: web.Request) -> web.Response:
    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    session_name = _sanitize_session_name(data.get("session_name"))
    if not session_name:
        return _json_error("invalid_session_name")

    phone = str(data.get("phone", "")).strip()
    api_hash = str(data.get("api_hash", "")).strip()
    save_env = _to_bool(data.get("save_env"), True)
    force = _to_bool(data.get("force"), False)

    if not phone.startswith("+") or len(phone) < 8:
        return _json_error("invalid_phone_number")
    if not api_hash:
        return _json_error("invalid_api_hash")

    try:
        api_id = int(str(data.get("api_id", "")).strip())
    except (TypeError, ValueError):
        return _json_error("invalid_api_id")
    if api_id <= 0:
        return _json_error("invalid_api_id")

    async with _account_lock:
        pending = _pending_logins.get(session_name)
        if pending is not None:
            return _json_error("login_already_pending", status=409, state=pending.state)

        existing_client = _clients.get(session_name)
        if existing_client is not None:
            existing_authorized = False
            try:
                if existing_client.is_connected():
                    existing_authorized = await existing_client.is_user_authorized()
            except Exception:
                existing_authorized = False

            if existing_authorized and not force:
                meta = _account_meta.get(session_name, {})
                return _json_ok(status="authorized", account=meta)

            _clients.pop(session_name, None)
            _account_meta.pop(session_name, None)
            await _safe_disconnect(existing_client)
            await _stop_workers(session_name)

            if force:
                _delete_account_record(session_name)

        client = TelegramClient(
            StringSession(),
            api_id, api_hash,
            connection_retries=5, retry_delay=3, flood_sleep_threshold=60,
        )

        try:
            await client.connect()
            if await client.is_user_authorized():
                meta = await _adopt_client(
                    session_name, client,
                    api_id=api_id, api_hash=api_hash,
                    save_env=save_env, phone=phone,
                )
                return _json_ok(status="authorized", account=meta)

            sent = await client.send_code_request(phone, force_sms=False)
            _pending_logins[session_name] = PendingLogin(
                session_name=session_name,
                api_id=api_id, api_hash=api_hash, phone=phone, client=client,
                phone_code_hash=getattr(sent, "phone_code_hash", "") or "",
                state="code_required", save_env=save_env,
            )
            return _json_ok(status="code_required")

        except FloodWaitError as exc:
            await _safe_disconnect(client)
            return _json_error("flood_wait", wait_seconds=exc.seconds)
        except AuthKeyError:
            await _safe_disconnect(client)
            return _json_error("auth_key_invalid")
        except RPCError as exc:
            await _safe_disconnect(client)
            return _json_error(_rpc_message(exc).lower())
        except Exception as exc:
            await _safe_disconnect(client)
            return _json_error(str(exc), status=500)


async def _api_login_code(request: web.Request) -> web.Response:
    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    session_name = _sanitize_session_name(data.get("session_name"))
    if not session_name:
        return _json_error("invalid_session_name")

    code = str(data.get("code", "")).strip()
    if not code:
        return _json_error("invalid_code")

    async with _account_lock:
        pending = _pending_logins.get(session_name)
        if pending is None:
            return _json_error("no_pending_login", status=404)
        if pending.state != "code_required":
            return _json_error("invalid_login_state", status=409, state=pending.state)

        pending.last_code = code

        try:
            await pending.client.sign_in(
                phone=pending.phone, code=code,
                phone_code_hash=pending.phone_code_hash,
            )
        except SessionPasswordNeededError:
            pending.state = "password_required"
            return _json_ok(status="password_required")
        except FloodWaitError as exc:
            return _json_error("flood_wait", wait_seconds=exc.seconds)
        except RPCError as exc:
            message = _rpc_message(exc)
            if "PHONE_CODE_EXPIRED" in message:
                _pending_logins.pop(session_name, None)
                await _safe_disconnect(pending.client)
                return _json_error("phone_code_expired", status=410)
            if "UNREGISTERED" in message:
                pending.state = "signup_required"
                return _json_ok(status="signup_required")
            return _json_error(message.lower())
        except Exception as exc:
            _pending_logins.pop(session_name, None)
            await _safe_disconnect(pending.client)
            return _json_error(str(exc), status=500)

        try:
            account = await _finalize_pending_locked(session_name, pending)
        except Exception as exc:
            _pending_logins.pop(session_name, None)
            await _safe_disconnect(pending.client)
            return _json_error(str(exc), status=500)

        return _json_ok(status="authorized", account=account)


async def _api_login_password(request: web.Request) -> web.Response:
    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    session_name = _sanitize_session_name(data.get("session_name"))
    if not session_name:
        return _json_error("invalid_session_name")

    password = str(data.get("password", ""))
    if not password:
        return _json_error("invalid_password")

    async with _account_lock:
        pending = _pending_logins.get(session_name)
        if pending is None:
            return _json_error("no_pending_login", status=404)
        if pending.state != "password_required":
            return _json_error("invalid_login_state", status=409, state=pending.state)

        try:
            await pending.client.sign_in(password=password)
        except SessionPasswordNeededError:
            return _json_error("password_required")
        except FloodWaitError as exc:
            return _json_error("flood_wait", wait_seconds=exc.seconds)
        except RPCError as exc:
            message = _rpc_message(exc)
            if "PASSWORD_HASH_INVALID" in message:
                return _json_error("password_hash_invalid")
            return _json_error(message.lower())
        except Exception as exc:
            _pending_logins.pop(session_name, None)
            await _safe_disconnect(pending.client)
            return _json_error(str(exc), status=500)

        try:
            account = await _finalize_pending_locked(session_name, pending)
        except Exception as exc:
            _pending_logins.pop(session_name, None)
            await _safe_disconnect(pending.client)
            return _json_error(str(exc), status=500)

        return _json_ok(status="authorized", account=account)


async def _api_login_signup(request: web.Request) -> web.Response:
    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    session_name = _sanitize_session_name(data.get("session_name"))
    if not session_name:
        return _json_error("invalid_session_name")

    first_name = str(data.get("first_name", "")).strip() or "User"
    last_name = str(data.get("last_name", "")).strip()

    async with _account_lock:
        pending = _pending_logins.get(session_name)
        if pending is None:
            return _json_error("no_pending_login", status=404)
        if pending.state != "signup_required":
            return _json_error("invalid_login_state", status=409, state=pending.state)
        if not pending.last_code:
            return _json_error("missing_login_code", status=409)

        try:
            await pending.client.sign_up(
                code=pending.last_code, first_name=first_name, last_name=last_name,
            )
        except FloodWaitError as exc:
            return _json_error("flood_wait", wait_seconds=exc.seconds)
        except RPCError as exc:
            return _json_error(_rpc_message(exc).lower())
        except Exception as exc:
            _pending_logins.pop(session_name, None)
            await _safe_disconnect(pending.client)
            return _json_error(str(exc), status=500)

        try:
            account = await _finalize_pending_locked(session_name, pending)
        except Exception as exc:
            _pending_logins.pop(session_name, None)
            await _safe_disconnect(pending.client)
            return _json_error(str(exc), status=500)

        return _json_ok(status="authorized", account=account)


async def _api_login_cancel(request: web.Request) -> web.Response:
    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    session_name = _sanitize_session_name(data.get("session_name"))
    if not session_name:
        return _json_error("invalid_session_name")

    async with _account_lock:
        pending = _pending_logins.pop(session_name, None)
        if pending is not None:
            await _safe_disconnect(pending.client)
            log.info("Pending login for session '%s' cancelled.", session_name)

    return _json_ok(status="cancelled")


async def _api_login_string(request: web.Request) -> web.Response:
    """
    Register an account from an already-existing Telethon StringSession —
    no phone/code/2FA step at all. Verifies the session is actually
    authorized before saving it, and auto-fills the phone number from
    Telegram itself.
    """
    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    session_name = _sanitize_session_name(data.get("session_name"))
    if not session_name:
        return _json_error("invalid_session_name")

    session_string = str(data.get("session_string", "")).strip()
    if not session_string:
        return _json_error("invalid_session_string")

    api_hash = str(data.get("api_hash", "")).strip()
    if not api_hash:
        return _json_error("invalid_api_hash")

    try:
        api_id = int(str(data.get("api_id", "")).strip())
    except (TypeError, ValueError):
        return _json_error("invalid_api_id")
    if api_id <= 0:
        return _json_error("invalid_api_id")

    force = _to_bool(data.get("force"), False)

    async with _account_lock:
        if session_name in _pending_logins:
            return _json_error("login_already_pending", status=409)

        existing_client = _clients.get(session_name)
        if existing_client is not None:
            existing_authorized = False
            try:
                if existing_client.is_connected():
                    existing_authorized = await existing_client.is_user_authorized()
            except Exception:
                existing_authorized = False

            if existing_authorized and not force:
                meta = _account_meta.get(session_name, {})
                return _json_ok(status="authorized", account=meta)

            _clients.pop(session_name, None)
            _account_meta.pop(session_name, None)
            await _safe_disconnect(existing_client)
            await _stop_workers(session_name)

            if force:
                _delete_account_record(session_name)
        elif not force and session_name in _accounts_cache:
            return _json_error("account_already_exists", status=409)

        try:
            client = TelegramClient(
                StringSession(session_string),
                api_id, api_hash,
                connection_retries=5, retry_delay=3, flood_sleep_threshold=60,
            )
            await client.connect()
        except ValueError:
            return _json_error("invalid_session_string")
        except AuthKeyError:
            return _json_error("auth_key_invalid")
        except FloodWaitError as exc:
            return _json_error("flood_wait", wait_seconds=exc.seconds)
        except RPCError as exc:
            return _json_error(_rpc_message(exc).lower())
        except Exception as exc:
            return _json_error(str(exc), status=500)

        if not await client.is_user_authorized():
            await _safe_disconnect(client)
            return _json_error("session_unauthorized")

        try:
            me = await client.get_me()
            phone = f"+{me.phone}" if me and me.phone else ""
        except Exception:
            phone = ""

        account = await _adopt_client(
            session_name, client,
            api_id=api_id, api_hash=api_hash,
            save_env=True, phone=phone,
        )
        return _json_ok(status="authorized", account=account)


async def _finalize_pending_locked(session_name: str, pending: PendingLogin) -> Dict[str, Any]:
    account = await _adopt_client(
        session_name, pending.client,
        api_id=pending.api_id, api_hash=pending.api_hash,
        save_env=pending.save_env, phone=pending.phone,
    )
    _pending_logins.pop(session_name, None)
    return account


# =============================================================================
# ACCOUNT MANAGEMENT API
# =============================================================================

async def _api_accounts(request: web.Request) -> web.Response:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    return _json_ok(accounts=_accounts_snapshot())


async def _api_delete_account(request: web.Request) -> web.Response:
    session_name = _sanitize_session_name(request.match_info.get("name", ""))
    if not session_name:
        return _json_error("invalid_session_name")

    async with _account_lock:
        pending = _pending_logins.pop(session_name, None)
        if pending is not None:
            await _safe_disconnect(pending.client)

        client = _clients.pop(session_name, None)
        if client is not None:
            await _safe_disconnect(client)

        _account_meta.pop(session_name, None)
        await _stop_workers(session_name)

        for task_key, entries in list(_active_tasks.items()):
            if task_key[0] == session_name:
                for sig, evt, task in entries:
                    task.cancel()

        _delete_account_record(session_name)
    log.info("Account '%s' deleted.", session_name)
    return _json_ok(status="deleted")


async def _api_reconnect_account(request: web.Request) -> web.Response:
    session_name = _sanitize_session_name(request.match_info.get("name", ""))
    if not session_name:
        return _json_error("invalid_session_name")

    ok, message = await _connect_session(session_name)
    if not ok:
        return _json_error(message)
    return _json_ok(status="connected")


async def _api_disconnect_account(request: web.Request) -> web.Response:
    session_name = _sanitize_session_name(request.match_info.get("name", ""))
    if not session_name:
        return _json_error("invalid_session_name")

    async with _account_lock:
        client = _clients.pop(session_name, None)
        _account_meta.pop(session_name, None)

        if client is not None:
            await _safe_disconnect(client)

        await _stop_workers(session_name)

        for task_key, entries in list(_active_tasks.items()):
            if task_key[0] == session_name:
                for sig, evt, task in entries:
                    task.cancel()

    log.info("Account '%s' disconnected.", session_name)
    return _json_ok(status="disconnected")


# =============================================================================
# CONFIG API
# =============================================================================

async def _api_get_config(request: web.Request) -> web.Response:
    config = await load_config()
    connected_sessions = sorted(
        name for name, client in _clients.items() if client.is_connected()
    )

    return _json_ok(
        rules=config.get("rules", []),
        sessions=_account_names(),
        connected_sessions=connected_sessions,
        settings=_settings,
        defaults={
            "cps": DEFAULT_CPS,
            "timeout": DEFAULT_TIMEOUT,
            "max_cps": MAX_CPS,
            "min_cps": MIN_CPS,
            "max_timeout": MAX_TIMEOUT,
            "match_modes": list(MATCH_MODES),
            "default_match_mode": DEFAULT_MATCH_MODE,
            "chat_modes": [CHAT_MODE_ALL, CHAT_MODE_LIST],
            "default_chat_mode": CHAT_MODE_LIST,
            "reaction_delay_ms": DEFAULT_REACTION_DELAY_MS,
            "skip_first_refresh": DEFAULT_SKIP_FIRST_REFRESH,
            "refresh_every_n_clicks": DEFAULT_REFRESH_EVERY_N_CLICKS,
            "burst_count": DEFAULT_BURST_COUNT,
            "max_reaction_delay_ms": MAX_REACTION_DELAY_MS,
            "max_refresh_every_n_clicks": MAX_REFRESH_EVERY_N_CLICKS,
            "max_burst_count": MAX_BURST_COUNT,
            "min_workers": MIN_WORKERS,
            "max_workers": MAX_WORKERS,
            "max_concurrent_clicks_cap": MAX_CONCURRENT_CLICKS_CAP,
            "workers_per_session": DEFAULT_SETTINGS["workers_per_session"],
        },
    )


async def _api_post_config(request: web.Request) -> web.Response:
    global _settings

    data = await _read_json(request)
    if data is None:
        return _json_error("invalid_json")

    rules = data.get("rules")
    if not isinstance(rules, list):
        return _json_error("rules_must_be_list")

    settings_raw = data.get("settings")
    if isinstance(settings_raw, dict):
        _settings = _normalize_settings(settings_raw)

    now = int(time.time())
    cleaned_rules: List[Dict[str, Any]] = []

    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            return _json_error(f"rule_{index + 1}_invalid")
        normalized = _normalize_rule(rule, now)
        ok, error = _validate_rule(normalized)
        if not ok:
            return _json_error(f"rule_{index + 1}_{error}")
        cleaned_rules.append(normalized)

    new_config = {"rules": cleaned_rules}
    await save_config(new_config)

    # Adjust worker pools for all connected sessions.
    for session_name in _clients:
        await _ensure_workers(session_name)

    log.info("Config updated via Web UI. %d rule(s) saved.", len(cleaned_rules))
    return _json_ok(rules_saved=len(cleaned_rules), rules=cleaned_rules, settings=_settings)


# =============================================================================
# STATUS / LOG API
# =============================================================================

async def _api_status(request: web.Request) -> web.Response:
    config = await load_config()
    rules = config.get("rules", [])

    tasks: List[Dict[str, Any]] = []
    active_count = 0
    for (session_name, chat_id, message_id), entries in _active_tasks.items():
        for sig, evt, task in entries:
            running = not task.done()
            if running:
                active_count += 1
            tasks.append({
                "session": session_name,
                "chat_id": chat_id,
                "message_id": message_id,
                "signature": sig[:16],
                "running": running,
            })

    accounts = _accounts_snapshot()
    connected_accounts = [item for item in accounts if item["connected"]]

    return _json_ok(
        accounts=accounts,
        sessions={item["name"]: "connected" for item in connected_accounts},
        tasks=tasks,
        stats={
            "connected_accounts": len(connected_accounts),
            "active_tasks": active_count,
            "rules": len(rules),
            "enabled_rules": len([r for r in rules if _to_bool(r.get("enabled"), True)]),
        },
        session_stats=_session_stats,
        settings=_settings,
        log_lines=list(LOG_BUFFER)[-100:],
    )


async def _api_logs_clear(request: web.Request) -> web.Response:
    LOG_BUFFER.clear()
    return _json_ok(status="cleared")


async def _api_log_sse(request: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    await resp.prepare(request)

    for line in list(LOG_BUFFER):
        await resp.write(f"data: {line}\n\n".encode("utf-8"))

    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    SSE_SUBSCRIBERS.append(queue)

    try:
        while True:
            try:
                line = await asyncio.wait_for(queue.get(), timeout=25.0)
                await resp.write(f"data: {line}\n\n".encode("utf-8"))
            except asyncio.TimeoutError:
                await resp.write(b": heartbeat\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        try:
            SSE_SUBSCRIBERS.remove(queue)
        except ValueError:
            pass

    return resp


# =============================================================================
# STATIC UI
# =============================================================================

async def _serve_index(request: web.Request) -> web.Response:
    if not HTML_FILE.exists():
        return web.Response(
            text="<h1>index.html not found</h1><p>Place index.html next to auto_clicker.py.</p>",
            content_type="text/html",
            status=404,
        )
    content = HTML_FILE.read_text(encoding="utf-8")
    return web.Response(text=content, content_type="text/html", charset="utf-8")


# =============================================================================
# WEB APP
# =============================================================================

def _build_app() -> web.Application:
    app = web.Application()

    app.router.add_get("/", _serve_index)

    app.router.add_get("/api/config", _api_get_config)
    app.router.add_post("/api/config", _api_post_config)

    app.router.add_get("/api/status", _api_status)
    app.router.add_get("/api/log/stream", _api_log_sse)
    app.router.add_post("/api/logs/clear", _api_logs_clear)

    app.router.add_post("/api/accounts/login/start", _api_login_start)
    app.router.add_post("/api/accounts/login/code", _api_login_code)
    app.router.add_post("/api/accounts/login/password", _api_login_password)
    app.router.add_post("/api/accounts/login/signup", _api_login_signup)
    app.router.add_post("/api/accounts/login/cancel", _api_login_cancel)
    app.router.add_post("/api/accounts/login/string", _api_login_string)

    app.router.add_get("/api/accounts", _api_accounts)
    app.router.add_delete("/api/accounts/{name}", _api_delete_account)
    app.router.add_post("/api/accounts/{name}/reconnect", _api_reconnect_account)
    app.router.add_post("/api/accounts/{name}/disconnect", _api_disconnect_account)

    return app


# =============================================================================
# SHUTDOWN / MAIN
# =============================================================================

async def _shutdown(runner: web.AppRunner) -> None:
    log.info("Shutting down. Cancelling active tasks and workers.")

    # Cancel all click tasks.
    all_tasks: List[asyncio.Task] = []
    for entries in _active_tasks.values():
        for sig, evt, task in entries:
            task.cancel()
            all_tasks.append(task)
    if all_tasks:
        await asyncio.gather(*all_tasks, return_exceptions=True)

    # Stop all workers.
    for session_name in list(_workers.keys()):
        await _stop_workers(session_name)

    async with _account_lock:
        for name, client in list(_clients.items()):
            await _safe_disconnect(client)
            log.info("Session '%s' disconnected.", name)

        _clients.clear()
        _account_meta.clear()

        for pending in _pending_logins.values():
            await _safe_disconnect(pending.client)
        _pending_logins.clear()

    await runner.cleanup()
    log.info("Web server stopped.")


async def main() -> None:
    global _config_cache, _accounts_cache

    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    _config_cache = _load_config_sync()
    _rebuild_rule_index()
    _accounts_cache = _load_accounts_sync()

    app = _build_app()
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()

    site = web.TCPSite(runner, WEB_HOST, WEB_PORT)
    await site.start()

    log.info("Web UI available at http://127.0.0.1:%d", WEB_PORT)

    cleanup_task = asyncio.create_task(_cleanup_pending_loop())

    await boot_clients()

    if not _clients:
        log.warning("No sessions connected. Open the Web UI and add an account.")
    else:
        log.info(
            "%d session(s) active. Workers per session: %d. Listening for matching messages.",
            len(_clients),
            _settings.get("workers_per_session", DEFAULT_SETTINGS["workers_per_session"]),
        )

    stop_main = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        log.info("Interrupt received.")
        stop_main.set()

    try:
        import signal
        loop.add_signal_handler(signal.SIGINT, _on_signal)
        loop.add_signal_handler(signal.SIGTERM, _on_signal)
    except (NotImplementedError, AttributeError):
        pass

    try:
        await stop_main.wait()
    except KeyboardInterrupt:
        pass
    finally:
        cleanup_task.cancel()
        await asyncio.gather(cleanup_task, return_exceptions=True)
        await _shutdown(runner)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass