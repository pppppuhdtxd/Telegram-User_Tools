# Changelog

All notable changes to this project are documented here.
Format follows [Semantic Versioning](https://semver.org): **MAJOR.MINOR.PATCH**

---

## [1.0.0] — 2026-10-26

**Source:** Initial release — three independent Telegram userbot tools consolidated into one monorepo.

### Added

- **auto_clicker/** — Inline-button auto-clicker for Telegram bots
  - Per-session job queue with configurable worker pool
  - Web UI for account management and rule configuration
  - Keyword/regex/emoji-safe button matching
  - Live SSE logs, per-session click statistics
  - All-chats mode (groups only, excludes channels/private)

- **click_farm/** — Telegram mini-game assistant backend
  - Web dashboard for managing game interactions
  - Automated callback answer handling with delays
  - JSON-based persistent data storage
  - ThreadingHTTPServer with custom endpoints

- **scheduled_sender/** — Multi-account scheduled message sender
  - Extensible mode system (BaseMode pattern)
  - Web UI for configuration and scheduling
  - Telethon session management via REST API
  - FloodWait handling and scheduled message CRUD