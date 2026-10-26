# Telegram-User_Tools

A monorepo of three independent Telegram userbot tools, each with its own web UI.

**Current version:** `1.0.0`

See [CHANGELOG.md](CHANGELOG.md) for the full history.

---

## Tools

| Tool | Description | Entry Point |
|------|-------------|-------------|
| **auto_clicker** | Inline-button auto-clicker for Telegram bots with web management UI | `auto_clicker/auto_clicker.py` |
| **click_farm** | Private Telegram mini-game assistant with web dashboard | `click_farm/click_farm.py` |
| **scheduled_sender** | Multi-account scheduled message sender with extensible mode system | `scheduled_sender/scheduled_sender.py` |

---

## Quick Start

Each tool is completely independent — run the one you need:

```bash
# auto_clicker
cd auto_clicker
pip install telethon aiohttp
python auto_clicker.py

# click_farm
cd click_farm
pip install telethon
python click_farm.py

# scheduled_sender
cd scheduled_sender
pip install telethon aiohttp
python scheduled_sender.py
```

All three tools include a web-based management UI accessible at `http://127.0.0.1:<port>` (ports are configurable via environment variables or defaults in each script).

---

## Common Dependencies

- Python 3.11+
- [Telethon](https://docs.telethon.dev/) (`pip install telethon`)
- `aiohttp` for tools with web UI (`pip install aiohttp`)

---

## Structure

```
Telegram-User_Tools/
├── VERSION                 ← single source of truth for version
├── CHANGELOG.md            ← all changes since the beginning
├── README.md               ← this file
├── .gitignore
├── auto_clicker/
│   ├── auto_clicker.py     ← main entry point
│   ├── index.html          ← web UI
│   └── note.txt
├── click_farm/
│   ├── click_farm.py       ← main entry point
│   ├── dashboard.html      ← web UI
│   └── README_STRING_SESSION.txt
└── scheduled_sender/
    ├── scheduled_sender.py ← main entry point
    ├── web_interface.html  ← web UI
    └── note.txt
```

---

## Versioning

This project follows [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`).

| Part | When to bump |
| --- | --- |
| **MAJOR** | Breaking architectural change |
| **MINOR** | New feature, new tool, meaningful enhancement |
| **PATCH** | Bug fix, refactor, doc update |

To release a new version:

1. Update `VERSION`
2. Add entry to `CHANGELOG.md`
3. Update `README.md` Current version line
4. Commit with the version as message: `git commit -m "1.0.1"`
5. Push and tag: `git push origin main && git tag "v$(cat VERSION)" && git push origin "v$(cat VERSION)"`

---

## License

Private project — no public license.