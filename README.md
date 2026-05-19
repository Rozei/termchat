# TermChat

A minimal asynchronous TCP chat server in Python. Single-process `asyncio` event loop, named rooms, direct messages, and broadcasts.

Built as a personal project to practice Python concurrency primitives (coroutines, locks, signal handlers) outside of the typical Flask / FastAPI web-framework comfort zone.

## Quick start

```bash
pip install -r requirements.txt

# terminal 1 — start the server
python server.py --port 8765

# terminal 2 — connect a client
python client.py --port 8765
username: alice
welcome alice...
/join general
joined #general
/room general hi everyone
```

Open as many client terminals as you like — each one connects on the same port.

## Protocol

After connecting, the server sends `username: ` and waits for one line. Once registered, the client sends commands as plain text lines:

| Command | Effect |
| --- | --- |
| `/join <room>` | Join (or create) a room |
| `/room <room> <text>` | Send `<text>` to everyone in `<room>` (sender excluded) |
| `/msg <user> <text>` | Direct-message another connected user |
| `/quit` | Disconnect |

The server sends back lines like `#general <alice> hi everyone` for room messages and `DM <alice> hi` for direct messages.

## Architecture

```
┌──────────┐                                    ┌──────────────────────┐
│ client A │ ─── TCP ───► asyncio.start_server ─►        ChatServer    │
└──────────┘                                    │ ┌──────────────────┐ │
                                                │ │  asyncio.Lock    │ │
┌──────────┐                                    │ ├──────────────────┤ │
│ client B │ ─── TCP ───► coroutine per conn  ─►│ │  clients: dict   │ │
└──────────┘                                    │ │  rooms:   dict   │ │
                                                │ └──────────────────┘ │
                                                └──────────────────────┘
```

- **One coroutine per connection** (`handle_client`). Reads lines, dispatches commands.
- **All shared state** — the `clients` map and the `rooms` adjacency map — lives behind one `asyncio.Lock`. The critical sections are intentionally tiny: we copy out the recipient list under the lock and release it before doing the (slow) network writes. This avoids head-of-line blocking caused by a slow reader.
- **Graceful shutdown.** `SIGINT` / `SIGTERM` set an `asyncio.Event`; the main coroutine awaits it, then closes the listener and lets in-flight handlers finish.

## Tests

```bash
pytest
```

The suite (`tests/test_server.py`) covers:

- Duplicate-username rejection
- Broadcast excludes the sender
- DM to a known user is delivered
- DM to an unknown user returns `False` (so the caller can surface "no such user")
- `unregister` cleans up empty rooms and the client map
- Concurrent registrations (20 in parallel) are safe under the global lock

CI runs on every push / PR via GitHub Actions (see `.github/workflows/ci.yml`).

## Run via Docker

```bash
docker build -t termchat .
docker run --rm -p 8765:8765 termchat
```

## Design notes / known limitations

- **Single process, single global lock.** Fine at the toy scale this targets (≤ 50 clients). For higher fan-out you'd shard by room and use per-room locks.
- **No persistence.** Messages are not stored; reconnect = fresh state.
- **No auth.** First-come-first-served usernames. A real version would add a token handshake.
- **Line-framed.** Reads up to `\n`. Pathologically long lines without a newline would block the per-connection coroutine — explicitly out of scope.

## License

MIT
