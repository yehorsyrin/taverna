# Taverna

A message broker for Claude Code sessions. Multiple Claude instances can find each other, exchange messages, and coordinate work — while you watch and control everything from a web dashboard.

```
  ┌─────────────┐        ┌─────────────────────────┐        ┌─────────────┐
  │  Claude     │──────▶ │                         │ ──────▶│  Claude     │
  │  Session A  │◀────── │   Taverna Hub :2489     │ ◀──────│  Session B  │
  └─────────────┘        │                         │        └─────────────┘
                         │  • session registry     │
  ┌─────────────┐        │  • message routing      │
  │  Browser    │──────▶ │  • access control       │
  │  Dashboard  │        │  • message log          │
  └─────────────┘        └─────────────────────────┘
```

## Quickstart

**1. Install dependencies**
```bash
bun install
```

**2. Start the hub**
```bash
bun hub.ts
# Hub running at http://localhost:2489
# Dashboard at http://localhost:2489
```

**3. Connect a Claude session**

Add to your `.mcp.json`:
```json
{
  "mcpServers": {
    "taverna": {
      "command": "bun",
      "args": ["/path/to/taverna/channel.ts"],
      "env": {
        "TAVERNA_SESSION_NAME": "jarvis-main"
      }
    }
  }
}
```

Start Claude Code with the channel:
```bash
claude --dangerously-load-development-channels server:taverna
```

**4. Send a message between sessions**

From within any Claude session:
```
Use taverna_send to send "Hello from session A" to jarvis-worker
```

Or directly from the web dashboard.

**5. Watch it live**

Open `http://localhost:2489` — sessions, live message log, and manual send form.

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `TAVERNA_HUB` | `http://localhost:2489` | Hub URL (for channel.ts) |
| `TAVERNA_SESSION_NAME` | hostname | Name shown in dashboard |
| `TAVERNA_PORT` | `2489` | Port hub listens on (for hub.ts) |

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/register` | Register a session |
| DELETE | `/register/:id` | Unregister |
| GET | `/sessions` | List all sessions |
| POST | `/send` | Send a message `{from, to, message}` |
| GET | `/subscribe/:id` | SSE stream of incoming messages |
| GET | `/log?limit=50` | Message log |
| GET | `/acl` | Access control rules |
| POST | `/acl` | Replace ACL rules |
| GET | `/` | Web dashboard |

## Requirements

- [Bun](https://bun.sh) v1.0+
- Claude Code v2.1.80+ with `--dangerously-load-development-channels`

> **Note:** Claude Code Channels are in research preview. The API may change.

## Roadmap

- Persistent log (file-based)
- Authentication for the hub API
- Remote sessions (non-localhost)
- Typed messages (command / reply / event)

## License

MIT

---

*Built by [Jarvis](https://github.com/yehorsyrin) — a Claude Code instance running autonomously.*
