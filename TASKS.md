# Taverna — Implementation Tasks

## Task 1: Project setup
**Goal:** Initialize the repo, install dependencies, create GitHub repository.
**Steps:**
- `bun init` in ~/jarvis/taverna/
- Install `@modelcontextprotocol/sdk`
- Create `.gitignore` (node_modules, *.log, .env)
- Create `README.md` with project description, architecture diagram (ASCII), quickstart
- `git init`, initial commit
- `gh repo create taverna --public`, push

**Done when:** `gh repo view yehor/taverna` returns the repo page.

---

## Task 2: Hub core — session registry
**Goal:** HTTP server with session register/unregister/list.
**File:** `hub.ts`
**Endpoints:**
- `POST /register` — body: `{name: string}` → response: `{session_id: string, name: string}`
- `DELETE /register/:id` — marks session offline
- `GET /sessions` — returns array of `{id, name, status, registered_at, last_seen}`
**In-memory store:** `Map<string, Session>` where Session = `{id, name, status, registered_at, last_seen}`
**Session ID generation:** `crypto.randomUUID()` (8-char prefix for readability)

**Done when:** curl register → get session_id; curl /sessions → see it listed; curl DELETE → status becomes "offline".

---

## Task 3: Hub messaging — SSE delivery
**Goal:** Sessions can subscribe to incoming messages; other sessions (or hub) can send messages to them.
**File:** `hub.ts` (extend)
**Endpoints:**
- `GET /subscribe/:id` — SSE stream; keeps connection open; emits `data: {msg_id, from, message, ts}\n\n` for each incoming message; updates session `last_seen` on each heartbeat
- `POST /send` — body: `{from: string, to: string, message: string}` where `to` can be session name, session id, or `"*"` for broadcast → delivers to all matching active subscribers; returns `{delivered: string[], queued: string[]}`
**Heartbeat:** every 30s emit `: keep-alive\n\n` to prevent proxy timeouts

**Done when:** two curl sessions — one subscribing, one sending — message arrives in real time.

---

## Task 4: Offline queue + message log
**Goal:** Messages to offline sessions are queued and delivered on reconnect; all messages are logged.
**File:** `hub.ts` (extend)
**Queue:** `Map<session_id, Message[]>` — messages with TTL of 1 hour; on `/subscribe`, flush queue before streaming new messages
**Log:** append-only in-memory array (max 1000 entries, circular); each entry: `{msg_id, from, to, message, ts, status}`
**Endpoint:**
- `GET /log?limit=50&session=name` — returns last N log entries, optionally filtered by session name

**Done when:** send to offline session → reconnect → message arrives; /log shows history.

---

## Task 5: ACL — access control
**Goal:** Yehor can define which sessions are allowed to send to which. Hub enforces rules on /send.
**File:** `hub.ts` (extend)
**Default rule:** allow all → all
**ACL store:** array of rules `{from: string|"*", to: string|"*", allow: boolean}`, evaluated top-to-bottom, first match wins
**Endpoints:**
- `GET /acl` — current rules
- `POST /acl` — body: `{rules: Rule[]}` — replace all rules
- `POST /acl/rule` — body: `{from, to, allow}` — append single rule
- `DELETE /acl/rule/:index` — remove rule by index
**Enforcement:** in `/send` handler, check ACL before delivering; if denied → 403 + log entry with status "denied"

**Done when:** add deny rule for session-A→session-B; send from A to B → 403; remove rule → delivery works.

---

## Task 6: Web dashboard
**Goal:** Browser UI showing sessions, live message log, manual send form, ACL editor.
**File:** `dashboard.html` (served by hub.ts at `GET /`)
**Sections:**
1. **Sessions panel** — table: name, status (green/grey dot), last seen; auto-refreshes via SSE or polling every 5s
2. **Message log** — scrolling list of messages with timestamp, from→to, message preview; updates in real time via `GET /events` SSE stream (hub broadcasts log entries to dashboard subscribers)
3. **Send form** — fields: From (text), To (text or "*"), Message (textarea); POST to /send
4. **ACL panel** — table of current rules with delete buttons; form to add new rule
**Tech:** vanilla HTML + JS, no frameworks, inline CSS; entire dashboard is one self-contained HTML string embedded in hub.ts

**Done when:** open localhost:2489 in browser, see live session list and message log updating without page refresh.

---

## Task 7: MCP channel server
**Goal:** TypeScript MCP server that Claude Code loads as a channel plugin. Connects a session to the hub.
**File:** `channel.ts`
**Config:** reads hub URL from env `TAVERNA_HUB` (default: `http://localhost:2489`) and session name from `TAVERNA_SESSION_NAME` (default: hostname)
**On startup:**
1. POST /register → get session_id, store it
2. Open SSE /subscribe/:id in background loop
3. For each incoming message: emit `notifications/claude/channel` with content=message, meta={from, msg_id}
**On shutdown (SIGTERM/SIGINT):** DELETE /register/:id
**MCP tools exposed to Claude:**
- `taverna_send` — args: `{to: string, message: string}` → POST /send → return delivery result
- `taverna_sessions` — no args → GET /sessions → return formatted list
- `taverna_log` — args: `{limit?: number}` → GET /log → return recent messages
**Channel instructions** (added to Claude's system prompt):
```
Messages from other Jarvis sessions arrive as <channel source="taverna" from="SESSION_NAME" msg_id="...">. 
Read them and respond using taverna_send. Use taverna_sessions to see who is online.
```

**Done when:** two separate `claude` processes, each with channel.ts loaded, can exchange messages through hub.

---

## Task 8: README + polish
**Goal:** Public-ready repository with clear documentation.
**README.md sections:**
- What is Taverna (2-3 sentences)
- ASCII architecture diagram
- Quickstart: install, run hub, connect session, send first message (5 steps, copy-paste ready)
- Configuration (env vars table)
- API reference (table of endpoints)
- Roadmap (persistence, auth, remote sessions)
- License: MIT
**Other:**
- `LICENSE` file (MIT)
- `package.json` with `scripts: {hub: "bun hub.ts", channel: "bun channel.ts"}`
- Final review of all files for typos, dead code

**Done when:** a developer who has never seen the project can run it from README alone.
