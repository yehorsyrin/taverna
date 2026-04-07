#!/usr/bin/env bun
/**
 * Taverna Hub — central message broker for Claude Code sessions.
 * Usage: bun hub.ts
 */

const PORT = parseInt(Bun.env.TAVERNA_PORT ?? '2489')

// ─── Types ────────────────────────────────────────────────────────────────────

type SessionStatus = 'online' | 'offline'

interface Session {
  id: string
  name: string
  status: SessionStatus
  registered_at: string
  last_seen: string
}

interface Message {
  msg_id: string
  from: string
  to: string
  message: string
  ts: string
  status: 'delivered' | 'queued' | 'denied' | 'no_recipient'
}

interface AclRule {
  from: string  // session name or "*"
  to: string    // session name or "*"
  allow: boolean
}

// ─── State ────────────────────────────────────────────────────────────────────

const sessions = new Map<string, Session>()
const subscribers = new Map<string, Set<(data: string) => void>>()  // session_id → SSE writers
const queue = new Map<string, Message[]>()                           // session_id → pending msgs
const log: Message[] = []                                            // circular, max 1000
const dashboardListeners = new Set<(data: string) => void>()

let acl: AclRule[] = []  // empty = allow all

// ─── Helpers ─────────────────────────────────────────────────────────────────

function shortId(): string {
  return crypto.randomUUID().slice(0, 8)
}

function now(): string {
  return new Date().toISOString()
}

function sessionByName(name: string): Session | undefined {
  for (const s of sessions.values()) if (s.name === name) return s
}

function sessionById(id: string): Session | undefined {
  return sessions.get(id)
}

function isAllowed(from: string, to: string): boolean {
  if (acl.length === 0) return true
  for (const rule of acl) {
    const fromMatch = rule.from === '*' || rule.from === from
    const toMatch = rule.to === '*' || rule.to === to
    if (fromMatch && toMatch) return rule.allow
  }
  return true  // default allow if no rule matches
}

function addToLog(msg: Message) {
  log.push(msg)
  if (log.length > 1000) log.shift()
  // notify dashboard
  const data = `data: ${JSON.stringify(msg)}\n\n`
  for (const emit of dashboardListeners) emit(data)
}

function deliver(targetSession: Session, msg: Message): boolean {
  const subs = subscribers.get(targetSession.id)
  if (!subs || subs.size === 0) return false
  const data = `data: ${JSON.stringify(msg)}\n\n`
  for (const emit of subs) emit(data)
  return true
}

function enqueue(targetSession: Session, msg: Message) {
  if (!queue.has(targetSession.id)) queue.set(targetSession.id, [])
  queue.get(targetSession.id)!.push(msg)
}

function flushQueue(sessionId: string) {
  const pending = queue.get(sessionId)
  if (!pending || pending.length === 0) return
  const subs = subscribers.get(sessionId)
  if (!subs || subs.size === 0) return
  for (const msg of pending) {
    const data = `data: ${JSON.stringify(msg)}\n\n`
    for (const emit of subs) emit(data)
  }
  queue.delete(sessionId)
}

function pruneQueue() {
  const cutoff = Date.now() - 60 * 60 * 1000  // 1 hour TTL
  for (const [id, msgs] of queue.entries()) {
    const fresh = msgs.filter(m => new Date(m.ts).getTime() > cutoff)
    if (fresh.length === 0) queue.delete(id)
    else queue.set(id, fresh)
  }
}
setInterval(pruneQueue, 5 * 60 * 1000)

// ─── HTTP handler ─────────────────────────────────────────────────────────────

Bun.serve({
  port: PORT,
  hostname: '0.0.0.0',

  async fetch(req) {
    const url = new URL(req.url)
    const path = url.pathname
    const method = req.method

    // ── POST /register ──────────────────────────────────────────────────────
    if (method === 'POST' && path === '/register') {
      const body = await req.json() as { name?: string }
      if (!body.name) return json({ error: 'name required' }, 400)
      const id = shortId()
      const session: Session = {
        id,
        name: body.name,
        status: 'online',
        registered_at: now(),
        last_seen: now(),
      }
      sessions.set(id, session)
      return json({ session_id: id, name: body.name })
    }

    // ── DELETE /register/:id ─────────────────────────────────────────────────
    if (method === 'DELETE' && path.startsWith('/register/')) {
      const id = path.split('/')[2]
      const session = sessions.get(id)
      if (!session) return json({ error: 'not found' }, 404)
      session.status = 'offline'
      session.last_seen = now()
      subscribers.delete(id)
      return json({ ok: true })
    }

    // ── GET /sessions ────────────────────────────────────────────────────────
    if (method === 'GET' && path === '/sessions') {
      return json([...sessions.values()])
    }

    // ── POST /send ───────────────────────────────────────────────────────────
    if (method === 'POST' && path === '/send') {
      const body = await req.json() as { from?: string; to?: string; message?: string }
      if (!body.from || !body.to || !body.message)
        return json({ error: 'from, to, message required' }, 400)

      const delivered: string[] = []
      const queued: string[] = []
      const denied: string[] = []

      const targets: Session[] = body.to === '*'
        ? [...sessions.values()].filter(s => s.name !== body.from)
        : (() => { const s = sessionByName(body.to!); return s ? [s] : [] })()

      if (targets.length === 0 && body.to !== '*') {
        const msg: Message = { msg_id: shortId(), from: body.from, to: body.to, message: body.message, ts: now(), status: 'no_recipient' }
        addToLog(msg)
        return json({ delivered, queued, denied, error: 'recipient not found' }, 404)
      }

      for (const target of targets) {
        if (!isAllowed(body.from, target.name)) {
          denied.push(target.name)
          const msg: Message = { msg_id: shortId(), from: body.from, to: target.name, message: body.message, ts: now(), status: 'denied' }
          addToLog(msg)
          continue
        }
        const msg: Message = { msg_id: shortId(), from: body.from, to: target.name, message: body.message, ts: now(), status: 'queued' }
        if (deliver(target, msg)) {
          msg.status = 'delivered'
          delivered.push(target.name)
        } else {
          enqueue(target, msg)
          queued.push(target.name)
        }
        addToLog(msg)
      }

      return json({ delivered, queued, denied })
    }

    // ── GET /subscribe/:id ───────────────────────────────────────────────────
    if (method === 'GET' && path.startsWith('/subscribe/')) {
      const id = path.split('/')[2]
      const session = sessions.get(id)
      if (!session) return json({ error: 'session not found' }, 404)
      session.status = 'online'
      session.last_seen = now()

      if (!subscribers.has(id)) subscribers.set(id, new Set())

      const stream = new ReadableStream({
        start(ctrl) {
          const enc = new TextEncoder()
          const emit = (data: string) => ctrl.enqueue(enc.encode(data))
          subscribers.get(id)!.add(emit)

          // flush queued messages
          flushQueue(id)

          // heartbeat
          const hb = setInterval(() => ctrl.enqueue(enc.encode(': keep-alive\n\n')), 30_000)

          req.signal.addEventListener('abort', () => {
            clearInterval(hb)
            subscribers.get(id)?.delete(emit)
            if (session) {
              session.status = 'offline'
              session.last_seen = now()
            }
          })
        },
      })

      return new Response(stream, {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
        },
      })
    }

    // ── GET /log ─────────────────────────────────────────────────────────────
    if (method === 'GET' && path === '/log') {
      const limit = parseInt(url.searchParams.get('limit') ?? '50')
      const session = url.searchParams.get('session')
      let entries = session ? log.filter(m => m.from === session || m.to === session) : log
      return json(entries.slice(-limit).reverse())
    }

    // ── GET /acl ─────────────────────────────────────────────────────────────
    if (method === 'GET' && path === '/acl') {
      return json(acl)
    }

    // ── POST /acl ─────────────────────────────────────────────────────────────
    if (method === 'POST' && path === '/acl') {
      const body = await req.json() as { rules?: AclRule[] }
      if (!Array.isArray(body.rules)) return json({ error: 'rules array required' }, 400)
      acl = body.rules
      return json({ ok: true, rules: acl.length })
    }

    // ── POST /acl/rule ────────────────────────────────────────────────────────
    if (method === 'POST' && path === '/acl/rule') {
      const rule = await req.json() as AclRule
      if (!rule.from || !rule.to || typeof rule.allow !== 'boolean')
        return json({ error: 'from, to, allow required' }, 400)
      acl.push(rule)
      return json({ ok: true, index: acl.length - 1 })
    }

    // ── DELETE /acl/rule/:index ───────────────────────────────────────────────
    if (method === 'DELETE' && path.startsWith('/acl/rule/')) {
      const idx = parseInt(path.split('/')[3])
      if (isNaN(idx) || idx < 0 || idx >= acl.length)
        return json({ error: 'invalid index' }, 400)
      acl.splice(idx, 1)
      return json({ ok: true })
    }

    // ── GET /events (dashboard SSE) ───────────────────────────────────────────
    if (method === 'GET' && path === '/events') {
      const stream = new ReadableStream({
        start(ctrl) {
          const enc = new TextEncoder()
          const emit = (data: string) => ctrl.enqueue(enc.encode(data))
          dashboardListeners.add(emit)
          ctrl.enqueue(enc.encode(': connected\n\n'))
          const hb = setInterval(() => ctrl.enqueue(enc.encode(': keep-alive\n\n')), 30_000)
          req.signal.addEventListener('abort', () => {
            clearInterval(hb)
            dashboardListeners.delete(emit)
          })
        },
      })
      return new Response(stream, {
        headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
      })
    }

    // ── GET / (dashboard) ─────────────────────────────────────────────────────
    if (method === 'GET' && (path === '/' || path === '')) {
      return new Response(DASHBOARD_HTML, { headers: { 'Content-Type': 'text/html' } })
    }

    return json({ error: 'not found' }, 404)
  },
})

console.log(`
  ████████╗ █████╗ ██╗   ██╗███████╗██████╗ ███╗   ██╗ █████╗
     ██╔══╝██╔══██╗██║   ██║██╔════╝██╔══██╗████╗  ██║██╔══██╗
     ██║   ███████║██║   ██║█████╗  ██████╔╝██╔██╗ ██║███████║
     ██║   ██╔══██║╚██╗ ██╔╝██╔══╝  ██╔══██╗██║╚██╗██║██╔══██║
     ██║   ██║  ██║ ╚████╔╝ ███████╗██║  ██║██║ ╚████║██║  ██║
     ╚═╝   ╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝

  Message broker for Claude Code sessions
  Built by Jarvis · https://github.com/yehorsyrin/taverna

  Hub running at http://localhost:${PORT}
  Dashboard  → http://localhost:${PORT}/
`)

// ─── Dashboard HTML ───────────────────────────────────────────────────────────

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Taverna</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: monospace; background: #0d1117; color: #c9d1d9; min-height: 100vh; padding: 24px; }
    h1 { color: #f0883e; font-size: 1.5rem; margin-bottom: 24px; }
    h2 { color: #8b949e; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 10px; }
    .grid { display: grid; grid-template-columns: 300px 1fr; gap: 24px; }
    .panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
    .panel.full { grid-column: 1 / -1; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th { text-align: left; color: #8b949e; padding: 4px 8px; border-bottom: 1px solid #21262d; }
    td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
    .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
    .online { background: #3fb950; }
    .offline { background: #6e7681; }
    .log-entry { padding: 6px 0; border-bottom: 1px solid #21262d; font-size: 0.82rem; line-height: 1.4; }
    .log-entry:last-child { border-bottom: none; }
    .log-from { color: #58a6ff; }
    .log-to { color: #f0883e; }
    .log-ts { color: #6e7681; font-size: 0.75rem; }
    .log-status-delivered { color: #3fb950; }
    .log-status-queued { color: #d29922; }
    .log-status-denied { color: #f85149; }
    .log-status-no_recipient { color: #6e7681; }
    #log-list { max-height: 300px; overflow-y: auto; }
    form { display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
    input, textarea, select { background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; padding: 6px 10px; border-radius: 4px; font-family: monospace; font-size: 0.85rem; }
    textarea { resize: vertical; min-height: 60px; }
    button { background: #238636; color: #fff; border: none; padding: 7px 14px; border-radius: 4px; cursor: pointer; font-family: monospace; font-size: 0.85rem; }
    button:hover { background: #2ea043; }
    button.danger { background: #b62324; }
    button.danger:hover { background: #da3633; }
    .acl-row { display: flex; gap: 8px; align-items: center; font-size: 0.82rem; padding: 4px 0; border-bottom: 1px solid #21262d; }
    .allow { color: #3fb950; }
    .deny { color: #f85149; }
    .label { color: #8b949e; font-size: 0.75rem; margin-bottom: 2px; }
  </style>
</head>
<body>
  <h1>🍺 Taverna</h1>
  <div class="grid">
    <div>
      <div class="panel" style="margin-bottom:16px">
        <h2>Sessions</h2>
        <table>
          <thead><tr><th>Name</th><th>Status</th><th>Last seen</th></tr></thead>
          <tbody id="sessions-body"><tr><td colspan="3" style="color:#6e7681">Loading…</td></tr></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Send message</h2>
        <form id="send-form">
          <div><div class="label">From</div><input id="s-from" placeholder="session-name" required></div>
          <div><div class="label">To (name or *)</div><input id="s-to" placeholder="session-name or *" required></div>
          <div><div class="label">Message</div><textarea id="s-msg" required></textarea></div>
          <button type="submit">Send</button>
          <div id="send-result" style="font-size:0.8rem;color:#6e7681"></div>
        </form>
      </div>
    </div>

    <div>
      <div class="panel" style="margin-bottom:16px">
        <h2>Message log <span id="log-count" style="color:#6e7681;font-weight:normal"></span></h2>
        <div id="log-list"></div>
      </div>
      <div class="panel">
        <h2>Access control rules</h2>
        <div id="acl-list" style="margin-bottom:12px">Loading…</div>
        <form id="acl-form" style="flex-direction:row;flex-wrap:wrap;gap:8px;align-items:flex-end">
          <div><div class="label">From</div><input id="a-from" placeholder="* or name" style="width:120px"></div>
          <div><div class="label">To</div><input id="a-to" placeholder="* or name" style="width:120px"></div>
          <div><div class="label">Allow?</div>
            <select id="a-allow"><option value="true">Allow</option><option value="false">Deny</option></select>
          </div>
          <button type="submit">Add rule</button>
        </form>
      </div>
    </div>
  </div>

  <script>
    // ── Sessions ──────────────────────────────────────────────────────────────
    async function refreshSessions() {
      const r = await fetch('/sessions').then(r => r.json())
      const tbody = document.getElementById('sessions-body')
      if (!r.length) { tbody.innerHTML = '<tr><td colspan="3" style="color:#6e7681">No sessions</td></tr>'; return }
      tbody.innerHTML = r.map(s => \`
        <tr>
          <td><span class="dot \${s.status}"></span>\${s.name}</td>
          <td>\${s.status}</td>
          <td style="color:#6e7681">\${timeAgo(s.last_seen)}</td>
        </tr>\`).join('')
    }
    setInterval(refreshSessions, 5000)
    refreshSessions()

    // ── Log ───────────────────────────────────────────────────────────────────
    async function loadLog() {
      const msgs = await fetch('/log?limit=100').then(r => r.json())
      renderLog(msgs)
    }

    function renderLog(msgs) {
      const el = document.getElementById('log-list')
      document.getElementById('log-count').textContent = \`(\${msgs.length})\`
      el.innerHTML = msgs.map(m => \`
        <div class="log-entry">
          <span class="log-from">\${m.from}</span> → <span class="log-to">\${m.to}</span>
          <span class="log-status-\${m.status}" style="margin-left:8px">[\${m.status}]</span>
          <div>\${m.message}</div>
          <div class="log-ts">\${new Date(m.ts).toLocaleTimeString()}</div>
        </div>\`).join('')
    }
    loadLog()

    // Live log via SSE
    const es = new EventSource('/events')
    es.onmessage = e => {
      const msg = JSON.parse(e.data)
      const el = document.getElementById('log-list')
      const entry = document.createElement('div')
      entry.className = 'log-entry'
      entry.innerHTML = \`
        <span class="log-from">\${msg.from}</span> → <span class="log-to">\${msg.to}</span>
        <span class="log-status-\${msg.status}" style="margin-left:8px">[\${msg.status}]</span>
        <div>\${msg.message}</div>
        <div class="log-ts">\${new Date(msg.ts).toLocaleTimeString()}</div>\`
      el.insertBefore(entry, el.firstChild)
      document.getElementById('log-count').textContent = \`(\${el.children.length})\`
    }

    // ── Send form ─────────────────────────────────────────────────────────────
    document.getElementById('send-form').addEventListener('submit', async e => {
      e.preventDefault()
      const from = document.getElementById('s-from').value
      const to = document.getElementById('s-to').value
      const message = document.getElementById('s-msg').value
      const r = await fetch('/send', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({from,to,message}) }).then(r => r.json())
      document.getElementById('send-result').textContent = JSON.stringify(r)
    })

    // ── ACL ───────────────────────────────────────────────────────────────────
    async function refreshAcl() {
      const rules = await fetch('/acl').then(r => r.json())
      const el = document.getElementById('acl-list')
      if (!rules.length) { el.textContent = 'No rules — all traffic allowed.'; return }
      el.innerHTML = rules.map((r, i) => \`
        <div class="acl-row">
          <span>\${r.from} → \${r.to}</span>
          <span class="\${r.allow ? 'allow' : 'deny'}">\${r.allow ? 'ALLOW' : 'DENY'}</span>
          <button class="danger" onclick="deleteRule(\${i})" style="padding:2px 8px">×</button>
        </div>\`).join('')
    }
    refreshAcl()

    window.deleteRule = async (i) => {
      await fetch('/acl/rule/' + i, { method: 'DELETE' })
      refreshAcl()
    }

    document.getElementById('acl-form').addEventListener('submit', async e => {
      e.preventDefault()
      const from = document.getElementById('a-from').value
      const to = document.getElementById('a-to').value
      const allow = document.getElementById('a-allow').value === 'true'
      await fetch('/acl/rule', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({from,to,allow}) })
      refreshAcl()
      e.target.reset()
    })

    // ── Util ──────────────────────────────────────────────────────────────────
    function timeAgo(iso) {
      const s = Math.floor((Date.now() - new Date(iso)) / 1000)
      if (s < 60) return s + 's ago'
      if (s < 3600) return Math.floor(s/60) + 'm ago'
      return Math.floor(s/3600) + 'h ago'
    }
  </script>
</body>
</html>`
