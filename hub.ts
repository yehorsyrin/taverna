#!/usr/bin/env bun
/**
 * Taverna Hub — central message broker for Claude Code sessions.
 * Usage: bun hub.ts
 *
 * Configuration (env vars):
 *   TAVERNA_PORT        Port to listen on (default: 2489)
 *   TAVERNA_API_KEY     If set, all API requests require Authorization: Bearer <key>.
 *                       Dashboard (GET /), SSE events (GET /events), health (GET /health),
 *                       and webhook (POST /webhook) are always public.
 *   TAVERNA_LOG_FILE    Path to persist message log (default: taverna.log in cwd)
 *   TAVERNA_WEBHOOK_KEY If set, POST /webhook requires X-Webhook-Key: <key>
 */

import { existsSync, readFileSync, appendFileSync } from 'fs'

const PORT        = parseInt(Bun.env.TAVERNA_PORT ?? '2489')
const API_KEY     = Bun.env.TAVERNA_API_KEY ?? null
const WEBHOOK_KEY = Bun.env.TAVERNA_WEBHOOK_KEY ?? null
const LOG_FILE    = Bun.env.TAVERNA_LOG_FILE ?? 'taverna.log'
const START_TIME  = Date.now()
const VERSION     = '1.0.0'

// ─── Types ────────────────────────────────────────────────────────────────────

type SessionStatus = 'online' | 'offline'
type MessageType   = 'message' | 'command' | 'reply' | 'event' | 'broadcast'

interface Session {
  id:              string
  name:            string
  status:          SessionStatus
  registered_at:   string
  last_seen:       string
  msg_sent:        number
  msg_received:    number
}

interface Message {
  msg_id:    string
  from:      string
  to:        string
  message:   string
  type:      MessageType
  reply_to?: string
  ts:        string
  status:    'delivered' | 'queued' | 'denied' | 'no_recipient'
}

interface AclRule {
  from:  string   // session name or "*"
  to:    string   // session name or "*"
  allow: boolean
}

// ─── State ────────────────────────────────────────────────────────────────────

const sessions          = new Map<string, Session>()
const subscribers       = new Map<string, Set<(d: string) => void>>()
const queue             = new Map<string, Message[]>()
const log: Message[]    = []
const dashboardListeners = new Set<(d: string) => void>()
let   acl: AclRule[]    = []
let   totalMessages     = 0

// ─── Helpers ─────────────────────────────────────────────────────────────────

const shortId  = () => crypto.randomUUID().slice(0, 8)
const now      = () => new Date().toISOString()
const uptime   = () => Math.floor((Date.now() - START_TIME) / 1000)

function sessionByName(name: string): Session | undefined {
  // prefer online session; fall back to most recently seen
  let fallback: Session | undefined
  for (const s of sessions.values()) {
    if (s.name !== name) continue
    if (s.status === 'online') return s
    if (!fallback || s.last_seen > fallback.last_seen) fallback = s
  }
  return fallback
}

function isAllowed(from: string, to: string): boolean {
  if (acl.length === 0) return true
  for (const rule of acl) {
    if ((rule.from === '*' || rule.from === from) && (rule.to === '*' || rule.to === to))
      return rule.allow
  }
  return true
}

function checkAuth(req: Request): boolean {
  if (!API_KEY) return true
  return req.headers.get('Authorization') === `Bearer ${API_KEY}`
}

function notifyDashboard(event: string, payload: unknown) {
  const data = `data: ${JSON.stringify({ event, payload })}\n\n`
  for (const emit of dashboardListeners) emit(data)
}

function addToLog(msg: Message) {
  log.push(msg)
  totalMessages++
  if (log.length > 1000) log.shift()
  try { appendFileSync(LOG_FILE, JSON.stringify(msg) + '\n') } catch { /* best effort */ }
  notifyDashboard('message', msg)
}

function deliver(target: Session, msg: Message): boolean {
  const subs = subscribers.get(target.id)
  if (!subs || subs.size === 0) return false
  const data = `data: ${JSON.stringify(msg)}\n\n`
  for (const emit of subs) emit(data)
  target.msg_received++
  return true
}

function enqueue(target: Session, msg: Message) {
  if (!queue.has(target.id)) queue.set(target.id, [])
  queue.get(target.id)!.push(msg)
}

function flushQueue(sessionId: string) {
  const pending = queue.get(sessionId)
  if (!pending?.length) return
  const subs = subscribers.get(sessionId)
  if (!subs?.size) return
  const session = sessions.get(sessionId)
  for (const msg of pending) {
    const data = `data: ${JSON.stringify(msg)}\n\n`
    for (const emit of subs) emit(data)
    if (session) session.msg_received++
  }
  queue.delete(sessionId)
}

function pruneQueue() {
  const cutoff = Date.now() - 60 * 60 * 1000
  for (const [id, msgs] of queue.entries()) {
    const fresh = msgs.filter(m => new Date(m.ts).getTime() > cutoff)
    if (!fresh.length) queue.delete(id)
    else queue.set(id, fresh)
  }
}

function dispatchMessage(body: {
  from: string; to: string; message: string
  type?: MessageType; reply_to?: string
}): { delivered: string[]; queued: string[]; denied: string[]; error?: string } {
  const delivered: string[] = []
  const queued: string[]    = []
  const denied: string[]    = []
  const type: MessageType   = body.type ?? (body.to === '*' ? 'broadcast' : 'message')

  const targets: Session[] = body.to === '*'
    ? [...sessions.values()].filter(s => s.name !== body.from)
    : (() => { const s = sessionByName(body.to); return s ? [s] : [] })()

  if (!targets.length && body.to !== '*') {
    const msg: Message = { msg_id: shortId(), from: body.from, to: body.to, message: body.message, type, reply_to: body.reply_to, ts: now(), status: 'no_recipient' }
    addToLog(msg)
    return { delivered, queued, denied, error: 'recipient not found' }
  }

  for (const target of targets) {
    if (!isAllowed(body.from, target.name)) {
      denied.push(target.name)
      addToLog({ msg_id: shortId(), from: body.from, to: target.name, message: body.message, type, reply_to: body.reply_to, ts: now(), status: 'denied' })
      continue
    }
    const msg: Message = { msg_id: shortId(), from: body.from, to: target.name, message: body.message, type, reply_to: body.reply_to, ts: now(), status: 'queued' }
    const sender = sessionByName(body.from)
    if (sender) sender.msg_sent++
    if (deliver(target, msg)) {
      msg.status = 'delivered'; delivered.push(target.name)
    } else {
      enqueue(target, msg); queued.push(target.name)
    }
    addToLog(msg)
  }
  notifyDashboard('sessions', [...sessions.values()])
  return { delivered, queued, denied }
}

// ─── Load persisted log ───────────────────────────────────────────────────────
if (existsSync(LOG_FILE)) {
  try {
    const lines = readFileSync(LOG_FILE, 'utf8').trim().split('\n').filter(Boolean)
    for (const line of lines) {
      try { log.push(JSON.parse(line)) } catch { /* skip malformed */ }
    }
    if (log.length > 1000) log.splice(0, log.length - 1000)
    totalMessages = log.length
  } catch { /* ignore */ }
}

setInterval(pruneQueue, 5 * 60 * 1000)

// ─── HTTP ────────────────────────────────────────────────────────────────────

Bun.serve({
  port: PORT,
  hostname: '0.0.0.0',
  idleTimeout: 0,

  async fetch(req) {
    const url    = new URL(req.url)
    const path   = url.pathname
    const method = req.method

    // CORS for local dev
    const cors = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Authorization,Content-Type,X-Webhook-Key' }
    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: cors })

    // ── Public: dashboard ────────────────────────────────────────────────────
    if (method === 'GET' && (path === '/' || path === '')) {
      return new Response(DASHBOARD_HTML, { headers: { 'Content-Type': 'text/html', ...cors } })
    }

    // ── Public: dashboard SSE ────────────────────────────────────────────────
    if (method === 'GET' && path === '/events') {
      const stream = new ReadableStream({
        start(ctrl) {
          const enc = new TextEncoder()
          const emit = (d: string) => { try { ctrl.enqueue(enc.encode(d)) } catch { /* ignore */ } }
          dashboardListeners.add(emit)
          emit(': connected\n\n')
          // send current state immediately
          emit(`data: ${JSON.stringify({ event: 'init', payload: { sessions: [...sessions.values()], log: log.slice(-100).reverse(), acl } })}\n\n`)
          const hb = setInterval(() => emit(': keep-alive\n\n'), 25_000)
          req.signal.addEventListener('abort', () => { clearInterval(hb); dashboardListeners.delete(emit) })
        },
      })
      return new Response(stream, { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', ...cors } })
    }

    // ── Public: health ───────────────────────────────────────────────────────
    if (method === 'GET' && path === '/health') {
      const online = [...sessions.values()].filter(s => s.status === 'online').length
      return new Response(JSON.stringify({
        status: 'ok', version: VERSION,
        uptime: uptime(),
        sessions: { total: sessions.size, online },
        messages: { total: totalMessages, in_memory: log.length },
        queue_depth: [...queue.values()].reduce((n, q) => n + q.length, 0),
        acl_rules: acl.length,
        api_auth: !!API_KEY,
        webhook_auth: !!WEBHOOK_KEY,
      }), { headers: { 'Content-Type': 'application/json', ...cors } })
    }

    // ── Public: webhook (external push) ─────────────────────────────────────
    if (method === 'POST' && path === '/webhook') {
      if (WEBHOOK_KEY && req.headers.get('X-Webhook-Key') !== WEBHOOK_KEY)
        return new Response(JSON.stringify({ error: 'unauthorized' }), { status: 401, headers: cors })
      const body = await req.json() as { to?: string; message?: string; from?: string; type?: MessageType }
      if (!body.to || !body.message)
        return new Response(JSON.stringify({ error: 'to, message required' }), { status: 400, headers: cors })
      const result = dispatchMessage({ from: body.from ?? 'webhook', to: body.to, message: body.message, type: body.type ?? 'event' })
      return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json', ...cors } })
    }

    // ── Auth check ───────────────────────────────────────────────────────────
    if (!checkAuth(req)) {
      return new Response(JSON.stringify({ error: 'unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json', ...cors } })
    }

    // ── POST /register ───────────────────────────────────────────────────────
    if (method === 'POST' && path === '/register') {
      const body = await req.json() as { name?: string }
      if (!body.name) return jsonRes({ error: 'name required' }, 400)
      // evict any previous sessions with the same name
      for (const [eid, es] of sessions.entries()) {
        if (es.name === body.name) { subscribers.delete(eid); sessions.delete(eid) }
      }
      const id = shortId()
      sessions.set(id, { id, name: body.name, status: 'online', registered_at: now(), last_seen: now(), msg_sent: 0, msg_received: 0 })
      notifyDashboard('sessions', [...sessions.values()])
      return jsonRes({ session_id: id, name: body.name })
    }

    // ── DELETE /register/:id ─────────────────────────────────────────────────
    if (method === 'DELETE' && path.startsWith('/register/')) {
      const s = sessions.get(path.split('/')[2])
      if (!s) return jsonRes({ error: 'not found' }, 404)
      s.status = 'offline'; s.last_seen = now()
      subscribers.delete(s.id)
      notifyDashboard('sessions', [...sessions.values()])
      return jsonRes({ ok: true })
    }

    // ── POST /ping/:id ───────────────────────────────────────────────────────
    if (method === 'POST' && path.startsWith('/ping/')) {
      const s = sessions.get(path.split('/')[2])
      if (!s) return jsonRes({ error: 'not found' }, 404)
      s.last_seen = now(); s.status = 'online'
      return jsonRes({ ok: true })
    }

    // ── POST /sessions/cleanup ───────────────────────────────────────────────
    if (method === 'POST' && path === '/sessions/cleanup') {
      let removed = 0
      for (const [id, s] of sessions.entries()) {
        if (s.status === 'offline') { subscribers.delete(id); sessions.delete(id); removed++ }
      }
      notifyDashboard('sessions', [...sessions.values()])
      return jsonRes({ removed })
    }

    // ── GET /sessions ────────────────────────────────────────────────────────
    if (method === 'GET' && path === '/sessions') {
      return jsonRes([...sessions.values()])
    }

    // ── POST /send ───────────────────────────────────────────────────────────
    if (method === 'POST' && path === '/send') {
      const body = await req.json() as { from?: string; to?: string; message?: string; type?: MessageType; reply_to?: string }
      if (!body.from || !body.to || !body.message) return jsonRes({ error: 'from, to, message required' }, 400)
      const result = dispatchMessage(body as { from: string; to: string; message: string; type?: MessageType; reply_to?: string })
      const status = result.error ? 404 : 200
      return jsonRes(result, status)
    }

    // ── GET /subscribe/:id ───────────────────────────────────────────────────
    if (method === 'GET' && path.startsWith('/subscribe/')) {
      const id = path.split('/')[2]
      const s  = sessions.get(id)
      if (!s) return jsonRes({ error: 'session not found' }, 404)
      s.status = 'online'; s.last_seen = now()
      if (!subscribers.has(id)) subscribers.set(id, new Set())

      const stream = new ReadableStream({
        start(ctrl) {
          const enc  = new TextEncoder()
          const emit = (d: string) => { try { ctrl.enqueue(enc.encode(d)) } catch { /* ignore */ } }
          subscribers.get(id)!.add(emit)
          flushQueue(id)
          const hb = setInterval(() => emit(': keep-alive\n\n'), 25_000)
          req.signal.addEventListener('abort', () => {
            clearInterval(hb)
            subscribers.get(id)?.delete(emit)
            if (s) { s.status = 'offline'; s.last_seen = now() }
            notifyDashboard('sessions', [...sessions.values()])
          })
        },
      })
      return new Response(stream, { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', ...cors } })
    }

    // ── GET /log ─────────────────────────────────────────────────────────────
    if (method === 'GET' && path === '/log') {
      const limit   = parseInt(url.searchParams.get('limit') ?? '100')
      const session = url.searchParams.get('session')
      const type    = url.searchParams.get('type')
      let entries   = log.slice()
      if (session) entries = entries.filter(m => m.from === session || m.to === session)
      if (type)    entries = entries.filter(m => m.type === type)
      return jsonRes(entries.slice(-limit).reverse())
    }

    // ── ACL ───────────────────────────────────────────────────────────────────
    if (method === 'GET'  && path === '/acl')        return jsonRes(acl)
    if (method === 'POST' && path === '/acl') {
      const body = await req.json() as { rules?: AclRule[] }
      if (!Array.isArray(body.rules)) return jsonRes({ error: 'rules array required' }, 400)
      acl = body.rules
      notifyDashboard('acl', acl)
      return jsonRes({ ok: true, rules: acl.length })
    }
    if (method === 'POST' && path === '/acl/rule') {
      const rule = await req.json() as AclRule
      if (!rule.from || !rule.to || typeof rule.allow !== 'boolean') return jsonRes({ error: 'from, to, allow required' }, 400)
      acl.push(rule)
      notifyDashboard('acl', acl)
      return jsonRes({ ok: true, index: acl.length - 1 })
    }
    if (method === 'DELETE' && path.startsWith('/acl/rule/')) {
      const idx = parseInt(path.split('/')[3])
      if (isNaN(idx) || idx < 0 || idx >= acl.length) return jsonRes({ error: 'invalid index' }, 400)
      acl.splice(idx, 1)
      notifyDashboard('acl', acl)
      return jsonRes({ ok: true })
    }

    return jsonRes({ error: 'not found' }, 404)
  },
})

function jsonRes(data: unknown, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  })
}

console.log(`
  ████████╗ █████╗ ██╗   ██╗███████╗██████╗ ███╗   ██╗ █████╗
     ██╔══╝██╔══██╗██║   ██║██╔════╝██╔══██╗████╗  ██║██╔══██╗
     ██║   ███████║██║   ██║█████╗  ██████╔╝██╔██╗ ██║███████║
     ██║   ██╔══██║╚██╗ ██╔╝██╔══╝  ██╔══██╗██║╚██╗██║██╔══██║
     ██║   ██║  ██║ ╚████╔╝ ███████╗██║  ██║██║ ╚████║██║  ██║
     ╚═╝   ╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝

  Message broker for Claude Code sessions  v${VERSION}
  Built by Jarvis · https://github.com/yehorsyrin/taverna

  Hub      → http://localhost:${PORT}
  Dashboard → http://localhost:${PORT}/
  Health   → http://localhost:${PORT}/health
  Webhook  → POST http://localhost:${PORT}/webhook
  Auth     → ${API_KEY ? '🔒 API key required' : '🔓 No auth (set TAVERNA_API_KEY to enable)'}
`)

// ─── Dashboard ────────────────────────────────────────────────────────────────

const DASHBOARD_HTML = /* html */`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🍺 Taverna</title>
  <style>
    :root {
      --bg:       #0d1117;
      --bg2:      #161b22;
      --bg3:      #21262d;
      --border:   #30363d;
      --text:     #e6edf3;
      --muted:    #8b949e;
      --accent:   #f0883e;
      --blue:     #58a6ff;
      --green:    #3fb950;
      --yellow:   #d29922;
      --red:      #f85149;
      --purple:   #bc8cff;
      --cyan:     #39d3f5;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'SF Mono', 'Fira Code', monospace; background: var(--bg); color: var(--text); min-height: 100vh; display: flex; flex-direction: column; }

    /* ── Header ── */
    header {
      background: var(--bg2);
      border-bottom: 1px solid var(--border);
      padding: 12px 24px;
      display: flex;
      align-items: center;
      gap: 24px;
      position: sticky;
      top: 0;
      z-index: 100;
    }
    .logo { color: var(--accent); font-size: 1.2rem; font-weight: 700; letter-spacing: -0.02em; white-space: nowrap; }
    .logo span { opacity: 0.6; font-size: 0.75rem; margin-left: 6px; vertical-align: middle; }
    .stats { display: flex; gap: 16px; flex: 1; }
    .stat { display: flex; align-items: center; gap: 6px; font-size: 0.78rem; color: var(--muted); }
    .stat strong { color: var(--text); }
    .conn-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--green); animation: pulse 2s infinite; flex-shrink: 0; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
    .conn-dot.disconnected { background: var(--red); animation: none; }

    /* ── Layout ── */
    .main { display: flex; flex: 1; overflow: hidden; }
    aside {
      width: 220px;
      flex-shrink: 0;
      background: var(--bg2);
      border-right: 1px solid var(--border);
      overflow-y: auto;
      padding: 16px 12px;
    }
    .main-content { flex: 1; display: flex; flex-direction: column; overflow: hidden; }

    /* ── Sidebar sessions ── */
    .sidebar-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted); margin-bottom: 10px; padding-left: 4px; }
    .session-card {
      padding: 8px 10px;
      border-radius: 6px;
      margin-bottom: 6px;
      cursor: pointer;
      border: 1px solid transparent;
      transition: background 0.15s, border-color 0.15s;
    }
    .session-card:hover { background: var(--bg3); }
    .session-card.active { background: var(--bg3); border-color: var(--border); }
    .session-card.filter-active { border-color: var(--blue) !important; }
    .sc-name { font-size: 0.85rem; display: flex; align-items: center; gap: 6px; }
    .sc-label { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .sc-send-btn { background: none; border: none; cursor: pointer; font-size: 0.8rem; opacity: 0; transition: opacity 0.15s; padding: 0 2px; line-height: 1; }
    .session-card:hover .sc-send-btn { opacity: 1; }
    .sc-meta { font-size: 0.72rem; color: var(--muted); margin-top: 3px; display: flex; gap: 8px; justify-content: space-between; }
    .dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
    .dot.online  { background: var(--green); }
    .dot.offline { background: var(--muted); }

    /* ── Tabs ── */
    .tabs { display: flex; gap: 2px; padding: 12px 20px 0; border-bottom: 1px solid var(--border); background: var(--bg2); flex-shrink: 0; }
    .tab { padding: 7px 14px; font-size: 0.8rem; cursor: pointer; border-radius: 4px 4px 0 0; color: var(--muted); border: 1px solid transparent; border-bottom: none; transition: color 0.15s; margin-bottom: -1px; }
    .tab:hover { color: var(--text); }
    .tab.active { color: var(--accent); background: var(--bg); border-color: var(--border); }

    /* ── Tab panels ── */
    .panels { flex: 1; overflow: hidden; }
    .panel { display: none; height: 100%; overflow-y: auto; padding: 20px; }
    .panel.active { display: flex; flex-direction: column; gap: 16px; }

    /* ── Log panel ── */
    .log-toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .log-toolbar input, .log-toolbar select { background: var(--bg2); border: 1px solid var(--border); color: var(--text); padding: 5px 10px; border-radius: 4px; font-family: inherit; font-size: 0.8rem; }
    .log-toolbar input { flex: 1; min-width: 140px; }
    .log-entries { flex: 1; }
    .log-entry {
      padding: 9px 12px;
      border-radius: 5px;
      background: var(--bg2);
      border: 1px solid var(--border);
      margin-bottom: 6px;
      font-size: 0.82rem;
      line-height: 1.5;
      transition: border-color 0.1s;
    }
    .log-entry:hover { border-color: var(--accent); }
    .log-entry.new { animation: fadein 0.3s ease; }
    @keyframes fadein { from{opacity:0;transform:translateY(-4px)} to{opacity:1;transform:none} }
    .le-header { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .le-from { color: var(--blue); font-weight: 600; }
    .le-to   { color: var(--accent); font-weight: 600; }
    .le-type { font-size: 0.68rem; padding: 1px 5px; border-radius: 3px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
    .le-type.message   { background: #1f3a5f; color: var(--blue); }
    .le-type.command   { background: #3d2a00; color: var(--yellow); }
    .le-type.reply     { background: #1a3a1a; color: var(--green); }
    .le-type.event     { background: #2a1a3a; color: var(--purple); }
    .le-type.broadcast { background: #1a3a3a; color: var(--cyan); }
    .le-status { font-size: 0.7rem; margin-left: auto; }
    .le-status.delivered  { color: var(--green); }
    .le-status.queued     { color: var(--yellow); }
    .le-status.denied     { color: var(--red); }
    .le-status.no_recipient { color: var(--muted); }
    .le-body { margin-top: 5px; color: var(--text); word-break: break-word; }
    .le-reply { font-size: 0.72rem; color: var(--muted); margin-top: 2px; }
    .le-reply-btn { background: none; border: none; color: var(--muted); font-size: 0.7rem; cursor: pointer; margin-left: auto; padding: 2px 6px; border-radius: 3px; opacity: 0; transition: opacity 0.15s; font-family: inherit; }
    .le-reply-btn:hover { color: var(--blue); background: var(--bg3); }
    .log-entry:hover .le-reply-btn { opacity: 1; }
    .le-ts { font-size: 0.7rem; color: var(--muted); margin-top: 3px; }
    .log-empty { color: var(--muted); text-align: center; padding: 40px; font-size: 0.85rem; }

    /* ── Send panel ── */
    .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .form-group { display: flex; flex-direction: column; gap: 5px; }
    .form-group.full { grid-column: 1/-1; }
    label { font-size: 0.72rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
    input, textarea, select {
      background: var(--bg2);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 8px 10px;
      border-radius: 5px;
      font-family: inherit;
      font-size: 0.85rem;
      transition: border-color 0.15s;
    }
    input:focus, textarea:focus, select:focus { outline: none; border-color: var(--blue); }
    textarea { resize: vertical; min-height: 80px; }
    .btn { padding: 8px 16px; border-radius: 5px; border: none; cursor: pointer; font-family: inherit; font-size: 0.85rem; font-weight: 600; transition: opacity 0.15s; }
    .btn:hover { opacity: 0.85; }
    .btn-primary { background: var(--green); color: #000; }
    .btn-danger  { background: var(--red);   color: #fff; }
    .btn-muted   { background: var(--bg3);   color: var(--text); border: 1px solid var(--border); }
    .result-box { background: var(--bg2); border: 1px solid var(--border); border-radius: 5px; padding: 10px; font-size: 0.8rem; color: var(--muted); }
    .result-box.ok    { border-color: var(--green); color: var(--green); }
    .result-box.error { border-color: var(--red);   color: var(--red); }

    /* ── ACL panel ── */
    .acl-rule {
      display: flex; align-items: center; gap: 10px;
      padding: 9px 12px; background: var(--bg2); border: 1px solid var(--border);
      border-radius: 5px; margin-bottom: 6px; font-size: 0.83rem;
    }
    .acl-rule .from { color: var(--blue); }
    .acl-rule .arrow { color: var(--muted); }
    .acl-rule .to   { color: var(--accent); }
    .acl-rule .verdict { margin-left: auto; font-weight: 700; font-size: 0.75rem; padding: 2px 7px; border-radius: 3px; }
    .acl-rule .verdict.allow { background: #1a3a1a; color: var(--green); }
    .acl-rule .verdict.deny  { background: #3a1a1a; color: var(--red); }
    .acl-empty { color: var(--muted); font-size: 0.85rem; padding: 12px 0; }
    .acl-default { font-size: 0.78rem; color: var(--muted); margin-bottom: 12px; padding: 8px 12px; background: var(--bg2); border-radius: 5px; border-left: 3px solid var(--green); }

    /* ── Webhook panel ── */
    .code-block { background: var(--bg2); border: 1px solid var(--border); border-radius: 5px; padding: 12px 14px; font-size: 0.8rem; color: var(--cyan); position: relative; }
    .copy-btn { position: absolute; top: 8px; right: 8px; background: var(--bg3); border: 1px solid var(--border); color: var(--muted); padding: 3px 8px; border-radius: 4px; cursor: pointer; font-size: 0.72rem; font-family: inherit; }
    .copy-btn:hover { color: var(--text); }
    .section-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 8px; }
    .info-row { display: flex; gap: 12px; align-items: baseline; padding: 6px 0; border-bottom: 1px solid var(--border); font-size: 0.83rem; }
    .info-row:last-child { border-bottom: none; }
    .info-key   { color: var(--muted); width: 140px; flex-shrink: 0; }
    .info-value { color: var(--text); word-break: break-all; }

    /* ── Health panel ── */
    .health-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }
    .health-card { background: var(--bg2); border: 1px solid var(--border); border-radius: 6px; padding: 14px 16px; }
    .hc-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 6px; }
    .hc-value { font-size: 1.5rem; font-weight: 700; color: var(--text); }
    .hc-value.ok     { color: var(--green); }
    .hc-value.warn   { color: var(--yellow); }
    .hc-value.small  { font-size: 1rem; }
    .hc-sub   { font-size: 0.72rem; color: var(--muted); margin-top: 4px; }

    .label-hint { font-size: 0.68rem; color: var(--muted); font-weight: 400; text-transform: none; letter-spacing: 0; }
    details summary { list-style: none; }
    details summary::-webkit-details-marker { display: none; }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

    /* ── Responsive ── */
    @media (max-width: 680px) {
      aside { display: none; }
      .form-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>

<header>
  <div class="logo">🍺 Taverna <span>v${VERSION}</span></div>
  <div class="stats">
    <div class="stat"><div class="conn-dot" id="conn-dot"></div> <span id="stat-online">—</span> online</div>
    <div class="stat">📨 <strong id="stat-msgs">—</strong> messages</div>
    <div class="stat">⏱ <strong id="stat-uptime">—</strong></div>
  </div>
</header>

<div class="main">
  <aside>
    <div class="sidebar-title" style="display:flex;justify-content:space-between;align-items:center">
      <span>Sessions</span>
      <button class="btn btn-muted" style="font-size:0.65rem;padding:2px 6px" onclick="clearOffline()" title="Remove all offline sessions">Clear offline</button>
    </div>
    <div id="session-list"><div style="color:var(--muted);font-size:0.8rem;padding:8px 4px">No sessions</div></div>
  </aside>

  <div class="main-content">
    <div class="tabs">
      <div class="tab active" onclick="showTab('log')">📋 Log</div>
      <div class="tab" onclick="showTab('send')">📤 Send</div>
      <div class="tab" onclick="showTab('acl')">🛡 ACL</div>
      <div class="tab" onclick="showTab('webhook')">🔗 Webhook</div>
      <div class="tab" onclick="showTab('health')">💚 Health</div>
    </div>

    <div class="panels">

      <!-- LOG -->
      <div class="panel active" id="panel-log">
        <div class="log-toolbar">
          <input id="log-search" placeholder="Search messages…" oninput="filterLog()">
          <select id="log-session" onchange="filterLog()">
            <option value="">All sessions</option>
          </select>
          <select id="log-type" onchange="filterLog()">
            <option value="">All types</option>
            <option>message</option><option>command</option>
            <option>reply</option><option>event</option><option>broadcast</option>
          </select>
          <button class="btn btn-muted" onclick="clearLogFilter()">Clear</button>
        </div>
        <div class="log-entries" id="log-entries">
          <div class="log-empty">No messages yet</div>
        </div>
      </div>

      <!-- SEND -->
      <div class="panel" id="panel-send">
        <datalist id="session-names"></datalist>
        <div class="form-grid">
          <div class="form-group">
            <label>From <span class="label-hint">(your session name)</span></label>
            <input id="s-from" placeholder="dashboard" list="session-names" autocomplete="off">
          </div>
          <div class="form-group">
            <label>To <span class="label-hint">(session name or * for all)</span></label>
            <input id="s-to" placeholder="session-name or *" list="session-names" autocomplete="off">
          </div>
          <div class="form-group">
            <label>Type</label>
            <select id="s-type">
              <option value="message">message</option>
              <option value="command">command</option>
              <option value="event">event</option>
              <option value="broadcast">broadcast</option>
              <option value="reply">reply</option>
            </select>
          </div>
          <div class="form-group full">
            <label>Message</label>
            <textarea id="s-msg" placeholder="Type your message…" onkeydown="if(event.ctrlKey&&event.key==='Enter')sendMessage()"></textarea>
            <span class="label-hint" style="margin-top:3px">Ctrl+Enter to send</span>
          </div>
          <div class="form-group full">
            <details>
              <summary style="cursor:pointer;font-size:0.75rem;color:var(--muted);user-select:none">Advanced</summary>
              <div class="form-group" style="margin-top:8px">
                <label>Reply-to msg_id</label>
                <input id="s-reply-to" placeholder="leave empty if not a reply">
              </div>
            </details>
          </div>
          <div class="form-group full">
            <button class="btn btn-primary" onclick="sendMessage()">Send</button>
          </div>
          <div class="form-group full">
            <div class="result-box" id="send-result" style="display:none"></div>
          </div>
        </div>
      </div>

      <!-- ACL -->
      <div class="panel" id="panel-acl">
        <div class="acl-default" id="acl-default">Default: <strong>allow all</strong> — no rules defined.</div>
        <div id="acl-rules"></div>
        <div style="margin-top:4px">
          <div class="section-title">Add rule</div>
          <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
            <div class="form-group"><label>From</label><input id="a-from" placeholder="* or name" style="width:130px"></div>
            <div class="form-group"><label>To</label><input id="a-to" placeholder="* or name" style="width:130px"></div>
            <div class="form-group"><label>Verdict</label>
              <select id="a-allow" style="width:100px">
                <option value="true">ALLOW</option>
                <option value="false">DENY</option>
              </select>
            </div>
            <button class="btn btn-primary" onclick="addRule()">Add</button>
          </div>
          <div style="margin-top:12px;font-size:0.75rem;color:var(--muted)">
            Rules are evaluated top-to-bottom. First match wins. Use * as wildcard. Default is allow-all if no rules.
          </div>
        </div>
      </div>

      <!-- WEBHOOK -->
      <div class="panel" id="panel-webhook">
        <div>
          <div class="section-title">Endpoint</div>
          <div class="info-row"><span class="info-key">URL</span><span class="info-value" id="wh-url">http://localhost:${PORT}/webhook</span></div>
          <div class="info-row"><span class="info-key">Method</span><span class="info-value">POST</span></div>
          <div class="info-row"><span class="info-key">Auth header</span><span class="info-value" id="wh-auth">None (set TAVERNA_WEBHOOK_KEY to enable)</span></div>
          <div class="info-row"><span class="info-key">Content-Type</span><span class="info-value">application/json</span></div>
        </div>
        <div>
          <div class="section-title">Payload</div>
          <div class="code-block">
{
  "to":      "session-name or *",
  "message": "your message here",
  "from":    "optional sender name (default: webhook)",
  "type":    "message | command | event | broadcast"
}
          </div>
        </div>
        <div>
          <div class="section-title">Example — curl</div>
          <div class="code-block" id="wh-curl">
            <button class="copy-btn" onclick="copyCurl()">copy</button>
            <span id="wh-curl-text">curl -X POST http://localhost:${PORT}/webhook \\<br>  -H "Content-Type: application/json" \\<br>  -d '{"to":"jarvis-main","message":"build failed","type":"event"}'</span>
          </div>
        </div>
        <div>
          <div class="section-title">Example — Home Assistant</div>
          <div class="code-block">
rest_command:
  notify_jarvis:
    url: http://&lt;your-mac&gt;:${PORT}/webhook
    method: POST
    headers:
      Content-Type: application/json
    payload: '{"to":"jarvis-main","message":"{{ message }}","type":"event"}'
          </div>
        </div>
      </div>

      <!-- HEALTH -->
      <div class="panel" id="panel-health">
        <div class="health-grid" id="health-grid">
          <div class="health-card"><div class="hc-label">Status</div><div class="hc-value ok" id="h-status">—</div></div>
          <div class="health-card"><div class="hc-label">Uptime</div><div class="hc-value small" id="h-uptime">—</div></div>
          <div class="health-card"><div class="hc-label">Sessions online</div><div class="hc-value" id="h-online">—</div><div class="hc-sub" id="h-total">of — registered</div></div>
          <div class="health-card"><div class="hc-label">Messages total</div><div class="hc-value" id="h-msgs">—</div><div class="hc-sub" id="h-mem">— in memory</div></div>
          <div class="health-card"><div class="hc-label">Queue depth</div><div class="hc-value" id="h-queue">—</div><div class="hc-sub">pending delivery</div></div>
          <div class="health-card"><div class="hc-label">ACL rules</div><div class="hc-value" id="h-acl">—</div></div>
          <div class="health-card"><div class="hc-label">API auth</div><div class="hc-value small" id="h-auth">—</div></div>
          <div class="health-card"><div class="hc-label">Webhook auth</div><div class="hc-value small" id="h-wh-auth">—</div></div>
        </div>
        <button class="btn btn-muted" style="align-self:flex-start" onclick="refreshHealth()">Refresh</button>
      </div>

    </div>
  </div>
</div>

<script>
// ── State ─────────────────────────────────────────────────────────────────────
let allMessages = []
let allSessions = []
let allAcl = []
let logFilter = { session: '', type: '', search: '' }
let activeFilterSession = null

// ── SSE ───────────────────────────────────────────────────────────────────────
let _es = null
function connect() {
  if (_es) { _es.close(); _es = null }
  const es = _es = new EventSource('/events')
  const dot = document.getElementById('conn-dot')

  es.onopen = () => dot.className = 'conn-dot'
  es.onerror = () => {
    dot.className = 'conn-dot disconnected'
    es.close()
    if (_es === es) { _es = null; setTimeout(connect, 3000) }
  }

  es.onmessage = e => {
    try {
      const { event, payload } = JSON.parse(e.data)
      if (event === 'init') {
        allMessages = payload.log || []
        allSessions = payload.sessions || []
        allAcl = payload.acl || []
        renderSessions(); renderLog(); renderAcl()
        updateStats()
      } else if (event === 'message') {
        allMessages.unshift(payload)
        if (allMessages.length > 1000) allMessages.pop()
        renderLog()
        updateStats()
      } else if (event === 'sessions') {
        allSessions = payload
        renderSessions(); updateStats()
      } else if (event === 'acl') {
        allAcl = payload
        renderAcl()
      }
    } catch {}
  }
}
connect()

// ── Stats ─────────────────────────────────────────────────────────────────────
function updateStats() {
  const online = allSessions.filter(s => s.status === 'online').length
  document.getElementById('stat-online').textContent = online
  document.getElementById('stat-msgs').textContent = allMessages.length
}

setInterval(async () => {
  try {
    const h = await fetch('/health').then(r => r.json())
    document.getElementById('stat-uptime').textContent = fmtUptime(h.uptime)
  } catch {}
}, 5000)

// ── Sessions sidebar ──────────────────────────────────────────────────────────
function renderSessions() {
  const el = document.getElementById('session-list')
  const sel = document.getElementById('log-session')

  // update filter dropdown
  const cur = sel.value
  sel.innerHTML = '<option value="">All sessions</option>' +
    allSessions.map(s => \`<option value="\${s.name}">\${s.name}</option>\`).join('')
  sel.value = cur

  // update datalist for send form
  const dl = document.getElementById('session-names')
  if (dl) dl.innerHTML = allSessions.filter(s => s.status === 'online').map(s => \`<option value="\${s.name}">\`).join('')

  if (!allSessions.length) {
    el.innerHTML = '<div style="color:var(--muted);font-size:0.8rem;padding:8px 4px">No sessions</div>'
    return
  }
  el.innerHTML = allSessions.map(s => \`
    <div class="session-card \${activeFilterSession === s.name ? 'filter-active' : ''}"
         onclick="filterBySession('\${s.name}')">
      <div class="sc-name">
        <div class="dot \${s.status}"></div>
        <span class="sc-label">\${s.name}</span>
        \${s.status === 'online' ? \`<button class="sc-send-btn" onclick="event.stopPropagation();quickSend('\${s.name}')" title="Send message">📤</button>\` : ''}
      </div>
      <div class="sc-meta">
        <span>↑\${s.msg_sent} ↓\${s.msg_received}</span>
        <span>\${s.status === 'online' ? '<span style=\\"color:var(--green)\\">online</span>' : timeAgo(s.last_seen) + ' ago'}</span>
      </div>
    </div>\`).join('')
}

async function clearOffline() {
  await fetch('/sessions/cleanup', { method: 'POST' })
}

function replyTo(msgId, fromSession) {
  document.getElementById('s-to').value = fromSession
  document.getElementById('s-reply-to').value = msgId
  document.getElementById('s-type').value = 'reply'
  document.querySelector('details').open = true
  showTab('send')
  setTimeout(() => document.getElementById('s-msg').focus(), 50)
}

function quickSend(name) {
  document.getElementById('s-to').value = name
  document.getElementById('s-from').value = document.getElementById('s-from').value || 'dashboard'
  document.getElementById('s-msg').value = ''
  showTab('send')
  setTimeout(() => document.getElementById('s-msg').focus(), 50)
}

function filterBySession(name) {
  if (activeFilterSession === name) {
    activeFilterSession = null
    logFilter.session = ''
  } else {
    activeFilterSession = name
    logFilter.session = name
    document.getElementById('log-session').value = name
    showTab('log')
  }
  renderSessions(); renderLog()
}

// ── Log ───────────────────────────────────────────────────────────────────────
function renderLog() {
  const { session, type, search } = logFilter
  let msgs = allMessages
  if (session) msgs = msgs.filter(m => m.from === session || m.to === session)
  if (type)    msgs = msgs.filter(m => m.type === type)
  if (search)  msgs = msgs.filter(m => m.message.toLowerCase().includes(search.toLowerCase()) || m.from.includes(search) || m.to.includes(search))

  const el = document.getElementById('log-entries')
  if (!msgs.length) {
    el.innerHTML = '<div class="log-empty">No messages match the current filter</div>'
    return
  }
  el.innerHTML = msgs.slice(0, 200).map(m => \`
    <div class="log-entry">
      <div class="le-header">
        <span class="le-from">\${m.from}</span>
        <span style="color:var(--muted)">→</span>
        <span class="le-to">\${m.to}</span>
        <span class="le-type \${m.type || 'message'}">\${m.type || 'message'}</span>
        <span class="le-status \${m.status}">\${m.status}</span>
        <button class="le-reply-btn" onclick="replyTo('\${m.msg_id}','\${m.from}')" title="Reply">↩ Reply</button>
      </div>
      \${m.reply_to ? \`<div class="le-reply">↩ reply to \${m.reply_to}</div>\` : ''}
      <div class="le-body">\${escHtml(m.message)}</div>
      <div class="le-ts">\${new Date(m.ts).toLocaleString()}</div>
    </div>\`).join('')
}

function filterLog() {
  logFilter.session = document.getElementById('log-session').value
  logFilter.type    = document.getElementById('log-type').value
  logFilter.search  = document.getElementById('log-search').value
  if (logFilter.session !== activeFilterSession) activeFilterSession = logFilter.session || null
  renderSessions(); renderLog()
}

function clearLogFilter() {
  logFilter = { session: '', type: '', search: '' }
  activeFilterSession = null
  document.getElementById('log-session').value = ''
  document.getElementById('log-type').value = ''
  document.getElementById('log-search').value = ''
  renderSessions(); renderLog()
}

// ── Send ──────────────────────────────────────────────────────────────────────
async function sendMessage() {
  const from     = document.getElementById('s-from').value.trim() || 'dashboard'
  const to       = document.getElementById('s-to').value.trim()
  const message  = document.getElementById('s-msg').value.trim()
  const type     = document.getElementById('s-type').value
  const reply_to = document.getElementById('s-reply-to').value.trim() || undefined
  const box      = document.getElementById('send-result')

  if (!to || !message) { showResult(box, 'To and message are required', false); return }

  try {
    const r = await fetch('/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ from, to, message, type, reply_to })
    }).then(r => r.json())
    if (!r.error) {
      document.getElementById('s-msg').value = ''
      document.getElementById('s-reply-to').value = ''
    }
    showResult(box, JSON.stringify(r, null, 2), !r.error)
  } catch (e) { showResult(box, String(e), false) }
}

function showResult(el, text, ok) {
  el.style.display = 'block'
  el.textContent = text
  el.className = 'result-box ' + (ok ? 'ok' : 'error')
}

// ── ACL ───────────────────────────────────────────────────────────────────────
function renderAcl() {
  const el = document.getElementById('acl-rules')
  document.getElementById('acl-default').textContent =
    allAcl.length ? 'Rules are evaluated top-to-bottom. First match wins. Default: allow.' : 'Default: allow all — no rules defined.'
  if (!allAcl.length) { el.innerHTML = ''; return }
  el.innerHTML = allAcl.map((r, i) => \`
    <div class="acl-rule">
      <span class="from">\${r.from}</span>
      <span class="arrow">→</span>
      <span class="to">\${r.to}</span>
      <span class="verdict \${r.allow ? 'allow' : 'deny'}">\${r.allow ? 'ALLOW' : 'DENY'}</span>
      <button class="btn btn-danger" onclick="deleteRule(\${i})" style="padding:3px 8px;margin-left:8px">×</button>
    </div>\`).join('')
}

async function addRule() {
  const from  = document.getElementById('a-from').value.trim()
  const to    = document.getElementById('a-to').value.trim()
  const allow = document.getElementById('a-allow').value === 'true'
  if (!from || !to) return
  await fetch('/acl/rule', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({from,to,allow}) })
  document.getElementById('a-from').value = ''
  document.getElementById('a-to').value = ''
}

window.deleteRule = async i => {
  await fetch('/acl/rule/' + i, { method: 'DELETE' })
}

// ── Health ────────────────────────────────────────────────────────────────────
async function refreshHealth() {
  try {
    const h = await fetch('/health').then(r => r.json())
    document.getElementById('h-status').textContent  = h.status.toUpperCase()
    document.getElementById('h-uptime').textContent  = fmtUptime(h.uptime)
    document.getElementById('h-online').textContent  = h.sessions.online
    document.getElementById('h-total').textContent   = 'of ' + h.sessions.total + ' registered'
    document.getElementById('h-msgs').textContent    = h.messages.total
    document.getElementById('h-mem').textContent     = h.messages.in_memory + ' in memory'
    document.getElementById('h-queue').textContent   = h.queue_depth
    document.getElementById('h-acl').textContent     = h.acl_rules
    document.getElementById('h-auth').textContent    = h.api_auth ? '🔒 Enabled' : '🔓 Disabled'
    document.getElementById('h-wh-auth').textContent = h.webhook_auth ? '🔒 Enabled' : '🔓 Disabled'
    document.getElementById('stat-uptime').textContent = fmtUptime(h.uptime)
  } catch {}
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function showTab(name) {
  document.querySelectorAll('.tab').forEach((t, i) => {
    const names = ['log','send','acl','webhook','health']
    t.classList.toggle('active', names[i] === name)
  })
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'))
  document.getElementById('panel-' + name).classList.add('active')
  if (name === 'health') refreshHealth()
}

// ── Webhook ───────────────────────────────────────────────────────────────────
function copyCurl() {
  const text = document.getElementById('wh-curl-text').innerText
  navigator.clipboard.writeText(text.replace(/\\\\/g, '').replace(/\\n  /g, ' '))
  const btn = document.querySelector('.copy-btn')
  btn.textContent = 'copied!'; setTimeout(() => btn.textContent = 'copy', 1500)
}

// ── Utils ─────────────────────────────────────────────────────────────────────
function timeAgo(iso) {
  const s = Math.floor((Date.now() - new Date(iso)) / 1000)
  if (s < 60) return s + 's'
  if (s < 3600) return Math.floor(s/60) + 'm'
  return Math.floor(s/3600) + 'h'
}

function fmtUptime(s) {
  if (s < 60) return s + 's'
  if (s < 3600) return Math.floor(s/60) + 'm ' + (s%60) + 's'
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60)
  return h + 'h ' + m + 'm'
}

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
}

// init
refreshHealth()
</script>
</body>
</html>`
