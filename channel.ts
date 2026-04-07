#!/usr/bin/env bun
/**
 * Taverna Channel — MCP server that connects a Claude Code session to the Taverna hub.
 *
 * Configuration (env vars):
 *   TAVERNA_HUB          Hub base URL (default: http://localhost:2489)
 *   TAVERNA_SESSION_NAME Session name shown in dashboard (default: hostname)
 *
 * Usage: add to .mcp.json, then start Claude with:
 *   claude --dangerously-load-development-channels server:taverna
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { hostname } from 'os'

const HUB = (Bun.env.TAVERNA_HUB ?? 'http://localhost:2489').replace(/\/$/, '')
const SESSION_NAME = Bun.env.TAVERNA_SESSION_NAME ?? `${hostname()}-${Math.random().toString(36).slice(2, 6)}`
const API_KEY = Bun.env.TAVERNA_API_KEY

let sessionId: string | null = null

// ─── MCP server ───────────────────────────────────────────────────────────────

const mcp = new Server(
  { name: 'taverna', version: '1.0.0' },
  {
    capabilities: {
      experimental: { 'claude/channel': {} },
      tools: {},
    },
    instructions:
      `You are connected to Taverna, a message broker for Claude sessions.\n` +
      `Incoming messages from other sessions arrive as <channel source="taverna" from="SESSION_NAME" msg_id="...">.\n` +
      `Read them and respond using the taverna_send tool.\n` +
      `Use taverna_sessions to see who is online. Use taverna_log to review recent messages.`,
  },
)

// ─── Tools ────────────────────────────────────────────────────────────────────

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'taverna_send',
      description: 'Send a message to another session (or broadcast to all with to="*")',
      inputSchema: {
        type: 'object',
        properties: {
          to: { type: 'string', description: 'Target session name, or "*" for broadcast' },
          message: { type: 'string', description: 'Message to send' },
          type: { type: 'string', enum: ['message', 'command', 'reply', 'event', 'broadcast'], description: 'Message type (default: message)' },
          reply_to: { type: 'string', description: 'msg_id this message is replying to (optional)' },
        },
        required: ['to', 'message'],
      },
    },
    {
      name: 'taverna_sessions',
      description: 'List all sessions registered with the Taverna hub',
      inputSchema: { type: 'object', properties: {} },
    },
    {
      name: 'taverna_log',
      description: 'Get recent messages from the Taverna hub log',
      inputSchema: {
        type: 'object',
        properties: {
          limit: { type: 'number', description: 'Number of messages to return (default 20)' },
        },
      },
    },
  ],
}))

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const name = req.params.name
  const args = req.params.arguments as Record<string, unknown>

  if (name === 'taverna_send') {
    if (!sessionId) await register()
    if (!sessionId) return text('Not connected to hub — hub may be offline')
    const res = await fetch(`${HUB}/send`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders() },
      body: JSON.stringify({ from: SESSION_NAME, to: args.to, message: args.message, type: args.type ?? 'message', reply_to: args.reply_to }),
    })
    const data = await res.json()
    return text(JSON.stringify(data))
  }

  if (name === 'taverna_sessions') {
    const data = await fetch(`${HUB}/sessions`, { headers: authHeaders() }).then(r => r.json())
    const lines = data.map((s: { name: string; status: string; last_seen: string }) =>
      `${s.status === 'online' ? '🟢' : '⚫'} ${s.name} (${timeAgo(s.last_seen)})`
    )
    return text(lines.join('\n') || 'No sessions registered')
  }

  if (name === 'taverna_log') {
    const limit = (args.limit as number) ?? 20
    const data = await fetch(`${HUB}/log?limit=${limit}`, { headers: authHeaders() }).then(r => r.json())
    const lines = data.map((m: { from: string; to: string; message: string; ts: string; status: string }) =>
      `[${new Date(m.ts).toLocaleTimeString()}] ${m.from} → ${m.to} [${m.status}]: ${m.message}`
    )
    return text(lines.join('\n') || 'No messages in log')
  }

  throw new Error(`Unknown tool: ${name}`)
})

function text(content: string) {
  return { content: [{ type: 'text' as const, text: content }] }
}

function authHeaders(): Record<string, string> {
  return API_KEY ? { 'Authorization': `Bearer ${API_KEY}` } : {}
}

function timeAgo(iso: string): string {
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  return `${Math.floor(s / 3600)}h ago`
}

// ─── Registration & subscription ─────────────────────────────────────────────

async function register() {
  try {
    const res = await fetch(`${HUB}/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders() },
      body: JSON.stringify({ name: SESSION_NAME }),
    })
    const data = await res.json()
    sessionId = data.session_id
  } catch {
    // hub not available — continue without registration
  }
}

async function subscribe() {
  // re-register if we lost our session (hub restart)
  if (!sessionId) await register()
  if (!sessionId) { setTimeout(subscribe, 5_000); return }

  try {
    const res = await fetch(`${HUB}/subscribe/${sessionId}`, { headers: authHeaders() })
    if (!res.body) { sessionId = null; setTimeout(subscribe, 5_000); return }
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue
        try {
          const msg = JSON.parse(line.slice(6))
          await mcp.notification({
            method: 'notifications/claude/channel',
            params: {
              content: msg.message,
              meta: { from: String(msg.from), msg_id: String(msg.msg_id) },
            },
          })
        } catch { /* ignore parse errors */ }
      }
    }
  } catch { /* ignore connection errors */ }

  // lost connection — clear session id and reconnect (will re-register)
  sessionId = null
  setTimeout(subscribe, 5_000)
}

async function unregister() {
  if (!sessionId) return
  try {
    await fetch(`${HUB}/register/${sessionId}`, { method: 'DELETE', headers: authHeaders() })
  } catch { /* best effort */ }
}

// ─── Startup ──────────────────────────────────────────────────────────────────

process.on('SIGTERM', async () => { await unregister(); process.exit(0) })
process.on('SIGINT', async () => { await unregister(); process.exit(0) })

await register()
subscribe()  // start background SSE listener (non-blocking)
await mcp.connect(new StdioServerTransport())
