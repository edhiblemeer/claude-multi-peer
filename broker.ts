#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { homedir } from "node:os";
import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
  RegisterVirtualPeerRequest,
  RegisterVirtualPeerResponse,
  UnregisterVirtualPeerRequest,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${homedir()}/.claude-multi-peer.db`;

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

// B1: virtual peer columns (additive migration — safe on existing DBs)
function ensureColumn(table: string, column: string, decl: string) {
  const cols = db.query(`PRAGMA table_info(${table})`).all() as { name: string }[];
  if (!cols.some((c) => c.name === column)) {
    db.run(`ALTER TABLE ${table} ADD COLUMN ${column} ${decl}`);
  }
}
ensureColumn("peers", "virtual_peer", "INTEGER NOT NULL DEFAULT 0");
ensureColumn("peers", "parent_id", "TEXT");
ensureColumn("peers", "role", "TEXT");

// Index for fast lookup of virtual peers by (parent_id, role)
db.run(
  `CREATE INDEX IF NOT EXISTS idx_peers_parent_role ON peers (parent_id, role)`
);

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Issue #1: broker-side push tracking to prevent delivery amplification.
// The v2 poll returns undelivered messages WITHOUT marking them delivered
// (ack is required). Server-side, an in-memory pushedMessageIds set kept the
// "already-pushed but not yet acked" state so a subsequent poll on the same
// session would just retry the ack, not re-push. That set is per-process and
// dies with the session: after a spurious respawn (or a duplicate server
// process for the same peer) the new process re-pushes the same messages
// because it has no memory of the prior push. Persisting that memory on the
// broker fixes the amplification for both "one peer respawning" and "two
// concurrent server processes for the same peer" cases.
//
//   pushed_at    — set to now each time /poll-messages-v2 hands out a row
//   push_count   — incremented each time; capped by MAX_PUSH_ATTEMPTS
//
// A row is only handed out when either pushed_at is NULL (first send) or the
// last push is older than PUSH_GRACE_MS (previous push likely lost — retry).
ensureColumn("messages", "pushed_at", "TEXT");
ensureColumn("messages", "push_count", "INTEGER NOT NULL DEFAULT 0");

// Day56 stale-cleanup grace-ification (ack-based delivery safety).
// With ack-based delivery, undelivered messages are NORMAL (a peer that is busy/mid-turn
// legitimately has queued, un-acked messages). The old immediate delete-on-first-PID-failure
// could wipe a LIVE session's inbox on a transient PID mis-read (EPERM etc.). So:
//   (a) A peer is only removed once we are confident it is gone: PID check must fail
//       MAX_PID_FAILURES times consecutively AND its last_seen must be older than the
//       grace window (a live session heartbeats every 15s, so last_seen stays fresh and
//       it is never removed no matter how the PID check flakes).
//   (b) Undelivered messages are NOT deleted together with the peer. Instead a separate
//       grace sweep removes only undelivered messages that are BOTH older than
//       MESSAGE_GRACE_MS AND addressed to a peer that no longer exists — so a live peer's
//       queued messages are never touched.
const PID_GRACE_MS = 90_000; // last_seen must be older than this before a PID-failed peer is removed
const MAX_PID_FAILURES = 3; // consecutive PID-check failures required before removal
const MESSAGE_GRACE_MS = 5 * 60_000; // undelivered messages younger than this are kept even if recipient is gone

// claude-multi-peer additions:
// (1) DELIVERED_MESSAGE_TTL_MS — delivered messages older than this are pruned by the rotation
//     sweep, preventing unbounded growth of ~/.claude-multi-peer.db and slow prepared-statement
//     scans that develop over months of use.
// (2) MESSAGE_ROTATION_INTERVAL_MS — how often the rotation sweep runs.
// Override the TTL via env var CLAUDE_PEERS_MESSAGE_TTL_HOURS (float, default 168 = 7 days).
const DELIVERED_MESSAGE_TTL_MS =
  parseFloat(process.env.CLAUDE_PEERS_MESSAGE_TTL_HOURS ?? "168") * 3600_000;
const MESSAGE_ROTATION_INTERVAL_MS = 60 * 60_000; // every hour

// Issue #1: push-tracking constants (see the pushed_at / push_count column comment above).
// PUSH_GRACE_MS is how long the broker suppresses a re-hand-out of a message that was
// already returned once but not yet acked. Long enough to cover a spuriously-respawning
// server (~20s cycle reported in the wild) so the new spawn does not re-push the same
// message the previous spawn already pushed. Override via CLAUDE_PEERS_PUSH_GRACE_SEC.
// MAX_PUSH_ATTEMPTS is the defensive cap on re-hand-outs of the same message. On the
// (attempts+1)th failed ack, the message is force-marked delivered with a loud log so
// it can never amplify past that cap regardless of client behavior. Override via
// CLAUDE_PEERS_MAX_PUSH_ATTEMPTS.
const PUSH_GRACE_MS =
  parseFloat(process.env.CLAUDE_PEERS_PUSH_GRACE_SEC ?? "60") * 1000;
const MAX_PUSH_ATTEMPTS = parseInt(
  process.env.CLAUDE_PEERS_MAX_PUSH_ATTEMPTS ?? "5",
  10
);

// Consecutive PID-check failure counter per peer id (reset to 0 on any successful check).
const pidFailureCounts = new Map<string, number>();

function cleanStalePeers() {
  const now = Date.now();
  const peers = db
    .query("SELECT id, pid, virtual_peer, parent_id, last_seen FROM peers")
    .all() as {
    id: string;
    pid: number;
    virtual_peer: number;
    parent_id: string | null;
    last_seen: string;
  }[];
  const alive = new Set<string>();
  const removed = new Set<string>();

  // Decide removal per peer: PID must be confirmed dead (N consecutive failures) AND stale.
  function removalConfirmed(peer: { id: string; last_seen: string }): boolean {
    const failures = (pidFailureCounts.get(peer.id) ?? 0) + 1;
    pidFailureCounts.set(peer.id, failures);
    const lastSeenMs = Date.parse(peer.last_seen);
    const staleForMs = Number.isNaN(lastSeenMs) ? Infinity : now - lastSeenMs;
    return failures >= MAX_PID_FAILURES && staleForMs > PID_GRACE_MS;
  }

  for (const peer of peers) {
    try {
      // Check if process is still alive (signal 0 doesn't kill, just checks)
      process.kill(peer.pid, 0);
      alive.add(peer.id);
      pidFailureCounts.set(peer.id, 0); // reset on success
    } catch {
      // Process appears dead — but only remove after grace (N failures + last_seen stale)
      if (removalConfirmed(peer)) {
        db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
        pidFailureCounts.delete(peer.id);
        removed.add(peer.id);
      }
    }
  }

  // Orphan-virtual-peer sweep: any virtual peer whose parent was actually removed
  // (parent gone AND not merely mid-grace). Parents still within grace keep their children.
  for (const peer of peers) {
    if (
      peer.virtual_peer === 1 &&
      peer.parent_id &&
      !alive.has(peer.parent_id) &&
      removed.has(peer.parent_id)
    ) {
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      pidFailureCounts.delete(peer.id);
      removed.add(peer.id);
    }
  }

  // (b) Grace sweep for undelivered messages: only drop ones that are old AND orphaned
  // (recipient peer no longer exists). Never touches messages for a still-registered peer.
  const messageCutoff = new Date(now - MESSAGE_GRACE_MS).toISOString();
  db.run(
    `DELETE FROM messages
     WHERE delivered = 0
       AND sent_at < ?
       AND to_id NOT IN (SELECT id FROM peers)`,
    [messageCutoff]
  );
}

cleanStalePeers();

// Periodically clean stale peers (every 30s)
setInterval(cleanStalePeers, 30_000);

// claude-multi-peer: delivered message rotation sweep.
// Prunes messages that have been delivered AND are older than DELIVERED_MESSAGE_TTL_MS.
// Undelivered messages are handled separately in cleanStalePeers() (never touched
// while their recipient is still registered).
function rotateDeliveredMessages() {
  try {
    const cutoff = new Date(Date.now() - DELIVERED_MESSAGE_TTL_MS).toISOString();
    const result = db.run(
      `DELETE FROM messages WHERE delivered = 1 AND sent_at < ?`,
      [cutoff]
    );
    if (result.changes > 0) {
      console.error(
        `[broker] message rotation: pruned ${result.changes} delivered messages older than ${cutoff}`
      );
    }
  } catch (e) {
    console.error(`[broker] message rotation failed: ${e}`);
  }
}
rotateDeliveredMessages();
setInterval(rotateDeliveredMessages, MESSAGE_ROTATION_INTERVAL_MS);

// P2: Self-watchdog — exit if no endpoint hit refreshes lastHealthOk within window (Day52 hang prevention)
// refresh trigger: /health endpoint OR /heartbeat OR /poll-messages (natural request flow)
// threshold 5min (60s was too aggressive: false self-exit when /health had no natural caller)
let lastHealthOk = Date.now();
setInterval(() => {
  const stale = Date.now() - lastHealthOk;
  if (stale > 300_000) {
    console.error(`[broker] self-watchdog detected stale ${stale}ms since last endpoint hit (5min threshold exceeded), exiting`);
    process.exit(1);
  }
}, 10_000);

// P3: Force SQLite WAL checkpoint every hour (Day52 WAL file 肥大化 prevention)
setInterval(() => {
  try {
    db.run("PRAGMA wal_checkpoint(TRUNCATE)");
    console.error(`[broker] WAL checkpoint executed`);
  } catch (e) {
    console.error(`[broker] WAL checkpoint failed: ${e}`);
  }
}, 60 * 60 * 1000);

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, virtual_peer, parent_id, role)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL)
`);

const insertVirtualPeer = db.prepare(`
  INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, virtual_peer, parent_id, role)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

// Heartbeat for parent also bumps its virtual children (so they stay "fresh").
const updateChildrenLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE parent_id = ? AND virtual_peer = 1
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const deleteVirtualChildren = db.prepare(`
  DELETE FROM peers WHERE parent_id = ? AND virtual_peer = 1
`);

const selectVirtualByParentRole = db.prepare(`
  SELECT * FROM peers WHERE parent_id = ? AND role = ? AND virtual_peer = 1
`);

const selectParentPeer = db.prepare(`
  SELECT * FROM peers WHERE id = ? AND virtual_peer = 0
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

// Issue #1: push-tracked variant used by /poll-messages-v2. Returns messages
// that are (a) undelivered, (b) either never pushed OR whose last push was
// before the grace cutoff, and (c) still under the retry cap. Ordered oldest
// first so retries fall in the same order as originals.
const selectPushableV2 = db.prepare(`
  SELECT * FROM messages
  WHERE to_id = ?
    AND delivered = 0
    AND push_count < ?
    AND (pushed_at IS NULL OR pushed_at < ?)
  ORDER BY sent_at ASC
`);

// Issue #1: bump push tracking on a v2 hand-out. Broker sets pushed_at = now
// and increments push_count. The message stays undelivered until /ack-message
// (or until it exceeds MAX_PUSH_ATTEMPTS and is force-marked below).
const bumpPushed = db.prepare(`
  UPDATE messages SET pushed_at = ?, push_count = push_count + 1 WHERE id = ?
`);

// Issue #1: any undelivered message whose push_count has hit the cap AND is
// past the grace cutoff is treated as poisoned — force-marked delivered with
// a loud log. This is the defensive backstop that guarantees amplification
// cannot exceed MAX_PUSH_ATTEMPTS copies no matter how many times a client
// respawns. Prefer this over silent unbounded retry.
const selectPoisonedForRecipient = db.prepare(`
  SELECT id, from_id, push_count FROM messages
  WHERE to_id = ?
    AND delivered = 0
    AND push_count >= ?
    AND (pushed_at IS NULL OR pushed_at < ?)
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

// claude-multi-peer: persistent ID recovery on re-registration.
//
// When a Claude Code session restarts (Ctrl-C, /clear, host reboot), the original
// upstream behavior mints a brand-new random peer ID every time, breaking every
// SERVER_MAP entry, Discord bridge routing, and outside-of-session workflow that
// remembered the old ID. Downstream tooling (bridges, dashboards, subagents) then
// has to be manually re-mapped every restart.
//
// Recovery rule: on re-registration, if a previous DEAD real peer exists whose
// (cwd, git_root) fingerprint matches this one, and it died within the recent
// grace window, its ID is reused for the new session. This keeps SERVER_MAP
// entries stable across restarts without any user action.
//
// The reuse window is bounded so that long-abandoned peers with the same cwd
// (e.g. after `git rm`, disk reformat, or repo rename) do not silently steal
// a stale identity. Override via CLAUDE_PEERS_ID_REUSE_WINDOW_HOURS (default 24h).
const ID_REUSE_WINDOW_MS =
  parseFloat(process.env.CLAUDE_PEERS_ID_REUSE_WINDOW_HOURS ?? "24") * 3600_000;

function findReusableId(cwd: string, gitRoot: string | null): string | null {
  const cutoff = new Date(Date.now() - ID_REUSE_WINDOW_MS).toISOString();
  // Prefer matching on git_root when present (worktree-safe), else on cwd.
  const row = db
    .query(
      `SELECT id, pid FROM peers
       WHERE virtual_peer = 0
         AND cwd = ?
         AND (git_root IS ? OR (git_root IS NOT NULL AND ? IS NOT NULL AND git_root = ?))
         AND last_seen >= ?
       ORDER BY last_seen DESC
       LIMIT 1`
    )
    .get(cwd, gitRoot, gitRoot, gitRoot, cutoff) as { id: string; pid: number } | null;
  if (!row) return null;
  // Only reuse if the previous PID is actually dead (do not steal a live one).
  try {
    process.kill(row.pid, 0);
    return null; // previous peer is still alive under a different PID collision path
  } catch {
    return row.id;
  }
}

function handleRegister(body: RegisterRequest): RegisterResponse {
  const now = new Date().toISOString();

  // Remove any existing real (non-virtual) registration for this PID (re-registration).
  // Virtual peers under that old parent are cascade-removed by parent_id.
  const existing = db
    .query("SELECT id FROM peers WHERE pid = ? AND virtual_peer = 0")
    .get(body.pid) as { id: string } | null;
  if (existing) {
    deleteVirtualChildren.run(existing.id);
    deletePeer.run(existing.id);
  }

  // Try to inherit the peer ID from a recent dead session at the same workspace.
  // Falls back to a fresh random ID if no match is found within the reuse window.
  const reusedId = findReusableId(body.cwd, body.git_root);
  const id = reusedId ?? generateId();
  if (reusedId) {
    // Overwrite the stale row rather than leave a dupe when we reuse an ID.
    deleteVirtualChildren.run(reusedId);
    deletePeer.run(reusedId);
    console.error(
      `[broker] persistent ID: reused ${reusedId} for cwd=${body.cwd} (previous session ended within ${ID_REUSE_WINDOW_MS / 3600_000}h)`
    );
  }

  insertPeer.run(id, body.pid, body.cwd, body.git_root, body.tty, body.summary, now, now);
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  const now = new Date().toISOString();
  updateLastSeen.run(now, body.id);
  // Cascade heartbeat to virtual children so they don't appear stale.
  updateChildrenLastSeen.run(now, body.id);
  // P2: refresh self-watchdog (natural request flow indicates event loop is live)
  lastHealthOk = Date.now();
}

function handleRegisterVirtualPeer(
  body: RegisterVirtualPeerRequest
): RegisterVirtualPeerResponse {
  if (!body.parent_id || !body.role) {
    throw new Error("parent_id and role are required");
  }
  // Idempotent: if a virtual peer with this (parent_id, role) already exists, return it.
  const existing = selectVirtualByParentRole.get(body.parent_id, body.role) as
    | Peer
    | null;
  if (existing) {
    if (body.summary && body.summary !== existing.summary) {
      updateSummary.run(body.summary, existing.id);
    }
    return { id: existing.id, created: false };
  }

  // Look up parent to inherit pid/cwd/git_root/tty for liveness + filtering.
  const parent = selectParentPeer.get(body.parent_id) as Peer | null;
  if (!parent) {
    throw new Error(`Parent peer ${body.parent_id} not found (or is itself virtual)`);
  }

  const id = generateId();
  const now = new Date().toISOString();
  const summary = body.summary ?? `[subagent ${body.role}]`;
  insertVirtualPeer.run(
    id,
    parent.pid,
    parent.cwd,
    parent.git_root,
    parent.tty,
    summary,
    now,
    now,
    body.parent_id,
    body.role
  );
  return { id, created: true };
}

function handleUnregisterVirtualPeer(body: UnregisterVirtualPeerRequest): void {
  if (!body.parent_id) {
    throw new Error("parent_id is required");
  }
  if (body.role) {
    const existing = selectVirtualByParentRole.get(body.parent_id, body.role) as
      | Peer
      | null;
    if (existing) {
      deletePeer.run(existing.id);
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [existing.id]);
    }
  } else {
    // Unregister all virtual children
    const children = db
      .query("SELECT id FROM peers WHERE parent_id = ? AND virtual_peer = 1")
      .all(body.parent_id) as { id: string }[];
    for (const c of children) {
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [c.id]);
    }
    deleteVirtualChildren.run(body.parent_id);
  }
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function normalizePeerRow(row: Record<string, unknown>): Peer {
  return {
    id: row.id as string,
    pid: row.pid as number,
    cwd: row.cwd as string,
    git_root: (row.git_root ?? null) as string | null,
    tty: (row.tty ?? null) as string | null,
    summary: (row.summary ?? "") as string,
    registered_at: row.registered_at as string,
    last_seen: row.last_seen as string,
    virtual_peer: ((row.virtual_peer ?? 0) as number) === 1,
    parent_id: (row.parent_id ?? null) as string | null,
    role: (row.role ?? null) as string | null,
  };
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let rawPeers: Record<string, unknown>[];

  switch (body.scope) {
    case "machine":
      rawPeers = selectAllPeers.all() as Record<string, unknown>[];
      break;
    case "directory":
      rawPeers = selectPeersByDirectory.all(body.cwd) as Record<string, unknown>[];
      break;
    case "repo":
      if (body.git_root) {
        rawPeers = selectPeersByGitRoot.all(body.git_root) as Record<string, unknown>[];
      } else {
        // No git root, fall back to directory
        rawPeers = selectPeersByDirectory.all(body.cwd) as Record<string, unknown>[];
      }
      break;
    default:
      rawPeers = selectAllPeers.all() as Record<string, unknown>[];
  }

  let peers = rawPeers.map(normalizePeerRow);

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Issue #2: annotate the caller's own entry with is_self=true so the caller
  // can identify itself without a separate whoami round-trip. Kept as a flag
  // rather than a filter so the caller can still see (and reason about) its
  // own registration in the list.
  if (body.caller_id) {
    for (const p of peers) {
      if (p.id === body.caller_id) {
        p.is_self = true;
      }
    }
  }

  // Verify each peer's process is still alive
  return peers.filter((p) => {
    try {
      process.kill(p.pid, 0);
      return true;
    } catch {
      // Clean up dead peer (and its virtual children if it's a parent).
      // Never delete the caller's own row on a transient PID check miss —
      // if the caller is asking, it is by definition alive; deleting its
      // row here would wipe its inbox on the next stale-sweep race.
      if (body.caller_id && p.id === body.caller_id) {
        return true;
      }
      if (!p.virtual_peer) {
        deleteVirtualChildren.run(p.id);
      }
      deletePeer.run(p.id);
      return false;
    }
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // claude-multi-peer: adapter routing.
  // If to_id is prefixed "adapter:<type>:<external_id>", the message is destined for an
  // external platform (e.g. Discord channel) served by a running adapter process. The
  // adapter drains these via /adapter-poll. We insert directly without a peers-table check.
  if (body.to_id.startsWith("adapter:")) {
    insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
    return { ok: true };
  }

  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

// claude-multi-peer: adapter → peer (inbound) delivery.
// Called by a running adapter (e.g. Discord bot) when it receives a message from its
// platform. from_id is a synthetic adapter identity like "adapter:discord:1516364092447916072".
// to_id must be a real peer ID resolved by the adapter from its channel-to-peer map.
function handleAdapterDeliver(body: {
  from_id: string;
  to_id: string;
  text: string;
}): { ok: boolean; error?: string } {
  if (!body.from_id.startsWith("adapter:")) {
    return { ok: false, error: `Adapter deliver requires from_id starting with "adapter:"` };
  }
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }
  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

// claude-multi-peer: adapter drain queue (outbound).
// Adapter polls this endpoint to fetch messages any peer sent to "adapter:<type>:..." and
// marks them delivered on read. Matches the ack-based path used for peers: adapter should
// only mark delivered on the broker side once it has actually posted to the platform.
const selectAdapterOutbound = db.prepare(`
  SELECT * FROM messages
  WHERE to_id LIKE ? AND delivered = 0
  ORDER BY sent_at ASC
`);
function handleAdapterPoll(body: { adapter: string }): { messages: Message[] } {
  if (!body.adapter) return { messages: [] };
  const prefix = `adapter:${body.adapter}:%`;
  const messages = selectAdapterOutbound.all(prefix) as Message[];
  // Mark them delivered — adapter is expected to attempt post; failed posts will
  // stay lost. This matches how peers currently poll (fire-and-forget). A future
  // extension could use ack semantics matching /poll-messages-v2 + /ack-message.
  for (const msg of messages) markDelivered.run(msg.id);
  return { messages };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];

  // Mark them as delivered
  for (const msg of messages) {
    markDelivered.run(msg.id);
  }

  // P2: refresh self-watchdog (poll-messages is the highest-frequency endpoint = best liveness signal)
  lastHealthOk = Date.now();

  return { messages };
}

// Day56 ack-based delivery (Option 1', backward-compatible).
// /poll-messages-v2: returns undelivered messages but does NOT mark them delivered.
// The caller must call /ack-message {id} after each message is successfully pushed
// to the model. If a push fails, the message is left undelivered and re-returned on
// the next poll (redelivery). Legacy /poll-messages (mark-on-poll) stays untouched
// so old server.ts sessions keep working until they restart onto v2.
//
// Issue #1: push tracking layer. The v2 poll now filters by pushed_at / push_count
// so the same message is not handed out more often than once per PUSH_GRACE_MS and
// never more than MAX_PUSH_ATTEMPTS times. This survives across process restarts
// (state is on the broker, not in the calling server's memory) so a session that
// respawns mid-cycle cannot re-push messages the previous spawn already delivered.
function handlePollMessagesV2(body: PollMessagesRequest): PollMessagesResponse {
  const nowIso = new Date().toISOString();
  const graceCutoffIso = new Date(Date.now() - PUSH_GRACE_MS).toISOString();

  // Poison the messages that have exceeded MAX_PUSH_ATTEMPTS. This runs before
  // the select so poisoned rows aren't returned again in this same call.
  const poisoned = selectPoisonedForRecipient.all(
    body.id,
    MAX_PUSH_ATTEMPTS,
    graceCutoffIso
  ) as { id: number; from_id: string; push_count: number }[];
  for (const p of poisoned) {
    markDelivered.run(p.id);
    console.error(
      `[broker] amplification cap: force-delivered msg ${p.id} to ${body.id} from ${p.from_id} after ${p.push_count} push attempts without ack (see CLAUDE_PEERS_MAX_PUSH_ATTEMPTS)`
    );
  }

  const messages = selectPushableV2.all(
    body.id,
    MAX_PUSH_ATTEMPTS,
    graceCutoffIso
  ) as Message[];

  // Record the push attempt. Broker will not hand these out again until either
  // the caller acks or PUSH_GRACE_MS elapses (whichever comes first).
  for (const msg of messages) {
    bumpPushed.run(nowIso, msg.id);
  }

  // P2: refresh self-watchdog (v2 poll is now the highest-frequency endpoint on new sessions)
  lastHealthOk = Date.now();

  return { messages };
}

// /ack-message: mark a single message delivered after the caller confirmed a successful push.
function handleAckMessage(body: { id: number }): { ok: boolean } {
  markDelivered.run(body.id);

  // P2: refresh self-watchdog (natural request flow indicates event loop is live)
  lastHealthOk = Date.now();

  return { ok: true };
}

function handleUnregister(body: { id: string }): void {
  // Cascade-remove any virtual children before deleting the parent
  deleteVirtualChildren.run(body.id);
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        lastHealthOk = Date.now();
        return Response.json({ status: "ok" });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/poll-messages-v2":
          return Response.json(handlePollMessagesV2(body as PollMessagesRequest));
        case "/ack-message":
          return Response.json(handleAckMessage(body as { id: number }));
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        case "/register-virtual-peer":
          return Response.json(
            handleRegisterVirtualPeer(body as RegisterVirtualPeerRequest)
          );
        case "/unregister-virtual-peer":
          handleUnregisterVirtualPeer(body as UnregisterVirtualPeerRequest);
          return Response.json({ ok: true });
        case "/adapter-deliver":
          return Response.json(
            handleAdapterDeliver(body as { from_id: string; to_id: string; text: string })
          );
        case "/adapter-poll":
          return Response.json(handleAdapterPoll(body as { adapter: string }));
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
