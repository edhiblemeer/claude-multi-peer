#!/usr/bin/env bun
/**
 * claude-peers MCP server
 *
 * Spawned by Claude Code as a stdio MCP server (one per instance).
 * Connects to the shared broker daemon for peer discovery and messaging.
 * Declares claude/channel capability to push inbound messages immediately.
 *
 * Usage:
 *   claude --dangerously-load-development-channels server:claude-peers
 *
 * With .mcp.json:
 *   { "claude-peers": { "command": "bun", "args": ["./server.ts"] } }
 */

import { fileURLToPath } from "node:url";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import type {
  PeerId,
  Peer,
  RegisterResponse,
  PollMessagesResponse,
  RegisterVirtualPeerResponse,
} from "./shared/types.ts";
import {
  generateSummary,
  getGitBranch,
  getRecentFiles,
} from "./shared/summarize.ts";
import {
  formatClientCapabilities,
  supportsClaudeChannel,
} from "./shared/channel.ts";

// --- Configuration ---

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const BROKER_URL = `http://127.0.0.1:${BROKER_PORT}`;
const POLL_INTERVAL_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 15_000;
// P4: broker /health probe timeout — 2s strict → 10s default (env override・Day52 false-dead 防止)
const BROKER_HEALTH_TIMEOUT_MS = parseInt(process.env.BROKER_HEALTH_TIMEOUT_MS ?? "10000", 10);
const BROKER_SCRIPT = fileURLToPath(new URL("./broker.ts", import.meta.url));

// --- Broker communication ---

async function brokerFetch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Broker error (${path}): ${res.status} ${err}`);
  }
  return res.json() as Promise<T>;
}

async function isBrokerAlive(): Promise<boolean> {
  try {
    const res = await fetch(`${BROKER_URL}/health`, { signal: AbortSignal.timeout(BROKER_HEALTH_TIMEOUT_MS) });
    return res.ok;
  } catch {
    return false;
  }
}

async function ensureBroker(): Promise<void> {
  if (await isBrokerAlive()) {
    log("Broker already running");
    return;
  }

  log("Starting broker daemon...");
  const proc = Bun.spawn(["bun", BROKER_SCRIPT], {
    stdio: ["ignore", "ignore", "inherit"],
    // Detach so the broker survives if this MCP server exits
    // On macOS/Linux, the broker will keep running
  });

  // Unref so this process can exit without waiting for the broker
  proc.unref();

  // Wait for it to come up
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    if (await isBrokerAlive()) {
      log("Broker started");
      return;
    }
  }

  // P5: EADDRINUSE fallback — spawn 6s timeout 後 既存 broker への retry connect (Day52 EADDRINUSE crash 後 graceful recovery)
  log("Broker spawn timeout, retrying connect to existing broker (EADDRINUSE fallback)...");
  for (let i = 0; i < 10; i++) {
    await new Promise((r) => setTimeout(r, 1000));
    if (await isBrokerAlive()) {
      log("Existing broker now responsive after fallback wait");
      return;
    }
  }

  throw new Error("Failed to start broker daemon after 16 seconds (spawn + fallback)");
}

// --- Utility ---

function log(msg: string) {
  // MCP stdio servers must only use stderr for logging (stdout is the MCP protocol)
  console.error(`[claude-peers] ${msg}`);
}

async function getGitRoot(cwd: string): Promise<string | null> {
  try {
    const proc = Bun.spawn(["git", "rev-parse", "--show-toplevel"], {
      cwd,
      stdout: "pipe",
      stderr: "ignore",
    });
    const text = await new Response(proc.stdout).text();
    const code = await proc.exited;
    if (code === 0) {
      return text.trim();
    }
  } catch {
    // not a git repo
  }
  return null;
}

function getTty(): string | null {
  try {
    // Try to get the parent's tty from the process tree
    const ppid = process.ppid;
    if (ppid) {
      const proc = Bun.spawnSync(["ps", "-o", "tty=", "-p", String(ppid)]);
      const tty = new TextDecoder().decode(proc.stdout).trim();
      if (tty && tty !== "?" && tty !== "??") {
        return tty;
      }
    }
  } catch {
    // ignore
  }
  return null;
}

// --- State ---

let myId: PeerId | null = null;
let myCwd = process.cwd();
let myGitRoot: string | null = null;
// claude-multi-peer: track the current summary in-process so whoami can report it
// without a broker round-trip. Kept in sync by set_summary and by the initial
// register call. Null until set_summary is called for the first time.
let mySummary: string | null = null;
let inboundPollingStarted = false;
let inboundPollTimer: ReturnType<typeof setInterval> | null = null;

// Day56 ack-based delivery (Option 1'): track messages this session has already pushed
// to the model but whose /ack-message has not yet succeeded. Used to (a) avoid re-pushing
// a duplicate when the broker still returns it (push ok but ack failed) and (b) retry the
// ack on the next poll. Entries are removed once the ack succeeds, so this stays bounded to
// the small set of currently-un-acked messages.
const pushedMessageIds = new Set<number>();

// B1: subagent virtual peer cache (role -> virtual peer id).
// Populated lazily on first tool call carrying subagent_role; persisted on broker side keyed by (parent_id, role).
const virtualPeerByRole = new Map<string, PeerId>();

async function resolveVirtualPeerId(role: string): Promise<PeerId> {
  if (!myId) {
    throw new Error("Main session not registered with broker yet");
  }
  const trimmed = role.trim();
  if (!trimmed) {
    throw new Error("subagent_role must be a non-empty string");
  }
  const cached = virtualPeerByRole.get(trimmed);
  if (cached) return cached;

  const res = await brokerFetch<RegisterVirtualPeerResponse>(
    "/register-virtual-peer",
    {
      parent_id: myId,
      role: trimmed,
      summary: `[subagent ${trimmed}]`,
    }
  );
  virtualPeerByRole.set(trimmed, res.id);
  log(
    `Virtual peer ${res.created ? "registered" : "resolved (existing)"} for role "${trimmed}": ${res.id}`
  );
  return res.id;
}

// --- MCP Server ---

const mcp = new Server(
  { name: "claude-multi-peer", version: "0.1.0" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `You are connected to the claude-multi-peer network. Other Claude Code instances on this machine can see you and send you messages.

IMPORTANT: When you receive a <channel source="claude-peers" ...> message, RESPOND IMMEDIATELY. Do not wait until your current task is finished. Pause what you are doing, reply to the message using send_message, then resume your work. Treat incoming peer messages like a coworker tapping you on the shoulder — answer right away, even if you're in the middle of something.

Read the from_id, from_summary, and from_cwd attributes to understand who sent the message. Reply by calling send_message with their from_id.

Available tools:
- whoami: Return your own peer ID and identity (cwd, git_root, summary) — call this first when you need to tell another peer who you are
- list_peers: Discover other Claude Code instances (scope: machine/directory/repo)
- send_message: Send a message to another instance by ID
- set_summary: Set a 1-2 sentence summary of what you're working on (visible to other peers)
- check_messages: Manually check for new messages

When you start, proactively call set_summary to describe what you're working on. This helps other instances understand your context.`,
  }
);

// --- Tool definitions ---

const SUBAGENT_ROLE_PROP = {
  type: "string" as const,
  description:
    'Optional subagent role label (e.g. "coordinator-boost"). When provided, the tool acts on behalf of a per-role virtual peer that has its own peer ID and inbox, distinct from the main Claude Code session. Use the same role string across calls within one subagent to keep its identity stable. Omit for normal main-session behavior.',
};

const TOOLS = [
  {
    name: "whoami",
    description:
      "Return your own peer identity — your randomly assigned peer ID, working directory, git root, and current summary. Call this when another peer asks who you are, when you need to include your own ID in a message body, or when you want to verify what identity the broker sees for you (e.g. after a restart). With subagent_role, returns the virtual peer's identity instead of the main session's.",
    inputSchema: {
      type: "object" as const,
      properties: {
        subagent_role: SUBAGENT_ROLE_PROP,
      },
    },
  },
  {
    name: "list_peers",
    description:
      "List other Claude Code instances (and registered subagent virtual peers) running on this machine. Returns their ID, working directory, git repo, summary, and — for subagents — their parent session and role.",
    inputSchema: {
      type: "object" as const,
      properties: {
        scope: {
          type: "string" as const,
          enum: ["machine", "directory", "repo"],
          description:
            'Scope of peer discovery. "machine" = all instances on this computer. "directory" = same working directory. "repo" = same git repository (including worktrees or subdirectories).',
        },
        subagent_role: SUBAGENT_ROLE_PROP,
      },
      required: ["scope"],
    },
  },
  {
    name: "send_message",
    description:
      "Send a message to another Claude Code instance (or subagent virtual peer) by peer ID. The message will be pushed into the target's session immediately via channel notification (or queued for explicit polling for subagent virtual peers). You can also send to an external chat-platform channel served by a running adapter by using an adapter to_id like \"adapter:discord:<channel_id>\"; the adapter (see adapters/discord/) will post the text to that channel.",
    inputSchema: {
      type: "object" as const,
      properties: {
        to_id: {
          type: "string" as const,
          description: "The peer ID of the target (from list_peers — can be a main session or a subagent virtual peer)",
        },
        message: {
          type: "string" as const,
          description: "The message to send",
        },
        subagent_role: SUBAGENT_ROLE_PROP,
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "set_summary",
    description:
      "Set a brief summary (1-2 sentences) of what you are currently working on. This is visible to other Claude Code instances when they list peers. With subagent_role, sets the virtual peer's summary instead of the main session's.",
    inputSchema: {
      type: "object" as const,
      properties: {
        summary: {
          type: "string" as const,
          description: "A 1-2 sentence summary of your current work",
        },
        subagent_role: SUBAGENT_ROLE_PROP,
      },
      required: ["summary"],
    },
  },
  {
    name: "check_messages",
    description:
      "Manually check for new messages from other Claude Code instances. Messages are normally pushed automatically via channel notifications to the main session, but subagents must use this with subagent_role to drain their own virtual peer inbox.",
    inputSchema: {
      type: "object" as const,
      properties: {
        subagent_role: SUBAGENT_ROLE_PROP,
      },
    },
  },
];

// --- Tool handlers ---

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;

  switch (name) {
    case "whoami": {
      const { subagent_role } = args as { subagent_role?: string };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        // For a subagent role, resolve to that virtual peer and fetch its record;
        // otherwise report the main session's own identity.
        if (subagent_role) {
          const targetId = await resolveVirtualPeerId(subagent_role);
          const peers = await brokerFetch<Peer[]>("/list-peers", {
            scope: "machine",
            cwd: myCwd,
            git_root: myGitRoot,
            exclude_id: null,
          });
          const me = peers.find((p) => p.id === targetId);
          const lines = [
            `ID: ${targetId}`,
            `Role: ${subagent_role}`,
            `Parent: ${myId}`,
            `CWD: ${myCwd}`,
          ];
          if (myGitRoot) lines.push(`Repo: ${myGitRoot}`);
          if (me?.summary) lines.push(`Summary: ${me.summary}`);
          return {
            content: [{ type: "text" as const, text: lines.join("\n") }],
          };
        }
        const lines = [
          `ID: ${myId}`,
          `PID: ${process.pid}`,
          `CWD: ${myCwd}`,
        ];
        if (myGitRoot) lines.push(`Repo: ${myGitRoot}`);
        if (mySummary) lines.push(`Summary: ${mySummary}`);
        else lines.push(`Summary: (not set — call set_summary to describe your work)`);
        return {
          content: [{ type: "text" as const, text: lines.join("\n") }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error resolving whoami: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "list_peers": {
      const scope = (args as { scope: string }).scope as "machine" | "directory" | "repo";
      const subagentRole = (args as { subagent_role?: string }).subagent_role;
      try {
        // Resolve identity: if subagent_role is provided, exclude its virtual peer id; else exclude main session id.
        let excludeId: PeerId | null = myId;
        if (subagentRole) {
          excludeId = await resolveVirtualPeerId(subagentRole);
        }
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope,
          cwd: myCwd,
          git_root: myGitRoot,
          exclude_id: excludeId,
        });

        if (peers.length === 0) {
          return {
            content: [
              {
                type: "text" as const,
                text: `No other Claude Code instances found (scope: ${scope}).`,
              },
            ],
          };
        }

        const lines = peers.map((p) => {
          const parts = [
            `ID: ${p.id}`,
            `PID: ${p.pid}`,
            `CWD: ${p.cwd}`,
          ];
          if (p.git_root) parts.push(`Repo: ${p.git_root}`);
          if (p.tty) parts.push(`TTY: ${p.tty}`);
          if (p.summary) parts.push(`Summary: ${p.summary}`);
          if (p.virtual_peer) {
            parts.push(`Virtual peer (subagent)`);
            if (p.role) parts.push(`Role: ${p.role}`);
            if (p.parent_id) parts.push(`Parent: ${p.parent_id}`);
          }
          parts.push(`Last seen: ${p.last_seen}`);
          return parts.join("\n  ");
        });

        return {
          content: [
            {
              type: "text" as const,
              text: `Found ${peers.length} peer(s) (scope: ${scope}):\n\n${lines.join("\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error listing peers: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "send_message": {
      const { to_id, message, subagent_role } = args as {
        to_id: string;
        message: string;
        subagent_role?: string;
      };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const fromId = subagent_role
          ? await resolveVirtualPeerId(subagent_role)
          : myId;
        const result = await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
          from_id: fromId,
          to_id,
          text: message,
        });
        if (!result.ok) {
          return {
            content: [{ type: "text" as const, text: `Failed to send: ${result.error}` }],
            isError: true,
          };
        }
        const fromLabel = subagent_role
          ? `subagent virtual peer ${fromId} (role: ${subagent_role})`
          : `peer ${fromId}`;
        return {
          content: [
            {
              type: "text" as const,
              text: `Message sent to peer ${to_id} from ${fromLabel}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error sending message: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "set_summary": {
      const { summary, subagent_role } = args as {
        summary: string;
        subagent_role?: string;
      };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const targetId = subagent_role
          ? await resolveVirtualPeerId(subagent_role)
          : myId;
        await brokerFetch("/set-summary", { id: targetId, summary });
        // claude-multi-peer: mirror main-session summary in-process so whoami reflects it.
        if (!subagent_role) mySummary = summary;
        const scope = subagent_role
          ? ` for subagent ${subagent_role} (${targetId})`
          : "";
        return {
          content: [
            { type: "text" as const, text: `Summary updated${scope}: "${summary}"` },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting summary: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "check_messages": {
      const { subagent_role } = args as { subagent_role?: string };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const targetId = subagent_role
          ? await resolveVirtualPeerId(subagent_role)
          : myId;
        const result = await brokerFetch<PollMessagesResponse>("/poll-messages", { id: targetId });
        const scope = subagent_role
          ? ` (subagent ${subagent_role} / ${targetId})`
          : "";
        if (result.messages.length === 0) {
          return {
            content: [{ type: "text" as const, text: `No new messages${scope}.` }],
          };
        }
        const lines = result.messages.map(
          (m) => `From ${m.from_id} (${m.sent_at}):\n${m.text}`
        );
        return {
          content: [
            {
              type: "text" as const,
              text: `${result.messages.length} new message(s)${scope}:\n\n${lines.join("\n\n---\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error checking messages: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

// --- Polling loop for inbound messages ---

async function pollAndPushMessages() {
  if (!myId) return;

  try {
    // Day56: use ack-based v2 poll — broker returns undelivered WITHOUT marking delivered.
    // We ack each message only AFTER its push succeeds, so a push that never surfaces
    // (busy/mid-turn session) is redelivered on the next poll instead of being silently lost.
    const result = await brokerFetch<PollMessagesResponse>("/poll-messages-v2", { id: myId });

    for (const msg of result.messages) {
      // Already pushed this session but not yet acked (previous ack failed): just retry the
      // ack, do NOT re-push (avoids a duplicate surfacing to the model).
      if (pushedMessageIds.has(msg.id)) {
        try {
          await brokerFetch("/ack-message", { id: msg.id });
          pushedMessageIds.delete(msg.id);
        } catch (ackErr) {
          log(`Re-ack failed for message ${msg.id}: ${ackErr instanceof Error ? ackErr.message : String(ackErr)}`);
        }
        continue;
      }

      // Look up the sender's info for context
      let fromSummary = "";
      let fromCwd = "";
      try {
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope: "machine",
          cwd: myCwd,
          git_root: myGitRoot,
        });
        const sender = peers.find((p) => p.id === msg.from_id);
        if (sender) {
          fromSummary = sender.summary;
          fromCwd = sender.cwd;
        }
      } catch {
        // Non-critical, proceed without sender info
      }

      // Push as channel notification — this is what makes it immediate.
      // If this throws, we skip the ack below → the message stays undelivered → redelivered.
      await mcp.notification({
        method: "notifications/claude/channel",
        params: {
          content: msg.text,
          meta: {
            from_id: msg.from_id,
            from_summary: fromSummary,
            from_cwd: fromCwd,
            sent_at: msg.sent_at,
          },
        },
      });

      // Push succeeded — record it, then confirm delivery to the broker.
      pushedMessageIds.add(msg.id);
      try {
        await brokerFetch("/ack-message", { id: msg.id });
        pushedMessageIds.delete(msg.id); // acked cleanly; broker won't return it again
      } catch (ackErr) {
        // Push landed but ack failed: keep the id so we retry the ack (only) next poll.
        log(`Ack failed for message ${msg.id} (will retry): ${ackErr instanceof Error ? ackErr.message : String(ackErr)}`);
      }

      log(`Pushed message from ${msg.from_id}: ${msg.text.slice(0, 80)}`);
    }
  } catch (e) {
    // Broker might be down temporarily, don't crash
    log(`Poll error: ${e instanceof Error ? e.message : String(e)}`);
  }
}

function startInboundMessagePolling() {
  if (inboundPollingStarted) {
    return;
  }

  const clientCapabilities = mcp.getClientCapabilities();
  const clientVersion = mcp.getClientVersion();
  const clientName = clientVersion?.name ?? "unknown";
  const clientVersionText = clientVersion?.version ? ` ${clientVersion.version}` : "";

  log(`Client connected: ${clientName}${clientVersionText}`);
  log(`Client capabilities: ${formatClientCapabilities(clientCapabilities)}`);

  const forcePoll = process.env.CLAUDE_PEERS_FORCE_POLL === "1";
  const hasChannel = supportsClaudeChannel(clientCapabilities);

  // DEBUG: write capabilities + state to a file for inspection.
  // FIX (edhiblemeer/claude-multi-peer Day72 2026-07-21): path was hardcoded to Windows dev
  // path `D:/dev/claude-peers-mcp/debug-capabilities.log` which silently failed on Linux/macOS
  // hosts, hiding capability-negotiation state. Route through homedir() so the log lands
  // predictably on any platform and can be overridden via env for CI/tests.
  try {
    const fs = require("node:fs");
    const debugPath =
      process.env.CLAUDE_PEERS_DEBUG_LOG ?? `${homedir()}/.claude-multi-peer-debug.log`;
    const debugLine = `[${new Date().toISOString()}] pid=${process.pid} cwd=${myCwd} client=${clientName}${clientVersionText} forcePoll=${forcePoll} hasChannel=${hasChannel} caps=${formatClientCapabilities(clientCapabilities)}\n`;
    fs.appendFileSync(debugPath, debugLine);
  } catch (e) {
    log(`Debug log write failed: ${e}`);
  }

  if (!hasChannel) {
    if (forcePoll) {
      log(
        "Client does not advertise experimental claude/channel, but CLAUDE_PEERS_FORCE_POLL=1 is set — forcing polling anyway."
      );
    } else {
      log(
        "Client does not advertise experimental claude/channel; skipping background polling so check_messages remains reliable. (Set CLAUDE_PEERS_FORCE_POLL=1 to override)"
      );
      return;
    }
  }

  inboundPollTimer = setInterval(pollAndPushMessages, POLL_INTERVAL_MS);
  inboundPollingStarted = true;
  log("Client supports claude/channel; background polling enabled.");
}

// --- Startup ---

async function main() {
  // 1. Ensure broker is running
  await ensureBroker();

  // 2. Gather context
  myCwd = process.cwd();
  myGitRoot = await getGitRoot(myCwd);
  const tty = getTty();

  log(`CWD: ${myCwd}`);
  log(`Git root: ${myGitRoot ?? "(none)"}`);
  log(`TTY: ${tty ?? "(unknown)"}`);

  // 3. Generate initial summary via gpt-5.4-nano (non-blocking, best-effort)
  let initialSummary = "";
  const summaryPromise = (async () => {
    try {
      const branch = await getGitBranch(myCwd);
      const recentFiles = await getRecentFiles(myCwd);
      const summary = await generateSummary({
        cwd: myCwd,
        git_root: myGitRoot,
        git_branch: branch,
        recent_files: recentFiles,
      });
      if (summary) {
        initialSummary = summary;
        log(`Auto-summary: ${summary}`);
      }
    } catch (e) {
      log(`Auto-summary failed (non-critical): ${e instanceof Error ? e.message : String(e)}`);
    }
  })();

  // Wait briefly for summary, but don't block startup
  await Promise.race([summaryPromise, new Promise((r) => setTimeout(r, 3000))]);

  // 4. Register with broker
  const reg = await brokerFetch<RegisterResponse>("/register", {
    pid: process.pid,
    cwd: myCwd,
    git_root: myGitRoot,
    tty,
    summary: initialSummary,
  });
  myId = reg.id;
  log(`Registered as peer ${myId}`);

  // If summary generation is still running, update it when done
  if (!initialSummary) {
    summaryPromise.then(async () => {
      if (initialSummary && myId) {
        try {
          await brokerFetch("/set-summary", { id: myId, summary: initialSummary });
          log(`Late auto-summary applied: ${initialSummary}`);
        } catch {
          // Non-critical
        }
      }
    });
  }

  // 5. Connect MCP over stdio
  mcp.oninitialized = () => {
    startInboundMessagePolling();
  };

  await mcp.connect(new StdioServerTransport());
  log("MCP connected");

  // 6. Start polling for inbound messages only when the client supports channel push
  //    Non-channel clients rely on check_messages so their messages stay queued.

  // 7. Start heartbeat
  const heartbeatTimer = setInterval(async () => {
    if (myId) {
      try {
        await brokerFetch("/heartbeat", { id: myId });
      } catch {
        // Non-critical
      }
    }
  }, HEARTBEAT_INTERVAL_MS);

  // 8. Clean up on exit
  const cleanup = async () => {
    if (inboundPollTimer) {
      clearInterval(inboundPollTimer);
      inboundPollTimer = null;
    }
    clearInterval(heartbeatTimer);
    if (myId) {
      try {
        await brokerFetch("/unregister", { id: myId });
        log("Unregistered from broker");
      } catch {
        // Best effort
      }
    }
    process.exit(0);
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);
}

main().catch((e) => {
  log(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});
