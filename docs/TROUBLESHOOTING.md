# Troubleshooting

Real-world stumbling blocks we hit when migrating a 5-session fleet from
`louislva/claude-peers-mcp` to `claude-multi-peer`, and the workarounds/fixes
that got us unstuck. Discovered 2026-07-21.

## Symptom: Peers register but pushed messages never appear in Claude UI

`list_peers` shows every session and `send_message` returns success, but the
receiving Claude Code session does not see the message in its transcript. You
have to call `check_messages` manually to drain the inbox.

### Root cause

The inbound polling loop only starts when the MCP client advertises the
experimental `claude/channel` capability. Some Claude Code launches don't
advertise it (or advertise it under a name the server doesn't recognise), so
`supportsClaudeChannel()` returns false and the server historically hit an early
`return;` — no polling, no `notifications/claude/channel` push.

### Fix

**As of Day67 (2026-07-31): polling now defaults ON**, so this symptom no longer
occurs out of the box. The server polls even when the client doesn't advertise
`claude/channel`. No env setup required.

If you specifically want to opt out (rely on `check_messages` only), set:

```bash
export CLAUDE_PEERS_FORCE_POLL=0
```

The old `CLAUDE_PEERS_FORCE_POLL=1` opt-in flag is still accepted as a no-op
for backward compatibility with existing launcher scripts.

## Symptom: `server:claude-multi-peer` starts the wrong (old) broker

You register the MCP server, launch a session with
`--dangerously-load-development-channels server:claude-multi-peer`, but a
different broker (e.g. the upstream `claude-peers-mcp`) picks up the peer.
Signs: broker cwd is the old repo path (`/proc/<pid>/cwd`), or
`lsof <homedir>/.claude-multi-peer.db` returns nothing.

### Root cause

If `claude-multi-peer` isn't in the MCP registry when the session starts, the
CLI falls back to whatever server *is* registered under a similar name
(e.g. an upstream `claude-peers` you installed earlier). The session appears
"working" but is actually talking to the old broker with the old database and
none of the new features (persistent IDs, whoami, adapter routing).

### Fix

Register `claude-multi-peer` **first**, then launch sessions:

```bash
claude mcp add --scope user --transport stdio claude-multi-peer -- \
  bun ~/claude-multi-peer/server.ts

# Optional: remove the old one to prevent name collisions
claude mcp remove --scope user claude-peers
```

Verify with `claude mcp list` — you want a single `✔ Connected` for
`claude-multi-peer` and no old duplicates.

## Symptom: Restarting the broker leaves peers orphaned

You kill and restart the broker (e.g. after upgrading). All existing sessions
still show up in `list_peers` from their own perspective, but the new broker
sees none of them, so cross-session messages stop working.

### Status (as of 2026-07-31)

PR #4 (soft-delete tombstones), PR #5 (`CLAUDE_PEERS_FORCE_POLL` default ON),
and the auto-re-register PR that landed on 2026-07-31 make broker restart
transparent for the common cases:

- **Short bounce with DB intact** — sessions keep polling; the restarted broker
  still has their rows in `peers`, so nothing breaks. Verified by the
  4-scenario isolated-port test run on 2026-07-31.
- **Long outage with alive sessions** — soft-delete keeps the tombstoned row
  around for `CLAUDE_PEERS_TOMBSTONE_TTL_HOURS` (default 24h), so re-register
  under the same `(cwd, git_root)` fingerprint reuses the original ID and
  external routing tables stay valid.
- **DB wipe (or tombstone TTL expired)** — `/heartbeat` and `/poll-messages-v2`
  now return `HTTP 410 {ok:false, error:"unknown_peer", id}` when the id has
  no live row. The MCP server catches this on the next heartbeat (~15s) or
  poll tick (~1s) and calls `reregisterWithBroker()` in place — a fresh ID is
  minted, the session keeps running, and the recovery is logged via
  `console.error` (`[server] broker returned unknown_peer for <old>; re-registered as <new>`).

If sessions still fail to receive messages after a broker restart, check
`~/.claude-multi-peer-debug.log` for `unknown_peer` re-register events, and
file an issue with the log excerpt.

### Legacy workaround — only needed if auto re-register fails

If for some reason the re-register does not fire (e.g. broker completely
unreachable, or an older `server.ts` build without the recovery path),
restart every peer session: `Ctrl+C` in each terminal and relaunch with the
same `claude` command.

## Symptom: Persistent peer IDs aren't reused after a restart

The README promises that within `CLAUDE_PEERS_ID_REUSE_WINDOW_HOURS`
(default 24), a re-registering peer with the same `cwd + git_root` gets its
old ID back. In practice, before PR #4, we saw fresh IDs on every restart.

### Status (fixed in PR #4, 2026-07-31)

Root cause was that `cleanStalePeers()` hard-deleted rows within ~2 minutes
of the previous PID dying, so the reuse table `findReusableId()` searched
was empty long before the 24h window mattered.

PR #4 replaces the hard-delete with a soft-delete tombstone. Dead rows are
kept with `deleted_at` set, `findReusableId()` accepts tombstoned rows
within the reuse window, and a slower sweep hard-deletes tombstones once
they age past `CLAUDE_PEERS_TOMBSTONE_TTL_HOURS` (default 24h).

Combined with the auto re-register (see previous section), external mappings
(Discord bridge routing tables, dashboards) stay valid across a restart in
all common cases — no `refresh_map` script needed.

If you still see fresh IDs after a restart within the reuse window, verify:

- `CLAUDE_PEERS_SOFT_DELETE` is not set to `0` (kill switch — hard-delete path)
- The tombstone TTL hasn't been shortened below your restart interval
- The `(cwd, git_root)` fingerprint really matches (check `bun cli.ts peers`
  before and after the restart)

If all of the above check out and you still see fresh IDs, please file an
issue with your reproduction (`git rev-parse --show-toplevel` + sequence of
registers).

## Symptom: Debug log write "silently" swallowed on Linux/macOS

Older builds hardcoded a Windows path
(`D:/dev/claude-peers-mcp/debug-capabilities.log`) for the capability-negotiation
debug log. On non-Windows hosts the write threw an `ENOENT`/`EACCES` that was
caught and logged only through the MCP stderr channel, so the debug file never
appeared where you'd expect it.

### Fix

Fixed in commit `d9a144d` — the log now lives at
`~/.claude-multi-peer-debug.log`, and `CLAUDE_PEERS_DEBUG_LOG=/some/path`
overrides it for CI or per-session diagnosis.

## Example: idempotent fleet launcher

If you launch several sessions at once (one per project/lane), this pattern
keeps each session pointed at the right server and inherits `FORCE_POLL`:

```bash
#!/usr/bin/env bash
BASE="$HOME/dev"
export CLAUDE_PEERS_FORCE_POLL=1
CLAUDE='claude --dangerously-load-development-channels server:claude-multi-peer --continue'

# idempotent broker start
if ! ss -ltn 2>/dev/null | grep -q ':7899 '; then
  ( cd "$HOME/claude-multi-peer" && nohup "$HOME/.bun/bin/bun" broker.ts > /tmp/multi-peer-broker.log 2>&1 & )
  sleep 3
fi

for name in project-a project-b project-c; do
  dir="$BASE/$name"
  [ -d "$dir" ] || continue
  # ...spawn a terminal tab that cds into $dir and runs $CLAUDE...
done
```

## When you hit something new

Please open an issue at
<https://github.com/edhiblemeer/claude-multi-peer/issues> with:

- What you were trying to do
- The exact `claude mcp list` output
- `lsof -i :7899` (broker) and `lsof <homedir>/.claude-multi-peer.db`
- The last few lines of `~/.claude-multi-peer-debug.log`
- Whether `CLAUDE_PEERS_FORCE_POLL=1` was set

Every stumbling block above came from real production use — chances are
someone else will hit the same one.
