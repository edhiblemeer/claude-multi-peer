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
`supportsClaudeChannel()` returns false and the server hits an early
`return;` — no polling, no `notifications/claude/channel` push.

### Fix

Start each session with `CLAUDE_PEERS_FORCE_POLL=1` set:

```bash
CLAUDE_PEERS_FORCE_POLL=1 claude \
  --dangerously-load-development-channels server:claude-multi-peer --continue
```

This makes the server start polling regardless of the capability handshake. It
does **not** bypass any security check — it just decouples polling from
capability negotiation.

For a permanent fix, put the export in the shell script that launches your
fleet (or in your shell rc):

```bash
export CLAUDE_PEERS_FORCE_POLL=1
```

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

### Root cause

`server.ts` calls `/register` once at startup and never re-registers. If the
broker restarts, every existing peer is invisible to the new broker until it
runs its `main()` again — which only happens on a fresh session launch.

### Workaround (until we ship an auto re-register loop)

Restart every peer session after any broker restart. In our fleet that means
`Ctrl+C` in each terminal and relaunching with the same `claude` command.

## Symptom: Persistent peer IDs aren't reused after a restart

The README promises that within `CLAUDE_PEERS_ID_REUSE_WINDOW_HOURS`
(default 24), a re-registering peer with the same `cwd + git_root` gets its
old ID back. In practice we saw fresh IDs on every restart.

### What we saw

Restarting a session that had been running from `/home/fleet/dev/boost` a few
minutes earlier still minted a brand-new ID. Any external mapping (Discord
bridge routing tables, dashboards) that remembered the previous ID broke.

### Workaround

Use a `refresh_map`-style script that dynamically resolves `cwd → peer ID`
via `bun cli.ts peers` and rewrites your external mapping (e.g. bridge
`SERVER_MAP_JSON`) on every session bootstrap. See the launcher example
below.

The persistent-ID path itself may need a separate investigation — please
file an issue with your reproduction (`git rev-parse --show-toplevel` +
sequence of registers) if you hit it.

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
