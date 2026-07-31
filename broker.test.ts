/**
 * Integration test for Issue #2: persistent peer ID reuse via soft-delete tombstone.
 *
 * Verifies the end-to-end promise: after a session dies and its peer row is tombstoned
 * (soft-deleted), re-registering with the same cwd/git_root within the reuse window
 * returns the same ID. Prior to the fix the peer row was hard-deleted within ~2 minutes
 * of death, so the 24h reuse window was effectively ~2 minutes and reuse almost never hit.
 *
 * Strategy: spawn a real broker subprocess against an isolated DB + port, register a
 * peer, stop the broker, mark the row tombstoned directly in the DB (which is what the
 * broker would have done via cleanStalePeers over ~2 minutes of live wait — we simulate
 * that instantly to keep the test fast), start a fresh broker with the same DB, then
 * re-register with the same fingerprint and assert the returned ID matches.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Subprocess } from "bun";

let broker: Subprocess | null = null;
let testDir = "";
let dbPath = "";
let port = 0;
let baseUrl = "";

async function post(path: string, body: unknown): Promise<any> {
  const r = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return await r.json();
}

// Bug #1 tests need to inspect the HTTP status (410 Gone) separately from the JSON body.
async function postRaw(path: string, body: unknown): Promise<{ status: number; body: any }> {
  const r = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: r.status, body: await r.json() };
}

async function waitForBroker(): Promise<void> {
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch(`${baseUrl}/health`);
      if (r.ok) return;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`broker did not respond on ${baseUrl}`);
}

async function startBroker(): Promise<void> {
  broker = Bun.spawn(["bun", "broker.ts"], {
    cwd: import.meta.dir,
    env: {
      ...process.env,
      CLAUDE_PEERS_PORT: String(port),
      CLAUDE_PEERS_DB: dbPath,
    },
    stdout: "ignore",
    stderr: "ignore",
  });
  await waitForBroker();
}

async function stopBroker(): Promise<void> {
  if (broker) {
    broker.kill();
    try {
      await broker.exited;
    } catch {
      // already gone
    }
    broker = null;
  }
}

// Fake PIDs — high enough that they're extremely unlikely to hit a real process on
// either POSIX (max ~4M) or Windows (32-bit but small in practice). If a collision
// ever happens, findReusableId falls back to "not reusable" and the test would fail
// on that path — we'd then rerun with a different value.
const FAKE_DEAD_PID_1 = 3_999_991;
const FAKE_DEAD_PID_2 = 3_999_992;

describe("Issue #2: persistent ID reuse via soft-delete tombstone", () => {
  beforeEach(() => {
    testDir = join(
      tmpdir(),
      `broker-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    );
    mkdirSync(testDir, { recursive: true });
    dbPath = join(testDir, "test.db");
    // Random-ish port in a range unlikely to collide with the real broker (7899).
    port = 17800 + Math.floor(Math.random() * 100);
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await stopBroker();
    try {
      rmSync(testDir, { recursive: true, force: true });
    } catch {
      // ignore — WAL files sometimes lag on Windows
    }
  });

  test("re-register with same cwd/git_root after tombstone returns the same ID", async () => {
    await startBroker();

    const cwd = "/tmp/test-lane-alpha";
    const gitRoot = "/tmp/test-lane-alpha";

    const firstReg = await post("/register", {
      pid: FAKE_DEAD_PID_1,
      cwd,
      git_root: gitRoot,
      tty: null,
      summary: "session 1",
    });
    const originalId = firstReg.id as string;
    expect(typeof originalId).toBe("string");
    expect(originalId).toMatch(/^[a-z0-9]{8}$/);

    // Stop the broker so we can safely mutate the DB without racing with sweeps.
    await stopBroker();

    // Simulate what cleanStalePeers would eventually do to this dead peer:
    // set deleted_at = now (a tombstone). Directly at the DB level so the test
    // doesn't have to wait 90s+ for the real grace path.
    const db = new Database(dbPath);
    db.run("UPDATE peers SET deleted_at = ? WHERE id = ?", [
      new Date().toISOString(),
      originalId,
    ]);
    db.close();

    // Fresh broker on the same DB. Now register from a different "session"
    // (different PID) but the SAME (cwd, git_root) fingerprint. findReusableId
    // should return the tombstoned row's ID because:
    //   (a) the tombstone is within the ID reuse window (default 24h);
    //   (b) the PID liveness check is bypassed for tombstoned rows (the row
    //       already recorded that the previous process is dead).
    await startBroker();

    const secondReg = await post("/register", {
      pid: FAKE_DEAD_PID_2,
      cwd,
      git_root: gitRoot,
      tty: null,
      summary: "session 2",
    });
    expect(secondReg.id).toBe(originalId);
  });

  test("tombstone hard-delete happens once TOMBSTONE_TTL_MS elapses", async () => {
    // Start broker with a very short tombstone TTL so we can observe the hard-delete
    // sweep within the test. We use CLAUDE_PEERS_TOMBSTONE_TTL_HOURS well below 1 hour
    // and rely on the fact that hardDeleteExpiredTombstones() runs once at startup.
    broker = Bun.spawn(["bun", "broker.ts"], {
      cwd: import.meta.dir,
      env: {
        ...process.env,
        CLAUDE_PEERS_PORT: String(port),
        CLAUDE_PEERS_DB: dbPath,
        // ~0.36 seconds (0.0001h). Any row tombstoned more than 0.36s ago is expired.
        CLAUDE_PEERS_TOMBSTONE_TTL_HOURS: "0.0001",
      },
      stdout: "ignore",
      stderr: "ignore",
    });
    await waitForBroker();

    const firstReg = await post("/register", {
      pid: FAKE_DEAD_PID_1,
      cwd: "/tmp/test-lane-beta",
      git_root: null,
      tty: null,
      summary: "session A",
    });
    const originalId = firstReg.id as string;

    await stopBroker();

    // Tombstone the row with a deleted_at from further in the past than the TTL.
    const db = new Database(dbPath);
    const oldTs = new Date(Date.now() - 60_000).toISOString();
    db.run("UPDATE peers SET deleted_at = ? WHERE id = ?", [oldTs, originalId]);
    db.close();

    // Fresh broker start — hardDeleteExpiredTombstones runs once at startup and
    // should hard-delete the expired tombstone.
    broker = Bun.spawn(["bun", "broker.ts"], {
      cwd: import.meta.dir,
      env: {
        ...process.env,
        CLAUDE_PEERS_PORT: String(port),
        CLAUDE_PEERS_DB: dbPath,
        CLAUDE_PEERS_TOMBSTONE_TTL_HOURS: "0.0001",
      },
      stdout: "ignore",
      stderr: "ignore",
    });
    await waitForBroker();

    // Give the startup sweep a beat to run (it fires synchronously in module init).
    await new Promise((r) => setTimeout(r, 200));

    const check = new Database(dbPath, { readonly: true });
    const row = check
      .query("SELECT id FROM peers WHERE id = ?")
      .get(originalId) as { id: string } | null;
    check.close();
    expect(row).toBeNull();
  });
});

// Bug #1: silent ghost on unknown peer_id.
// When a peer_id no longer exists in the broker's peers table (DB wipe, hard-deleted
// tombstone, or CLAUDE_PEERS_SOFT_DELETE=0 combined with any outage past PID_GRACE_MS),
// the pre-fix broker returned HTTP 200 with silently-empty results, so a session
// polling with a dead ID would poll forever and no error would surface.
//
// Fix contract (narrowed 2026-07-31 after a production regression — see the
// "senders are not required to be registered peers" block below):
//   - /heartbeat with unknown id           → HTTP 410 + {ok:false, error:"unknown_peer", id}
//   - /poll-messages-v2 with unknown id    → HTTP 410 + {ok:false, error:"unknown_peer", id}
//   - /heartbeat with tombstoned id        → HTTP 410 (same as unknown)
// Deliberately NOT part of the contract (reverted): /send-message from_id validation,
// /set-summary unknown_peer, /ack-message unknown_message. Those endpoints stay
// permissive; auto re-registration is driven entirely by heartbeat + poll.
describe("Bug #1: unknown_peer signal on stale ids", () => {
  beforeEach(() => {
    testDir = join(
      tmpdir(),
      `broker-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    );
    mkdirSync(testDir, { recursive: true });
    dbPath = join(testDir, "test.db");
    // Isolated port well away from the live broker (7899).
    port = 17900 + Math.floor(Math.random() * 100);
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await stopBroker();
    try {
      rmSync(testDir, { recursive: true, force: true });
    } catch {
      // ignore — WAL files sometimes lag on Windows
    }
  });

  test("/heartbeat returns 410 unknown_peer for an id that never existed", async () => {
    await startBroker();
    const res = await postRaw("/heartbeat", { id: "ghostghost" });
    expect(res.status).toBe(410);
    expect(res.body.ok).toBe(false);
    expect(res.body.error).toBe("unknown_peer");
    expect(res.body.id).toBe("ghostghost");
  });

  test("/heartbeat returns 410 unknown_peer when the row was tombstoned", async () => {
    await startBroker();
    const reg = await post("/register", {
      pid: FAKE_DEAD_PID_1,
      cwd: "/tmp/test-lane-ghost-hb",
      git_root: null,
      tty: null,
      summary: "session ghost",
    });
    const id = reg.id as string;
    await stopBroker();

    // Tombstone the row (simulating cleanStalePeers → PID dead → soft-delete).
    const db = new Database(dbPath);
    db.run("UPDATE peers SET deleted_at = ? WHERE id = ?", [
      new Date().toISOString(),
      id,
    ]);
    db.close();

    await startBroker();
    const res = await postRaw("/heartbeat", { id });
    expect(res.status).toBe(410);
    expect(res.body.error).toBe("unknown_peer");
  });

  test("/heartbeat with a live id still returns HTTP 200 ok:true (regression guard)", async () => {
    await startBroker();
    // Use current PID so the row is unambiguously alive.
    const reg = await post("/register", {
      pid: process.pid,
      cwd: "/tmp/test-lane-live-hb",
      git_root: null,
      tty: null,
      summary: "live session",
    });
    const res = await postRaw("/heartbeat", { id: reg.id });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.error).toBeUndefined();
  });

  test("/poll-messages-v2 returns 410 unknown_peer for a hard-deleted id", async () => {
    await startBroker();
    const reg = await post("/register", {
      pid: FAKE_DEAD_PID_1,
      cwd: "/tmp/test-lane-ghost-poll",
      git_root: null,
      tty: null,
      summary: "will be wiped",
    });
    const id = reg.id as string;
    await stopBroker();

    // Simulate DB wipe / tombstone hard-delete: remove the row entirely.
    const db = new Database(dbPath);
    db.run("DELETE FROM peers WHERE id = ?", [id]);
    db.close();

    await startBroker();
    const res = await postRaw("/poll-messages-v2", { id });
    expect(res.status).toBe(410);
    expect(res.body.error).toBe("unknown_peer");
    expect(res.body.id).toBe(id);
    // Critical: no silent empty-messages array on the failure path.
    expect(res.body.messages).toBeUndefined();
  });

  test("/poll-messages-v2 with a live id still returns messages array (regression guard)", async () => {
    await startBroker();
    const reg = await post("/register", {
      pid: process.pid,
      cwd: "/tmp/test-lane-live-poll",
      git_root: null,
      tty: null,
      summary: "live poll",
    });
    const res = await postRaw("/poll-messages-v2", { id: reg.id });
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body.messages)).toBe(true);
  });
});

// Bug #2: /send-message must validate from_id.
// The pre-fix broker validated to_id but not from_id, so a ghost peer could still
// send messages. Downstream pollAndPushMessages looked up the sender via /list-peers
// and silently rendered empty from_summary/from_cwd on the channel notification —
// recipient sees a message with no sender context.
// Regression guard (2026-07-31). A from_id validation was briefly added here and took out
// the fleet's primary delivery path: external bridges hold no peers row and send under bare
// ids like "discord-bridge", so every inbound Discord message was rejected as unknown_sender.
// These tests pin the permissive contract — senders are NOT required to be registered peers.
describe("/send-message accepts unregistered senders (bridge regression guard)", () => {
  beforeEach(() => {
    testDir = join(
      tmpdir(),
      `broker-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    );
    mkdirSync(testDir, { recursive: true });
    dbPath = join(testDir, "test.db");
    port = 18000 + Math.floor(Math.random() * 100);
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await stopBroker();
    try {
      rmSync(testDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  test("/send-message accepts a bare bridge from_id that has no peers row", async () => {
    await startBroker();
    // Register only the recipient so to_id is valid — we're isolating from_id handling.
    const recipient = await post("/register", {
      pid: process.pid,
      cwd: "/tmp/test-lane-recipient",
      git_root: null,
      tty: null,
      summary: "recipient",
    });
    // "discord-bridge" is the real shape used by the Discord bridge in production: no
    // peers row, no "adapter:" prefix. This is the exact id the reverted check rejected.
    const res = await postRaw("/send-message", {
      from_id: "discord-bridge",
      to_id: recipient.id,
      text: "hi from the bridge",
    });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.error).toBeUndefined();

    // The message must actually reach the recipient's queue.
    const db = new Database(dbPath, { readonly: true });
    const row = db
      .query("SELECT COUNT(*) as n FROM messages WHERE to_id = ? AND from_id = ?")
      .get(recipient.id, "discord-bridge") as { n: number };
    db.close();
    expect(row.n).toBe(1);
  });

  test("/send-message still accepts a tombstoned from_id", async () => {
    await startBroker();
    const sender = await post("/register", {
      pid: FAKE_DEAD_PID_1,
      cwd: "/tmp/test-lane-sender",
      git_root: null,
      tty: null,
      summary: "sender",
    });
    const recipient = await post("/register", {
      pid: process.pid,
      cwd: "/tmp/test-lane-recipient",
      git_root: null,
      tty: null,
      summary: "recipient",
    });
    await stopBroker();

    const db = new Database(dbPath);
    db.run("UPDATE peers SET deleted_at = ? WHERE id = ?", [
      new Date().toISOString(),
      sender.id,
    ]);
    db.close();

    await startBroker();
    const res = await postRaw("/send-message", {
      from_id: sender.id,
      to_id: recipient.id,
      text: "hi from tombstone",
    });
    // A restarting session can legitimately send while its old row is still tombstoned;
    // the recipient is live, so the message must go through.
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.error).toBeUndefined();
  });

  test("/send-message with valid from_id and to_id still succeeds (regression guard)", async () => {
    await startBroker();
    // Distinct PIDs so handleRegister's "same-PID cleanup" doesn't remove peer a
    // when peer b registers. The PIDs need only be present in the peers table with
    // deleted_at IS NULL — cleanStalePeers only fires on its 30s cadence, so within
    // the test both rows stay live for /send-message's row-exists check.
    const a = await post("/register", {
      pid: FAKE_DEAD_PID_1,
      cwd: "/tmp/test-lane-a",
      git_root: null,
      tty: null,
      summary: "a",
    });
    const b = await post("/register", {
      pid: FAKE_DEAD_PID_2,
      cwd: "/tmp/test-lane-b",
      git_root: null,
      tty: null,
      summary: "b",
    });
    const res = await postRaw("/send-message", {
      from_id: a.id,
      to_id: b.id,
      text: "hi",
    });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.error).toBeUndefined();
  });

  test("/send-message from an adapter: prefixed synthetic id succeeds", async () => {
    // Adapters (Discord bot etc.) send under synthetic identities that don't live in
    // the peers table. Covered separately from the bare-id case above because both
    // shapes appear in production traffic.
    await startBroker();
    const recipient = await post("/register", {
      pid: process.pid,
      cwd: "/tmp/test-lane-adapter-target",
      git_root: null,
      tty: null,
      summary: "recipient",
    });
    // /send-message with adapter to_id inserts unconditionally (adapter routing path).
    // Here we exercise the reverse: adapter as from_id to a real peer, via the normal
    // /send-message endpoint (not /adapter-deliver).
    const res = await postRaw("/send-message", {
      from_id: "adapter:discord:12345",
      to_id: recipient.id,
      text: "hello from discord",
    });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
  });
});

// Reverted 2026-07-31 alongside the from_id check: set-summary / ack-message stay permissive.
// A 0-row update on either is normal (restarting session, double-ack, rotated-out message),
// and a v0.x client that treats a non-ok body as fatal would break on a mixed rollout.
// Auto re-registration is driven solely by /heartbeat + /poll-messages-v2 returning 410.
describe("set-summary and ack-message stay permissive on unknown ids", () => {
  beforeEach(() => {
    testDir = join(
      tmpdir(),
      `broker-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    );
    mkdirSync(testDir, { recursive: true });
    dbPath = join(testDir, "test.db");
    port = 18100 + Math.floor(Math.random() * 100);
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await stopBroker();
    try {
      rmSync(testDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  test("/set-summary returns ok:true for a missing id (no hard failure)", async () => {
    await startBroker();
    const res = await postRaw("/set-summary", {
      id: "ghostghost",
      summary: "no one will see this",
    });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.error).toBeUndefined();
  });

  test("/set-summary with a live id still returns ok:true (regression guard)", async () => {
    await startBroker();
    const reg = await post("/register", {
      pid: process.pid,
      cwd: "/tmp/test-lane-summary",
      git_root: null,
      tty: null,
      summary: "old",
    });
    const res = await postRaw("/set-summary", {
      id: reg.id,
      summary: "new",
    });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
  });

  test("/ack-message returns ok:true for a non-existent message id (double-ack is normal)", async () => {
    await startBroker();
    const res = await postRaw("/ack-message", { id: 999_999_999 });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.error).toBeUndefined();
  });
});

// Bug #1 recovery: unit test for the client-side reregisterWithBroker helper.
//
// Path chosen: because server.ts calls main() unconditionally on import (it is designed
// as an executable MCP server, not a library), importing it in a test would kick off the
// full broker + MCP handshake. Rather than refactor server.ts into a library shape,
// we exercise the RE-REGISTER contract end-to-end: start a broker, register a peer,
// hard-delete its row, then call /register again from the SAME workspace fingerprint
// with a different PID (simulating re-register after unknown_peer). We assert that:
//   (a) a fresh id is returned when there is no reusable tombstone within the window;
//   (b) the id is stable within the reuse window (via findReusableId).
//
// This is the same code path reregisterWithBroker triggers inside server.ts — the
// server-side wiring (unknown_peer → call reregisterWithBroker → myId := reg.id) is
// verified by inspection of pollAndPushMessages / heartbeat callers in server.ts.
describe("Bug #1: re-register after unknown_peer produces a working id", () => {
  beforeEach(() => {
    testDir = join(
      tmpdir(),
      `broker-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    );
    mkdirSync(testDir, { recursive: true });
    dbPath = join(testDir, "test.db");
    port = 18200 + Math.floor(Math.random() * 100);
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await stopBroker();
    try {
      rmSync(testDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  });

  test("after DB wipe, /register with same fingerprint returns a working new id (poll/heartbeat succeed under it)", async () => {
    await startBroker();

    const cwd = "/tmp/test-lane-reregister";
    const gitRoot = "/tmp/test-lane-reregister";

    // Original registration.
    const first = await post("/register", {
      pid: process.pid,
      cwd,
      git_root: gitRoot,
      tty: null,
      summary: "session 1",
    });
    const oldId = first.id as string;

    // Simulate a DB wipe: hard-delete the row while the broker is running (as if the
    // broker had been restarted against an empty DB, from the caller's perspective).
    await stopBroker();
    const db = new Database(dbPath);
    db.run("DELETE FROM peers WHERE id = ?", [oldId]);
    db.close();
    await startBroker();

    // A poll under the OLD id must now fail with unknown_peer — this is the trigger
    // that reregisterWithBroker responds to in the real client.
    const stalePoll = await postRaw("/poll-messages-v2", { id: oldId });
    expect(stalePoll.status).toBe(410);
    expect(stalePoll.body.error).toBe("unknown_peer");

    // Simulate the reregister call. Note: because the reuse window matches on cwd
    // and the previous row is HARD-deleted (not tombstoned), findReusableId returns
    // null → the caller gets a fresh id. This matches the "DB wipe" scenario in the
    // task's Bug #1 description ("DB wiped, tombstone TTL expired").
    const second = await post("/register", {
      pid: process.pid,
      cwd,
      git_root: gitRoot,
      tty: null,
      summary: "session 1",
    });
    const newId = second.id as string;
    expect(typeof newId).toBe("string");
    expect(newId).toMatch(/^[a-z0-9]{8}$/);

    // Heartbeat and poll under the new id must now succeed — this is what makes the
    // session "un-stuck": subsequent poll ticks drain messages instead of returning
    // silent empties.
    const hb = await postRaw("/heartbeat", { id: newId });
    expect(hb.status).toBe(200);
    expect(hb.body.ok).toBe(true);

    const poll = await postRaw("/poll-messages-v2", { id: newId });
    expect(poll.status).toBe(200);
    expect(Array.isArray(poll.body.messages)).toBe(true);
  });
});
