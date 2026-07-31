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
