// test/soak/accounts: an active TypeScript verifier for the V2 runtime soak test.
//
// It unions the three event logs (alpha/beta/gamma) on /id and, per account,
// reconstructs {set, balance, seq} by reducing events in the order the runtime
// delivers them. Each event carries an `oracle` — the connector's authoritative
// post-event truth — so any lost, duplicated, reordered, or torn document surfaces
// as a localized contradiction. The connector routes each id to a single collection
// (id % N): Flow guarantees strict ordering within a collection but not across them,
// so an id's events must stay in one log to reduce in seq order. Three probes:
//
//   1. Union + in-order reduction. The `set` op is associative but NOT
//      commutative, so a correct reconstruction proves the runtime delivered an
//      id's events in seq order (Flow's within-collection key+clock ordering).
//   2. Expected-vs-oracle (stateful). The per-account accumulator is persisted
//      in connector state and resumed on Open, so the seq-contiguity check spans
//      session boundaries — the cross-session exactly-once probe.
//   3. Conservation (stateless, per-txn). Each shard sums balanceDelta over the
//      events it processed THIS transaction; a two-round Flush scatter/gather
//      sums every shard's per-txn delta and asserts the global is exactly zero.
//      This holds every txn because V2 keeps both legs of a transfer in one txn.
//
// Violations are published (and logged), never thrown: a soak test should keep
// running and accumulate evidence.

import {
  Document,
  FlushResponse,
  IDerivation,
  SourceFromAlpha,
  SourceFromBeta,
  SourceFromGamma,
} from "flow/test/soak/accounts.ts";

// The 8 set members, in the canonical order the connector emits them.
const MEMBERS = ["a", "b", "c", "d", "e", "f", "g", "h"];

// Structural view of an event doc — a supertype of the three (identical) Source
// types. `set`/`balanceDelta` are optional because events.schema.json marks only
// id/seq/ts/oracle required; the connector always emits them in practice.
type EventDoc = {
  id: number;
  seq: number;
  set?: { add?: string[]; remove?: string[]; intersect?: string[] };
  balanceDelta?: number;
  oracle: { seq: number; set: string[]; balance: number };
};

// In-memory reconstruction of one account.
type Account = {
  seq: number; // last applied seq, or -1 if no event has been applied yet
  set: Set<string>; // running membership
  balance: number; // running cumulative balance
};

// Persisted (JSON) form of an account inside connector state.
type StoredAccount = { seq: number; set: string[]; balance: number };

type Mismatch = {
  kind: ("seq" | "set" | "balance")[];
  expectedSeq?: number;
  gotSeq?: number;
  seqClass?: "gap" | "dupOrReorder";
  expectedSet?: string[];
  gotSet?: string[];
  expectedBalance?: number;
  gotBalance?: number;
};

// One Flush state patch: { keyBegin: delta } namespaced by the shard's keyBegin.
type StatePatch = {
  accounts?: { [id: string]: StoredAccount | null };
  deltas?: { [keyBegin: string]: number };
};

function sortMembers(s: Set<string>): string[] {
  return [...s].sort((a, b) => MEMBERS.indexOf(a) - MEMBERS.indexOf(b));
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

export class Derivation extends IDerivation {
  // Full union of all ids across all shards (the leader broadcasts the whole
  // task-level state on Open). NOT pruned: ids this shard doesn't own are inert
  // (never read, never re-persisted) and keep the map split-safe.
  private accounts: Map<number, Account>;
  // Ids mutated since the last flush; published and persisted at flush.
  private touched: Set<number>;
  // This shard's per-transaction signed balance delta (conservation probe).
  private delta: number;
  // This shard's keyBegin: a stable per-shard label for `deltas` and sentinel ids.
  private keyBegin: number;
  // Last event's verdict per touched id, so the coalesced published doc carries
  // ok/mismatch and the matching oracle.
  private lastMismatch: Map<number, Mismatch>;
  private lastOracle: Map<number, EventDoc["oracle"]>;

  constructor(
    open: {
      state: unknown;
      range?: { keyBegin?: number; keyEnd?: number };
    },
  ) {
    super(open);

    this.accounts = new Map();
    const state = (open.state ?? {}) as { accounts?: { [id: string]: StoredAccount } };
    for (const [idStr, acct] of Object.entries(state.accounts ?? {})) {
      if (acct == null) continue;
      this.accounts.set(Number(idStr), {
        seq: acct.seq,
        set: new Set(acct.set ?? []),
        balance: acct.balance ?? 0,
      });
    }

    // keyBegin labels this shard's `deltas` contribution and its sentinel id. It
    // is omitted from the Open range when zero (shard 0 / single-shard), hence `?? 0`.
    this.keyBegin = open.range?.keyBegin ?? 0;
    this.touched = new Set();
    this.delta = 0;
    this.lastMismatch = new Map();
    this.lastOracle = new Map();
  }

  fromAlpha(read: { doc: SourceFromAlpha }): Document[] {
    return this.ingest(read.doc);
  }
  fromBeta(read: { doc: SourceFromBeta }): Document[] {
    return this.ingest(read.doc);
  }
  fromGamma(read: { doc: SourceFromGamma }): Document[] {
    return this.ingest(read.doc);
  }

  // Replay one event into its account: check seq contiguity, apply the set op
  // and balance delta, and compare the result to the event's oracle. Publication
  // is deferred to flush() so multiple events for one id within a txn coalesce.
  private ingest(e: EventDoc): Document[] {
    let acct = this.accounts.get(e.id);
    if (acct === undefined) {
      acct = { seq: -1, set: new Set(), balance: 0 };
      this.accounts.set(e.id, acct);
    }

    // seq-contiguity (probe #2): a fresh account expects seq 0; an existing one
    // expects prevSeq+1. This — not oracle.seq (always == event.seq by
    // construction) — is the real exactly-once/ordering probe. A missing *tail*
    // event is liveness, not a violation: we simply never advance past it and
    // flag nothing. An *interior* gap is a violation: the next event arrives with
    // a seq above expected and trips the `gap` branch here.
    const expectedSeq = acct.seq + 1;
    const seqClass: "gap" | "dupOrReorder" | undefined = e.seq === expectedSeq
      ? undefined
      : e.seq > expectedSeq
      ? "gap"
      : "dupOrReorder";

    // Apply unconditionally, even on a contiguity failure: this derivation is a
    // probe, not a repair tool. A re-applied dup or a skipped-over gap drifts the
    // reconstruction away from the oracle, which is exactly the signal we want.
    this.applyOp(acct.set, e.set);
    const d = e.balanceDelta ?? 0;
    acct.balance += d;
    acct.seq = e.seq;

    // oracle comparison (probe #1): reduced state must equal the connector's
    // post-event truth.
    const mismatch = this.compareToOracle(e, acct, seqClass, expectedSeq);
    if (mismatch === undefined) this.lastMismatch.delete(e.id);
    else this.lastMismatch.set(e.id, mismatch);
    this.lastOracle.set(e.id, e.oracle);

    this.touched.add(e.id);
    this.delta += d;
    return [];
  }

  // add = union, remove = difference, intersect = intersection — applied over the
  // op's OPERAND member array (not the result), mutating the membership in place.
  private applyOp(set: Set<string>, op: EventDoc["set"]): void {
    if (!op) return;
    if (op.add !== undefined) {
      for (const m of op.add) set.add(m);
    } else if (op.remove !== undefined) {
      for (const m of op.remove) set.delete(m);
    } else if (op.intersect !== undefined) {
      const keep = new Set(op.intersect);
      for (const m of [...set]) if (!keep.has(m)) set.delete(m);
    }
  }

  private compareToOracle(
    e: EventDoc,
    acct: Account,
    seqClass: "gap" | "dupOrReorder" | undefined,
    expectedSeq: number,
  ): Mismatch | undefined {
    const kind: ("seq" | "set" | "balance")[] = [];
    const m: Mismatch = { kind };

    if (seqClass !== undefined || e.oracle.seq !== e.seq) {
      kind.push("seq");
      m.expectedSeq = expectedSeq;
      m.gotSeq = e.seq;
      m.seqClass = seqClass;
    }
    const got = sortMembers(acct.set);
    if (!arraysEqual(got, e.oracle.set)) {
      kind.push("set");
      m.expectedSet = e.oracle.set;
      m.gotSet = got;
    }
    if (acct.balance !== e.oracle.balance) {
      kind.push("balance");
      m.expectedBalance = e.oracle.balance;
      m.gotBalance = acct.balance;
    }
    return kind.length > 0 ? m : undefined;
  }

  override async flush(statePatches?: unknown[]): Promise<FlushResponse> {
    // Round 2: statePatches is the concatenated array of every shard's round-1
    // `state.updated` (including this shard's own). Gather and assert.
    if (statePatches && statePatches.length > 0) {
      return this.gatherConservation(statePatches as StatePatch[]);
    }
    // Round 1: publish touched accounts and contribute this shard's per-txn delta.
    return this.publishAccounts();
  }

  private publishAccounts(): FlushResponse {
    const published: Document[] = [];
    const accountsPatch: { [id: string]: StoredAccount } = {};

    for (const id of this.touched) {
      const acct = this.accounts.get(id)!;
      const set = sortMembers(acct.set);
      const mismatch = this.lastMismatch.get(id);

      const doc = {
        id,
        seq: acct.seq,
        set,
        balance: acct.balance,
        oracle: this.lastOracle.get(id),
        ok: mismatch === undefined,
        ...(mismatch !== undefined ? { mismatch } : {}),
      };
      published.push(doc as Document);
      accountsPatch[String(id)] = { seq: acct.seq, set, balance: acct.balance };
    }

    this.lastMismatch.clear();
    this.lastOracle.clear();

    // Emit state in round 1 only: every iteration's state is persisted, so
    // contributing the accounts patch again in round 2 would merge it twice.
    // `deltas` is cruft here (ignored on Open); it exists so peers can gather it.
    return {
      published,
      state: {
        updated: {
          accounts: accountsPatch,
          deltas: { [String(this.keyBegin)]: this.delta },
        },
        mergePatch: true,
      },
      more: true,
    };
  }

  private gatherConservation(patches: StatePatch[]): FlushResponse {
    // Sum every shard's per-txn delta. Each shard contributes exactly one entry
    // keyed by its unique keyBegin, so summing all `deltas` values across the
    // concatenated array counts each shard once (no double-count). Every shard
    // sees the same array and computes the same `global`.
    let global = 0;
    const shardKeys: string[] = [];
    for (const p of patches) {
      for (const [k, v] of Object.entries(p?.deltas ?? {})) {
        shardKeys.push(k);
        global += v ?? 0;
      }
    }

    const published: Document[] = [];
    if (global !== 0) {
      // Per-shard sentinel id, always < 0 so it never collides with a real id
      // (>= 0) or with another shard's sentinel (distinct keyBegin).
      const sentinelId = -(this.keyBegin + 1);
      published.push({
        id: sentinelId,
        violation: { sum: global, keyBegin: this.keyBegin, shardKeys },
      } as Document);
      console.error(JSON.stringify({
        level: "ERROR",
        msg: "conservation violated: global per-transaction balance delta is non-zero",
        fields: { source: "accounts-derivation", sum: global, keyBegin: this.keyBegin },
      }));
    }

    this.touched.clear();
    this.delta = 0;
    return { published, more: false };
  }

  override async reset() {
    this.accounts.clear();
    this.touched.clear();
    this.delta = 0;
    this.lastMismatch.clear();
    this.lastOracle.clear();
  }
}
