//! What the destination must hold, and how we know.
//!
//! Every check here is an externally observable property of the destination,
//! expressed in the workload's own vocabulary — conservation, oracle agreement,
//! cardinality, monotonicity. None of them inspects connector internals: not
//! staged file names, not checkpoint contents, not the shape of a connector state
//! document. Those legitimately differ across the four connector classes, so a
//! test that asserted on them would pass or fail for reasons unrelated to
//! consistency and would obstruct the very refactors it should protect.
//!
//! The correct answer is *computed*, not recorded. Each workload document carries
//! an `oracle`: the producer's authoritative truth for that account after that
//! event. So the expectation is derived from the collection's own contents,
//! which is why there are no snapshots here despite the repository's convention —
//! a snapshot would add a stale artifact without adding information.
//!
//! ## Row order is not given
//!
//! Rows reach these checks in the order the destination returned them, which for a table
//! read with `SELECT *` is no order at all — only the reference connector's own tables
//! replay the order rows were appended in.
//!
//! So any check whose meaning depends on order must establish that order itself, from
//! `seq`. Getting this wrong reports violations against a correct connector, which is the
//! worst thing this suite can do.
//!
//! The monotonicity checks are the deliberate exceptions — `check_merged_delta`'s and
//! `check_log`'s — because they are *about* arrival order rather than merely computed in it.
//! Both are exempted for subjects whose destination cannot preserve it.

use anyhow::Context;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// One workload document, in the fields the invariants rest on. See
/// `tests/soak/capture/events.schema.json` for the full wire contract.
#[derive(Deserialize, serde::Serialize, Clone, Debug, PartialEq)]
pub struct Event {
    pub id: i64,
    pub seq: i64,
    /// One leg of a matched transfer pair. Absent on a document that moved no
    /// money, and summed by the reduction of the merged collection.
    #[serde(default, rename = "balanceDelta")]
    pub balance_delta: i64,
    pub oracle: Oracle,
}

impl Event {
    /// Parse one row as a destination returned it.
    ///
    /// Two shapes arrive here and both are legitimate. A connector that stores documents
    /// whole — the reference one — yields the collection document itself. A SQL
    /// destination yields *columns*, and Flow names each column by the JSON pointer of
    /// the field it projects, so the nested `oracle` object arrives flattened as
    /// `oracle/seq` and `oracle/balance`. A materialized table is columns, and a standard
    /// binding need not carry a root document at all (see the connectors'
    /// `no_flow_document` option), so there is nothing to parse whole.
    ///
    /// Pointer-named columns are therefore folded back into the object they came from.
    /// Unrelated columns — `flow_published_at`, the workload's `set/` and `transfer/`
    /// fields — fold harmlessly and are then ignored, because nothing here is required to
    /// understand every projection a connector chose to materialize.
    pub fn from_row(row: &str) -> anyhow::Result<Self> {
        let value: serde_json::Value = serde_json::from_str(row).context("row is not JSON")?;

        let Some(columns) = value.as_object() else {
            anyhow::bail!("row is not a JSON object");
        };

        let mut document = serde_json::Map::new();
        for (name, value) in columns {
            insert_pointer(&mut document, name, value.clone());
        }

        // A nested field may also arrive as one column holding JSON text, and both shapes
        // occur in the *same run*: field selection is per binding, so this workload's delta
        // binding materialized `oracle/seq` and `oracle/balance` as separate columns while
        // its merge binding materialized `oracle` whole. A reader has to accept either.
        if let Some(text) = document.get("oracle").and_then(|o| o.as_str()) {
            let parsed: serde_json::Value = serde_json::from_str(text)
                .context("the oracle column is neither an object nor JSON text")?;
            document.insert("oracle".to_string(), parsed);
        }

        serde_json::from_value(serde_json::Value::Object(document))
            .context("row does not describe an event")
    }
}

/// Place `value` at the `/`-delimited `path` within `object`, creating objects as needed.
///
/// A conflict — a path descending through a value that is already a scalar — leaves the
/// scalar alone rather than discarding it. That only happens when a destination has both a
/// column `x` and a column `x/y`, which no projection produces, and silently dropping
/// data would be the worse failure of the two.
fn insert_pointer(
    object: &mut serde_json::Map<String, serde_json::Value>,
    path: &str,
    value: serde_json::Value,
) {
    let mut segments = path.split('/');
    let mut leaf = segments.next().unwrap_or(path).to_string();
    let mut cursor = object;

    for next in segments {
        let entry = cursor
            .entry(std::mem::replace(&mut leaf, next.to_string()))
            .or_insert_with(|| serde_json::Value::Object(Default::default()));

        match entry.as_object_mut() {
            Some(nested) => cursor = nested,
            None => return,
        }
    }
    cursor.insert(leaf, value);
}

#[derive(Deserialize, serde::Serialize, Clone, Debug, Default, PartialEq)]
pub struct Oracle {
    pub seq: i64,
    pub balance: i64,
}

/// The property a violation belongs to. Exemptions are expressed against these,
/// so a connector making a weaker guarantee still gets held to the rest.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Invariant {
    /// Balances sum to exactly zero, because every transfer is a matched pair.
    Conservation,
    /// The materialized reduction equals the oracle carried by the
    /// highest-sequence document delivered for that key.
    OracleAgreement,
    /// Every document the collection holds reached the destination.
    NoLoss,
    /// No document reached the destination twice.
    NoDuplicates,
    /// A key's sequence only ever advances at the sink.
    Monotonicity,
    /// The latest delta row per key reconstructs the standard row.
    StandardDeltaAgreement,
}

impl std::fmt::Display for Invariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Conservation => "conservation",
            Self::OracleAgreement => "oracle-agreement",
            Self::NoLoss => "no-loss",
            Self::NoDuplicates => "no-duplicates",
            Self::Monotonicity => "monotonicity",
            Self::StandardDeltaAgreement => "standard-delta-agreement",
        };
        f.write_str(name)
    }
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub invariant: Invariant,
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.invariant, self.detail)
    }
}

/// What the collection holds, per account. Built by reading the collection
/// itself, so it is an expectation the connector under test had no hand in.
#[derive(Clone, Debug, Default)]
pub struct Account {
    pub seqs: BTreeSet<i64>,
    pub total_delta: i64,
    pub max_seq: i64,
    /// Oracle of the highest-sequence document, i.e. the account's final truth.
    pub final_oracle: Oracle,
    /// Per-sequence document, so a delivered row can be compared against the one
    /// the collection actually holds.
    pub by_seq: BTreeMap<i64, Event>,
}

#[derive(Clone, Debug, Default)]
pub struct Expectation {
    pub accounts: BTreeMap<i64, Account>,
    /// Documents whose `(id, seq)` the read surfaced more than once.
    ///
    /// Recorded because the expectation folds them to one while a materialization
    /// does not: an append binding writes a row per document it is given, and a
    /// merge binding reduces each into the target key. So a non-zero count here
    /// means the two sides are measuring different things, and a comparison
    /// against a reducing binding is not sound until it is explained.
    pub duplicated_documents: usize,
}

impl Expectation {
    /// Fold the collection's documents into a per-account expectation.
    pub fn from_documents(documents: impl IntoIterator<Item = Event>) -> Self {
        let mut accounts: BTreeMap<i64, Account> = BTreeMap::new();
        let mut duplicated_documents = 0;

        for event in documents {
            let account = accounts.entry(event.id).or_default();

            // A collection keyed [/id, /seq] holds each (id, seq) once, but the
            // read is of raw journal content and may surface a document twice if
            // the capture itself re-emitted it. Counting a sequence once keeps
            // the expectation a set, matching what a correct materialization
            // delivers.
            if account.seqs.insert(event.seq) {
                account.total_delta += event.balance_delta;
            } else {
                duplicated_documents += 1;
            }
            if event.seq >= account.max_seq || account.by_seq.is_empty() {
                account.max_seq = event.seq;
                account.final_oracle = event.oracle.clone();
            }
            account.by_seq.insert(event.seq, event);
        }

        Self {
            accounts,
            duplicated_documents,
        }
    }

    pub fn documents(&self) -> usize {
        self.accounts.values().map(|a| a.seqs.len()).sum()
    }
}

/// What each binding of the materialization under test should hold, and what it
/// does hold.
///
/// The two collections have separate expectations because they have separate
/// producers: a capture routes each document to one binding, so one capture with
/// two bindings would partition accounts between the collections rather than
/// writing both. See `harness::catalog`.
pub struct Bindings {
    /// Expectation for the `[/id]`-keyed, sum-reduced collection.
    pub merged_expected: Expectation,
    /// Expectation for the `[/id, /seq]`-keyed append-only collection.
    pub log_expected: Expectation,
    /// The merged collection materialized with standard (merge) semantics: one row
    /// per account — or `None` when the scenario's subject cannot take a standard
    /// binding at all, as the document-counter class cannot.
    pub standard: Option<Vec<Event>>,
    /// The merged collection materialized with delta-updates: one row per account
    /// per transaction, in delivery order.
    pub merged_delta: Vec<Event>,
    /// The append-only collection materialized with delta-updates: one row per
    /// document, in delivery order.
    pub log: Vec<Event>,
}

/// Check every invariant, returning what failed.
///
/// Checks run against a quiescent destination. That is not a convenience: some
/// legitimate patterns make rows visible before the Flow transaction commits —
/// the document-counter class appends during `Store` — so a mid-flight read would
/// report a violation where none exists.
pub fn check(b: &Bindings) -> Vec<Violation> {
    let mut violations = Vec::new();

    check_workload(&b.merged_expected, &mut violations);
    check_workload(&b.log_expected, &mut violations);
    check_merged_delta(&b.merged_expected, &b.merged_delta, &mut violations);
    check_log(&b.log_expected, &b.log, &mut violations);

    // A subject with no standard binding is not thereby excused anything: the two
    // delta bindings still carry per-document cardinality, the running-sum-against-
    // oracle check and monotonicity, which are the sharpest checks in the suite.
    // Only the two that need a reduced row are skipped.
    if let Some(standard) = &b.standard {
        check_standard(&b.merged_expected, standard, &mut violations);
        check_standard_delta_agreement(standard, &b.merged_delta, &mut violations);
    }

    violations
}

/// The expectation itself must conserve. This is the baseline guard: a wiring
/// problem that made the harness read an empty or torn collection shows up here
/// as a failure rather than as a vacuous pass everywhere else.
fn check_workload(expected: &Expectation, out: &mut Vec<Violation>) {
    if expected.accounts.is_empty() {
        out.push(Violation {
            invariant: Invariant::NoLoss,
            detail: "the collection held no documents, so nothing was verified".to_string(),
        });
        return;
    }

    let sum: i64 = expected.accounts.values().map(|a| a.total_delta).sum();
    if sum != 0 {
        out.push(Violation {
            invariant: Invariant::Conservation,
            detail: format!(
                "the collection itself does not conserve: Σ balanceDelta = {sum} over {} accounts. \
                 The workload or the read is at fault, not the connector.",
                expected.accounts.len()
            ),
        });
    }
}

fn check_standard(expected: &Expectation, rows: &[Event], out: &mut Vec<Violation>) {
    let by_id: BTreeMap<i64, &Event> = rows.iter().map(|r| (r.id, r)).collect();

    if by_id.len() != rows.len() {
        out.push(Violation {
            invariant: Invariant::NoDuplicates,
            detail: format!(
                "the standard binding holds {} rows for {} distinct keys",
                rows.len(),
                by_id.len()
            ),
        });
    }

    for (id, account) in &expected.accounts {
        let Some(row) = by_id.get(id) else {
            out.push(Violation {
                invariant: Invariant::NoLoss,
                detail: format!("account {id} is absent from the standard binding"),
            });
            continue;
        };

        // The reduced sum of deltas *is* the account's balance, and the oracle says
        // what that balance should be, so any disagreement means the account's
        // documents were not applied exactly once.
        //
        // Which way it went is deliberately not inferred from the sign: a balance
        // is signed, so a duplicated debit leaves the sum *below* the expectation
        // and a duplicated credit above it. The counting checks over the
        // append-only bindings settle duplicate-versus-loss unambiguously; this one
        // reports that the arithmetic does not hold.
        if row.balance_delta != account.total_delta {
            out.push(Violation {
                invariant: Invariant::OracleAgreement,
                detail: format!(
                    "account {id}: reduced balance {} but the collection sums to {}",
                    row.balance_delta, account.total_delta
                ),
            });
        }
        if row.balance_delta != row.oracle.balance {
            out.push(Violation {
                invariant: Invariant::OracleAgreement,
                detail: format!(
                    "account {id}: reduced balance {} disagrees with its own oracle {}",
                    row.balance_delta, row.oracle.balance
                ),
            });
        }
        if row.seq != account.max_seq {
            out.push(Violation {
                invariant: Invariant::OracleAgreement,
                detail: format!(
                    "account {id}: reduced seq {} but the collection's latest is {}",
                    row.seq, account.max_seq
                ),
            });
        }
    }

    for id in by_id.keys() {
        if !expected.accounts.contains_key(id) {
            out.push(Violation {
                invariant: Invariant::NoDuplicates,
                detail: format!(
                    "the standard binding holds account {id}, which the collection does not"
                ),
            });
        }
    }

    let sum: i64 = rows.iter().map(|r| r.balance_delta).sum();
    if sum != 0 {
        out.push(Violation {
            invariant: Invariant::Conservation,
            detail: format!("Σ balance over the standard binding is {sum}, not zero"),
        });
    }
}

/// The append-only collection materialized as deltas: the sharpest detector in
/// the suite, because every document has a distinct key and so a duplicate
/// delivery is an extra row rather than an invisible re-reduction.
fn check_log(expected: &Expectation, rows: &[Event], out: &mut Vec<Violation>) {
    let mut seen: BTreeMap<(i64, i64), usize> = BTreeMap::new();
    let mut last_seq: BTreeMap<i64, i64> = BTreeMap::new();

    for row in rows {
        *seen.entry((row.id, row.seq)).or_default() += 1;

        // Strictly less-than, and the baseline always advances to what was just delivered.
        // Both match `check_merged_delta`, and the two must agree because a run's suppression
        // counts are read as evidence about which exemptions are load-bearing — two checkers
        // counting the same event differently makes that evidence incomparable.
        //
        // `<` rather than `<=` because a *repeated* seq is not a regression of order; it is a
        // duplicate, which `NoDuplicates` below owns. Counting it here too reported one fault
        // as two, under two different invariants, one of which is often exempt.
        //
        // And tracking the last delivered seq rather than a high-water mark means a replay is
        // one violation rather than one per row it replays: after 1..10 then 8, 9, 10, the 8 is
        // the regression and the 9 and 10 that follow it are in order.
        if let Some(previous) = last_seq.get(&row.id) {
            if row.seq < *previous {
                out.push(Violation {
                    invariant: Invariant::Monotonicity,
                    detail: format!(
                        "account {}: the log binding delivered seq {} after {previous}",
                        row.id, row.seq
                    ),
                });
            }
        }
        last_seq.insert(row.id, row.seq);

        // A faithfully transported document is still itself.
        if let Some(document) = expected
            .accounts
            .get(&row.id)
            .and_then(|a| a.by_seq.get(&row.seq))
        {
            if document.oracle != row.oracle || document.balance_delta != row.balance_delta {
                out.push(Violation {
                    invariant: Invariant::OracleAgreement,
                    detail: format!(
                        "account {} seq {}: the log binding holds {:?} but the collection holds {:?}",
                        row.id, row.seq, row, document
                    ),
                });
            }
        }
    }

    for (id, account) in &expected.accounts {
        for seq in &account.seqs {
            match seen.remove(&(*id, *seq)) {
                None => out.push(Violation {
                    invariant: Invariant::NoLoss,
                    detail: format!("account {id} seq {seq} never reached the log binding"),
                }),
                Some(1) => {}
                Some(n) => out.push(Violation {
                    invariant: Invariant::NoDuplicates,
                    detail: format!("account {id} seq {seq} reached the log binding {n} times"),
                }),
            }
        }
    }

    for ((id, seq), n) in seen {
        out.push(Violation {
            invariant: Invariant::NoDuplicates,
            detail: format!(
                "the log binding holds account {id} seq {seq} ({n}×), which the collection does not"
            ),
        });
    }
}

/// The merged collection materialized as deltas: one row per account per
/// transaction, each carrying that transaction's reduction.
///
/// The running sum of those rows must equal the oracle at every step. This is
/// where a duplicated *transaction* shows up — the same reduced document applied
/// twice leaves the running sum ahead of the oracle from that row onward.
fn check_merged_delta(expected: &Expectation, rows: &[Event], out: &mut Vec<Violation>) {
    let mut running: BTreeMap<i64, i64> = BTreeMap::new();
    let mut last_seq: BTreeMap<i64, i64> = BTreeMap::new();
    let mut reported: BTreeSet<i64> = BTreeSet::new();
    let mut seen: BTreeMap<(i64, i64), usize> = BTreeMap::new();

    // The running sum is arithmetic over a key's history, so it has to be accumulated in
    // sequence order — not in whatever order the destination handed the rows back. A
    // SQL destination has no reason to return them ordered: `SELECT *` on a delta table
    // gave `[10, 12, 4, 9, 26, ...]`, which made the first row's own delta look like the
    // whole history and reported a violation against a connector that was exactly right.
    //
    // Monotonicity below is the opposite: it is *about* arrival order, and reads
    // `delivery` rather than this. A destination that cannot preserve delivery order
    // cannot be held to it at all — see the exemption the harness adds for such subjects.
    let delivery = rows;
    let mut rows: Vec<&Event> = rows.iter().collect();
    rows.sort_by_key(|row| (row.id, row.seq));

    for row in rows {
        // Two rows for one (account, seq) is a transaction applied twice: each row
        // is one transaction's reduction, and a later transaction with more events
        // for the account necessarily carries a higher sequence. This is the
        // unambiguous duplicate signal for the merged collection, where the summed
        // arithmetic can only say that something is wrong.
        *seen.entry((row.id, row.seq)).or_default() += 1;

        let running = running.entry(row.id).or_default();
        *running += row.balance_delta;

        // One report per account: a divergence persists across every later row of
        // that account, and repeating it says nothing new.
        if *running != row.oracle.balance && reported.insert(row.id) {
            out.push(Violation {
                invariant: Invariant::OracleAgreement,
                detail: format!(
                    "account {}: delta rows accumulate to {running} at seq {}, but its oracle says {}",
                    row.id, row.seq, row.oracle.balance
                ),
            });
        }
    }

    for row in delivery {
        if let Some(previous) = last_seq.get(&row.id) {
            if row.seq < *previous {
                out.push(Violation {
                    invariant: Invariant::Monotonicity,
                    detail: format!(
                        "account {}: the delta binding delivered seq {} after {previous}",
                        row.id, row.seq
                    ),
                });
            }
        }
        last_seq.insert(row.id, row.seq);
    }

    for ((id, seq), n) in &seen {
        if *n > 1 {
            out.push(Violation {
                invariant: Invariant::NoDuplicates,
                detail: format!(
                    "account {id}: the delta binding holds {n} rows for seq {seq}, so that \
                     transaction was applied more than once"
                ),
            });
        }
    }

    for (id, account) in &expected.accounts {
        match running.get(id) {
            None => out.push(Violation {
                invariant: Invariant::NoLoss,
                detail: format!("account {id} is absent from the delta binding"),
            }),
            Some(total) if *total != account.total_delta => out.push(Violation {
                invariant: Invariant::OracleAgreement,
                detail: format!(
                    "account {id}: delta rows sum to {total} but the collection sums to {}",
                    account.total_delta
                ),
            }),
            Some(_) => {}
        }
    }
}

/// The two views of one collection must agree: reducing the delta history down to
/// its latest row per key reconstructs the standard row.
fn check_standard_delta_agreement(
    standard: &[Event],
    merged_delta: &[Event],
    out: &mut Vec<Violation>,
) {
    let mut latest: BTreeMap<i64, &Event> = BTreeMap::new();
    let mut totals: BTreeMap<i64, i64> = BTreeMap::new();

    // Latest by *sequence*, not by position: rows arrive in whatever order the destination
    // returned them, and `SELECT *` on a table has no order at all. Taking the last one
    // seen picked an arbitrary row and reported every account as disagreeing.
    for row in merged_delta {
        match latest.get(&row.id) {
            Some(existing) if existing.seq >= row.seq => {}
            _ => {
                latest.insert(row.id, row);
            }
        }
        *totals.entry(row.id).or_default() += row.balance_delta;
    }

    for row in standard {
        let Some(delta) = latest.get(&row.id) else {
            continue; // Absence is reported by the per-binding checks.
        };

        if row.seq != delta.seq || row.oracle != delta.oracle {
            out.push(Violation {
                invariant: Invariant::StandardDeltaAgreement,
                detail: format!(
                    "account {}: standard holds seq {} oracle {:?}, latest delta holds seq {} oracle {:?}",
                    row.id, row.seq, row.oracle, delta.seq, delta.oracle
                ),
            });
        }
        if Some(&row.balance_delta) != totals.get(&row.id) {
            out.push(Violation {
                invariant: Invariant::StandardDeltaAgreement,
                detail: format!(
                    "account {}: standard balance {} but delta rows sum to {:?}",
                    row.id,
                    row.balance_delta,
                    totals.get(&row.id)
                ),
            });
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn event(id: i64, seq: i64, delta: i64, balance: i64) -> Event {
        Event {
            id,
            seq,
            balance_delta: delta,
            oracle: Oracle { seq, balance },
        }
    }

    /// A matched pair of transfers, delivered once each to every binding.
    fn fixture() -> Bindings {
        let documents = vec![
            event(1, 0, -5, -5),
            event(1, 1, -3, -8),
            event(2, 0, 5, 5),
            event(2, 1, 3, 8),
        ];

        Bindings {
            merged_expected: Expectation::from_documents(documents.clone()),
            log_expected: Expectation::from_documents(documents.clone()),
            standard: Some(vec![event(1, 1, -8, -8), event(2, 1, 8, 8)]),
            merged_delta: documents.clone(),
            log: documents,
        }
    }

    fn kinds(bindings: &Bindings) -> Vec<Invariant> {
        check(bindings).iter().map(|v| v.invariant).collect()
    }

    #[test]
    fn clean_delivery_has_no_violations() {
        let violations = check(&fixture());
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn a_duplicated_log_row_is_a_duplicate_not_a_loss() {
        let mut bindings = fixture();
        bindings.log.push(event(1, 1, -3, -8));

        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::NoDuplicates), "{kinds:?}");
        assert!(!kinds.contains(&Invariant::NoLoss), "{kinds:?}");
    }

    #[test]
    fn a_missing_log_row_is_a_loss() {
        let mut bindings = fixture();
        bindings.log.retain(|e| !(e.id == 1 && e.seq == 1));

        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::NoLoss), "{kinds:?}");
        assert!(!kinds.contains(&Invariant::NoDuplicates), "{kinds:?}");
    }

    /// The case the whole suite exists for: a transaction applied twice. Under
    /// last-write-wins alone this is invisible, which is why the workload pairs a
    /// summed counter with an append-only binding.
    ///
    /// Note the duplicated debit leaves the reduced balance *below* the
    /// expectation, which is why duplicate-versus-loss is settled by counting rows
    /// rather than by comparing sums.
    #[test]
    fn a_replayed_transaction_shows_in_the_summed_binding() {
        let mut bindings = fixture();
        bindings.standard = Some(vec![event(1, 1, -11, -8), event(2, 1, 8, 8)]);
        bindings.merged_delta.insert(2, event(1, 1, -3, -8));

        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::NoDuplicates), "{kinds:?}");
        assert!(kinds.contains(&Invariant::OracleAgreement), "{kinds:?}");
        assert!(kinds.contains(&Invariant::Conservation), "{kinds:?}");
        assert!(!kinds.contains(&Invariant::NoLoss), "{kinds:?}");
    }

    /// A duplicated credit rather than a debit: the sums move the other way, and
    /// the verdict must be the same.
    #[test]
    fn duplicate_detection_does_not_depend_on_the_sign_of_the_balance() {
        let mut bindings = fixture();
        bindings.standard = Some(vec![event(1, 1, -8, -8), event(2, 1, 11, 8)]);
        bindings.merged_delta.push(event(2, 1, 3, 8));

        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::NoDuplicates), "{kinds:?}");
        assert!(!kinds.contains(&Invariant::NoLoss), "{kinds:?}");
    }

    /// A subject with no standard binding still gets the delta-binding checks, which
    /// is what makes the document-counter class — Snowpipe's shape, delta-only — worth
    /// testing at all.
    #[test]
    fn a_delta_only_subject_is_still_held_to_the_delta_checks() {
        let mut bindings = fixture();
        bindings.standard = None;
        assert!(check(&bindings).is_empty());

        bindings.log.push(event(1, 1, -3, -8));
        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::NoDuplicates), "{kinds:?}");
    }

    #[test]
    fn regression_in_delivered_sequence_breaks_monotonicity() {
        let mut bindings = fixture();
        bindings.log.swap(0, 1);

        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::Monotonicity), "{kinds:?}");
    }

    /// A tail-truncated materialization is internally consistent — every check
    /// over the destination alone still passes — which is why the expectation is
    /// read from the collection rather than derived from what arrived.
    #[test]
    fn tail_truncation_is_a_loss_only_against_the_collection() {
        let mut bindings = fixture();
        bindings.log.retain(|e| e.seq == 0);
        bindings.merged_delta.retain(|e| e.seq == 0);
        bindings.standard = Some(vec![event(1, 0, -5, -5), event(2, 0, 5, 5)]);

        let kinds = kinds(&bindings);
        assert!(kinds.contains(&Invariant::NoLoss), "{kinds:?}");
    }

    /// The two monotonicity checkers must count a regression the same way, because a run's
    /// suppression counts are read as evidence about which exemptions carry weight — and two
    /// checkers scoring the same event differently makes that evidence incomparable.
    ///
    /// They disagreed twice: `check_log` treated a repeated seq as a regression (it is a
    /// duplicate, which `NoDuplicates` owns) and held a high-water mark, so one replay of
    /// `8, 9, 10` after `10` scored three violations where the delta checker scored one.
    #[test]
    fn both_monotonicity_checkers_score_a_regression_the_same() {
        // 0, 1, 2, then a replay of 1, 2 — one regression, at the replayed 1.
        let replayed = vec![
            event(1, 0, -1, -1),
            event(1, 1, -1, -2),
            event(1, 2, -1, -3),
            event(1, 1, -1, -2),
            event(1, 2, -1, -3),
        ];

        let count = |violations: Vec<Violation>| {
            violations
                .iter()
                .filter(|v| v.invariant == Invariant::Monotonicity)
                .count()
        };

        let expected = Expectation::from_documents(replayed.clone());

        let mut log = Vec::new();
        check_log(&expected, &replayed, &mut log);

        let mut delta = Vec::new();
        check_merged_delta(&expected, &replayed, &mut delta);

        assert_eq!(count(log), 1, "the log checker scores one regression");
        assert_eq!(count(delta), 1, "and so does the delta checker");
    }
}

#[cfg(test)]
mod row_test {
    use super::*;

    /// A destination that stores documents whole, which is the reference connector and
    /// any binding materializing a root document.
    #[test]
    fn a_document_row_parses() {
        let row = r#"{"id":7,"seq":3,"balanceDelta":-25,"oracle":{"seq":3,"balance":100}}"#;
        let event = Event::from_row(row).unwrap();

        assert_eq!(event.id, 7);
        assert_eq!(event.balance_delta, -25);
        assert_eq!(
            event.oracle,
            Oracle {
                seq: 3,
                balance: 100
            }
        );
    }

    /// A real row from `materialize-databricks`, copied from a run rather than imagined.
    ///
    /// Flow names each column by the JSON pointer of the field it projects, so a nested
    /// object arrives flattened — `oracle/balance` rather than an `oracle` object. Anything
    /// asserted here has to come from a run, because that shape is not guessable.
    #[test]
    fn a_real_column_row_folds_its_pointer_named_columns() {
        let row = r#"{"balanceDelta":-28,"flow_published_at":"2026-08-03T01:44:00.319887Z",
                      "id":0,"oracle/balance":-266,"oracle/seq":22,
                      "oracle/set":"[\"a\",\"b\"]","seq":22,"set/add":"[\"a\"]",
                      "set/intersect":null,"set/remove":null,"transfer/amount":28,
                      "transfer/from":0,"transfer/to":25,"ts":"2026-08-03T01:44:00.116428Z"}"#;

        let event = Event::from_row(row).unwrap();

        assert_eq!(event.id, 0);
        assert_eq!(event.seq, 22);
        assert_eq!(event.balance_delta, -28);
        assert_eq!(
            event.oracle,
            Oracle {
                seq: 22,
                balance: -266
            },
            "oracle/seq and oracle/balance must fold back into the oracle object",
        );
    }

    /// The merge binding of the very same run, which materialized `oracle` whole rather
    /// than flattened. Both shapes must parse, which is why replacing one with the other
    /// was wrong.
    #[test]
    fn a_real_merge_binding_row_parses_its_json_text_column() {
        let row = r#"{"balanceDelta":403,"flow_published_at":"2026-08-03T01:49:50.246911Z",
                      "id":0,"oracle":"{\"balance\":403,\"seq\":42,\"set\":[\"d\",\"g\"]}",
                      "seq":42,"set/remove":"[\"a\"]","transfer/amount":68,
                      "ts":"2026-08-03T01:49:50.243781Z"}"#;

        let event = Event::from_row(row).unwrap();
        assert_eq!(event.id, 0);
        assert_eq!(event.balance_delta, 403);
        assert_eq!(
            event.oracle,
            Oracle {
                seq: 42,
                balance: 403
            },
        );
    }

    /// Extra columns are ordinary: a destination carries `flow_document`, `flow_published_at`
    /// and whatever else the connector adds, and a read must not choke on them.
    #[test]
    fn unknown_columns_are_ignored() {
        let row = r#"{"id":1,"seq":0,"oracle":{"seq":0,"balance":0},
                      "flow_document":"{}","flow_published_at":"2026-01-01T00:00:00Z"}"#;
        let event = Event::from_row(row).unwrap();

        assert_eq!(event.id, 1);
        // Absent on a document that moved no money.
        assert_eq!(event.balance_delta, 0);
    }

    #[test]
    fn a_row_that_is_not_an_event_is_refused() {
        assert!(Event::from_row("not json").is_err());
        assert!(Event::from_row(r#"{"id":1}"#).is_err());
        assert!(
            Event::from_row(r#"{"id":1,"seq":0,"oracle":"not json"}"#).is_err(),
            "an oracle column holding neither an object nor JSON must be reported",
        );
    }
}
