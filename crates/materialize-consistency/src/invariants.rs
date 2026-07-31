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
}

impl Expectation {
    /// Fold the collection's documents into a per-account expectation.
    pub fn from_documents(documents: impl IntoIterator<Item = Event>) -> Self {
        let mut accounts: BTreeMap<i64, Account> = BTreeMap::new();

        for event in documents {
            let account = accounts.entry(event.id).or_default();

            // A collection keyed [/id, /seq] holds each (id, seq) once, but the
            // read is of raw journal content and may surface a document twice if
            // the capture itself re-emitted it. Counting a sequence once keeps
            // the expectation a set, matching what a correct materialization
            // delivers.
            if account.seqs.insert(event.seq) {
                account.total_delta += event.balance_delta;
            }
            if event.seq >= account.max_seq || account.by_seq.is_empty() {
                account.max_seq = event.seq;
                account.final_oracle = event.oracle.clone();
            }
            account.by_seq.insert(event.seq, event);
        }

        Self { accounts }
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
                detail: format!("the standard binding holds account {id}, which the collection does not"),
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

        match last_seq.get(&row.id) {
            Some(previous) if row.seq <= *previous => out.push(Violation {
                invariant: Invariant::Monotonicity,
                detail: format!(
                    "account {}: the log binding delivered seq {} after {previous}",
                    row.id, row.seq
                ),
            }),
            _ => {
                last_seq.insert(row.id, row.seq);
            }
        }

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

    for row in merged_delta {
        latest.insert(row.id, row);
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
}
