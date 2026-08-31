//! What the destination must hold, and how we know.
//!
//! Every check here is an externally observable property of the destination,
//! expressed in the workload's own vocabulary — conservation, oracle agreement,
//! cardinality, monotonicity. None of them inspects connector internals; internals
//! differ across the four connector classes.
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
//! read with `SELECT *` is no order at all, so any check whose meaning depends on order
//! must establish that order itself, from `seq`.
//!
//! The monotonicity checks (`check_merged_delta` and `check_log`) are exceptions to the
//! ordering rule because they are *about* arrival order. Both are exempted for a connector
//! under test whose delivery order the harness cannot recover from the destination.

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
    ///
    /// Aliased because [`Event::from_row`] folds column names to lower case, so the collection's
    /// own `balanceDelta` and a destination's `BALANCEDELTA` both arrive as `balancedelta`.
    #[serde(default, rename = "balanceDelta", alias = "balancedelta")]
    pub balance_delta: i64,
    pub oracle: Oracle,
}

impl Event {
    /// Parse one row as a destination returned it.
    ///
    /// Two shapes arrive here, and both are legitimate:
    ///
    /// 1. **A whole document**, from a connector that stores documents as they are — the
    ///    reference one. The nested `oracle` is a nested object:
    ///    `{"id":7,"seq":3,"balanceDelta":-25,"oracle":{"seq":3,"balance":100}}`
    /// 2. **Columns**, from a SQL destination. Flow names each column by the JSON pointer of
    ///    the field it projects, so the same `oracle` arrives flattened into two columns:
    ///    `{"id":7,"seq":3,"balanceDelta":-25,"oracle/seq":3,"oracle/balance":100}`
    ///
    /// Shape 2 is not a fallback for shape 1: a materialized table *is* columns, and a
    /// standard binding need not carry a root document at all (see the connectors'
    /// `no_flow_document` option), so often there is nothing to parse whole.
    ///
    /// Both shapes are therefore reduced to shape 1 — pointer-named columns are folded back
    /// into the object they came from — and parsed once.
    ///
    /// A row carries more columns than the four fields [`Event`] declares. The connector adds its
    /// own — `flow_published_at`, `flow_document` — and the workload writes fields no
    /// invariant reads, such as `set/add` and `transfer/amount`. All of them fold harmlessly
    /// and are then ignored, because nothing here is required to understand every projection
    /// a connector chose to materialize.
    ///
    /// **Names are folded to lower case, because a destination's identifier casing is its own.**
    /// Snowflake upper-cases an unquoted identifier, so the same document arrives as `ID`, `SEQ`
    /// and `BALANCEDELTA` — while its *pointer*-named columns must be quoted to contain a `/` and
    /// so keep their case, giving one row in two casings at once. Postgres folds the other way.
    pub fn from_row(row: &str) -> anyhow::Result<Self> {
        let value: serde_json::Value = serde_json::from_str(row).context("row is not JSON")?;

        let Some(columns) = value.as_object() else {
            anyhow::bail!("row is not a JSON object");
        };

        let mut document = serde_json::Map::new();
        for (name, value) in columns {
            insert_pointer(&mut document, &name.to_lowercase(), value.clone());
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
/// A conflict is a path descending through a value that is already a scalar. It happens only
/// when a destination has both a column `x` and a column `x/y`, which no projection produces.
///
/// One of the two columns must lose, and this keeps the scalar. Descending would have to
/// replace `x` with an object, and a scalar `oracle` is the JSON-text form that
/// [`Event::from_row`] re-parses into the whole object — so it carries at least as much as the
/// `oracle/seq` being dropped, and possibly more.
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
    /// A document that arrived carries the values the collection holds for it —
    /// the fields [`Event`] parses, not byte-for-byte.
    DocumentIntegrity,
    /// Every row in the destination corresponds to a document the collection holds.
    ///
    /// Not exemptable, like [`Invariant::DocumentIntegrity`]: no class of connector has a
    /// reason to invent a key or alter a document in transit, whatever else it gets wrong.
    NoFabrication,
    /// The latest delta row per key reconstructs the standard row.
    StandardDeltaAgreement,
}

impl Invariant {
    /// Every invariant, so anything enumerating them cannot fall behind the enum.
    pub const ALL: [Invariant; 8] = [
        Invariant::Conservation,
        Invariant::OracleAgreement,
        Invariant::NoLoss,
        Invariant::NoDuplicates,
        Invariant::Monotonicity,
        Invariant::DocumentIntegrity,
        Invariant::NoFabrication,
        Invariant::StandardDeltaAgreement,
    ];
}

impl std::fmt::Display for Invariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Conservation => "conservation",
            Self::OracleAgreement => "oracle-agreement",
            Self::NoLoss => "no-loss",
            Self::NoDuplicates => "no-duplicates",
            Self::Monotonicity => "monotonicity",
            Self::DocumentIntegrity => "document-integrity",
            Self::NoFabrication => "no-fabrication",
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
    /// How many times the read surfaced an `(id, seq)` it had already seen. A document
    /// surfaced three times counts twice, because both repeats are surplus.
    ///
    /// [`Account::seqs`] is a set, so a repeat fails its insert and changes nothing else —
    /// the balance is not added twice, and `by_seq` overwrites at the same key. This counter
    /// is therefore the only place a caller can learn that the read held duplicates.
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

/// The expectation itself must hold: it must name at least one account, and its accounts'
/// balances must sum to exactly zero. Every transfer is a matched pair of legs, so the
/// collection cannot sum to anything else.
///
/// This is the baseline guard. A wiring problem that made the harness read an empty or torn
/// collection shows up here as a failure, rather than as a vacuous pass everywhere else.
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
        // If the last materialized row's seq is behind the collection's maximum seq, we
        // consider it as data loss.
        if row.seq < account.max_seq {
            out.push(Violation {
                invariant: Invariant::NoLoss,
                detail: format!(
                    "account {id}: reduced seq {} is behind the collection's latest {}",
                    row.seq, account.max_seq
                ),
            });
        } else if row.seq > account.max_seq {
            out.push(Violation {
                invariant: Invariant::OracleAgreement,
                detail: format!(
                    "account {id}: reduced seq {} is ahead of the collection's latest {}",
                    row.seq, account.max_seq
                ),
            });
        }
    }

    for id in by_id.keys() {
        if !expected.accounts.contains_key(id) {
            out.push(Violation {
                invariant: Invariant::NoFabrication,
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

/// Walk each account's rows in the order the destination delivered them. Its `seq` must never
/// decrease along that walk.
///
/// Shared by both delta checkers, because they must count a regression identically. A run
/// counts the violations each exemption suppressed, and those counts are what the
/// `max_suppressed` ceilings are set from. `scenarios::REORDERING_CEILING` is 500 because
/// membership-change runs were each observed to suppress between 9 and 93 of these
/// violations. Both checkers feed that one ceiling, so if they counted a replay differently
/// the ceiling would be set from two different units.
fn check_monotonic(rows: &[Event], binding: &str, out: &mut Vec<Violation>) {
    let mut last_seq: BTreeMap<i64, i64> = BTreeMap::new();

    for row in rows {
        if let Some(previous) = last_seq.get(&row.id) {
            // `<` rather than `<=`, because a *repeated* seq is not a regression
            // of order, it is a duplicate, and `NoDuplicates` owns that.
            if row.seq < *previous {
                out.push(Violation {
                    invariant: Invariant::Monotonicity,
                    detail: format!(
                        "account {}: {binding} delivered seq {} after {previous}",
                        row.id, row.seq
                    ),
                });
            }
        }
        // The baseline advances to what was just delivered rather than holding a high-water
        // mark, so a replay is one violation rather than one per row it replays: after
        // 1..10 then 8, 9, 10, the 8 is the regression and the 9 and 10 after it are in order.
        last_seq.insert(row.id, row.seq);
    }
}

/// Check the append-only collection, materialized with delta-updates.
///
/// That collection is keyed `[/id, /seq]`, so every document holds a distinct key and gets its
/// own row. A duplicate delivery is therefore a second row, which is counted directly. The
/// merged collection cannot do this. A duplicate there is summed into the reduced balance,
/// where only arithmetic reveals it.
///
/// Row counts settle loss, duplication and fabrication. The row's own fields settle integrity,
/// and arrival order settles monotonicity.
fn check_log(expected: &Expectation, rows: &[Event], out: &mut Vec<Violation>) {
    let mut seen: BTreeMap<(i64, i64), usize> = BTreeMap::new();

    check_monotonic(rows, "the log binding", out);

    for row in rows {
        *seen.entry((row.id, row.seq)).or_default() += 1;

        // A faithfully transported document is still itself.
        if let Some(document) = expected
            .accounts
            .get(&row.id)
            .and_then(|a| a.by_seq.get(&row.seq))
        {
            if document.oracle != row.oracle || document.balance_delta != row.balance_delta {
                out.push(Violation {
                    invariant: Invariant::DocumentIntegrity,
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
            invariant: Invariant::NoFabrication,
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
    let mut reported: BTreeSet<i64> = BTreeSet::new();
    let mut seen: BTreeMap<(i64, i64), usize> = BTreeMap::new();

    // The running sum is arithmetic over a key's history, so it is accumulated in sequence
    // order, not in whatever order the destination handed the rows back — see the module doc.
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

    check_monotonic(delivery, "the delta binding", out);

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

    // The highest sequence each account's rows reached. This is a signal that allows us to
    // catch a missing tail of rows. Note that instead of just checking for the last document's
    // sequence, we take the maximum of sequence across all rows, since monotonicity may not
    // hold for certain cases, but we still want to be able to verify that the maximum sequence
    // matches between the collection and the materialized rows.
    let mut highest: BTreeMap<i64, i64> = BTreeMap::new();
    for row in delivery {
        let seen = highest.entry(row.id).or_insert(row.seq);
        *seen = (*seen).max(row.seq);
    }

    for (id, account) in &expected.accounts {
        if let Some(seq) = highest.get(id) {
            if *seq < account.max_seq {
                out.push(Violation {
                    invariant: Invariant::NoLoss,
                    detail: format!(
                        "account {id}: the delta binding's rows stop at seq {seq}, behind the \
                         collection's latest {}",
                        account.max_seq
                    ),
                });
            }
        }

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

    // Rows for an account the collection does not hold. For a `documentCounter` subject,
    // which has no standard binding, this is the merged path's only extra-row detector.
    for (id, seq) in highest {
        if !expected.accounts.contains_key(&id) {
            out.push(Violation {
                invariant: Invariant::NoFabrication,
                detail: format!(
                    "the delta binding holds account {id} (up to seq {seq}), which the collection \
                     does not"
                ),
            });
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

    /// A Snowflake row, verbatim from a run: unquoted identifiers upper-cased by the
    /// destination, and pointer-named columns keeping their case because they must be quoted to
    /// contain a `/`. One row in two casings at once.
    #[test]
    fn a_row_parses_whatever_case_the_destination_returns() {
        let row = r#"{"BALANCEDELTA":-139,"FLOW_PUBLISHED_AT":"2026-08-16T01:55:04Z","ID":0,
            "SEQ":29,"TS":"2026-08-16T01:55:03Z","oracle/balance":-178,"oracle/seq":29,
            "oracle/set":["a","f"],"set/remove":["b"],"transfer/amount":22}"#;

        let event = Event::from_row(row).expect("an upper-cased row is still a document");
        assert_eq!(event.id, 0);
        assert_eq!(event.seq, 29);
        assert_eq!(event.balance_delta, -139);
        assert_eq!(
            event.oracle,
            Oracle {
                seq: 29,
                balance: -178
            }
        );
    }

    /// The reference connector's shape — a whole document, mixed case — must keep working.
    #[test]
    fn a_stored_document_parses_unchanged() {
        let row = r#"{"id":7,"seq":3,"balanceDelta":42,"oracle":{"seq":3,"balance":100}}"#;

        let event = Event::from_row(row).expect("a stored document parses");
        assert_eq!((event.id, event.seq, event.balance_delta), (7, 3, 42));
        assert_eq!(
            event.oracle,
            Oracle {
                seq: 3,
                balance: 100
            }
        );
    }

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

    /// The name an invariant prints and the name it deserializes from must be the same string.
    ///
    /// There are two mappings — serde's `kebab-case` rename and the `Display` impl — and an
    /// exemption is written with one and reported with the other, so a drift between them
    /// would silently stop an exemption from matching the violations it names.
    #[test]
    fn every_invariant_name_round_trips() {
        for invariant in Invariant::ALL {
            let printed = invariant.to_string();
            let parsed: Invariant = serde_json::from_value(serde_json::json!(printed))
                .unwrap_or_else(|err| panic!("{printed:?} does not deserialize: {err}"));
            assert_eq!(
                parsed, invariant,
                "{printed:?} round-trips to a different variant"
            );
        }
    }

    /// A replay is one regression, not one per replayed row.
    ///
    /// Both checkers share `check_monotonic`, so their *agreement* is structural and needs no
    /// test; this pins the scoring rule itself.
    #[test]
    fn a_replay_is_one_monotonicity_violation() {
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
