//! The recovery floor: where a replay of a disk journal may begin.
//!
//! The daemon derives the floor from journal contents, so it also persists it,
//! as a label on the journal's own spec under a name each deployment states.
//! Reading it back is the label's only use here. It is a seek position and never
//! a filter, because filtering by clock could drop a record from the middle of a
//! delta, and a floor which is absent or behind costs replay work and changes
//! nothing about the result.
//!
//! The value encoding is fixed rather than configurable: sixteen lowercase hex
//! characters of the u64 message clock. Fixed-width hex orders lexicographically
//! as it does numerically, and it matches the `estuary.dev/truncated-at` value
//! Flow already writes, so tooling which reads that label name generically
//! cannot misparse a disk journal.

use anyhow::Context;
use proto_gazette::{broker, uuid};

/// Modification time at which a replay of `journal` begins, in seconds.
///
/// A fragment is persisted no earlier than the records it holds, so a fragment
/// modified before the floor's clock holds only records below the floor and the
/// broker may skip it. Zero, which is an unnamed or absent label, begins at the
/// first fragment available.
pub async fn seek(
    client: &gazette::journal::Client,
    journal: &str,
    label: &str,
) -> anyhow::Result<i64> {
    if label.is_empty() {
        return Ok(0);
    }
    let request = broker::ListRequest {
        selector: Some(broker::LabelSelector {
            include: Some(broker::LabelSet {
                labels: vec![broker::Label {
                    name: "name".to_string(),
                    value: journal.to_string(),
                    prefix: false,
                }],
            }),
            exclude: None,
        }),
        ..Default::default()
    };

    let response = client
        .list(request)
        .await
        .with_context(|| format!("listing {journal} for its {label} label"))?;

    let Some(value) = response
        .journals
        .iter()
        .filter_map(|listed| listed.spec.as_ref())
        .flat_map(|spec| spec.labels.iter())
        .flat_map(|set| set.labels.iter())
        .find(|candidate| candidate.name == label)
    else {
        return Ok(0);
    };
    let clock = u64::from_str_radix(&value.value, 16)
        .with_context(|| format!("{journal} has a malformed {label} of {:?}", value.value))?;

    Ok(uuid::Clock::from_u64(clock).to_unix().0 as i64)
}
