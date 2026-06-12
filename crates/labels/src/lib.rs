// NOTE constants in this file must be mirrored in
// go/labels/labels.go
// See that file for descriptions of each label.

use proto_gazette::broker::{Label, LabelSelector, LabelSet};

// JournalSpec & ShardSpec labels.
pub const BUILD: &str = "estuary.dev/build";
pub const COLLECTION: &str = "estuary.dev/collection";
pub const CORDON: &str = "estuary.dev/cordon";
pub const FIELD_PREFIX: &str = "estuary.dev/field/";
pub const FLAG_PREFIX: &str = "estuary.dev/flag/";
// Flag which enables the V2 task runtime.
// TODO(whb): remove once the runtime-v2 migration is complete.
pub const RUNTIME_V2_FLAG: &str = "estuary.dev/flag/enable-runtime-v2";
pub const KEY_BEGIN: &str = "estuary.dev/key-begin";
pub const KEY_BEGIN_MIN: &str = "00000000";
pub const KEY_END: &str = "estuary.dev/key-end";
pub const KEY_END_MAX: &str = "ffffffff";
pub const MANAGED_BY_FLOW: &str = "estuary.dev/flow";

// ShardSpec labels.
pub const TASK_NAME: &str = "estuary.dev/task-name";
pub const TASK_TYPE: &str = "estuary.dev/task-type";
pub const TASK_TYPE_CAPTURE: &str = "capture";
pub const TASK_TYPE_DERIVATION: &str = "derivation";
pub const TASK_TYPE_MATERIALIZATION: &str = "materialization";
pub const RCLOCK_BEGIN: &str = "estuary.dev/rclock-begin";
pub const RCLOCK_BEGIN_MIN: &str = KEY_BEGIN_MIN;
pub const RCLOCK_END: &str = "estuary.dev/rclock-end";
pub const RCLOCK_END_MAX: &str = KEY_END_MAX;
pub const SPLIT_TARGET: &str = "estuary.dev/split-target";
pub const SPLIT_SOURCE: &str = "estuary.dev/split-source";
pub const LOG_LEVEL: &str = "estuary.dev/log-level";
pub const LOGS_JOURNAL: &str = "estuary.dev/logs-journal";
pub const STATS_JOURNAL: &str = "estuary.dev/stats-journal";
// Shard labels related to network connectivity to shards.
pub const HOSTNAME: &str = "estuary.dev/hostname";
pub const EXPOSE_PORT: &str = "estuary.dev/expose-port";
pub const PORT_PROTO_PREFIX: &str = "estuary.dev/port-proto/";
pub const PORT_PUBLIC_PREFIX: &str = "estuary.dev/port-public/";

// A used subset of Gazette labels, defined in go.gazette.dev/core/labels/labels.go.
pub const CONTENT_TYPE: &str = "content-type";
pub const CONTENT_TYPE_JSON_LINES: &str = "application/x-ndjson";
pub const CONTENT_TYPE_RECOVERY_LOG: &str = "application/x-gazette-recoverylog";
pub const MANAGED_BY: &str = "app.gazette.dev/managed-by";

pub mod partition;
pub mod shard;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("expected one label for {0} (got {1:?})")]
    ExpectedOne(String, Vec<Label>),
    #[error("label {0} value is empty but shouldn't be")]
    ValueEmpty(String),
    #[error("invalid value {value:?} for label {name}")]
    InvalidValue { name: String, value: String },
    #[error("both split-source {0} and split-target {1} are set but shouldn't be")]
    SplitSourceAndTarget(String, String),
    #[error("failed to parse label value as integer")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("failed to parse label value as UTF-8 string")]
    PercentDecode(#[from] std::str::Utf8Error),
    #[error("invalid value type for partition field value encoding")]
    InvalidValueType,
    #[error("labels are not sorted by (name, value): {0:?} precedes {1:?}")]
    NotSorted(Label, Label),
}

/// Retrieve the sub-slice of Label having the given label `name`.
/// Or, if no labels match, return an empty slice.
#[inline]
pub fn values<'s>(set: &'s LabelSet, name: &str) -> &'s [Label] {
    &set.labels[range(set, name)]
}

/// Determine the offset range of Labels that match `name`.
/// If `name` is not found, then return an empty range at its insertion point.
pub fn range(set: &LabelSet, name: &str) -> std::ops::Range<usize> {
    let set = set.labels.as_slice();

    let mut index = match set.binary_search_by(|probe| probe.name.as_str().cmp(name)) {
        Ok(index) => index,
        Err(index) => return index..index, // Not found.
    };
    // binary_search_by can return any match. Step backwards to the first.
    while index != 0 && set[index - 1].name == name {
        index -= 1;
    }

    // Find the first label which is strictly larger than `name`.
    let n = set[index..]
        .binary_search_by(|probe| {
            if probe.name.as_str().le(name) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        })
        .unwrap_err();

    index..(index + n)
}

/// Build a LabelSet from the input iterator of label names and values.
pub fn build_set<I, S>(it: I) -> LabelSet
where
    I: IntoIterator<Item = (S, S)>,
    S: AsRef<str>,
{
    let mut set = LabelSet { labels: Vec::new() };

    for (name, value) in it {
        set = add_value(set, name.as_ref(), value.as_ref());
    }
    set
}

/// Update a LabelSet, replacing all labels of `name` with a single label having `value`.
/// If `name` has the special suffix ":prefix", the Label is marked as a prefix
/// match. It's only valid to use ":prefix" within the context of a LabelSelector.
pub fn set_value(mut set: LabelSet, name: &str, value: &str) -> LabelSet {
    let (name, prefix) = if name.ends_with(":prefix") {
        (&name[..name.len() - 7], true)
    } else {
        (name, false)
    };

    set.labels.splice(
        range(&set, name),
        [Label {
            name: name.to_string(),
            value: value.to_string(),
            prefix,
        }],
    );
    set
}

/// Add a Label `name` with `value`, retaining any existing Labels of `name`.
/// If `name` has the special suffix ":prefix", the Label is marked as a prefix
/// match. It's only valid to use ":prefix" within the context of a LabelSelector.
pub fn add_value(mut set: LabelSet, name: &str, value: &str) -> LabelSet {
    let (name, prefix) = if name.ends_with(":prefix") {
        (&name[..name.len() - 7], true)
    } else {
        (name, false)
    };
    let r = range(&set, name);

    // Within the range of labels matching `name`, find the insertion point for `value`.
    let index = match set.labels[r.start..r.end]
        .binary_search_by(|probe| probe.value.as_str().cmp(value))
    {
        Ok(_index) => return set,      // `value` is already present.
        Err(index) => r.start + index, // Insertion point.
    };

    set.labels.insert(
        index,
        Label {
            name: name.to_string(),
            value: value.to_string(),
            prefix,
        },
    );
    set
}

/// Update a LabelSet, removing all labels of `name`.
pub fn remove(mut set: LabelSet, name: &str) -> LabelSet {
    set.labels.drain(range(&set, name));
    set
}

// Determine whether `label` is managed by the Flow data-plane,
// as opposed to the Flow control plane.
// * Data-plane labels exist exclusively within the data-plane,
//   and use its Etcd as their source of truth.
// * All other labels are set by the control plane.
pub fn is_data_plane_label(label: &str) -> bool {
    // If `label` has FIELD_PREFIX as a prefix, its suffix is an encoded logical partition.
    if label.starts_with(FIELD_PREFIX) {
        return true;
    }
    match label {
        // Key and R-Clock splits are performed within the data-plane.
        CORDON | KEY_BEGIN | KEY_END | RCLOCK_BEGIN | RCLOCK_END | SPLIT_SOURCE | SPLIT_TARGET => {
            true
        }
        _ => false,
    }
}

/// Percent-encoding of string values so that they can be used in label values.
pub fn percent_encoding<'s>(s: &'s str) -> percent_encoding::PercentEncode<'s> {
    // The set of characters that must be percent-encoded when used in partition
    // values. It's nearly everything, aside from a few special cases.
    const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.');
    percent_encoding::utf8_percent_encode(s, SET)
}

pub fn expect_one_u32(set: &LabelSet, name: &str) -> Result<u32, Error> {
    let value = expect_one(set, name)?;

    let (8, Ok(parsed)) = (value.len(), u32::from_str_radix(value, 16)) else {
        return Err(Error::InvalidValue {
            name: name.to_string(),
            value: value.to_string(),
        });
    };
    Ok(parsed)
}

pub fn expect_one<'s>(set: &'s LabelSet, name: &str) -> Result<&'s str, Error> {
    let labels = values(set, name);

    if labels.len() != 1 {
        return Err(Error::ExpectedOne(name.to_string(), labels.to_vec()));
    } else if labels[0].value.is_empty() {
        return Err(Error::ValueEmpty(name.to_string()));
    } else {
        Ok(labels[0].value.as_str())
    }
}

pub fn maybe_one<'s>(set: &'s LabelSet, name: &str) -> Result<&'s str, Error> {
    let labels = values(set, name);

    if labels.len() > 1 {
        return Err(Error::ExpectedOne(name.to_string(), labels.to_vec()));
    } else if labels.is_empty() {
        return Ok("");
    } else if labels[0].value.is_empty() {
        return Err(Error::ValueEmpty(name.to_string()));
    } else {
        Ok(labels[0].value.as_str())
    }
}

/// Returns whether `set` is matched by `selector`, faithfully porting gazette's
/// `LabelSelector.Matches` (`go.gazette.dev/core/broker/protocol`):
/// an excluded label matching any of `set` is a non-match; otherwise every
/// included label must match. A name present in `set` but not the selector is
/// ignored, and a selector value matches when it is empty (wildcard), exactly
/// equal, or (for a `prefix` label) a prefix of the set value.
///
/// The merge-join algorithm requires both selector and set labels sorted by
/// (name, value); this is validated and an unsorted input is rejected with
/// `Error::NotSorted` rather than silently re-sorted, because externally-issued
/// tokens are untrusted and a mis-sorted selector must not be reinterpreted.
pub fn matches(selector: &LabelSelector, set: &LabelSet) -> Result<bool, Error> {
    let include = selector.include.as_ref().map_or(&[][..], |s| &s.labels);
    let exclude = selector.exclude.as_ref().map_or(&[][..], |s| &s.labels);

    validate_sorted(include)?;
    validate_sorted(exclude)?;
    validate_sorted(&set.labels)?;

    if match_selector(exclude, &set.labels, false) {
        return Ok(false); // At least one excluded label is matched.
    } else if !match_selector(include, &set.labels, true) {
        return Ok(false); // Not every included label is matched.
    }
    Ok(true)
}

/// Validate that `labels` are sorted ascending by (name, value).
fn validate_sorted(labels: &[Label]) -> Result<(), Error> {
    for w in labels.windows(2) {
        let (a, b) = (&w[0], &w[1]);
        if (a.name.as_str(), a.value.as_str()) > (b.name.as_str(), b.value.as_str()) {
            return Err(Error::NotSorted(a.clone(), b.clone()));
        }
    }
    Ok(())
}

/// Port of gazette's `matchSelector`: for each label name, determine whether at
/// least one selector value matches a set value. With `req_all`, every selector
/// name must match (used for includes); otherwise a single match suffices (used
/// for excludes). Both inputs must be sorted by (name, value).
fn match_selector(sel: &[Label], set: &[Label], req_all: bool) -> bool {
    let mut it = LabelJoin {
        set_l: sel,
        set_r: set,
        l_beg: 0,
        l_end: 0,
        r_beg: 0,
        r_end: 0,
    };
    while let Some((l_beg, l_end, r_beg, r_end)) = it.next() {
        if l_beg == l_end {
            continue; // A `set` label name which is not in `sel`.
        }
        // Determine if at least one label value of `sel` is present in `set`.
        let mut matched = false;
        let (mut a, mut b) = (&sel[l_beg..l_end], &set[r_beg..r_end]);

        while !matched && !a.is_empty() && !b.is_empty() {
            if a[0].value.is_empty()
                || (!a[0].prefix && a[0].value == b[0].value)
                || (a[0].prefix && b[0].value.starts_with(&a[0].value))
            {
                matched = true; // Empty selector value implicitly matches any value.
            } else if a[0].value < b[0].value {
                a = &a[1..];
            } else {
                b = &b[1..];
            }
        }

        if !req_all && matched {
            return true;
        } else if req_all && !matched {
            return false;
        }
    }
    req_all
}

/// Full outer join of two (name, value)-sorted `[Label]` slices, yielding for
/// each distinct name the half-open index ranges of its labels on each side.
/// Mirrors gazette's `labelJoin`.
struct LabelJoin<'a> {
    set_l: &'a [Label],
    set_r: &'a [Label],
    l_beg: usize,
    l_end: usize,
    r_beg: usize,
    r_end: usize,
}

impl<'a> LabelJoin<'a> {
    fn next(&mut self) -> Option<(usize, usize, usize, usize)> {
        let (len_l, len_r) = (self.set_l.len(), self.set_r.len());

        if self.l_beg == len_l && self.r_beg == len_r {
            return None; // Both sequences complete.
        }
        let c: i32 = if self.l_beg == len_l {
            1 // LHS sequence complete. Step RHS.
        } else if self.r_beg == len_r {
            -1 // RHS sequence complete. Step LHS.
        } else {
            match self.set_l[self.l_beg]
                .name
                .cmp(&self.set_r[self.r_beg].name)
            {
                std::cmp::Ordering::Equal => 0,   // Step both.
                std::cmp::Ordering::Less => -1,   // Step LHS.
                std::cmp::Ordering::Greater => 1, // Step RHS.
            }
        };

        while self.l_end != len_l
            && c <= 0
            && self.set_l[self.l_end].name == self.set_l[self.l_beg].name
        {
            self.l_end += 1;
        }
        while self.r_end != len_r
            && c >= 0
            && self.set_r[self.r_end].name == self.set_r[self.r_beg].name
        {
            self.r_end += 1;
        }

        let cur = (self.l_beg, self.l_end, self.r_beg, self.r_end);
        (self.l_beg, self.r_beg) = (self.l_end, self.r_end);
        Some(cur)
    }
}

#[cfg(test)]
mod test {
    use crate::*;

    #[test]
    fn label_range_cases() {
        let set = crate::build_set([
            ("a", "1"),
            ("b", "2"),
            ("b", "3"),
            ("d", "4"),
            ("e", "5"),
            ("e", "6"),
            ("e:prefix", "7"),
        ]);

        assert_eq!(range(&set, "_"), 0..0);
        assert_eq!(range(&set, "a"), 0..1);
        assert_eq!(range(&set, "aa"), 1..1);
        assert_eq!(range(&set, "b"), 1..3);
        assert_eq!(range(&set, "c"), 3..3);
        assert_eq!(range(&set, "d"), 3..4);
        assert_eq!(range(&set, "dd"), 4..4);
        assert_eq!(range(&set, "e"), 4..7);
        assert_eq!(range(&set, "ee"), 7..7);
        assert_eq!(range(&set, "z"), 7..7);
    }

    #[test]
    fn mutation_cases() {
        let mut set = crate::build_set([("a", "aa"), ("c", "cc"), ("d", "dd"), ("z", "")]);

        set = add_value(set, "a", "aa.2");
        set = set_value(set, "d:prefix", "dd.2");
        set = add_value(set, "b:prefix", "bb.1");
        set = remove(set, "c");
        set = remove(set, "z");

        insta::assert_json_snapshot!(set, @r###"
        {
          "labels": [
            {
              "name": "a",
              "value": "aa"
            },
            {
              "name": "a",
              "value": "aa.2"
            },
            {
              "name": "b",
              "value": "bb.1",
              "prefix": true
            },
            {
              "name": "d",
              "value": "dd.2",
              "prefix": true
            }
          ]
        }
        "###);

        // Adding values out of order and with repeats.
        for v in &["aa.2", "aa.1", "aa.3", "aa.1", "aa.2", "aa.4", "aa.0"] {
            set = add_value(set, "a", v);
        }

        insta::assert_json_snapshot!(set, @r###"
        {
          "labels": [
            {
              "name": "a",
              "value": "aa"
            },
            {
              "name": "a",
              "value": "aa.0"
            },
            {
              "name": "a",
              "value": "aa.1"
            },
            {
              "name": "a",
              "value": "aa.2"
            },
            {
              "name": "a",
              "value": "aa.3"
            },
            {
              "name": "a",
              "value": "aa.4"
            },
            {
              "name": "b",
              "value": "bb.1",
              "prefix": true
            },
            {
              "name": "d",
              "value": "dd.2",
              "prefix": true
            }
          ]
        }
        "###);
    }

    #[test]
    fn percent_encode() {
        let cases = [
            ("foo", "foo"),
            ("one/two", "one%2Ftwo"),
            ("hello, world!", "hello%2C%20world%21"),
            (
                "no.no&no-no@no$yes_yes();",
                "no.no%26no-no%40no%24yes_yes%28%29%3B",
            ),
            (
                "http://example/path?q1=v1&q2=v2;ex%20tra",
                "http%3A%2F%2Fexample%2Fpath%3Fq1%3Dv1%26q2%3Dv2%3Bex%2520tra",
            ),
        ];
        for (fixture, expect) in cases {
            assert_eq!(percent_encoding(fixture).to_string(), expect);
        }
    }

    fn selector(include: LabelSet, exclude: LabelSet) -> LabelSelector {
        LabelSelector {
            include: Some(include),
            exclude: Some(exclude),
        }
    }

    #[test]
    fn matcher_parity_cases() {
        let empty = || crate::build_set(std::iter::empty::<(&str, &str)>());

        // Exact match.
        assert!(
            matches(
                &selector(crate::build_set([("env", "prod")]), empty()),
                &crate::build_set([("env", "prod"), ("zone", "a")]),
            )
            .unwrap()
        );
        // Exact mismatch.
        assert!(
            !matches(
                &selector(crate::build_set([("env", "prod")]), empty()),
                &crate::build_set([("env", "qa")]),
            )
            .unwrap()
        );
        // Missing included name => no match.
        assert!(
            !matches(
                &selector(crate::build_set([("env", "prod")]), empty()),
                &crate::build_set([("zone", "a")]),
            )
            .unwrap()
        );

        // Prefix match (selector value is a prefix of the set value).
        assert!(
            matches(
                &selector(
                    crate::set_value(empty(), "id:prefix", "task/00ab/"),
                    empty()
                ),
                &crate::build_set([("id", "task/00ab/0000-0000")]),
            )
            .unwrap()
        );
        assert!(
            !matches(
                &selector(
                    crate::set_value(empty(), "id:prefix", "task/00ab/"),
                    empty()
                ),
                &crate::build_set([("id", "task/00cd/0000-0000")]),
            )
            .unwrap()
        );

        // Empty selector value is a wildcard: name must merely be present.
        assert!(
            matches(
                &selector(crate::build_set([("env", "")]), empty()),
                &crate::build_set([("env", "anything")]),
            )
            .unwrap()
        );

        // Multi-value include matches if any value is present.
        assert!(
            matches(
                &selector(crate::build_set([("env", "prod"), ("env", "qa")]), empty()),
                &crate::build_set([("env", "qa")]),
            )
            .unwrap()
        );

        // Exclude: a matched excluded label vetoes the match.
        assert!(
            !matches(
                &selector(empty(), crate::build_set([("zone", "a")])),
                &crate::build_set([("env", "prod"), ("zone", "a")]),
            )
            .unwrap()
        );
        // Exclude with no overlap still matches.
        assert!(
            matches(
                &selector(
                    crate::build_set([("env", "prod")]),
                    crate::build_set([("zone", "b")])
                ),
                &crate::build_set([("env", "prod"), ("zone", "a")]),
            )
            .unwrap()
        );

        // Empty selector matches everything.
        assert!(matches(&selector(empty(), empty()), &crate::build_set([("x", "y")])).unwrap());
    }

    #[test]
    fn matcher_rejects_unsorted() {
        let unsorted = LabelSet {
            labels: vec![
                Label {
                    name: "z".to_string(),
                    value: "1".to_string(),
                    prefix: false,
                },
                Label {
                    name: "a".to_string(),
                    value: "1".to_string(),
                    prefix: false,
                },
            ],
        };
        let err = matches(
            &selector(
                unsorted,
                crate::build_set(std::iter::empty::<(&str, &str)>()),
            ),
            &crate::build_set([("a", "1")]),
        )
        .unwrap_err();
        assert!(matches!(err, Error::NotSorted(..)));
    }
}
