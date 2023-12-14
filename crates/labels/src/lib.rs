// NOTE constants in this file must be mirrored in
// go/labels/labels.go
// See that file for descriptions of each label.

use proto_gazette::broker::{Label, LabelSet};

// JournalSpec & ShardSpec labels.
pub const BUILD: &str = "estuary.dev/build";
pub const COLLECTION: &str = "estuary.dev/collection";
pub const FIELD_PREFIX: &str = "estuary.dev/field/";
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
pub const RCLOCK_BEGIN_MIN: &str = KEY_BEGIN;
pub const RCLOCK_END: &str = "estuary.dev/rclock-end";
pub const RCLOCK_END_MAX: &str = KEY_END_MAX;
pub const SPLIT_TARGET: &str = "estuary.dev/split-target";
pub const SPLIT_SOURCE: &str = "estuary.dev/split-source";
pub const LOG_LEVEL: &str = "estuary.dev/log-level";
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

pub mod encode;
pub mod parse;

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
    let mut labels: Vec<Label> = it
        .into_iter()
        .map(|(name, value)| Label {
            name: name.as_ref().to_string(),
            value: value.as_ref().to_string(),
        })
        .collect();

    labels.sort_by(|l, r| l.name.cmp(&r.name));

    LabelSet { labels }
}

/// Update a LabelSet, replacing all labels of `name` with a single label having `value`.
pub fn set_value(set: &mut LabelSet, name: &str, value: &str) {
    set.labels.splice(
        range(set, name),
        [Label {
            name: name.to_string(),
            value: value.to_string(),
        }],
    );
}

/// Update a LabelSet, adding a new label of `name` and `value`.
pub fn add_value(set: &mut LabelSet, name: &str, value: &str) {
    set.labels.insert(
        range(set, name).end,
        Label {
            name: name.to_string(),
            value: value.to_string(),
        },
    );
}

/// Update a LabelSet, removing all labels of `name`.
pub fn remove(set: &mut LabelSet, name: &str) {
    set.labels.drain(range(set, name));
}

#[cfg(test)]
mod test {
    use crate::*;

    #[test]
    fn label_range_cases() {
        let set = crate::build_set([
            ("a", ""),
            ("b", ""),
            ("b", ""),
            ("d", ""),
            ("e", ""),
            ("e", ""),
            ("e", ""),
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
        let set = &mut set;

        add_value(set, "a", "aa.2");
        set_value(set, "d", "dd.2");
        add_value(set, "b", "bb.1");
        remove(set, "c");
        remove(set, "z");

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
              "value": "bb.1"
            },
            {
              "name": "d",
              "value": "dd.2"
            }
          ]
        }
        "###);
    }
}
