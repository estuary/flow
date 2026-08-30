use crate::{Error, KEY_BEGIN, RCLOCK_BEGIN, expect_one, expect_one_u32, maybe_one, set_value};
use proto_flow::{flow, ops};
use proto_gazette::broker::LabelSet;

/// Encode a ShardLabeling into a LabelSet.
pub fn encode_labeling(mut set: LabelSet, labeling: &ops::ShardLabeling) -> LabelSet {
    set = set_value(set, crate::BUILD, &labeling.build);

    if !labeling.hostname.is_empty() {
        set = set_value(set, crate::HOSTNAME, &labeling.hostname);
    }
    set = set_value(set, crate::LOG_LEVEL, labeling.log_level().as_str_name());

    if let Some(range) = &labeling.range {
        set = encode_range_spec(set, range);
    }

    if !labeling.split_source.is_empty() {
        set = set_value(set, crate::SPLIT_SOURCE, &labeling.split_source);
    }
    if !labeling.split_target.is_empty() {
        set = set_value(set, crate::SPLIT_TARGET, &labeling.split_target);
    }

    set = set_value(set, crate::TASK_NAME, &labeling.task_name);
    set = set_value(set, crate::TASK_TYPE, labeling.task_type().as_str_name());

    for (name, value) in &labeling.flags {
        set = set_value(set, &format!("{}{name}", crate::FLAG_PREFIX), value);
    }

    set = set_value(set, crate::LOGS_JOURNAL, &labeling.logs_journal);
    set = set_value(set, crate::STATS_JOURNAL, &labeling.stats_journal);

    if labeling.shuffle_disk_limit_bytes != 0 {
        set = set_value(
            set,
            crate::SHUFFLE_DISK_LIMIT,
            &labeling.shuffle_disk_limit_bytes.to_string(),
        );
    }

    set
}

/// Decode a ShardLabeling from a LabelSet.
pub fn decode_labeling(set: &LabelSet) -> Result<ops::ShardLabeling, Error> {
    let build = expect_one(set, crate::BUILD)?.to_string();
    let hostname = maybe_one(set, crate::HOSTNAME)?.to_string();

    let log_level = expect_one(set, crate::LOG_LEVEL)?;
    let log_level = match ops::log::Level::from_str_name(log_level) {
        None | Some(ops::log::Level::UndefinedLevel) => {
            return Err(Error::InvalidValue {
                name: crate::LOG_LEVEL.to_string(),
                value: log_level.to_string(),
            });
        }
        Some(e) => e,
    } as i32;

    let range = if has_range_spec(set) {
        Some(decode_range_spec(set)?)
    } else {
        None
    };
    let split_source = maybe_one(set, crate::SPLIT_SOURCE)?.to_string();
    let split_target = maybe_one(set, crate::SPLIT_TARGET)?.to_string();
    let task_name = expect_one(set, crate::TASK_NAME)?.to_string();

    let task_type = expect_one(set, crate::TASK_TYPE)?;
    let task_type = match ops::TaskType::from_str_name(task_type) {
        None | Some(ops::TaskType::InvalidType) => {
            return Err(Error::InvalidValue {
                name: crate::TASK_TYPE.to_string(),
                value: task_type.to_string(),
            });
        }
        Some(e) => e,
    } as i32;

    let logs_journal = maybe_one(set, crate::LOGS_JOURNAL)?.to_string();
    let stats_journal = maybe_one(set, crate::STATS_JOURNAL)?.to_string();

    let shuffle_disk_limit_bytes = match maybe_one(set, crate::SHUFFLE_DISK_LIMIT)? {
        "" => 0,
        value => value.parse()?,
    };

    let mut flags = std::collections::BTreeMap::new();
    for label in &set.labels {
        if let Some(name) = label.name.strip_prefix(crate::FLAG_PREFIX) {
            flags.insert(name.to_string(), label.value.clone());
        }
    }

    if !split_source.is_empty() && !split_target.is_empty() {
        return Err(Error::SplitSourceAndTarget(
            split_source.to_string(),
            split_target.to_string(),
        ));
    }

    Ok(ops::ShardLabeling {
        build,
        hostname,
        log_level,
        range,
        split_source,
        split_target,
        task_name,
        task_type,
        logs_journal,
        stats_journal,
        flags,
        shuffle_disk_limit_bytes,
    })
}

/// Encode a RangeSpec into a LabelSet.
pub fn encode_range_spec(mut set: LabelSet, spec: &flow::RangeSpec) -> LabelSet {
    let fmt = |v: u32| format!("{v:08x}");

    set = set_value(set, crate::KEY_BEGIN, &fmt(spec.key_begin));
    set = set_value(set, crate::KEY_END, &fmt(spec.key_end));
    set = set_value(set, crate::RCLOCK_BEGIN, &fmt(spec.r_clock_begin));
    set = set_value(set, crate::RCLOCK_END, &fmt(spec.r_clock_end));
    set
}

/// Decode a RangeSpec from a LabelSet.
pub fn decode_range_spec(set: &LabelSet) -> Result<flow::RangeSpec, Error> {
    Ok(flow::RangeSpec {
        key_begin: expect_one_u32(set, crate::KEY_BEGIN)?,
        key_end: expect_one_u32(set, crate::KEY_END)?,
        r_clock_begin: expect_one_u32(set, crate::RCLOCK_BEGIN)?,
        r_clock_end: expect_one_u32(set, crate::RCLOCK_END)?,
    })
}

/// Determine if the LabelSet encodes a RangeSpec.
pub fn has_range_spec(set: &LabelSet) -> bool {
    for name in [
        crate::KEY_BEGIN,
        crate::KEY_END,
        crate::RCLOCK_BEGIN,
        crate::RCLOCK_END,
    ] {
        if !super::range(set, name).is_empty() {
            return true;
        }
    }
    false
}

/// Build the shard ID suffix that's implied by the LabelSet.
/// This suffix is appended to the task template's base ID
/// to form a complete shard ID.
pub fn id_suffix(set: &LabelSet) -> Result<String, Error> {
    let key_begin = expect_one(&set, KEY_BEGIN)?;
    let rclock_begin = expect_one(&set, RCLOCK_BEGIN)?;
    Ok(format!("{key_begin}-{rclock_begin}"))
}

/// One shard of an even split of the range space: its range, the LabelSet
/// encoding that range, and its complete shard ID.
pub struct EvenSplit {
    pub range: flow::RangeSpec,
    pub labels: LabelSet,
    pub id: String,
}

/// Split the key and r-clock spaces into `key_splits` and `rclock_splits` even
/// parts, and return the `key_splits * rclock_splits` shards which tile their
/// cross product. Each is named by appending its [`id_suffix`] to `id_prefix`
/// (a shard template ID, *without* a trailing '/').
///
/// Shards are ordered key-major, which is also the lexicographic order of their
/// IDs. Production only ever splits on key — an r-clock split arises later, by
/// subdividing an existing shard — so `rclock_splits` is one outside of tests
/// which need a two-dimensional topology.
pub fn even_splits(id_prefix: &str, key_splits: u32, rclock_splits: u32) -> Vec<EvenSplit> {
    even_bounds(key_splits)
        .flat_map(|(key_begin, key_end)| {
            even_bounds(rclock_splits).map(move |(r_clock_begin, r_clock_end)| {
                let range = flow::RangeSpec {
                    key_begin,
                    key_end,
                    r_clock_begin,
                    r_clock_end,
                };
                let labels = encode_range_spec(LabelSet::default(), &range);
                let id = format!(
                    "{id_prefix}/{}",
                    id_suffix(&labels).expect("we just encoded the range spec")
                );
                EvenSplit { range, labels, id }
            })
        })
        .collect()
}

/// The `count` inclusive [begin, end] bounds which evenly tile the full `u32`
/// space, in ascending order. Empty if `count` is zero.
fn even_bounds(count: u32) -> impl Iterator<Item = (u32, u32)> {
    // Widths are computed in u64 so that the full `u32::MAX + 1` space is
    // representable, and so that the arithmetic doesn't depend on usize width.
    const SPACE: u64 = u32::MAX as u64 + 1;

    (0..count as u64).map(move |i| {
        (
            (SPACE * i / count as u64) as u32,
            (SPACE * (i + 1) / count as u64 - 1) as u32,
        )
    })
}

/// Extract a shard's templated ID prefix, *including* the trailing '/' which
/// separates it from the key/r-clock suffix. Returns None if `name` has no '/'.
///
/// The trailing '/' is significant when the prefix is used as an `id:prefix`
/// label-selector scope: retaining it ensures `acmeCo/foo/` cannot bleed into a
/// sibling task such as `acmeCo/foobar/...`.
pub fn id_prefix<'n>(name: &'n str) -> Option<&'n str> {
    name.rfind('/').map(|i| &name[..i + 1])
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::build_set;

    #[test]
    fn test_encoding() {
        let labeling = ops::ShardLabeling {
            build: "a-build".to_string(),
            hostname: "a.hostname".to_string(),
            log_level: ops::log::Level::Info as i32,
            range: Some(flow::RangeSpec {
                key_begin: 256,
                key_end: 1024,
                r_clock_begin: u32::MIN,
                r_clock_end: u32::MAX,
            }),
            split_source: "split/source".to_string(),
            split_target: "split/target".to_string(),
            task_name: "task/name".to_string(),
            task_type: ops::TaskType::Derivation as i32,
            flags: [
                ("buffer-size".to_string(), "1024".to_string()),
                ("enable-new-thing".to_string(), "true".to_string()),
            ]
            .into(),
            logs_journal: "logs/journal".to_string(),
            stats_journal: "stats/journal".to_string(),
            shuffle_disk_limit_bytes: 134217728,
        };

        let set = encode_labeling(LabelSet::default(), &labeling);

        insta::assert_json_snapshot!(set, @r#"
        {
          "labels": [
            {
              "name": "estuary.dev/build",
              "value": "a-build"
            },
            {
              "name": "estuary.dev/flag/buffer-size",
              "value": "1024"
            },
            {
              "name": "estuary.dev/flag/enable-new-thing",
              "value": "true"
            },
            {
              "name": "estuary.dev/hostname",
              "value": "a.hostname"
            },
            {
              "name": "estuary.dev/key-begin",
              "value": "00000100"
            },
            {
              "name": "estuary.dev/key-end",
              "value": "00000400"
            },
            {
              "name": "estuary.dev/log-level",
              "value": "info"
            },
            {
              "name": "estuary.dev/logs-journal",
              "value": "logs/journal"
            },
            {
              "name": "estuary.dev/rclock-begin",
              "value": "00000000"
            },
            {
              "name": "estuary.dev/rclock-end",
              "value": "ffffffff"
            },
            {
              "name": "estuary.dev/shuffle-disk-limit",
              "value": "134217728"
            },
            {
              "name": "estuary.dev/split-source",
              "value": "split/source"
            },
            {
              "name": "estuary.dev/split-target",
              "value": "split/target"
            },
            {
              "name": "estuary.dev/stats-journal",
              "value": "stats/journal"
            },
            {
              "name": "estuary.dev/task-name",
              "value": "task/name"
            },
            {
              "name": "estuary.dev/task-type",
              "value": "derivation"
            }
          ]
        }
        "#);

        let id = format!("base/shard/id/{}", id_suffix(&set).unwrap());
        assert_eq!(id, "base/shard/id/00000100-00000000");
        assert_eq!(id_prefix(&id), Some("base/shard/id/"));
    }

    #[test]
    fn test_decode_cases() {
        let case = |set| match decode_labeling(&set) {
            Ok(ok) => serde_json::to_value(ok).unwrap(),
            Err(err) => serde_json::Value::String(err.to_string()),
        };

        // All labels except SPLIT_TARGET set.
        let model = build_set([
            (crate::BUILD, "a-build"),
            ("estuary.dev/flag/buffer-size", "1024"),
            ("estuary.dev/flag/enable-new-thing", "true"),
            (crate::HOSTNAME, "a.hostname"),
            (crate::KEY_BEGIN, "00000001"),
            (crate::KEY_END, "00000002"),
            (crate::LOG_LEVEL, "info"),
            (crate::RCLOCK_BEGIN, "00000003"),
            (crate::RCLOCK_END, "00000004"),
            (crate::SPLIT_SOURCE, "split/source"),
            (crate::TASK_NAME, "the/task"),
            (crate::TASK_TYPE, "capture"),
            (crate::LOGS_JOURNAL, "logs/journal"),
            (crate::STATS_JOURNAL, "stats/journal"),
        ]);

        insta::assert_json_snapshot!(
            case(model.clone()),
            @r#"
        {
          "build": "a-build",
          "flags": {
            "buffer-size": "1024",
            "enable-new-thing": "true"
          },
          "hostname": "a.hostname",
          "logLevel": "info",
          "logsJournal": "logs/journal",
          "range": {
            "keyBegin": 1,
            "keyEnd": 2,
            "rClockBegin": 3,
            "rClockEnd": 4
          },
          "splitSource": "split/source",
          "statsJournal": "stats/journal",
          "taskName": "the/task",
          "taskType": "capture"
        }
        "#
        );

        // Optional labels removed & split target instead of source.
        let mut set = model.clone();
        for name in [
            crate::HOSTNAME,
            crate::SPLIT_SOURCE,
            crate::KEY_BEGIN,
            crate::KEY_END,
            crate::RCLOCK_BEGIN,
            crate::RCLOCK_END,
            crate::LOGS_JOURNAL,
            crate::STATS_JOURNAL,
        ] {
            set = crate::remove(set, name);
        }
        set = crate::add_value(set, crate::SPLIT_TARGET, "split/target");

        insta::assert_json_snapshot!(case(set),
            @r#"
        {
          "build": "a-build",
          "flags": {
            "buffer-size": "1024",
            "enable-new-thing": "true"
          },
          "logLevel": "info",
          "splitTarget": "split/target",
          "taskName": "the/task",
          "taskType": "capture"
        }
        "#
        );

        // Expected label is missing.
        let set = crate::remove(model.clone(), crate::BUILD);
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/build (got [])""###);

        // Expected label has too many values.
        let set = crate::add_value(model.clone(), crate::BUILD, "other");
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/build (got [Label { name: \"estuary.dev/build\", value: \"a-build\", prefix: false }, Label { name: \"estuary.dev/build\", value: \"other\", prefix: false }])""###);

        // Invalid log level.
        let set = crate::set_value(model.clone(), crate::LOG_LEVEL, "invalid");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"invalid\" for label estuary.dev/log-level""###);

        // Invalid task type.
        let set = crate::set_value(model.clone(), crate::TASK_TYPE, "invalid");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"invalid\" for label estuary.dev/task-type""###);

        // Invalid hex range (not 8 bytes).
        let set = crate::set_value(model.clone(), crate::KEY_BEGIN, "0011");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"0011\" for label estuary.dev/key-begin""###);

        // Invalid hex range (not hex).
        let set = crate::set_value(model.clone(), crate::KEY_BEGIN, "0000000z");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"0000000z\" for label estuary.dev/key-begin""###);

        // Missing subset of RangeSpec.
        let set = crate::remove(model.clone(), crate::RCLOCK_BEGIN);
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/rclock-begin (got [])""###);

        // Split source AND target.
        let set = crate::add_value(model.clone(), crate::SPLIT_TARGET, "split/target");
        insta::assert_json_snapshot!(case(set),
            @r###""both split-source split/source and split-target split/target are set but shouldn't be""###);
    }

    #[test]
    fn test_even_splits() {
        let case = |key_splits, rclock_splits| {
            even_splits(
                "derivation/acmeCo/thing/0000000000000000",
                key_splits,
                rclock_splits,
            )
            .into_iter()
            .map(|s| {
                (
                    s.id,
                    (s.range.key_begin, s.range.key_end),
                    (s.range.r_clock_begin, s.range.r_clock_end),
                )
            })
            .collect::<Vec<_>>()
        };

        // One shard spans the whole range space.
        insta::assert_debug_snapshot!(case(1, 1), @r###"
        [
            (
                "derivation/acmeCo/thing/0000000000000000/00000000-00000000",
                (
                    0,
                    4294967295,
                ),
                (
                    0,
                    4294967295,
                ),
            ),
        ]
        "###);

        // Three shards tile the key space without gap or overlap, and the last
        // ends at u32::MAX (the division is exact, so no remainder is dropped).
        insta::assert_debug_snapshot!(case(3, 1), @r###"
        [
            (
                "derivation/acmeCo/thing/0000000000000000/00000000-00000000",
                (
                    0,
                    1431655764,
                ),
                (
                    0,
                    4294967295,
                ),
            ),
            (
                "derivation/acmeCo/thing/0000000000000000/55555555-00000000",
                (
                    1431655765,
                    2863311529,
                ),
                (
                    0,
                    4294967295,
                ),
            ),
            (
                "derivation/acmeCo/thing/0000000000000000/aaaaaaaa-00000000",
                (
                    2863311530,
                    4294967295,
                ),
                (
                    0,
                    4294967295,
                ),
            ),
        ]
        "###);

        // An r-clock split alone leaves every shard spanning the full key space,
        // distinguished only by the second half of its ID suffix.
        insta::assert_debug_snapshot!(case(1, 2), @r###"
        [
            (
                "derivation/acmeCo/thing/0000000000000000/00000000-00000000",
                (
                    0,
                    4294967295,
                ),
                (
                    0,
                    2147483647,
                ),
            ),
            (
                "derivation/acmeCo/thing/0000000000000000/00000000-80000000",
                (
                    0,
                    4294967295,
                ),
                (
                    2147483648,
                    4294967295,
                ),
            ),
        ]
        "###);

        // A 2x2 split tiles the two-dimensional space, key-major.
        insta::assert_debug_snapshot!(case(2, 2), @r###"
        [
            (
                "derivation/acmeCo/thing/0000000000000000/00000000-00000000",
                (
                    0,
                    2147483647,
                ),
                (
                    0,
                    2147483647,
                ),
            ),
            (
                "derivation/acmeCo/thing/0000000000000000/00000000-80000000",
                (
                    0,
                    2147483647,
                ),
                (
                    2147483648,
                    4294967295,
                ),
            ),
            (
                "derivation/acmeCo/thing/0000000000000000/80000000-00000000",
                (
                    2147483648,
                    4294967295,
                ),
                (
                    0,
                    2147483647,
                ),
            ),
            (
                "derivation/acmeCo/thing/0000000000000000/80000000-80000000",
                (
                    2147483648,
                    4294967295,
                ),
                (
                    2147483648,
                    4294967295,
                ),
            ),
        ]
        "###);

        // Zero splits in either dimension is an empty cross product.
        assert!(case(0, 3).is_empty());
        assert!(case(3, 0).is_empty());

        // Every combination tiles the two-dimensional space exactly.
        for key_splits in [1, 2, 3, 5, 16] {
            for rclock_splits in [1, 2, 3, 7, 100] {
                let splits = even_splits("prefix", key_splits, rclock_splits);
                assert_eq!(splits.len() as u32, key_splits * rclock_splits);

                // Shards are key-major, so each contiguous run of
                // `rclock_splits` shares one key range and tiles the r-clock
                // space, and the runs themselves tile the key space.
                let runs: Vec<_> = splits.chunks(rclock_splits as usize).collect();

                assert_eq!(runs[0][0].range.key_begin, 0);
                assert_eq!(runs[runs.len() - 1][0].range.key_end, u32::MAX);

                for pair in runs.windows(2) {
                    assert_eq!(
                        pair[0][0].range.key_end + 1,
                        pair[1][0].range.key_begin,
                        "key gap or overlap at {key_splits}x{rclock_splits}"
                    );
                }

                for run in runs {
                    assert_eq!(run[0].range.r_clock_begin, 0);
                    assert_eq!(run[run.len() - 1].range.r_clock_end, u32::MAX);

                    for pair in run.windows(2) {
                        assert_eq!(pair[0].range.key_begin, pair[1].range.key_begin);
                        assert_eq!(pair[0].range.key_end, pair[1].range.key_end);
                        assert_eq!(
                            pair[0].range.r_clock_end + 1,
                            pair[1].range.r_clock_begin,
                            "r-clock gap or overlap at {key_splits}x{rclock_splits}"
                        );
                    }
                    // Each ID agrees with the labels it was built from, so
                    // shards sharing a key range are still distinctly named.
                    for split in run {
                        assert!(split.id.ends_with(&id_suffix(&split.labels).unwrap()));
                    }
                }
            }
        }
    }
}
