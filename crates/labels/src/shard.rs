use crate::{expect_one, expect_one_u32, maybe_one, set_value, Error, KEY_BEGIN, RCLOCK_BEGIN};
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

    set = set_value(set, crate::LOGS_JOURNAL, &labeling.logs_journal);
    set = set_value(set, crate::STATS_JOURNAL, &labeling.stats_journal);

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
            })
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
            })
        }
        Some(e) => e,
    } as i32;

    let logs_journal = maybe_one(set, crate::LOGS_JOURNAL)?.to_string();
    let stats_journal = maybe_one(set, crate::STATS_JOURNAL)?.to_string();

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

/// Extract a shard's templated ID prefix.
pub fn id_prefix<'n>(name: &'n str) -> Option<&'n str> {
    name.rsplitn(2, "/").skip(1).next()
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
            logs_journal: "logs/journal".to_string(),
            stats_journal: "stats/journal".to_string(),
        };

        let set = encode_labeling(LabelSet::default(), &labeling);

        insta::assert_json_snapshot!(set, @r###"
        {
          "labels": [
            {
              "name": "estuary.dev/build",
              "value": "a-build"
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
        "###);

        let id = format!("base/shard/id/{}", id_suffix(&set).unwrap());
        assert_eq!(id, "base/shard/id/00000100-00000000");
        assert_eq!(id_prefix(&id), Some("base/shard/id"));
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
            @r###"
        {
          "build": "a-build",
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
        "###
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
            @r###"
        {
          "build": "a-build",
          "logLevel": "info",
          "splitTarget": "split/target",
          "taskName": "the/task",
          "taskType": "capture"
        }
        "###
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
}
