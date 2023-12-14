use proto_flow::{flow, ops};
use proto_gazette::broker::{Label, LabelSet};

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
}

pub type Result<Ok> = std::result::Result<Ok, Error>;

/// Parse a LabelSet attached to a Flow Shard into its ShardLabeling.
pub fn shard_labeling(set: &LabelSet) -> Result<ops::ShardLabeling> {
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
        Some(range_spec(set)?)
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
    })
}

/// Returns true if the LabelSet encodes a RangeSpec.
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

/// Parse a LabelSet attached to a Flow Shard into its RangeSpec.
pub fn range_spec(set: &LabelSet) -> Result<flow::RangeSpec> {
    Ok(flow::RangeSpec {
        key_begin: expect_one_u32(set, crate::KEY_BEGIN)?,
        key_end: expect_one_u32(set, crate::KEY_END)?,
        r_clock_begin: expect_one_u32(set, crate::RCLOCK_BEGIN)?,
        r_clock_end: expect_one_u32(set, crate::RCLOCK_END)?,
    })
}

fn expect_one_u32(set: &LabelSet, name: &str) -> Result<u32> {
    let value = expect_one(set, name)?;

    let (8, Ok(parsed)) = (value.len(), u32::from_str_radix(value, 16)) else {
        return Err(Error::InvalidValue {
            name: name.to_string(),
            value: value.to_string(),
        });
    };
    Ok(parsed)
}

fn expect_one<'s>(set: &'s LabelSet, name: &str) -> Result<&'s str> {
    let labels = super::values(set, name);

    if labels.len() != 1 {
        return Err(Error::ExpectedOne(name.to_string(), labels.to_vec()));
    } else if labels[0].value.is_empty() {
        return Err(Error::ValueEmpty(name.to_string()));
    } else {
        Ok(labels[0].value.as_str())
    }
}

fn maybe_one<'s>(set: &'s LabelSet, name: &str) -> Result<&'s str> {
    let labels = super::values(set, name);

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

#[cfg(test)]
mod test {
    use super::shard_labeling;
    use crate::build_set;

    #[test]
    fn test_parsing_cases() {
        let case = |set| match shard_labeling(&set) {
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
        ]);

        insta::assert_json_snapshot!(
            case(model.clone()),
            @r###"
        {
          "build": "a-build",
          "hostname": "a.hostname",
          "logLevel": "info",
          "range": {
            "keyBegin": 1,
            "keyEnd": 2,
            "rClockBegin": 3,
            "rClockEnd": 4
          },
          "splitSource": "split/source",
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
        ] {
            crate::remove(&mut set, name);
        }
        crate::add_value(&mut set, crate::SPLIT_TARGET, "split/target");

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
        let mut set = model.clone();
        crate::remove(&mut set, crate::BUILD);
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/build (got [])""###);

        // Expected label has too many values.
        let mut set = model.clone();
        crate::add_value(&mut set, crate::BUILD, "other");
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/build (got [Label { name: \"estuary.dev/build\", value: \"a-build\" }, Label { name: \"estuary.dev/build\", value: \"other\" }])""###);

        // Invalid log level.
        let mut set = model.clone();
        crate::set_value(&mut set, crate::LOG_LEVEL, "invalid");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"invalid\" for label estuary.dev/log-level""###);

        // Invalid task type.
        let mut set = model.clone();
        crate::set_value(&mut set, crate::TASK_TYPE, "invalid");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"invalid\" for label estuary.dev/task-type""###);

        // Invalid hex range (not 8 bytes).
        let mut set = model.clone();
        crate::set_value(&mut set, crate::KEY_BEGIN, "0011");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"0011\" for label estuary.dev/key-begin""###);

        // Invalid hex range (not hex).
        let mut set = model.clone();
        crate::set_value(&mut set, crate::KEY_BEGIN, "0000000z");
        insta::assert_json_snapshot!(case(set),
            @r###""invalid value \"0000000z\" for label estuary.dev/key-begin""###);

        // Missing subset of RangeSpec.
        let mut set = model.clone();
        crate::remove(&mut set, crate::RCLOCK_BEGIN);
        insta::assert_json_snapshot!(case(set),
            @r###""expected one label for estuary.dev/rclock-begin (got [])""###);

        // Split source AND target.
        let mut set = model.clone();
        crate::add_value(&mut set, crate::SPLIT_TARGET, "split/target");
        insta::assert_json_snapshot!(case(set),
            @r###""both split-source split/source and split-target split/target are set but shouldn't be""###);
    }
}
