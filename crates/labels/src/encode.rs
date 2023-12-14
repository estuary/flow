use crate::set_value;
use proto_flow::{flow, ops};
use proto_gazette::broker::LabelSet;

// Encode a ShardLabeling into a LabelSet.
pub fn shard_labeling(set: &mut LabelSet, labeling: &ops::ShardLabeling) {
    set_value(set, crate::BUILD, &labeling.build);

    if !labeling.hostname.is_empty() {
        set_value(set, crate::HOSTNAME, &labeling.hostname);
    }
    set_value(set, crate::LOG_LEVEL, labeling.log_level().as_str_name());

    if let Some(range) = &labeling.range {
        range_spec(set, range);
    }

    if !labeling.split_source.is_empty() {
        set_value(set, crate::SPLIT_SOURCE, &labeling.split_source);
    }
    if !labeling.split_target.is_empty() {
        set_value(set, crate::SPLIT_TARGET, &labeling.split_target);
    }

    set_value(set, crate::TASK_NAME, &labeling.task_name);
    set_value(set, crate::TASK_TYPE, labeling.task_type().as_str_name());
}

/// Encode a RangeSpec into a LabelSet.
pub fn range_spec(set: &mut LabelSet, spec: &flow::RangeSpec) {
    let fmt = |v: u32| format!("{v:08x}");

    set_value(set, crate::KEY_BEGIN, &fmt(spec.key_begin));
    set_value(set, crate::KEY_END, &fmt(spec.key_end));
    set_value(set, crate::RCLOCK_BEGIN, &fmt(spec.r_clock_begin));
    set_value(set, crate::RCLOCK_END, &fmt(spec.r_clock_end));
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encoding() {
        let labeling = ops::ShardLabeling {
            build: "a-build".to_string(),
            hostname: "a.hostname".to_string(),
            log_level: ops::log::Level::Info as i32,
            ports: Vec::new(),
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
        };

        let mut set = LabelSet::default();
        shard_labeling(&mut set, &labeling);

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
    }
}
