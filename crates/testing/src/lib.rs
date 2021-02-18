mod action;
mod clock;
mod graph;
mod run;

pub use action::{Action, Case};
pub use clock::Clock;
pub use graph::{Graph, PendingStat};
pub use run::run_test_case;

// Testing fixture builders used by sub-module unit tests.
#[cfg(test)]
mod tests {
    use super::{Clock, PendingStat};
    use models::{names, tables};
    use protocol::flow::test_spec::step::Type as TestStepType;
    use protocol::protocol::header::Etcd;

    pub fn transform_fixture(
        source: &str,
        transform: &str,
        derivation: &str,
        read_delay: u32,
    ) -> tables::Transform {
        tables::Transform {
            // Parameterized columns of the transform.
            derivation: names::Collection::new(derivation),
            read_delay_seconds: Some(read_delay),
            source_collection: names::Collection::new(source),
            transform: names::Transform::new(transform),

            // Stubbed / ignored columns.
            priority: 0,
            publish_lambda: None,
            rollback_on_register_conflict: false,
            scope: url::Url::parse("http://scope").unwrap(),
            shuffle_hash: protocol::flow::shuffle::Hash::None,
            shuffle_key: None,
            shuffle_lambda: None,
            source_partitions: None,
            source_schema: None,
            update_lambda: None,
        }
    }

    pub fn clock_fixture(revision: i64, journals: &[(&str, i64)]) -> Clock {
        Clock::new(
            &Etcd {
                revision,
                ..Default::default()
            },
            journals.iter().map(|(j, o)| (*j, o)),
        )
    }

    pub fn step_fixture(step_type: TestStepType, collection: &str) -> tables::TestStep {
        tables::TestStep {
            scope: url::Url::parse("http://scope").unwrap(),
            step_type,
            collection: names::Collection::new(collection),
            partitions: None,
            documents: vec![serde_json::Value::Bool(true)],
            step_index: 0,
            test: names::Test::new("A Test"),
        }
    }

    pub fn stat_fixture(ready_at: u64, derivation: &str) -> PendingStat {
        PendingStat {
            ready_at_seconds: ready_at,
            derivation: names::Collection::new(derivation),
        }
    }
}
