# Dekaf Tests

This directory contains tests for Dekaf, Estuary's Kafka-compatible interface for Flow collections.

## Test Categories

### Unit/Integration Tests (run without infrastructure)

- `field_extraction_tests.rs` - Tests for Avro schema generation and field extraction
- `dekaf_integration_test.rs` - Tests for document encoding, deletions, and field selection

These tests use snapshot testing (`insta`) and don't require external infrastructure.

### E2E Tests (require local stack)

E2E tests verify Dekaf behavior with real Kafka clients against the full Flow stack.

**Files:**

- `e2e_basic.rs` - Basic roundtrip: publish specs, inject documents, consume via Kafka
- `e2e_empty_fetch.rs` - Regression test for PR #1693 (empty fetch MessageSetSize)

**Infrastructure:**

- `e2e/mod.rs` - `DekafTestEnv` test harness for publishing fixtures and waiting for readiness
- `e2e/kafka.rs` - `KafkaConsumer` wrapper with Avro decoding, `KafkaConsumerBuilder` for custom config
- `e2e/fixtures/` - Flow YAML catalog fixtures

## Running Tests

### Unit/Integration Tests

```bash
cargo test -p dekaf
```

### E2E Tests

E2E tests are marked `#[ignore]` and require the local stack:

```bash
# Start local stack (in separate terminal)
mise run local:stack

# Run E2E tests
cargo test -p dekaf -- --ignored
```

Or run specific E2E tests:

```bash
cargo test -p dekaf e2e_empty_fetch -- --ignored
```

## Writing E2E Tests

### Test Structure

```rust
mod e2e;

use e2e::{kafka::snapshot_records, DekafTestEnv};
use serde_json::json;

const FIXTURE: &str = include_str!("e2e/fixtures/basic.flow.yaml");

#[ignore] // Requires local stack
#[tokio::test]
async fn test_something() -> anyhow::Result<()> {
    e2e::init_tracing();

    // DekafTestEnv::setup and inject_documents log automatically
    let env = DekafTestEnv::setup("test_name", FIXTURE).await?;
    env.inject_documents("data", vec![
        json!({"id": "1", "value": "hello"}),
    ]).await?;

    tracing::info!("Creating Kafka consumer");
    let consumer = env.kafka_consumer("test-token-12345");
    consumer.subscribe(&["test_topic"])?;

    // fetch() reads all available records until idle (2s timeout)
    tracing::info!("Fetching all available documents");
    let records = consumer.fetch().await?;
    tracing::info!(count = records.len(), "Received");

    // Use snapshots instead of manual assertions
    insta::assert_json_snapshot!(snapshot_records(&records));

    env.cleanup().await?;
    Ok(())
}
```

### Logging Guidelines

E2E tests run with `--no-capture` so output streams in real-time. Add `tracing::info!()` calls at significant steps:

- `DekafTestEnv::setup()` and `inject_documents()` log automatically
- Log before consumer creation and fetches
- Log results with structured fields: `tracing::info!(count = records.len(), "Received")`
- Keep messages concise: "Fetching 2 documents" not "Fetching documents via Dekaf"
- Don't log cleanup or other obvious bookkeeping

### Fetching and Snapshots

- `consumer.fetch()` reads all available records until no more arrive within 2 seconds
- This avoids hinting an expected count, which could hide bugs where extra documents are sent
- Use `snapshot_records(&records)` to create snapshot-friendly output (excludes non-deterministic fields like offset)
- Named snapshots help when a test has multiple fetch calls: `insta::assert_json_snapshot!("after_reset", snapshot_records(&records))`

### Custom Consumer Configuration

Use `kafka_consumer_builder()` for tests that need specific librdkafka settings:

```rust
let consumer = env
    .kafka_consumer_builder("test-token")
    .set("fetch.wait.max.ms", "100")
    .set("session.timeout.ms", "6000")
    .build();
```

### Fixtures

Fixtures are standard Flow YAML catalogs. Names are automatically rewritten to unique test namespaces (e.g., `test_data` becomes `test/dekaf/my_test/a1b2/test_data`).

Required components:

- **Collection** with schema and key
- **Capture** using `source-http-ingest` for document injection
- **Materialization** with `dekaf` endpoint

See `e2e/fixtures/basic.flow.yaml` for the standard pattern.

## Test Plan

The full test plan is documented in `../doc/testing/`:

- `TOP_10_REGRESSION_TESTS.md` - Priority regression tests from production issues
- `E2E_TEST_PLAN.md` - Comprehensive test coverage plan
- `IMPLEMENTATION_PLAN.md` - Implementation progress and deliverables

## Snapshots

Snapshot files are in `snapshots/`. After making changes that affect snapshots:

```bash
cargo insta test -p dekaf
cargo insta accept  # Review and accept changes
```
