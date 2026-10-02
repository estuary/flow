//! The lab's reference capture connector: a **fake** of source-postgres. It
//! never connects to a database. It simulates one, and emulates the protocol
//! effects of capturing it, as fast as its stdout allows:
//!
//! - Discovery of the endpoint configuration's declared tables (or a canned
//!   set of three), with schemas shaped as source-postgres generates them.
//! - Synthetic documents of each bound collection's top-level projections, so
//!   a binding of any collection whose fields it can generate gets that
//!   collection's document shapes. Validate fails on any it can't.
//! - `Opened` with explicit acknowledgements (which are read and ignored), a
//!   SourcedSchema of each binding, and `BackfillBegin` / `BackfillComplete`
//!   of each binding's backfill, each in a checkpoint of its own.
//! - Backfills read in ascending key order by chunk, each followed by the
//!   replication log written as it ran, in one checkpoint. Changes of rows
//!   beyond a backfill's cursor are dropped. Once backfills complete, each
//!   replication transaction is its own checkpoint.
//! - Merge-patch connector state shaped as sqlcapture's, from which a later
//!   session resumes exactly where the last committed.
//!
//! Output is deterministic: a pure function of the endpoint configuration's
//! `seed` and the state a session opens from. Natural points to change are
//! listed in each module (`sim`, `plan`, `wire`), and `config::canned_tables`.
//!
//! It speaks the protobuf codec only (its `local:` endpoint must set
//! `protobuf: true`), and joins the host's connectors cgroup.

mod config;
mod discover;
mod plan;
mod sim;
mod wire;

use anyhow::Context;
use proto_flow::capture::{Request, request, response};
use std::io::Read;

const CODEC: connector_init::Codec = connector_init::Codec::Proto;

fn main() -> std::process::ExitCode {
    match serve() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            let line = serde_json::json!({"level": "error", "msg": "capture-fake-postgres failed", "fields": {"error": format!("{err:#}")}});
            eprintln!("{line}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Serve unary requests until an Open, which then captures until stdin closes.
fn serve() -> anyhow::Result<()> {
    use std::os::fd::FromRawFd;

    runtime_lab::connector::enter_cgroup()?;
    // Owned outright, so the reader of Acknowledges can take it.
    let mut stdin = unsafe { std::fs::File::from_raw_fd(0) };
    let mut out = wire::Writer::stdout();
    let mut buffer = Vec::with_capacity(1 << 16);
    let mut chunk = vec![0u8; 1 << 16];

    loop {
        let n = stdin.read(&mut chunk).context("reading stdin")?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);

        for request in CODEC.decode::<Request>(&mut buffer)? {
            let kind = match request.kind.context("request sets no sub-message")? {
                request::Kind::Spec(_) => response::Kind::Spec(Box::new(spec())),
                request::Kind::Discover(discover) => {
                    let config = config::EndpointConfig::parse(&discover.config_json)?;
                    response::Kind::Discovered(discover::discovered(&config))
                }
                request::Kind::Validate(validate) => {
                    response::Kind::Validated(validated(&validate)?)
                }
                request::Kind::Apply(_) => response::Kind::Applied(response::Applied::default()),
                request::Kind::Open(open) => {
                    let (capture, schemas) = opened(*open)?;
                    std::thread::spawn(move || read_acknowledgements(stdin, buffer));
                    // Ends only when stdout fails, as when the runtime is done with it.
                    let err = capture.run(&mut out, schemas).unwrap_err();
                    return Err(err).context("writing stdout");
                }
                request::Kind::Acknowledge(_) => anyhow::bail!("Acknowledge before Open"),
            };
            out.message(kind).context("writing stdout")?;
        }
    }
}

/// Read Acknowledges until stdin closes, which ends the session. They're
/// otherwise ignored: source-postgres uses them only to advance its slot.
fn read_acknowledgements(mut stdin: std::fs::File, mut buffer: Vec<u8>) {
    let result = (|| -> anyhow::Result<()> {
        let mut chunk = vec![0u8; 1 << 12];
        loop {
            for request in CODEC.decode::<Request>(&mut buffer)? {
                match request.kind {
                    Some(request::Kind::Acknowledge(_)) => {}
                    other => anyhow::bail!("expected Acknowledge, not {other:?}"),
                }
            }
            let n = stdin.read(&mut chunk).context("reading stdin")?;
            if n == 0 {
                return Ok(());
            }
            buffer.extend_from_slice(&chunk[..n]);
        }
    })();

    match result {
        Ok(()) => std::process::exit(0),
        Err(err) => {
            let line = serde_json::json!({"level": "error", "msg": "capture-fake-postgres failed", "fields": {"error": format!("{err:#}")}});
            eprintln!("{line}");
            std::process::exit(1);
        }
    }
}

fn spec() -> response::Spec {
    let table = serde_json::json!({
        "type": "object",
        "required": ["namespace", "stream", "columns", "key", "rows"],
        "properties": {
            "namespace": {"type": "string"},
            "stream": {"type": "string"},
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "type"],
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string", "description": "bool, int2, int4, int8, float8, numeric, text, varchar(n), bytea, date, time, timestamp, timestamptz, uuid, or jsonb"},
                        "nullable": {"type": "boolean", "default": false},
                    },
                },
            },
            "key": {"type": "array", "items": {"type": "string"}},
            "rows": {"type": "integer", "minimum": 0},
            "changeWeight": {"type": "number", "minimum": 0, "default": 1.0},
        },
    });
    let config_schema = serde_json::json!({
        "type": "object",
        "title": "Lab fake Postgres capture",
        "properties": {
            "seed": {"type": "integer", "default": 0},
            "backfillChunkSize": {"type": "integer", "minimum": 1, "default": 50000},
            "txnsPerChunk": {"type": "integer", "minimum": 0, "default": 20},
            "changesPerTxn": {"type": "integer", "minimum": 1, "default": 10},
            "tables": {"type": "array", "items": table, "description": "Defaults to a canned set of three tables."},
        },
    });
    let resource_schema = serde_json::json!({
        "type": "object",
        "required": ["namespace", "stream"],
        "properties": {
            "namespace": {"type": "string", "x-schema-name": true},
            "stream": {"type": "string", "x-collection-name": true},
        },
    });

    response::Spec {
        protocol: 3032023,
        config_schema_json: config_schema.to_string().into(),
        resource_config_schema_json: resource_schema.to_string().into(),
        documentation_url: "https://github.com/estuary/flow/tree/master/crates/runtime-lab"
            .to_string(),
        resource_path_pointers: vec!["/namespace".to_string(), "/stream".to_string()],
        ..Default::default()
    }
}

fn validated(validate: &request::Validate) -> anyhow::Result<response::Validated> {
    let config = config::EndpointConfig::parse(&validate.config_json)?;
    let mut bindings = Vec::new();

    for binding in &validate.bindings {
        let resource: config::ResourceConfig =
            serde_json::from_slice(&binding.resource_config_json)
                .context("parsing resource configuration")?;
        let table = config.table(&resource)?;
        let collection = binding
            .collection
            .as_ref()
            .context("binding is missing its collection")?;
        plan::compile(collection, table)?;

        bindings.push(response::validated::Binding {
            resource_path: vec![resource.namespace, resource.stream],
        });
    }
    Ok(response::Validated { bindings })
}

/// The capture of an Open, and the SourcedSchema of each of its bindings.
fn opened(open: request::Open) -> anyhow::Result<(sim::Capture, Vec<serde_json::Value>)> {
    let spec = open.capture.context("Open is missing its capture")?;
    let config = config::EndpointConfig::parse(&spec.config_json)?;
    let state: sim::State = if open.state_json.is_empty() {
        sim::State::default()
    } else {
        serde_json::from_slice(&open.state_json).context("parsing connector state")?
    };
    let txn = match &state.cursor {
        Some(cursor) => {
            sim::parse_lsn(cursor).with_context(|| format!("invalid cursor {cursor:?}"))?
                / sim::LSN_STEP
        }
        None => 0,
    };

    let mut bindings = Vec::new();
    let mut schemas = Vec::new();
    for binding in &spec.bindings {
        let resource: config::ResourceConfig =
            serde_json::from_slice(&binding.resource_config_json)
                .context("parsing resource configuration")?;
        let table = config.table(&resource)?;
        let collection = binding
            .collection
            .as_ref()
            .context("binding is missing its collection")?;
        let plan = plan::compile(collection, table)?;

        let mut prior = state.bindings.get(&binding.state_key).cloned();
        if let Some(prior) = &mut prior {
            // A completed backfill's `false` is the residue of its patch.
            prior.metadata.backfill_complete = prior.metadata.backfill_complete.filter(|c| *c);
        }
        let new = prior.is_none();
        let state = prior.unwrap_or_else(|| sim::BindingState {
            mode: sim::Mode::Backfill,
            key_columns: table.key.clone(),
            scanned: None,
            backfilled: 0,
            metadata: sim::Metadata {
                next_key: table.rows,
                backfill_complete: None,
            },
        });

        schemas.push(discover::sourced_schema(table));
        bindings.push(sim::Binding {
            plan,
            state_key: binding.state_key.clone(),
            weight: (table.change_weight * 1000.0).round() as u64,
            state,
            new,
            dirty: false,
        });
    }

    let capture = sim::Capture {
        seed: config.seed,
        backfill_chunk_size: config.backfill_chunk_size,
        txns_per_chunk: config.txns_per_chunk,
        changes_per_txn: config.changes_per_txn,
        bindings,
        txn,
    };
    Ok((capture, schemas))
}

#[cfg(test)]
mod test {
    use super::*;
    use proto_flow::flow;

    /// A collection of `table` with the projections its discovered schema
    /// infers, as a build would produce.
    fn collection_of(table: &config::Table) -> flow::CollectionSpec {
        use config::ColumnType as T;

        let projections = table
            .columns
            .iter()
            .map(|column| {
                let type_ = T::parse(&column.type_).unwrap();
                let (types, format, encoding, max_length): (&[&str], &str, &str, u32) = match type_
                {
                    T::Bool => (&["boolean"], "", "", 0),
                    T::Int2 | T::Int4 | T::Int8 => (&["integer"], "", "", 0),
                    T::Float8 => (&["number", "string"], "number", "", 0),
                    T::Numeric => (&["string"], "number", "", 0),
                    T::Text => (&["string"], "", "", 0),
                    T::Varchar(n) => (&["string"], "", "", n),
                    T::Bytea => (&["string"], "", "base64", 0),
                    T::Date => (&["string"], "date", "", 0),
                    T::Time => (&["string"], "time", "", 0),
                    T::Timestamp | T::Timestamptz => (&["string"], "date-time", "", 0),
                    T::Uuid => (&["string"], "uuid", "", 0),
                    T::Jsonb => (
                        &["array", "boolean", "null", "number", "object", "string"],
                        "",
                        "",
                        0,
                    ),
                };
                let mut types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
                if column.nullable && !types.contains(&"null".to_string()) {
                    types.push("null".to_string());
                }
                flow::Projection {
                    ptr: format!("/{}", column.name),
                    field: column.name.clone(),
                    inference: Some(flow::Inference {
                        types,
                        string: Some(flow::inference::String {
                            format: format.to_string(),
                            content_encoding: encoding.to_string(),
                            max_length,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            })
            .collect();

        flow::CollectionSpec {
            name: format!("acmeCo/{}/{}", table.namespace, table.stream),
            key: table.key.iter().map(|k| format!("/{k}")).collect(),
            projections,
            ..Default::default()
        }
    }

    fn open_of(config: &serde_json::Value, state: Option<&serde_json::Value>) -> request::Open {
        let parsed = config::EndpointConfig::parse(config.to_string().as_bytes()).unwrap();
        let bindings = parsed
            .tables
            .iter()
            .map(|table| flow::capture_spec::Binding {
                resource_config_json:
                    serde_json::json!({"namespace": table.namespace, "stream": table.stream})
                        .to_string()
                        .into(),
                collection: Some(Box::new(collection_of(table))),
                state_key: format!("{}%2F{}", table.namespace, table.stream),
                ..Default::default()
            })
            .collect();

        request::Open {
            capture: Some(flow::CaptureSpec {
                name: "acmeCo/lab/pg".to_string(),
                config_json: config.to_string().into(),
                bindings,
                ..Default::default()
            }),
            state_json: state.map(|s| s.to_string().into()).unwrap_or_default(),
            ..Default::default()
        }
    }

    /// Run a capture of `open` until `limit` messages, rendered one per line.
    fn run_capture(open: request::Open, limit: usize) -> Vec<String> {
        use std::os::fd::FromRawFd;

        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (mut read, write) = unsafe {
            (
                std::fs::File::from_raw_fd(fds[0]),
                std::fs::File::from_raw_fd(fds[1]),
            )
        };
        let (capture, schemas) = opened(open).unwrap();
        let writer = std::thread::spawn(move || {
            let mut out = wire::Writer::new(write);
            // Fails once the reader below has what it wants, and hangs up.
            let _ = capture.run(&mut out, schemas);
        });

        let mut buffer = Vec::new();
        let mut lines = Vec::new();
        let mut chunk = vec![0u8; 1 << 16];
        while lines.len() < limit {
            let n = read.read(&mut chunk).unwrap();
            buffer.extend_from_slice(&chunk[..n]);
            for response in CODEC
                .decode::<proto_flow::capture::Response>(&mut buffer)
                .unwrap()
            {
                lines.push(render(response));
            }
        }
        std::mem::drop(read);
        writer.join().unwrap();
        lines.truncate(limit);
        lines
    }

    fn render(response: proto_flow::capture::Response) -> String {
        match response.kind.unwrap() {
            response::Kind::Opened(o) => format!("Opened {o:?}"),
            response::Kind::SourcedSchema(s) => format!("SourcedSchema {}", s.binding),
            response::Kind::Captured(c) => {
                let doc: serde_json::Value = serde_json::from_slice(&c.doc_json).unwrap();
                format!("Captured {} {doc}", c.binding)
            }
            response::Kind::Checkpoint(c) => match c.state {
                Some(state) => format!(
                    "Checkpoint {}",
                    String::from_utf8_lossy(&state.updated_json)
                ),
                None => "Checkpoint".to_string(),
            },
            response::Kind::BackfillBegin(b) => format!("BackfillBegin {}", b.binding),
            response::Kind::BackfillComplete(b) => format!("BackfillComplete {}", b.binding),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn small_config() -> serde_json::Value {
        let mut tables = serde_json::to_value(config::canned_tables()).unwrap();
        for (table, rows) in tables.as_array_mut().unwrap().iter_mut().zip([5, 4, 0]) {
            table["rows"] = rows.into();
        }
        serde_json::json!({
            "seed": 7,
            "backfillChunkSize": 3,
            "txnsPerChunk": 1,
            "changesPerTxn": 3,
            "tables": tables,
        })
    }

    #[test]
    fn output_stream_of_a_seed() {
        let lines = run_capture(open_of(&small_config(), None), 48);
        insta::assert_snapshot!(lines.join("\n"));
    }

    /// A session opened from the state of any checkpoint (mid-backfill, or
    /// after) continues with exactly what the original session emitted after it.
    #[test]
    fn resumed_sessions_continue_exactly() {
        let config = small_config();
        let original = run_capture(open_of(&config, None), 300);
        // A resumed session re-sends its start-up (Opened, a SourcedSchema of
        // each binding, and a Checkpoint), and then continues.
        let startup = 1 + 3 + 1;

        let mut state = serde_json::json!({});
        let mut resumed = 0;
        for (index, line) in original.iter().enumerate().take(100) {
            let Some(patch) = line.strip_prefix("Checkpoint ") else {
                continue;
            };
            merge(&mut state, serde_json::from_str(patch).unwrap());
            // A BackfillBegin's own checkpoint has no cursor yet.
            if state.get("cursor").is_none() {
                continue;
            }
            let next = run_capture(open_of(&config, Some(&state)), 60);
            for (offset, line) in next[startup..].iter().enumerate() {
                assert_eq!(
                    line,
                    &original[index + 1 + offset],
                    "resumed after line {index}, at offset {offset}"
                );
            }
            resumed += 1;
        }
        assert!(resumed > 10, "resumed only {resumed} times");
    }

    /// RFC 7396 merge patch.
    fn merge(target: &mut serde_json::Value, patch: serde_json::Value) {
        let serde_json::Value::Object(patch) = patch else {
            *target = patch;
            return;
        };
        if !target.is_object() {
            *target = serde_json::json!({});
        }
        let target = target.as_object_mut().unwrap();
        for (key, value) in patch {
            if value.is_null() {
                target.remove(&key);
            } else {
                merge(target.entry(key).or_insert(serde_json::Value::Null), value);
            }
        }
    }
}
