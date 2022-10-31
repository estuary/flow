use crate::{pipeline::Pipeline, registers::Registers};
use prost::Message;
use proto_flow::flow::{
    derive_api::{self, Code, Config, DocHeader},
    CollectionSpec, DerivationSpec, LambdaSpec, TransformSpec,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tuple::{TupleDepth, TuplePack};

#[test]
fn test_pipeline_stats() {
    let mut fixture = FixtureBuilder::new("test-derive-pipeline")
        .key(&["/key"])
        .schema(json!({
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "sum": {"type": "integer", "reduce": {"strategy": "sum"}},
            },
            "reduce": {"strategy": "merge"}
        }))
        .add_transform()
        // Transform 0 should have both update and publish stats.
        // The outputs are doubled here so we can verify on the output stats.
        .update(|val| Ok(vec![val.clone(), val]))
        .publish(|_, _, reg| Ok(vec![reg.clone(), reg]))
        .end_transform()
        .add_transform()
        // The second transform is just a publish-only identity function
        .publish(|source, _, _| Ok(vec![source]))
        .end_transform()
        .add_transform()
        // The third transform only updates, no publish
        .update(|val| Ok(vec![val]))
        .end_transform()
        .finish();

    fixture.add_source_document(0, key("bee"), r#"{"key": "bee", "sum": 2}"#.as_bytes());
    fixture.add_source_document(0, key("bee"), r#"{"key": "bee", "sum": 1}"#.as_bytes());
    fixture.add_source_document(1, key("knee"), r#"{"key": "knee", "sum": 7}"#.as_bytes());
    fixture.add_source_document(1, key("bee"), r#"{"key": "bee", "sum": 66}"#.as_bytes());

    // These should only show up as 2 registers created
    fixture.add_source_document(2, key("tea"), r#"{"key": "tea", "sum": 9}"#.as_bytes());
    fixture.add_source_document(2, key("tea"), r#"{"key": "tea", "sum": 1}"#.as_bytes());
    fixture.add_source_document(2, key("free"), r#"{"key": "free", "sum": 42}"#.as_bytes());

    let (docs, stats) = fixture.poll_to_completion();
    // Redact the time output on the snapshot so that it's deterministic.
    insta::assert_yaml_snapshot!(stats, {
        ".**.totalSeconds" => "time-redacted",
    });
    assert_eq!(2, docs.len());
}

fn key(k: impl TuplePack) -> Vec<u8> {
    let mut out = Vec::new();
    k.pack(&mut out, TupleDepth::new().increment()).unwrap();
    out
}

/// A helper for testing a `Pipeline` by rosoving trampoline tasks (lambdas) using Rust functions.
struct Fixture {
    _temp_dir: TempDir,
    pipeline: Pipeline,
    transforms: Vec<TransformFixture>,
}

#[derive(Default)]
struct TransformFixture {
    update: Option<Box<dyn FnMut(Value) -> Result<Vec<Value>, ()>>>,
    /// publish lambda parameters are `(source, prev_register, new_register)`
    publish: Option<Box<dyn FnMut(Value, Value, Value) -> Result<Vec<Value>, ()>>>,
}

impl Fixture {
    fn add_source_document(&mut self, tf_index: usize, key: Vec<u8>, doc: &[u8]) {
        self.pipeline
            .add_source_document(
                DocHeader {
                    uuid: None,
                    packed_key: key,
                    transform_index: tf_index as u32,
                },
                doc,
            )
            .expect("failed to add document");
    }

    // Polls the pipeline and resolves all trampoline tasks. The final output documents and stats
    // are returned.
    fn poll_to_completion(mut self) -> (Vec<Value>, derive_api::Stats) {
        self.pipeline.flush();

        let mut arena = Vec::with_capacity(1024);
        let mut out = Vec::with_capacity(4);

        let mut i = 0;
        while !self
            .pipeline
            .poll_and_trampoline(&mut arena, &mut out)
            .expect("failed to poll pipeline")
        {
            i += 1;
            if i > 2 {
                panic!("pipeline did not poll to idle after 3 attempts");
            }
            for frame in out.iter() {
                assert_eq!(Some(Code::Trampoline), Code::from_i32(frame.code as i32));
                let b = &arena[frame.begin as usize + 12..frame.end as usize];
                let req = derive_api::Invoke::decode(b).expect("failed to decode Invoke");

                let req_sources =
                    unsafe { invocation_args::<Value>(req.sources_memptr, req.sources_length) };

                let mut req_registers = if req.registers_length > 0 {
                    Some(unsafe {
                        invocation_args::<Vec<Value>>(req.registers_memptr, req.registers_length)
                            .into_iter()
                    })
                } else {
                    None
                };

                let Fixture {
                    ref mut pipeline,
                    ref mut transforms,
                    ..
                } = self;
                let tf = &mut transforms[req.transform_index as usize];

                // Invoke all lambdas and collect the results
                let mut results: Vec<Result<Vec<Value>, ()>> = Vec::new();
                for source in req_sources {
                    // is this an update or a publish lambda?
                    if let Some(regs) = req_registers.as_mut() {
                        let mut reg_args = regs.next().expect("missing register args").into_iter();
                        let prev = reg_args.next().expect("missing prev register arg");
                        let next = reg_args.next().unwrap_or_default();
                        assert!(
                            reg_args.next().is_none(),
                            "there should be at most 2 register arguments"
                        );
                        let result = tf.publish.as_mut().expect("publish lambda must exist")(
                            source, prev, next,
                        );
                        results.push(result);
                    } else {
                        // This is an update lambda
                        let result = tf.update.as_mut().expect("update lambda must exist")(source);
                        results.push(result);
                    }
                }

                // Now we can build and send the response.
                // Start by copying the 64 bit request id into the response
                let mut response =
                    (&arena[frame.begin as usize..frame.begin as usize + 8]).to_vec();
                if let Ok(docs) = results.into_iter().collect::<Result<Vec<_>, ()>>() {
                    response.push(1); // OK
                    serde_json::to_writer(&mut response, &docs).unwrap();
                } else {
                    response.push(0); // !OK
                    response.extend_from_slice(b"intentional test error"); // will show up as the error message
                }
                pipeline.resolve_task(&response);
            }
            arena.clear();
            out.clear();
        }

        let mut more = true;
        while more {
            (self.pipeline, more) = self.pipeline.drain_chunk(1, &mut arena, &mut out).unwrap();
        }

        let mut outputs = Vec::new();
        for frame in out.iter() {
            if frame.code == Code::DrainedReducedDocument as u32
                || frame.code == Code::DrainedCombinedDocument as u32
            {
                let slice = &arena[frame.begin as usize..frame.end as usize];
                let doc: Value = serde_json::from_slice(slice).expect("failed to parse output doc");
                outputs.push(doc);
            }
        }

        let stats_bytes = out.last().expect("missing stats output");
        let stats =
            derive_api::Stats::decode(&arena[stats_bytes.begin as usize..stats_bytes.end as usize])
                .expect("failed to decode stats");
        (outputs, stats)
    }
}

unsafe fn invocation_args<T: serde::de::DeserializeOwned>(pointer: u64, len: u64) -> Vec<T> {
    let mut s = str_from(pointer, len);
    println!("Sources:");
    println!("{}", s);
    let mut out = Vec::new();
    while !s.is_empty() {
        let consumed = {
            let mut deser = serde_json::Deserializer::from_str(s).into_iter::<T>();
            let val = deser.next().expect("expected Some").expect("deser error");
            out.push(val);
            deser.byte_offset()
        };
        s = &s[consumed..];
        if !s.is_empty() {
            s = &s[1..]; // trim off the comma
        }
    }
    out
}

unsafe fn str_from(pointer: u64, len_bytes: u64) -> &'static str {
    let slice = std::slice::from_raw_parts(pointer as *const u8, len_bytes as usize);
    std::str::from_utf8_unchecked(slice)
}

struct TransformBuilder {
    fixture: FixtureBuilder,
    tf: TransformFixture,
}
impl TransformBuilder {
    fn update<F>(mut self, update: F) -> Self
    where
        F: FnMut(Value) -> Result<Vec<Value>, ()> + 'static,
    {
        self.tf.update = Some(Box::new(update));
        self
    }

    fn publish<F>(mut self, publish: F) -> Self
    where
        F: FnMut(Value, Value, Value) -> Result<Vec<Value>, ()> + 'static,
    {
        self.tf.publish = Some(Box::new(publish));
        self
    }
    fn end_transform(self) -> FixtureBuilder {
        let TransformBuilder { mut fixture, tf } = self;
        if tf.update.is_none() && tf.publish.is_none() {
            panic!("must provide an update or publish lambda");
        }
        fixture.transforms.push(tf);
        fixture
    }
}

struct FixtureBuilder {
    derivation: String,
    transforms: Vec<TransformFixture>,
    collection_schema: Value,
    // Future enhancement can add a function to set this, but for now it's not needed.
    register_schema: Value,
    key_ptrs: Vec<String>,
}
//update: Option<Box<dyn FnMut(Value) -> Result<Vec<Value>, ()>>>,
//publish: Option<Box<dyn FnMut(Value, Value, Value) -> Result<Vec<Value>, ()>>>,
impl FixtureBuilder {
    fn new(derivation: impl Into<String>) -> FixtureBuilder {
        FixtureBuilder {
            derivation: derivation.into(),
            transforms: Vec::with_capacity(2),
            collection_schema: Value::Null,
            register_schema: Value::Bool(true),
            key_ptrs: Vec::new(),
        }
    }
    fn key(mut self, ptrs: &[&str]) -> Self {
        self.key_ptrs = ptrs.iter().map(|p| p.to_string()).collect();
        self
    }
    fn schema(mut self, collection_schema: Value) -> Self {
        self.collection_schema = collection_schema;
        self
    }

    fn add_transform(self) -> TransformBuilder {
        TransformBuilder {
            fixture: self,
            tf: TransformFixture::default(),
        }
    }
    fn finish(self) -> Fixture {
        let FixtureBuilder {
            derivation,
            transforms,
            collection_schema,
            register_schema,
            key_ptrs,
        } = self;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let reg = Registers::new(rocksdb::Options::default(), temp_dir.path()).unwrap();

        let transform_specs = transforms
            .iter()
            .enumerate()
            .map(|(i, tf)| {
                TransformSpec {
                    derivation: derivation.clone(),
                    transform: format!("transform-{}", i),
                    // Future opportunity to add the ability to test shuffle keys
                    shuffle: None,
                    update_lambda: tf.update.as_ref().map(|_| LambdaSpec {
                        typescript: String::from("test-update-placeholder"),
                        remote: String::new(),
                    }),
                    publish_lambda: tf.publish.as_ref().map(|_| LambdaSpec {
                        typescript: String::from("test-publish-placeholder"),
                        remote: String::new(),
                    }),
                }
            })
            .collect();

        let config = Config {
            derivation: Some(DerivationSpec {
                register_schema_uri: "http://example/register.schema".to_string(),
                register_schema_json: register_schema.to_string(),
                collection: Some(CollectionSpec {
                    collection: derivation.clone(),
                    schema_uri: "http://example/collection.schema".to_string(),
                    schema_json: collection_schema.to_string(),
                    key_ptrs,
                    uuid_ptr: String::from("/_meta/uuid"),
                    partition_fields: Vec::new(),
                    projections: Vec::new(),
                    ack_json_template: String::new(),
                    partition_template: None,
                }),
                register_initial_json: String::from("{}"),
                shard_template: None,
                recovery_log_template: None,
                transforms: transform_specs,
            }),
        };

        let pipeline = Pipeline::from_config_and_parts(config, reg, 0)
            .expect("failed to create test pipeline");
        Fixture {
            pipeline,
            _temp_dir: temp_dir,
            transforms,
        }
    }
}
