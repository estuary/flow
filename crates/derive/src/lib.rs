pub mod combine_api;
pub mod combiner;
pub mod context;
pub mod derive_api;
pub mod extract_api;
pub mod lambda;
pub mod nodejs;
pub mod pipeline;
pub mod registers;

pub use extract_api::extract_field;
pub use extract_api::extract_uuid_parts;

/// Common test utilities used by sub-modules.
#[cfg(test)]
pub mod test {
    use doc;
    use protocol::flow;
    use serde_json::json;
    use url::Url;

    pub use super::lambda::test::TestServer as LambdaTestServer;

    // Build a test schema fixture. Use gross Box::leak to coerce a 'static lifetime.
    pub fn build_min_max_schema() -> (&'static doc::SchemaIndex<'static>, Url) {
        let schema = json!({
            "properties": {
                "min": {
                    "type": "integer",
                    "reduce": {"strategy": "minimize"}
                },
                "max": {
                    "type": "number",
                    "reduce": {"strategy": "maximize"}
                },
            },
            "reduce": {"strategy": "merge"},
        });

        let uri = Url::parse("https://example/schema").unwrap();
        let scm: doc::Schema = json::schema::build::build_schema(uri.clone(), &schema).unwrap();
        let scm = Box::leak(Box::new(scm));

        let mut idx = doc::SchemaIndex::new();
        idx.add(scm).unwrap();
        idx.verify_references().unwrap();

        let idx = Box::leak(Box::new(idx));
        (idx, uri)
    }

    // Builds an empty RocksDB in a temporary directory,
    // initialized with the "registers" column family.
    pub fn build_test_rocks() -> (tempfile::TempDir, rocksdb::DB) {
        let dir = tempfile::TempDir::new().unwrap();

        let mut rocks_opts = rocksdb::Options::default();
        rocks_opts.create_if_missing(true);
        rocks_opts.set_error_if_exists(true);
        rocks_opts.create_missing_column_families(true);

        let db = rocksdb::DB::open_cf(
            &rocks_opts,
            dir.path(),
            [
                rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
                super::registers::REGISTERS_CF,
            ]
            .iter(),
        )
        .unwrap();

        (dir, db)
    }

    // Maps a flattened Field::Value message back into a JSON Value.
    pub fn field_to_value(arena: &[u8], field: &flow::field::Value) -> serde_json::Value {
        use flow::field::value::Kind;
        use serde_json::Value;
        let kind = flow::field::value::Kind::from_i32(field.kind).unwrap();

        match kind {
            Kind::Null => Value::Null,
            Kind::True => Value::Bool(true),
            Kind::False => Value::Bool(false),
            Kind::String => {
                let s = field.bytes.as_ref().unwrap();
                let b = arena.get(s.begin as usize..s.end as usize).unwrap();

                Value::String(std::str::from_utf8(b).unwrap().to_owned())
            }
            Kind::Unsigned => Value::Number(field.unsigned.into()),
            Kind::Signed => Value::Number(field.signed.into()),
            Kind::Double => serde_json::Number::from_f64(field.double)
                .map(|n| Value::Number(n))
                .unwrap(),
            Kind::Object | Kind::Array => {
                let s = field.bytes.as_ref().unwrap();
                let b = arena.get(s.begin as usize..s.end as usize).unwrap();

                serde_json::from_slice(b).unwrap()
            }
            _ => panic!("invalid field Kind"),
        }
    }
}
