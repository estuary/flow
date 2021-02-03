pub mod combine_api;
pub mod combiner;
pub mod context;
pub mod derive_api;
pub mod extract_api;
pub mod lambda;
pub mod nodejs;
pub mod pipeline;
pub mod registers;
pub mod schema_api;

pub use extract_api::extract_uuid_parts;
use std::sync::Once;

// TODO(johnny): Move to a common `instrument` crate?
/// Setup a global tracing subscriber using the RUST_LOG env variable.
pub fn setup_env_tracing() {
    static SUBSCRIBE: Once = Once::new();

    SUBSCRIBE.call_once(|| {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}

/// DebugJson is a new-type wrapper around any Serialize implementation
/// that wishes to support the Debug trait via JSON encoding itself.
pub struct DebugJson<S: serde::Serialize>(pub S);

impl<S: serde::Serialize> std::fmt::Debug for DebugJson<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(&self.0).unwrap())
    }
}

/// Common test utilities used by sub-modules.
#[cfg(test)]
pub mod test {
    use doc;
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
}
