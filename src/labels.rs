pub mod keys {
    pub const MANAGED_BY: &str = "app.gazette.dev/managed-by";
    pub const CATALOG_URL: &str = "estuary.dev/catalog-url";
    pub const DERIVATION: &str = "estuary.dev/derivation";
    pub const COLLECTION: &str = "estuary.dev/collection";
    pub const MATERIALIZATION_TARGET: &str = "estuary.dev/materialization-target";
    pub const MATERIALIZATION_TABLE_NAME: &str = "estuary.dev/materialization-table";
    pub const KEY_BEGIN: &str = "estuary.dev/key-begin";
    pub const KEY_END: &str = "estuary.dev/key-end";
    pub const RCLOCK_BEGIN: &str = "estuary.dev/rclock-begin";
    pub const RCLOCK_END: &str = "estuary.dev/rclock-end";
}

pub mod values {
    pub const FLOW: &str = "estuary.dev/flow";

    pub const DEFAULT_RCLOCK_BEGIN: &str = "0000000000000000";
    pub const DEFAULT_RCLOCK_END: &str = "ffffffffffffffff";

    pub const DEFAULT_KEY_BEGIN: &str = "00";
    pub const DEFAULT_KEY_END: &str = "ffffffff";
}

#[macro_export]
macro_rules! label_set {
    ($($key:expr => $value:expr),* $(,)*) => {{
        let mut labels: Vec<estuary_protocol::protocol::Label> = Vec::new();
        $(
            labels.push(estuary_protocol::protocol::Label {
                name: String::from($key),
                value: String::from($value),
            });
         )*
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        estuary_protocol::protocol::LabelSet { labels }
    }}
}
