use lazy_static::lazy_static;
use protocol::protocol as broker;
use regex::Regex;
use schemars::JsonSchema;
use serde::{de::Error as SerdeError, Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use validator::Validate;

// This module holds project-wide, type-safe wrappers, enums, and *very* simple
// structures which identify or name Flow concepts, and must be referenced from
// multiple different crates.

/// Collection names consist of Unicode letters, numbers, and symbols: - _ . /
///
/// Spaces and other special characters are disallowed.
#[derive(
    Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[schemars(example = "Collection::example")]
pub struct Collection(#[schemars(schema_with = "Collection::schema")] String);

impl Collection {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Collection {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Transform::example")]
pub struct Transform(String);

impl Transform {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Transform {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Capture names a Flow capture.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Capture::example")]
pub struct Capture(String);

impl Capture {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Capture {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Materialization names a Flow materialization.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Materialization::example")]
pub struct Materialization(String);

impl Materialization {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Materialization {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Test names a Flow catalog test.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Test::example")]
pub struct Test(String);

impl Test {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Test {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// Rule names a specification rule.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "Rule::example")]
pub struct Rule(String);

impl Rule {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl std::ops::Deref for Rule {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

/// JSON Pointer which identifies a location in a document.
#[derive(Serialize, Debug, Clone, JsonSchema, PartialEq, Eq, PartialOrd, Ord)]
#[schemars(example = "JsonPointer::example")]
pub struct JsonPointer(#[schemars(schema_with = "JsonPointer::schema")] String);

impl JsonPointer {
    pub fn new(ptr: impl Into<String>) -> Self {
        Self(ptr.into())
    }
}

impl std::ops::Deref for JsonPointer {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for JsonPointer {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for JsonPointer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        if !s.is_empty() && !s.starts_with("/") {
            Err(D::Error::custom(
                "non-empty JSON pointer must begin with '/'",
            ))
        } else {
            Ok(JsonPointer(s))
        }
    }
}

/// Ordered JSON-Pointers which define how a composite key may be extracted from
/// a collection document.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[schemars(example = "CompositeKey::example")]
pub struct CompositeKey(Vec<JsonPointer>);

impl CompositeKey {
    pub fn new(parts: impl Into<Vec<JsonPointer>>) -> Self {
        Self(parts.into())
    }
    pub fn example() -> Self {
        CompositeKey(vec![JsonPointer::example()])
    }
}

impl std::ops::Deref for CompositeKey {
    type Target = Vec<JsonPointer>;

    fn deref(&self) -> &Vec<JsonPointer> {
        &self.0
    }
}

/// Object is an alias for a JSON object.
pub type Object = serde_json::Map<String, Value>;

/// Lambdas are user functions which are invoked by the Flow runtime to
/// process and transform source collection documents into derived collections.
/// Flow supports multiple lambda run-times, with a current focus on TypeScript
/// and remote HTTP APIs.
///
/// TypeScript lambdas are invoked within on-demand run-times, which are
/// automatically started and scaled by Flow's task distribution in order
/// to best co-locate data and processing, as well as to manage fail-over.
///
/// Remote lambdas may be called from many Flow tasks, and are up to the
/// API provider to provision and scale.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Lambda::example_typescript")]
#[schemars(example = "Lambda::example_remote")]
pub enum Lambda {
    Typescript,
    Remote(String),
}

/// Partition selectors identify a desired subset of the
/// available logical partitions of a collection.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "PartitionSelector::example")]
pub struct PartitionSelector {
    /// Partition field names and corresponding values which must be matched
    /// from the Source collection. Only documents having one of the specified
    /// values across all specified partition names will be matched. For example,
    ///   source: [App, Web]
    ///   region: [APAC]
    /// would mean only documents of 'App' or 'Web' source and also occurring
    /// in the 'APAC' region will be processed.
    #[serde(default)]
    pub include: BTreeMap<String, Vec<Value>>,
    /// Partition field names and values which are excluded from the source
    /// collection. Any documents matching *any one* of the partition values
    /// will be excluded.
    #[serde(default)]
    pub exclude: BTreeMap<String, Vec<Value>>,
}

// TODO(johnny): I've dumped a bunch of new models here for the moment,
// but plan to refactor these and models in the `sources` crate into
// the estuary/protocols repo, once behavior changes are wrapped up.

#[derive(Deserialize, Debug, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(example = "CompressionCodec::example")]
pub enum CompressionCodec {
    None,
    Gzip,
    Zstandard,
    Snappy,
    GzipOffloadDecompression,
}

impl CompressionCodec {
    pub fn into_proto(self) -> i32 {
        use broker::CompressionCodec as Out;
        let out = match self {
            CompressionCodec::None => Out::None,
            CompressionCodec::Gzip => Out::Gzip,
            CompressionCodec::Zstandard => Out::Zstandard,
            CompressionCodec::Snappy => Out::Snappy,
            CompressionCodec::GzipOffloadDecompression => Out::GzipOffloadDecompression,
        };
        out as i32
    }
}

#[derive(Deserialize, Debug, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(example = "BucketType::example")]
pub enum BucketType {
    ///# Google Cloud Storage.
    Gcs,
    ///# Amazon Simple Storage Service.
    S3,
    ///# Azure object storage service.
    Azure,
}

// TODO(johnny): Regex validations that currently live in the `validation`
// crate will consolidated and applied as #[validate(regex)] field annotations.
// This also allows schemars to generate schemas with these patterns.
const TOKEN: &'static str = r"[\p{Letter}\p{Digit}\-_\.]+";

lazy_static! {
    // CATALOG_NAME_RE is components of unicode letters and numbers with a strict
    // subset of other allowed punctuation symbols, joined by a single '/'.
    // Compare to Gazette's ValidateToken and TokenSymbols:
    // https://github.com/gazette/core/blob/master/broker/protocol/validator.go#L52
    static ref CATALOG_NAME_RE: Regex = Regex::new(&["^", TOKEN, "(/", TOKEN, ")*", "$"].concat()).unwrap();
    // CATALOG_PREFIX_RE is components of CATALOG_NAME_RE, ending with a final '/'.
    // Note that CATALOG_NAME_RE is *not* allowed to end in '/'.
    static ref CATALOG_PREFIX_RE: Regex = Regex::new( &["^", "(", TOKEN, "/)*", "$"].concat()).unwrap();
    // BUCKET_RE matches a cloud provider bucket. Simplified from (look-around removed):
    // https://stackoverflow.com/questions/50480924/regex-for-s3-bucket-name
    static ref BUCKET_RE: Regex =
        Regex::new(r#"(^(([a-z0-9]|[a-z0-9][a-z0-9\-]*[a-z0-9])\.)*([a-z0-9]|[a-z0-9][a-z0-9\-]*[a-z0-9])$)"#).unwrap();
}

#[cfg(test)]
mod test {
    use super::{BUCKET_RE, CATALOG_NAME_RE, CATALOG_PREFIX_RE};

    #[test]
    fn test_regexes() {
        for (case, expect) in [
            ("valid", true),
            ("valid/1", true),
            ("valid/one/va_lid", true),
            ("valid-1/valid/2/th.ree", true),
            ("Приключения/Foo", true),
            ("/bad/leading/slash", false),
            ("bad/trailing/slash/", false),
            ("bad-middle//slash", false),
            ("", false),
            ("a-bad/sp ace", false),
            ("/", false),
        ] {
            assert!(CATALOG_NAME_RE.is_match(case) == expect);
        }

        for (case, expect) in [
            ("valid/", true),
            ("valid/1/", true),
            ("valid/one/va_lid/", true),
            ("valid-1/valid/2/th.ree/", true),
            ("Приключения/Foo/", true),
            ("/bad/leading/slash", false),
            ("bad-middle//slash", false),
            ("", true),
            ("a-bad/sp ace/", false),
            ("/", false),
        ] {
            assert!(CATALOG_PREFIX_RE.is_match(case) == expect);
        }

        for (case, expect) in [
            ("foo.bar.baz", true),
            ("foo-bar-baz", true),
            ("foo/bar/baz", false),
            ("Foo.Bar.Baz", false),
        ] {
            assert!(BUCKET_RE.is_match(case) == expect);
        }
    }
}

/// A Store into which Flow journal fragments may be written.
///
/// The persisted path of a journal fragment is determined by composing the
/// Store's bucket and prefix with the journal name and a content-addressed
/// fragment file name.
///
/// Eg, given a Store to S3 with bucket "my-bucket" and prefix "a/prefix",
/// along with a collection "example/events" having a logical partition "region",
/// then a complete persisted path might be:
///
///   s3://my-bucket/a/prefix/example/events/region=EU/utc_date=2021-10-25/utc_hour=13/000123-000456-789abcdef.gzip
///
#[derive(Serialize, Deserialize, Debug, JsonSchema, Validate)]
#[schemars(example = "Store::example")]
pub struct Store {
    /// Cloud storage provider.
    pub provider: BucketType,
    /// Bucket into which Flow will store data.
    #[validate(regex = "BUCKET_RE")]
    pub bucket: String,
    /// Optional prefix of keys written to the bucket.
    #[validate(regex = "CATALOG_PREFIX_RE")]
    #[serde(default)]
    pub prefix: Option<String>,
}

impl Store {
    pub fn to_url(&self) -> url::Url {
        let scheme = match self.provider {
            BucketType::Azure => "azure",
            BucketType::Gcs => "gs",
            BucketType::S3 => "s3",
        };
        let prefix = self.prefix.as_ref().map(String::as_str).unwrap_or("");
        url::Url::parse(&format!("{}://{}/{}", scheme, self.bucket, prefix))
            .expect("parsing as URL should never fail")
    }
}

/// A StorageMapping relates a prefix of the entity namespace,
/// such as acmeCo/sales/widgets/, to a backing cloud storage location.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Validate)]
// #[schemars(example = "StorageMapping::example_absolute")]
pub struct StorageMapping {
    // Catalog prefix to which this storage mapping applies.
    #[validate(regex = "CATALOG_PREFIX_RE")]
    #[serde(default)]
    pub prefix: String,
    /// # Stores for journal fragments under this prefix.
    ///
    /// Multiple stores may be specified, and all stores are periodically scanned
    /// to index applicable journal fragments. New fragments are always persisted
    /// to the first store in the list.
    ///
    /// This can be helpful in performing bucket migrations: adding a new store
    /// to the front of the list causes ongoing data to be written to that location,
    /// while historical data continues to be read and served from the prior stores.
    ///
    /// When running `flowctl test`, stores are ignored and a local temporary
    /// directory is used instead.
    #[validate]
    pub stores: Vec<Store>,
}

/// Configuration for fragment files produced by a collection.
// path_postfix_template and refresh_interval are deliberately not
// exposed here. We're fixing these values in place for now.
#[derive(Serialize, Deserialize, Debug, Default, JsonSchema, Validate, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "FragmentTemplate::example")]
pub struct FragmentTemplate {
    /// # Desired content length of each fragment, in megabytes before compression.
    /// When a collection journal fragment reaches this threshold, it will be
    /// closed off and pushed to cloud storage.
    /// If not set, a default of 512MB is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 32, max = 4096))]
    pub length: Option<u32>,
    /// # Codec used to compress Journal Fragments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_codec: Option<CompressionCodec>,
    /// # Duration for which historical fragments of a collection should be kept.
    /// If not set, then fragments are retained indefinitely.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "FragmentTemplate::duration_schema")]
    pub retention: Option<std::time::Duration>,
    /// # Maximum flush delay before in-progress fragments are closed and persisted into cloud storage.
    /// Intervals are converted into uniform time segments: 24h will "roll" all fragments at
    /// midnight UTC every day, 1h at the top of every hour, 15m a :00, :15, :30, :45 past the
    /// hour, and so on.
    /// If not set, then fragments are not flushed on time-based intervals.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "FragmentTemplate::duration_schema")]
    pub flush_interval: Option<std::time::Duration>,
}

#[derive(Serialize, Deserialize, Debug, Default, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "JournalTemplate::example")]
pub struct JournalTemplate {
    /// # Fragment configuration of collection journals.
    pub fragments: FragmentTemplate,
}

#[derive(Serialize, Deserialize, Debug, Default, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "ShardTemplate::example")]
pub struct ShardTemplate {
    /// # Disable processing of the task's shards.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disable: bool,
    /// # Minimum duration of task transactions.
    /// This duration lower-bounds the amount of time during which a transaction
    /// must process documents before it must flush and commit.
    /// It may run for more time if additional documents are available.
    /// The default value is zero seconds.
    /// Larger values may result in more data reduction, at the cost of
    /// more latency.
    /// EXPERIMENTAL: this field MAY be removed.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "FragmentTemplate::duration_schema")]
    pub min_txn_duration: Option<std::time::Duration>,
    /// # Maximum duration of task transactions.
    /// This duration upper-bounds the amount of time during which a transaction
    /// may process documents before it must flush and commit.
    /// It may run for less time if there aren't additional ready documents for
    /// it to process.
    /// If not set, the maximum duration defaults to one second.
    /// Some tasks, particularly materializations to large analytic warehouses
    /// like Snowflake, may benefit from a longer duration such as thirty seconds.
    /// EXPERIMENTAL: this field MAY be removed.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "FragmentTemplate::duration_schema")]
    pub max_txn_duration: Option<std::time::Duration>,
    /// # Number of hot standbys to keep for each task shard.
    /// Hot standbys of a shard actively replicate the shard's state to another
    /// machine, and are able to be quickly promoted to take over processing for
    /// the shard should its current primary fail.
    /// If not set, then no hot standbys are maintained.
    /// EXPERIMENTAL: this field MAY be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_standbys: Option<u32>,
    /// # Size of the ring buffer used to sequence documents for exactly-once semantics.
    /// The ring buffer is a performance optimization only:
    /// catalog tasks will replay portions of journals as
    /// needed when messages aren't available in the buffer.
    /// It can remain small if upstream task transactions are small,
    /// but larger transactions will achieve better performance with a
    /// larger ring.
    /// If not set, a reasonable default (currently 65,536) is used.
    /// EXPERIMENTAL: this field is LIKELY to be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ring_buffer_size: Option<u32>,
    /// # Size of the reader channel used for decoded documents.
    /// Larger values are recommended for tasks having more than one
    /// shard split and long, bursty transaction durations.
    /// If not set, a reasonable default (currently 65,536) is used.
    /// EXPERIMENTAL: this field is LIKELY to be removed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_channel_size: Option<u32>,
    /// # Log level of this tasks's shards.
    /// Log levels may currently be "error", "warn", "info", "debug", or "trace".
    /// If not set, the effective log level is "info".
    // NOTE(johnny): We're not making this an enum because it's likely
    // we'll introduce a modular logging capability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_level: Option<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}
