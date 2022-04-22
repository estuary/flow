use super::references::*;

use lazy_static::lazy_static;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use validator::Validate;

/// BucketType is a provider of object storage buckets,
/// which are used to durably storage journal fragments.
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

impl BucketType {
    pub fn example() -> Self {
        BucketType::S3
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Validate)]
#[schemars(example = "Store::example")]
pub struct Store {
    /// Cloud storage provider.
    pub provider: BucketType,
    /// Bucket into which Flow will store data.
    #[validate(regex = "BUCKET_RE")]
    pub bucket: String,
    /// Optional prefix of keys written to the bucket.
    #[validate]
    #[serde(default)]
    pub prefix: Option<Prefix>,
}

impl Store {
    pub fn to_url(&self) -> url::Url {
        let scheme = match self.provider {
            BucketType::Azure => "azure",
            BucketType::Gcs => "gs",
            BucketType::S3 => "s3",
        };
        let prefix = self.prefix.as_ref().map(Prefix::as_str).unwrap_or("");
        url::Url::parse(&format!("{}://{}/{}", scheme, self.bucket, prefix))
            .expect("parsing as URL should never fail")
    }
    pub fn example() -> Self {
        Self {
            provider: BucketType::S3,
            bucket: "my-bucket".to_string(),
            prefix: None,
        }
    }
}

/// Storage defines the backing cloud storage for journals.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Validate)]
pub struct StorageDef {
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

impl StorageDef {
    pub fn example() -> Self {
        Self {
            stores: vec![Store::example()],
        }
    }
}

/// A CompressionCodec may be applied to compress journal fragments before
/// they're persisted to cloud stoage. The compression applied to a journal
/// fragment is included in its filename, such as ".gz" for GZIP. A
/// collection's compression may be changed at any time, and will affect
/// newly-written journal fragments.
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
    pub fn example() -> Self {
        CompressionCodec::GzipOffloadDecompression
    }
}

/// A FragmentTemplate configures how journal fragment files are
/// produced as part of a collection.
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
    #[schemars(schema_with = "super::duration_schema")]
    pub retention: Option<std::time::Duration>,
    /// # Maximum flush delay before in-progress fragments are closed and persisted
    /// into cloud storage. Intervals are converted into uniform time segments:
    /// 24h will "roll" all fragments at midnight UTC every day, 1h at the top of
    /// every hour, 15m a :00, :15, :30, :45 past the hour, and so on.
    /// If not set, then fragments are not flushed on time-based intervals.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "super::duration_schema")]
    pub flush_interval: Option<std::time::Duration>,
}

impl FragmentTemplate {
    pub fn example() -> Self {
        Self {
            compression_codec: Some(CompressionCodec::Zstandard),
            flush_interval: Some(Duration::from_secs(3600)),
            ..Default::default()
        }
    }
    pub fn is_empty(&self) -> bool {
        let FragmentTemplate {
            length: o1,
            compression_codec: o2,
            retention: o3,
            flush_interval: o4,
        } = self;

        o1.is_none() && o2.is_none() && o3.is_none() && o4.is_none()
    }
}

/// A JournalTemplate configures the journals which make up the
/// physical partitions of a collection.
#[derive(Serialize, Deserialize, Debug, Default, JsonSchema, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "JournalTemplate::example")]
pub struct JournalTemplate {
    /// # Fragment configuration of collection journals.
    pub fragments: FragmentTemplate,
}

impl JournalTemplate {
    pub fn example() -> Self {
        Self {
            fragments: FragmentTemplate::example(),
        }
    }
    pub fn is_empty(&self) -> bool {
        let JournalTemplate { fragments } = self;
        fragments.is_empty()
    }
}

lazy_static! {
    // BUCKET_RE matches a cloud provider bucket. Simplified from (look-around removed):
    // https://stackoverflow.com/questions/50480924/regex-for-s3-bucket-name
    static ref BUCKET_RE: Regex =
        Regex::new(r#"(^(([a-z0-9]|[a-z0-9][a-z0-9\-]*[a-z0-9])\.)*([a-z0-9]|[a-z0-9][a-z0-9\-]*[a-z0-9])$)"#).unwrap();
}

#[cfg(test)]
mod test {
    use super::BUCKET_RE;

    #[test]
    fn test_regexes() {
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
