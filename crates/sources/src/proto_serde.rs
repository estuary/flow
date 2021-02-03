use protocol::{flow, protocol as broker};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

// This module is somewhat custom Serde & JSON-Schema wrapper generation
// for protobuf types. This is a one-way conversion: we parse these structures,
// and then immediately convert via into_proto(). They implement Serialize
// only because the JsonSchema trait requires it.

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "Label::example")]
pub struct Label {
    /// # Name of the Label.
    pub name: String,
    /// # Value of the Label.
    /// When used within a selector, if value is empty or omitted than
    /// the label selection matches any value.
    #[serde(default)]
    pub value: String,
}

impl Label {
    fn into_proto(self) -> broker::Label {
        let Self { name, value } = self;
        broker::Label { name, value }
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = "LabelSet::example")]
pub struct LabelSet {
    /// Labels of the set.
    #[serde(deserialize_with = "LabelSet::deser_labels")]
    pub labels: Vec<Label>,
}

impl LabelSet {
    fn deser_labels<'de, D>(d: D) -> Result<Vec<Label>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut labels = Vec::<Label>::deserialize(d)?;
        // Establish sorted order invariant.
        labels.sort_by(|lhs, rhs| (&lhs.name, &lhs.value).cmp(&(&rhs.name, &rhs.value)));
        Ok(labels)
    }

    fn into_proto(self) -> broker::LabelSet {
        let Self { labels } = self;
        broker::LabelSet {
            labels: labels.into_iter().map(Label::into_proto).collect(),
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
#[schemars(example = "LabelSelector::example")]
pub struct LabelSelector {
    /// # Included labels of the selector.
    #[serde(default)]
    pub include: LabelSet,
    /// # Excluded labels of the selector.
    #[serde(default)]
    pub exclude: LabelSet,
}

impl LabelSelector {
    fn into_proto(self) -> broker::LabelSelector {
        let Self { include, exclude } = self;
        let include = Some(include.into_proto());
        let exclude = Some(exclude.into_proto());
        broker::LabelSelector { include, exclude }
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "JournalSpec::example")]
pub struct JournalSpec {
    /// # Desired replication of journals.
    /// The default value is three, meaning that three machines in two deployment
    /// zones must fail before data loss can occur.
    #[serde(default)]
    pub replication: i32,
    /// # Assigned journal labels.
    /// Labels are a multi-map, where each label name may include multiple values.
    #[serde(default)]
    pub labels: LabelSet,
    /// # Fragment defines how journal content is mapped to fragment files.
    #[serde(default)]
    pub fragment: JournalSpecFragment,
    /// # Maximum rate, in bytes-per-second, at which data may be written to a journal.
    /// If zero, no journal-specific rate limiting is applied.
    #[serde(default)]
    pub max_append_rate: i64,
}

impl JournalSpec {
    fn into_proto(self) -> broker::JournalSpec {
        let Self {
            replication,
            labels,
            fragment,
            max_append_rate,
        } = self;

        broker::JournalSpec {
            name: String::new(),
            replication,
            labels: Some(labels.into_proto()),
            fragment: Some(fragment.into_proto()),
            flags: 0,
            max_append_rate,
        }
    }
}

#[derive(Deserialize, Serialize, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "JournalSpecFragment::example")]
pub struct JournalSpecFragment {
    /// # Desired byte content length of each fragment, before compression.
    /// When a journal fragment reaches this threshold, it will be closed off
    /// and a new one started, making its way to cloud storage.
    #[serde(default)]
    pub length: i64,
    /// # Codec used to compress Journal Fragments.
    #[serde(default)]
    pub compression_codec: CompressionCodec,
    /// # Storage backend base path for this Journal's Fragments.
    /// Must be in URL form, with the choice of backend defined by the scheme.
    /// The persisted path of a journal fragment is determined by joining the
    /// current store path with the journal name, and finally a content-address
    /// fragment file name.
    ///
    /// Eg, given a store of
    ///   "s3://My-AWS-bucket/a/prefix" and a journal of name "my/journal",
    /// a complete persisted path might be:
    ///   "s3://My-AWS-bucket/a/prefix/my/journal/000123-000456-789abcdef.gzip
    ///
    /// Multiple stores may be specified, and all stores are reguarly scanned
    /// to index applicable journal fragments. New fragments are always persisted
    /// to the first store in the list.
    ///
    /// This can be helpful in performing bucket migrations: adding a new store
    /// to the front of the list causes ongoing data to be written to that location,
    /// while historical data continues to be read and served from the prior stores.
    ///
    /// At least one store must be specified.
    #[serde(default)]
    pub stores: Vec<String>,
    /// # Period between refreshes of fragment listings from configured stores.
    #[serde(default)]
    pub refresh_interval: std::time::Duration,
    /// # Duration for which historical data of a journal should be kept.
    /// If zero, then fragments are retained indefinitely.
    #[serde(default)]
    pub retention: std::time::Duration,
    /// # Maximum flush delay before in-progress fragments are closed and persisted into cloud storage.
    /// Intervals are converted into uniform time segments: 24h will "roll" all fragments at
    /// midnight UTC every day, 1h at the top of every hour, 15m a :00, :15, :30, :45 past the
    /// hour, and so on.
    #[serde(default)]
    pub flush_interval: std::time::Duration,
    /// # Path postfix template evaluates to a partial path under which fragments are persisted to the store.
    #[serde(default)]
    pub path_postfix_template: String,
}

impl JournalSpecFragment {
    fn into_proto(self) -> broker::journal_spec::Fragment {
        let JournalSpecFragment {
            length,
            compression_codec,
            stores,
            refresh_interval,
            retention,
            flush_interval,
            path_postfix_template,
        } = self;

        broker::journal_spec::Fragment {
            length,
            compression_codec: compression_codec.into_proto(),
            stores,
            refresh_interval: Some(refresh_interval.into()),
            retention: Some(retention.into()),
            flush_interval: Some(flush_interval.into()),
            path_postfix_template,
        }
    }
}

#[derive(Deserialize, Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(example = "CompressionCodec::example")]
pub enum CompressionCodec {
    Invalid,
    None,
    Gzip,
    Zstandard,
    Snappy,
    GzipOffloadDecompression,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::Invalid
    }
}

impl CompressionCodec {
    fn into_proto(self) -> i32 {
        use broker::CompressionCodec as Out;
        match self {
            CompressionCodec::Invalid => Out::Invalid as i32,
            CompressionCodec::None => Out::None as i32,
            CompressionCodec::Gzip => Out::Gzip as i32,
            CompressionCodec::Zstandard => Out::Zstandard as i32,
            CompressionCodec::Snappy => Out::Snappy as i32,
            CompressionCodec::GzipOffloadDecompression => Out::GzipOffloadDecompression as i32,
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(example = "JournalRule::example")]
pub struct JournalRule {
    #[serde(skip)]
    pub rule: String,
    /// # Selector which determines whether the rule applies.
    #[serde(default)]
    pub selector: LabelSelector,
    /// # Template applied to the journal's specification.
    pub template: JournalSpec,
}

impl JournalRule {
    pub fn into_proto(self) -> flow::journal_rules::Rule {
        let Self {
            rule,
            selector,
            template,
        } = self;
        flow::journal_rules::Rule {
            rule,
            selector: Some(selector.into_proto()),
            template: Some(template.into_proto()),
        }
    }
}
