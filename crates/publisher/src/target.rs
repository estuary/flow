use anyhow::Context;
use proto_flow::flow;
use proto_gazette::broker;

/// Metadata for routing publications to a journal destination.
pub enum Target {
    /// `Mapped` targets dynamically resolve documents to one of a collection's
    /// physical partitions, creating partitions on-demand.
    Mapped(MappedTarget),
    /// `Fixed` targets publish to a single, pre-existing journal by name.
    Fixed(FixedTarget),
}

/// Routes documents to a collection's physical partitions via key hashing
/// and partition-field extraction.
pub struct MappedTarget {
    /// Target collection name (for logging/debugging).
    pub collection: models::Collection,
    /// Pre-built key extractors for the collection key pointers.
    pub key_extractors: Vec<doc::Extractor>,
    /// Partitioned fields of the collection.
    pub partition_fields: Vec<String>,
    /// Pre-built key extractors for partitioned fields.
    pub partition_extractors: Vec<doc::Extractor>,
    /// Template for partitions of this collection.
    pub partitions_template: broker::JournalSpec,
    /// Maximum number of allowed partitions for this target.
    pub partitions_limit: usize,
    /// Collection partitions prefix ("{partitions_template.name}/").
    pub partitions_prefix: String,
}

/// Routes documents to a single named journal that already exists.
pub struct FixedTarget {
    /// Journal to which the target publishes.
    pub journal: String,
}

impl Target {
    /// Build a Mapped Target from a built CollectionSpec.
    ///
    /// The Target authorizes to and watches all partitions of the collection.
    pub fn from_collection_spec(spec: &flow::CollectionSpec) -> anyhow::Result<Self> {
        let flow::CollectionSpec {
            name,
            key,
            partition_fields,
            projections,
            partition_template,
            ..
        } = spec;

        let partitions_template = partition_template
            .as_ref()
            .context("CollectionSpec missing partition_template")?
            .clone();
        let partitions_prefix = format!("{}/", &partitions_template.name);

        let policy = doc::SerPolicy::noop();
        let key_extractors =
            extractors::for_key(key, projections, &policy).context("building key extractors")?;
        let partition_extractors = extractors::for_fields(partition_fields, projections, &policy)
            .context("building partition extractors")?;

        // TODO(johnny): We require limits on the number of partitions we'll
        // dynamically create, but we don't have control-plane wiring for this
        // knob today. As a basic sanity check, cap to 100 partitions for all
        // but ops collections (where we create partitions for each data-plane task).
        let partitions_limit = if name.starts_with("ops/") {
            usize::MAX
        } else {
            100
        };

        Ok(Self::Mapped(MappedTarget {
            collection: models::Collection::new(name),
            key_extractors,
            partition_fields: partition_fields.clone(),
            partition_extractors,
            partitions_template,
            partitions_limit,
            partitions_prefix,
        }))
    }

    /// Build a Fixed Target that publishes to a single named journal.
    /// The target skips the partitions watch and partition-mapping machinery.
    pub fn for_fixed_journal(journal: impl Into<String>) -> Self {
        Self::Fixed(FixedTarget {
            journal: journal.into(),
        })
    }

    /// AuthZ object string for this target's lazy journal Client. For Mapped
    /// targets this is the partitions prefix; for Fixed it's the journal name.
    pub(crate) fn authz_object(&self) -> &str {
        match self {
            Self::Mapped(t) => &t.partitions_prefix,
            Self::Fixed(t) => &t.journal,
        }
    }
}
