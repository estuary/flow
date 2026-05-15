use anyhow::Context;
use proto_flow::flow;
use proto_gazette::broker;

/// Metadata for routing publications to a specific journal.
pub enum Binding {
    /// `Mapped` bindings dynamically resolve documents to one of a collection's
    /// physical partitions, creating partitions on-demand.
    Mapped(MappedBinding),
    /// `Fixed` bindings target a single, pre-existing journal by name.
    Fixed(FixedBinding),
}

/// Routes documents to a collection's physical partitions via key hashing
/// and partition-field extraction.
pub struct MappedBinding {
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
    /// Maximum number of allowed partitions for this binding.
    pub partitions_limit: usize,
    /// Collection partitions prefix ("{partitions_template.name}/").
    pub partitions_prefix: String,
}

/// Routes documents to a single named journal that already exists.
pub struct FixedBinding {
    /// Journal to which the binding publishes.
    pub journal: String,
}

impl Binding {
    /// Build Bindings for a CaptureSpec, one per active capture binding.
    pub fn from_capture_spec(spec: &flow::CaptureSpec) -> anyhow::Result<Vec<Self>> {
        spec.bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let collection_spec = binding
                    .collection
                    .as_ref()
                    .with_context(|| format!("capture binding {index} missing collection"))?;

                Self::from_collection_spec(collection_spec).with_context(|| {
                    format!("building binding for collection {}", collection_spec.name)
                })
            })
            .collect()
    }

    /// Build a Mapped Binding from a built CollectionSpec.
    ///
    /// The Binding authorizes to and watches all partitions of the collection.
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

        Ok(Self::Mapped(MappedBinding {
            collection: models::Collection::new(name),
            key_extractors,
            partition_fields: partition_fields.clone(),
            partition_extractors,
            partitions_template,
            partitions_limit,
            partitions_prefix,
        }))
    }

    /// Build a Fixed Binding that publishes to a single named journal.
    /// The binding skips the partitions watch and partition-mapping machinery.
    pub fn for_fixed_journal(journal: impl Into<String>) -> Self {
        Self::Fixed(FixedBinding {
            journal: journal.into(),
        })
    }

    /// AuthZ object string for this binding's lazy journal Client. For Mapped
    /// bindings this is the partitions prefix; for Fixed it's the journal name.
    pub(crate) fn authz_object(&self) -> &str {
        match self {
            Self::Mapped(b) => &b.partitions_prefix,
            Self::Fixed(b) => &b.journal,
        }
    }
}
