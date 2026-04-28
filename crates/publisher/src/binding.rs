use anyhow::Context;
use proto_flow::flow;
use proto_gazette::broker;

/// Metadata for mapping documents to collection partitions.
pub struct Binding {
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
    /// Collection partitions prefix ("{partitions_template.name}/"), or a
    /// more-specific prefix or journal name to which this binding is scoped.
    pub partitions_prefix_or_name: String,
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

                Self::from_collection_spec(collection_spec, None).with_context(|| {
                    format!("building binding for collection {}", collection_spec.name)
                })
            })
            .collect()
    }

    /// Build a Binding from a built CollectionSpec.
    ///
    /// If `partitions_prefix_or_name` is Some, the Binding will authorize-to and
    /// watch only that sub-prefix or specific journal. When None, the Binding
    /// authorizes to all partitions of the collection.
    ///
    /// `partitions_prefix_or_name` must be prefixed by the CollectionSpec's
    /// actual partition template prefix, or this routine errors.
    pub fn from_collection_spec(
        spec: &flow::CollectionSpec,
        partitions_prefix_or_name: Option<&str>,
    ) -> anyhow::Result<Self> {
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

        let partitions_prefix_or_name = if let Some(fixed) = partitions_prefix_or_name {
            if !fixed.starts_with(&partitions_prefix) {
                anyhow::bail!(
                    "prefix or name {fixed} must begin with collection prefix {partitions_prefix}"
                );
            }
            fixed.to_string()
        } else {
            partitions_prefix
        };

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

        Ok(Self {
            collection: models::Collection::new(name),
            key_extractors,
            partition_fields: partition_fields.clone(),
            partition_extractors,
            partitions_template,
            partitions_limit,
            partitions_prefix_or_name,
        })
    }
}
