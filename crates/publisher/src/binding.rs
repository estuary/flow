use anyhow::Context;
use proto_flow::flow;
use proto_gazette::broker;

pub struct Binding {
    /// Index of this Binding within the publishing task.
    pub index: u32,
    /// Lazy journal Client and partitions watch.
    pub client: super::LazyPartitionsClient,
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
}

impl Binding {
    /// Build Bindings for a CaptureSpec, one per active capture binding.
    ///
    /// Each binding's journal Client and partitions watch are initialized lazily:
    /// the `client_factory` is called only when a binding is first written to,
    /// producing a Client scoped to the binding's collection and the capture's
    /// task name.
    pub fn from_capture_spec(
        spec: &flow::CaptureSpec,
        client_factory: &super::JournalClientFactory,
    ) -> anyhow::Result<Vec<Self>> {
        let task_name = models::Name::new(&spec.name);

        spec.bindings
            .iter()
            // We must include inactive bindings so we may write their ACK intents.
            .chain(spec.inactive_bindings.iter())
            .enumerate()
            .map(|(index, binding)| {
                let collection_spec = binding
                    .collection
                    .as_ref()
                    .with_context(|| format!("capture binding {index} missing collection"))?;

                let collection = models::Collection::new(&collection_spec.name);
                let task_name = task_name.clone();
                let client_factory = client_factory.clone();

                let client_init: super::PartitionsClientInit = Box::new(move || {
                    let client = client_factory(collection.clone(), task_name);
                    let partitions =
                        super::watch::watch_partitions(client.clone(), collection.as_ref());
                    (client, partitions)
                });

                Self::from_collection_spec(index as u32, collection_spec, client_init)
            })
            .collect()
    }

    /// Build a Binding from a built CollectionSpec.
    ///
    /// Extracts key and partition-field extractors from the spec's projections.
    /// `client_init` is called lazily on first use to create the journal Client
    /// and partitions watch for this binding's collection.
    pub fn from_collection_spec(
        index: u32,
        spec: &flow::CollectionSpec,
        client_init: super::PartitionsClientInit,
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
            index,
            client: std::sync::LazyLock::new(client_init),
            collection: models::Collection::new(name),
            key_extractors,
            partition_fields: partition_fields.clone(),
            partition_extractors,
            partitions_template,
            partitions_limit,
        })
    }
}
