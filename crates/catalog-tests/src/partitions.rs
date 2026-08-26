use crate::store::CollectionStore;
use anyhow::Context;
use proto_flow::flow::CollectionSpec;
use proto_gazette::broker::LabelSet;

/// Partition-routing state for one collection.
pub struct Partitioning {
    /// Collection's `partition_template.name`, which embeds its generation ID.
    template_name: String,
    /// Collection's `partition_template.labels`: the labels every one of its
    /// journals carries before partition fields and key range are added.
    template_labels: LabelSet,
    /// Sorted partition fields.
    fields: Vec<String>,
    /// Paired document extractors of partition fields.
    extractors: Vec<doc::Extractor>,
}

/// A collection's `partition_template.name`: the prefix shared by all of its
/// journal names, and the *only* unambiguous way to select them. Collection
/// names may nest — `acmeCo/nest` and `acmeCo/nest/inner` are both legal — so
/// the bare collection name of the former is a prefix of the latter's journals.
/// The template name appends the collection's generation ID, which no sibling or
/// descendant name can reproduce.
pub fn template_name(collection: &CollectionSpec) -> anyhow::Result<&str> {
    Ok(collection
        .partition_template
        .as_ref()
        .with_context(|| format!("collection {} missing partition_template", collection.name))?
        .name
        .as_str())
}

impl Partitioning {
    /// Build routing for `collection` from its partition template, fields, and
    /// projections.
    pub fn for_collection(collection: &CollectionSpec) -> anyhow::Result<Self> {
        let template = collection.partition_template.as_ref().with_context(|| {
            format!("collection {} missing partition_template", collection.name)
        })?;
        let template_name = template.name.clone();
        let template_labels = template.labels.clone().unwrap_or_default();

        let extractors = extractors::for_fields(
            &collection.partition_fields,
            &collection.projections,
            &doc::SerPolicy::noop(),
        )
        .context("building partition-field extractors")?;

        Ok(Self {
            template_name,
            template_labels,
            fields: collection.partition_fields.clone(),
            extractors,
        })
    }

    /// The store journal name and label set of `doc`'s logical partition. An
    /// unpartitioned collection has one journal, holding the empty logical
    /// partition.
    pub fn route<N: json::AsNode>(&self, doc: &N) -> anyhow::Result<(String, LabelSet)> {
        let set = labels::partition::encode_extracted_fields_labels(
            self.template_labels.clone(),
            &self.fields,
            &self.extractors,
            doc,
        )
        .context("encoding partition-field labels")?;

        let set = labels::partition::encode_key_range_labels(set, u32::MIN, u32::MAX);

        let name = labels::partition::full_name(&self.template_name, &set)
            .context("deriving partition journal name from its labels")?;
        let set = labels::add_value(set, "name", &name);

        Ok((name, set))
    }
}

/// Route `doc` (already serialized to `body`) into `store` under its logical
/// partition. Returns the journal it landed in.
pub fn append_routed(
    store: &mut CollectionStore,
    routing: &Partitioning,
    doc: &serde_json::Value,
    body: Vec<u8>,
) -> anyhow::Result<String> {
    let (journal, labels) = routing.route(doc)?;
    store.append(&journal, labels, body);
    Ok(journal)
}
