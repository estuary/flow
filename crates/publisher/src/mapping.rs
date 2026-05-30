use super::watch;
use proto_gazette::broker;

/// Find or create a partition covering the given document key and partitions.
///
/// Searches the current listing for a matching partition. If none exists,
/// builds and applies a new partition spec, then waits for the listing to
/// update and retries if needed.
///
/// Takes an empty String `buffer` which optionally has pre-reserved capacity
/// and is pushed into, and then returned.
///
/// Panics if `fields` isn't sorted, or the same length as `extractors`.
///
/// Port of Go's `Mapper.Map` (`go/flow/mapping.go`).
pub(crate) async fn map_partition<N: json::AsNode>(
    binding: &super::MappedBinding,
    lazy: &std::sync::LazyLock<
        (
            gazette::journal::Client,
            tokens::PendingWatch<Vec<watch::PartitionSplit>>,
        ),
        crate::MappedClientInit,
    >,
    doc: &N,
    prefix: String,
    packed_key: bytes::BytesMut,
) -> tonic::Result<(String, bytes::BytesMut)> {
    let (prefix, packed_key, key_hash) = extract_mapping_context(binding, doc, prefix, packed_key)?;
    let (_doc, journal, packed_key) =
        map_partition_from_context(binding, lazy, doc, prefix, packed_key, key_hash).await?;
    Ok((journal, packed_key))
}

/// Owned-document variant of [`map_partition`].
///
/// Takes ownership of `doc` so that bump-backed heap documents can be moved
/// through the async mapping loop, rather than holding a borrowed `HeapNode`
/// across its `.await` points. Returns `doc` alongside the mapped journal.
pub(crate) async fn map_partition_owned(
    binding: &super::MappedBinding,
    lazy: &std::sync::LazyLock<
        (
            gazette::journal::Client,
            tokens::PendingWatch<Vec<watch::PartitionSplit>>,
        ),
        crate::MappedClientInit,
    >,
    doc: doc::OwnedNode,
    prefix: String,
    packed_key: bytes::BytesMut,
) -> tonic::Result<(doc::OwnedNode, String, bytes::BytesMut)> {
    let (prefix, packed_key, key_hash) =
        extract_mapping_context_owned(binding, &doc, prefix, packed_key)?;
    map_partition_from_context(binding, lazy, doc, prefix, packed_key, key_hash).await
}

async fn map_partition_from_context<D: PartitionDoc>(
    binding: &super::MappedBinding,
    lazy: &std::sync::LazyLock<
        (
            gazette::journal::Client,
            tokens::PendingWatch<Vec<watch::PartitionSplit>>,
        ),
        crate::MappedClientInit,
    >,
    doc: D,
    mut prefix: String,
    packed_key: bytes::BytesMut,
    key_hash: u32,
) -> tonic::Result<(D, String, bytes::BytesMut)> {
    let (client, partitions) = &(**lazy);
    let partitions = partitions.ready().await;

    loop {
        let refresh = partitions.token();
        let partitions = refresh.result()?;

        // Common case: we find a covering partition. Append its distinctive suffix and return.
        if let Some(idx) = pick_partition(partitions, &prefix, key_hash) {
            prefix.push_str(&partitions[idx].name[prefix.len()..]);
            return Ok((doc, prefix, packed_key));
        }
        // Uncommon case: a covering physical partition doesn't exist.

        // Have we exhausted the limit of partitions?
        if partitions.len() >= binding.partitions_limit {
            return Err(tonic::Status::resource_exhausted(format!(
                "collection {} has too many partitions ({}, limit is {})",
                binding.collection,
                partitions.len(),
                binding.partitions_limit
            )));
        }
        // Attempt to create a new full-range physical partition of this logical
        // partition. The logical-partition labels are extracted from the document
        // only here, on the uncommon path that actually needs them.
        let (name, request) = build_partition_apply(binding, &doc)?;
        let result = client.apply(request).await;

        match result {
            Ok(_response) => {
                tracing::info!(name, "created partition");
            }
            Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => {
                // Lost race to another process creating this partition.
                tracing::info!(name, "lost race to create partition");
            }
            Err(err) => {
                return Err(match err {
                    gazette::Error::Grpc(status) => status,
                    other => tonic::Status::internal(other.to_string()),
                });
            }
        }

        // Wait for the listing to update with the partition change.
        refresh.expired().await;
    }
}

/// Extract the partition-mapping context of `doc`: its packed key, key hash,
/// and the logical journal-name `prefix` of its partition.
///
/// Partition field values are encoded *directly* into the journal name, which
/// is all that's required to map a document in the common case where its
/// physical partition already exists.
fn extract_mapping_context<N: json::AsNode>(
    binding: &super::MappedBinding,
    doc: &N,
    mut prefix: String,
    mut packed_key: bytes::BytesMut,
) -> tonic::Result<(String, bytes::BytesMut, u32)> {
    doc::Extractor::extract_all(
        doc,
        &binding.key_extractors,
        doc::Encoding::Packed,
        &mut packed_key,
        None,
    );
    let key_hash = doc::Extractor::packed_hash(&packed_key);

    prefix.push_str(&binding.partitions_template.name);
    prefix.push('/');
    prefix = labels::partition::append_extracted_fields_name_suffix(
        prefix,
        &binding.partition_fields,
        &binding.partition_extractors,
        doc,
    )
    .map_err(|err| tonic::Status::internal(format!("failed to encode logical prefix: {err}")))?;

    Ok((prefix, packed_key, key_hash))
}

/// Owned-document counterpart of `extract_mapping_context`, which dispatches
/// `doc` to its inner `json::AsNode` representation.
fn extract_mapping_context_owned(
    binding: &super::MappedBinding,
    doc: &doc::OwnedNode,
    prefix: String,
    packed_key: bytes::BytesMut,
) -> tonic::Result<(String, bytes::BytesMut, u32)> {
    match doc {
        doc::OwnedNode::Heap(root) => match root.access() {
            Ok(heap_node) => extract_mapping_context(binding, &heap_node, prefix, packed_key),
            Err(embedded) => extract_mapping_context(binding, embedded.get(), prefix, packed_key),
        },
        doc::OwnedNode::Archived(archived) => {
            extract_mapping_context(binding, archived.get(), prefix, packed_key)
        }
    }
}

/// A document being mapped to a physical partition, which can encode the
/// labels of its logical partition on demand.
///
/// Mapping needs only the journal name in the common case: logical-partition
/// labels are required *only* when creating a new partition (uncommon).
/// This trait lets us defer extraction until it's needed, without holding
/// a borrowed `HeapNode` across an `.await` point.
trait PartitionDoc {
    /// Encode the document's logical-partition field values as
    /// `estuary.dev/field/` labels of `labels`, returning the extended set.
    fn encode_logical_partition_labels(
        &self,
        binding: &super::MappedBinding,
        labels: broker::LabelSet,
    ) -> tonic::Result<broker::LabelSet>;
}

impl<N: json::AsNode> PartitionDoc for &N {
    fn encode_logical_partition_labels(
        &self,
        binding: &super::MappedBinding,
        labels: broker::LabelSet,
    ) -> tonic::Result<broker::LabelSet> {
        labels::partition::encode_extracted_fields_labels(
            labels,
            &binding.partition_fields,
            &binding.partition_extractors,
            *self,
        )
        .map_err(|err| {
            tonic::Status::internal(format!("failed to encode logical partitions: {err}"))
        })
    }
}

impl PartitionDoc for doc::OwnedNode {
    fn encode_logical_partition_labels(
        &self,
        binding: &super::MappedBinding,
        labels: broker::LabelSet,
    ) -> tonic::Result<broker::LabelSet> {
        // Dispatch to the `&N` impl over `doc::OwnedNode`'s inner representation.
        match self {
            doc::OwnedNode::Heap(root) => match root.access() {
                Ok(heap_node) => (&heap_node).encode_logical_partition_labels(binding, labels),
                Err(embedded) => embedded
                    .get()
                    .encode_logical_partition_labels(binding, labels),
            },
            doc::OwnedNode::Archived(archived) => archived
                .get()
                .encode_logical_partition_labels(binding, labels),
        }
    }
}

/// Find a covering partition for the given logical prefix and hex key.
///
/// Binary searches over the sorted `PartitionSplit` slice to find the first
/// partition whose name starts with `logical_prefix` and whose key_end >= key_hash,
/// then validates that key_begin <= key_hash. Both ends are inclusive.
///
/// Port of Go's `pickPartition` (`go/flow/mapping.go`).
fn pick_partition(
    partitions: &[watch::PartitionSplit],
    logical_prefix: &str,
    key_hash: u32,
) -> Option<usize> {
    // Find the first partition where:
    //   name prefix > logical_prefix, OR
    //   name prefix == logical_prefix AND key_end >= key_hash
    let ind = partitions.partition_point(|p| {
        let name = &p.name;

        if name.len() >= logical_prefix.len() {
            // Compare the name's prefix portion against logical_prefix.
            // When equal, further compare on key_end against key_hash.
            match name[..logical_prefix.len()].cmp(logical_prefix) {
                std::cmp::Ordering::Less => return true,
                std::cmp::Ordering::Greater => return false,
                std::cmp::Ordering::Equal => p.key_end < key_hash,
            }
        } else {
            // Name is shorter than the prefix — compare what we have.
            name.as_ref() < logical_prefix
        }
    });

    if ind == partitions.len() {
        return None;
    }

    let p = &partitions[ind];

    // Verify the partition name starts with logical_prefix.
    if !p.name.starts_with(logical_prefix) {
        return None;
    }

    // Partition key_begin must also <= key_hash.
    if p.key_begin > key_hash {
        return None;
    }

    Some(ind)
}

// Build an ApplyRequest to create a new full-range physical partition of the
// logical partition implied by `doc`.
fn build_partition_apply<D: PartitionDoc>(
    binding: &super::MappedBinding,
    doc: &D,
) -> tonic::Result<(String, broker::ApplyRequest)> {
    let mut spec = binding.partitions_template.clone();

    // Encode labels of a single physical partition covering the full key
    // range, then the logical-partition fields extracted from `doc`.
    let labels = labels::partition::encode_key_range_labels(
        spec.labels.take().unwrap_or_default(),
        u32::MIN,
        u32::MAX,
    );
    let labels = doc.encode_logical_partition_labels(binding, labels)?;

    let name = labels::partition::full_name(&spec.name, &labels).unwrap();
    spec.name = name.clone();
    spec.labels = Some(labels);

    Ok((
        name,
        broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: 0, // Expect it's created by this Apply.
                upsert: Some(spec),
                delete: String::new(),
            }],
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use proto_flow::flow;
    use serde_json::json;
    use watch::PartitionSplit;

    fn splits(specs: &[(&str, u32, u32)]) -> Vec<PartitionSplit> {
        specs
            .iter()
            .map(|(name, begin, end)| PartitionSplit {
                name: (*name).into(),
                key_begin: *begin,
                key_end: *end,
                mod_revision: 0,
            })
            .collect()
    }

    #[test]
    fn test_pick_partition() {
        // Empty partition list.
        assert_eq!(pick_partition(&[], "coll/", 0), None);

        // Single full-range partition.
        let p = splits(&[("coll/a=1/pivot=00000000", 0, u32::MAX)]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x80000000), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=1/", u32::MAX), Some(0));
        // Wrong prefix.
        assert_eq!(pick_partition(&p, "coll/a=2/", 0), None);

        // Two partitions splitting the key space.
        let p = splits(&[
            ("coll/a=1/pivot=00000000", 0x00000000, 0x7fffffff),
            ("coll/a=1/pivot=80000000", 0x80000000, 0xffffffff),
        ]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x00000000), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x40000000), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x7fffffff), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x80000000), Some(1));
        assert_eq!(pick_partition(&p, "coll/a=1/", 0xffffffff), Some(1));

        // Gap between partitions: key_hash falls between covered ranges.
        let p = splits(&[
            ("coll/a=1/pivot=00000000", 0x00000000, 0x3fffffff),
            ("coll/a=1/pivot=80000000", 0x80000000, 0xffffffff),
        ]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x3fffffff), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x40000000), None);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x7fffffff), None);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x80000000), Some(1));

        // Multiple logical prefixes interleaved in sorted order.
        let p = splits(&[
            ("coll/a=1/pivot=00000000", 0x00000000, 0xffffffff),
            ("coll/a=2/pivot=00000000", 0x00000000, 0xffffffff),
            ("coll/a=3/pivot=00000000", 0x00000000, 0x7fffffff),
            ("coll/a=3/pivot=80000000", 0x80000000, 0xffffffff),
        ]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0x50000000), Some(0));
        assert_eq!(pick_partition(&p, "coll/a=2/", 0x50000000), Some(1));
        assert_eq!(pick_partition(&p, "coll/a=3/", 0x50000000), Some(2));
        assert_eq!(pick_partition(&p, "coll/a=3/", 0x90000000), Some(3));
        // Prefix before all partitions.
        assert_eq!(pick_partition(&p, "coll/a=0/", 0), None);
        // Prefix after all partitions.
        assert_eq!(pick_partition(&p, "coll/a=4/", 0), None);

        // Key hash past the end of all matching partitions.
        let p = splits(&[
            ("coll/a=1/pivot=00000000", 0x00000000, 0x77000000),
            ("coll/a=1/pivot=78000000", 0x78000000, 0xdd000000),
        ]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0xef000000), None);

        // Logical prefix is a proper prefix of a partition's field value.
        // "qib=ab/" must NOT match "qib=abcabc/pivot=00000000".
        let p = splits(&[
            ("coll/qib=abcabc/pivot=00000000", 0x00000000, 0xffffffff),
            ("coll/qib=d/pivot=00000000", 0x00000000, 0xffffffff),
        ]);
        assert_eq!(pick_partition(&p, "coll/qib=ab/", 0), None);
        assert_eq!(pick_partition(&p, "coll/qib=abcabc/", 0), Some(0));
        assert_eq!(pick_partition(&p, "coll/qib=d/", 0xdc000000), Some(1));

        // Partition name shorter than the logical prefix.
        let p = splits(&[("co/", 0, u32::MAX)]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0), None);

        // Short name that sorts after the prefix.
        let p = splits(&[("z/", 0, u32::MAX)]);
        assert_eq!(pick_partition(&p, "coll/a=1/", 0), None);
    }

    /// Build a test MappedBinding from a built CollectionSpec.
    fn test_binding(spec: &flow::CollectionSpec) -> super::super::MappedBinding {
        let flow::CollectionSpec {
            name,
            partition_template,
            partition_fields,
            projections,
            key,
            ..
        } = spec;

        let partition_template = partition_template.clone().unwrap();
        let partitions_prefix = format!("{}/", &partition_template.name);
        let policy = doc::SerPolicy::noop();

        let key_extractors = extractors::for_key(key, projections, &policy).unwrap();
        let partition_extractors =
            extractors::for_fields(partition_fields, projections, &policy).unwrap();

        super::super::MappedBinding {
            collection: models::Collection::new(name),
            key_extractors,
            partition_fields: partition_fields.clone(),
            partition_extractors,
            partitions_template: partition_template,
            partitions_limit: 100,
            partitions_prefix,
        }
    }

    #[tokio::test]
    async fn test_extract_mapping_context_and_partition_apply() {
        let source = build::arg_source_to_url("./src/test.flow.yaml", false).unwrap();
        let build::Output { built, .. } = build::for_local_test(&source, true)
            .await
            .into_result()
            .unwrap();

        let tables::BuiltCollection { spec, .. } = built
            .built_collections
            .get_key(&models::Collection::new("example/collection"))
            .unwrap();

        let spec = spec.as_ref().unwrap();
        let binding = test_binding(spec);

        // extract_mapping_context encodes partition field values directly into
        // the logical journal-name prefix.
        let doc_1 = json!({"a_key": "k", "a_bool": true, "a_str": "hello"});
        let (prefix_1, _, _) =
            extract_mapping_context(&binding, &doc_1, String::new(), bytes::BytesMut::new())
                .unwrap();

        let (prefix_2, _, _) = extract_mapping_context(
            &binding,
            &json!({"a_key": "k", "a_bool": false, "a_str": "world"}),
            String::new(),
            bytes::BytesMut::new(),
        )
        .unwrap();

        // Pre-allocated capacity doesn't affect the output.
        let (prefix_3, _, _) = extract_mapping_context(
            &binding,
            &json!({"a_key": "k", "a_bool": true, "a_str": "reused"}),
            String::with_capacity(256),
            bytes::BytesMut::new(),
        )
        .unwrap();

        insta::assert_json_snapshot!("logical_prefixes", json!([prefix_1, prefix_2, prefix_3]));

        // build_partition_apply creates a full-key-range partition spec, with
        // the logical-partition field labels extracted from the document only
        // when a new partition must be created.
        let (name, request) = build_partition_apply(&binding, &&doc_1).unwrap();

        insta::assert_json_snapshot!(
            "physical_partition_apply",
            json!({
                "name": name,
                "change": request.changes.into_iter().next().unwrap(),
            })
        );
    }
}
