use super::{Binding, Request, Response};
use crate::Accumulator;
use anyhow::Context;
use bytes::BufMut;
use doc::HeapNode;
use proto_flow::runtime::combine_request;

pub fn recv_client_open(open: Request) -> anyhow::Result<(Accumulator, Vec<Binding>)> {
    let Some(open) = open.open else {
        anyhow::bail!("expected Open");
    };

    let mut specs = Vec::new();
    let mut bindings = Vec::new();

    for combine_request::open::Binding {
        full,
        key,
        projections,
        schema_json,
        ser_policy,
        uuid_ptr,
        values,
    } in open.bindings
    {
        let ser_policy = extractors::map_policy(&ser_policy.unwrap_or_default());
        let key = extractors::for_key(&key, &projections, &ser_policy)
            .context("building key extractors")?;
        let values = extractors::for_fields(&values, &projections, &ser_policy)
            .context("building values extractors")?;
        let built_schema = doc::validation::build_bundle(&schema_json)
            .context("schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;

        let uuid_ptr = if uuid_ptr.is_empty() {
            None
        } else {
            Some(doc::ptr::Pointer::from_str(&uuid_ptr))
        };

        specs.push((full, key.clone(), "source", None, validator));
        bindings.push(Binding {
            key,
            ser_policy: ser_policy.clone(),
            uuid_ptr,
            values,
        });
    }

    Ok((
        Accumulator::new(doc::combine::Spec::with_bindings(specs.into_iter()))?,
        bindings,
    ))
}

pub fn recv_client_add(
    accumulator: &mut Accumulator,
    add: Request,
    bindings: &[Binding],
) -> anyhow::Result<()> {
    let Some(combine_request::Add {
        binding: binding_index,
        doc_json,
        front,
    }) = add.add
    else {
        anyhow::bail!("expected Add");
    };

    let binding = bindings
        .get(binding_index as usize)
        .with_context(|| "invalid binding {binding}")?;

    let (memtable, alloc, mut doc) = accumulator
        .doc_bytes_to_heap_node(&doc_json)
        .context("couldn't parse added document as JSON")?;

    if let Some(uuid_ptr) = &binding.uuid_ptr {
        if let Some(uuid) = uuid_ptr.query(&doc) {
            // Skip this document if its UUID is marked as an ACK.
            // TODO(johnny): Reconsider whether we need this after shuffle refactors.
            let skip = (|| {
                let HeapNode::String(uuid) = uuid else {
                    anyhow::bail!("UUID is not a string")
                };
                let uuid = uuid::Uuid::parse_str(uuid)?;

                let (_producer, _clock, flags) = crate::uuid::parse(uuid)?;

                Ok(flags.is_ack())
            })()
            .with_context(|| {
                format!(
                    "invalid document UUID {}",
                    serde_json::to_string(&doc::SerPolicy::noop().on(uuid)).unwrap()
                )
            })?;

            if skip {
                return Ok(());
            }
        }

        let Ok(_) = uuid_ptr.create_heap_node(
            &mut doc,
            doc::HeapNode::String(doc::BumpStr::from_str(crate::UUID_PLACEHOLDER, alloc)),
            alloc,
        ) else {
            anyhow::bail!("unable to create document UUID placeholder");
        };
    }

    memtable.add(binding_index, doc, front)?;

    Ok(())
}

pub fn send_client_response(
    bindings: &[Binding],
    buf: &mut bytes::BytesMut,
    drained: doc::combine::DrainedDoc,
) -> anyhow::Result<Response> {
    let doc::combine::DrainedDoc { meta, root } = drained;

    let binding_index = meta.binding();
    let binding = &bindings[binding_index];

    let key_packed = doc::Extractor::extract_all_owned(&root, &binding.key, buf);
    let values_packed = doc::Extractor::extract_all_owned(&root, &binding.values, buf);

    serde_json::to_writer(buf.writer(), &binding.ser_policy.on_owned(&root))
        .expect("document serialization cannot fail");
    let doc_json = buf.split().freeze();

    Ok(Response {
        binding: binding_index as u32,
        deleted: meta.deleted(),
        doc_json,
        front: meta.front(),
        key_packed,
        values_packed,
    })
}
