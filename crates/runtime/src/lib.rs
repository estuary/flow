use bytes::BufMut;
use proto_flow::flow::Projection;
use tuple::TuplePack;

mod task_runtime;
pub use task_runtime::TaskRuntime;

mod task_service;
pub use task_service::TaskService;

pub mod derive;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";

pub fn extract_packed_node<'alloc>(
    doc: &doc::LazyNode<'alloc, 'static, doc::ArchivedNode>,
    ptrs: &[doc::Pointer],
    shape: &doc::inference::Shape,
    out: &mut bytes::BytesMut,
) -> bytes::Bytes {
    match doc {
        doc::LazyNode::Heap(doc) => extract_packed(doc, ptrs, shape, out),
        doc::LazyNode::Node(doc) => extract_packed(*doc, ptrs, shape, out),
    }
}

pub fn extract_packed<N: doc::AsNode>(
    doc: &N,
    ptrs: &[doc::Pointer],
    shape: &doc::inference::Shape,
    out: &mut bytes::BytesMut,
) -> bytes::Bytes {
    let mut w = out.writer();

    for ptr in ptrs {
        if let Some(node) = ptr.query(doc) {
            node.as_node()
                .pack(&mut w, tuple::TupleDepth::new().increment())
                .unwrap();
        } else {
            let (shape, _) = shape.locate(ptr);

            match &shape.default {
                Some((val, _)) => val
                    .pack(&mut w, tuple::TupleDepth::new().increment())
                    .unwrap(),
                None => doc::Node::Null::<serde_json::Value>
                    .pack(&mut w, tuple::TupleDepth::new().increment())
                    .unwrap(),
            };
        }
    }
    out.split().freeze()
}

pub fn field_to_ptr(field: &str, projections: &[Projection]) -> anyhow::Result<doc::Pointer> {
    match projections.binary_search_by_key(&field, |p| &p.field) {
        Ok(index) => Ok(doc::Pointer::from(&projections[index].ptr)),
        Err(_) => Err(anyhow::anyhow!("field {field} is not a projection")),
    }
}

pub fn fields_to_ptrs(
    fields: &[String],
    projections: &[Projection],
) -> anyhow::Result<Vec<doc::Pointer>> {
    fields
        .iter()
        .map(|field| field_to_ptr(field, projections))
        .collect::<Result<Vec<_>, _>>()
}
