use bytes::BufMut;
use tuple::TuplePack;

mod task_runtime;
pub use task_runtime::TaskRuntime;

mod task_service;
pub use task_service::TaskService;

pub mod derive;

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";

fn extract_packed<'alloc>(
    doc: &doc::LazyNode<'alloc, 'static, doc::ArchivedNode>,
    ptrs: &[doc::Pointer],
    shape: &doc::inference::Shape,
    out: &mut bytes::BytesMut,
) -> bytes::Bytes {
    match doc {
        doc::LazyNode::Heap(doc) => extract_packed_impl(doc, ptrs, shape, out),
        doc::LazyNode::Node(doc) => extract_packed_impl(*doc, ptrs, shape, out),
    }
}

fn extract_packed_impl<N: doc::AsNode>(
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
