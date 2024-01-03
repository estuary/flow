use std::ops::DerefMut;

#[cxx::bridge]
mod ffi {

    extern "Rust" {
        // Context for this parsing.
        type Context;
        // Node which is being parsed.
        type Node<'c>;
        // Field of an Object Node.
        type Field<'c>;

        // Set Node to an Array and return its items for initialization.
        unsafe fn set_array<'c>(
            ctx: &'c mut Context,
            node: &mut Node<'c>,
            size: usize,
        ) -> &'c mut [Node<'c>];

        // Set Node to an Object and return its fields for initialization.
        unsafe fn set_object<'c>(
            ctx: &'c mut Context,
            node: &mut Node<'c>,
            size: usize,
        ) -> &'c mut [Field<'c>];

        // Set the property of a Field and return is value Node for initialization.
        unsafe fn set_field<'c>(
            ctx: &'c mut Context,
            field: &'c mut Field<'c>,
            property: &str,
        ) -> &'c mut Node<'c>;

        // Sort already-initialized fields into property order.
        fn sort_fields(fields: &mut [Field<'_>]);

        // Set Node as a scalar from the given value.
        fn set_bool(node: &mut Node<'_>, value: bool);
        fn set_f64(node: &mut Node<'_>, value: f64);
        fn set_i64(node: &mut Node<'_>, value: i64);
        fn set_null(node: &mut Node<'_>);
        unsafe fn set_string<'c>(ctx: &'c mut Context, node: &mut Node<'c>, value: &str);
        fn set_u64(node: &mut Node<'_>, value: u64);

        unsafe fn complete<'c>(ctx: &'c mut Context, offset: usize, node: &Node<'c>);
    }

    unsafe extern "C++" {
        include!("simd-doc/src/simd-doc.h");

        #[namespace = "simdjson::dom"]
        type parser;

        fn new_parser() -> UniquePtr<parser>;

        fn parse_many<'c>(
            ctx: &'c mut Context,
            node: &mut Node<'c>,
            padded_vec: &mut [u8],
            parser: &mut UniquePtr<parser>,
        ) -> Result<usize>;
    }
}

pub use ffi::*;

pub fn do_it(
    parser: &mut cxx::UniquePtr<parser>,
    input: &mut Vec<u8>,
) -> Result<Vec<(usize, doc::OwnedHeapNode)>, cxx::Exception> {
    let input_len = input.len();
    input.extend_from_slice(&[0; 64]);
    input.truncate(input_len);

    let mut ctx = Context {
        alloc: std::sync::Arc::new(doc::Allocator::with_capacity(input_len * 2)),
        out: Vec::new(),
    };
    let mut node = Node(doc::HeapNode::Null);

    let consumed = parse_many(&mut ctx, &mut node, &mut *input, parser)?;
    input.drain(..consumed);

    Ok(ctx.out)
}

struct Context {
    alloc: std::sync::Arc<doc::Allocator>,
    out: Vec<(usize, doc::OwnedHeapNode)>,
}

#[repr(transparent)]
pub struct Node<'c>(pub doc::HeapNode<'c>);
#[repr(transparent)]
pub struct Field<'c>(pub doc::HeapField<'c>);

fn set_array<'c>(ctx: &'c mut Context, node: &mut Node<'c>, size: usize) -> &'c mut [Node<'c>] {
    let mut array = doc::BumpVec::with_capacity_in(size, &ctx.alloc);
    // Safety: parse_many() will fully initialize all `size` items,
    // or will throw an exception which entirely discards all work.
    unsafe { array.set_len(size) };

    // Safety: Node is repr(transparent) with its inner doc::HeapNode.
    // parse_many() will be responsible for its initialization.
    let ffi_array: &'c mut [Node<'c>] = unsafe { std::mem::transmute(array.deref_mut()) };

    node.0 = doc::HeapNode::Array(array);
    ffi_array
}

fn set_object<'c>(ctx: &'c mut Context, node: &mut Node<'c>, size: usize) -> &'c mut [Field<'c>] {
    let mut fields = doc::BumpVec::with_capacity_in(size, &ctx.alloc);
    // Safety: parse_many() will fully initialize all `size` fields,
    // or will throw an exception which entirely discards all work.
    unsafe { fields.set_len(size) };

    // Safety: Field is repr(transparent) with its inner doc::HeapField.
    // parse_many() will be responsible for its initialization.
    let ffi_fields: &'c mut [Field<'c>] = unsafe { std::mem::transmute(fields.deref_mut()) };

    node.0 = doc::HeapNode::Object(fields);
    ffi_fields
}

fn set_field<'c>(
    ctx: &'c mut Context,
    field: &'c mut Field<'c>,
    property: &str,
) -> &'c mut Node<'c> {
    field.0.property = doc::BumpStr::from_str(property, &ctx.alloc);

    // Safety: Node is repr(transparent) with its inner doc::HeapNode.
    // parse_many() will be responsible for its initialization.
    let ffi_node: &'c mut Node<'c> = unsafe { std::mem::transmute(&mut field.0.value) };
    ffi_node
}

fn set_bool(node: &mut Node<'_>, value: bool) {
    node.0 = doc::HeapNode::Bool(value)
}

fn set_f64(node: &mut Node<'_>, value: f64) {
    node.0 = doc::HeapNode::Float(value)
}

fn set_i64(node: &mut Node<'_>, value: i64) {
    node.0 = if value < 0 {
        doc::HeapNode::NegInt(value)
    } else {
        doc::HeapNode::PosInt(value as u64)
    };
}

fn set_null(node: &mut Node<'_>) {
    node.0 = doc::HeapNode::Null;
}

fn set_string<'c>(ctx: &'c mut Context, node: &mut Node<'c>, value: &str) {
    node.0 = doc::HeapNode::String(doc::BumpStr::from_str(value, &ctx.alloc));
}

fn set_u64(node: &mut Node<'_>, value: u64) {
    node.0 = doc::HeapNode::PosInt(value)
}

fn sort_fields(fields: &mut [Field<'_>]) {
    fields.sort_by(|l, r| l.0.property.cmp(&r.0.property));
}

fn complete<'c>(ctx: &'c mut Context, offset: usize, node: &Node<'c>) {
    let node =
        unsafe { doc::OwnedHeapNode::new(std::mem::transmute_copy(&node.0), ctx.alloc.clone()) };
    ctx.out.push((offset, node));
}
