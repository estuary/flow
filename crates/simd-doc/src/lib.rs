#[cxx::bridge]
mod ffi {

    extern "Rust" {
        type Alloc;
        type Node<'alloc>;
        type Field<'alloc>;

        unsafe fn set_array<'alloc>(
            alloc: &'alloc Alloc,
            node: &mut Node<'alloc>,
            size: usize,
        ) -> &'alloc mut [Node<'alloc>];

        unsafe fn set_object<'alloc>(
            alloc: &'alloc Alloc,
            node: &mut Node<'alloc>,
            size: usize,
        ) -> &'alloc mut [Field<'alloc>];

        unsafe fn set_field<'alloc>(
            alloc: &'alloc Alloc,
            field: &'alloc mut Field<'alloc>,
            property: &str,
        ) -> &'alloc mut Node<'alloc>;

        fn sort_fields(fields: &mut [Field<'_>]);

        fn set_bool(node: &mut Node<'_>, value: bool);
        fn set_f64(node: &mut Node<'_>, value: f64);
        fn set_i64(node: &mut Node<'_>, value: i64);
        fn set_null(node: &mut Node<'_>);
        unsafe fn set_string<'alloc>(alloc: &'alloc Alloc, node: &mut Node<'alloc>, value: &str);
        fn set_u64(node: &mut Node<'_>, value: u64);

        fn fizzle(sib: usize, node: &Node<'_>);
    }

    unsafe extern "C++" {
        include!("simd-doc/src/simd-doc.h");

        #[namespace = "simdjson::dom"]
        type parser;

        fn new_parser() -> UniquePtr<parser>;

        fn parse_many(
            parser: &mut UniquePtr<parser>,
            padded_vec: &mut [u8],
            alloc: &Alloc,
            node: &mut Node,
        ) -> Result<usize>;
    }
}

pub use ffi::*;
use std::ops::DerefMut;

pub struct Alloc(pub doc::Allocator);
pub struct Node<'alloc>(pub doc::HeapNode<'alloc>);
pub struct Field<'alloc>(pub doc::HeapField<'alloc>);

fn set_array<'alloc>(
    alloc: &'alloc Alloc,
    node: &mut Node<'alloc>,
    size: usize,
) -> &'alloc mut [Node<'alloc>] {
    let mut array = doc::BumpVec::with_capacity_in(size, &alloc.0);
    // Safety: parse_many() will fully initialize all `size` items,
    // or will throw an exception which entirely discards all work.
    unsafe { array.set_len(size) };

    // Safety: Node has the same layout as doc::HeapNode.
    // parse_many() will be responsible for its initialization.
    let ffi_array: &'alloc mut [Node<'alloc>] = unsafe { std::mem::transmute(array.deref_mut()) };

    node.0 = doc::HeapNode::Array(array);
    ffi_array
}

fn set_object<'alloc>(
    alloc: &'alloc Alloc,
    node: &mut Node<'alloc>,
    size: usize,
) -> &'alloc mut [Field<'alloc>] {
    let mut fields = doc::BumpVec::with_capacity_in(size, &alloc.0);
    // Safety: parse_many() will fully initialize all `size` fields,
    // or will throw an exception which entirely discards all work.
    unsafe { fields.set_len(size) };

    // Safety: Field has the same layout as doc::HeapField.
    // parse_many() will be responsible for its initialization.
    let ffi_fields: &'alloc mut [Field<'alloc>] =
        unsafe { std::mem::transmute(fields.deref_mut()) };

    node.0 = doc::HeapNode::Object(fields);
    ffi_fields
}

fn set_field<'alloc>(
    alloc: &'alloc Alloc,
    field: &'alloc mut Field<'alloc>,
    property: &str,
) -> &'alloc mut Node<'alloc> {
    field.0.property = doc::BumpStr::from_str(property, &alloc.0);

    // Safety: Node has the same layout as doc::HeapNode.
    // parse_many() will be responsible for its initialization.
    let ffi_node: &'alloc mut Node<'alloc> = unsafe { std::mem::transmute(&mut field.0.value) };
    ffi_node
}

fn set_bool(node: &mut Node<'_>, value: bool) {
    node.0 = doc::HeapNode::Bool(value)
}

fn set_f64(node: &mut Node<'_>, value: f64) {
    node.0 = doc::HeapNode::Float(value)
}

fn set_i64(node: &mut Node<'_>, value: i64) {
    node.0 = doc::HeapNode::NegInt(value)
}

fn set_null(node: &mut Node<'_>) {
    node.0 = doc::HeapNode::Null;
}

fn set_string<'alloc>(alloc: &'alloc Alloc, node: &mut Node<'alloc>, value: &str) {
    node.0 = doc::HeapNode::String(doc::BumpStr::from_str(value, &alloc.0));
}

fn set_u64(node: &mut Node<'_>, value: u64) {
    node.0 = doc::HeapNode::PosInt(value)
}

fn sort_fields(fields: &mut [Field<'_>]) {
    fields.sort_by(|l, r| l.0.property.cmp(&r.0.property));
}

fn fizzle(sib: usize, node: &Node<'_>) {
    let foo = serde_json::to_string(&doc::SerPolicy::default().on(&node.0)).unwrap();
    eprintln!("fizzle: {sib} {foo}");
}
