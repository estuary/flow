use std::ops::DerefMut;

#[cxx::bridge]
mod ffi {

    extern "Rust" {
        type Allocator; // doc::Allocator.
        type Docs<'a>; // Vec<(usize, doc::HeapNode)>
        type HeapField<'a>; // doc::HeapField.
        type HeapNode<'a>; // doc::HeapNode.

        // Set Node to an Array and return its items for initialization.
        unsafe fn set_array<'a>(
            alloc: &'a Allocator,
            node: &mut HeapNode<'a>,
            size: usize,
        ) -> &'a mut [HeapNode<'a>];

        // Set Node to an Object and return its fields for initialization.
        unsafe fn set_object<'a>(
            alloc: &'a Allocator,
            node: &mut HeapNode<'a>,
            size: usize,
        ) -> &'a mut [HeapField<'a>];

        // Set the property of a Field and return is value Node for initialization.
        unsafe fn set_field<'a>(
            alloc: &'a Allocator,
            field: &'a mut HeapField<'a>,
            property_ptr: *const c_char,
            property_len: usize,
        ) -> &'a mut HeapNode<'a>;

        // Sort already-initialized fields into property order.
        fn sort_fields(fields: &mut [HeapField<'_>]);

        // Set Node as a scalar from the given value.
        fn set_bool(node: &mut HeapNode<'_>, value: bool);
        fn set_f64(node: &mut HeapNode<'_>, value: f64);
        fn set_i64(node: &mut HeapNode<'_>, value: i64);
        fn set_null(node: &mut HeapNode<'_>);
        unsafe fn set_string<'a>(
            alloc: &'a Allocator,
            node: &mut HeapNode<'a>,
            value_ptr: *const c_char,
            value_len: usize,
        );
        fn set_u64(node: &mut HeapNode<'_>, value: u64);

        unsafe fn complete<'a>(docs: &mut Docs<'a>, offset: usize, node: &HeapNode<'a>);
    }

    unsafe extern "C++" {
        include!("simd-doc/src/ffi/simd-doc.h");

        #[namespace = "simdjson::dom"]
        type parser;

        fn new_parser(capacity: usize) -> UniquePtr<parser>;

        fn parse_many<'a>(
            alloc: &'a Allocator,
            docs: &mut Docs<'a>,
            node: &mut HeapNode<'a>,
            input: &mut [u8],
            parser: &mut UniquePtr<parser>,
        ) -> Result<usize>;
    }
}

pub(crate) use ffi::{new_parser, parser};

impl super::Parser {
    pub fn parse_simd<'a>(
        &mut self,
        alloc: &'a doc::Allocator,
        docs: &mut Vec<(usize, doc::HeapNode<'a>)>,
        input: &mut Vec<u8>,
    ) -> Result<(), cxx::Exception> {
        // We must pad `input` with requisite extra bytes.
        let input_len = input.len();
        input.extend_from_slice(&[0; 64]);
        input.truncate(input_len);

        if input_len == 0 {
            return Ok(());
        }

        // Safety: Allocator and Docs are repr(transparent) wrappers.
        let alloc: &'a Allocator = unsafe { std::mem::transmute(alloc) };
        let docs: &mut Docs<'a> = unsafe { std::mem::transmute(docs) };

        let mut node = HeapNode(doc::HeapNode::Null);
        let consumed = ffi::parse_many(alloc, docs, &mut node, &mut *input, &mut self.0)?;
        input.drain(..consumed);

        Ok(())
    }
}

#[repr(transparent)]
struct Allocator(doc::Allocator);
#[repr(transparent)]
struct Docs<'a>(Vec<(usize, doc::HeapNode<'a>)>);
#[repr(transparent)]
struct HeapNode<'a>(doc::HeapNode<'a>);
#[repr(transparent)]
struct HeapField<'a>(doc::HeapField<'a>);

#[inline(always)]
fn set_array<'a>(
    alloc: &'a Allocator,
    node: &mut HeapNode<'a>,
    size: usize,
) -> &'a mut [HeapNode<'a>] {
    let mut array = doc::BumpVec::with_capacity_in(size, &alloc.0);
    // Safety: parse_many() will fully initialize all `size` items,
    // or will throw an exception which entirely discards all work.
    unsafe { array.set_len(size) };

    // Safety: Node is repr(transparent) with its inner doc::HeapNode.
    // parse_many() will be responsible for its initialization.
    let ffi_array: &'a mut [HeapNode<'a>] = unsafe { std::mem::transmute(array.deref_mut()) };

    node.0 = doc::HeapNode::Array(array);
    ffi_array
}

#[inline(always)]
fn set_object<'a>(
    alloc: &'a Allocator,
    node: &mut HeapNode<'a>,
    size: usize,
) -> &'a mut [HeapField<'a>] {
    let mut fields = doc::BumpVec::with_capacity_in(size, &alloc.0);
    // Safety: parse_many() will fully initialize all `size` fields,
    // or will throw an exception which entirely discards all work.
    unsafe { fields.set_len(size) };

    // Safety: Field is repr(transparent) with its inner doc::HeapField.
    // parse_many() will be responsible for its initialization.
    let ffi_fields: &'a mut [HeapField<'a>] = unsafe { std::mem::transmute(fields.deref_mut()) };

    node.0 = doc::HeapNode::Object(fields);
    ffi_fields
}

#[inline(always)]
fn set_field<'a>(
    alloc: &'a Allocator,
    field: &'a mut HeapField<'a>,
    property_ptr: *const core::ffi::c_char,
    property_len: usize,
) -> &'a mut HeapNode<'a> {
    // Safety: UTF-8 is validated by simdjson.
    // We don't use cxx's rust::Str because it does an expensive extra UTF-8 validation.
    field.0.property =
        unsafe { doc::BumpStr::from_raw_parts(property_ptr as *const u8, property_len, &alloc.0) };

    // Safety: Node is repr(transparent) with its inner doc::HeapNode.
    // parse_many() will be responsible for its initialization.
    let ffi_node: &'a mut HeapNode<'a> = unsafe { std::mem::transmute(&mut field.0.value) };
    ffi_node
}

#[inline(always)]
fn set_bool(node: &mut HeapNode<'_>, value: bool) {
    node.0 = doc::HeapNode::Bool(value)
}

#[inline(always)]
fn set_f64(node: &mut HeapNode<'_>, value: f64) {
    node.0 = doc::HeapNode::Float(value)
}

#[inline(always)]
fn set_i64(node: &mut HeapNode<'_>, value: i64) {
    node.0 = if value < 0 {
        doc::HeapNode::NegInt(value)
    } else {
        doc::HeapNode::PosInt(value as u64)
    };
}

#[inline(always)]
fn set_null(node: &mut HeapNode<'_>) {
    node.0 = doc::HeapNode::Null;
}

#[inline(always)]
fn set_string<'a>(
    alloc: &'a Allocator,
    node: &mut HeapNode<'a>,
    value_ptr: *const core::ffi::c_char,
    value_len: usize,
) {
    // Safety: UTF-8 is validated by simdjson.
    // We don't use cxx's rust::Str because it does an expensive extra UTF-8 validation.
    let value =
        unsafe { doc::BumpStr::from_raw_parts(value_ptr as *const u8, value_len, &alloc.0) };
    node.0 = doc::HeapNode::String(value);
}

#[inline(always)]
fn set_u64(node: &mut HeapNode<'_>, value: u64) {
    node.0 = doc::HeapNode::PosInt(value)
}

#[inline(always)]
fn sort_fields(fields: &mut [HeapField<'_>]) {
    fields.sort_by(|l, r| l.0.property.cmp(&r.0.property));
}

#[inline(always)]
fn complete<'a>(docs: &mut Docs<'a>, offset: usize, node: &HeapNode<'a>) {
    let node = unsafe { std::mem::transmute_copy(&node.0) };
    docs.0.push((offset, node));
}
