use super::Transcoded;
use std::ops::DerefMut;

// Implement Transcoded delegates for use from C++.
impl Transcoded {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.v.as_mut_ptr()
    }
    fn capacity(&self) -> usize {
        self.v.capacity()
    }
    fn len(&self) -> usize {
        self.v.len()
    }
    fn reserve(&mut self, additional: usize) {
        self.v.reserve(additional);
    }
    unsafe fn set_len(&mut self, len: usize) {
        self.v.set_len(len)
    }
}

#[cxx::bridge]
mod ffi {

    extern "Rust" {
        // Expose routines that allow C++ to emplace a buffer of transcoded documents.
        type Transcoded;
        fn as_mut_ptr(self: &mut Transcoded) -> *mut u8;
        fn capacity(self: &Transcoded) -> usize;
        fn len(self: &Transcoded) -> usize;
        fn reserve(self: &mut Transcoded, additional: usize);
        unsafe fn set_len(self: &mut Transcoded, len: usize);

        type Allocator; // Wraps doc::Allocator.
        type HeapField<'a>; // Wraps doc::HeapField.
        type HeapNode<'a>; // Wraps doc::HeapNode.
        type Parsed<'a>; // Vec<(usize, doc::HeapNode)>

        // Set `node` to an Array of the given `size`,
        // and return its `items_out` (length `size`) for initialization,
        // as well as its `tape_length_out` (initially 1) for further accumulation.
        unsafe fn set_array<'a>(
            alloc: &'a Allocator,
            node: &mut HeapNode<'a>,
            size: usize,
            items_out: &mut &'a mut [HeapNode<'a>],
            tape_length_out: &mut *mut i32,
        );

        // Set `node` to an Object of the given `size`,
        // and return its `fields_out` (length `size`) for initialization,
        // as well as its `tape_length_out` (initially 1) for further accumulation.
        unsafe fn set_object<'a>(
            alloc: &'a Allocator,
            node: &mut HeapNode<'a>,
            size: usize,
            fields_out: &mut &'a mut [HeapField<'a>],
            tape_length_out: &mut *mut i32,
        );

        // Set the property of `field` and return is value Node for initialization.
        unsafe fn set_field<'a>(
            alloc: &'a Allocator,
            field: &'a mut HeapField<'a>,
            property_ptr: *const c_char,
            property_len: usize,
        ) -> &'a mut HeapNode<'a>;

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

        // Sort already-initialized fields into property order.
        fn sort_heap_fields(fields: &mut [HeapField<'_>]);

        unsafe fn complete<'a>(output: &mut Parsed<'a>, node: &HeapNode<'a>, next_offset: i64);
    }

    unsafe extern "C++" {
        include!("simd-doc/src/ffi/simd-doc.hpp");

        type Parser;
        fn new_parser(capacity: usize) -> UniquePtr<Parser>;

        fn parse<'a>(
            self: Pin<&mut Parser>,
            input: &[u8],
            offset: i64,
            alloc: &'a Allocator,
            node: &mut HeapNode<'a>,
            output: &mut Parsed,
        ) -> Result<()>;

        fn transcode<'a>(
            self: Pin<&mut Parser>,
            input: &[u8],
            output: &mut Transcoded,
        ) -> Result<()>;
    }
}

// ffi::Parser is safe to Send across threads (but is not Sync).
unsafe impl Send for ffi::Parser {}

pub(crate) use ffi::{new_parser, Parser};

#[repr(transparent)]
pub(crate) struct Allocator(doc::Allocator);
#[repr(transparent)]
pub(crate) struct HeapNode<'a>(doc::HeapNode<'a>);
#[repr(transparent)]
pub(crate) struct HeapField<'a>(doc::HeapField<'a>);
#[repr(transparent)]
pub(crate) struct Parsed<'a>(Vec<(doc::HeapNode<'a>, i64)>);

#[inline(always)]
fn set_array<'a>(
    alloc: &'a Allocator,
    node: &mut HeapNode<'a>,
    size: usize,
    items_out: &mut &'a mut [HeapNode<'a>],
    tape_length_out: &mut *mut i32,
) {
    node.0 = doc::HeapNode::Array(1, doc::BumpVec::with_capacity_in(size, &alloc.0));

    let doc::HeapNode::Array(tape_length, items) = &mut node.0 else {
        unreachable!()
    };

    // Safety: HeapNode is repr(transparent) with its inner doc::HeapNode.
    // Parser will be responsible for initialization of all `size` items,
    // or will throw an exception which discards all partial work.
    let items: &'a mut [HeapNode<'a>] = unsafe {
        items.set_len(size);
        std::mem::transmute(items.deref_mut())
    };

    *items_out = items;
    *tape_length_out = tape_length as *mut i32;
}

#[inline(always)]
fn set_object<'a>(
    alloc: &'a Allocator,
    node: &mut HeapNode<'a>,
    size: usize,
    fields_out: &mut &'a mut [HeapField<'a>],
    tape_length_out: &mut *mut i32,
) {
    node.0 = doc::HeapNode::Object(1, doc::BumpVec::with_capacity_in(size, &alloc.0));

    let doc::HeapNode::Object(tape_length, fields) = &mut node.0 else {
        unreachable!()
    };

    // Safety: HeapField is repr(transparent) with its inner doc::HeapField.
    // Parser will be responsible for initialization of all `size` fields,
    // or will throw an exception which discards all partial work.
    let fields: &'a mut [HeapField<'a>] = unsafe {
        fields.set_len(size);
        std::mem::transmute(fields.deref_mut())
    };

    *fields_out = fields;
    *tape_length_out = tape_length as *mut i32;
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

    // Safety: HeapNode is repr(transparent) with its inner doc::HeapNode.
    // Parser will be responsible for its initialization.
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
fn sort_heap_fields(fields: &mut [HeapField<'_>]) {
    fields.sort_by(|l, r| l.0.property.cmp(&r.0.property));
}

fn complete<'a>(parsed: &mut Parsed<'a>, node: &HeapNode<'a>, next_offset: i64) {
    let node = unsafe { std::mem::transmute_copy(&node.0) };
    parsed.0.push((node, next_offset));
}
