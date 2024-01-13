#pragma once

#include "simdjson.h"
#include "rust/cxx.h"

using namespace simdjson;

#include "simd-doc/src/ffi/mod.rs.h"

inline std::unique_ptr<dom::parser> new_parser(size_t capacity)
{
    return std::make_unique<dom::parser>(capacity);
}

void walk_element(const Allocator &alloc, dom::element elem, HeapNode &node);

// Parse many JSON documents from `input`, calling back with each before starting the next.
// Return the number of unconsumed remainder bytes.
inline size_t parse_many(
    const Allocator &alloc,
    Docs &docs,
    rust::Slice<uint8_t> input,
    HeapNode &node,
    std::unique_ptr<dom::parser> &parser)
{
    dom::document_stream stream = parser->parse_many(input.data(), input.size(), input.size());

    for (dom::document_stream::iterator it = stream.begin(); it != stream.end(); ++it)
    {
        walk_element(alloc, *it, node);
        complete(docs, it.current_index(), node);
    }
    return stream.size_in_bytes() - stream.truncated_bytes();
}

// Recursively walk a `dom::element`, initializing `node` with its structure.
void walk_element(const Allocator &alloc, dom::element elem, HeapNode &node)
{
    switch (elem.type())
    {
    case dom::element_type::ARRAY:
    {
        dom::array arr = elem;

        if (arr.size() >= 0xFFFFFF) [[unlikely]]
        {
            throw std::out_of_range("array is too large");
        }
        rust::Slice<HeapNode> items = set_array(alloc, node, arr.size());
        rust::Slice<HeapNode>::iterator it = items.begin();

        for (dom::element child : arr)
        {
            walk_element(alloc, child, *(it++));
        }
        break;
    }
    case dom::element_type::OBJECT:
    {
        dom::object obj = elem;

        if (obj.size() >= 0xFFFFFF) [[unlikely]]
        {
            throw std::out_of_range("object is too large");
        }
        rust::Slice<HeapField> fields = set_object(alloc, node, obj.size());
        rust::Slice<HeapField>::iterator it = fields.begin();

        // Track whether field properties are already sorted.
        std::string_view last_key;
        bool must_sort = false;

        for (dom::key_value_pair child : obj)
        {
            if (child.key < last_key)
            {
                must_sort = true;
            }
            walk_element(alloc, child.value, set_field(alloc, *(it++), child.key.data(), child.key.size()));
            last_key = child.key;
        }

        // Restore the sorted invariant of doc::HeapNode::Object fields.
        if (must_sort)
        {
            sort_fields(fields);
        }
        break;
    }
    case dom::element_type::INT64:
        set_i64(node, elem);
        break;
    case dom::element_type::UINT64:
        set_u64(node, elem);
        break;
    case dom::element_type::DOUBLE:
        set_f64(node, elem);
        break;
    case dom::element_type::STRING:
    {
        std::string_view str = elem;
        set_string(alloc, node, str.data(), str.size());
        break;
    }
    case dom::element_type::BOOL:
        set_bool(node, elem);
        break;
    default:
        set_null(node);
        break;
    }
}