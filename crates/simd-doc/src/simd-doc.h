#pragma once

#include "simdjson.h"
#include "rust/cxx.h"

using namespace simdjson;

#include "simd-doc/src/lib.rs.h"

inline std::unique_ptr<dom::parser> new_parser()
{
    return std::make_unique<dom::parser>();
}

void walk_element(Context &ctx, dom::element elem, Node &node);

// Parse many JSON documents from `padded_vec`, calling back with each before starting the next.
// Return the number of unconsumed remainder bytes.
inline size_t parse_many(
    Context &ctx,
    Node &node,
    rust::Slice<uint8_t> padded_vec,
    std::unique_ptr<dom::parser> &parser)
{
    dom::document_stream stream = parser->parse_many(padded_vec.data(), padded_vec.size(), padded_vec.size());

    for (dom::document_stream::iterator it = stream.begin(); it != stream.end(); ++it)
    {
        walk_element(ctx, *it, node);
        complete(ctx, it.current_index(), node);
    }
    return stream.size_in_bytes() - stream.truncated_bytes();
}

// Recursively walk a `dom::element`, initializing `node` with its structure.
void walk_element(Context &ctx, dom::element elem, Node &node)
{
    switch (elem.type())
    {
    case dom::element_type::ARRAY:
    {
        dom::array arr = elem;

        if (arr.size() >= 0xFFFFFF)
        {
            throw std::out_of_range("array is too large");
        }
        rust::Slice<Node> items = set_array(ctx, node, arr.size());
        rust::Slice<Node>::iterator it = items.begin();

        for (dom::element child : arr)
        {
            walk_element(ctx, child, *(it++));
        }
        break;
    }
    case dom::element_type::OBJECT:
    {
        dom::object obj = elem;

        if (obj.size() >= 0xFFFFFF)
        {
            throw std::out_of_range("object is too large");
        }
        rust::Slice<Field> fields = set_object(ctx, node, obj.size());
        rust::Slice<Field>::iterator it = fields.begin();

        // Track whether field properties are already sorted.
        rust::Str last_property;
        bool must_sort = false;

        for (dom::key_value_pair child : obj)
        {
            rust::Str property(child.key.data(), child.key.size());

            if (property < last_property)
            {
                must_sort = true;
            }

            walk_element(ctx, child.value, set_field(ctx, *(it++), property));
            last_property = property;
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
        set_string(ctx, node, rust::Str(str.data(), str.size()));
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