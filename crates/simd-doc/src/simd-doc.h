#pragma once

#include "simdjson.h"
#include "rust/cxx.h"

using namespace simdjson;

#include "simd-doc/src/lib.rs.h"

inline std::unique_ptr<dom::parser> new_parser()
{
    return std::make_unique<dom::parser>();
}

void baz(dom::element element,
         const Alloc &alloc,
         Node &node);

inline size_t parse_many(
    std::unique_ptr<dom::parser> &parser,
    rust::Slice<uint8_t> padded_vec,
    const Alloc &alloc,
    Node &node)
{
    dom::document_stream stream = parser->parse_many(padded_vec.data(), padded_vec.size(), padded_vec.size());

    for (dom::document_stream::iterator it = stream.begin(); it != stream.end(); ++it) {
        baz(*it, alloc, node);
        fizzle(it.current_index(), node);
    }
    return stream.truncated_bytes();
}

void baz(dom::element elem, const Alloc &alloc, Node &node)
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
        rust::Slice<Node>::iterator cur = set_array(alloc, node, arr.size()).begin();

        for (dom::element child : arr)
        {
            baz(child, alloc, *(cur++));
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
        rust::Slice<Field> fields = set_object(alloc, node, obj.size());
        rust::Slice<Field>::iterator cur = fields.begin();

        std::string_view last_key;
        bool must_sort = false;

        for (dom::key_value_pair child : obj)
        {
            baz(
                child.value,
                alloc,
                set_field(alloc, *(cur++), rust::Str(child.key.data(), child.key.size())));

            if (child.key < last_key) {
                must_sort = true;
            }
            last_key = child.key;
        }

        if (must_sort) {
            sort_fields(fields);
        }

        break;
    }
    case dom::element_type::INT64:
    {
        int64_t value = elem;
        if (value < 0)
        {
            set_i64(node, value);
        }
        else
        {
            set_u64(node, static_cast<uint64_t>(value));
        }
        break;
    }
    case dom::element_type::UINT64:
    {
        set_u64(node, elem);
        break;
    }
    case dom::element_type::DOUBLE:
    {
        set_f64(node, elem);
        break;
    }
    case dom::element_type::STRING:
    {
        std::string_view str = elem;
        set_string(alloc, node, rust::Str(str.data(), str.size()));
        break;
    }
    case dom::element_type::BOOL:
    {
        set_bool(node, elem);
        break;
    }
    case dom::element_type::NULL_VALUE:
    {
        set_null(node);
        break;
    }
    }
}