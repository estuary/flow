#pragma once

#include "simdjson.h"
#include "rust/cxx.h"

using namespace simdjson::ondemand;

#include "simd-doc/src/ffi/mod.rs.h"

inline std::unique_ptr<parser> new_parser(size_t capacity)
{
    return std::make_unique<parser>(capacity);
}

typedef struct
{
    rust::Vec<uint64_t> &v;

    union
    {
        uint64_t word;
        uint8_t data[8];
    } partial;

    uint8_t partial_len;
} output;

typedef union
{
    struct
    {
        uint32_t p1;
        uint32_t p2;
    } parts;

    uint64_t word;

    struct
    {
        uint32_t len;
        int32_t pos;
    } indirect;

    struct
    {
        uint8_t data[7];
        uint8_t len;
    } inline_;
} astr;

// Example inline:
// |08000000 68656c6c 6f000005 00000000| ....hello....... 00000000
//  (tag)    (data and len)    (zeros)
// Example out-of-line:
// |08000000 13000000 e4ffffff 00000000|
//  (tag)    (len)    (pos)    (zeros)
inline bool astr_is_inline(const astr &str)
{
    return str.indirect.pos >= 0;
}

typedef struct
{
    struct
    {
        uint8_t tag;
        uint8_t boolean;
        uint8_t zeros[2];

        union
        {
            int32_t i32;
            uint32_t u32;
        } p2;
    } w1;

    union
    {
        uint64_t u64;
        int64_t i64;
        double f64;

        struct
        {
            union
            {
                uint32_t u32;
                int32_t i32;
            } p3;

            uint32_t zero;
        } parts;
    } w2;
} anode;

inline astr emplace_string(
    output &out,
    const std::string_view &view)
{
    astr str = {};
    const char *data = view.data();
    size_t rem = view.length();

    // Small string optimization.
    if (rem < 8)
    {
        str.inline_.len = rem;
        memcpy(&str.inline_.data[0], data, rem);
        return str;
    }

    // Store as negative so that the MSB is set.
    // This allows for distinguishing inline vs indirect representations,
    // since inline never sets the high bit of length (maximum of 7).
    str.indirect.pos = -1 - (out.v.size() << 3) - out.partial_len;
    str.indirect.len = view.length();

    memcpy(&out.partial.data[out.partial_len], data, 8 - out.partial_len);
    out.v.push_back(out.partial.word);

    data += 8 - out.partial_len;
    rem -= 8 - out.partial_len;

    while (rem >= 8)
    {
        out.v.push_back(*reinterpret_cast<const uint64_t *>(data));
        data += 8;
        rem -= 8;
    }

    out.partial.word = 0;
    memcpy(&out.partial.data[0], data, rem);
    out.partial_len = rem;

    return str;
}

inline void emplace_node(anode &node, output &out)
{
    int32_t pos = out.v.size() << 3;

    switch (node.w1.tag)
    {
    case 0x00: // Array.
    {
        node.w1.p2.i32 = -(4 + pos + node.w1.p2.i32);
        break;
    }
    case 0x06: // Object.
    {
        node.w1.p2.i32 = -(4 + pos + node.w1.p2.i32);
        break;
    }
    case 0x08: // String.
    {
        if (node.w2.parts.p3.i32 < 0)
        {
            // Switch `pos` from a negative, absolute location to negative, relative offset.
            node.w2.parts.p3.i32 = -(5 + pos + node.w2.parts.p3.i32);
        }
        break;
    }
    }

    union
    {
        anode node;
        uint64_t words[2];
    } foo = {node};

    out.v.push_back(foo.words[0]);
    out.v.push_back(foo.words[1]);

    // std::cout << "out.v.size is now " << (out.v.size() << 3) << std::endl;
}

anode walk_node(value &doc, output &out);

// Parse many JSON documents from `padded_vec`, calling back with each before starting the next.
// Return the number of unconsumed remainder bytes.
inline size_t
parse_many(
    rust::Slice<uint8_t> input,
    rust::Vec<uint64_t> &v,
    std::unique_ptr<parser> &parser)
{
    output out = {v, {0}, 0};

    const bool allow_comma_separated = false;
    document_stream stream = parser->iterate_many(input.data(), input.size(), input.size(), allow_comma_separated);

    for (document_stream::iterator it = stream.begin(); it != stream.end(); ++it)
    {
        size_t header = out.v.size();
        out.v.push_back(static_cast<uint64_t>(it.current_index()) << 32);

        document_reference doc = *it;
        value root = doc.get_value();
        anode node = walk_node(root, out);

        /*
        if (out.partial_len != 0)
        {
            out.v.push_back(out.partial.word);
            out.partial_len = 0;
        }
        */
        emplace_node(node, out);

        out.v[header] |= static_cast<uint64_t>(out.v.size() - header - 1);
    }

    return stream.size_in_bytes() - stream.truncated_bytes();
}

// Recursively walk a `dom::element`, initializing `node` with its structure.
anode walk_node(value &elem, output &out)
{
    std::vector<std::pair<astr, anode>> children;

    anode node = {};

    switch (elem.type())
    {
    case json_type::array:
    {
        array arr = elem;
        // std::cout << "starting array" << std::endl;

        for (value child : arr)
        {
            // std::cout << "next array elem" << std::endl;
            astr empty = {};

            children.push_back(
                std::make_pair(empty, walk_node(child, out)));
        }

        if (out.partial_len != 0)
        {
            out.v.push_back(out.partial.word);
            out.partial_len = 0;
        }
        int32_t pos = out.v.size() << 3;

        for (auto &child : children)
        {
            emplace_node(child.second, out);
        }

        // std::cout << "array done" << std::endl;

        node.w1.tag = 0x00000000;
        node.w1.p2.i32 = -pos;
        node.w2.parts.p3.u32 = children.size();
        return node;
    }
    case json_type::object:
    {
        object obj = elem;
        // std::cout << "starting obj" << std::endl;

        // Track whether field properties are already sorted.
        std::string_view last_key;
        bool must_sort = false;

        for (field child : obj)
        {
            std::string_view key = child.unescaped_key(false);
            // std::cout << "key: " << key << " last_key: " << last_key << std::endl;

            if (key < last_key)
            {
                must_sort = true;
            }
            last_key = key;

            children.push_back(std::make_pair(
                emplace_string(out, key),
                walk_node(child.value(), out)));
        }

        // Restore the sorted invariant of doc::HeapNode::Object fields.
        // if (must_sort)
        //{
        // sort_fields(fields);
        //}

        if (out.partial_len != 0)
        {
            out.v.push_back(out.partial.word);
            out.partial_len = 0;
        }
        int32_t pos = out.v.size() << 3;

        for (auto &child : children)
        {
            if (child.first.indirect.pos < 0)
            {
                // Switch `pos` from a negative, absolute location to negative, relative offset.
                child.first.indirect.pos = -(1 + (out.v.size() << 3) + child.first.indirect.pos);
            }
            out.v.push_back(child.first.word);
            emplace_node(child.second, out);
        }

        // std::cout << "obj done " << must_sort << std::endl;

        node.w1.tag = 0x06;
        node.w1.p2.i32 = -pos;
        node.w2.parts.p3.u32 = children.size();
        return node;
    }
    case json_type::number:
    {
        number num = elem.get_number();

        switch (num.get_number_type())
        {
        case number_type::signed_integer:
        {
            int64_t v = num.get_int64();
            // std::cout << "i64 " << v << std::endl;

            if (v < 0)
            {
                node.w1.tag = 0x04;
                node.w2.i64 = v;
            }
            else
            {
                node.w1.tag = 0x07;
                node.w2.u64 = v;
            }
            break;
        }
        case number_type::unsigned_integer:
        {
            uint64_t v = num.get_int64();
            // std::cout << "u64 " << v << std::endl;

            node.w1.tag = 0x07;
            node.w2.u64 = v;
            break;
        }
        case number_type::floating_point_number:
        {
            double v = num.get_double();
            // std::cout << "f64 " << v << std::endl;

            node.w1.tag = 0x03;
            node.w2.f64 = v;
            break;
        }
        }
        return node;
    }
    case json_type::string:
    {
        std::string_view view = elem;
        // std::cout << "str " << view << std::endl;

        astr str = emplace_string(out, view);
        node.w1.tag = 0x08;
        node.w1.p2.u32 = str.parts.p1;
        node.w2.parts.p3.u32 = str.parts.p2;
        return node;
    }
    case json_type::boolean:
    {
        bool b = elem;
        // std::cout << "bool " << b << std::endl;
        node.w1.tag = 0x01;
        node.w1.boolean = b;
        return node;
    }
    default:
        node.w1.tag = 0x05;
        return node;
    }
}