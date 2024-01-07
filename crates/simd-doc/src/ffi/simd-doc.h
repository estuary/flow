#pragma once

#include "simdjson.h"
#include "rust/cxx.h"

using namespace simdjson::ondemand;

// #include "simd-doc/src/ffi/mod.rs.h"

inline std::unique_ptr<parser> new_parser(size_t capacity)
{
    return std::make_unique<parser>(capacity);
}

typedef struct
{
    rust::Vec<uint64_t> &v;
    uint64_t partial;
    uint8_t partial_len;
} output;

std::pair<uint64_t, uint64_t> walk_node(value &doc, output &out);

// Parse many JSON documents from `padded_vec`, calling back with each before starting the next.
// Return the number of unconsumed remainder bytes.
inline size_t
parse_many(
    rust::Slice<uint8_t> input,
    std::unique_ptr<parser> &parser,
    rust::Vec<uint64_t> &v)
{
    const bool allow_comma_separated = false;
    document_stream stream = parser->iterate_many(input.data(), input.size(), input.size(), allow_comma_separated);

    output out = {v, 0, 0};

    for (document_stream::iterator it = stream.begin(); it != stream.end(); ++it)
    {
        size_t offset = it.current_index();
        document_reference doc = *it;
        value root = doc.get_value();
        walk_node(root, out);
    }
    return stream.size_in_bytes() - stream.truncated_bytes();
}

inline std::pair<uint32_t, uint32_t> emplace_string(
    const std::string_view &str,
    output &out)
{
    const char *data = str.data();
    size_t rem = str.length();

    // Small string optimization.
    if (rem < 8)
    {
        union
        {
            uint8_t b[8];
            uint32_t l[2];
        } u = {0};

        u.b[7] = static_cast<uint8_t>(rem);
        memcpy(&u.b[0], data, rem);
        return {u.l[0], u.l[1]};
    }

    uint32_t pos = out.v.size() * 8 + out.partial_len;

    // Set high bit to flag this is out-of-line.
    // Note the inline representation never sets MSB (since length < 8).
    pos = 1 << 31 | pos;

    memcpy(reinterpret_cast<uint8_t *>(&out.partial) + out.partial_len, data, 8 - out.partial_len);
    out.v.push_back(out.partial);
    data += out.partial_len;
    rem -= out.partial_len;

    while (rem >= 8)
    {
        out.v.push_back(*reinterpret_cast<const uint64_t *>(data));
        data += 8;
        rem -= 8;
    }

    out.partial = 0;
    memcpy(reinterpret_cast<uint8_t *>(&out.partial), data, rem);
    out.partial_len = rem;

    return {static_cast<uint32_t>(str.length()), pos};
}

inline uint32_t emplace_node(
    std::pair<uint64_t, uint64_t> node,
    output &out)
{
    uint32_t pos = out.v.size() * 8;

    switch (node.first >> 56)
    {
    case 0x08:
    {
        // This node is a string. Next check if it's an inline (small string)
        // our out-of-line representation.
        // Example inline:
        // |08000000 68656c6c 6f000005 00000000| ....hello....... 00000000
        //  (tag)    (data)      (len) (zeros)
        // Example out-of-line:
        // |08000000 13000000 e4ffffff 00000000|
        //  (tag)    (len)    (offset) (zeros)

        // This correspond
        if (node.second & 0x0800000000)
        {
        }
    }
    }
}

// Recursively walk a `dom::element`, initializing `node` with its structure.
std::pair<uint64_t, uint64_t> walk_node(value &elem, output &out)
{
    switch (elem.type())
    {
    case json_type::array:
    {
        array arr = elem;
        std::cout << "starting array" << std::endl;

        for (value child : arr)
        {
            std::cout << "next array elem" << std::endl;
            walk_node(child, out);
        }

        std::cout << "array done" << std::endl;
        break;
    }
    case json_type::object:
    {
        object obj = elem;
        std::cout << "starting obj" << std::endl;

        // Track whether field properties are already sorted.
        std::string_view last_key;
        bool must_sort = false;

        for (field child : obj)
        {
            std::string_view key = child.unescaped_key(false);
            std::cout << "key: " << key << " last_key: " << last_key << std::endl;

            if (key < last_key)
            {
                must_sort = true;
            }
            walk_node(child.value(), out);
            last_key = key;
        }

        // Restore the sorted invariant of doc::HeapNode::Object fields.
        // if (must_sort)
        //{
        // sort_fields(fields);
        //}
        std::cout << "obj done " << must_sort << std::endl;

        break;
    }
    case json_type::number:
    {
        number num = elem.get_number();

        switch (num.get_number_type())
        {
        case number_type::signed_integer:
        {
            int64_t v = num.get_int64();
            std::cout << "i64 " << v << std::endl;
            return {v < 0 ? 0x0400000000000000ul : 0x0700000000000000ul, *reinterpret_cast<uint64_t *>(&v)};
        }
        case number_type::unsigned_integer:
        {
            uint64_t v = num.get_int64();
            std::cout << "u64 " << v << std::endl;
            return {0x0700000000000000ul, v};
        }
        case number_type::floating_point_number:
        {
            double v = num.get_double();
            std::cout << "f64 " << v << std::endl;
            return {0x0700000000000000ul, *reinterpret_cast<uint64_t *>(&v)};
        }
        }
    }
    case json_type::string:
    {
        std::string_view str = elem;
        std::cout << "str " << str << std::endl;
        std::pair<uint32_t, uint32_t> em = emplace_string(str, out);
        return {0x0800000000000000ul + em.first, static_cast<uint64_t>(em.second) << 32};
    }
    case json_type::boolean:
    {
        bool b = elem;
        std::cout << "bool " << b << std::endl;
        return {b ? 0x0101000000000000ul : 0x0100000000000000ul, 0x0ul};
    }
    default:
        return {0x0500000000000000ul, 0x0ul};
    }
}