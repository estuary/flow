#pragma once

#include "simdjson.h"
#include "rust/cxx.h"

using namespace simdjson;

class SimdParser;

#include "simd-doc/src/ffi/mod.rs.h"

typedef union
{
    struct
    {
        char data[7];
        uint8_t len;
    } in;

    struct
    {
        uint32_t len;
        int32_t pos;
    } out;

    struct
    {
        uint32_t p1;
        uint32_t p2;
    } parts;

    void resolve(const size_t offset);
} pstr;

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

    void resolve(const size_t offset);
} pnode;

typedef struct
{
    pstr property;
    pnode node;
} pfield;

class SimdParser
{
public:
    SimdParser(size_t capacity) : parser(capacity), out(NULL){};
    size_t parse_many(rust::Slice<uint8_t> input, Out &next_out);

private:
    pstr place_string(const char *const d, const size_t len);
    pnode place_array(pnode *const d, const size_t len);
    pnode place_object(pfield *const d, const size_t len);

    void pad();
    pnode walk_node(dom::element elem);

    std::vector<std::vector<pfield>> fields_pool;
    std::vector<std::vector<pnode>> items_pool;

    dom::parser parser;
    Out *out;
};

inline std::unique_ptr<SimdParser> new_parser(size_t capacity)
{
    return std::make_unique<SimdParser>(capacity);
}

inline void SimdParser::pad()
{
    size_t n = (8 - (out->len() % 8)) % 8;
    static const uint8_t ZEROS[8] = {};
    out->extend(ZEROS, n);
}

inline pstr SimdParser::place_string(const char *const d, const size_t len)
{
    switch (len)
    {
    case 7:
        return {.in = {.data = {d[0], d[1], d[2], d[3], d[4], d[5], d[6]}, .len = 7}};
    case 6:
        return {.in = {.data = {d[0], d[1], d[2], d[3], d[4], d[5], 0}, .len = 6}};
    case 5:
        return {.in = {.data = {d[0], d[1], d[2], d[3], d[4], 0, 0}, .len = 5}};
    case 4:
        return {.in = {.data = {d[0], d[1], d[2], d[3], 0, 0, 0}, .len = 4}};
    case 3:
        return {.in = {.data = {d[0], d[1], d[2], 0, 0, 0, 0}, .len = 3}};
    case 2:
        return {.in = {.data = {d[0], d[1], 0, 0, 0, 0, 0}, .len = 2}};
    case 1:
        return {.in = {.data = {d[0], 0, 0, 0, 0, 0, 0}, .len = 1}};
    case 0:
        return {.in = {.data = {}, .len = 0}};
    default:
    {
        // Store `pos` as negative and offset by -1 so that the MSB is set.
        // This allows for distinguishing inline vs indirect representations,
        // since inline never sets the high bit of length (maximum of 7).
        pstr ret = {.out = {.len = len, .pos = -1 - static_cast<int32_t>(out->len())}};
        out->extend(reinterpret_cast<const uint8_t *>(d), len);
        return ret;
    }
    }
}

inline pnode SimdParser::place_array(pnode *const d, const size_t len)
{
    pad();
    static_assert(sizeof(pnode) == 16);

    const size_t offset = out->len();
    for (size_t i = 0; i != len; ++i)
    {
        d[i].resolve(offset + i * 16);
    }
    out->extend(reinterpret_cast<const uint8_t *>(d), len * 16);

    return pnode{
        .w1 = {.tag = 0x00, .boolean = {}, .zeros = {}, .p2 = {.i32 = static_cast<int32_t>(offset)}},
        .w2 = {.parts = {.p3 = {.u32 = static_cast<uint32_t>(len)}, .zero = {}}},
    };
}

inline pnode SimdParser::place_object(pfield *const d, const size_t len)
{
    pad();
    static_assert(sizeof(pfield) == 24);

    const size_t offset = out->len();
    for (size_t i = 0; i != len; ++i)
    {
        d[i].property.resolve(offset + i * 24);
        d[i].node.resolve(offset + i * 24 + 8);
    }
    out->extend(reinterpret_cast<const uint8_t *>(d), len * 24);

    return pnode{
        .w1 = {.tag = 0x06, .boolean = {}, .zeros = {}, .p2 = {.i32 = static_cast<int32_t>(offset)}},
        .w2 = {.parts = {.p3 = {.u32 = static_cast<uint32_t>(len)}, .zero = {}}},
    };
}

inline void pstr::resolve(const size_t offset)
{
    // If `pos` is negative (out-of-line representation, switch from a negative,
    // absolute location to negative, relative offset.
    if (out.pos < 0)
    {
        // Switch `pos` from a negative, absolute location to negative, relative offset.
        out.pos = -out.pos - offset - 1;
    }
}

inline void pnode::resolve(const size_t offset)
{
    switch (w1.tag)
    {
    case 0x00: // Array.
    {
        w1.p2.i32 = w1.p2.i32 - (offset + 4);
        break;
    }
    case 0x06: // Object.
    {
        w1.p2.i32 = w1.p2.i32 - (offset + 4);
        break;
    }
    case 0x08: // String.
    {
        reinterpret_cast<pstr *>(&w1.p2)->resolve(offset + 4);
        break;
    }
    }
}

pnode SimdParser::walk_node(dom::element elem)
{
    switch (elem.type())
    {
    case dom::element_type::ARRAY:
    {
        std::vector<pnode> items;
        if (!items_pool.empty())
        {
            items_pool.back().swap(items);
            items_pool.pop_back();
        }
        items.reserve(8);

        for (dom::element child : elem.get_array())
        {
            items.emplace_back(walk_node(child));
        }
        pnode ret = place_array(items.data(), items.size());

        items.clear();
        items_pool.emplace_back(std::move(items));

        return ret;
    }
    case dom::element_type::OBJECT:
    {
        std::vector<pfield> children;
        if (!fields_pool.empty())
        {
            fields_pool.back().swap(children);
            fields_pool.pop_back();
        }
        children.reserve(8);

        // Track whether field properties are already sorted.
        std::string_view last_key;
        bool must_sort = false;

        for (dom::key_value_pair field : elem.get_object())
        {
            pstr property = place_string(field.key.data(), field.key.length());

            if (field.key < last_key)
            {
                must_sort = true;
            }
            last_key = field.key;

            children.emplace_back(
                std::move(property),
                walk_node(field.value));
        }

        // Restore the sorted invariant of doc::HeapNode::Object fields.
        // if (must_sort)
        //{
        // sort_fields(fields);
        //}

        pnode ret = place_object(children.data(), children.size());

        children.clear();
        fields_pool.emplace_back(std::move(children));

        return ret;
    }
    case dom::element_type::INT64:
    {
        int64_t v = elem.get_int64();
        if (v < 0)
        {
            return pnode{.w1 = {.tag = 0x04}, .w2 = {.i64 = v}};
        }
        else
        {
            return pnode{.w1 = {.tag = 0x07}, .w2 = {.u64 = static_cast<uint64_t>(v)}};
        }
    }
    case dom::element_type::UINT64:
    {
        return pnode{.w1 = {.tag = 0x07}, .w2 = {.u64 = elem.get_uint64()}};
    }
    case dom::element_type::DOUBLE:
    {
        return pnode{.w1 = {.tag = 0x03}, .w2 = {.f64 = elem.get_double()}};
    }
    case dom::element_type::STRING:
    {
        pstr s = place_string(elem.get_c_str(), elem.get_string_length());
        return pnode{
            .w1 = {.tag = 0x08, .p2 = {.u32 = s.parts.p1}},
            .w2 = {.parts = {.p3 = {.u32 = s.parts.p2}}},
        };
    }
    case dom::element_type::BOOL:
    {
        return pnode{.w1 = {.tag = 0x01, .boolean = elem.get_bool()}};
    }
    default:
        return pnode{.w1 = {.tag = 0x05}};
    }
}

inline size_t
SimdParser::parse_many(rust::Slice<uint8_t> input, Out &next_out)
{
    out = &next_out;

    dom::document_stream stream = parser.parse_many(input.data(), input.size(), input.size());

    for (dom::document_stream::iterator it = stream.begin(); it != stream.end(); ++it)
    {
        out->begin(it.current_index());

        pnode node = walk_node(*it);
        place_array(&node, 1);

        out->finish();
    }

    size_t consumed = stream.size_in_bytes() - stream.truncated_bytes();

    out = NULL;

    return consumed;
}