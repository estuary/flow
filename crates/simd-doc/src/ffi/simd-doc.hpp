#pragma once

#include "simdjson.h"
#include "rust/cxx.h"
#include <bit>

using namespace simdjson;

// Forward declaration for generated cxx bridge header.
class Parser;

#include "simd-doc/src/ffi/mod.rs.h"
#include <type_traits>

static_assert(
    std::endian::native == std::endian::little,
    "This implementation requires a little-endian architecture");

// pword is a 64-bit little-endian word placed in an rkyv buffer.
union pword
{
    uint64_t u64;
    int64_t i64;
    double f64;

    struct
    {
        uint32_t l; // Low u32 bits of the word.
        uint32_t h; // High u32 bits of the word.
    } u32;
};
static_assert(sizeof(pword) == 8);

// pnode is a doc::ArchivedNode representation placed in an rkyv buffer.
struct pnode
{
    pword w1;
    pword w2;
};
static_assert(sizeof(pnode) == 16);

// pfield is a doc::ArchivedField representation placed in an rkyv buffer.
struct pfield
{
    pword property;
    pnode node;
};
static_assert(sizeof(pfield) == 24);

// pbuffer is an aligned output buffer into which rkyv-transcoded documents are placed.
struct pbuffer
{
    uint8_t *data;                        // Raw output buffer.
    uint64_t len, cap;                    // Buffer length and capacity.
    std::vector<std::vector<pword>> pool; // Idle scratch buffers.
    Transcoded *ffi;                      // Backing Rust struct.

    // Write `len` instances of `src` into the buffer.
    template <typename T>
    inline void extend(const T *const src, const size_t len);
    // Pad with zeros until 8-byte aligned.
    void pad();
};

template <typename T>
inline void pbuffer::extend(const T *const src, size_t len)
{
    len = len * sizeof(T);

    // Do we need to grow the backing buffer?
    if (this->len + len > cap) [[unlikely]]
    {
        ffi->set_len(this->len);
        ffi->reserve(len);

        data = ffi->as_mut_ptr();
        cap = ffi->capacity();
    }
    memcpy(reinterpret_cast<T *>(data + this->len), src, len);
    this->len += len;
}

inline void pbuffer::pad()
{
    uint64_t n = (8 - (len % 8)) % 8;
    static const uint8_t ZEROS[8] = {};
    extend(ZEROS, n);
}

// Compute a uint64_t bit mask with the low `length` bytes set.
#define STR_MASK(length) ((1ull << ((length) * 8ull)) - 1ull)

// Build a pword holding an inline string representation, which is a
// short-string optimization that's also implemented by rkyv.
// * Inline strings have seven low bytes of data and one high byte of length,
//   with the highest bit always left unset.
// * Indirect strings store data earlier in the archive and reference
//   it using an always-negative offset (highest bit is always set).
#define STR_INLINE(word, length) \
    pword { .u64 = (word) | ((length) << 56) }

// Resolve the inner offset of a string placed at `offset`.
inline void pstr_resolve(uint32_t &pos, const uint64_t offset)
{
    // Is this an indirect representation?
    if (pos & 0x80000000)
    {
        // Switch from a negative absolute location, to a negative relative offset.
        pos = ~pos - offset;
    }
}

// Resolve the inner offsets of a pnode placed at `offset`.
inline void pnode_resolve(pnode &n, uint64_t offset)
{
    // Offset adjustments below are because offsets are relative to the position
    // of the offset uint32_t, which comes after the node tag and padding.

    switch (n.w1.u64 & 0xff)
    {
    case 0x00: // Array.
    case 0x06: // Object.
    {
        n.w1.u32.h = n.w1.u32.h - (offset + 4);
        break;
    }
    case 0x08: // String.
    {
        pstr_resolve(n.w2.u32.l, offset + 4);
        break;
    }
    }
}

// Place the resolved contents of an array into the buffer.
inline pnode place_array(pbuffer &buf, pnode *const d, const uint64_t len)
{
    buf.pad();

    const uint64_t offset = buf.len;
    for (uint64_t i = 0; i != len; ++i)
    {
        pnode_resolve(d[i], offset + i * sizeof(pnode));
    }
    buf.extend(d, len);

    return pnode{
        .w1 = {.u32 = {.l = 0x00, .h = static_cast<uint32_t>(offset)}},
        .w2 = {.u32 = {.l = static_cast<uint32_t>(len), .h = 0}},
    };
}

// Place the resolved contents of an object into the buffer.
inline pnode place_object(pbuffer &buf, pfield *const d, const uint64_t len)
{
    buf.pad();

    const uint64_t offset = buf.len;
    for (uint64_t i = 0; i != len; ++i)
    {
        pstr_resolve(d[i].property.u32.h, offset + i * sizeof(pfield));
        pnode_resolve(d[i].node, offset + i * sizeof(pfield) + 8);
    }
    buf.extend(d, len);

    return pnode{
        .w1 = {.u32 = {.l = 0x06, .h = static_cast<uint32_t>(offset)}},
        .w2 = {.u32 = {.l = static_cast<uint32_t>(len), .h = 0}},
    };
}

// Forward declaration of transcode_node, used by transcode_array and transcode_object.
pnode transcode_node(pbuffer &buf, dom::element_type typ, dom::element elem);

// Transcode a simdjson array using a depth-first walk of its items.
__attribute__((flatten)) pnode transcode_array(pbuffer &buf, dom::array arr)
{
    uint64_t len = arr.size();

    if (len >= 0xffffff) [[unlikely]]
    {
        throw std::out_of_range("array is too large");
    }

    std::vector<pword> scratch;
    if (!buf.pool.empty())
    {
        buf.pool.back().swap(scratch);
        buf.pool.pop_back();
    }

    // Allocate space for our array items (two words per item).
    scratch.reserve(len * 2);
    pnode *item = reinterpret_cast<pnode *>(scratch.data());

    for (dom::element cur : arr)
    {
        *(item++) = transcode_node(buf, cur.type(), cur);
    }
    pnode ret = place_array(buf, reinterpret_cast<pnode *>(scratch.data()), len);

    buf.pool.emplace_back(std::move(scratch));
    return ret;
}

// Sort the fields of a transcoded object.
__attribute__((noinline)) void sort_pfields(pbuffer &buf, pfield *const d, const uint64_t len)
{
    auto view = [&buf](const pword &w) -> std::string_view
    {
        if (w.u32.h & 0x80000000)
        {
            // This property is an indirect representation that points to its string.
            return std::string_view(reinterpret_cast<const char *>(buf.data + ~w.u32.h), w.u32.l);
        }
        else
        {
            // This property is an inline representation of its short string.
            return std::string_view(reinterpret_cast<const char *>(&w), w.u64 >> 56);
        }
    };
    std::sort(d, d + len, [view](const pfield &lhs, const pfield &rhs) -> bool
              { return view(lhs.property) < view(rhs.property); });
}

// Transcode a simdjson object using a depth-first walk of its fields.
__attribute__((flatten)) pnode transcode_object(pbuffer &buf, dom::object obj)
{
    uint64_t len = obj.size();

    if (len >= 0xffffff) [[unlikely]]
    {
        throw std::out_of_range("object is too large");
    }

    std::vector<pword> scratch;
    if (!buf.pool.empty())
    {
        buf.pool.back().swap(scratch);
        buf.pool.pop_back();
    }

    // Allocate space for our object fields (three words per field).
    scratch.reserve(len * 3);
    pfield *field = reinterpret_cast<pfield *>(scratch.data());

    // Track whether field properties are already sorted.
    std::string_view last_key;
    uint32_t unsorted = 0;

    for (dom::key_value_pair cur : obj)
    {
        dom::element_type cur_type = cur.value.type();

        unsorted += (cur.key <= last_key);
        last_key = cur.key;

        if (cur.key.length() < 8)
        {
            // Store using inline representation.

            // SAFETY: cur.key is drawn from simdjson's internal string buffer which
            // is allocated with sufficient size to parse a maximum-size document
            // having only empty strings (3 bytes per string), and is then further
            // extended with 64 SIMD padding bytes. This means we can always fetch
            // 8 bytes without worrying about running off the edge of an allocated page.
            // We then mask out extra bytes to zero by &-ing with the STR_MASK macro.
            uint64_t word;
            memcpy(&word, cur.key.data(), 8);

            field->property = STR_INLINE(word & STR_MASK(cur.key.length()), cur.key.length());
        }
        else
        {
            // Store using indirect representation.
            field->property = pword{
                .u32 = {
                    .l = static_cast<uint32_t>(cur.key.length()),
                    .h = static_cast<uint32_t>(~buf.len),
                }};
            buf.extend(cur.key.data(), cur.key.length());
        }

        field->node = transcode_node(buf, cur_type, cur.value);
        field++;
    }

    // Restore the sorted invariant of doc::ArchivedNode::Object fields.
    if (unsorted != 0)
    {
        sort_pfields(buf, reinterpret_cast<pfield *>(scratch.data()), len);
    }
    pnode ret = place_object(buf, reinterpret_cast<pfield *>(scratch.data()), len);

    buf.pool.emplace_back(std::move(scratch));
    return ret;
}

inline pnode transcode_node(pbuffer &buf, dom::element_type typ, dom::element elem)
{
    switch (typ)
    {
    case dom::element_type::ARRAY:
    {
        return transcode_array(buf, elem.get_array());
    }
    case dom::element_type::OBJECT:
    {
        return transcode_object(buf, elem.get_object());
    }
    case dom::element_type::INT64:
    {
        int64_t v = elem.get_int64();
        if (v < 0)
        {
            return pnode{.w1 = {.u64 = 0x04}, .w2 = {.i64 = v}};
        }
        else
        {
            return pnode{.w1 = {.u64 = 0x07}, .w2 = {.i64 = v}};
        }
    }
    case dom::element_type::UINT64:
    {
        return pnode{.w1 = {.u64 = 0x07}, .w2 = {.u64 = elem.get_uint64()}};
    }
    case dom::element_type::DOUBLE:
    {
        return pnode{.w1 = {.u64 = 0x03}, .w2 = {.f64 = elem.get_double()}};
    }
    case dom::element_type::STRING:
    {
        const char *const d = elem.get_c_str();
        size_t len = elem.get_string_length();

        if (len < 8)
        {
            // Store using inline representation.
            // SAFETY: See comment in transcode_object().
            uint64_t word;
            memcpy(&word, d, 8);

            pword s = STR_INLINE(word & STR_MASK(len), len);
            return pnode{
                .w1 = {.u32 = {.l = 0x08, .h = s.u32.l}},
                .w2 = {.u32 = {.l = s.u32.h, .h = 0}},
            };
        }
        else
        {
            // Store using indirect representation.
            pnode ret = pnode{
                .w1 = {.u32 = {.l = 0x08, .h = static_cast<uint32_t>(len)}},
                .w2 = {.u32 = {.l = static_cast<uint32_t>(~buf.len), .h = 0}},
            };
            buf.extend(d, len);
            return ret;
        }
    }
    case dom::element_type::BOOL:
    {
        if (elem.get_bool())
        {
            return pnode{.w1 = {.u64 = 0x101}, .w2 = {}};
        }
        else
        {
            return pnode{.w1 = {.u64 = 0x01}, .w2 = {}};
        }
    }
    default:
        return pnode{.w1 = {.u64 = 0x05}, .w2 = {}};
    }
}

// Recursively walk a `dom::element`, initializing `out` with its structure.
void parse_node(const Allocator &alloc, dom::element_type typ, dom::element elem, HeapNode &out)
{
    switch (typ)
    {
    case dom::element_type::ARRAY:
    {
        dom::array arr = elem;

        if (arr.size() >= 0xFFFFFF) [[unlikely]]
        {
            throw std::out_of_range("array is too large");
        }
        rust::Slice<HeapNode> items = set_array(alloc, out, arr.size());
        rust::Slice<HeapNode>::iterator it = items.begin();

        for (dom::element cur : arr)
        {
            parse_node(alloc, cur.type(), cur, *(it++));
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
        rust::Slice<HeapField> fields = set_object(alloc, out, obj.size());
        rust::Slice<HeapField>::iterator it = fields.begin();

        // Track whether field properties are already sorted.
        std::string_view last_key;
        uint32_t unsorted = 0;

        for (dom::key_value_pair cur : obj)
        {
            dom::element_type cur_type = cur.value.type();
            HeapNode &child = set_field(alloc, *(it++), cur.key.data(), cur.key.size());

            unsorted += (cur.key <= last_key);
            last_key = cur.key;

            parse_node(alloc, cur_type, cur.value, child);
        }

        // Restore the sorted invariant of doc::HeapNode::Object fields.
        if (unsorted != 0)
        {
            sort_heap_fields(fields);
        }
        break;
    }
    case dom::element_type::INT64:
        set_i64(out, elem);
        break;
    case dom::element_type::UINT64:
        set_u64(out, elem);
        break;
    case dom::element_type::DOUBLE:
        set_f64(out, elem);
        break;
    case dom::element_type::STRING:
    {
        std::string_view str = elem;
        set_string(alloc, out, str.data(), str.size());
        break;
    }
    case dom::element_type::BOOL:
        set_bool(out, elem);
        break;
    default:
        set_null(out);
        break;
    }
}

class Parser
{
public:
    Parser(size_t capacity) : parser(capacity) {};
    void parse(const rust::Slice<const uint8_t> input, int64_t offset, const Allocator &alloc, HeapNode &node, Parsed &output);
    void transcode(const rust::Slice<const uint8_t> input, Transcoded &output);

private:
    std::vector<std::vector<pword>> pool;
    dom::parser parser;
};

inline std::unique_ptr<Parser> new_parser(size_t capacity)
{
    return std::make_unique<Parser>(capacity);
}

// Parse many JSON documents from `input`, calling back with each before starting the next.
// Return the number of unconsumed remainder bytes.
inline void Parser::parse(const rust::Slice<const uint8_t> input, int64_t offset, const Allocator &alloc, HeapNode &node, Parsed &output)
{
    dom::document_stream stream = parser.parse_many(input.data(), input.size(), input.size());

    for (dom::document_stream::iterator it = stream.begin(); it != stream.end();)
    {
        dom::element elem = *it;
        parse_node(alloc, elem.type(), elem, node);

        ++it; // Step to the next document and verify trailing newline.
        if (input.data()[it.current_index() - 1] != '\n')
        {
            throw std::out_of_range("missing trailing newline");
        }

        complete(output, node, offset + static_cast<int64_t>(it.current_index()));
    }

    if (stream.truncated_bytes() != 0 && input.size() != 0)
    {
        throw std::out_of_range("extra bytes remain after processing all documents");
    }
}

inline void Parser::transcode(const rust::Slice<const uint8_t> input, Transcoded &output)
{
    dom::document_stream stream = parser.parse_many(input.data(), input.size(), input.size());

    pbuffer buf = pbuffer{
        .data = output.as_mut_ptr(),
        .len = output.len(),
        .cap = output.capacity(),
        .pool = {},
        .ffi = &output,
    };
    this->pool.swap(buf.pool);

    for (dom::document_stream::iterator it = stream.begin(); it != stream.end();)
    {
        // Write the document header placeholder.
        pword header = {.u32 = {.l = 0, .h = 0}};
        buf.extend(&header, 1);
        uint64_t start_len = buf.len;

        dom::element elem = *it;
        pnode root = transcode_node(buf, elem.type(), elem);
        place_array(buf, &root, 1);

        ++it; // Step to the next document and verify trailing newline.
        if (input.data()[it.current_index() - 1] != '\n')
        {
            throw std::out_of_range("missing trailing newline");
        }

        // Update and re-write header now that we know the next offset and length.
        header.u32.l = static_cast<uint32_t>(it.current_index());
        header.u32.h = buf.len - start_len;
        memcpy(buf.data + start_len - sizeof(pword), &header, sizeof(header));
    }

    output.set_len(buf.len);
    this->pool.swap(buf.pool);

    if (stream.truncated_bytes() != 0 && input.size() != 0)
    {
        throw std::out_of_range("extra bytes remain after processing all documents");
    }
}