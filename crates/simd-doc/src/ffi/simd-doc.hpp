#pragma once

#include <utility>
#include "rust/cxx.h"
#include "simdjson.h"

struct Allocator;
struct HeapNode;
struct Parsed;
struct Transcoded;

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

class Parser
{
public:
    Parser(size_t capacity) : parser(capacity) {};
    void parse(const rust::Slice<const uint8_t> input, int64_t offset, const Allocator &alloc, HeapNode &node, Parsed &output);
    void transcode(const rust::Slice<const uint8_t> input, Transcoded &output);

private:
    std::vector<std::vector<pword>> pool;
    simdjson::dom::parser parser;
};

std::unique_ptr<Parser> new_parser(size_t capacity);