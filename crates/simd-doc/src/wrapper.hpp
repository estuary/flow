/*
#pragma once

// #include "stddef.h"
#include "stdint.h"

const char *error_message(int error_code);

struct Parser;
struct Parser *parser_new();
void parser_drop(struct Parser *parser);

struct Docs;
struct Docs *docs_new();
void docs_drop(struct Docs *docs);

int parse_many(
    struct Parser *parser,
    struct Docs *docs,
    const uint8_t *buffer,
    uint64_t len,
    uint64_t batch_size);

struct Elem;
struct Elem *elem_new();
void elem_drop(struct Elem *element);

int docs_step(struct Docs *docs, struct Elem *root);

/*
struct Documents
{
    document_stream inner;
    document_stream::iterator cur;
};

inline std::unique_ptr<Documents> parse_many(
    std::unique_ptr<parser> &parser,
    rust::Slice<uint8_t> padded_vec,
    size_t batch_size)
{
    auto documents = std::make_unique<Documents>();
    documents->inner = parser->parse_many(padded_vec.data(), padded_vec.size(), batch_size);
    documents->cur = documents->inner.begin();
    return documents;
}
struct document_stream_iter
{
    cur document_stream::iterator;
    end document_stream::iterator;
}
*/