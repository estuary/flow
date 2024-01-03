/*
#include "wrapper.hpp"
#include "simdjson.h"

using namespace simdjson;
using simdjson::error_code;

const char *error_message(int code)
{
    return simdjson::error_message(static_cast<error_code>(code));
}

struct Parser
{
    dom::parser inner;
};

struct Parser *parser_new()
{
    return new (struct Parser){simdjson::dom::parser()};
}

void parser_drop(Parser *parser)
{
    delete parser;
}

struct Docs
{
    dom::document_stream inner;
    dom::document_stream::iterator cur;
};

Docs *docs_new()
{
    return new (struct Docs){};
}

void docs_drop(Docs *docs)
{
    delete docs;
}

struct Elem
{
    dom::element inner;
};

Elem *elem_new()
{
    return new (struct Elem){};
}

void elem_drop(Elem *elem)
{
    delete elem;
}

int parse_many(Parser *parser, Docs *docs, const uint8_t *buffer, uint64_t len, uint64_t batch_size)
{
    error_code code = parser->inner.parse_many(buffer, len, batch_size).get(docs->inner);
    if (code == 0)
    {
        docs->cur = docs->inner.begin();
    }
    return code;
}

*/