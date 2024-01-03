/*
#include "simd-doc.h"

std::unique_ptr<parser> new_parser()
{
    return std::make_unique<parser>();
}

std::unique_ptr<document_stream> parse_many(
    std::unique_ptr<parser> &parser,
    rust::Slice<uint8_t> padded_vec,
    size_t batch_size)
{
    throw new std::runtime_error("whoops");

    return std::make_unique<document_stream>(
        parser->parse_many(padded_vec.data(), padded_vec.size(), batch_size));
}
*/