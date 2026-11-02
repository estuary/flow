#include <rocksdb/iterator.h>
#include <rocksdb/c.h>
#include <memory>

using rocksdb::Iterator;
using rocksdb::Slice;

extern "C" {

// Note: this definition is copied from github.com/facebook/rocksdb/db/c.cc:89
// We must copy, as this struct is defined in a .cc we don't have access too.
struct rocksdb_iterator_t        { Iterator*         rep; };

size_t arena_iter_next(rocksdb_iterator_t* cit, size_t arena_len, char* arena_out);

} // extern "C"

// Macro which appends rocksdb::Slice |s| to |arena_out|, prefixed by size.
#define APPEND_SLICE(s) (               \
  {                                     \
    size_t l = s.size();                \
    arena_out[0] = char((l)>>24);       \
    arena_out[1] = char((l)>>16);       \
    arena_out[2] = char((l)>>8);        \
    arena_out[3] = char((l)>>0);        \
    memcpy(arena_out + 4, s.data(), l); \
    arena_out = arena_out + 4 + l;      \
    arena_off += 4 + l;                 \
  }                                     \
)

// arena_iter_next fills up to |arena_len| bytes of |arena_out| with successive
// length-prefixed keys and values of |cit|. The input |cit| must be Valid().
// At return, the last key & value stored in |arena_out| is at the current
// iterator offset. If |arena_len| is insufficient to store the next key,
// then the iterator is still stepped but |arena_out| is left empty.
size_t arena_iter_next(rocksdb_iterator_t* cit, size_t arena_len, char* arena_out) {

  Iterator* it = cit->rep;
  size_t arena_off = 0;

  if (!it->Valid()) {
    return arena_off;
  }

  while(true) {
    it->Next();

    if (!it->Valid()) {
      break;
    }

    Slice key = it->key();
    Slice val = it->value();

    if ((arena_off + 8 + key.size() + val.size()) > arena_len) {
      // Insufficient arena buffer remains for this key/value.
      if (arena_off > 0) {
        // At least one key/value is already being returned.
        // Step backwards to save this iterator offset for later.
        it->Prev();
      }
      break;
    }

    APPEND_SLICE(key);
    APPEND_SLICE(val);
  }
  return arena_off;
}
