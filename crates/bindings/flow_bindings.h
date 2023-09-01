#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Opaque pointer for a Service instance in the ABI.
 */
typedef struct ServiceImpl {
  uint8_t _private[0];
} ServiceImpl;

/**
 * Output frame produced by a Service.
 */
typedef struct Out {
  /**
   * Service-defined response code.
   */
  uint32_t code;
  /**
   * Begin data offset within the arena.
   */
  uint32_t begin;
  /**
   * End data offset within the arena.
   */
  uint32_t end;
} Out;

/**
 * Channel is shared between CGO and Rust, and holds details
 * about the language interconnect.
 */
typedef struct Channel {
  struct ServiceImpl *svc_impl;
  uint8_t *arena_ptr;
  uintptr_t arena_len;
  uintptr_t arena_cap;
  struct Out *out_ptr;
  uintptr_t out_len;
  uintptr_t out_cap;
  uint8_t *err_ptr;
  uintptr_t err_len;
  uintptr_t err_cap;
  uint8_t *tracing_dispatch;
} Channel;

/**
 * Input frame produced from CGO, which is a single service invocation.
 * 16 bytes, or 1/4 of a typical cache line.
 */
typedef struct In1 {
  const uint8_t *data_ptr;
  uint32_t data_len;
  uint32_t code;
} In1;

/**
 * Four invocations, composed into one struct.
 * 64 bytes, or one typical cache line.
 */
typedef struct In4 {
  struct In1 in0;
  struct In1 in1;
  struct In1 in2;
  struct In1 in3;
} In4;

/**
 * Sixteen invocations, composed into one struct.
 * 256 bytes, or four typical cache lines.
 */
typedef struct In16 {
  struct In4 in0;
  struct In4 in1;
  struct In4 in2;
  struct In4 in3;
} In16;

/**
 * Statistics related to memory allocations for the entire (rust portion) of the application. The
 * precise meaning of most fields are included in the [jemalloc man
 * page](http://jemalloc.net/jemalloc.3.html). The first group of fields in this struct can be
 * found in the man page prefixed by "stats.". These fields are all gauges that are in terms of
 * bytes.
 *
 * The `_ops_total` fields are _not_ provided by jemalloc, but instead come from instrumenting the
 * allocator to track the number of invocations. Those represent monotonic counters of the number
 * of invocations.
 */
typedef struct GlobalMemoryStats {
  uint64_t active;
  uint64_t allocated;
  uint64_t mapped;
  uint64_t metadata;
  uint64_t resident;
  uint64_t retained;
  uint64_t alloc_ops_total;
  uint64_t dealloc_ops_total;
  uint64_t realloc_ops_total;
} GlobalMemoryStats;

typedef struct TaskServiceImplPtr {
  uint8_t _private[0];
} TaskServiceImplPtr;

typedef struct TaskService {
  struct TaskServiceImplPtr *svc_ptr;
  uint8_t *err_ptr;
  uintptr_t err_len;
  uintptr_t err_cap;
} TaskService;

struct Channel *combine_create(int32_t log_level, int32_t log_dest_fd);

void combine_invoke1(struct Channel *ch, struct In1 i);

void combine_invoke4(struct Channel *ch, struct In4 i);

void combine_invoke16(struct Channel *ch, struct In16 i);

void combine_drop(struct Channel *ch);

struct Channel *extract_create(int32_t log_level, int32_t log_dest_fd);

void extract_invoke1(struct Channel *ch, struct In1 i);

void extract_invoke4(struct Channel *ch, struct In4 i);

void extract_invoke16(struct Channel *ch, struct In16 i);

void extract_drop(struct Channel *ch);

/**
 * Returns general statistics on memory allocations perfomed from within libbindings.
 */
struct GlobalMemoryStats get_memory_stats(void);

struct TaskService *new_task_service(const uint8_t *config_ptr, uint32_t config_len);

void task_service_drop(struct TaskService *svc);

struct Channel *upper_case_create(int32_t log_level, int32_t log_dest_fd);

void upper_case_invoke1(struct Channel *ch, struct In1 i);

void upper_case_invoke4(struct Channel *ch, struct In4 i);

void upper_case_invoke16(struct Channel *ch, struct In16 i);

void upper_case_drop(struct Channel *ch);

struct ServiceImpl *create_upper_case_naive(void);

uint32_t upper_case_naive(struct ServiceImpl *svc,
                          uint32_t _code,
                          const uint8_t *in_ptr,
                          uint32_t in_len,
                          const uint8_t **out_ptr,
                          uint32_t *out_len);
