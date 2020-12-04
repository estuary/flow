#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Opaque pointer for a Service instance in the ABI.
 */
typedef struct {
  uint8_t _private[0];
} ServiceImpl;

/**
 * Output frame produced by a Service.
 */
typedef struct {
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
typedef struct {
  ServiceImpl *svc_impl;
  uint8_t *arena_ptr;
  uintptr_t arena_len;
  uintptr_t arena_cap;
  Out *out_ptr;
  uintptr_t out_len;
  uintptr_t out_cap;
  uint8_t *err_ptr;
  uintptr_t err_len;
  uintptr_t err_cap;
} Channel;

/**
 * Input frame produced from CGO, which is a single service invocation.
 * 16 bytes, or 1/4 of a typical cache line.
 */
typedef struct {
  const uint8_t *data_ptr;
  uint32_t data_len;
  uint32_t code;
} In1;

/**
 * Four invocations, composed into one struct.
 * 64 bytes, or one typical cache line.
 */
typedef struct {
  In1 in0;
  In1 in1;
  In1 in2;
  In1 in3;
} In4;

/**
 * Sixteen invocations, composed into one struct.
 * 256 bytes, or four typical cache lines.
 */
typedef struct {
  In4 in0;
  In4 in1;
  In4 in2;
  In4 in3;
} In16;

Channel *extractor_create(void);

void extractor_invoke1(Channel *ch, In1 i);

void extractor_invoke4(Channel *ch, In4 i);

void extractor_invoke16(Channel *ch, In16 i);

void extractor_drop(Channel *ch);

Channel *upper_case_create(void);

void upper_case_invoke1(Channel *ch, In1 i);

void upper_case_invoke4(Channel *ch, In4 i);

void upper_case_invoke16(Channel *ch, In16 i);

void upper_case_drop(Channel *ch);

ServiceImpl *create_upper_case_naive(void);

uint32_t upper_case_naive(ServiceImpl *svc,
                          uint32_t _code,
                          const uint8_t *in_ptr,
                          uint32_t in_len,
                          const uint8_t **out_ptr,
                          uint32_t *out_len);
