#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Output
 */
typedef struct {
  uint32_t code;
  uint32_t begin;
  uint32_t end;
} Out;

/**
 * Opaque pointer for a Service instance.
 */
typedef struct {
  uint8_t _private[0];
} ServiceImpl;

/**
 * Channel is shared between CGO and Rust, and holds details
 * about the language interconnect.
 */
typedef struct {
  uint8_t *arena_ptr;
  uintptr_t arena_len;
  uintptr_t arena_cap;
  Out *out_ptr;
  uintptr_t out_len;
  uintptr_t out_cap;
  ServiceImpl *svc_impl;
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
  In1 in00;
  In1 in01;
  In1 in02;
  In1 in03;
  In1 in04;
  In1 in05;
  In1 in06;
  In1 in07;
  In1 in08;
  In1 in09;
  In1 in10;
  In1 in11;
  In1 in12;
  In1 in13;
  In1 in14;
  In1 in15;
} In16;

Channel *upper_case_create(void);

void upper_case_invoke1(Channel *ch, In1 i);

void upper_case_invoke4(Channel *ch, In4 i);

void upper_case_invoke16(Channel *ch, In16 i);

void upper_case_drop(Channel *ch);

/**
 * upper_case_naive is not part of UpperCase's service interface.
 * It's here for comparative benchmarking with a more traditional CGO call style.
 */
uint32_t upper_case_naive(uint32_t _code,
                          const uint8_t *in_ptr,
                          uint32_t in_len,
                          const uint8_t **out_ptr,
                          uint32_t *out_len);
