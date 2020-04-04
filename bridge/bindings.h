#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
  OK,
  UTF8_PARSE_ERROR,
  MSG_JSON_PARSE_ERROR,
  MSG_UUID_BAD_LOCATION,
  MSG_UUID_NOT_A_STRING,
  MSG_UUID_PARSE_ERROR,
  JSON_PTR_NOT_ROOTED,
} status_t;

typedef struct builder_t builder_t;

typedef struct message_t message_t;

typedef struct {
  uint8_t *ptr;
  uintptr_t len;
  uintptr_t cap;
} buffer_t;

typedef struct {
  uint8_t bytes[16];
} uuid_t;

void buffer_drop(buffer_t b);

message_t *msg_builder_build(const builder_t *b);

void msg_builder_drop(builder_t *b);

status_t msg_builder_new(const char *uuid_ptr, builder_t **out);

void msg_drop(message_t *m);

uuid_t msg_get_uuid(const message_t *m);

buffer_t msg_marshal_json(const message_t *m);

void msg_set_uuid(message_t *m, uuid_t to);

uintptr_t status_description(status_t status, uint8_t *out, uintptr_t out_cap);
