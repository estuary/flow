#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
  EST_OK,
  EST_UTF8_PARSE_ERROR,
  EST_MSG_JSON_PARSE_ERROR,
  EST_MSG_UUID_BAD_LOCATION,
  EST_MSG_UUID_NOT_A_STRING,
  EST_MSG_UUID_PARSE_ERROR,
  EST_JSON_PTR_NOT_ROOTED,
} est_status_t;

typedef enum {
  EST_DOES_NOT_EXIST,
  EST_NULL,
  EST_TRUE,
  EST_FALSE,
  EST_UNSIGNED,
  EST_SIGNED,
  EST_FLOAT,
  EST_STRING,
  EST_OBJECT,
  EST_ARRAY,
} est_type_t;

typedef struct est_json_ptr_t est_json_ptr_t;

typedef struct est_msg_t est_msg_t;

typedef struct {
  const est_json_ptr_t *ptr;
  est_type_t type_;
  uint64_t unsigned_;
  int64_t signed_;
  double float_;
  uint32_t begin;
  uint32_t end;
} est_extract_field_t;

typedef struct {
  uint8_t bytes[16];
} est_uuid_t;

void est_json_ptr_drop(est_json_ptr_t *p);

est_status_t est_json_ptr_new(const char *uuid_ptr, est_json_ptr_t **out);

void est_msg_drop(est_msg_t *m);

uintptr_t est_msg_extract_fields(const est_msg_t *m,
                                 est_extract_field_t *fields,
                                 uintptr_t fields_len,
                                 uint8_t *buf,
                                 uintptr_t buf_len);

est_uuid_t est_msg_get_uuid(const est_msg_t *m);

uintptr_t est_msg_marshal_json(const est_msg_t *m, uint8_t *buf, uintptr_t buf_len);

est_msg_t *est_msg_new(const est_json_ptr_t *uuid_ptr);

void est_msg_set_uuid(est_msg_t *m, est_uuid_t to);

uintptr_t est_status_description(est_status_t status, uint8_t *out, uintptr_t out_cap);
