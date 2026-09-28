#ifndef VECTRA_PARQUET_THRIFT_H
#define VECTRA_PARQUET_THRIFT_H

/*
 * Thrift compact-protocol reader for Parquet metadata.
 *
 * Parquet stores its footer (FileMetaData) and every page header in the
 * Thrift compact protocol. This reader walks a byte range with every read
 * bounds-checked: a short or malformed buffer sets `err` and each later read
 * returns zero, so a decoder can read a whole struct and check `err` once.
 * Nesting is capped (PQT_MAX_DEPTH) so a crafted footer cannot recurse the
 * stack away while skipping unknown fields.
 */

#include <stdint.h>
#include <stddef.h>

#define PQT_MAX_DEPTH 64

/* Compact-protocol wire types. */
enum {
    PQT_STOP   = 0,
    PQT_TRUE   = 1,
    PQT_FALSE  = 2,
    PQT_BYTE   = 3,
    PQT_I16    = 4,
    PQT_I32    = 5,
    PQT_I64    = 6,
    PQT_DOUBLE = 7,
    PQT_BINARY = 8,
    PQT_LIST   = 9,
    PQT_SET    = 10,
    PQT_MAP    = 11,
    PQT_STRUCT = 12
};

typedef struct {
    const uint8_t *p;
    const uint8_t *end;
    int            err;
    int            depth;
} PqThrift;

static inline void pqt_init(PqThrift *t, const uint8_t *p, size_t n) {
    t->p = p;
    t->end = p + n;
    t->err = 0;
    t->depth = 0;
}

uint64_t pqt_varint(PqThrift *t);
int64_t  pqt_zigzag(PqThrift *t);
uint8_t  pqt_byte(PqThrift *t);

/* Next struct field header. Returns 0 at the STOP marker (or on error),
   1 otherwise with *id and *type set. *last_id carries the previous field id
   of the enclosing struct (start it at 0). */
int pqt_field(PqThrift *t, int16_t *last_id, int16_t *id, int *type);

/* Length-prefixed byte string; returns a pointer into the buffer and its
   length, or NULL with err set when it runs past the end. */
const uint8_t *pqt_binary(PqThrift *t, uint32_t *len);

/* List/set header: element count and element wire type. The count is checked
   against the bytes left, since every element occupies at least one byte. */
uint32_t pqt_list(PqThrift *t, int *elem_type);

/* Skip a struct field value of the given wire type (a boolean field carries
   its value in the type and has no payload). */
void pqt_skip(PqThrift *t, int type);

/* Skip one list element of the given wire type (a boolean element is one
   byte). */
void pqt_skip_elem(PqThrift *t, int type);

/* Read an integer field of any integer wire type as int64. */
int64_t pqt_int(PqThrift *t, int type);

#endif /* VECTRA_PARQUET_THRIFT_H */
