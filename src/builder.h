#ifndef VECTRA_BUILDER_H
#define VECTRA_BUILDER_H

#include "types.h"

/* Growable array builder for collecting results */
typedef struct {
    VecType  type;
    int64_t  length;
    int64_t  capacity;
    uint8_t *validity;
    union {
        int64_t *i64;
        double  *dbl;
        uint8_t *bln;
    } buf;
    /* String builder uses separate buffers */
    int64_t *str_offsets;
    char    *str_data;
    int64_t  str_data_len;
    int64_t  str_data_cap;
} VecArrayBuilder;

/* Initialize a builder for a given type */
VecArrayBuilder vec_builder_init(VecType type);

/* Pre-allocate capacity for at least `extra` more rows.
   Avoids repeated realloc in tight loops. */
void vec_builder_reserve(VecArrayBuilder *b, int64_t extra);

/* Append a full VecArray to the builder */
void vec_builder_append_array(VecArrayBuilder *b, const VecArray *arr);

/* Append a single value from arr at row index */
void vec_builder_append_one(VecArrayBuilder *b, const VecArray *arr, int64_t row);

/* Append a single NA value */
void vec_builder_append_na(VecArrayBuilder *b);

/* Append n NA values at once */
void vec_builder_append_na_n(VecArrayBuilder *b, int64_t n);

/* Append the same row from arr `count` times (for join fan-out).
   Only deep-copies strings once if count > 0. */
void vec_builder_append_repeat(VecArrayBuilder *b, const VecArray *arr,
                               int64_t row, int64_t count);

/* Finish builder into a VecArray (builder is consumed) */
VecArray vec_builder_finish(VecArrayBuilder *b);

/* Free builder without finishing */
void vec_builder_free(VecArrayBuilder *b);

#endif /* VECTRA_BUILDER_H */
