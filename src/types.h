#ifndef VECTRA_TYPES_H
#define VECTRA_TYPES_H

#include <stdint.h>
#include <stddef.h>

typedef enum {
    VEC_INT64  = 0,
    VEC_DOUBLE = 1,
    VEC_BOOL   = 2,
    VEC_STRING = 3
} VecType;

static inline const char *vec_type_name(VecType t) {
    switch (t) {
    case VEC_INT64:  return "int64";
    case VEC_DOUBLE: return "double";
    case VEC_BOOL:   return "bool";
    case VEC_STRING: return "string";
    }
    return "unknown";
}

/*
 * VecArray: columnar array for a single column of data.
 *
 * Ownership semantics for VEC_STRING arrays:
 *   - offsets[] is ALWAYS owned by this array (freed by vec_array_free).
 *   - data may be owned (owns_data==1) or borrowed (owns_data==0).
 *   - When borrowed, the external owner (e.g. KeyArena.str_data) must
 *     outlive this array.  vec_array_free will skip freeing data.
 *   - For non-string types, owns_data is always 1 and has no effect.
 *
 * Construction:
 *   - vec_array_alloc() and vec_builder_finish() set owns_data=1.
 *   - Code that borrows string data must explicitly set owns_data=0.
 *
 * Copying:
 *   - Struct assignment (arr2 = arr1) copies the flag.  If the original
 *     owns its data, only ONE copy may be freed; the other must have
 *     its pointers NULLed or its owns_data set to 0 before free.
 *   - Prefer vec_array_alloc + memcpy for deep copies.
 */
typedef struct {
    VecType   type;
    int64_t   length;
    uint8_t  *validity;   /* bit-packed: bit i=1 means valid */
    uint8_t   owns_data;  /* 1 = owns buf.str.data (default);
                             0 = borrowed, vec_array_free skips str data.
                             Only meaningful for VEC_STRING; always 1
                             for fixed-width types. */
    union {
        int64_t  *i64;
        double   *dbl;
        uint8_t  *bln;    /* 0/1 values */
        struct {
            int64_t *offsets;   /* length+1 entries; always owned */
            char    *data;      /* may be borrowed (see owns_data) */
            int64_t  data_len;
        } str;
    } buf;
} VecArray;

typedef struct {
    int64_t    n_rows;     /* physical row count in underlying arrays */
    int        n_cols;
    char     **col_names;
    VecArray  *columns;

    /* Selection vector: if non-NULL, logical rows are sel[0..sel_n-1].
       Each entry is a physical row index into the underlying arrays.
       If NULL, logical rows = physical rows 0..n_rows-1. */
    int32_t   *sel;
    int32_t    sel_n;      /* number of selected rows */
} VecBatch;

/* Logical row count (respects selection vector) */
static inline int64_t vec_batch_logical_rows(const VecBatch *b) {
    return b->sel ? (int64_t)b->sel_n : b->n_rows;
}

/* Map logical row index to physical row index */
static inline int64_t vec_batch_physical_row(const VecBatch *b, int64_t li) {
    return b->sel ? (int64_t)b->sel[li] : li;
}

typedef struct {
    int       n_cols;
    char    **col_names;
    VecType  *col_types;
} VecSchema;

/* Pull-based plan node */
typedef struct VecNode VecNode;
typedef VecBatch* (*NextBatchFn)(VecNode *self);
typedef void      (*FreeFn)(VecNode *self);

struct VecNode {
    NextBatchFn   next_batch;
    FreeFn        free_node;
    VecSchema     output_schema;
    const char   *kind;       /* node type name for explain() */
};

#endif /* VECTRA_TYPES_H */
