#ifndef VECTRA_TYPES_H
#define VECTRA_TYPES_H

#include <stdint.h>
#include <stddef.h>

typedef enum {
    VEC_INT64  = 0,
    VEC_DOUBLE = 1,
    VEC_BOOL   = 2,
    VEC_STRING = 3,
    VEC_INT8   = 4,   /* signed 8-bit  [-128, 127] */
    VEC_INT16  = 5,   /* signed 16-bit [-32768, 32767] */
    VEC_INT32  = 6    /* signed 32-bit [-2^31, 2^31-1] */
} VecType;

static inline const char *vec_type_name(VecType t) {
    switch (t) {
    case VEC_INT64:  return "int64";
    case VEC_DOUBLE: return "double";
    case VEC_BOOL:   return "bool";
    case VEC_STRING: return "string";
    case VEC_INT8:   return "int8";
    case VEC_INT16:  return "int16";
    case VEC_INT32:  return "int32";
    }
    return "unknown";
}

/* Element size in bytes for fixed-width types (0 for variable-length). */
static inline uint8_t vec_type_elem_size(VecType t) {
    switch (t) {
    case VEC_INT8:   return 1;
    case VEC_INT16:  return 2;
    case VEC_INT32:  return 4;
    case VEC_INT64:  return 8;
    case VEC_DOUBLE: return 8;
    case VEC_BOOL:   return 1;
    case VEC_STRING: return 0;
    }
    return 0;
}

/* True for any integer type (narrow or int64). */
static inline int vec_type_is_int(VecType t) {
    return t == VEC_INT8 || t == VEC_INT16 || t == VEC_INT32 || t == VEC_INT64;
}

/* True for fixed-width types (everything except string). */
static inline int vec_type_is_fixed(VecType t) {
    return t != VEC_STRING;
}

/*
 * VecStrDict: deferred dictionary form for a VEC_STRING column.
 *
 * A string column stored on disk as a TDC_MODEL_DICT_1D block can be decoded
 * to (unique values + per-row indices) instead of a flat per-row buffer. The
 * collect direct-read path opts into this so it interns each unique value once
 * and fills the R character vector by index, skipping the per-row CHARSXP work
 * that dominates a wide-duplicated string collect.
 *
 * When VecArray.str_dict != NULL:
 *   - the array is VEC_STRING;
 *   - buf.str stays the placeholder installed by vec_array_alloc (unused);
 *   - validity carries NA rows as usual (the dict decode does not);
 *   - dict_offsets/dict_data hold the unique values, indices[] the per-row
 *     dictionary index (every index is < dict_count, validated on decode).
 *
 * Ownership: str_dict and its three buffers are owned by the array and freed
 * by vec_array_free. This representation is produced only by the collect
 * direct-read decode and consumed only by the collect string fill; it never
 * reaches a filter/join/project/window node, so no other consumer needs to
 * understand it.
 */
typedef struct {
    uint32_t  dict_count;     /* number of unique dictionary entries */
    uint32_t *dict_offsets;   /* dict_count + 1 byte offsets into dict_data */
    uint8_t  *dict_data;      /* concatenated unique values */
    size_t    dict_data_size; /* == dict_offsets[dict_count] */
    uint32_t *indices;        /* length entries; dictionary index per row */
} VecStrDict;

/*
 * VecArray: columnar array for a single column of data.
 *
 * Two distinct flags govern the data buffer:
 *
 *   owns_data — free responsibility.
 *     1 (default): vec_array_free() releases the buffer.
 *     0: external owner; vec_array_free() leaves it alone. Two unrelated
 *        producers set this: (a) string arenas (KeyArena.str_data) shared
 *        across many arrays, and (b) the tdc-backed direct-write decoder
 *        path that materializes into a caller-provided buffer.
 *
 *   data_borrowed — provenance signal: "decoder wrote into a caller-supplied
 *     direct buffer (vtr1_read_rowgroup_tdc_ex / parallel reader)."
 *     1 implies owns_data == 0, but the converse does NOT hold (string
 *     arenas also set owns_data == 0). Callers that need to distinguish
 *     "decoder honored my direct_buf" from "string data borrowed from an
 *     arena" MUST check this flag, not !owns_data.
 *
 *     Fixed-width numeric columns (DOUBLE, INT64, etc.) honor direct_bufs;
 *     variable-width strings do not — the tdc reader always allocates a
 *     heap for offsets/data and data_borrowed stays 0 there. Callers fall
 *     back to a copy when direct_bufs[i] was non-NULL but data_borrowed
 *     came back 0 (one-shot warning via the VTR_DEBUG_DIRECT env var — see
 *     collect.c).
 *
 * Ownership notes for VEC_STRING:
 *   - offsets[] is ALWAYS owned by this array (freed by vec_array_free).
 *   - data may be owned (owns_data==1) or borrowed (owns_data==0).
 *   - When borrowed, the external owner must outlive this array.
 *
 * Construction:
 *   - vec_array_alloc() and vec_builder_finish() set owns_data=1,
 *     data_borrowed=0.
 *   - Code that borrows must explicitly set owns_data=0; the direct-write
 *     decoder paths additionally set data_borrowed=1.
 *
 * Copying:
 *   - Struct assignment (arr2 = arr1) copies the flags. Only ONE copy may
 *     be freed when owns_data==1; the other must have its pointers NULLed
 *     or its owns_data set to 0 before free.
 *   - Prefer vec_array_alloc + memcpy for deep copies.
 */
typedef struct {
    VecType   type;
    int64_t   length;
    uint8_t  *validity;     /* bit-packed: bit i=1 means valid */
    uint8_t   owns_data;    /* 1 = vec_array_free() releases the buffer */
    uint8_t   data_borrowed;/* 1 = buffer was provided by the caller via the
                               direct-write decoder API (implies !owns_data).
                               Distinct from owns_data so callers can tell
                               "decoder honored my direct_buf" apart from
                               "string data borrowed from a key arena". */
    union {
        int64_t  *i64;
        int32_t  *i32;
        int16_t  *i16;
        int8_t   *i8;
        double   *dbl;
        uint8_t  *bln;    /* 0/1 values */
        struct {
            int64_t *offsets;   /* length+1 entries; always owned */
            char    *data;      /* may be borrowed (see owns_data) */
            int64_t  data_len;
        } str;
    } buf;
    /* Optional deferred-dictionary form for VEC_STRING (NULL otherwise).
       Owned by this array; see VecStrDict. When set, buf.str is unused. */
    VecStrDict *str_dict;
} VecArray;

/* Read an integer value from a VecArray, widened to int64. */
static inline int64_t vec_array_get_int(const VecArray *arr, int64_t i) {
    switch (arr->type) {
    case VEC_INT8:   return (int64_t)arr->buf.i8[i];
    case VEC_INT16:  return (int64_t)arr->buf.i16[i];
    case VEC_INT32:  return (int64_t)arr->buf.i32[i];
    case VEC_INT64:  return arr->buf.i64[i];
    default:         return 0;
    }
}

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
    char    **col_annotations;  /* optional per-column annotation (NULL entry = none) */
} VecSchema;

/* Pull-based plan node */
typedef struct VecNode VecNode;
typedef VecBatch* (*NextBatchFn)(VecNode *self);
typedef void      (*FreeFn)(VecNode *self);

/* Exact number of rows this node will emit, read off metadata without pulling
   a single batch; -1 when the query would have to run to know. Optional: a
   node that leaves it NULL reports "unknown", so a node kind that never
   implements it is over-cautious rather than wrong. Row-preserving nodes
   implement it by delegating to their child.

   Distinct from row_count_hint, which is an upper bound (a scan reports every
   stored row even when pruning or a predicate will drop some) and therefore
   cannot answer nrow(). */
typedef int64_t   (*StaticRowsFn)(const VecNode *self);

struct VecNode {
    NextBatchFn   next_batch;
    FreeFn        free_node;
    StaticRowsFn  static_rows;    /* NULL = row count not derivable */
    VecSchema     output_schema;
    const char   *kind;       /* node type name for explain() */
    int64_t       row_count_hint; /* upper-bound estimate for preallocation;
                                     <= 0 = unknown */
};

/* static_rows dispatch: the row count, or -1 when the node cannot supply it. */
int64_t vec_node_static_rows(const VecNode *node);

#endif /* VECTRA_TYPES_H */
