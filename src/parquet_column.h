#ifndef VECTRA_PARQUET_COLUMN_H
#define VECTRA_PARQUET_COLUMN_H

/*
 * Parquet column decoding: one column chunk, page by page, into VecArrays.
 *
 * A PqLeaf describes an output column: which leaf of the Parquet schema it
 * reads, its definition/repetition depth, and how the physical values convert
 * to a vectra type (PqConv). A PqCursor walks one column chunk of one row
 * group: it holds the chunk's compressed bytes, decodes one page at a time
 * (dictionary, levels, values), and hands out rows on request, so decoded
 * memory is bounded by the batch plus one page regardless of row-group size.
 *
 * Cursors never raise an R error: every failure returns -1 with a message in
 * the cursor's `err`, so they can run inside an OpenMP region (one column per
 * thread) and the caller raises after the region.
 */

#include "types.h"
#include "parquet_codec.h"
#include <stdint.h>
#include <stddef.h>

/* Physical -> output conversion. */
typedef enum {
    PQC_BOOL = 0,     /* BOOLEAN -> bool */
    PQC_I32,          /* INT32 (signed, or unsigned <= 16 bits) -> int32 */
    PQC_U32,          /* INT32 read as uint32 -> int64 */
    PQC_I64,          /* INT64 -> int64 */
    PQC_U64,          /* INT64 read as uint64 -> double */
    PQC_INT_DIV,      /* INT32/INT64 -> double, divided by `div` (dates,
                         times, timestamps, decimals) */
    PQC_INT96,        /* INT96 timestamp -> double seconds since epoch */
    PQC_FLOAT,        /* FLOAT -> double */
    PQC_DOUBLE,       /* DOUBLE -> double */
    PQC_STRING,       /* BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY bytes -> string */
    PQC_DEC_BYTES,    /* big-endian two's-complement decimal -> double */
    PQC_FLOAT16,      /* FIXED_LEN_BYTE_ARRAY(2) half float -> double */
    PQC_UUID          /* FIXED_LEN_BYTE_ARRAY(16) -> canonical UUID text */
} PqConv;

typedef struct {
    char    *name;            /* output column name */
    int      leaf;            /* leaf ordinal = column-chunk index in a row group */
    int      phys;            /* physical type */
    int32_t  type_length;     /* FIXED_LEN_BYTE_ARRAY width */
    int      max_def, max_rep;
    int      list_null_def;   /* list column: def below this = NA row */
    int      list_elem_def;   /* list column: def at/above this = one element */
    PqConv   conv;
    double   div;             /* PQC_INT_DIV / PQC_DEC_BYTES divisor */
    VecType  out_type;        /* VEC_STRING for list columns */
    char    *annotation;      /* "Date", "POSIXct|UTC", ... or NULL */
    char    *unsupported;     /* reason the column cannot be read, or NULL */
    int      stats_ok;        /* chunk min/max are usable for zone maps */
} PqLeaf;

/* Decoded values of one page (or of the dictionary). */
typedef struct {
    int             kind;     /* PQV_* in parquet_column.c */
    int64_t         n;
    const uint8_t  *raw;      /* fixed-width little-endian values */
    int             width;
    uint8_t        *owned;    /* buffer `raw` points into when owned */
    uint8_t        *u8;       /* booleans, one byte each */
    int64_t        *i64;      /* delta-decoded integers */
    const uint8_t **sp;       /* byte-array values: pointer + length */
    uint32_t       *sl;
    uint8_t        *sbuf;     /* reconstructed DELTA_BYTE_ARRAY bytes */
    uint32_t       *idx;      /* dictionary indices */
} PqVals;

typedef struct {
    const PqLeaf *leaf;
    uint8_t      *chunk;       /* compressed column chunk (owned) */
    size_t        chunk_len;
    size_t        pos;         /* next page header */
    int           codec;
    PqCodecCtx    codec_ctx;

    uint8_t      *dict_buf;    /* decompressed dictionary page */
    uint8_t      *dict_out;    /* dictionary converted to the output type */
    PqVals        dict;
    int           has_dict;

    uint8_t      *page_buf;    /* decompressed current data page */
    uint32_t     *def, *rep;   /* page levels (NULL when max level is 0) */
    int64_t       n_levels, lvl_pos;
    PqVals        vals;
    uint8_t      *page_out;    /* page values converted to the output type */
    int64_t       val_pos;

    char         *list_sep;    /* list column element separator (borrowed) */
    char          err[256];
} PqCursor;

/* Start a cursor over a chunk. Takes ownership of `chunk`. */
void pq_cursor_init(PqCursor *c, const PqLeaf *leaf, int codec,
                    uint8_t *chunk, size_t chunk_len, const char *list_sep);
void pq_cursor_free(PqCursor *c);

/* Decode the next n rows into *out, which the caller allocated with
   vec_array_alloc(leaf->out_type, n) (all rows NA). Allocates nothing that can
   raise, so it is safe inside an OpenMP region. Returns 0, or -1 with c->err
   set. */
int pq_cursor_read(PqCursor *c, int64_t n, VecArray *out);

/* Decode a plain-encoded statistics bound to a double / int64 / string prefix
   usable by the zone-map check. Returns 0 when the bound cannot be used. */
int pq_stat_to_double(const PqLeaf *leaf, const uint8_t *b, uint32_t len,
                      double *out);
int pq_stat_to_int64(const PqLeaf *leaf, const uint8_t *b, uint32_t len,
                     int64_t *out);

#endif /* VECTRA_PARQUET_COLUMN_H */
