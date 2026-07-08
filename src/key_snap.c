#include "key_snap.h"
#include "array.h"
#include <stdlib.h>
#include <string.h>

KeySnap snap_create(int n_keys, const VecType *types) {
    KeySnap s;
    memset(&s, 0, sizeof(s));
    s.n_keys = n_keys;
    s.types = (VecType *)malloc((size_t)n_keys * sizeof(VecType));
    memcpy(s.types, types, (size_t)n_keys * sizeof(VecType));
    s.i64  = (int64_t *)calloc((size_t)n_keys, sizeof(int64_t));
    s.dbl  = (double  *)calloc((size_t)n_keys, sizeof(double));
    s.bln  = (uint8_t *)calloc((size_t)n_keys, sizeof(uint8_t));
    s.str_offs = (int64_t *)calloc((size_t)(n_keys + 1), sizeof(int64_t));
    s.valid = (uint8_t *)calloc((size_t)n_keys, sizeof(uint8_t));
    return s;
}

void snap_free(KeySnap *s) {
    free(s->types); free(s->i64); free(s->dbl); free(s->bln);
    free(s->str_data); free(s->str_offs); free(s->valid);
    memset(s, 0, sizeof(*s));
}

/* Check if row in batch matches the snapshot */
int snap_matches(const KeySnap *s, const VecBatch *batch,
                 int64_t row, const int *key_indices) {
    if (!s->initialized) return 0;
    for (int k = 0; k < s->n_keys; k++) {
        const VecArray *col = &batch->columns[key_indices[k]];
        int cur_valid = vec_array_is_valid(col, row);
        if (cur_valid != s->valid[k]) return 0;
        if (!cur_valid) continue; /* both NA = equal */
        switch (s->types[k]) {
        case VEC_INT64:
            if (col->buf.i64[row] != s->i64[k]) return 0;
            break;
        case VEC_INT32:
            if ((int64_t)col->buf.i32[row] != s->i64[k]) return 0;
            break;
        case VEC_INT16:
            if ((int64_t)col->buf.i16[row] != s->i64[k]) return 0;
            break;
        case VEC_INT8:
            if ((int64_t)col->buf.i8[row] != s->i64[k]) return 0;
            break;
        case VEC_DOUBLE:
            if (col->buf.dbl[row] != s->dbl[k]) return 0;
            break;
        case VEC_BOOL:
            if (col->buf.bln[row] != s->bln[k]) return 0;
            break;
        case VEC_STRING: {
            int64_t cs = col->buf.str.offsets[row];
            int64_t ce = col->buf.str.offsets[row + 1];
            int64_t clen = ce - cs;
            int64_t slen = s->str_offs[k + 1] - s->str_offs[k];
            if (clen != slen) return 0;
            if (clen > 0 && s->str_data &&
                memcmp(col->buf.str.data + cs,
                       s->str_data + s->str_offs[k], (size_t)clen) != 0)
                return 0;
            break;
        }
        }
    }
    return 1;
}

/* Capture the current row's keys into the snapshot */
void snap_update(KeySnap *s, const VecBatch *batch,
                 int64_t row, const int *key_indices) {
    s->initialized = 1;

    /* First pass: compute total string length */
    int64_t str_total = 0;
    for (int k = 0; k < s->n_keys; k++) {
        const VecArray *col = &batch->columns[key_indices[k]];
        s->valid[k] = (uint8_t)vec_array_is_valid(col, row);
        if (!s->valid[k]) continue;
        switch (s->types[k]) {
        case VEC_INT64:  s->i64[k] = col->buf.i64[row]; break;
        case VEC_INT32:  s->i64[k] = (int64_t)col->buf.i32[row]; break;
        case VEC_INT16:  s->i64[k] = (int64_t)col->buf.i16[row]; break;
        case VEC_INT8:   s->i64[k] = (int64_t)col->buf.i8[row]; break;
        case VEC_DOUBLE: s->dbl[k] = col->buf.dbl[row]; break;
        case VEC_BOOL:   s->bln[k] = col->buf.bln[row]; break;
        case VEC_STRING: {
            int64_t cs = col->buf.str.offsets[row];
            int64_t ce = col->buf.str.offsets[row + 1];
            str_total += ce - cs;
            break;
        }
        }
    }

    /* Ensure string buffer capacity */
    if (str_total > s->str_cap) {
        s->str_cap = str_total > 256 ? str_total * 2 : 256;
        s->str_data = (char *)realloc(s->str_data, (size_t)s->str_cap);
    }

    /* Second pass: copy string data */
    int64_t off = 0;
    for (int k = 0; k < s->n_keys; k++) {
        s->str_offs[k] = off;
        if (s->types[k] == VEC_STRING && s->valid[k]) {
            const VecArray *col = &batch->columns[key_indices[k]];
            int64_t cs = col->buf.str.offsets[row];
            int64_t ce = col->buf.str.offsets[row + 1];
            int64_t len = ce - cs;
            if (len > 0)
                memcpy(s->str_data + off, col->buf.str.data + cs, (size_t)len);
            off += len;
        }
    }
    s->str_offs[s->n_keys] = off;
}
