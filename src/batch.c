#include "batch.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

VecBatch *vec_batch_alloc(int n_cols, int64_t n_rows) {
    VecBatch *b = (VecBatch *)calloc(1, sizeof(VecBatch));
    if (!b) vectra_error("failed to allocate VecBatch");
    b->n_cols = n_cols;
    b->n_rows = n_rows;
    b->columns = (VecArray *)calloc((size_t)n_cols, sizeof(VecArray));
    if (!b->columns && n_cols > 0) vectra_error("failed to allocate batch columns");
    b->col_names = (char **)calloc((size_t)n_cols, sizeof(char *));
    if (!b->col_names && n_cols > 0) vectra_error("failed to allocate batch col_names");
    return b;
}

void vec_batch_free(VecBatch *batch) {
    if (!batch) return;
    for (int i = 0; i < batch->n_cols; i++) {
        vec_array_free(&batch->columns[i]);
        free(batch->col_names[i]);
    }
    free(batch->columns);
    free(batch->col_names);
    free(batch->sel);
    free(batch);
}

void vec_batch_set_names(VecBatch *batch, char **names) {
    for (int i = 0; i < batch->n_cols; i++) {
        free(batch->col_names[i]);
        batch->col_names[i] = (char *)malloc(strlen(names[i]) + 1);
        if (!batch->col_names[i]) vectra_error("failed to allocate column name");
        strcpy(batch->col_names[i], names[i]);
    }
}

VecBatch *vec_batch_compact(VecBatch *batch) {
    if (!batch || !batch->sel) return batch;

    int32_t n_sel = batch->sel_n;
    VecBatch *out = vec_batch_alloc(batch->n_cols, (int64_t)n_sel);
    vec_batch_set_names(out, batch->col_names);

    for (int c = 0; c < batch->n_cols; c++) {
        const VecArray *src = &batch->columns[c];
        VecArray dst = vec_array_alloc(src->type, (int64_t)n_sel);

        switch (src->type) {
        case VEC_INT64:
            for (int32_t j = 0; j < n_sel; j++) {
                int64_t pi = (int64_t)batch->sel[j];
                if (vec_array_is_valid(src, pi)) {
                    vec_array_set_valid(&dst, j);
                    dst.buf.i64[j] = src->buf.i64[pi];
                }
            }
            break;
        case VEC_DOUBLE:
            for (int32_t j = 0; j < n_sel; j++) {
                int64_t pi = (int64_t)batch->sel[j];
                if (vec_array_is_valid(src, pi)) {
                    vec_array_set_valid(&dst, j);
                    dst.buf.dbl[j] = src->buf.dbl[pi];
                }
            }
            break;
        case VEC_BOOL:
            for (int32_t j = 0; j < n_sel; j++) {
                int64_t pi = (int64_t)batch->sel[j];
                if (vec_array_is_valid(src, pi)) {
                    vec_array_set_valid(&dst, j);
                    dst.buf.bln[j] = src->buf.bln[pi];
                }
            }
            break;
        case VEC_STRING: {
            /* Compute total string data needed */
            int64_t total = 0;
            for (int32_t j = 0; j < n_sel; j++) {
                int64_t pi = (int64_t)batch->sel[j];
                if (vec_array_is_valid(src, pi))
                    total += src->buf.str.offsets[pi + 1] -
                             src->buf.str.offsets[pi];
            }
            free(dst.buf.str.data);
            dst.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            dst.buf.str.data_len = total;

            int64_t off = 0;
            for (int32_t j = 0; j < n_sel; j++) {
                int64_t pi = (int64_t)batch->sel[j];
                dst.buf.str.offsets[j] = off;
                if (vec_array_is_valid(src, pi)) {
                    vec_array_set_valid(&dst, j);
                    int64_t s = src->buf.str.offsets[pi];
                    int64_t slen = src->buf.str.offsets[pi + 1] - s;
                    if (slen > 0)
                        memcpy(dst.buf.str.data + off,
                               src->buf.str.data + s, (size_t)slen);
                    off += slen;
                }
            }
            dst.buf.str.offsets[n_sel] = off;
            break;
        }
        }

        out->columns[c] = dst;
    }

    vec_batch_free(batch);
    return out;
}
