#include "limit.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

static VecBatch *limit_next_batch(VecNode *self) {
    LimitNode *ln = (LimitNode *)self;
    if (ln->rows_emitted >= ln->max_rows) return NULL;

    VecBatch *batch = ln->child->next_batch(ln->child);
    if (!batch) return NULL;

    int64_t remaining = ln->max_rows - ln->rows_emitted;
    if (batch->n_rows <= remaining) {
        ln->rows_emitted += batch->n_rows;
        return batch;
    }

    /* Need to truncate this batch */
    int64_t keep = remaining;
    VecBatch *out = vec_batch_alloc(batch->n_cols, keep);
    for (int c = 0; c < batch->n_cols; c++) {
        VecArray *src = &batch->columns[c];
        VecArray dst = vec_array_alloc(src->type, keep);

        switch (src->type) {
        case VEC_INT64:
            for (int64_t i = 0; i < keep; i++) {
                if (vec_array_is_valid(src, i)) {
                    vec_array_set_valid(&dst, i);
                    dst.buf.i64[i] = src->buf.i64[i];
                } else {
                    vec_array_set_null(&dst, i);
                }
            }
            break;
        case VEC_DOUBLE:
            for (int64_t i = 0; i < keep; i++) {
                if (vec_array_is_valid(src, i)) {
                    vec_array_set_valid(&dst, i);
                    dst.buf.dbl[i] = src->buf.dbl[i];
                } else {
                    vec_array_set_null(&dst, i);
                }
            }
            break;
        case VEC_BOOL:
            for (int64_t i = 0; i < keep; i++) {
                if (vec_array_is_valid(src, i)) {
                    vec_array_set_valid(&dst, i);
                    dst.buf.bln[i] = src->buf.bln[i];
                } else {
                    vec_array_set_null(&dst, i);
                }
            }
            break;
        case VEC_STRING: {
            int64_t total = 0;
            for (int64_t i = 0; i < keep; i++) {
                if (vec_array_is_valid(src, i))
                    total += src->buf.str.offsets[i + 1] - src->buf.str.offsets[i];
            }
            free(dst.buf.str.data);
            dst.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
            dst.buf.str.data_len = total;
            int64_t off = 0;
            for (int64_t i = 0; i < keep; i++) {
                dst.buf.str.offsets[i] = off;
                if (vec_array_is_valid(src, i)) {
                    vec_array_set_valid(&dst, i);
                    int64_t s = src->buf.str.offsets[i];
                    int64_t slen = src->buf.str.offsets[i + 1] - s;
                    memcpy(dst.buf.str.data + off, src->buf.str.data + s, (size_t)slen);
                    off += slen;
                } else {
                    vec_array_set_null(&dst, i);
                }
            }
            dst.buf.str.offsets[keep] = off;
            break;
        }
        }

        out->columns[c] = dst;
        const char *nm = batch->col_names[c];
        out->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(out->col_names[c], nm);
    }

    vec_batch_free(batch);
    ln->rows_emitted += keep;
    return out;
}

static void limit_free(VecNode *self) {
    LimitNode *ln = (LimitNode *)self;
    ln->child->free_node(ln->child);
    vec_schema_free(&ln->base.output_schema);
    free(ln);
}

LimitNode *limit_node_create(VecNode *child, int64_t max_rows) {
    LimitNode *ln = (LimitNode *)calloc(1, sizeof(LimitNode));
    if (!ln) vectra_error("alloc failed for LimitNode");
    ln->child = child;
    ln->max_rows = max_rows;
    ln->rows_emitted = 0;

    ln->base.output_schema = vec_schema_copy(&child->output_schema);
    ln->base.next_batch = limit_next_batch;
    ln->base.free_node = limit_free;
    ln->base.kind = "LimitNode";

    return ln;
}
