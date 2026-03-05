#include "sort.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/* Context for qsort_r / comparison */
typedef struct {
    VecArray  *columns;    /* all columns of materialized data */
    int        n_keys;
    SortKey   *keys;
} SortCtx;

static int compare_rows(const SortCtx *ctx, int64_t a, int64_t b) {
    for (int k = 0; k < ctx->n_keys; k++) {
        int ci = ctx->keys[k].col_index;
        int desc = ctx->keys[k].descending;
        const VecArray *col = &ctx->columns[ci];

        int a_valid = vec_array_is_valid(col, a);
        int b_valid = vec_array_is_valid(col, b);

        /* NA last (ascending) or NA first (descending), matching R */
        if (!a_valid && !b_valid) continue;
        if (!a_valid) return desc ? -1 : 1;   /* NA sorts last in ASC */
        if (!b_valid) return desc ? 1 : -1;

        int cmp = 0;
        switch (col->type) {
        case VEC_DOUBLE: {
            double va = col->buf.dbl[a], vb = col->buf.dbl[b];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
            break;
        }
        case VEC_INT64: {
            int64_t va = col->buf.i64[a], vb = col->buf.i64[b];
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
            break;
        }
        case VEC_BOOL: {
            uint8_t va = col->buf.bln[a], vb = col->buf.bln[b];
            cmp = (int)va - (int)vb;
            break;
        }
        case VEC_STRING: {
            int64_t sa = col->buf.str.offsets[a], ea = col->buf.str.offsets[a + 1];
            int64_t sb = col->buf.str.offsets[b], eb = col->buf.str.offsets[b + 1];
            int64_t la = ea - sa, lb = eb - sb;
            int64_t minlen = la < lb ? la : lb;
            cmp = memcmp(col->buf.str.data + sa, col->buf.str.data + sb,
                         (size_t)minlen);
            if (cmp == 0) cmp = (la < lb) ? -1 : (la > lb) ? 1 : 0;
            break;
        }
        }

        if (cmp != 0) return desc ? -cmp : cmp;
    }
    return 0;
}

/* Merge sort for stable ordering */
static void merge_sort(int64_t *indices, int64_t *tmp, int64_t n,
                       const SortCtx *ctx) {
    if (n <= 1) return;
    int64_t mid = n / 2;
    merge_sort(indices, tmp, mid, ctx);
    merge_sort(indices + mid, tmp, n - mid, ctx);

    /* Merge */
    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (compare_rows(ctx, indices[i], indices[j]) <= 0)
            tmp[k++] = indices[i++];
        else
            tmp[k++] = indices[j++];
    }
    while (i < mid) tmp[k++] = indices[i++];
    while (j < n) tmp[k++] = indices[j++];
    memcpy(indices, tmp, (size_t)n * sizeof(int64_t));
}

/* Gather: reorder arrays by sorted indices */
static VecArray gather_array(const VecArray *src, const int64_t *indices,
                             int64_t n) {
    VecArray dst = vec_array_alloc(src->type, n);

    switch (src->type) {
    case VEC_INT64:
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                dst.buf.i64[i] = src->buf.i64[si];
            } else {
                vec_array_set_null(&dst, i);
            }
        }
        break;
    case VEC_DOUBLE:
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                dst.buf.dbl[i] = src->buf.dbl[si];
            } else {
                vec_array_set_null(&dst, i);
            }
        }
        break;
    case VEC_BOOL:
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                dst.buf.bln[i] = src->buf.bln[si];
            } else {
                vec_array_set_null(&dst, i);
            }
        }
        break;
    case VEC_STRING: {
        /* First pass: compute total string length */
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                total += src->buf.str.offsets[si + 1] - src->buf.str.offsets[si];
            }
        }
        free(dst.buf.str.data);
        dst.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
        dst.buf.str.data_len = total;

        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            dst.buf.str.offsets[i] = off;
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                int64_t s = src->buf.str.offsets[si];
                int64_t e = src->buf.str.offsets[si + 1];
                int64_t slen = e - s;
                memcpy(dst.buf.str.data + off, src->buf.str.data + s, (size_t)slen);
                off += slen;
            } else {
                vec_array_set_null(&dst, i);
            }
        }
        dst.buf.str.offsets[n] = off;
        break;
    }
    }

    return dst;
}

static VecBatch *sort_next_batch(VecNode *self) {
    SortNode *sn = (SortNode *)self;
    if (sn->done) return NULL;
    sn->done = 1;

    /* Materialize all child batches using builders */
    int n_cols = sn->child->output_schema.n_cols;
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(sn->child->output_schema.col_types[c]);

    VecBatch *batch;
    while ((batch = sn->child->next_batch(sn->child)) != NULL) {
        if (!batch->sel) {
            for (int c = 0; c < n_cols; c++)
                vec_builder_append_array(&builders[c], &batch->columns[c]);
        } else {
            int64_t n_logical = vec_batch_logical_rows(batch);
            for (int c = 0; c < n_cols; c++)
                vec_builder_reserve(&builders[c], n_logical);
            for (int64_t li = 0; li < n_logical; li++) {
                int64_t pi = vec_batch_physical_row(batch, li);
                for (int c = 0; c < n_cols; c++)
                    vec_builder_append_one(&builders[c],
                                           &batch->columns[c], pi);
            }
        }
        vec_batch_free(batch);
    }

    /* Finish builders into arrays */
    VecArray *columns = (VecArray *)malloc((size_t)n_cols * sizeof(VecArray));
    int64_t n_rows = builders[0].length;
    for (int c = 0; c < n_cols; c++)
        columns[c] = vec_builder_finish(&builders[c]);
    free(builders);

    if (n_rows == 0) {
        /* Empty result */
        VecBatch *result = vec_batch_alloc(n_cols, 0);
        for (int c = 0; c < n_cols; c++) {
            result->columns[c] = columns[c];
            const char *nm = sn->child->output_schema.col_names[c];
            result->col_names[c] = (char *)malloc(strlen(nm) + 1);
            strcpy(result->col_names[c], nm);
        }
        free(columns);
        return result;
    }

    /* Build sort indices */
    int64_t *indices = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    int64_t *tmp = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    for (int64_t i = 0; i < n_rows; i++) indices[i] = i;

    SortCtx ctx;
    ctx.columns = columns;
    ctx.n_keys = sn->n_keys;
    ctx.keys = sn->keys;

    merge_sort(indices, tmp, n_rows, &ctx);
    free(tmp);

    /* Gather columns by sorted indices */
    VecBatch *result = vec_batch_alloc(n_cols, n_rows);
    for (int c = 0; c < n_cols; c++) {
        result->columns[c] = gather_array(&columns[c], indices, n_rows);
        const char *nm = sn->child->output_schema.col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }

    /* Cleanup */
    free(indices);
    for (int c = 0; c < n_cols; c++)
        vec_array_free(&columns[c]);
    free(columns);

    return result;
}

static void sort_free(VecNode *self) {
    SortNode *sn = (SortNode *)self;
    sn->child->free_node(sn->child);
    free(sn->keys);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

SortNode *sort_node_create(VecNode *child, int n_keys, SortKey *keys) {
    SortNode *sn = (SortNode *)calloc(1, sizeof(SortNode));
    if (!sn) vectra_error("alloc failed for SortNode");
    sn->child = child;
    sn->n_keys = n_keys;
    sn->keys = keys;
    sn->done = 0;

    /* Output schema = child schema (same columns, just reordered) */
    sn->base.output_schema = vec_schema_copy(&child->output_schema);
    sn->base.next_batch = sort_next_batch;
    sn->base.free_node = sort_free;
    sn->base.kind = "SortNode";

    return sn;
}
