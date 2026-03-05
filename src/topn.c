#include "topn.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>

/*
 * Top-N via binary heap.
 *
 * For ascending sort (smallest N): use a MAX-heap of size N.
 * New row enters if it's smaller than heap max. After all rows consumed,
 * extract-sort the heap for final ordered output.
 *
 * The heap stores row indices into a materialized column set.
 * We materialize lazily: append all rows into builders, but the heap
 * only tracks which N to keep. At the end, gather only those N rows
 * and sort them.
 *
 * This is O(n log k) time and O(n) temporary storage for the full
 * materialization + O(k) for the heap. A streaming approach that avoids
 * materializing non-heap rows would be O(k) memory but requires
 * copying rows into the heap, which is complex for strings. The
 * materialize-then-select approach is simpler and still much faster
 * than full sort for small k.
 */

/* Comparison context (same structure as sort.c) */
typedef struct {
    VecArray  *columns;
    int        n_keys;
    SortKey   *keys;
} TopNCtx;

static int compare_rows_topn(const TopNCtx *ctx, int64_t a, int64_t b) {
    for (int k = 0; k < ctx->n_keys; k++) {
        int ci = ctx->keys[k].col_index;
        int desc = ctx->keys[k].descending;
        const VecArray *col = &ctx->columns[ci];

        int a_valid = vec_array_is_valid(col, a);
        int b_valid = vec_array_is_valid(col, b);

        if (!a_valid && !b_valid) continue;
        if (!a_valid) return desc ? -1 : 1;
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

/* Binary heap operations.
   For ascending sort: max-heap (parent >= children in sort order).
   We want to keep the SMALLEST N, so the heap root is the LARGEST
   of those N. A new row replaces the root if it's smaller. */

static void heap_sift_down(int64_t *heap, int64_t size,
                            int64_t pos, const TopNCtx *ctx) {
    while (1) {
        int64_t largest = pos;
        int64_t left = 2 * pos + 1;
        int64_t right = 2 * pos + 2;

        if (left < size &&
            compare_rows_topn(ctx, heap[left], heap[largest]) > 0)
            largest = left;
        if (right < size &&
            compare_rows_topn(ctx, heap[right], heap[largest]) > 0)
            largest = right;

        if (largest == pos) break;
        int64_t tmp = heap[pos];
        heap[pos] = heap[largest];
        heap[largest] = tmp;
        pos = largest;
    }
}

static void heap_sift_up(int64_t *heap, int64_t pos, const TopNCtx *ctx) {
    while (pos > 0) {
        int64_t parent = (pos - 1) / 2;
        if (compare_rows_topn(ctx, heap[pos], heap[parent]) <= 0)
            break;
        int64_t tmp = heap[pos];
        heap[pos] = heap[parent];
        heap[parent] = tmp;
        pos = parent;
    }
}

/* Merge sort for final ordering of the k selected rows */
static void topn_merge_sort(int64_t *indices, int64_t *tmp, int64_t n,
                             const TopNCtx *ctx) {
    if (n <= 1) return;
    int64_t mid = n / 2;
    topn_merge_sort(indices, tmp, mid, ctx);
    topn_merge_sort(indices + mid, tmp, n - mid, ctx);

    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (compare_rows_topn(ctx, indices[i], indices[j]) <= 0)
            tmp[k++] = indices[i++];
        else
            tmp[k++] = indices[j++];
    }
    while (i < mid) tmp[k++] = indices[i++];
    while (j < n) tmp[k++] = indices[j++];
    memcpy(indices, tmp, (size_t)n * sizeof(int64_t));
}

/* Gather: reorder arrays by indices */
static VecArray topn_gather(const VecArray *src, const int64_t *indices,
                             int64_t n) {
    VecArray dst = vec_array_alloc(src->type, n);

    switch (src->type) {
    case VEC_INT64:
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                dst.buf.i64[i] = src->buf.i64[si];
            }
        }
        break;
    case VEC_DOUBLE:
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                dst.buf.dbl[i] = src->buf.dbl[si];
            }
        }
        break;
    case VEC_BOOL:
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si)) {
                vec_array_set_valid(&dst, i);
                dst.buf.bln[i] = src->buf.bln[si];
            }
        }
        break;
    case VEC_STRING: {
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si))
                total += src->buf.str.offsets[si + 1] -
                         src->buf.str.offsets[si];
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
                int64_t slen = src->buf.str.offsets[si + 1] - s;
                if (slen > 0)
                    memcpy(dst.buf.str.data + off,
                           src->buf.str.data + s, (size_t)slen);
                off += slen;
            }
        }
        dst.buf.str.offsets[n] = off;
        break;
    }
    }
    return dst;
}

static VecBatch *topn_next_batch(VecNode *self) {
    TopNNode *tn = (TopNNode *)self;
    if (tn->done) return NULL;
    tn->done = 1;

    int n_cols = tn->child->output_schema.n_cols;
    int64_t k = tn->limit;

    /* Phase 1: materialize all child rows into arrays */
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(tn->child->output_schema.col_types[c]);

    VecBatch *batch;
    while ((batch = tn->child->next_batch(tn->child)) != NULL) {
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

    VecArray *columns = (VecArray *)malloc((size_t)n_cols * sizeof(VecArray));
    int64_t n_rows = builders[0].length;
    for (int c = 0; c < n_cols; c++)
        columns[c] = vec_builder_finish(&builders[c]);
    free(builders);

    if (n_rows == 0 || k <= 0) {
        int64_t out_n = 0;
        VecBatch *result = vec_batch_alloc(n_cols, out_n);
        for (int c = 0; c < n_cols; c++) {
            result->columns[c] = columns[c];
            const char *nm = tn->child->output_schema.col_names[c];
            result->col_names[c] = (char *)malloc(strlen(nm) + 1);
            strcpy(result->col_names[c], nm);
        }
        free(columns);
        return result;
    }

    /* Clamp k to actual row count */
    if (k > n_rows) k = n_rows;

    TopNCtx ctx;
    ctx.columns = columns;
    ctx.n_keys = tn->n_keys;
    ctx.keys = tn->keys;

    /* Phase 2: build max-heap of size k */
    int64_t *heap = (int64_t *)malloc((size_t)k * sizeof(int64_t));
    int64_t heap_size = 0;

    /* Fill heap with first k rows */
    for (int64_t r = 0; r < k && r < n_rows; r++) {
        heap[heap_size] = r;
        heap_size++;
        heap_sift_up(heap, heap_size - 1, &ctx);
    }

    /* Process remaining rows: if row < heap max, replace */
    for (int64_t r = k; r < n_rows; r++) {
        if (compare_rows_topn(&ctx, r, heap[0]) < 0) {
            heap[0] = r;
            heap_sift_down(heap, heap_size, 0, &ctx);
        }
    }

    /* Phase 3: sort the k heap entries for final output order */
    int64_t *tmp = (int64_t *)malloc((size_t)k * sizeof(int64_t));
    topn_merge_sort(heap, tmp, k, &ctx);
    free(tmp);

    /* Phase 4: gather columns by sorted indices */
    VecBatch *result = vec_batch_alloc(n_cols, k);
    for (int c = 0; c < n_cols; c++) {
        result->columns[c] = topn_gather(&columns[c], heap, k);
        const char *nm = tn->child->output_schema.col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }

    free(heap);
    for (int c = 0; c < n_cols; c++)
        vec_array_free(&columns[c]);
    free(columns);

    return result;
}

static void topn_free(VecNode *self) {
    TopNNode *tn = (TopNNode *)self;
    tn->child->free_node(tn->child);
    free(tn->keys);
    vec_schema_free(&tn->base.output_schema);
    free(tn);
}

TopNNode *topn_node_create(VecNode *child, int n_keys, SortKey *keys,
                            int64_t limit) {
    TopNNode *tn = (TopNNode *)calloc(1, sizeof(TopNNode));
    if (!tn) vectra_error("alloc failed for TopNNode");
    tn->child = child;
    tn->n_keys = n_keys;
    tn->keys = keys;
    tn->limit = limit;
    tn->done = 0;

    tn->base.output_schema = vec_schema_copy(&child->output_schema);
    tn->base.next_batch = topn_next_batch;
    tn->base.free_node = topn_free;
    tn->base.kind = "TopNNode";

    return tn;
}
