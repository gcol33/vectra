#include "sort.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "vtr1.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Default memory budget: 1 GB */
#define DEFAULT_MEM_BUDGET (1024LL * 1024 * 1024)

/* Row group size for spill files */
#define SPILL_RG_SIZE 65536

/* Output batch size during merge */
#define MERGE_BATCH_SIZE 65536

/* Sort phases */
#define SORT_INIT     0
#define SORT_MEMORY   1
#define SORT_MERGING  2
#define SORT_DONE     3

/* ------------------------------------------------------------------ */
/*  Value comparison (works across different VecArray pointers)        */
/* ------------------------------------------------------------------ */

static int compare_value(const VecArray *a, int64_t ra,
                         const VecArray *b, int64_t rb, int desc) {
    int a_valid = vec_array_is_valid(a, ra);
    int b_valid = vec_array_is_valid(b, rb);

    if (!a_valid && !b_valid) return 0;
    if (!a_valid) return desc ? -1 : 1;   /* NA sorts last in ASC */
    if (!b_valid) return desc ? 1 : -1;

    int cmp = 0;
    switch (a->type) {
    case VEC_DOUBLE: {
        double va = a->buf.dbl[ra], vb = b->buf.dbl[rb];
        cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        break;
    }
    case VEC_INT64: {
        int64_t va = a->buf.i64[ra], vb = b->buf.i64[rb];
        cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        break;
    }
    case VEC_BOOL: {
        uint8_t va = a->buf.bln[ra], vb = b->buf.bln[rb];
        cmp = (int)va - (int)vb;
        break;
    }
    case VEC_STRING: {
        int64_t sa = a->buf.str.offsets[ra], ea = a->buf.str.offsets[ra + 1];
        int64_t sb = b->buf.str.offsets[rb], eb = b->buf.str.offsets[rb + 1];
        int64_t la = ea - sa, lb = eb - sb;
        int64_t minlen = la < lb ? la : lb;
        cmp = memcmp(a->buf.str.data + sa, b->buf.str.data + sb,
                     (size_t)minlen);
        if (cmp == 0) cmp = (la < lb) ? -1 : (la > lb) ? 1 : 0;
        break;
    }
    }

    return desc ? -cmp : cmp;
}

/* Compare two rows by sort keys.  Column arrays may come from
   different batches (used for both in-memory sort and k-way merge). */
static int compare_rows_cross(const VecArray *cols_a, int64_t ra,
                               const VecArray *cols_b, int64_t rb,
                               const SortKey *keys, int n_keys) {
    for (int k = 0; k < n_keys; k++) {
        int ci = keys[k].col_index;
        int cmp = compare_value(&cols_a[ci], ra, &cols_b[ci], rb,
                                keys[k].descending);
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Merge sort (in-memory, for sorting a single run before spill)     */
/* ------------------------------------------------------------------ */

typedef struct {
    VecArray  *columns;
    int        n_keys;
    SortKey   *keys;
} InMemCtx;

static void merge_sort_impl(int64_t *indices, int64_t *tmp, int64_t n,
                             const InMemCtx *ctx) {
    if (n <= 1) return;
    int64_t mid = n / 2;
    merge_sort_impl(indices, tmp, mid, ctx);
    merge_sort_impl(indices + mid, tmp, n - mid, ctx);

    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (compare_rows_cross(ctx->columns, indices[i],
                               ctx->columns, indices[j],
                               ctx->keys, ctx->n_keys) <= 0)
            tmp[k++] = indices[i++];
        else
            tmp[k++] = indices[j++];
    }
    while (i < mid) tmp[k++] = indices[i++];
    while (j < n)   tmp[k++] = indices[j++];
    memcpy(indices, tmp, (size_t)n * sizeof(int64_t));
}

/* ------------------------------------------------------------------ */
/*  Gather: reorder an array by sorted indices                        */
/* ------------------------------------------------------------------ */

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
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) {
            int64_t si = indices[i];
            if (vec_array_is_valid(src, si))
                total += src->buf.str.offsets[si + 1] - src->buf.str.offsets[si];
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
                memcpy(dst.buf.str.data + off, src->buf.str.data + s,
                       (size_t)slen);
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

/* ------------------------------------------------------------------ */
/*  Memory estimation for builders                                    */
/* ------------------------------------------------------------------ */

static int64_t estimate_builder_memory(const VecArrayBuilder *builders,
                                        int n_cols) {
    int64_t total = 0;
    for (int c = 0; c < n_cols; c++) {
        const VecArrayBuilder *b = &builders[c];
        total += vec_validity_bytes(b->capacity);
        switch (b->type) {
        case VEC_INT64:  total += b->capacity * (int64_t)sizeof(int64_t); break;
        case VEC_DOUBLE: total += b->capacity * (int64_t)sizeof(double);  break;
        case VEC_BOOL:   total += b->capacity; break;
        case VEC_STRING:
            total += (b->capacity + 1) * (int64_t)sizeof(int64_t);
            total += b->str_data_cap;
            break;
        }
    }
    return total;
}

/* ------------------------------------------------------------------ */
/*  Spill: sort in-memory data and write to a temp .vtr file          */
/* ------------------------------------------------------------------ */

static char *make_run_path(const char *temp_dir, int run_id) {
    static int sort_counter = 0;
    int id = sort_counter++;
    int len = snprintf(NULL, 0, "%s/vectra_sort_%d_%d.vtr",
                       temp_dir, id, run_id);
    char *path = (char *)malloc((size_t)(len + 1));
    snprintf(path, (size_t)(len + 1), "%s/vectra_sort_%d_%d.vtr",
             temp_dir, id, run_id);
    return path;
}

/* Finish builders, sort, write to a spill .vtr, free arrays.
   Returns file path (caller frees) or NULL if builders were empty.
   Builders are consumed (zeroed) regardless. */
static char *spill_sorted_run(VecArrayBuilder *builders, int n_cols,
                               const VecSchema *schema,
                               const SortKey *keys, int n_keys,
                               const char *temp_dir, int run_id) {
    /* Finish builders into arrays */
    int64_t n_rows = builders[0].length;
    VecArray *columns = (VecArray *)malloc((size_t)n_cols * sizeof(VecArray));
    for (int c = 0; c < n_cols; c++)
        columns[c] = vec_builder_finish(&builders[c]);

    if (n_rows == 0) {
        for (int c = 0; c < n_cols; c++)
            vec_array_free(&columns[c]);
        free(columns);
        return NULL;
    }

    /* Sort via indices */
    int64_t *indices = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    int64_t *tmp     = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    for (int64_t i = 0; i < n_rows; i++) indices[i] = i;

    InMemCtx ctx = { columns, n_keys, (SortKey *)keys };
    merge_sort_impl(indices, tmp, n_rows, &ctx);
    free(tmp);

    /* Write multi-rowgroup spill file */
    char *path = make_run_path(temp_dir, run_id);
    uint32_t n_rgs = (uint32_t)((n_rows + SPILL_RG_SIZE - 1) / SPILL_RG_SIZE);

    FILE *fp = fopen(path, "wb");
    if (!fp) vectra_error("cannot create spill file: %s", path);

    vtr1_write_header(fp, schema, n_rgs);

    for (uint32_t rg = 0; rg < n_rgs; rg++) {
        int64_t start = (int64_t)rg * SPILL_RG_SIZE;
        int64_t end   = start + SPILL_RG_SIZE;
        if (end > n_rows) end = n_rows;
        int64_t rg_rows = end - start;

        VecBatch *batch = vec_batch_alloc(n_cols, rg_rows);
        for (int c = 0; c < n_cols; c++) {
            batch->columns[c] = gather_array(&columns[c],
                                              indices + start, rg_rows);
            batch->col_names[c] = (char *)malloc(
                strlen(schema->col_names[c]) + 1);
            strcpy(batch->col_names[c], schema->col_names[c]);
        }
        vtr1_write_rowgroup(fp, batch);
        vec_batch_free(batch);
    }

    fclose(fp);
    free(indices);
    for (int c = 0; c < n_cols; c++)
        vec_array_free(&columns[c]);
    free(columns);

    return path;
}

/* ------------------------------------------------------------------ */
/*  K-way merge state and helpers                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    Vtr1File *file;
    int      *col_mask;
    uint32_t  n_rgs;
    uint32_t  next_rg;
    VecBatch *batch;      /* currently loaded rowgroup */
    int64_t   cursor;     /* current row within batch */
    int       exhausted;
} MergeRun;

typedef struct {
    MergeRun *runs;
    int       n_runs;
    int      *heap;       /* min-heap of run indices */
    int       heap_size;
    SortKey  *keys;
    int       n_keys;
    int       n_cols;
    VecSchema schema;     /* for output batch column names */
} MergeState;

/* Load next rowgroup for a run, or mark exhausted */
static void merge_run_load_next(MergeRun *run) {
    if (run->batch) {
        vec_batch_free(run->batch);
        run->batch = NULL;
    }
    if (run->next_rg < run->n_rgs) {
        run->batch = vtr1_read_rowgroup(run->file, run->next_rg,
                                         run->col_mask);
        run->next_rg++;
        run->cursor = 0;
    } else {
        run->exhausted = 1;
    }
}

/* Advance cursor by one row.  Returns 1 if run is exhausted. */
static int merge_run_advance(MergeRun *run) {
    run->cursor++;
    if (run->batch && run->cursor < run->batch->n_rows)
        return 0;
    merge_run_load_next(run);
    return run->exhausted;
}

/* Compare current rows of two runs */
static int merge_compare(const MergeState *ms, int a, int b) {
    MergeRun *ra = &ms->runs[a];
    MergeRun *rb = &ms->runs[b];
    return compare_rows_cross(ra->batch->columns, ra->cursor,
                              rb->batch->columns, rb->cursor,
                              ms->keys, ms->n_keys);
}

/* ---- Min-heap operations ---- */

static void heap_sift_up(MergeState *ms, int pos) {
    while (pos > 0) {
        int parent = (pos - 1) / 2;
        if (merge_compare(ms, ms->heap[pos], ms->heap[parent]) < 0) {
            int t = ms->heap[pos];
            ms->heap[pos] = ms->heap[parent];
            ms->heap[parent] = t;
            pos = parent;
        } else {
            break;
        }
    }
}

static void heap_sift_down(MergeState *ms, int pos) {
    for (;;) {
        int smallest = pos;
        int left  = 2 * pos + 1;
        int right = 2 * pos + 2;
        if (left  < ms->heap_size &&
            merge_compare(ms, ms->heap[left],  ms->heap[smallest]) < 0)
            smallest = left;
        if (right < ms->heap_size &&
            merge_compare(ms, ms->heap[right], ms->heap[smallest]) < 0)
            smallest = right;
        if (smallest == pos) break;
        int t = ms->heap[pos];
        ms->heap[pos] = ms->heap[smallest];
        ms->heap[smallest] = t;
        pos = smallest;
    }
}

static void heap_insert(MergeState *ms, int run_idx) {
    ms->heap[ms->heap_size++] = run_idx;
    heap_sift_up(ms, ms->heap_size - 1);
}

static int heap_pop(MergeState *ms) {
    int top = ms->heap[0];
    ms->heap[0] = ms->heap[--ms->heap_size];
    if (ms->heap_size > 0)
        heap_sift_down(ms, 0);
    return top;
}

/* Free all merge resources */
static void merge_state_free(MergeState *ms) {
    if (!ms) return;
    for (int r = 0; r < ms->n_runs; r++) {
        MergeRun *run = &ms->runs[r];
        if (run->batch)   vec_batch_free(run->batch);
        if (run->file)    vtr1_close(run->file);
        free(run->col_mask);
    }
    free(ms->runs);
    free(ms->heap);
    vec_schema_free(&ms->schema);
    free(ms);
}

/* Produce the next batch from the k-way merge */
static VecBatch *merge_next_batch(SortNode *sn) {
    MergeState *ms = (MergeState *)sn->merge;

    if (ms->heap_size == 0) {
        sn->phase = SORT_DONE;
        return NULL;
    }

    /* Builders with pre-reserved capacity */
    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)ms->n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < ms->n_cols; c++) {
        builders[c] = vec_builder_init(ms->schema.col_types[c]);
        vec_builder_reserve(&builders[c], MERGE_BATCH_SIZE);
    }

    int64_t count = 0;
    while (count < MERGE_BATCH_SIZE && ms->heap_size > 0) {
        int win = heap_pop(ms);
        MergeRun *run = &ms->runs[win];

        /* Append current row from winning run */
        for (int c = 0; c < ms->n_cols; c++)
            vec_builder_append_one(&builders[c],
                                   &run->batch->columns[c], run->cursor);
        count++;

        /* Advance and re-insert if not exhausted */
        if (!merge_run_advance(run))
            heap_insert(ms, win);
    }

    if (count == 0) {
        for (int c = 0; c < ms->n_cols; c++)
            vec_builder_free(&builders[c]);
        free(builders);
        sn->phase = SORT_DONE;
        return NULL;
    }

    VecBatch *result = vec_batch_alloc(ms->n_cols, count);
    for (int c = 0; c < ms->n_cols; c++) {
        result->columns[c] = vec_builder_finish(&builders[c]);
        const char *nm = ms->schema.col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }
    result->n_rows = count;
    free(builders);

    return result;
}

/* ------------------------------------------------------------------ */
/*  Input consumption and run generation                              */
/* ------------------------------------------------------------------ */

static void add_run_path(SortNode *sn, char *path) {
    if (sn->n_runs >= sn->runs_cap) {
        sn->runs_cap = sn->runs_cap == 0 ? 8 : sn->runs_cap * 2;
        sn->run_paths = (char **)realloc(sn->run_paths,
            (size_t)sn->runs_cap * sizeof(char *));
    }
    sn->run_paths[sn->n_runs++] = path;
}

/* Build a single-run in-memory result (identical to original sort) */
static void build_memory_result(SortNode *sn, VecArray *columns,
                                 int n_cols, int64_t n_rows) {
    const VecSchema *schema = &sn->base.output_schema;

    if (n_rows == 0) {
        VecBatch *result = vec_batch_alloc(n_cols, 0);
        for (int c = 0; c < n_cols; c++) {
            result->columns[c] = columns[c];
            const char *nm = schema->col_names[c];
            result->col_names[c] = (char *)malloc(strlen(nm) + 1);
            strcpy(result->col_names[c], nm);
        }
        free(columns);
        sn->mem_result = result;
        sn->phase = SORT_MEMORY;
        return;
    }

    int64_t *indices = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    int64_t *tmp     = (int64_t *)malloc((size_t)n_rows * sizeof(int64_t));
    for (int64_t i = 0; i < n_rows; i++) indices[i] = i;

    InMemCtx ctx = { columns, sn->n_keys, sn->keys };
    merge_sort_impl(indices, tmp, n_rows, &ctx);
    free(tmp);

    VecBatch *result = vec_batch_alloc(n_cols, n_rows);
    for (int c = 0; c < n_cols; c++) {
        result->columns[c] = gather_array(&columns[c], indices, n_rows);
        const char *nm = schema->col_names[c];
        result->col_names[c] = (char *)malloc(strlen(nm) + 1);
        strcpy(result->col_names[c], nm);
    }

    free(indices);
    for (int c = 0; c < n_cols; c++)
        vec_array_free(&columns[c]);
    free(columns);

    sn->mem_result = result;
    sn->phase = SORT_MEMORY;
}

/* Initialize the k-way merge over spilled runs */
static void init_merge(SortNode *sn) {
    int n_cols = sn->base.output_schema.n_cols;

    MergeState *ms = (MergeState *)calloc(1, sizeof(MergeState));
    ms->n_runs  = sn->n_runs;
    ms->n_keys  = sn->n_keys;
    ms->keys    = sn->keys;
    ms->n_cols  = n_cols;
    ms->schema  = vec_schema_copy(&sn->base.output_schema);

    ms->runs = (MergeRun *)calloc((size_t)sn->n_runs, sizeof(MergeRun));
    ms->heap = (int *)malloc((size_t)sn->n_runs * sizeof(int));
    ms->heap_size = 0;

    for (int r = 0; r < sn->n_runs; r++) {
        MergeRun *run = &ms->runs[r];
        run->file    = vtr1_open(sn->run_paths[r]);
        run->n_rgs   = run->file->header.n_rowgroups;
        run->next_rg = 0;
        run->col_mask = (int *)malloc((size_t)n_cols * sizeof(int));
        for (int c = 0; c < n_cols; c++)
            run->col_mask[c] = 1;

        /* Load first rowgroup */
        merge_run_load_next(run);
        if (!run->exhausted)
            heap_insert(ms, r);
    }

    sn->merge = ms;
    sn->phase = SORT_MERGING;
}

/* Consume all child batches, generate sorted runs */
static void consume_input(SortNode *sn) {
    int n_cols = sn->base.output_schema.n_cols;
    const VecSchema *schema = &sn->base.output_schema;
    int can_spill = (sn->temp_dir != NULL && sn->mem_budget > 0);

    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(schema->col_types[c]);

    /* Pull all child batches */
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

        /* Spill if memory budget exceeded */
        if (can_spill && builders[0].length > 0 &&
            estimate_builder_memory(builders, n_cols) > sn->mem_budget) {
            char *path = spill_sorted_run(builders, n_cols, schema,
                                           sn->keys, sn->n_keys,
                                           sn->temp_dir, sn->n_runs);
            if (path) add_run_path(sn, path);
            /* Reinitialize builders (consumed by spill) */
            for (int c = 0; c < n_cols; c++)
                builders[c] = vec_builder_init(schema->col_types[c]);
        }
    }

    int64_t remaining = builders[0].length;

    if (sn->n_runs == 0) {
        /* Everything fit in memory — fast path */
        VecArray *columns = (VecArray *)malloc(
            (size_t)n_cols * sizeof(VecArray));
        for (int c = 0; c < n_cols; c++)
            columns[c] = vec_builder_finish(&builders[c]);
        free(builders);
        build_memory_result(sn, columns, n_cols, remaining);
        return;
    }

    /* Multiple runs: spill the final chunk too */
    if (remaining > 0) {
        char *path = spill_sorted_run(builders, n_cols, schema,
                                       sn->keys, sn->n_keys,
                                       sn->temp_dir, sn->n_runs);
        if (path) add_run_path(sn, path);
    }

    /* Free builders (already consumed by spill or empty) */
    for (int c = 0; c < n_cols; c++)
        vec_builder_free(&builders[c]);
    free(builders);

    /* Set up the k-way merge */
    init_merge(sn);
}

/* ------------------------------------------------------------------ */
/*  VecNode interface                                                 */
/* ------------------------------------------------------------------ */

static VecBatch *sort_next_batch(VecNode *self) {
    SortNode *sn = (SortNode *)self;

    if (sn->phase == SORT_DONE)
        return NULL;

    if (sn->phase == SORT_INIT)
        consume_input(sn);  /* sets phase to MEMORY or MERGING */

    if (sn->phase == SORT_MEMORY) {
        VecBatch *result = sn->mem_result;
        sn->mem_result = NULL;
        sn->phase = SORT_DONE;
        return result;
    }

    if (sn->phase == SORT_MERGING)
        return merge_next_batch(sn);

    return NULL;
}

static void sort_free(VecNode *self) {
    SortNode *sn = (SortNode *)self;
    sn->child->free_node(sn->child);
    free(sn->keys);

    if (sn->mem_result)
        vec_batch_free(sn->mem_result);

    if (sn->merge)
        merge_state_free((MergeState *)sn->merge);

    /* Delete spill files */
    for (int r = 0; r < sn->n_runs; r++) {
        if (sn->run_paths[r]) {
            remove(sn->run_paths[r]);
            free(sn->run_paths[r]);
        }
    }
    free(sn->run_paths);
    free(sn->temp_dir);

    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

SortNode *sort_node_create(VecNode *child, int n_keys, SortKey *keys,
                           const char *temp_dir) {
    SortNode *sn = (SortNode *)calloc(1, sizeof(SortNode));
    if (!sn) vectra_error("alloc failed for SortNode");

    sn->child      = child;
    sn->n_keys     = n_keys;
    sn->keys       = keys;
    sn->phase      = SORT_INIT;
    sn->mem_budget = DEFAULT_MEM_BUDGET;

    if (temp_dir) {
        sn->temp_dir = (char *)malloc(strlen(temp_dir) + 1);
        strcpy(sn->temp_dir, temp_dir);
    }

    sn->base.output_schema = vec_schema_copy(&child->output_schema);
    sn->base.next_batch    = sort_next_batch;
    sn->base.free_node     = sort_free;
    sn->base.kind          = "SortNode";

    return sn;
}
