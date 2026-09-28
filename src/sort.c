#include "sort.h"
#include "plan_budget.h"
#include "vec_omp.h"
#include "array.h"
#include "batch.h"
#include "schema.h"
#include "builder.h"
#include "vtr1_tdc.h"
#include "vtr_codec.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Row group size for spill files */
#define SPILL_RG_SIZE 65536

/* Output batch size during merge */
#define MERGE_BATCH_SIZE 65536

/* Rows per batch the in-memory path gathers and emits: the default write_vtr
   row-group size. */
#define SORT_EMIT_ROWS 131072

/* Sort working space per buffered row, counted against the budget: the
   permutation plus the radix sort's encoded keys and its two ping-pong
   buffers (the merge-sort fallback needs half of that). */
#define SORT_WORK_BYTES_PER_ROW 32

/* Upper bound on the number of runs merged at once. The actual fan-in is
   chosen per-sort from the measured row width (compute_merge_fanin); this
   caps open files / heap size for narrow data. A run count above the
   fan-in is reduced by multi-pass merging, which is what keeps merge
   memory O(1) in the input size rather than growing with the run count. */
#define SORT_MAX_FANIN 64

/* Sort phases */
#define SORT_INIT     0
#define SORT_MEMORY   1
#define SORT_MERGING  2
#define SORT_DONE     3

/* ------------------------------------------------------------------ */
/*  Value comparison (works across different VecArray pointers)        */
/* ------------------------------------------------------------------ */

int sort_compare_value(const VecArray *a, int64_t ra,
                       const VecArray *b, int64_t rb, int desc, int na_last) {
    int a_valid = vec_array_is_valid(a, ra);
    int b_valid = vec_array_is_valid(b, rb);

    /* NA handling is keyed by na_last so the merge comparator stays consistent
       with the radix run encoder in either mode (a mismatch mis-orders valid
       rows across spilled runs). na_last = 1: NA sorts positionally last
       regardless of desc (dplyr arrange). na_last = 0: NA behaves as the
       maximum value and flips with desc (window value sorts -- cume_dist
       treats NA as the largest). */
    if (!a_valid && !b_valid) return 0;
    if (!a_valid || !b_valid) {
        int cmp = !a_valid ? 1 : -1;   /* NA compares as the greater value */
        return na_last ? cmp : (desc ? -cmp : cmp);
    }

    int cmp = 0;
    switch (a->type) {
    case VEC_DOUBLE: {
        double va = a->buf.dbl[ra], vb = b->buf.dbl[rb];
        /* A computed NaN is a valid value; sort all NaN together, after finite
           values, so grouping sees them as one key (matches R). */
        int na = va != va, nb = vb != vb;
        if (na || nb)
            cmp = (na && nb) ? 0 : (na ? 1 : -1);
        else
            cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        break;
    }
    case VEC_INT64: {
        int64_t va = a->buf.i64[ra], vb = b->buf.i64[rb];
        cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        break;
    }
    case VEC_INT32: {
        int32_t va = a->buf.i32[ra], vb = b->buf.i32[rb];
        cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        break;
    }
    case VEC_INT16: {
        int16_t va = a->buf.i16[ra], vb = b->buf.i16[rb];
        cmp = (va < vb) ? -1 : (va > vb) ? 1 : 0;
        break;
    }
    case VEC_INT8: {
        int8_t va = a->buf.i8[ra], vb = b->buf.i8[rb];
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
        cmp = (minlen > 0) ? memcmp(a->buf.str.data + sa, b->buf.str.data + sb,
                                     (size_t)minlen) : 0;
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
        int cmp = sort_compare_value(&cols_a[ci], ra, &cols_b[ci], rb,
                                keys[k].descending, keys[k].na_last);
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Radix sort for single-key numeric columns (O(n) vs O(n log n))   */
/* ------------------------------------------------------------------ */

/* Encode int64 as uint64 for radix sort: flip sign bit so that
   negative values sort before positive.  NAs get max value (sort last). */
static inline uint64_t radix_encode_i64(int64_t v, int valid, int desc,
                                        int na_last) {
    uint64_t u;
    if (!valid) {
        /* na_last: NA is positionally last in both directions (final key is the
           max after the desc flip). Otherwise NA is the maximum value and flips
           with desc. Must match compare_value's NA handling. */
        if (na_last) u = desc ? 0ULL : 0xFFFFFFFFFFFFFFFFULL;
        else         u = 0xFFFFFFFFFFFFFFFFULL;
    } else {
        u = (uint64_t)v ^ 0x8000000000000000ULL;   /* flip sign bit */
    }
    return desc ? ~u : u;
}

/* Encode double as uint64 for radix sort: IEEE 754 bit trick.
   Positive doubles already sort correctly as uint64 after sign flip.
   Negative doubles need all bits flipped. */
static inline uint64_t radix_encode_dbl(double v, int valid, int desc,
                                        int na_last) {
    uint64_t u;
    if (!valid) {
        if (na_last) u = desc ? 0ULL : 0xFFFFFFFFFFFFFFFFULL;
        else         u = 0xFFFFFFFFFFFFFFFFULL;
    } else if (v != v) {
        /* All NaN payloads map to one key, just past +Inf and before NA, so
           NaN rows cluster into a single group (matches compare_value). */
        u = 0xFFFFFFFFFFFFFFFEULL;
    } else {
        uint64_t bits;
        memcpy(&bits, &v, sizeof(bits));
        /* If sign bit set (negative), flip all bits; else flip just sign bit */
        if (bits & 0x8000000000000000ULL)
            u = ~bits;
        else
            u = bits ^ 0x8000000000000000ULL;
    }
    return desc ? ~u : u;
}

/* LSD radix sort on uint64 keys.  Sorts indices[0..n-1] by keys[0..n-1].
   Uses 8-bit radix (256 buckets, 8 passes).  tmp is scratch of size n. */
static void radix_sort_u64(uint64_t *keys, int64_t *indices, int64_t *tmp_idx,
                           uint64_t *tmp_keys, int64_t n) {
    uint64_t *src_k = keys, *dst_k = tmp_keys;
    int64_t *src_i = indices, *dst_i = tmp_idx;

    for (int pass = 0; pass < 8; pass++) {
        int shift = pass * 8;
        int64_t count[256] = {0};

        /* Histogram */
        for (int64_t i = 0; i < n; i++)
            count[(src_k[i] >> shift) & 0xFF]++;

        /* Prefix sum */
        int64_t offset[256];
        offset[0] = 0;
        for (int b = 1; b < 256; b++)
            offset[b] = offset[b - 1] + count[b - 1];

        /* Scatter */
        for (int64_t i = 0; i < n; i++) {
            int bucket = (int)((src_k[i] >> shift) & 0xFF);
            int64_t pos = offset[bucket]++;
            dst_k[pos] = src_k[i];
            dst_i[pos] = src_i[i];
        }

        /* Swap src/dst for next pass */
        uint64_t *tk = src_k; src_k = dst_k; dst_k = tk;
        int64_t  *ti = src_i; src_i = dst_i; dst_i = ti;
    }

    /* After 8 passes (even number), result is back in the original arrays
       (keys, indices).  If it ended up in tmp, copy back. */
    if (src_k != keys) {
        memcpy(keys, src_k, (size_t)n * sizeof(uint64_t));
        memcpy(indices, src_i, (size_t)n * sizeof(int64_t));
    }
}

/* Try radix sort for single-key numeric columns.
   Returns 1 if radix sort was used, 0 if caller should fall back to merge sort. */
static int try_radix_sort(int64_t *indices, int64_t n,
                          const VecArray *columns, int n_keys,
                          const SortKey *keys) {
    /* Only for single numeric key */
    if (n_keys != 1) return 0;
    if (n < 256) return 0;  /* merge sort is fine for small arrays */

    int ci = keys[0].col_index;
    const VecArray *col = &columns[ci];
    int desc = keys[0].descending;
    int na_last = keys[0].na_last;

    if (col->type != VEC_INT64 && col->type != VEC_DOUBLE) return 0;

    /* Encode keys */
    uint64_t *enc = (uint64_t *)malloc((size_t)n * sizeof(uint64_t));
    uint64_t *tmp_k = (uint64_t *)malloc((size_t)n * sizeof(uint64_t));
    int64_t *tmp_i = (int64_t *)malloc((size_t)n * sizeof(int64_t));
    if (!enc || !tmp_k || !tmp_i) {
        free(enc); free(tmp_k); free(tmp_i);
        return 0;
    }

    if (col->type == VEC_INT64) {
        for (int64_t i = 0; i < n; i++)
            enc[i] = radix_encode_i64(col->buf.i64[indices[i]],
                                       vec_array_is_valid(col, indices[i]),
                                       desc, na_last);
    } else {
        for (int64_t i = 0; i < n; i++)
            enc[i] = radix_encode_dbl(col->buf.dbl[indices[i]],
                                       vec_array_is_valid(col, indices[i]),
                                       desc, na_last);
    }

    radix_sort_u64(enc, indices, tmp_i, tmp_k, n);

    free(enc);
    free(tmp_k);
    free(tmp_i);
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Merge sort (in-memory, for sorting a single run before spill)     */
/* ------------------------------------------------------------------ */

typedef struct {
    VecArray  *columns;
    int        n_keys;
    SortKey   *keys;
} InMemCtx;

/* Buffer-toggling merge sort.
   If to_aux=0: sort a[0..n-1] in place, using b as scratch. Result in a.
   If to_aux=1: sort a[0..n-1] into b[0..n-1], using a as scratch. Result in b.
   This eliminates the O(n) memcpy per merge level — total copies drop from
   O(n log n) to O(n) (leaf-level copies only). */
static void merge_sort_impl(int64_t *a, int64_t *b, int64_t n,
                             const InMemCtx *ctx, int to_aux) {
    if (n <= 1) {
        if (to_aux && n == 1) b[0] = a[0];
        return;
    }
    int64_t mid = n / 2;

    /* Sort both halves into the opposite buffer so we can merge from there */
#ifdef _OPENMP
    if (n > VEC_OMP_THRESHOLD) {
        #pragma omp task shared(ctx) if(n > VEC_OMP_THRESHOLD)
        merge_sort_impl(a, b, mid, ctx, !to_aux);
        #pragma omp task shared(ctx) if(n > VEC_OMP_THRESHOLD)
        merge_sort_impl(a + mid, b + mid, n - mid, ctx, !to_aux);
        #pragma omp taskwait
    } else {
#endif
        merge_sort_impl(a, b, mid, ctx, !to_aux);
        merge_sort_impl(a + mid, b + mid, n - mid, ctx, !to_aux);
#ifdef _OPENMP
    }
#endif

    /* After recursion, sorted halves are in the opposite buffer.
       Merge from src into dst (the target buffer for this level). */
    int64_t *src = to_aux ? a : b;
    int64_t *dst = to_aux ? b : a;

    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (compare_rows_cross(ctx->columns, src[i],
                               ctx->columns, src[j],
                               ctx->keys, ctx->n_keys) <= 0)
            dst[k++] = src[i++];
        else
            dst[k++] = src[j++];
    }
    while (i < mid) dst[k++] = src[i++];
    while (j < n)   dst[k++] = src[j++];
}

/* ------------------------------------------------------------------ */
/*  Gather: reorder arrays by sorted indices                          */
/* ------------------------------------------------------------------ */

/* Rows per parallel gather task. A multiple of 8, so tasks write disjoint
   validity bytes. */
#define GATHER_BLOCK 8192

/* dst[i] = src[perm[i]] for i in [lo, hi), values and validity, for a
   fixed-width column. lo must be a multiple of 8. */
static void gather_fixed_range(VecArray *dst, const VecArray *src,
                               const int64_t *perm, int64_t lo, int64_t hi) {
    for (int64_t i = lo; i < hi; i++) {
        if (vec_array_is_valid(src, perm[i])) vec_array_set_valid(dst, i);
        else                                  vec_array_set_null(dst, i);
    }
#define GATHER_VALUES(field)                                                \
    for (int64_t i = lo; i < hi; i++) {                                     \
        if (i + VEC_PREFETCH_AHEAD < hi)                                    \
            VEC_PREFETCH_READ(&src->buf.field[perm[i + VEC_PREFETCH_AHEAD]]); \
        dst->buf.field[i] = src->buf.field[perm[i]];                        \
    }
    switch (src->type) {
    case VEC_INT64:  GATHER_VALUES(i64); break;
    case VEC_DOUBLE: GATHER_VALUES(dbl); break;
    case VEC_INT32:  GATHER_VALUES(i32); break;
    case VEC_INT16:  GATHER_VALUES(i16); break;
    case VEC_INT8:   GATHER_VALUES(i8);  break;
    case VEC_BOOL:   GATHER_VALUES(bln); break;
    case VEC_STRING: break;
    }
#undef GATHER_VALUES
}

/* String column gathered in permutation order: sized in one pass, filled in
   a second. */
static VecArray gather_string(const VecArray *src, const int64_t *indices,
                              int64_t n) {
    VecArray dst = vec_array_alloc(VEC_STRING, n);
    int64_t total = 0;
    for (int64_t i = 0; i < n; i++) {
        int64_t si = indices[i];
        if (vec_array_is_valid(src, si))
            total += src->buf.str.offsets[si + 1] - src->buf.str.offsets[si];
    }
    free(dst.buf.str.data);
    dst.buf.str.data = (char *)malloc((size_t)(total > 0 ? total : 1));
    if (!dst.buf.str.data) vectra_error("alloc failed for sorted strings");
    dst.buf.str.data_len = total;

    int64_t off = 0;
    for (int64_t i = 0; i < n; i++) {
        dst.buf.str.offsets[i] = off;
        int64_t si = indices[i];
        if (vec_array_is_valid(src, si)) {
            vec_array_set_valid(&dst, i);
            int64_t s = src->buf.str.offsets[si];
            int64_t slen = src->buf.str.offsets[si + 1] - s;
            memcpy(dst.buf.str.data + off, src->buf.str.data + s, (size_t)slen);
            off += slen;
        } else {
            vec_array_set_null(&dst, i);
        }
    }
    dst.buf.str.offsets[n] = off;
    return dst;
}

/* ------------------------------------------------------------------ */
/*  Shared sort + gather for the in-memory and spill paths            */
/* ------------------------------------------------------------------ */

/* Sorted row permutation of columns[0..n). Radix for a single numeric key,
   stable merge sort otherwise. Transient working space is at most
   SORT_WORK_BYTES_PER_ROW * n including the returned permutation. */
static int64_t *sort_permutation(VecArray *columns, int64_t n,
                                 const SortKey *keys, int n_keys) {
    int64_t *indices = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
    if (!indices) vectra_error("alloc failed for sort permutation");
    for (int64_t i = 0; i < n; i++) indices[i] = i;

    if (!try_radix_sort(indices, n, columns, n_keys, keys)) {
        int64_t *tmp = (int64_t *)malloc((size_t)(n > 0 ? n : 1) * sizeof(int64_t));
        if (!tmp) vectra_error("alloc failed for sort scratch");
        InMemCtx ctx = { columns, n_keys, (SortKey *)keys };
#ifdef _OPENMP
        if (n > VEC_OMP_THRESHOLD) {
            #pragma omp parallel
            {
                #pragma omp single
                merge_sort_impl(indices, tmp, n, &ctx, 0);
            }
        } else {
#endif
            merge_sort_impl(indices, tmp, n, &ctx, 0);
#ifdef _OPENMP
        }
#endif
        free(tmp);
    }
    return indices;
}

/* Batch of `count` rows gathered from columns in permutation order. The
   fixed-width columns are gathered in one parallel region over
   (column, GATHER_BLOCK rows) tasks, so a batch costs one fork/join rather
   than one per column; string columns are gathered serially. */
static VecBatch *gather_batch(const VecArray *columns, const VecSchema *schema,
                              const int64_t *perm, int64_t count) {
    int n_cols = schema->n_cols;
    VecBatch *batch = vec_batch_alloc(n_cols, count);
    for (int c = 0; c < n_cols; c++) {
        batch->columns[c] = columns[c].type == VEC_STRING
            ? gather_string(&columns[c], perm, count)
            : vec_array_alloc(columns[c].type, count);
        batch->col_names[c] = (char *)malloc(strlen(schema->col_names[c]) + 1);
        strcpy(batch->col_names[c], schema->col_names[c]);
    }

    int64_t n_blocks = (count + GATHER_BLOCK - 1) / GATHER_BLOCK;
    int64_t n_tasks = n_blocks * n_cols;
#ifdef _OPENMP
    #pragma omp parallel for schedule(static) if(count * n_cols > VEC_OMP_THRESHOLD)
#endif
    for (int64_t t = 0; t < n_tasks; t++) {
        int c = (int)(t / n_blocks);
        if (columns[c].type == VEC_STRING) continue;
        int64_t lo = (t % n_blocks) * GATHER_BLOCK;
        int64_t hi = lo + GATHER_BLOCK < count ? lo + GATHER_BLOCK : count;
        gather_fixed_range(&batch->columns[c], &columns[c], perm, lo, hi);
    }
    return batch;
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

    int64_t *indices = sort_permutation(columns, n_rows, keys, n_keys);

    /* Write multi-rowgroup spill file via the tdc writer; the writer
     * self-finalizes the trailing rowgroup index in close. */
    char *path = make_run_path(temp_dir, run_id);
    Vtr1TdcWriter *w = vtr1_open_tdc_writer(path, schema);
    for (int64_t start = 0; start < n_rows; start += SPILL_RG_SIZE) {
        int64_t rg_rows = n_rows - start;
        if (rg_rows > SPILL_RG_SIZE) rg_rows = SPILL_RG_SIZE;
        VecBatch *batch = gather_batch(columns, schema, indices + start, rg_rows);
        vtr1_write_rowgroup_tdc(w, batch, VTR_SPILL_COMPRESS, NULL, NULL);
        vec_batch_free(batch);
    }

    vtr1_close_tdc_writer(w);
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
    Vtr1TdcFile *file;
    int         *col_mask;
    uint32_t     n_rgs;
    uint32_t     next_rg;
    VecBatch    *batch;      /* currently loaded rowgroup */
    int64_t      cursor;     /* current row within batch */
    int          exhausted;
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
        run->batch = vtr1_read_rowgroup_tdc(run->file, run->next_rg,
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
    int cmp = compare_rows_cross(ra->batch->columns, ra->cursor,
                                 rb->batch->columns, rb->cursor,
                                 ms->keys, ms->n_keys);
    if (cmp != 0) return cmp;
    /* Stable tiebreak across runs: on equal keys prefer the row from the
       lower-indexed run. Runs are generated in input order (run 0 holds the
       earliest input rows) and each run is internally stable (radix LSD /
       merge_sort_impl), and reduce_runs merges consecutive groups so the
       ordering is preserved across reduction passes — so lower run index =
       earlier input position. Within one run only the cursor row competes,
       so intra-run order is already input order. This makes the spilled
       merge match the in-memory stable path. */
    return (a < b) ? -1 : (a > b) ? 1 : 0;
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
        if (run->file)    vtr1_close_tdc(run->file);
        free(run->col_mask);
    }
    free(ms->runs);
    free(ms->heap);
    vec_schema_free(&ms->schema);
    free(ms);
}

/* Build one merged output batch (up to MERGE_BATCH_SIZE rows) from the
   k-way heap, or NULL when the merge is drained. Shared by the streaming
   output path (merge_next_batch) and the intermediate reduction passes
   (merge_drain_to_writer). */
static VecBatch *merge_build_one_batch(MergeState *ms) {
    if (ms->heap_size == 0)
        return NULL;

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

/* Produce the next batch from the k-way merge (streaming output path) */
static VecBatch *merge_next_batch(SortNode *sn) {
    MergeState *ms = (MergeState *)sn->merge;
    VecBatch *result = merge_build_one_batch(ms);
    if (!result) {
        sn->phase = SORT_DONE;
        vec_mem_set(&sn->mem, 0);
    }
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

/* Arm the in-memory path: sort a permutation over the buffered columns and
   keep both; sort_next_batch gathers SORT_EMIT_ROWS rows per call. Emitting
   bounded batches rather than one gathered copy of the whole buffer keeps the
   peak at the buffer plus its permutation, and hands downstream consumers
   (a write_vtr row group, a grouped aggregate) cache-sized batches. */
static void build_memory_result(SortNode *sn, VecArray *columns,
                                 int64_t n_rows) {
    sn->mem_cols = columns;
    sn->mem_n    = n_rows;
    sn->mem_pos  = 0;
    sn->mem_perm = sort_permutation(columns, n_rows, sn->keys, sn->n_keys);
    sn->phase    = SORT_MEMORY;
}

static void free_memory_result(SortNode *sn) {
    if (sn->mem_cols) {
        for (int c = 0; c < sn->base.output_schema.n_cols; c++)
            vec_array_free(&sn->mem_cols[c]);
        free(sn->mem_cols);
        sn->mem_cols = NULL;
    }
    free(sn->mem_perm);
    sn->mem_perm = NULL;
}

static VecBatch *memory_next_batch(SortNode *sn) {
    int64_t left = sn->mem_n - sn->mem_pos;
    if (left <= 0) {
        free_memory_result(sn);
        vec_mem_set(&sn->mem, 0);
        sn->phase = SORT_DONE;
        return NULL;
    }
    int64_t count = left < SORT_EMIT_ROWS ? left : SORT_EMIT_ROWS;
    VecBatch *b = gather_batch(sn->mem_cols, &sn->base.output_schema,
                               sn->mem_perm + sn->mem_pos, count);
    sn->mem_pos += count;
    return b;
}

/* Open a k-way merge over the given run files. Loads the first rowgroup of
   each run into the heap; peak resident = k decoded rowgroups. */
static MergeState *merge_state_open(SortNode *sn, char **paths, int k) {
    int n_cols = sn->base.output_schema.n_cols;

    MergeState *ms = (MergeState *)calloc(1, sizeof(MergeState));
    ms->n_runs  = k;
    ms->n_keys  = sn->n_keys;
    ms->keys    = sn->keys;
    ms->n_cols  = n_cols;
    ms->schema  = vec_schema_copy(&sn->base.output_schema);

    ms->runs = (MergeRun *)calloc((size_t)k, sizeof(MergeRun));
    ms->heap = (int *)malloc((size_t)k * sizeof(int));
    ms->heap_size = 0;

    for (int r = 0; r < k; r++) {
        MergeRun *run = &ms->runs[r];
        run->file    = vtr1_open_tdc(paths[r]);
        if (!run->file)
            vectra_error("vtr1_open_tdc failed for spill run %s", paths[r]);
        run->n_rgs   = vtr1_tdc_n_rowgroups(run->file);
        run->next_rg = 0;
        run->col_mask = (int *)malloc((size_t)n_cols * sizeof(int));
        for (int c = 0; c < n_cols; c++)
            run->col_mask[c] = 1;

        /* Load first rowgroup */
        merge_run_load_next(run);
        if (!run->exhausted)
            heap_insert(ms, r);
    }
    return ms;
}

/* Drain a k-way merge fully into a new sorted run file. Used by the
   intermediate reduction passes; peak = k decoded rowgroups + one output
   batch, independent of total rows. */
static void merge_drain_to_writer(MergeState *ms, const char *out_path) {
    Vtr1TdcWriter *w = vtr1_open_tdc_writer(out_path, &ms->schema);
    VecBatch *b;
    while ((b = merge_build_one_batch(ms)) != NULL) {
        vtr1_write_rowgroup_tdc(w, b, VTR_SPILL_COMPRESS, NULL, NULL);
        vec_batch_free(b);
    }
    vtr1_close_tdc_writer(w);
}

/* Choose the merge fan-in so that k decoded rowgroups fit in ~half the
   memory available to the merge: k = (budget/2) / (SPILL_RG_SIZE *
   bytes_per_row), where bytes_per_row is the measured decoded width of the
   spilled data (est_bytes / est_rows accumulated at spill time). Clamped to
   [2, SORT_MAX_FANIN]. Wide rows (geometry / long strings) get a small
   fan-in, narrow numeric rows a large one, so merge-phase resident memory
   stays near the budget regardless of row width or total size. *reserve is
   set to the merge's resident bytes: k row groups plus one output batch. */
static int compute_merge_fanin(int64_t est_bytes, int64_t est_rows,
                               int64_t budget, int64_t *reserve) {
    if (budget <= 0 || budget == INT64_MAX) budget = VECTRA_SORT_MEM_DEFAULT;
    int64_t row_bytes = (est_rows > 0) ? est_bytes / est_rows : 1;
    if (row_bytes < 1) row_bytes = 1;
    int64_t rg_bytes = (int64_t)SPILL_RG_SIZE * row_bytes;
    int64_t fanin = (budget / 2) / (rg_bytes > 0 ? rg_bytes : 1);
    if (fanin < 2) fanin = 2;
    if (fanin > SORT_MAX_FANIN) fanin = SORT_MAX_FANIN;
    *reserve = fanin * rg_bytes + (int64_t)MERGE_BATCH_SIZE * row_bytes;
    return (int)fanin;
}

/* Reduce the spilled run count to <= fanin by repeated bounded-fan-in
   merge passes. Each pass merges disjoint groups of <= fanin runs into one
   new sorted run and deletes the consumed inputs; a lone tail run is
   carried through untouched. After this the final merge opens <= fanin
   runs at once, so peak resident is O(fanin) rather than O(n_runs). Cost is
   ceil(log_fanin(n_runs)) extra read+write passes over the spilled data. */
static void reduce_runs(SortNode *sn, int fanin) {
    while (sn->n_runs > fanin) {
        char **new_paths = NULL;
        int    new_n = 0, new_cap = 0;

        for (int start = 0; start < sn->n_runs; start += fanin) {
            int k = sn->n_runs - start;
            if (k > fanin) k = fanin;

            if (new_n >= new_cap) {
                new_cap = new_cap == 0 ? 8 : new_cap * 2;
                new_paths = (char **)realloc(new_paths,
                    (size_t)new_cap * sizeof(char *));
            }

            if (k == 1) {
                /* Lone tail run: carry through, moving ownership. */
                new_paths[new_n++] = sn->run_paths[start];
                sn->run_paths[start] = NULL;
                continue;
            }

            MergeState *ms = merge_state_open(sn, &sn->run_paths[start], k);
            char *out = make_run_path(sn->temp_dir, new_n);
            merge_drain_to_writer(ms, out);
            merge_state_free(ms);   /* closes the k input files */

            for (int r = start; r < start + k; r++) {
                if (sn->run_paths[r]) {
                    remove(sn->run_paths[r]);
                    free(sn->run_paths[r]);
                    sn->run_paths[r] = NULL;
                }
            }
            new_paths[new_n++] = out;
        }

        free(sn->run_paths);
        sn->run_paths = new_paths;
        sn->n_runs    = new_n;
        sn->runs_cap  = new_cap;
    }
}

/* Initialize the k-way merge over spilled runs, first reducing the run
   count to the bounded fan-in via multi-pass merging. */
static void init_merge(SortNode *sn, int fanin) {
    reduce_runs(sn, fanin);
    sn->merge = merge_state_open(sn, sn->run_paths, sn->n_runs);
    sn->phase = SORT_MERGING;
}

/* Consume all child batches, generate sorted runs */
static void consume_input(SortNode *sn) {
    int n_cols = sn->base.output_schema.n_cols;
    const VecSchema *schema = &sn->base.output_schema;
    int can_spill = sn->temp_dir != NULL &&
                    (sn->mem.grant != NULL || sn->mem.cap > 0);

    VecArrayBuilder *builders = (VecArrayBuilder *)calloc(
        (size_t)n_cols, sizeof(VecArrayBuilder));
    for (int c = 0; c < n_cols; c++)
        builders[c] = vec_builder_init(schema->col_types[c]);

    int64_t total_rows = 0;

    /* Measured decoded width of the spilled data, used to size the merge
       fan-in so the final merge stays within the memory budget. */
    int64_t spill_est_bytes = 0;
    int64_t spill_est_rows  = 0;

    /* Pull all child batches. Before a batch is appended, the sort predicts
       the buffer's allocated bytes after the append (builder growth included)
       plus the permutation and radix scratch sorting it will need, and
       reserves that on the plan's memory pool. If the pool refuses, the
       current buffer is spilled first and its reservation released. So the
       budget bounds what is actually allocated, not the rows held. A batch
       arriving at an empty buffer is always taken. */
    VecBatch *batch;
    while ((batch = sn->child->next_batch(sn->child)) != NULL) {
        int64_t n_logical = vec_batch_logical_rows(batch);
        int64_t held = builders[0].length;
        int64_t need = vec_builders_bytes_after_batch(builders, n_cols, batch)
                     + SORT_WORK_BYTES_PER_ROW * (held + n_logical);
        if (!vec_mem_try(&sn->mem, need)) {
            if (can_spill && held > 0) {
                spill_est_bytes += vec_builders_bytes(builders, n_cols);
                spill_est_rows  += held;
                char *path = spill_sorted_run(builders, n_cols, schema,
                                               sn->keys, sn->n_keys,
                                               sn->temp_dir, sn->n_runs);
                if (path) add_run_path(sn, path);
                /* Reinitialize builders (consumed by spill) */
                for (int c = 0; c < n_cols; c++)
                    builders[c] = vec_builder_init(schema->col_types[c]);
                vec_mem_set(&sn->mem, 0);
                need = vec_builders_bytes_after_batch(builders, n_cols, batch)
                     + SORT_WORK_BYTES_PER_ROW * n_logical;
            }
            vec_mem_set(&sn->mem, need);
        }

        total_rows += n_logical;
        if (!batch->sel) {
            for (int c = 0; c < n_cols; c++)
                vec_builder_append_array(&builders[c], &batch->columns[c]);
        } else {
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

    sn->total_rows = total_rows;

    int64_t remaining = builders[0].length;

    if (sn->n_runs == 0) {
        /* Everything fit in memory — fast path */
        VecArray *columns = (VecArray *)malloc(
            (size_t)n_cols * sizeof(VecArray));
        for (int c = 0; c < n_cols; c++)
            columns[c] = vec_builder_finish(&builders[c]);
        free(builders);
        build_memory_result(sn, columns, remaining);
        return;
    }

    /* Multiple runs: spill the final chunk too */
    if (remaining > 0) {
        spill_est_bytes += vec_builders_bytes(builders, n_cols);
        spill_est_rows  += remaining;
        char *path = spill_sorted_run(builders, n_cols, schema,
                                       sn->keys, sn->n_keys,
                                       sn->temp_dir, sn->n_runs);
        if (path) add_run_path(sn, path);
    }

    /* Free builders (already consumed by spill or empty) */
    for (int c = 0; c < n_cols; c++)
        vec_builder_free(&builders[c]);
    free(builders);

    /* Set up the k-way merge with a fan-in bounded to what the pool would
       give the merge now, and reserve the merge's resident bytes. */
    vec_mem_set(&sn->mem, 0);
    int64_t merge_bytes;
    int fanin = compute_merge_fanin(spill_est_bytes, spill_est_rows,
                                    vec_mem_allowance(&sn->mem), &merge_bytes);
    vec_mem_set(&sn->mem, merge_bytes);
    init_merge(sn, fanin);
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

    if (sn->phase == SORT_MEMORY)
        return memory_next_batch(sn);

    if (sn->phase == SORT_MERGING)
        return merge_next_batch(sn);

    return NULL;
}

static void sort_free(VecNode *self) {
    SortNode *sn = (SortNode *)self;
    sn->child->free_node(sn->child);
    free(sn->keys);

    free_memory_result(sn);
    vec_mem_acct_free(&sn->mem);

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

int64_t sort_node_total_rows(const SortNode *sn) {
    return sn->total_rows;
}

/* Sorting reorders rows, never adds or drops them. */
static int64_t sort_static_rows(const VecNode *self) {
    return vec_node_static_rows(((const SortNode *)self)->child);
}

VEC_ONE_CHILD_FN(sort_children, SortNode, child)
VEC_BUDGET_ACCT_FN(sort_set_grant, SortNode, mem)

void sort_node_share_grant(SortNode *sn, VecMemGrant *grant) {
    vec_node_clear_budgeted(&sn->base);
    vec_mem_acct_rebind(&sn->mem, grant);
}

SortNode *sort_node_create(VecNode *child, int n_keys, SortKey *keys,
                           const char *temp_dir, int64_t mem_budget) {
    SortNode *sn = (SortNode *)calloc(1, sizeof(SortNode));
    if (!sn) vectra_error("alloc failed for SortNode");

    sn->child      = child;
    sn->n_keys     = n_keys;
    sn->keys       = keys;
    sn->phase      = SORT_INIT;
    sn->mem_budget = mem_budget;
    sn->mem        = vec_mem_acct(NULL, mem_budget);
    sn->total_rows = -1;

    if (temp_dir) {
        sn->temp_dir = (char *)malloc(strlen(temp_dir) + 1);
        strcpy(sn->temp_dir, temp_dir);
    }

    sn->base.output_schema = vec_schema_copy(&child->output_schema);
    sn->base.next_batch    = sort_next_batch;
    sn->base.free_node     = sort_free;
    sn->base.static_rows   = sort_static_rows;
    sn->base.kind          = "SortNode";
    sn->base.children = sort_children;
    vec_node_set_budgeted(&sn->base, sn->mem_budget, sort_set_grant);
    sn->base.row_count_hint = child->row_count_hint;

    return sn;
}
