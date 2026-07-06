#include "agg_spill.h"
#include "error.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define AGG_SPILL_DEFAULT_BUDGET (64LL * 1024 * 1024)  /* 64 MiB */
#define AGG_SPILL_MERGE_BLOCK    4096                   /* elements per run read */

/* ------------------------------------------------------------------ */
/*  Element comparison (raw 8-byte slot interpreted per store type)    */
/* ------------------------------------------------------------------ */

static inline double slot_as_f64(uint64_t x) {
    double d;
    memcpy(&d, &x, sizeof(d));
    return d;
}

static int cmp_f64(const void *a, const void *b) {
    double da = slot_as_f64(*(const uint64_t *)a);
    double db = slot_as_f64(*(const uint64_t *)b);
    return (da > db) - (da < db);
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t ua = *(const uint64_t *)a, ub = *(const uint64_t *)b;
    return (ua > ub) - (ua < ub);
}

/* Strict "a before b" in ascending order, for the merge heap. */
static inline int slot_less(AggSpillType t, uint64_t a, uint64_t b) {
    if (t == AGG_SPILL_U64) return a < b;
    return slot_as_f64(a) < slot_as_f64(b);
}

static void sort_buffer(AggSpill *s) {
    qsort(s->buf, (size_t)s->len, sizeof(uint64_t),
          s->type == AGG_SPILL_U64 ? cmp_u64 : cmp_f64);
}

/* ------------------------------------------------------------------ */
/*  Construction and spilling                                          */
/* ------------------------------------------------------------------ */

void agg_spill_init(AggSpill *s, AggSpillType type,
                    int64_t mem_budget, const char *temp_dir) {
    memset(s, 0, sizeof(*s));
    s->type = type;
    s->mem_budget = mem_budget > 0 ? mem_budget : AGG_SPILL_DEFAULT_BUDGET;
    s->temp_dir = temp_dir;
}

static char *spill_run_path(const char *temp_dir) {
    static int counter = 0;
    int id = counter++;
    int len = snprintf(NULL, 0, "%s/vectra_aggspill_%d.bin", temp_dir, id);
    char *path = (char *)malloc((size_t)(len + 1));
    if (!path) vectra_error("agg spill alloc failed");
    snprintf(path, (size_t)(len + 1), "%s/vectra_aggspill_%d.bin", temp_dir, id);
    return path;
}

/* Sort the in-RAM buffer and write it as one ascending run file. Resets len. */
static void spill_flush_run(AggSpill *s) {
    if (s->len == 0) return;
    sort_buffer(s);

    char *path = spill_run_path(s->temp_dir);
    FILE *fp = fopen(path, "wb");
    if (!fp) vectra_error("agg spill: cannot open run file %s", path);
    if (fwrite(s->buf, sizeof(uint64_t), (size_t)s->len, fp) != (size_t)s->len) {
        fclose(fp);
        vectra_error("agg spill: short write to %s", path);
    }
    fclose(fp);

    if (s->n_runs >= s->runs_cap) {
        s->runs_cap = s->runs_cap == 0 ? 8 : s->runs_cap * 2;
        s->run_paths = (char **)realloc(s->run_paths,
                                        (size_t)s->runs_cap * sizeof(char *));
        s->run_counts = (int64_t *)realloc(s->run_counts,
                                           (size_t)s->runs_cap * sizeof(int64_t));
        if (!s->run_paths || !s->run_counts) vectra_error("agg spill alloc failed");
    }
    s->run_paths[s->n_runs] = path;
    s->run_counts[s->n_runs] = s->len;
    s->n_runs++;
    s->len = 0;
}

static inline void spill_push_slot(AggSpill *s, uint64_t slot) {
    if (s->len >= s->cap) {
        s->cap = s->cap == 0 ? 1024 : s->cap * 2;
        s->buf = (uint64_t *)realloc(s->buf, (size_t)s->cap * sizeof(uint64_t));
        if (!s->buf) vectra_error("agg spill alloc failed");
    }
    s->buf[s->len++] = slot;
    s->n_total++;
    /* Spill once the buffer's bytes cross the budget (only if a temp_dir
       exists to spill into; otherwise the buffer grows, matching the old
       in-RAM behavior). */
    if (s->temp_dir && s->len * (int64_t)sizeof(uint64_t) >= s->mem_budget)
        spill_flush_run(s);
}

void agg_spill_push_f64(AggSpill *s, double v) {
    uint64_t slot;
    memcpy(&slot, &v, sizeof(slot));
    spill_push_slot(s, slot);
}

void agg_spill_push_u64(AggSpill *s, uint64_t v) {
    spill_push_slot(s, v);
}

/* ------------------------------------------------------------------ */
/*  External merge                                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    FILE     *fp;
    uint64_t *block;      /* read buffer */
    int64_t   block_len;  /* valid elements in block */
    int64_t   block_pos;  /* next element to consume */
    int64_t   remaining;  /* elements still unread in the file */
    uint64_t  head;       /* current front element (valid while live) */
    int       live;
} MergeCursor;

static void cursor_refill(MergeCursor *c) {
    if (c->remaining == 0) { c->live = 0; return; }
    int64_t want = c->remaining < AGG_SPILL_MERGE_BLOCK
                 ? c->remaining : AGG_SPILL_MERGE_BLOCK;
    size_t got = fread(c->block, sizeof(uint64_t), (size_t)want, c->fp);
    if (got != (size_t)want) vectra_error("agg spill: short read during merge");
    c->block_len = want;
    c->block_pos = 0;
    c->remaining -= want;
    c->head = c->block[c->block_pos++];
    c->live = 1;
}

/* Advance to the next element; sets live=0 when the run is exhausted. */
static void cursor_advance(MergeCursor *c) {
    if (c->block_pos < c->block_len) {
        c->head = c->block[c->block_pos++];
    } else {
        cursor_refill(c);
    }
}

/* Min-heap of cursor indices ordered by head value. */
typedef struct {
    MergeCursor *cur;
    int          n_cursors;  /* total cursors (for cleanup), >= heap size */
    int         *heap;
    int          n;          /* live entries in the heap */
    AggSpillType type;
} MergeHeap;

static void heap_sift_up(MergeHeap *h, int pos) {
    while (pos > 0) {
        int parent = (pos - 1) / 2;
        if (slot_less(h->type, h->cur[h->heap[pos]].head,
                              h->cur[h->heap[parent]].head)) {
            int t = h->heap[pos]; h->heap[pos] = h->heap[parent]; h->heap[parent] = t;
            pos = parent;
        } else break;
    }
}

static void heap_sift_down(MergeHeap *h, int pos) {
    for (;;) {
        int l = 2 * pos + 1, r = 2 * pos + 2, smallest = pos;
        if (l < h->n && slot_less(h->type, h->cur[h->heap[l]].head,
                                          h->cur[h->heap[smallest]].head))
            smallest = l;
        if (r < h->n && slot_less(h->type, h->cur[h->heap[r]].head,
                                          h->cur[h->heap[smallest]].head))
            smallest = r;
        if (smallest == pos) break;
        int t = h->heap[pos]; h->heap[pos] = h->heap[smallest]; h->heap[smallest] = t;
        pos = smallest;
    }
}

/* Open all runs and build the merge heap. The final in-RAM buffer is flushed to
   a run first so every value lives in exactly one run. */
static MergeHeap *merge_open(AggSpill *s) {
    spill_flush_run(s);  /* push the residual buffer as its own run */

    MergeHeap *h = (MergeHeap *)calloc(1, sizeof(MergeHeap));
    h->type = s->type;
    h->n_cursors = s->n_runs;
    h->cur = (MergeCursor *)calloc((size_t)(s->n_runs > 0 ? s->n_runs : 1),
                                   sizeof(MergeCursor));
    h->heap = (int *)malloc((size_t)(s->n_runs > 0 ? s->n_runs : 1) * sizeof(int));

    for (int i = 0; i < s->n_runs; i++) {
        MergeCursor *c = &h->cur[i];
        c->fp = fopen(s->run_paths[i], "rb");
        if (!c->fp) vectra_error("agg spill: cannot reopen run %s", s->run_paths[i]);
        c->block = (uint64_t *)malloc(AGG_SPILL_MERGE_BLOCK * sizeof(uint64_t));
        c->remaining = s->run_counts[i];
        cursor_refill(c);
        if (c->live) { h->heap[h->n] = i; heap_sift_up(h, h->n); h->n++; }
    }
    return h;
}

/* Pop the smallest remaining element. Returns 1 and writes *out, or 0 when the
   merge is drained. */
static int merge_pop(MergeHeap *h, uint64_t *out) {
    if (h->n == 0) return 0;
    int top = h->heap[0];
    *out = h->cur[top].head;
    cursor_advance(&h->cur[top]);
    if (!h->cur[top].live) {
        h->heap[0] = h->heap[--h->n];
    }
    if (h->n > 0) heap_sift_down(h, 0);
    return 1;
}

/* Close every cursor (including runs already drained out of the heap and any
   left open when the merge stopped early, as median does). */
static void merge_close(MergeHeap *h) {
    for (int i = 0; i < h->n_cursors; i++) {
        if (h->cur[i].fp) fclose(h->cur[i].fp);
        free(h->cur[i].block);
    }
    free(h->heap);
    free(h->cur);
    free(h);
}

/* ------------------------------------------------------------------ */
/*  Reductions                                                         */
/* ------------------------------------------------------------------ */

double agg_spill_median(AggSpill *s) {
    int64_t n = s->n_total;
    if (n == 0) return 0.0;  /* caller guards empty via n_total */

    int64_t lo = (n - 1) / 2, hi = n / 2;

    if (s->n_runs == 0) {
        /* All in RAM: identical to the historical in-place path. */
        sort_buffer(s);
        double v_lo = slot_as_f64(s->buf[lo]);
        double v_hi = slot_as_f64(s->buf[hi]);
        return (v_lo + v_hi) / 2.0;
    }

    MergeHeap *h = merge_open(s);
    double v_lo = 0.0, v_hi = 0.0;
    uint64_t slot;
    int64_t i = 0;
    while (merge_pop(h, &slot)) {
        if (i == lo) v_lo = slot_as_f64(slot);
        if (i == hi) { v_hi = slot_as_f64(slot); break; }
        i++;
    }
    merge_close(h);
    return (v_lo + v_hi) / 2.0;
}

int64_t agg_spill_n_distinct(AggSpill *s) {
    if (s->n_total == 0) return 0;

    if (s->n_runs == 0) {
        sort_buffer(s);
        int64_t count = 1;
        for (int64_t i = 1; i < s->len; i++)
            if (s->buf[i] != s->buf[i - 1]) count++;
        return count;
    }

    MergeHeap *h = merge_open(s);
    int64_t count = 0;
    int have_prev = 0;
    uint64_t prev = 0, slot;
    while (merge_pop(h, &slot)) {
        if (!have_prev || slot != prev) { count++; prev = slot; have_prev = 1; }
    }
    merge_close(h);
    return count;
}

void agg_spill_free(AggSpill *s) {
    free(s->buf);
    for (int i = 0; i < s->n_runs; i++) {
        if (s->run_paths[i]) {
            remove(s->run_paths[i]);
            free(s->run_paths[i]);
        }
    }
    free(s->run_paths);
    free(s->run_counts);
    memset(s, 0, sizeof(*s));
}
