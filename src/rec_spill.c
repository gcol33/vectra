#include "rec_spill.h"
#include "error.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define REC_SPILL_DEFAULT_BUDGET (64LL * 1024 * 1024)  /* 64 MiB */
#define REC_SPILL_MERGE_BLOCK    4096                    /* records per run read */

/* ------------------------------------------------------------------ */
/*  Construction and spilling                                          */
/* ------------------------------------------------------------------ */

void rec_spill_init(RecSpill *s, size_t elem, RecCmp cmp,
                    int64_t mem_budget, const char *temp_dir) {
    memset(s, 0, sizeof(*s));
    s->elem = elem;
    s->cmp = cmp;
    s->mem_budget = mem_budget > 0 ? mem_budget : REC_SPILL_DEFAULT_BUDGET;
    s->temp_dir = temp_dir;
}

int64_t rec_spill_total(const RecSpill *s) { return s->n_total; }

static inline unsigned char *slot_ptr(RecSpill *s, int64_t i) {
    return s->buf + (size_t)i * s->elem;
}

static void sort_buffer(RecSpill *s) {
    if (s->len == 0) return;
    qsort(s->buf, (size_t)s->len, s->elem, s->cmp);
}

static char *spill_run_path(const char *temp_dir) {
    static int counter = 0;
    int id = counter++;
    int len = snprintf(NULL, 0, "%s/vectra_recspill_%d.bin", temp_dir, id);
    char *path = (char *)malloc((size_t)(len + 1));
    if (!path) vectra_error("rec spill alloc failed");
    snprintf(path, (size_t)(len + 1), "%s/vectra_recspill_%d.bin", temp_dir, id);
    return path;
}

/* Sort the in-RAM buffer and write it as one ascending run file. Resets len. */
static void spill_flush_run(RecSpill *s) {
    if (s->len == 0) return;
    sort_buffer(s);

    char *path = spill_run_path(s->temp_dir);
    FILE *fp = fopen(path, "wb");
    if (!fp) vectra_error("rec spill: cannot open run file %s", path);
    if (fwrite(s->buf, s->elem, (size_t)s->len, fp) != (size_t)s->len) {
        fclose(fp);
        vectra_error("rec spill: short write to %s", path);
    }
    fclose(fp);

    if (s->n_runs >= s->runs_cap) {
        s->runs_cap = s->runs_cap == 0 ? 8 : s->runs_cap * 2;
        s->run_paths = (char **)realloc(s->run_paths,
                                        (size_t)s->runs_cap * sizeof(char *));
        s->run_counts = (int64_t *)realloc(s->run_counts,
                                           (size_t)s->runs_cap * sizeof(int64_t));
        if (!s->run_paths || !s->run_counts) vectra_error("rec spill alloc failed");
    }
    s->run_paths[s->n_runs] = path;
    s->run_counts[s->n_runs] = s->len;
    s->n_runs++;
    s->len = 0;
}

void rec_spill_push(RecSpill *s, const void *elem) {
    if (s->len >= s->cap) {
        s->cap = s->cap == 0 ? 1024 : s->cap * 2;
        s->buf = (unsigned char *)realloc(s->buf, (size_t)s->cap * s->elem);
        if (!s->buf) vectra_error("rec spill alloc failed");
    }
    memcpy(slot_ptr(s, s->len), elem, s->elem);
    s->len++;
    s->n_total++;
    /* Spill once the buffer's bytes cross the budget (only if a temp_dir exists
       to spill into; otherwise the buffer grows, an in-RAM-only store). */
    if (s->temp_dir && s->len * (int64_t)s->elem >= s->mem_budget)
        spill_flush_run(s);
}

void rec_spill_free(RecSpill *s) {
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

/* ------------------------------------------------------------------ */
/*  External merge cursor                                              */
/* ------------------------------------------------------------------ */

typedef struct {
    FILE          *fp;
    unsigned char *block;      /* elem-sized read buffer, MERGE_BLOCK records */
    int64_t        block_len;  /* valid records in block */
    int64_t        block_pos;  /* next record to consume */
    int64_t        remaining;  /* records still unread in the file */
    unsigned char *head;       /* current front record (points into block) */
    int            live;
} MergeCursor;

struct RecMerge {
    RecSpill    *s;
    size_t       elem;
    RecCmp       cmp;

    int          in_ram;       /* 1 => walk the sorted buffer directly */
    int64_t      buf_pos;      /* in-RAM scan position */

    MergeCursor *cur;          /* file merge cursors */
    int          n_cursors;
    int         *heap;         /* cursor indices ordered by head record */
    int          n;            /* live heap entries */
};

static void cursor_refill(MergeCursor *c, size_t elem) {
    if (c->remaining == 0) { c->live = 0; return; }
    int64_t want = c->remaining < REC_SPILL_MERGE_BLOCK
                 ? c->remaining : REC_SPILL_MERGE_BLOCK;
    size_t got = fread(c->block, elem, (size_t)want, c->fp);
    if (got != (size_t)want) vectra_error("rec spill: short read during merge");
    c->block_len = want;
    c->block_pos = 0;
    c->remaining -= want;
    c->head = c->block;
    c->block_pos = 1;
    c->live = 1;
}

static void cursor_advance(MergeCursor *c, size_t elem) {
    if (c->block_pos < c->block_len) {
        c->head = c->block + (size_t)c->block_pos * elem;
        c->block_pos++;
    } else {
        cursor_refill(c, elem);
    }
}

static void heap_sift_up(RecMerge *m, int pos) {
    while (pos > 0) {
        int parent = (pos - 1) / 2;
        if (m->cmp(m->cur[m->heap[pos]].head, m->cur[m->heap[parent]].head) < 0) {
            int t = m->heap[pos]; m->heap[pos] = m->heap[parent]; m->heap[parent] = t;
            pos = parent;
        } else break;
    }
}

static void heap_sift_down(RecMerge *m, int pos) {
    for (;;) {
        int l = 2 * pos + 1, r = 2 * pos + 2, smallest = pos;
        if (l < m->n && m->cmp(m->cur[m->heap[l]].head,
                               m->cur[m->heap[smallest]].head) < 0)
            smallest = l;
        if (r < m->n && m->cmp(m->cur[m->heap[r]].head,
                               m->cur[m->heap[smallest]].head) < 0)
            smallest = r;
        if (smallest == pos) break;
        int t = m->heap[pos]; m->heap[pos] = m->heap[smallest]; m->heap[smallest] = t;
        pos = smallest;
    }
}

RecMerge *rec_spill_merge_begin(RecSpill *s) {
    RecMerge *m = (RecMerge *)calloc(1, sizeof(RecMerge));
    if (!m) vectra_error("rec spill alloc failed");
    m->s = s;
    m->elem = s->elem;
    m->cmp = s->cmp;

    if (s->n_runs == 0) {
        /* Never spilled: sort the buffer once and walk it in place. Identical
           order to the file path, no I/O. */
        sort_buffer(s);
        m->in_ram = 1;
        m->buf_pos = 0;
        return m;
    }

    /* Push the residual buffer as its own run so every record lives in a run. */
    spill_flush_run(s);

    m->cur = (MergeCursor *)calloc((size_t)s->n_runs, sizeof(MergeCursor));
    m->heap = (int *)malloc((size_t)s->n_runs * sizeof(int));
    if (!m->cur || !m->heap) vectra_error("rec spill alloc failed");
    m->n_cursors = s->n_runs;

    for (int i = 0; i < s->n_runs; i++) {
        MergeCursor *c = &m->cur[i];
        c->fp = fopen(s->run_paths[i], "rb");
        if (!c->fp) vectra_error("rec spill: cannot reopen run %s", s->run_paths[i]);
        c->block = (unsigned char *)malloc((size_t)REC_SPILL_MERGE_BLOCK * s->elem);
        if (!c->block) vectra_error("rec spill alloc failed");
        c->remaining = s->run_counts[i];
        cursor_refill(c, s->elem);
        if (c->live) { m->heap[m->n] = i; heap_sift_up(m, m->n); m->n++; }
    }
    return m;
}

int rec_spill_merge_next(RecMerge *m, void *out) {
    if (m->in_ram) {
        if (m->buf_pos >= m->s->len) return 0;
        memcpy(out, slot_ptr(m->s, m->buf_pos), m->elem);
        m->buf_pos++;
        return 1;
    }
    if (m->n == 0) return 0;
    int top = m->heap[0];
    memcpy(out, m->cur[top].head, m->elem);
    cursor_advance(&m->cur[top], m->elem);
    if (!m->cur[top].live) {
        m->heap[0] = m->heap[--m->n];
    }
    if (m->n > 0) heap_sift_down(m, 0);
    return 1;
}

void rec_spill_merge_end(RecMerge *m) {
    if (!m) return;
    for (int i = 0; i < m->n_cursors; i++) {
        if (m->cur[i].fp) fclose(m->cur[i].fp);
        free(m->cur[i].block);
    }
    free(m->heap);
    free(m->cur);
    free(m);
}
