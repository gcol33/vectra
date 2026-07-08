#include "agg_spill.h"
#include <string.h>

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

/* ------------------------------------------------------------------ */
/*  Construction and pushing                                           */
/* ------------------------------------------------------------------ */

void agg_spill_init(AggSpill *s, AggSpillType type,
                    int64_t mem_budget, const char *temp_dir) {
    s->type = type;
    rec_spill_init(&s->rec, sizeof(uint64_t),
                   type == AGG_SPILL_U64 ? cmp_u64 : cmp_f64,
                   mem_budget, temp_dir);
}

void agg_spill_push_f64(AggSpill *s, double v) {
    uint64_t slot;
    memcpy(&slot, &v, sizeof(slot));
    rec_spill_push(&s->rec, &slot);
}

void agg_spill_push_u64(AggSpill *s, uint64_t v) {
    rec_spill_push(&s->rec, &v);
}

int64_t agg_spill_total(const AggSpill *s) {
    return rec_spill_total(&s->rec);
}

/* ------------------------------------------------------------------ */
/*  Reductions                                                         */
/* ------------------------------------------------------------------ */

double agg_spill_median(AggSpill *s) {
    int64_t n = rec_spill_total(&s->rec);
    if (n == 0) return 0.0;  /* caller guards empty via agg_spill_total */

    int64_t lo = (n - 1) / 2, hi = n / 2;

    RecMerge *m = rec_spill_merge_begin(&s->rec);
    double v_lo = 0.0, v_hi = 0.0;
    uint64_t slot;
    int64_t i = 0;
    while (rec_spill_merge_next(m, &slot)) {
        if (i == lo) v_lo = slot_as_f64(slot);
        if (i == hi) { v_hi = slot_as_f64(slot); break; }
        i++;
    }
    rec_spill_merge_end(m);
    return (v_lo + v_hi) / 2.0;
}

int64_t agg_spill_n_distinct(AggSpill *s) {
    if (rec_spill_total(&s->rec) == 0) return 0;

    RecMerge *m = rec_spill_merge_begin(&s->rec);
    int64_t count = 0;
    int have_prev = 0;
    uint64_t prev = 0, slot;
    while (rec_spill_merge_next(m, &slot)) {
        if (!have_prev || slot != prev) { count++; prev = slot; have_prev = 1; }
    }
    rec_spill_merge_end(m);
    return count;
}

void agg_spill_free(AggSpill *s) {
    rec_spill_free(&s->rec);
}
