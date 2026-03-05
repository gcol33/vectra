#ifndef VECTRA_AGG_OPS_H
#define VECTRA_AGG_OPS_H

#include "types.h"
#include <stdint.h>

typedef enum {
    AGG_COUNT,       /* count non-NA */
    AGG_COUNT_STAR,  /* count all rows */
    AGG_SUM,
    AGG_MEAN,
    AGG_MIN,
    AGG_MAX
} AggKind;

/* Accumulator for one aggregation per group */
typedef struct {
    AggKind   kind;
    VecType   input_type;
    int       na_rm;       /* 1 = skip NAs */
    /* Per-group accumulators (length = n_groups, grown dynamically) */
    int64_t   n_groups;
    int64_t   capacity;
    int64_t  *count;       /* count of non-NA values seen */
    int64_t  *count_all;   /* count of all values (for count_star) */
    double   *sum_dbl;     /* double accumulator for sum/mean */
    int64_t  *sum_i64;     /* int64 accumulator for sum */
    double   *min_dbl;
    double   *max_dbl;
    int64_t  *min_i64;
    int64_t  *max_i64;
    int      *has_value;   /* 1 if any non-NA value seen (for min/max) */
    int      *has_na;      /* 1 if any NA seen in group (for na poisoning) */
} AggAccum;

/* Initialize accumulator */
AggAccum agg_accum_init(AggKind kind, VecType input_type, int na_rm);

/* Ensure capacity for n_groups */
void agg_accum_ensure(AggAccum *acc, int64_t n_groups);

/* Feed a value to group_id */
void agg_accum_feed(AggAccum *acc, int64_t group_id,
                    const VecArray *col, int64_t row);

/* Finish: produce result array of length n_groups */
VecArray agg_accum_finish(AggAccum *acc);

/* Free accumulator */
void agg_accum_free(AggAccum *acc);

#endif /* VECTRA_AGG_OPS_H */
