#ifndef VECTRA_INTERVAL_JOIN_H
#define VECTRA_INTERVAL_JOIN_H

#include "types.h"

/*
 * Interval overlap join.
 *
 * Joins each probe row's [start, end] range to every build row whose
 * [start, end] range overlaps it -- the 1-D analogue of a spatial bbox join
 * (data.table::foverlaps, GenomicRanges::findOverlaps). An optional equality
 * "blocking" key (a chromosome, a sensor id) restricts overlap testing to
 * rows that agree on the key, the same role the blocking key plays in the
 * fuzzy join.
 *
 * Both sides are routed through the external sort keyed by (block, start); a
 * single serial sweep-merge then keeps an active set of open intervals per
 * side, so each overlapping pair is emitted exactly once as the streams advance
 * -- output-sensitive and bounded (peak = the two active sets + one output
 * batch + the sorts' own spill), never materializing either side or the whole
 * match set.
 */

typedef enum {
    IJOIN_INNER,   /* only overlapping pairs */
    IJOIN_LEFT     /* every probe row; build columns NA when nothing overlaps */
} IntervalJoinKind;

typedef struct {
    VecNode  base;

    VecNode *probe_node;   /* sort( probe ) keyed by (block, start) */
    VecNode *build_node;   /* sort( build ) keyed by (block, start) */

    /* Resolved column indices (stable through the sort). */
    int probe_start_col, probe_end_col;
    int build_start_col, build_end_col;
    int probe_block_col, build_block_col;  /* -1 = no blocking */

    /* Config */
    IntervalJoinKind kind;
    int closed;       /* 1 = touching endpoints count as overlap; 0 = strict */
    int n_threads;    /* unused by the serial sweep; kept for API parity */

    int   p_ncols;    /* probe / build output column counts */
    int   b_ncols;
    int   out_ncols;
    char *suffix_y;

    /* Bounded serial sweep-merge state (opaque IvSweep*, in interval_join.c). */
    void *sweep;
} IntervalJoinNode;

IntervalJoinNode *interval_join_node_create(
    VecNode *probe, VecNode *build,
    int probe_start_col, int probe_end_col,
    int build_start_col, int build_end_col,
    int probe_block_col, int build_block_col,
    IntervalJoinKind kind, int closed, int n_threads,
    const char *suffix_y, int64_t mem_budget, const char *temp_dir);

#endif /* VECTRA_INTERVAL_JOIN_H */
