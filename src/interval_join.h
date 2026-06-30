#ifndef VECTRA_INTERVAL_JOIN_H
#define VECTRA_INTERVAL_JOIN_H

#include "types.h"
#include "join_partition.h"

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
 * Both sides are materialized resident, then within each block partition a
 * sweep-line over the interval endpoints emits each overlapping pair exactly
 * once -- output-sensitive, O((n + m) log(n + m) + pairs) per partition
 * rather than the n * m of an all-pairs scan.
 */

typedef enum {
    IJOIN_INNER,   /* only overlapping pairs */
    IJOIN_LEFT     /* every probe row; build columns NA when nothing overlaps */
} IntervalJoinKind;

typedef enum {
    IJSTATE_MATERIALIZE,
    IJSTATE_EMIT,
    IJSTATE_DONE
} IntervalJoinState;

/* One emitted pair. build_idx == -1 marks a left-join probe row with no
   overlap (build columns filled with NA). */
typedef struct {
    int64_t probe_idx;
    int64_t build_idx;
} IntervalMatch;

typedef struct {
    VecNode  base;

    VecNode *probe_node;
    VecNode *build_node;

    /* Resolved column indices */
    int probe_start_col, probe_end_col;
    int build_start_col, build_end_col;
    int probe_block_col, build_block_col;  /* -1 = no blocking */

    /* Config */
    IntervalJoinKind kind;
    int closed;       /* 1 = touching endpoints count as overlap; 0 = strict */
    int n_threads;

    /* Materialized sides */
    int       p_ncols; VecArray *p_cols; int64_t p_nrows;
    int       b_ncols; VecArray *b_cols; int64_t b_nrows;

    /* Block partitions (shared machinery) */
    JoinPartition *probe_parts;
    JoinPartition *build_parts;
    int64_t        n_parts;

    /* Match results (merged from all threads) */
    IntervalMatch *matches;
    int64_t        n_matches;

    /* Output state */
    IntervalJoinState state;
    int64_t           emit_pos;

    int   out_ncols;
    char *suffix_y;
} IntervalJoinNode;

IntervalJoinNode *interval_join_node_create(
    VecNode *probe, VecNode *build,
    int probe_start_col, int probe_end_col,
    int build_start_col, int build_end_col,
    int probe_block_col, int build_block_col,
    IntervalJoinKind kind, int closed, int n_threads,
    const char *suffix_y);

#endif /* VECTRA_INTERVAL_JOIN_H */
