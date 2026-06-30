#ifndef VECTRA_JOIN_PARTITION_H
#define VECTRA_JOIN_PARTITION_H

#include "types.h"

/*
 * Shared blocking-partition machinery for non-equi joins.
 *
 * Both the fuzzy join and the interval join restrict their (otherwise
 * quadratic) matching to rows that agree on an equality "blocking" key:
 * fuzzy strings within the same block, intervals on the same chromosome.
 * This module groups the materialized probe and build rows into one
 * partition per shared string block-key value, so the caller's match phase
 * runs per partition instead of across the whole cross product.
 */

/* Row indices into a materialized side sharing one block-key value. */
typedef struct {
    int64_t *rows;
    int64_t  n_rows;
    int64_t  capacity;
} JoinPartition;

/* Aligned probe/build partition arrays: probe_parts[k] and build_parts[k]
   share the same block key. */
typedef struct {
    JoinPartition *probe_parts;
    JoinPartition *build_parts;
    int64_t        n_parts;
} JoinPartitionSet;

/*
 * Partition probe and build rows by a shared string blocking key.
 *
 *   probe_block_col / build_block_col < 0  -> no blocking: a single partition
 *      holds every row of each side.
 *
 * With blocking, the block columns must be VEC_STRING. Probe rows whose key
 * is absent from the build side are dropped (they cannot match anything);
 * rows with a NULL block key are dropped.
 */
JoinPartitionSet join_partition_build(
    const VecArray *p_cols, int64_t p_nrows, int probe_block_col,
    const VecArray *b_cols, int64_t b_nrows, int build_block_col);

/* Free an array of partitions (the row-index buffers). */
void join_partition_free(JoinPartition *parts, int64_t n_parts);

/* Free both partition arrays of a set. */
void join_partition_set_free(JoinPartitionSet *set);

/*
 * Drain a child node fully into flat owned VecArrays, one per output column
 * (the resident-build materialization both non-equi joins share). Honors the
 * batch selection vector. Allocates *out_cols (ncols entries) and sets
 * *out_nrows.
 */
void join_materialize_side(VecNode *child, int ncols,
                           VecArray **out_cols, int64_t *out_nrows);

#endif /* VECTRA_JOIN_PARTITION_H */
