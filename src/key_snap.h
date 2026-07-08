#ifndef VECTRA_KEY_SNAP_H
#define VECTRA_KEY_SNAP_H

#include "types.h"
#include "batch.h"

/*
 * Group-boundary detector for a key-sorted stream.
 *
 * A KeySnap holds a copy of one row's group-key values. Walking a stream that
 * a SortNode has ordered by those keys, snap_matches() tells consecutive rows
 * with identical keys apart from a group boundary, and snap_update() captures
 * the new group's keys. Two NA keys compare equal (both-NA = same group), the
 * same key equality the hash aggregation uses. Shared by group_agg (summarise)
 * and group_topn (grouped slice) so a single boundary rule serves both.
 */
typedef struct {
    int       n_keys;
    VecType  *types;
    int64_t  *i64;
    double   *dbl;
    uint8_t  *bln;
    char     *str_data;
    int64_t  *str_offs;    /* n_keys + 1 entries */
    int64_t   str_cap;
    uint8_t  *valid;
    int       initialized;
} KeySnap;

/* Allocate a snapshot for n_keys columns of the given types. */
KeySnap snap_create(int n_keys, const VecType *types);

/* Free snapshot storage. */
void snap_free(KeySnap *s);

/* 1 if row `row` of `batch` has the same keys as the snapshot (an uninitialized
   snapshot never matches, opening the first group). key_indices maps key k to
   its column index in `batch`. */
int snap_matches(const KeySnap *s, const VecBatch *batch,
                 int64_t row, const int *key_indices);

/* Capture the keys of row `row` of `batch` into the snapshot (marks it
   initialized). */
void snap_update(KeySnap *s, const VecBatch *batch,
                 int64_t row, const int *key_indices);

#endif /* VECTRA_KEY_SNAP_H */
