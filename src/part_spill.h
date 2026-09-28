#ifndef VECTRA_PART_SPILL_H
#define VECTRA_PART_SPILL_H

#include "types.h"

/*
 * Partitioned spill: route rows into K run-files by a caller-computed
 * partition id (the grace-hash spill of a join, the overflow partitions of a
 * hash aggregation).
 *
 * Rows are buffered per partition and written as row groups of about
 * PART_SPILL_RG_ROWS rows, so a partition file holds a few full row groups
 * rather than one sliver per input batch (a 64-way split of a 131072-row
 * batch is ~2000 rows). All buffers together are flushed once they pass
 * `buf_budget` bytes. A partition's file is created on its first flush, so a
 * partition that never receives a row creates no file; part_spill_touch()
 * creates an empty one where a consumer needs every file to exist. Runs are
 * written with VTR_SPILL_COMPRESS. Within a partition, rows keep their input
 * order.
 */

#define PART_SPILL_RG_ROWS 65536

typedef struct PartSpill PartSpill;

/* K partitions writing to paths[0..K) (borrowed; must outlive the spill),
   rows laid out by `schema` (copied). Spill column c is read from input
   column col_map[c] (copied; NULL = column c). */
PartSpill *part_spill_create(const VecSchema *schema, const int *col_map,
                             int K, char *const *paths, int64_t buf_budget);

/* Route the logical rows of `batch`: pid[li] is the partition of logical row
   li, or -1 to skip it (pid NULL = every row to partition 0). */
void part_spill_route(PartSpill *ps, const VecBatch *batch, const int *pid);

/* 1 if partition p has received a row. */
int part_spill_used(const PartSpill *ps, int p);

/* Create partition p's file even if it received no rows. */
void part_spill_touch(PartSpill *ps, int p);

/* Flush every buffer, close every file, free the spill. */
void part_spill_close(PartSpill *ps);

/* Mix a recursion-depth salt into a key hash before taking it modulo K. A
   constant XOR before `% K` is only a fixed bucket permutation (keys that
   collide stay collided), so the salt goes in multiplicatively (murmur3
   fmix): a different depth reshuffles which keys share a partition, so an
   oversized partition actually splits when it is re-partitioned. Salt 0 (the
   top level) is the identity. */
static inline uint64_t part_spill_salt(uint64_t h, uint64_t salt) {
    if (salt == 0) return h;
    h ^= salt * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

#endif /* VECTRA_PART_SPILL_H */
