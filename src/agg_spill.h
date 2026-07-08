#ifndef VECTRA_AGG_SPILL_H
#define VECTRA_AGG_SPILL_H

#include <stdint.h>
#include "rec_spill.h"

/* Element interpretation for the scalar store: raw 8-byte slots compared as
   IEEE doubles or as unsigned 64-bit integers. */
typedef enum { AGG_SPILL_F64, AGG_SPILL_U64 } AggSpillType;

/*
 * Bounded, spill-safe collection of 8-byte scalars.
 *
 * The holistic aggregates median() and n_distinct() need every value (median)
 * or every distinct value (n_distinct) of a group to produce their result, so
 * their per-group state grows with the group size. AggSpill caps that resident
 * footprint by running the values through the shared external record sort
 * (RecSpill): they accumulate in an in-RAM buffer, spill to run files past
 * mem_budget, and the result is computed exactly by an external merge over the
 * runs plus the final in-RAM buffer, so a single arbitrarily large group costs
 * disk, not an unbounded RAM atom.
 *
 * median() feeds bit-cast doubles (AGG_SPILL_F64) and selects the middle
 * element(s). n_distinct() feeds 64-bit value hashes (AGG_SPILL_U64) and counts
 * distinct hashes -- the same distinct-hash semantics as the in-RAM hash set it
 * replaces, so results are unchanged (including the astronomically rare hash
 * collision that counts two values as one).
 */
typedef struct {
    AggSpillType type;
    RecSpill     rec;   /* 8-byte records; comparator picked from type */
} AggSpill;

/* Initialize a store. temp_dir may be NULL (no spilling); the pointer is
   borrowed and must outlive the store. mem_budget <= 0 selects a 64 MiB
   default. */
void    agg_spill_init(AggSpill *s, AggSpillType type,
                       int64_t mem_budget, const char *temp_dir);

void    agg_spill_push_f64(AggSpill *s, double v);
void    agg_spill_push_u64(AggSpill *s, uint64_t v);

/* Total values pushed; callers guard the empty case with this. */
int64_t agg_spill_total(const AggSpill *s);

/* Exact reductions over all pushed values. Both consume the run files but leave
   the struct safe to agg_spill_free. Callers guard the empty case via
   agg_spill_total. */
double  agg_spill_median(AggSpill *s);      /* AGG_SPILL_F64 */
int64_t agg_spill_n_distinct(AggSpill *s);  /* AGG_SPILL_U64 */

void    agg_spill_free(AggSpill *s);        /* frees buffers and unlinks runs */

#endif /* VECTRA_AGG_SPILL_H */
