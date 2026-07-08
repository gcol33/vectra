#ifndef VECTRA_REC_SPILL_H
#define VECTRA_REC_SPILL_H

#include <stdint.h>
#include <stddef.h>

/*
 * Bounded, spill-safe external sort of fixed-width byte records.
 *
 * Records of a fixed size accumulate in an in-RAM buffer; once the buffer's
 * byte size crosses mem_budget it is sorted (qsort with the caller's
 * comparator) and written to a run file, then reset. A caller then walks every
 * pushed record in ascending order through a merge cursor -- an in-place scan
 * of the sorted buffer when nothing spilled, or a k-way heap merge over the run
 * files when it did. Peak resident state is one buffer (<= mem_budget) plus, for
 * the file merge, one read block per run, so an arbitrarily large record set
 * costs disk, not an unbounded RAM atom.
 *
 * This is the shared engine under the holistic aggregates (agg_spill's median /
 * n_distinct feed 8-byte scalars) and the k-mer counter (16-byte (group, k-mer)
 * records). The comparator defines the sort order; equal records are adjacent
 * in the cursor output, which is what run-length consumers (distinct counts,
 * group counts) rely on.
 */

typedef int (*RecCmp)(const void *a, const void *b);

typedef struct {
    size_t      elem;         /* record size in bytes */
    RecCmp      cmp;          /* ascending comparator over two records */
    int64_t     mem_budget;   /* buffer byte cap before spilling; <=0 => 64 MiB */
    const char *temp_dir;     /* run-file dir; NULL => never spill (in-RAM only) */

    unsigned char *buf;       /* len*elem bytes */
    int64_t     len;          /* records in buffer */
    int64_t     cap;          /* buffer capacity in records */
    int64_t     n_total;      /* total pushed across all runs + buffer */

    char      **run_paths;    /* spilled run files (owned) */
    int64_t    *run_counts;   /* records per run */
    int         n_runs;
    int         runs_cap;
} RecSpill;

/* Initialize a store. temp_dir may be NULL (no spilling); the pointer is
   borrowed and must outlive the store. mem_budget <= 0 selects a 64 MiB
   default. */
void    rec_spill_init(RecSpill *s, size_t elem, RecCmp cmp,
                       int64_t mem_budget, const char *temp_dir);

void    rec_spill_push(RecSpill *s, const void *elem);
int64_t rec_spill_total(const RecSpill *s);

void    rec_spill_free(RecSpill *s);   /* frees buffers and unlinks runs */

/* Sorted-order cursor over every pushed record. begin() flushes the residual
   buffer (or sorts it in place when nothing spilled); next() copies the next
   ascending record into out (elem bytes) and returns 1, or returns 0 when
   drained; end() releases cursor state (the run files survive until
   rec_spill_free). One cursor at a time per store. */
typedef struct RecMerge RecMerge;

RecMerge *rec_spill_merge_begin(RecSpill *s);
int       rec_spill_merge_next(RecMerge *m, void *out);
void      rec_spill_merge_end(RecMerge *m);

#endif /* VECTRA_REC_SPILL_H */
