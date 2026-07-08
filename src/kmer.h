#ifndef VECTRA_KMER_H
#define VECTRA_KMER_H

#include "types.h"
#include "rec_spill.h"
#include "key_arena.h"

/* k-mer spectrum node.

   Counts fixed-length subsequences of a nucleotide string column, grouped by
   zero or more key columns. Output is one row per distinct (group, k-mer):
   the group key columns, a `kmer` string column, and an int64 `count`. It is a
   blocking node (consumes the whole child before emitting) that then streams
   its result in bounded batches, in the group_agg shape.

   Each k-mer is packed into a uint64 with the 2-bit A/C/G/T encoding
   (seq_util.h), so counting reduces to sorting integer keys. k is limited to
   1..32 (2 bits per base in 64). A window containing any non-ACGT base (N,
   IUPAC ambiguity code, gap) is skipped, the same convention as jellyfish /
   KMC. When canonical is set, a k-mer and its reverse complement are collapsed
   to the lexicographically smaller of the two (equivalently, the smaller packed
   value). Output row order is (group id, packed k-mer) ascending.

   Memory is bounded: each observed (group, k-mer) occurrence is pushed into an
   external record sort (RecSpill) that spills to disk past mem_budget, and the
   distinct-count reduction streams over the sorted records one output batch at
   a time. Only the group-key arena (one row per distinct group, the natural
   output cardinality) stays resident, not the k-mer table. */

typedef struct { int64_t gid; uint64_t kmer; } KmerRec;

typedef struct {
    VecNode     base;
    VecNode    *child;
    char       *seq_col;
    int         k;
    int         canonical;
    int         n_keys;
    char      **key_names;
    int64_t     mem_budget;   /* spill threshold for the record sort */
    char       *temp_dir;     /* owned run-file dir */

    int         phase;        /* 0 = consume, 1 = emit, 2 = done */
    RecSpill    spill;        /* KmerRec records, sorted by (gid, kmer) */
    KeyArena    arena;        /* distinct group keys; valid when n_keys > 0 */
    int         have_keys;
    RecMerge   *merge;        /* sorted-order cursor over spill (emit phase) */
    int         have_cur;     /* a (gid, kmer) run is open across emit batches */
    KmerRec     cur;          /* the open run's key */
    int64_t     cur_count;    /* occurrences of cur seen so far */
} KmerNode;

/* Create a k-mer spectrum node. Takes ownership of child and key_names.
   temp_dir is copied; mem_budget <= 0 selects the RecSpill default. */
KmerNode *kmer_node_create(VecNode *child, const char *seq_col,
                           int k, int canonical,
                           int n_keys, char **key_names,
                           int64_t mem_budget, const char *temp_dir);

#endif /* VECTRA_KMER_H */
