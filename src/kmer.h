#ifndef VECTRA_KMER_H
#define VECTRA_KMER_H

#include "types.h"

/* k-mer spectrum node.

   Counts fixed-length subsequences of a nucleotide string column, grouped by
   zero or more key columns. Output is one row per distinct (group, k-mer):
   the group key columns, a `kmer` string column, and an int64 `count`. It is a
   blocking node (consumes the whole child, emits one result batch) in the
   group_agg shape.

   Each k-mer is packed into a uint64 with the 2-bit A/C/G/T encoding
   (seq_util.h), so counting is an open-addressing hash over an integer key
   rather than a string. k is limited to 1..32 (2 bits per base in 64). A window
   containing any non-ACGT base (N, IUPAC ambiguity code, gap) is skipped, the
   same convention as jellyfish / KMC. When canonical is set, a k-mer and its
   reverse complement are collapsed to the lexicographically smaller of the two
   (equivalently, the smaller packed value). Output row order is unspecified
   (hash order). */
typedef struct {
    VecNode     base;
    VecNode    *child;
    char       *seq_col;
    int         k;
    int         canonical;
    int         n_keys;
    char      **key_names;
    int         done;
} KmerNode;

/* Create a k-mer spectrum node. Takes ownership of child and key_names. */
KmerNode *kmer_node_create(VecNode *child, const char *seq_col,
                           int k, int canonical,
                           int n_keys, char **key_names);

#endif /* VECTRA_KMER_H */
