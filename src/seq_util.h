#ifndef VECTRA_SEQ_UTIL_H
#define VECTRA_SEQ_UTIL_H

/* Shared nucleotide helpers for the sequence features (expr_seq, kmer).
   The 2-bit encoding A=0, C=1, G=2, T/U=3 is the single source of truth for
   both the codon table in expr_seq and the k-mer packing in kmer. */

/* Base -> 2-bit code (A=0, C=1, G=2, T/U=3); -1 for anything else
   (N, IUPAC ambiguity codes, gaps). */
static inline int seq_base2bit(char b) {
    switch (b) {
    case 'A': case 'a': return 0;
    case 'C': case 'c': return 1;
    case 'G': case 'g': return 2;
    case 'T': case 't': case 'U': case 'u': return 3;
    default:  return -1;
    }
}

#endif /* VECTRA_SEQ_UTIL_H */
