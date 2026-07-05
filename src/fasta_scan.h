#ifndef VECTRA_FASTA_SCAN_H
#define VECTRA_FASTA_SCAN_H

#include "types.h"
#include "byte_reader.h"

/* Streaming FASTA / FASTQ scan node.
   Records become rows: id, desc, seq (+ qual for FASTQ). One batch per
   batch_size records, so a larger-than-RAM read set never materializes.
   Gzip (.gz) input rides the shared byte_reader path. */
typedef struct {
    VecNode     base;
    ByteReader *reader;
    int         is_fastq;        /* 0 = FASTA, 1 = FASTQ */
    int64_t     batch_size;      /* records per batch */
    int         quiet;           /* suppress the end-of-scan record-count log */
    char       *path;            /* file path, for the log line */
    char       *pending_header;  /* FASTA: header of the next record (no '>') */
    int64_t     records_emitted; /* cumulative, for the log */
    int         exhausted;
    int         logged;          /* end-of-scan log emitted once */
} FastaScanNode;

/* Create a FASTA/FASTQ scan node.
   path:       path to a .fasta/.fa/.fastq/.fq file (optionally .gz)
   batch_size: records per batch (default 65536)
   is_fastq:   0 = FASTA, 1 = FASTQ
   quiet:      1 = suppress the end-of-scan record-count log */
FastaScanNode *fasta_scan_node_create(const char *path, int64_t batch_size,
                                      int is_fastq, int quiet);

#endif /* VECTRA_FASTA_SCAN_H */
