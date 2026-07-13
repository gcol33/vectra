#ifndef VECTRA_BYTE_READER_H
#define VECTRA_BYTE_READER_H

#include <stdint.h>

/* Abstract byte reader for streaming text scans (CSV, FASTA, FASTQ).
   Implementations: plain FILE* and gzip (streaming inflate via miniz).

   The only seek pattern the scanners use is "tell once, read forward, seek
   back to that mark", where the mark is a small offset near the start (the
   header, and the type-inference rewind over the first ~1000 rows). The gz
   reader satisfies this by streaming inflate through a 32 KB wrapping window
   and serving a backward seek by re-inflating from the start, so a file whose
   inflated size exceeds RAM (and a compressed size past 2 GB) reads fine. */

typedef struct ByteReader ByteReader;

struct ByteReader {
    int     (*getc_fn)(ByteReader *r);
    int     (*ungetc_fn)(ByteReader *r, int c);
    int64_t (*tell_fn)(ByteReader *r);
    int     (*seek_fn)(ByteReader *r, int64_t offset);
    void    (*close_fn)(ByteReader *r);
};

/* Open a reader for the given path.
   If path ends with ".gz", the file is decompressed entirely into memory
   via miniz and exposed as a memory-cursor reader; otherwise plain fopen.
   Returns NULL on failure (caller should vectra_error). */
ByteReader *byte_reader_open(const char *path);

#endif /* VECTRA_BYTE_READER_H */
