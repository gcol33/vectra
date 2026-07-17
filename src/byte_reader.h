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
   inflated size exceeds RAM (and a compressed size past 2 GB) reads fine. It
   transparently follows concatenated gzip members (as produced by bgzip and
   `cat a.gz b.gz`), so a multi-member .gz reads whole rather than stopping at
   the first member.

   getc returns EOF at both a clean end and a hard decode error; error_fn tells
   them apart. A scan that reaches EOF must check error_fn before trusting the
   result, so a truncated or corrupt stream fails loudly instead of silently
   returning a short read. */

typedef struct ByteReader ByteReader;

struct ByteReader {
    int     (*getc_fn)(ByteReader *r);
    int     (*ungetc_fn)(ByteReader *r, int c);
    int64_t (*tell_fn)(ByteReader *r);
    int     (*seek_fn)(ByteReader *r, int64_t offset);
    int     (*error_fn)(ByteReader *r);   /* nonzero after a hard read/decode error */
    void    (*close_fn)(ByteReader *r);
};

/* Open a reader for the given path.
   If path ends with ".gz", the body is streamed through miniz one block at a
   time (peak memory is the 32 KB window plus one input block, independent of
   file size); otherwise plain fopen. Returns NULL on failure (caller should
   vectra_error). */
ByteReader *byte_reader_open(const char *path);

#endif /* VECTRA_BYTE_READER_H */
