#ifndef VECTRA_CSV_READER_H
#define VECTRA_CSV_READER_H

#include <stdint.h>

/* Abstract byte reader for CSV scanning.
   Implementations: plain FILE* and gzip (whole-file inflate via miniz). */

typedef struct CsvReader CsvReader;

struct CsvReader {
    int     (*getc_fn)(CsvReader *r);
    int     (*ungetc_fn)(CsvReader *r, int c);
    int64_t (*tell_fn)(CsvReader *r);
    int     (*seek_fn)(CsvReader *r, int64_t offset);
    void    (*close_fn)(CsvReader *r);
};

/* Open a reader for the given path.
   If path ends with ".gz", the file is decompressed entirely into memory
   via miniz and exposed as a memory-cursor reader; otherwise plain fopen.
   Returns NULL on failure (caller should vectra_error). */
CsvReader *csv_reader_open(const char *path);

#endif /* VECTRA_CSV_READER_H */
