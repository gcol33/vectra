#ifndef VECTRA_FILE_MAP_H
#define VECTRA_FILE_MAP_H

#include <stdint.h>

/*
 * Read-only whole-file memory map.
 *
 * A mapped file costs address space rather than resident memory: the pages a
 * reader touches are the pages the OS keeps, and the rest stays on disk under
 * the OS's own eviction. That is what lets a file far larger than RAM be read
 * from at all, and what keeps the cost of a lookup off the size of the file it
 * looks in.
 *
 * `base` is byte-addressed and carries no alignment guarantee -- a file's
 * arrays begin wherever its header ends -- so read multi-byte values out of it
 * with memcpy rather than by casting a pointer, which would be undefined
 * behaviour on an unaligned address.
 *
 * A live mapping holds the file open. On Windows that blocks an in-place
 * replace of the mapped file until the mapping is closed, so a writer that
 * swaps a new version into place goes through vtr_atomic_replace(), which
 * retries a sharing violation rather than failing on it.
 */
typedef struct {
    const uint8_t *base;    /* mapped bytes; NULL when nothing is mapped */
    int64_t        size;    /* mapped length in bytes */
    void          *h_file;  /* Windows file HANDLE; NULL elsewhere */
    void          *h_map;   /* Windows section HANDLE; NULL elsewhere */
    int            fd;      /* POSIX descriptor; -1 elsewhere */
} VecFileMap;

/* Map the whole of `path` read-only.

   Returns 1 on success. Returns 0 on every failure -- missing, empty, larger
   than the address space, mapping unsupported on this filesystem -- leaving
   *m closed and zeroed. It never raises: a caller that cannot map a file is
   expected to have another way to proceed. */
int vec_file_map_open(VecFileMap *m, const char *path);

/* Release a mapping and its handles. Safe on an already-closed or zeroed map. */
void vec_file_map_close(VecFileMap *m);

#endif /* VECTRA_FILE_MAP_H */
