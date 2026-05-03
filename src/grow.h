#ifndef VECTRA_GROW_H
#define VECTRA_GROW_H

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include "error.h"

/* Doubling growth helper for typed growable buffers.
   Ensures *buf can hold at least `need` elements of `elem_size`, doubling
   *cap until it does. Reallocates via realloc(); aborts via vectra_error()
   on OOM. `what` names the buffer in the error message.
   This is the single source of truth for the realloc-and-double policy
   used by GBuf (csv_scan), FieldVec (csv_scan), FuzzyPartition
   (fuzzy_join), and DblVec (tiff_write). */
static inline void vec_grow_to(void **buf, int64_t *cap,
                               int64_t need, size_t elem_size,
                               const char *what) {
    if (need <= *cap) return;
    int64_t new_cap = *cap > 0 ? *cap : 1;
    while (new_cap < need) new_cap *= 2;
    void *p = realloc(*buf, (size_t)new_cap * elem_size);
    if (!p) vectra_error("realloc failed for %s", what);
    *buf = p;
    *cap = new_cap;
}

#endif /* VECTRA_GROW_H */
