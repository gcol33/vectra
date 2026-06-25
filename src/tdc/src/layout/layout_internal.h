/*
 * src/layout/layout_internal.h
 *
 * Internal header for layout iteration helpers. Used by models to
 * traverse blocks without duplicating index arithmetic.
 *
 * Hard boundary: layout answers "how do I walk this block?" — it never
 * predicts anything. Models call into layout helpers; layout helpers
 * never call models.
 */

#ifndef TDC_LAYOUT_INTERNAL_H
#define TDC_LAYOUT_INTERNAL_H

#include "tdc/types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Row-major index helpers ------------------------------------------- */

static inline int64_t tdc_rowmaj_2d(int64_t row, int64_t col, int64_t nx) {
    return row * nx + col;
}

static inline int64_t tdc_rowmaj_3d(int64_t z, int64_t y, int64_t x,
                                     int64_t ny, int64_t nx) {
    return (z * ny + y) * nx + x;
}

/* ----- STACK_2D slice accessor ------------------------------------------- */
/*
 * Returns a pointer to the start of slice `s` within a contiguous rank-3
 * block with shape {n_slices, ny, nx}. The pointer is cast to uint8_t*
 * and offset by s * ny * nx * elem_size bytes.
 */

static inline uint8_t *tdc_stack2d_slice(void *data, int64_t s,
                                         int64_t ny, int64_t nx,
                                         size_t elem_size) {
    return (uint8_t *)data + (size_t)(s * ny * nx) * elem_size;
}

static inline const uint8_t *tdc_stack2d_slice_const(const void *data, int64_t s,
                                                     int64_t ny, int64_t nx,
                                                     size_t elem_size) {
    return (const uint8_t *)data + (size_t)(s * ny * nx) * elem_size;
}

/* ----- Tile subdivision -------------------------------------------------- */
/*
 * Tiles subdivide a 2D (ny x nx) plane into square-ish blocks of at most
 * tile_size x tile_size. Edge tiles are smaller. Used by plane2d and
 * future block-relative transforms.
 */

typedef struct {
    int64_t y0, x0;    /* tile origin in the plane */
    int64_t ny, nx;    /* tile dimensions (may be smaller at edges) */
} tdc_tile;

/* Number of tiles covering an ny x nx plane with the given tile_size. */
int64_t tdc_tile_count(int64_t ny, int64_t nx, int64_t tile_size);

/* Get the idx-th tile (row-major tile ordering). */
tdc_tile tdc_tile_at(int64_t idx, int64_t ny, int64_t nx, int64_t tile_size);

#ifdef __cplusplus
}
#endif

#endif /* TDC_LAYOUT_INTERNAL_H */
