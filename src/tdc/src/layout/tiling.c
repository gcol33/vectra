/*
 * src/layout/tiling.c
 *
 * Tile-of-block subdivision. Used by the PLANE predictor in
 * model/plane2d.c (per-tile 3-coefficient plane fit) and available
 * for future block-relative coordinate transforms.
 *
 * Functions here are non-static because they are called from multiple
 * translation units (plane2d.c, potentially stack2d.c).
 */

#include "layout_internal.h"

int64_t tdc_tile_count(int64_t ny, int64_t nx, int64_t tile_size) {
    if (tile_size <= 0 || ny <= 0 || nx <= 0) return 0;
    int64_t tiles_y = (ny + tile_size - 1) / tile_size;
    int64_t tiles_x = (nx + tile_size - 1) / tile_size;
    return tiles_y * tiles_x;
}

tdc_tile tdc_tile_at(int64_t idx, int64_t ny, int64_t nx, int64_t tile_size) {
    tdc_tile t = {0, 0, 0, 0};
    if (tile_size <= 0 || ny <= 0 || nx <= 0) return t;

    int64_t tiles_x = (nx + tile_size - 1) / tile_size;
    int64_t tr = idx / tiles_x;
    int64_t tc = idx % tiles_x;

    t.y0 = tr * tile_size;
    t.x0 = tc * tile_size;
    t.ny = (t.y0 + tile_size <= ny) ? tile_size : (ny - t.y0);
    t.nx = (t.x0 + tile_size <= nx) ? tile_size : (nx - t.x0);
    return t;
}
