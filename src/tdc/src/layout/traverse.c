/*
 * src/layout/traverse.c
 *
 * Iteration order helpers used by models. Hard boundary: this file
 * answers "how do I walk this block?" — it never predicts anything.
 *
 * Currently provides:
 *   - Row-major index helpers (static inline in layout_internal.h)
 *   - STACK_2D slice accessor (static inline in layout_internal.h)
 *
 * Future (post-v0):
 *   - Z-order / Morton scan
 *   - Hilbert curve scan
 *
 * Models call into traverse helpers; traverse never calls models.
 *
 * All index helpers are static inline in layout_internal.h. This
 * translation unit exists so the CMakeLists.txt source list stays in
 * sync and so the header gets at least one compilation check.
 */

#include "layout_internal.h"
