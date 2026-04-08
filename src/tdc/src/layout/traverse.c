/*
 * src/layout/traverse.c
 *
 * Iteration order helpers used by models. Hard boundary: this file
 * answers "how do I walk this block?" — it never predicts anything.
 *
 *   - row-major scan (1D, 2D, 3D)
 *   - per-slice scan over STACK_2D
 *   - tile-by-tile scan
 *   - Z-order / Morton scan (post-v0; reserved)
 *
 * Models call into traverse helpers; traverse never calls models.
 */

#include "tdc/types.h"
