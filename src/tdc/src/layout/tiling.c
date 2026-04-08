/*
 * src/layout/tiling.c
 *
 * Tile-of-block subdivision. Currently used by the PLANE predictor in
 * model/plane2d.c (per-tile 3-coefficient plane fit). Will also be used
 * by future block-relative coordinate transforms.
 *
 * Source today: tile-walk loop in vectra/src/vtr_codec.c (PLANE branch).
 */

#include "tdc/types.h"
