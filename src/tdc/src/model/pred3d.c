/*
 * src/model/pred3d.c
 *
 * TDC_MODEL_PRED_3D — true 3D neighborhood predictor over a VOLUME_3D
 * block. Uses six face-adjacent voxels for prediction (left, right, up,
 * down, front, back), or a 3D Paeth-style variant.
 *
 * NEW in tdc v0 — no vectra source. Distinct from STACK_2D: a 3D volume
 * has meaningful inter-slice neighborhood structure (CT scans, voxel
 * rasters), so the predictor should look across all three axes rather
 * than treating each slice independently.
 */

#include "tdc/model.h"
#include "tdc/codec.h"
