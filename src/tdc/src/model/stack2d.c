/*
 * src/model/stack2d.c
 *
 * TDC_MODEL_STACK_2D — per-slice 2D predictor over a STACK_2D block
 * (rank 3, shape = {n_slices, ny, nx}).
 *
 * v0 strategy: loop over slices, dispatch each (ny, nx) plane to the
 * 2D predictor in model/pred2d.c. Optionally a per-pixel inter-slice
 * residual (slice[i] - slice[i-1]) before the in-plane prediction; this
 * is selected via tdc_stack2d_params (to be added in codec.h when
 * implementation begins).
 *
 * NEW in tdc v0 — no vectra source to extract from. The structural
 * justification is that 2D-stack data (e.g. multi-band rasters,
 * time series of frames) is dimensionally rank-3 but the most useful
 * neighborhood is in-plane, not volumetric.
 */

#include "tdc/model.h"
#include "tdc/codec.h"
