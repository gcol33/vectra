/*
 * tdc.h — umbrella header for the tdc compression library.
 *
 * tdc is a typed, dimensional compression library organized around four
 * orthogonal layers: model, representation, entropy, storage.
 *
 *   - model           dimension-aware predictors (1D, 2D, 2D-stack, 3D)
 *   - representation  dimension-agnostic transforms (quantize, shuffle, ...)
 *   - entropy         dimension-agnostic coders (lz, deflate, ...)
 *   - storage         self-describing block records and containers
 *
 * The unit of work is a tdc_block: a typed n-dim tile of values whose
 * structural rank and semantic layout are tracked separately. The same
 * codec pipeline encodes 1D vectors, 2D rasters, 2D stacks, and 3D volumes.
 */

#ifndef TDC_H
#define TDC_H

#include "tdc/types.h"
#include "tdc/error.h"
#include "tdc/codec.h"
#include "tdc/format.h"
#include "tdc/model.h"
#include "tdc/transform.h"
#include "tdc/entropy.h"
#include "tdc/stream.h"

#endif /* TDC_H */
