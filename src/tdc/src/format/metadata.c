/*
 * src/format/metadata.c
 *
 * Side-metadata blob (de)serializers used by individual models.
 *
 * Side metadata is a FIRST-CLASS section of the block record (lives
 * between the record header and the entropy payload). This file owns
 * the framing for those blobs:
 *
 *   - dictionary blob       (model/dict.c)
 *   - plane coefficients    (model/plane2d.c)
 *   - quantize parameters   (transform/quantize.c)
 *   - stack/3D model state  (model/stack2d.c, model/pred3d.c)
 *
 * Each model owns its own params struct (declared in tdc/codec.h);
 * this file provides the byte-level packing utilities so the model
 * code stays focused on the algorithm rather than serialization.
 */

#include "tdc/format.h"
