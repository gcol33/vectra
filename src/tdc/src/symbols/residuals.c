/*
 * src/symbols/residuals.c
 *
 * Symbol-stream helpers used by models and transforms. NOT a separate
 * pipeline phase — these are utility functions, not vtable-registered
 * stages. The pipeline has three phases: model, representation
 * (transform chain), entropy.
 *
 * Provides:
 *   - signed -> unsigned mapping helpers (shared with transform/zigzag.c)
 *   - varint encode/decode (used by dictionary side metadata, etc.)
 *   - widening / narrowing helpers (e.g. i8 residuals from u8 input)
 *
 * This file exists to avoid copy-paste of these primitives across
 * model/ and transform/ files. If a helper grows enough to be its own
 * transform, it gets promoted to transform/.
 */

#include "tdc/types.h"
