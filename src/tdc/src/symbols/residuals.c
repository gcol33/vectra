/*
 * src/symbols/residuals.c
 *
 * Symbol-stream helpers used by models and transforms. NOT a separate
 * pipeline phase — these are utility functions, not vtable-registered
 * stages. The pipeline has three phases: model, representation
 * (transform chain), entropy.
 *
 * Provides:
 *   - signed -> unsigned mapping helpers (zigzag; static inline in header)
 *   - varint encode/decode (LEB128; static inline in header)
 *   - widening / narrowing helpers (e.g. i8 residuals from u8 input)
 *
 * This file exists to avoid copy-paste of these primitives across
 * model/ and transform/ files. All zigzag and varint functions are
 * static inline in symbols_internal.h; this translation unit provides
 * a compilation check.
 */

#include "symbols_internal.h"
