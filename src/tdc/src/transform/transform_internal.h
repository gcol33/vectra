/*
 * src/transform/transform_internal.h
 *
 * Internal header listing extern declarations for every transform vtable.
 * Not part of the public include/ tree. The single consumer is
 * src/core/registry.c, which dispatches a tdc_xform_id to the matching
 * vtable address.
 *
 * Adding a new transform backend means:
 *   1. defining `const tdc_xform_vt tdc_xform_<name>_vt` in the
 *      backend's .c file (no `static`),
 *   2. adding an `extern` declaration here,
 *   3. adding the case to tdc_xform_get() in src/core/registry.c.
 */

#ifndef TDC_TRANSFORM_INTERNAL_H
#define TDC_TRANSFORM_INTERNAL_H

#include "tdc/transform.h"

#ifdef __cplusplus
extern "C" {
#endif

extern const tdc_xform_vt tdc_xform_byte_shuffle_vt;
extern const tdc_xform_vt tdc_xform_bit_shuffle_vt;
extern const tdc_xform_vt tdc_xform_quantize_vt;
extern const tdc_xform_vt tdc_xform_zigzag_vt;
/* Experimental range (0x0100-0x01FF): reference backend for the
 * "Extending" vignette. Statically compiled, always present. */
extern const tdc_xform_vt tdc_xform_complement_vt;

#ifdef __cplusplus
}
#endif

#endif /* TDC_TRANSFORM_INTERNAL_H */
