/*
 * src/entropy/entropy_internal.h
 *
 * Internal header listing extern declarations for every entropy vtable.
 * Not part of the public include/ tree. The single consumer is
 * src/core/registry.c, which dispatches a tdc_entropy_id to the matching
 * vtable address.
 *
 * Adding a new entropy backend means:
 *   1. defining `const tdc_entropy_vt tdc_entropy_<name>_vt` in the
 *      backend's .c file (no `static`),
 *   2. adding an `extern` declaration here,
 *   3. adding the case to tdc_entropy_get() in src/core/registry.c.
 */

#ifndef TDC_ENTROPY_INTERNAL_H
#define TDC_ENTROPY_INTERNAL_H

#include "tdc/entropy.h"

#ifdef __cplusplus
extern "C" {
#endif

extern const tdc_entropy_vt tdc_entropy_none_vt;
extern const tdc_entropy_vt tdc_entropy_lz2_vt;

#ifdef __cplusplus
}
#endif

#endif /* TDC_ENTROPY_INTERNAL_H */
