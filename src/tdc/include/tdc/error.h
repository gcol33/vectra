/*
 * tdc/error.h — frozen v0
 *
 * Error helpers. Status codes themselves live in types.h so they can be
 * used by every header without a dependency cycle.
 */

#ifndef TDC_ERROR_H
#define TDC_ERROR_H

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Human-readable string for a status code. Returns a static literal,
 * never NULL, never allocates. */
const char *tdc_strerror(tdc_status s);

#ifdef __cplusplus
}
#endif
#endif /* TDC_ERROR_H */
