/* Shared GEOS setup used by every libgeos-backed source file.
 *
 * The GEOS C API is supplied by the libgeos package and resolved at load time
 * through R_GetCCallable (see libgeos.c). vectra owns no GEOS source and links
 * no system library. Bind the API once per process, and hand out an error
 * handler that swallows GEOS messages so failures surface as the NULL returns
 * the callers already guard rather than as console noise.
 */
#ifndef VTR_GEOS_H
#define VTR_GEOS_H

#include "libgeos.h"

/* Bind the libgeos function pointers on first call; a no-op afterwards. */
void vtr_geos_ensure_api(void);

/* Error handler that discards GEOS messages (errors become guarded NULLs). */
void vtr_geos_quiet_handler(const char *message, void *userdata);

/* Bracket the code that calls the GEOS C API through the libgeos function
 * pointers. GEOS defines that API in C++, where the opaque handles
 * (GEOSGeometry, GEOSWKBReader, ...) are pointers to C++ classes; here they are
 * pointers to incomplete C structs. The two types are ABI-identical, but
 * clang's -fsanitize=function compares the type names and reports every call.
 * No C declaration can name the C++ types, so the function-type check alone is
 * turned off for the functions defined between these markers; every other
 * sanitizer check still applies to them.
 *
 * The body of an OpenMP parallel region is compiled into a separate function
 * that does not carry the attribute, so a region never calls GEOS directly: it
 * calls a named worker function defined between the markers, whose loop is an
 * orphaned `omp for`. */
#if defined(__clang__)
# define VTR_GEOS_CALLS_BEGIN \
    _Pragma("clang attribute push(__attribute__((no_sanitize(\"function\"))), apply_to = function)")
# define VTR_GEOS_CALLS_END _Pragma("clang attribute pop")
#else
# define VTR_GEOS_CALLS_BEGIN
# define VTR_GEOS_CALLS_END
#endif

#endif
