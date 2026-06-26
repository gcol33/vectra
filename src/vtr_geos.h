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

#endif
