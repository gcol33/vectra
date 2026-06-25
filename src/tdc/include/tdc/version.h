#ifndef TDC_VERSION_H
#define TDC_VERSION_H

/* tdc version — single source of truth for the library version.
 *
 * Kept in sync with CMakeLists.txt `project(tdc VERSION ...)`. Because tdc
 * is also vendored into non-CMake consumers (vectra's R Makevars build),
 * this header is plain and hand-edited, not generated. Bump both on release.
 */

#define TDC_VERSION_MAJOR 0
#define TDC_VERSION_MINOR 2
#define TDC_VERSION_PATCH 0
#define TDC_VERSION_STRING "0.2.0-dev"

#ifdef __cplusplus
extern "C" {
#endif

/* Returns the tdc version string, e.g. "0.1.0". The string is static and
 * must not be freed. */
const char *tdc_version(void);

#ifdef __cplusplus
}
#endif

#endif /* TDC_VERSION_H */
