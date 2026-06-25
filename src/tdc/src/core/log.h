/*
 * src/core/log.h
 *
 * Internal debug logging macros for tdc.
 *
 * By default these expand to no-ops so no stdio symbols (stderr,
 * fprintf) appear in release object files. R/CRAN policy forbids
 * writing to stdout/stderr from compiled code, so the vendored copy
 * that ships inside vectra must be built with logging disabled.
 *
 * Define TDC_ENABLE_STDERR_LOG at build time to restore fprintf
 * output for local profiling / debugging.
 */

#ifndef TDC_CORE_LOG_H
#define TDC_CORE_LOG_H

#ifdef TDC_ENABLE_STDERR_LOG
#  include <stdio.h>
#  define TDC_LOG(...)    fprintf(stderr, __VA_ARGS__)
#  define TDC_LOG_FLUSH() fflush(stderr)
#else
#  define TDC_LOG(...)    ((void)0)
#  define TDC_LOG_FLUSH() ((void)0)
#endif

#endif /* TDC_CORE_LOG_H */
