/*
 * src/core/timer.h
 *
 * High-resolution wallclock timer + env-gated stage-timer check.
 * Single source of truth — replaces the identical copies that lived in
 * encode.c, decode.c, decode_ex.c, and the bench harnesses.
 *
 * Static inline: each TU gets its own copy, no link dependency.
 */

#ifndef TDC_CORE_TIMER_H
#define TDC_CORE_TIMER_H

/* clock_gettime / CLOCK_MONOTONIC are POSIX.1b. Under strict -std=c11
 * glibc hides them unless _POSIX_C_SOURCE is set. Must be defined before
 * any system header in this TU. */
#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#  define _POSIX_C_SOURCE 200809L
#endif

#include <stdlib.h>

#if defined(_WIN32)
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
static inline double tdc_now_secs(void) {
    static LARGE_INTEGER freq;
    static int inited = 0;
    LARGE_INTEGER t;
    if (!inited) { QueryPerformanceFrequency(&freq); inited = 1; }
    QueryPerformanceCounter(&t);
    return (double)t.QuadPart / (double)freq.QuadPart;
}
#else
#  include <time.h>
static inline double tdc_now_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}
#endif

/* Returns 1 when TDC_STAGE_TIMERS=1 (or any non-"0" value) is set.
 * Result is cached after first call. */
static inline int tdc_stage_timers_enabled(void) {
    static int cached = -1;
    if (cached < 0) {
        const char *e = getenv("TDC_STAGE_TIMERS");
        cached = (e && e[0] && e[0] != '0') ? 1 : 0;
    }
    return cached;
}

#endif /* TDC_CORE_TIMER_H */
