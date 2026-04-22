#ifndef VEC_OMP_H
#define VEC_OMP_H

/* Single source of truth for OpenMP inclusion.
 *
 * clang-22's <omp.h> uses `match` as a pragma-clause token inside
 * `#pragma omp declare variant(...) match(...)`. R's <Rinternals.h>
 * defines `match` as `Rf_match` via the default remapping macros.
 * If the R header is included first, the macro rewrites the pragma
 * clause and the file fails to compile on Fedora clang. Undef across
 * the include and restore afterwards. Every .c/.h in this package must
 * go through this header — never include <omp.h> directly. */

#ifdef _OPENMP
#  ifdef match
#    define VEC_OMP_SAVED_MATCH 1
#    undef match
#  endif
#  include <omp.h>
#  ifdef VEC_OMP_SAVED_MATCH
#    define match Rf_match
#    undef VEC_OMP_SAVED_MATCH
#  endif
#endif

/* Minimum elements before spawning OpenMP threads */
#define VEC_OMP_THRESHOLD 32768

/* Helper: get number of threads, respecting R's settings */
static inline int vec_omp_threads(void) {
#ifdef _OPENMP
    int n = omp_get_max_threads();
    return n > 1 ? n : 1;
#else
    return 1;
#endif
}

#endif /* VEC_OMP_H */
