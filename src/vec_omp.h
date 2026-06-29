#ifndef VEC_OMP_H
#define VEC_OMP_H

/* OpenMP runtime declarations.
 *
 * We deliberately do NOT #include <omp.h>, and no other file in src/ may
 * either: every translation unit that needs the OpenMP runtime includes
 * this header instead. Two separate clang releases have shipped an omp.h
 * that breaks vectra when pulled in after R's headers:
 *
 *   - clang 21's bundled omp.h wrapper contains a "#pragma omp end declare
 *     variant" with no matching begin pragma, which failed compilation on
 *     CRAN's r-devel-linux-x86_64-debian-clang flavor and caused vectra
 *     0.5.1 to be archived.
 *
 *   - clang 22's omp.h opens with "#pragma omp begin declare variant
 *     match(device={kind(host)})". R's Rinternals.h defines the macro
 *     `match` -> `Rf_match`, so when omp.h is included after Rinternals.h
 *     the `match` clause is rewritten to `Rf_match` and the directive no
 *     longer parses ("expected 'match', 'adjust_args', or 'append_args'
 *     clause"). This broke six files on r-devel-linux-x86_64-fedora-clang.
 *
 * Both are in the wrapper itself, so an `#ifdef _OPENMP` guard around
 * `#include <omp.h>` doesn't help: when -fopenmp is on the compile line
 * _OPENMP is defined and the broken wrapper is pulled in regardless.
 *
 * The fix: skip the wrapper. The few runtime functions we use are
 * forward-declared below; libomp symbols resolve at link time. The
 * `#pragma omp ...` directives elsewhere in src/ are recognized by
 * the compiler without needing omp.h. */
#ifdef _OPENMP
extern int omp_get_max_threads(void);
extern int omp_get_thread_num(void);
extern int omp_in_parallel(void);
extern void omp_set_num_threads(int);
#endif

#include <stdlib.h> /* getenv */

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

/* Clamp the OpenMP team to two threads under R CMD check.
 *
 * CRAN's check farm is shared and forbids using more than two cores at
 * once; it signals this by setting _R_CHECK_LIMIT_CORES_. When that is
 * present we lower the default team size to 2. Every parallel region in
 * the package derives its width from omp_get_max_threads() (directly, or
 * via the global default after this call), so the clamp reaches all of
 * them and keeps tests, examples, and vignettes under the two-core
 * ceiling. Outside a check the variable is unset and the package uses
 * every available core. Called once from R_init_vectra. */
static inline void vec_omp_apply_core_limit(void) {
#ifdef _OPENMP
    const char *limit = getenv("_R_CHECK_LIMIT_CORES_");
    if (limit != NULL && *limit != '\0' && omp_get_max_threads() > 2)
        omp_set_num_threads(2);
#endif
}

#endif /* VEC_OMP_H */
