#ifndef VEC_OMP_H
#define VEC_OMP_H

/* OpenMP runtime declarations.
 *
 * We deliberately do NOT #include <omp.h>. clang 21's bundled omp.h
 * wrapper (/usr/lib/llvm-21/lib/clang/21/include/omp.h) contains a
 * "#pragma omp end declare variant" with no matching begin pragma,
 * which fails compilation on CRAN's r-devel-linux-x86_64-debian-clang
 * flavor and caused vectra 0.5.1 to be archived. The unbalanced pragma
 * is in the wrapper itself, so even an `#ifdef _OPENMP` guard around
 * `#include <omp.h>` doesn't help: when -fopenmp is on the compile
 * line, _OPENMP is defined and the broken wrapper is pulled in.
 *
 * The fix: skip the wrapper. The few runtime functions we use are
 * forward-declared below; libomp symbols resolve at link time. The
 * `#pragma omp ...` directives elsewhere in src/ are recognized by
 * the compiler without needing omp.h. */
#ifdef _OPENMP
extern int omp_get_max_threads(void);
extern int omp_get_thread_num(void);
extern int omp_in_parallel(void);
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
