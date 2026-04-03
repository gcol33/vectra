#ifndef VEC_OMP_H
#define VEC_OMP_H

#ifdef _OPENMP
#include <omp.h>
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
