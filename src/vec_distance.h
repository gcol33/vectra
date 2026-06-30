#ifndef VECTRA_VEC_DISTANCE_H
#define VECTRA_VEC_DISTANCE_H

#include <stddef.h>
#include <math.h>

/*
 * Distance / similarity kernels over two equal-length float32 embedding
 * vectors. Accumulation is in double for stability. cosine and l2 are
 * distances (smaller = more similar, for slice_min nearest-neighbour);
 * dot is a raw inner product (larger = more similar, for slice_max).
 */

static inline double vecdist_dot(const float *a, const float *b, int64_t dim) {
    double s = 0.0;
    for (int64_t i = 0; i < dim; i++) s += (double)a[i] * (double)b[i];
    return s;
}

static inline double vecdist_l2(const float *a, const float *b, int64_t dim) {
    double s = 0.0;
    for (int64_t i = 0; i < dim; i++) {
        double d = (double)a[i] - (double)b[i];
        s += d * d;
    }
    return sqrt(s);
}

/* Cosine distance = 1 - cosine similarity. NaN when either vector is all
   zeros (similarity undefined). */
static inline double vecdist_cosine(const float *a, const float *b, int64_t dim) {
    double dot = 0.0, na = 0.0, nb = 0.0;
    for (int64_t i = 0; i < dim; i++) {
        double x = (double)a[i], y = (double)b[i];
        dot += x * y;
        na  += x * x;
        nb  += y * y;
    }
    if (na == 0.0 || nb == 0.0) return NAN;
    return 1.0 - dot / (sqrt(na) * sqrt(nb));
}

#endif /* VECTRA_VEC_DISTANCE_H */
