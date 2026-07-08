/* Feature-space (attribute-space) k-nearest-neighbour over a resident cloud.
 *
 * This is the compute kernel behind the MOP (mobility-oriented parity)
 * transferability surface. Given a projection point (a cell's environmental
 * predictor vector) and a reference cloud (the calibration region's cells in
 * the same predictor space), it returns the mean distance to the nearest
 * `k` reference points, or the nearest `ceil(percentage% * N)` of them.
 *
 * The distance lives in predictor space, not on coordinates -- that is what
 * distinguishes this from the coordinate-based spatial_knn. Euclidean is the
 * distance as-is; Mahalanobis is folded into a linear whitening applied to
 * every point, so a single Euclidean kernel serves both metrics:
 *
 *     D_M(x,y)^2 = (x-y)' S^-1 (x-y),   S^-1 = R'R  (Cholesky, R upper)
 *                = ||R x - R y||^2.
 *
 * The caller passes R (the transform) for Mahalanobis and NULL for Euclidean;
 * every reference point is transformed once at build time and every query
 * point once per query, after which the kernel is plain Euclidean.
 *
 * Selection is brute-force scan + a bounded max-heap of the k smallest squared
 * distances, not a kd-tree. That is deliberate: MOP's primary mode asks for the
 * nearest `percentage%` of the cloud (large k), and environmental predictor
 * spaces are moderate-to-high dimensional (often ~10-20 bioclim layers) -- both
 * regimes defeat kd-tree pruning, which collapses toward a full scan above a
 * handful of dimensions and for k approaching N. The heap keeps peak memory at
 * O(k) per thread (not O(N) for a full sort) while the scan is embarrassingly
 * parallel over query rows. The reference cloud is resident and shared
 * read-only across threads; the query side streams one batch at a time.
 */

#include <R.h>
#include <Rinternals.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "vec_omp.h"
#include "feature_knn.h"

typedef struct {
    int n_ref;    /* reference points                                        */
    int n_var;    /* predictor dimensions                                     */
    double *pts;  /* n_ref * n_var, row-major, already whitened by `tf`       */
    double *tf;   /* n_var * n_var transform (column-major) or NULL (L2)      */
} FeatureKNN;

static void feature_knn_finalize(SEXP ptr) {
    FeatureKNN *idx = (FeatureKNN *) R_ExternalPtrAddr(ptr);
    if (idx == NULL) return;
    free(idx->pts);
    free(idx->tf);
    free(idx);
    R_ClearExternalPtr(ptr);
}

/* out[i] = sum_j tf[i + j*nv] * in[j]  (tf column-major), or copy when tf NULL */
static inline void apply_transform(const double *tf, int nv,
                                   const double *in, double *out) {
    if (tf == NULL) {
        memcpy(out, in, (size_t) nv * sizeof(double));
        return;
    }
    for (int i = 0; i < nv; i++) {
        double s = 0.0;
        for (int j = 0; j < nv; j++)
            s += tf[i + (size_t) j * nv] * in[j];
        out[i] = s;
    }
}

/* Sift element i down a max-heap of size n (heap[0] is the largest). */
static inline void heap_sift_down(double *h, int n, int i) {
    for (;;) {
        int l = 2 * i + 1, r = 2 * i + 2, m = i;
        if (l < n && h[l] > h[m]) m = l;
        if (r < n && h[r] > h[m]) m = r;
        if (m == i) break;
        double t = h[i]; h[i] = h[m]; h[m] = t;
        i = m;
    }
}

/* C_feature_knn_build(ref, transform):
 *   ref       -- n_ref x n_var numeric matrix, the resident reference cloud
 *                (caller has already dropped rows with any NA).
 *   transform -- n_var x n_var whitening matrix for Mahalanobis, or NULL for
 *                Euclidean.
 * Returns an external pointer to a FeatureKNN reused across every query batch. */
SEXP C_feature_knn_build(SEXP ref_sexp, SEXP transform_sexp) {
    SEXP dim = Rf_getAttrib(ref_sexp, R_DimSymbol);
    if (dim == R_NilValue || Rf_length(dim) != 2)
        Rf_error("vectra: reference cloud must be a numeric matrix");
    int n_ref = INTEGER(dim)[0];
    int n_var = INTEGER(dim)[1];
    if (n_ref < 1 || n_var < 1)
        Rf_error("vectra: reference cloud is empty");
    const double *ref = REAL(ref_sexp);

    double *tf = NULL;
    if (transform_sexp != R_NilValue) {
        SEXP td = Rf_getAttrib(transform_sexp, R_DimSymbol);
        if (td == R_NilValue || Rf_length(td) != 2 ||
            INTEGER(td)[0] != n_var || INTEGER(td)[1] != n_var)
            Rf_error("vectra: transform must be an n_var x n_var matrix");
        tf = (double *) malloc((size_t) n_var * (size_t) n_var * sizeof(double));
        if (tf == NULL)
            Rf_error("vectra: out of memory building feature-kNN index");
        memcpy(tf, REAL(transform_sexp),
               (size_t) n_var * (size_t) n_var * sizeof(double));
    }

    FeatureKNN *idx = (FeatureKNN *) calloc(1, sizeof(FeatureKNN));
    if (idx == NULL) {
        free(tf);
        Rf_error("vectra: out of memory building feature-kNN index");
    }
    idx->n_ref = n_ref;
    idx->n_var = n_var;
    idx->tf = tf;
    idx->pts = (double *) malloc((size_t) n_ref * (size_t) n_var * sizeof(double));
    if (idx->pts == NULL) {
        free(tf); free(idx);
        Rf_error("vectra: out of memory building feature-kNN index");
    }

    double *raw = (double *) malloc((size_t) n_var * sizeof(double));
    if (raw == NULL) {
        free(idx->pts); free(tf); free(idx);
        Rf_error("vectra: out of memory building feature-kNN index");
    }
    for (int p = 0; p < n_ref; p++) {
        for (int c = 0; c < n_var; c++)
            raw[c] = ref[p + (size_t) c * n_ref];
        apply_transform(tf, n_var, raw, idx->pts + (size_t) p * n_var);
    }
    free(raw);

    SEXP ptr = PROTECT(R_MakeExternalPtr(idx, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ptr, feature_knn_finalize, TRUE);
    UNPROTECT(1);
    return ptr;
}

/* C_feature_knn_query(idx, query, keff, nthreads):
 *   query -- n_q x n_var numeric matrix (one streamed batch of projection rows).
 *   keff  -- neighbours to average over (already resolved and capped to n_ref).
 * Returns a length-n_q numeric vector: per query row the mean distance to its
 * `keff` nearest reference points. A query row with any NA yields NA. */
SEXP C_feature_knn_query(SEXP idx_ptr, SEXP query_sexp, SEXP keff_sexp,
                         SEXP nthreads_sexp) {
    FeatureKNN *idx = (FeatureKNN *) R_ExternalPtrAddr(idx_ptr);
    if (idx == NULL)
        Rf_error("vectra: feature-kNN index is NULL (was it freed?)");

    SEXP dim = Rf_getAttrib(query_sexp, R_DimSymbol);
    if (dim == R_NilValue || Rf_length(dim) != 2)
        Rf_error("vectra: query must be a numeric matrix");
    int nq = INTEGER(dim)[0];
    int nv = INTEGER(dim)[1];
    if (nv != idx->n_var)
        Rf_error("vectra: query has %d predictors but the reference cloud has %d",
                 nv, idx->n_var);

    int k = INTEGER(keff_sexp)[0];
    if (k < 1) k = 1;
    if (k > idx->n_ref) k = idx->n_ref;

    const double *query = REAL(query_sexp);
    const double *pts = idx->pts;
    const double *tf = idx->tf;
    int n_ref = idx->n_ref;

    SEXP ans = PROTECT(Rf_allocVector(REALSXP, nq));
    double *out = REAL(ans);

    int nt = 1;
#ifdef _OPENMP
    nt = (Rf_length(nthreads_sexp) > 0) ? INTEGER(nthreads_sexp)[0] : 0;
    if (nt <= 0) nt = omp_get_max_threads();
    if (nt > nq) nt = nq > 0 ? nq : 1;
    if (nt < 1) nt = 1;
#else
    (void) nthreads_sexp;
#endif

    /* One scratch slice per thread: k for the heap, then 2*nv for the raw and
     * whitened query buffers. Allocated in the serial region so the parallel
     * region has no per-thread malloc that could fail unevenly. */
    size_t per = (size_t) k + 2 * (size_t) nv;
    double *scratch = (double *) malloc((size_t) nt * per * sizeof(double));
    if (scratch == NULL) {
        UNPROTECT(1);
        Rf_error("vectra: out of memory in feature-kNN query");
    }

    #pragma omp parallel num_threads(nt)
    {
        int tid = 0;
#ifdef _OPENMP
        tid = omp_get_thread_num();
#endif
        double *h  = scratch + (size_t) tid * per;
        double *qr = h + k;
        double *qt = qr + nv;

        #pragma omp for schedule(dynamic, 256)
        for (int q = 0; q < nq; q++) {
            int na = 0;
            for (int c = 0; c < nv; c++) {
                double v = query[q + (size_t) c * nq];
                if (ISNAN(v)) { na = 1; break; }
                qr[c] = v;
            }
            if (na) { out[q] = NA_REAL; continue; }
            apply_transform(tf, nv, qr, qt);

            int filled = 0;
            for (int p = 0; p < n_ref; p++) {
                const double *rp = pts + (size_t) p * nv;
                double d = 0.0;
                for (int c = 0; c < nv; c++) {
                    double diff = qt[c] - rp[c];
                    d += diff * diff;
                }
                if (filled < k) {
                    h[filled++] = d;
                    if (filled == k)
                        for (int i = k / 2 - 1; i >= 0; i--)
                            heap_sift_down(h, k, i);
                } else if (d < h[0]) {
                    h[0] = d;
                    heap_sift_down(h, k, 0);
                }
            }
            double s = 0.0;
            for (int i = 0; i < k; i++) s += sqrt(h[i]);
            out[q] = s / (double) k;
        }
    }

    free(scratch);
    UNPROTECT(1);
    return ans;
}
