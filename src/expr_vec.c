#include "expr.h"
#include "array.h"
#include "vec_distance.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "vec_omp.h"

/*
 * Embedding distance ops (cosine / l2 / dot).
 *
 * An embedding rides through the engine as a hex-encoded little-endian
 * float32 blob in an ordinary VEC_STRING column -- the same "opaque blob per
 * cell" trick geometry uses for hex-WKB, which keeps the bytes ASCII so they
 * round-trip any string codec losslessly. Each cell of 8*dim hex chars
 * decodes to `dim` floats. The query side is either a second embedding
 * column or a constant query vector decoded once and shared across threads.
 */

#define VEC_PAR_THRESHOLD 256
#define VEC_CHUNK 64

static inline int hexval(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* Number of float32 values a hex blob of `hexlen` chars encodes (0 if the
   length is not a multiple of 8). */
static inline int64_t hex_dim(int64_t hexlen) {
    if (hexlen <= 0 || (hexlen % 8) != 0) return 0;
    return hexlen / 8;
}

/* Decode `dim` little-endian float32 values from `hex` into dst (capacity
   >= dim). Returns 0 on success, -1 on a bad hex digit. */
static int decode_hex_floats(const char *hex, int64_t dim, float *dst) {
    for (int64_t k = 0; k < dim; k++) {
        const char *p = hex + k * 8;
        uint32_t u = 0;
        for (int byte = 0; byte < 4; byte++) {
            int hi = hexval(p[byte * 2]);
            int lo = hexval(p[byte * 2 + 1]);
            if (hi < 0 || lo < 0) return -1;
            uint32_t b = (uint32_t)((hi << 4) | lo);
            u |= b << (8 * byte);   /* little-endian: first byte is LSB */
        }
        float f;
        memcpy(&f, &u, sizeof(float));
        dst[k] = f;
    }
    return 0;
}

static inline const char *str_ptr(const VecArray *a, int64_t row) {
    return a->buf.str.data + a->buf.str.offsets[row];
}
static inline int64_t str_len(const VecArray *a, int64_t row) {
    return a->buf.str.offsets[row + 1] - a->buf.str.offsets[row];
}

/* Grow a per-thread float scratch buffer to at least `need` elements. */
static inline void ensure_cap(float **buf, int64_t *cap, int64_t need) {
    if (need > *cap) {
        *cap = need;
        *buf = (float *)realloc(*buf, (size_t)need * sizeof(float));
        if (!*buf) vectra_error("alloc failed for embedding scratch");
    }
}

static inline double vec_distance(char fn, const float *a, const float *b,
                                  int64_t dim) {
    switch (fn) {
    case 'c': return vecdist_cosine(a, b, dim);
    case 'e': return vecdist_l2(a, b, dim);
    case 'd': return vecdist_dot(a, b, dim);
    default:  return NAN;
    }
}

VecArray *vec_expr_eval_vec(const VecExpr *expr, const VecBatch *batch) {
    VecArray *a = vec_expr_eval(expr->operand, batch);
    if (a->type != VEC_STRING)
        vectra_error("embedding distance: column must be an embedding "
                     "(hex float32 string), got %s", vec_type_name(a->type));
    int64_t n = a->length;

    /* Second operand: a column of embeddings, or a constant query vector. */
    VecArray *b = NULL;
    float *qvec = NULL;
    int64_t qdim = 0;
    if (expr->left) {
        b = vec_expr_eval(expr->left, batch);
        if (b->type != VEC_STRING)
            vectra_error("embedding distance: second column must be an "
                         "embedding (hex float32 string)");
    } else if (expr->lit_str) {
        int64_t qlen = (int64_t)strlen(expr->lit_str);
        qdim = hex_dim(qlen);
        if (qdim == 0)
            vectra_error("embedding distance: malformed query vector");
        qvec = (float *)malloc((size_t)qdim * sizeof(float));
        if (!qvec) vectra_error("alloc failed for query vector");
        if (decode_hex_floats(expr->lit_str, qdim, qvec) != 0)
            vectra_error("embedding distance: bad hex digit in query vector");
    } else {
        vectra_error("embedding distance: missing query vector or column");
    }

    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    *out = vec_array_alloc(VEC_DOUBLE, n);

    char fn = expr->vec_fn;
    int do_par = (n > VEC_PAR_THRESHOLD);

#ifdef _OPENMP
    #pragma omp parallel if(do_par)
#endif
    {
        float *ta = NULL, *tb = NULL;
        int64_t capa = 0, capb = 0;
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, VEC_CHUNK)
#endif
        for (int64_t i = 0; i < n; i++) {
            if (!vec_array_is_valid(a, i)) { vec_array_set_null(out, i); continue; }

            int64_t da = hex_dim(str_len(a, i));
            if (da == 0) { vec_array_set_null(out, i); continue; }
            ensure_cap(&ta, &capa, da);
            if (decode_hex_floats(str_ptr(a, i), da, ta) != 0) {
                vec_array_set_null(out, i); continue;
            }

            const float *bp;
            int64_t db;
            if (b) {
                if (!vec_array_is_valid(b, i)) { vec_array_set_null(out, i); continue; }
                db = hex_dim(str_len(b, i));
                if (db == 0) { vec_array_set_null(out, i); continue; }
                ensure_cap(&tb, &capb, db);
                if (decode_hex_floats(str_ptr(b, i), db, tb) != 0) {
                    vec_array_set_null(out, i); continue;
                }
                bp = tb;
            } else {
                bp = qvec;
                db = qdim;
            }

            if (da != db) { vec_array_set_null(out, i); continue; }

            double d = vec_distance(fn, ta, bp, da);
            if (isnan(d)) { vec_array_set_null(out, i); continue; }
            out->buf.dbl[i] = d;
            vec_array_set_valid(out, i);
        }
        free(ta);
        free(tb);
    }

    free(qvec);
    vec_array_free(a); free(a);
    if (b) { vec_array_free(b); free(b); }
    return out;
}
