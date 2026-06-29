/* Scalar geometry expressions: libgeos ops inside mutate()/filter()/summarise().
 *
 * Geometry rides through the engine as hex-WKB in an ordinary string column
 * (the same representation the streaming spatial verbs use). These expressions
 * decode that column with GEOS one row at a time and compute a measure, a
 * predicate, or a derived geometry straight off it -- no per-batch round-trip
 * through sf. They slot into the expression evaluator exactly like the string
 * ops: vec_expr_eval dispatches EXPR_GEOM here, the geom_fn discriminator picks
 * the operation, and the result is a plain VecArray (double, bool, or hex-WKB
 * string) that flows on through the rest of the pipeline.
 *
 *   measures (geom -> double)   area, length/perimeter, x, y, n_points,
 *                               n_geometries, distance(a, b)
 *   predicates (geom -> bool)   is_valid, is_empty, is_simple, and the binary
 *                               topological relations intersects/within/...
 *   transforms (geom -> geom)   centroid, point_on_surface, boundary, envelope,
 *                               convex_hull, make_valid, buffer(d), simplify(t)
 *   type (geom -> string)       geometry_type
 *
 * The second geometry of a binary op (distance / predicate) is `right`: either
 * another geometry column, or a constant geometry serialized to a hex-WKB string
 * literal on the R side. A constant is decoded once and shared read-only across
 * threads (geometries are not bound to a context for read-only queries, the same
 * sharing the spatial verbs rely on); a column is decoded per row.
 *
 * Threads each take their own GEOS context, reader, and writer. Output rows are
 * written to disjoint indices, and the dynamic chunk of 256 keeps each thread's
 * validity bytes (8 rows each) disjoint too, so the writes are race-free.
 */

#include "expr.h"
#include "array.h"
#include "coerce.h"
#include "error.h"
#include "vec_omp.h"
#include "libgeos.h"
#include "vtr_geos.h"
#include <stdlib.h>
#include <string.h>

/* Below this row count the per-thread GEOS context setup is not worth it. */
#define GEOM_PAR_THRESHOLD 128
/* Chunk is a multiple of 8 so per-thread validity bytes never overlap. */
#define GEOM_CHUNK 256

typedef enum { GC_DOUBLE, GC_BOOL, GC_STRING, GC_GEOM } GeomCat;

static GeomCat geom_category(char fn) {
    switch (fn) {
    case 'A': case 'L': case 'X': case 'Y': case 'n': case 'g': case 'D':
        return GC_DOUBLE;
    case 'v': case 'm': case 's':
    case 'i': case 'w': case 'C': case 'O': case 'T': case 'R':
    case 'Q': case 'J': case 'K': case 'V':
        return GC_BOOL;
    case 't':
        return GC_STRING;
    default:  /* c o b e h M B S -> derived geometry */
        return GC_GEOM;
    }
}

VecType vec_expr_geom_result_type(char geom_fn) {
    switch (geom_category(geom_fn)) {
    case GC_DOUBLE: return VEC_DOUBLE;
    case GC_BOOL:   return VEC_BOOL;
    default:        return VEC_STRING; /* GC_STRING and GC_GEOM */
    }
}

static int geom_is_binary(char fn) {
    switch (fn) {
    case 'D': case 'i': case 'w': case 'C': case 'O': case 'T':
    case 'R': case 'Q': case 'J': case 'K': case 'V': return 1;
    default: return 0;
    }
}

static int geom_is_param(char fn) { return fn == 'B' || fn == 'S'; }

/* Unary measure: 1 on success (writes *out), 0 on failure/NA. */
static int geom_measure(GEOSContextHandle_t ctx, char fn,
                        const GEOSGeometry *g, double *out) {
    switch (fn) {
    case 'A': return GEOSArea_r(ctx, g, out);
    case 'L': return GEOSLength_r(ctx, g, out);
    case 'X': return GEOSGeomGetX_r(ctx, g, out);
    case 'Y': return GEOSGeomGetY_r(ctx, g, out);
    case 'n': { int v = GEOSGetNumCoordinates_r(ctx, g);
                if (v < 0) return 0; *out = (double) v; return 1; }
    case 'g': { int v = GEOSGetNumGeometries_r(ctx, g);
                if (v < 0) return 0; *out = (double) v; return 1; }
    default:  return 0;
    }
}

/* Unary predicate: 1 true, 0 false, 2 exception (-> NA). */
static char geom_unary_pred(GEOSContextHandle_t ctx, char fn,
                            const GEOSGeometry *g) {
    switch (fn) {
    case 'v': return GEOSisValid_r(ctx, g);
    case 'm': return GEOSisEmpty_r(ctx, g);
    case 's': return GEOSisSimple_r(ctx, g);
    default:  return 2;
    }
}

/* Binary predicate: 1 true, 0 false, 2 exception (-> NA). */
static char geom_binary_pred(GEOSContextHandle_t ctx, char fn,
                             const GEOSGeometry *a, const GEOSGeometry *b) {
    switch (fn) {
    case 'i': return GEOSIntersects_r(ctx, a, b);
    case 'w': return GEOSWithin_r(ctx, a, b);
    case 'C': return GEOSContains_r(ctx, a, b);
    case 'O': return GEOSOverlaps_r(ctx, a, b);
    case 'T': return GEOSTouches_r(ctx, a, b);
    case 'R': return GEOSCrosses_r(ctx, a, b);
    case 'Q': return GEOSEquals_r(ctx, a, b);
    case 'J': return GEOSDisjoint_r(ctx, a, b);
    case 'K': return GEOSCovers_r(ctx, a, b);
    case 'V': return GEOSCoveredBy_r(ctx, a, b);
    default:  return 2;
    }
}

/* Unary or parameterized transform returning a new geometry (caller destroys). */
static GEOSGeometry *geom_transform(GEOSContextHandle_t ctx, char fn,
                                    const GEOSGeometry *g, double param) {
    switch (fn) {
    case 'c': return GEOSGetCentroid_r(ctx, g);
    case 'o': return GEOSPointOnSurface_r(ctx, g);
    case 'b': return GEOSBoundary_r(ctx, g);
    case 'e': return GEOSEnvelope_r(ctx, g);
    case 'h': return GEOSConvexHull_r(ctx, g);
    case 'M': return GEOSMakeValid_r(ctx, g);
    case 'B': return GEOSBuffer_r(ctx, g, param, 8);
    case 'S': return GEOSTopologyPreserveSimplify_r(ctx, g, param);
    default:  return NULL;
    }
}

/* Hex-WKB bytes of row i in a string array (NULL when the row is NA/empty). */
static const unsigned char *hex_at(const VecArray *a, int64_t i, size_t *len) {
    if (!vec_array_is_valid(a, i)) { *len = 0; return NULL; }
    int64_t s = a->buf.str.offsets[i];
    int64_t e = a->buf.str.offsets[i + 1];
    *len = (size_t) (e - s);
    return (const unsigned char *) (a->buf.str.data + s);
}

VecArray *vec_expr_eval_geom(const VecExpr *expr, const VecBatch *batch) {
    vtr_geos_ensure_api();
    char fn = expr->geom_fn;
    GeomCat cat = geom_category(fn);
    int is_binary = geom_is_binary(fn);
    int is_param  = geom_is_param(fn);

    VecArray *g = vec_expr_eval(expr->operand, batch);
    if (g->type != VEC_STRING)
        vectra_error("geometry function expects a hex-WKB geometry column");
    int64_t n = g->length;

    /* Second operand: a geometry (binary) or a numeric parameter (transform). */
    VecArray *rarr = NULL;     /* binary: right geometry column / literal       */
    VecArray *parr = NULL;     /* param:  buffer distance / simplify tolerance   */
    int r_is_const = 0;
    if (is_binary) {
        if (!expr->right) vectra_error("geometry op missing second geometry");
        rarr = vec_expr_eval(expr->right, batch);
        if (rarr->type != VEC_STRING)
            vectra_error("second geometry argument must be hex-WKB");
        r_is_const = (expr->right->kind == EXPR_LIT_STRING);
    } else if (is_param) {
        if (!expr->right) vectra_error("buffer/simplify missing distance argument");
        VecArray *p = vec_expr_eval(expr->right, batch);
        parr = (p->type == VEC_DOUBLE) ? p : vec_coerce(p, VEC_DOUBLE);
        if (parr != p) { vec_array_free(p); free(p); }
    }

    /* A constant second geometry is parsed once and shared read-only. */
    GEOSContextHandle_t cctx = NULL;
    GEOSGeometry *cgeom = NULL;
    if (is_binary && r_is_const) {
        cctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(cctx, vtr_geos_quiet_handler, NULL);
        GEOSWKBReader *rd = GEOSWKBReader_create_r(cctx);
        size_t hl; const unsigned char *hx = (n > 0) ? hex_at(rarr, 0, &hl) : NULL;
        if (hx) cgeom = GEOSWKBReader_readHEX_r(cctx, rd, hx, hl);
        GEOSWKBReader_destroy_r(cctx, rd);
    }

    /* Numeric / boolean outputs land straight in the result array; string and
       geometry outputs collect per-row buffers, assembled after the loop. */
    VecArray *out = NULL;
    char **strs = NULL; int64_t *slens = NULL; unsigned char *ok = NULL;
    if (cat == GC_DOUBLE || cat == GC_BOOL) {
        out = (VecArray *) malloc(sizeof(VecArray));
        *out = vec_array_alloc(cat == GC_DOUBLE ? VEC_DOUBLE : VEC_BOOL, n);
        /* validity starts all-NA (zeroed); set_valid on success */
    } else {
        strs  = (char **)    calloc((size_t) (n > 0 ? n : 1), sizeof(char *));
        slens = (int64_t *)  calloc((size_t) (n > 0 ? n : 1), sizeof(int64_t));
        ok    = (unsigned char *) calloc((size_t) (n > 0 ? n : 1), sizeof(unsigned char));
        if (!strs || !slens || !ok) vectra_error("alloc failed in geometry op");
    }

    int do_par = (n > GEOM_PAR_THRESHOLD);
#ifdef _OPENMP
    #pragma omp parallel if(do_par)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        GEOSWKBWriter *writer = (cat == GC_GEOM) ? GEOSWKBWriter_create_r(ctx) : NULL;
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, GEOM_CHUNK)
#endif
        for (int64_t i = 0; i < n; i++) {
            size_t hl; const unsigned char *hx = hex_at(g, i, &hl);
            if (!hx) continue;  /* NA geometry -> NA output (already zeroed) */
            GEOSGeometry *xg = GEOSWKBReader_readHEX_r(ctx, reader, hx, hl);
            if (!xg) continue;

            if (cat == GC_DOUBLE && fn == 'D') {
                /* distance to the second geometry */
                GEOSGeometry *bg = cgeom; GEOSGeometry *bfree = NULL;
                if (!r_is_const) {
                    size_t bl; const unsigned char *bh = hex_at(rarr, i, &bl);
                    if (bh) { bg = bfree = GEOSWKBReader_readHEX_r(ctx, reader, bh, bl); }
                    else bg = NULL;
                }
                double d;
                if (bg && GEOSDistance_r(ctx, xg, bg, &d)) {
                    out->buf.dbl[i] = d; vec_array_set_valid(out, i);
                }
                if (bfree) GEOSGeom_destroy_r(ctx, bfree);
            } else if (cat == GC_DOUBLE) {
                double v;
                if (geom_measure(ctx, fn, xg, &v)) {
                    out->buf.dbl[i] = v; vec_array_set_valid(out, i);
                }
            } else if (cat == GC_BOOL) {
                char res;
                if (is_binary) {
                    GEOSGeometry *bg = cgeom; GEOSGeometry *bfree = NULL;
                    if (!r_is_const) {
                        size_t bl; const unsigned char *bh = hex_at(rarr, i, &bl);
                        if (bh) { bg = bfree = GEOSWKBReader_readHEX_r(ctx, reader, bh, bl); }
                        else bg = NULL;
                    }
                    res = bg ? geom_binary_pred(ctx, fn, xg, bg) : 2;
                    if (bfree) GEOSGeom_destroy_r(ctx, bfree);
                } else {
                    res = geom_unary_pred(ctx, fn, xg);
                }
                if (res == 0 || res == 1) {
                    out->buf.bln[i] = (uint8_t) res; vec_array_set_valid(out, i);
                }
            } else if (cat == GC_STRING) {
                /* geometry type name */
                char *t = GEOSGeomType_r(ctx, xg);
                if (t) {
                    int64_t tl = (int64_t) strlen(t);
                    char *s = (char *) malloc((size_t) (tl > 0 ? tl : 1));
                    if (!s) { GEOSFree_r(ctx, t); GEOSGeom_destroy_r(ctx, xg);
                              vectra_error("alloc failed in geometry op"); }
                    memcpy(s, t, (size_t) tl);
                    GEOSFree_r(ctx, t);
                    strs[i] = s; slens[i] = tl; ok[i] = 1;
                }
            } else { /* GC_GEOM: derived geometry as hex-WKB */
                double param = 0.0; int param_na = 0;
                if (is_param) {
                    if (vec_array_is_valid(parr, i)) param = parr->buf.dbl[i];
                    else param_na = 1;
                }
                GEOSGeometry *rg = param_na ? NULL : geom_transform(ctx, fn, xg, param);
                if (rg) {
                    size_t wl = 0;
                    unsigned char *buf = GEOSWKBWriter_writeHEX_r(ctx, writer, rg, &wl);
                    if (buf) {
                        char *s = (char *) malloc(wl > 0 ? wl : 1);
                        if (!s) { GEOSFree_r(ctx, buf); GEOSGeom_destroy_r(ctx, rg);
                                  GEOSGeom_destroy_r(ctx, xg);
                                  vectra_error("alloc failed in geometry op"); }
                        memcpy(s, buf, wl);
                        GEOSFree_r(ctx, buf);
                        strs[i] = s; slens[i] = (int64_t) wl; ok[i] = 1;
                    }
                    GEOSGeom_destroy_r(ctx, rg);
                }
            }
            GEOSGeom_destroy_r(ctx, xg);
        }
        if (writer) GEOSWKBWriter_destroy_r(ctx, writer);
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOS_finish_r(ctx);
    }

    if (cgeom) GEOSGeom_destroy_r(cctx, cgeom);
    if (cctx)  GEOS_finish_r(cctx);

    /* Assemble string / geometry output from the per-row buffers. */
    if (cat == GC_STRING || cat == GC_GEOM) {
        int64_t total = 0;
        for (int64_t i = 0; i < n; i++) if (ok[i]) total += slens[i];
        out = (VecArray *) malloc(sizeof(VecArray));
        *out = vec_array_alloc(VEC_STRING, n);
        free(out->buf.str.data);
        out->buf.str.data = (char *) malloc((size_t) (total > 0 ? total : 1));
        out->buf.str.data_len = total;
        int64_t off = 0;
        for (int64_t i = 0; i < n; i++) {
            out->buf.str.offsets[i] = off;
            if (ok[i]) {
                if (slens[i] > 0) memcpy(out->buf.str.data + off, strs[i], (size_t) slens[i]);
                off += slens[i];
                vec_array_set_valid(out, i);
                free(strs[i]);
            }
            /* else: validity stays 0 (NA) */
        }
        out->buf.str.offsets[n] = off;
        free(strs); free(slens); free(ok);
    }

    vec_array_free(g); free(g);
    if (rarr) { vec_array_free(rarr); free(rarr); }
    if (parr) { vec_array_free(parr); free(parr); }
    return out;
}
