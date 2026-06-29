/* GEOS-native vector overlay.
 *
 * Geometry operations run on the GEOS C API supplied by the libgeos package,
 * resolved at load time through R_GetCCallable (see libgeos.c). vectra owns no
 * GEOS source and links no system library: the GEOS binary lives in libgeos.
 *
 * Self-overlay (QGIS-style "Union (single layer)") splits a polygon layer into
 * disjoint pieces, each labelled with the inputs that cover it. The arrangement
 * is built per job by noding the boundary linework once and polygonising it into
 * faces, then locating each face against the covering inputs. Noding is a single
 * pass per job, so cost tracks the number of faces, not the overlap multiplicity.
 *
 * Two entry points let the R driver stream and bound memory:
 *   C_overlay_partition -- parse each feature ONCE (in parallel): repair, snap to
 *                          the precision grid, record its bounding box, and return
 *                          the cleaned WKB. Also groups features into connected
 *                          components from the boxes. The driver makes each
 *                          component an overlay job and tiles only the few that
 *                          are too large for the memory budget.
 *   C_overlay_run       -- overlay one batch of jobs, one OpenMP thread per job,
 *                          each clipping the (already cleaned) inputs to its tile
 *                          rectangle -- no repeated repair or snapping. The caller
 *                          flushes each batch, so peak memory is one batch, not
 *                          the whole result.
 */

#include <R.h>
#include <Rinternals.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "libgeos.h"
#include "vtr_geos.h"

#include "vec_omp.h"

#define overlay_geos_init      vtr_geos_ensure_api
#define overlay_error_handler  vtr_geos_quiet_handler

/* ---- areal normalisation ------------------------------------------------- */

static double areal_area(GEOSContextHandle_t ctx, const GEOSGeometry *g) {
    double a = 0.0;
    if (g == NULL) return 0.0;
    if (!GEOSArea_r(ctx, g, &a)) return 0.0;
    return a;
}

static GEOSGeometry *areal_only(GEOSContextHandle_t ctx, const GEOSGeometry *g) {
    if (g == NULL) return NULL;
    int t = GEOSGeomTypeId_r(ctx, g);
    if (t == GEOS_POLYGON || t == GEOS_MULTIPOLYGON) {
        if (GEOSisEmpty_r(ctx, g)) return NULL;
        return GEOSGeom_clone_r(ctx, g);
    }
    if (t != GEOS_GEOMETRYCOLLECTION) return NULL;
    int ng = GEOSGetNumGeometries_r(ctx, g);
    if (ng <= 0) return NULL;
    GEOSGeometry **parts = (GEOSGeometry **) malloc((size_t) ng * sizeof(GEOSGeometry *));
    if (parts == NULL) return NULL;
    int np = 0;
    for (int k = 0; k < ng; k++) {
        const GEOSGeometry *sub = GEOSGetGeometryN_r(ctx, g, k);
        int st = GEOSGeomTypeId_r(ctx, sub);
        if ((st == GEOS_POLYGON || st == GEOS_MULTIPOLYGON) && !GEOSisEmpty_r(ctx, sub))
            parts[np++] = GEOSGeom_clone_r(ctx, sub);
    }
    if (np == 0) { free(parts); return NULL; }
    GEOSGeometry *coll = GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, parts, np);
    if (coll == NULL) {
        for (int k = 0; k < np; k++) GEOSGeom_destroy_r(ctx, parts[k]);
        free(parts); return NULL;
    }
    free(parts);
    GEOSGeometry *out = GEOSUnaryUnion_r(ctx, coll);
    GEOSGeom_destroy_r(ctx, coll);
    return out;
}

/* ---- small dynamic int vector -------------------------------------------- */

typedef struct { int *idx; int n, cap; } IntVec;
static void iv_init(IntVec *v) { v->idx = NULL; v->n = 0; v->cap = 0; }
static void iv_push(IntVec *v, int x) {
    if (v->n == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 16;
        v->idx = (int *) realloc(v->idx, (size_t) v->cap * sizeof(int));
        if (v->idx == NULL) error("vectra overlay: out of memory");
    }
    v->idx[v->n++] = x;
}
static void strtree_cb(void *item, void *userdata) {
    iv_push((IntVec *) userdata, *(int *) item);
}

/* ---- union-find ---------------------------------------------------------- */

static int uf_find(int *p, int i) { while (p[i] != i) { p[i] = p[p[i]]; i = p[i]; } return i; }
static void uf_union(int *p, int a, int b) {
    int ra = uf_find(p, a), rb = uf_find(p, b);
    if (ra != rb) p[rb] = ra;
}

/* ---- output accumulation (per worker) ------------------------------------ */

/* One output row = one disjoint piece (face) clipped to one covering input.
 * `face` ties the rows that came from the same polygonised face together, so the
 * caller can rebuild the piece-id the overlaps are grouped by. */
typedef struct { char *hex; int origin; double area; int face; } OutPiece;
typedef struct { OutPiece *a; size_t n, cap; } OutList;

static void ol_init(OutList *o) { o->a = NULL; o->n = 0; o->cap = 0; }
static void ol_push(OutList *o, char *hex, int origin, double area, int face) {
    if (o->n == o->cap) {
        o->cap = o->cap ? o->cap * 2 : 256;
        o->a = (OutPiece *) realloc(o->a, o->cap * sizeof(OutPiece));
        if (o->a == NULL) error("vectra overlay: out of memory");
    }
    o->a[o->n].hex = hex; o->a[o->n].origin = origin;
    o->a[o->n].area = area; o->a[o->n].face = face; o->n++;
}

static void emit_piece(GEOSContextHandle_t ctx, GEOSWKBWriter *writer, OutList *out,
                       const GEOSGeometry *geom, int origin, double area, int face) {
    size_t len = 0;
    unsigned char *buf = GEOSWKBWriter_writeHEX_r(ctx, writer, geom, &len);
    if (buf == NULL) return;
    char *hex = (char *) malloc(len + 1);
    memcpy(hex, buf, len); hex[len] = '\0';
    GEOSFree_r(ctx, buf);
    ol_push(out, hex, origin, area, face);
}

/* ---- one tile ------------------------------------------------------------ */

/* Overlay already-cleaned inputs `members` (local chunk indices) clipped to
 * `rect` (NULL for none) into disjoint pieces, appending them to `out`. Inputs
 * are pre-repaired and pre-snapped (see C_overlay_partition), so this only parses,
 * keeps areal parts, and clips -- no makeValid, no setPrecision. */
static void process_tile(GEOSContextHandle_t ctx, GEOSWKBReader *reader, GEOSWKBWriter *writer,
                         const unsigned char **ptrs, const size_t *lens,
                         const int *members, int nmem, const double *rect,
                         OutList *out, double *inarea, int *gface, double prec) {
    GEOSGeometry **poly = (GEOSGeometry **) calloc((size_t) nmem, sizeof(GEOSGeometry *));
    const GEOSPreparedGeometry **prep =
        (const GEOSPreparedGeometry **) calloc((size_t) nmem, sizeof(const GEOSPreparedGeometry *));
    int *store = (int *) malloc((size_t) nmem * sizeof(int));
    if (poly == NULL || prep == NULL || store == NULL) error("vectra overlay: out of memory");

    GEOSSTRtree *tree = GEOSSTRtree_create_r(ctx, 10);
    int live = 0;
    for (int k = 0; k < nmem; k++) {
        store[k] = k;
        GEOSGeometry *g = GEOSWKBReader_read_r(ctx, reader, ptrs[members[k]], lens[members[k]]);
        int straddles = 0;
        if (g != NULL && rect != NULL) {
            /* Only a feature whose bounding box leaves the tile needs clipping; one
             * that sits wholly inside the tile is unchanged by the clip and is
             * already valid from the partition pass, so it skips the clip, the
             * validity check and the repair below -- the common case in a tile. */
            double xmin, ymin, xmax, ymax;
            straddles = !(GEOSGeom_getExtent_r(ctx, g, &xmin, &ymin, &xmax, &ymax) &&
                          xmin >= rect[0] && ymin >= rect[1] &&
                          xmax <= rect[2] && ymax <= rect[3]);
        }
        if (g != NULL && rect != NULL && straddles) {
            GEOSGeometry *gc = GEOSClipByRect_r(ctx, g, rect[0], rect[1], rect[2], rect[3]);
            GEOSGeom_destroy_r(ctx, g);
            /* GEOSClipByRect is a fast rectangle clip with no validity guarantee:
             * on a multi-ring polygon it can return an invalid geometry whose area
             * and point-in-polygon tests disagree, so the clip would be measured
             * smaller than the faces credited to it and the piece coverage no
             * longer sums to the input. Repair it before it is used as both the
             * area source and the covering geometry. */
            if (gc != NULL && !GEOSisValid_r(ctx, gc)) {
                GEOSGeometry *gv = GEOSMakeValid_r(ctx, gc);
                GEOSGeom_destroy_r(ctx, gc);
                gc = gv;
            }
            g = (gc != NULL) ? areal_only(ctx, gc) : NULL;
            if (gc != NULL) GEOSGeom_destroy_r(ctx, gc);
        } else if (g != NULL) {
            GEOSGeometry *ga = areal_only(ctx, g);
            GEOSGeom_destroy_r(ctx, g);
            g = ga;
        }
        poly[k] = g;
        inarea[members[k]] = areal_area(ctx, g);
        if (g != NULL) {
            prep[k] = GEOSPrepare_r(ctx, g);
            GEOSSTRtree_insert_r(ctx, tree, g, &store[k]);
            live++;
        }
    }

    if (live == 1) {
        for (int k = 0; k < nmem; k++) if (poly[k] != NULL) {
            int fid;
#ifdef _OPENMP
            #pragma omp atomic capture
            { fid = *gface; *gface += 1; }
#else
            fid = (*gface)++;
#endif
            emit_piece(ctx, writer, out, poly[k], members[k], inarea[members[k]], fid);
            break;
        }
    } else if (live > 1) {
        GEOSGeometry **bnds = (GEOSGeometry **) malloc((size_t) nmem * sizeof(GEOSGeometry *));
        int nb = 0;
        for (int k = 0; k < nmem; k++) {
            if (poly[k] == NULL) continue;
            GEOSGeometry *b = GEOSBoundary_r(ctx, poly[k]);
            if (b != NULL) bnds[nb++] = b;
        }
        GEOSGeometry *coll = (nb > 0)
            ? GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, bnds, nb) : NULL;
        free(bnds);
        GEOSGeometry *noded = (coll == NULL) ? NULL
            : (prec > 0.0 ? GEOSUnaryUnionPrec_r(ctx, coll, prec) : GEOSUnaryUnion_r(ctx, coll));
        if (coll != NULL) GEOSGeom_destroy_r(ctx, coll);

        if (noded != NULL) {
            const GEOSGeometry *lines[1] = { noded };
            GEOSGeometry *faces = GEOSPolygonize_r(ctx, lines, 1);
            GEOSGeom_destroy_r(ctx, noded);
            if (faces != NULL) {
                IntVec cand; iv_init(&cand);
                int nf = GEOSGetNumGeometries_r(ctx, faces);
                for (int f = 0; f < nf; f++) {
                    const GEOSGeometry *face = GEOSGetGeometryN_r(ctx, faces, f);
                    if (GEOSisEmpty_r(ctx, face)) continue;
                    double fa = areal_area(ctx, face);
                    if (fa <= 0.0) continue;
                    /* A face produced by noding the snapped boundaries can straddle
                     * an input edge at coarse precision, so the piece a covering
                     * input gets is the face clipped to that input -- credited the
                     * intersection area, not the whole face. The common case (face
                     * wholly inside the input) skips the clip. This keeps each
                     * input's pieces summing to its area regardless of the grid. */
                    cand.n = 0;
                    GEOSSTRtree_query_r(ctx, tree, face, strtree_cb, &cand);
                    int fid = -1;
                    for (int c = 0; c < cand.n; c++) {
                        int k = cand.idx[c];
                        if (poly[k] == NULL) continue;
                        if (!GEOSPreparedIntersects_r(ctx, prep[k], face)) continue;
                        const GEOSGeometry *piece = NULL;
                        GEOSGeometry *clip = NULL;
                        double a;
                        if (GEOSPreparedContains_r(ctx, prep[k], face)) {
                            piece = face; a = fa;
                        } else {
                            GEOSGeometry *inter = GEOSIntersection_r(ctx, face, poly[k]);
                            clip = (inter != NULL) ? areal_only(ctx, inter) : NULL;
                            if (inter != NULL) GEOSGeom_destroy_r(ctx, inter);
                            if (clip == NULL) continue;
                            a = areal_area(ctx, clip);
                            if (a <= 1e-9 * fa) { GEOSGeom_destroy_r(ctx, clip); continue; }
                            piece = clip;
                        }
                        if (fid < 0) {
#ifdef _OPENMP
                            #pragma omp atomic capture
                            { fid = *gface; *gface += 1; }
#else
                            fid = (*gface)++;
#endif
                        }
                        emit_piece(ctx, writer, out, piece, members[k], a, fid);
                        if (clip != NULL) GEOSGeom_destroy_r(ctx, clip);
                    }
                }
                free(cand.idx);
                GEOSGeom_destroy_r(ctx, faces);
            }
        }
    }

    for (int k = 0; k < nmem; k++) {
        if (prep[k] != NULL) GEOSPreparedGeom_destroy_r(ctx, prep[k]);
        if (poly[k] != NULL) GEOSGeom_destroy_r(ctx, poly[k]);
    }
    GEOSSTRtree_destroy_r(ctx, tree);
    free(poly); free((void *) prep); free(store);
}

/* ---- partition: clean + bbox + components -------------------------------- */

/* C_overlay_parse(wkb_list, grid, n_threads) -> VECSXP(2):
 *   [[1]] REALSXP matrix n x 4 (xmin, ymin, xmax, ymax), NA row on parse failure
 *   [[2]] VECSXP  cleaned WKB (raw) per feature: repaired, areal, snapped to grid
 * Each feature is parsed once, in parallel. Connected components are derived
 * separately (C_overlay_components) from the boxes alone, so the driver can parse
 * the layer in chunks -- bounding the transient raw-WKB copy to one chunk -- and
 * still label components globally. */
SEXP C_overlay_parse(SEXP wkb_list, SEXP grid_sexp, SEXP nthreads_sexp) {
    overlay_geos_init();
    int n = (int) Rf_length(wkb_list);
    double grid = (Rf_length(grid_sexp) > 0) ? REAL(grid_sexp)[0] : 0.0;
    int nthreads = (Rf_length(nthreads_sexp) > 0) ? INTEGER(nthreads_sexp)[0] : 0;

    const unsigned char **ptrs =
        (const unsigned char **) R_alloc((size_t) n, sizeof(const unsigned char *));
    size_t *lens = (size_t *) R_alloc((size_t) n, sizeof(size_t));
    for (int i = 0; i < n; i++) {
        SEXP raw = VECTOR_ELT(wkb_list, i);
        ptrs[i] = (const unsigned char *) RAW(raw);
        lens[i] = (size_t) Rf_length(raw);
    }

    unsigned char **cbuf = (unsigned char **) R_Calloc((size_t) n, unsigned char *);
    size_t *clen = (size_t *) R_Calloc((size_t) n, size_t);
    SEXP bbox = PROTECT(allocMatrix(REALSXP, n, 4));
    double *bb = REAL(bbox);
    for (int i = 0; i < n; i++) { bb[i] = bb[i+n] = bb[i+2*n] = bb[i+3*n] = NA_REAL; }

#ifdef _OPENMP
    {   /* clamp to the team cap so R CMD check's two-core limit reaches the
           explicit num_threads() below */
        int cap = omp_get_max_threads();
        if (nthreads <= 0 || nthreads > cap) nthreads = cap;
    }
#else
    nthreads = 1;
#endif

    /* parallel: parse -> make valid -> areal -> snap; record bbox + cleaned WKB */
#ifdef _OPENMP
    #pragma omp parallel num_threads(nthreads)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, overlay_error_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        GEOSWKBWriter *writer = GEOSWKBWriter_create_r(ctx);
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, 256)
#endif
        for (int i = 0; i < n; i++) {
            GEOSGeometry *g0 = GEOSWKBReader_read_r(ctx, reader, ptrs[i], lens[i]);
            if (g0 == NULL) continue;
            GEOSGeometry *gv = GEOSMakeValid_r(ctx, g0);
            GEOSGeom_destroy_r(ctx, g0);
            if (gv == NULL) continue;
            GEOSGeometry *g = areal_only(ctx, gv);
            GEOSGeom_destroy_r(ctx, gv);
            if (g != NULL && grid > 0.0) {
                GEOSGeometry *gs = GEOSGeom_setPrecision_r(ctx, g, grid, 0);
                GEOSGeom_destroy_r(ctx, g);
                g = (gs != NULL) ? areal_only(ctx, gs) : NULL;
                if (gs != NULL) GEOSGeom_destroy_r(ctx, gs);
            }
            if (g == NULL) continue;
            double xmin, ymin, xmax, ymax;
            if (GEOSGeom_getExtent_r(ctx, g, &xmin, &ymin, &xmax, &ymax)) {
                bb[i] = xmin; bb[i+n] = ymin; bb[i+2*n] = xmax; bb[i+3*n] = ymax;
            }
            size_t len = 0;
            unsigned char *buf = GEOSWKBWriter_write_r(ctx, writer, g, &len);
            if (buf != NULL) {
                cbuf[i] = (unsigned char *) malloc(len);
                if (cbuf[i] != NULL) { memcpy(cbuf[i], buf, len); clen[i] = len; }
                GEOSFree_r(ctx, buf);
            }
            GEOSGeom_destroy_r(ctx, g);
        }
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOSWKBWriter_destroy_r(ctx, writer);
        GEOS_finish_r(ctx);
    }

    /* serial: move cleaned WKB into an R list, freeing each C buffer as we go */
    SEXP clean = PROTECT(allocVector(VECSXP, n));
    for (int i = 0; i < n; i++) {
        if (cbuf[i] != NULL) {
            SEXP r = allocVector(RAWSXP, (R_xlen_t) clen[i]);
            memcpy(RAW(r), cbuf[i], clen[i]);
            SET_VECTOR_ELT(clean, i, r);
            free(cbuf[i]);
        } else {
            SET_VECTOR_ELT(clean, i, allocVector(RAWSXP, 0));
        }
    }
    R_Free(clen); R_Free(cbuf);

    SEXP out = PROTECT(allocVector(VECSXP, 2));
    SET_VECTOR_ELT(out, 0, bbox);
    SET_VECTOR_ELT(out, 1, clean);
    UNPROTECT(3);
    return out;
}

/* C_overlay_components(bbox) -> INTSXP length n: dense 1-based connected-component
 * id per feature, from bounding-box overlap (a valid superset of true overlap).
 * bbox is the n x 4 matrix from C_overlay_parse, with the chunk rows recombined
 * in the original feature order. */
SEXP C_overlay_components(SEXP bbox_sexp) {
    overlay_geos_init();
    int n = (int) Rf_nrows(bbox_sexp);
    double *bb = REAL(bbox_sexp);

    GEOSContextHandle_t ctx = GEOS_init_r();
    GEOSContext_setErrorMessageHandler_r(ctx, overlay_error_handler, NULL);
    GEOSGeometry **rect = (GEOSGeometry **) R_Calloc((size_t) n, GEOSGeometry *);
    int *store = (int *) R_Calloc((size_t) n, int);
    GEOSSTRtree *tree = GEOSSTRtree_create_r(ctx, 10);
    for (int i = 0; i < n; i++) {
        store[i] = i;
        if (!ISNA(bb[i])) {
            rect[i] = GEOSGeom_createRectangle_r(ctx, bb[i], bb[i+n], bb[i+2*n], bb[i+3*n]);
            if (rect[i] != NULL) GEOSSTRtree_insert_r(ctx, tree, rect[i], &store[i]);
        }
    }

    int *parent = (int *) R_Calloc((size_t) n, int);
    for (int i = 0; i < n; i++) parent[i] = i;
    IntVec cand; iv_init(&cand);
    for (int i = 0; i < n; i++) {
        if (rect[i] == NULL) continue;
        cand.n = 0;
        GEOSSTRtree_query_r(ctx, tree, rect[i], strtree_cb, &cand);
        for (int c = 0; c < cand.n; c++)
            if (cand.idx[c] > i) uf_union(parent, i, cand.idx[c]);
        if ((i & 4095) == 0) R_CheckUserInterrupt();
    }
    free(cand.idx);

    SEXP comp = PROTECT(allocVector(INTSXP, n));
    int *cp = INTEGER(comp);
    int *remap = (int *) R_Calloc((size_t) n, int);
    int next = 0;
    for (int i = 0; i < n; i++) {
        int r = uf_find(parent, i);
        if (remap[r] == 0) remap[r] = ++next;
        cp[i] = remap[r];
    }

    for (int i = 0; i < n; i++) if (rect[i] != NULL) GEOSGeom_destroy_r(ctx, rect[i]);
    GEOSSTRtree_destroy_r(ctx, tree);
    GEOS_finish_r(ctx);
    R_Free(remap); R_Free(parent); R_Free(store); R_Free(rect);

    UNPROTECT(1);
    return comp;
}

/* C_overlay_group(wkb_list) -> INTSXP length n: a dense 1-based group id per
 * feature, equal for features whose cleaned WKB is byte-identical. Precision
 * snapping in C_overlay_partition makes the many designation records stacked
 * over one site identical down to the byte, so grouping here lets the driver
 * run the overlay on one representative per group and fan the per-record
 * attributes back afterwards -- the duplicates add no faces, so the pieces are
 * unchanged. The group id of a run of identical features is the id assigned to
 * the first of them, so the first feature in each group is its representative. */
SEXP C_overlay_group(SEXP wkb_list) {
    R_xlen_t n = XLENGTH(wkb_list);
    SEXP grp = PROTECT(allocVector(INTSXP, n));
    int *g = INTEGER(grp);
    size_t cap = 1;
    while (cap < (size_t) (2 * n + 1)) cap <<= 1;
    size_t mask = cap - 1;
    R_xlen_t *slot = (R_xlen_t *) R_Calloc(cap, R_xlen_t);  /* stores i+1; 0 = empty */
    int ngroup = 0;
    for (R_xlen_t i = 0; i < n; i++) {
        SEXP r = VECTOR_ELT(wkb_list, i);
        const unsigned char *p = RAW(r);
        R_xlen_t len = XLENGTH(r);
        uint64_t h = 1469598103934665603ULL;            /* FNV-1a 64-bit */
        for (R_xlen_t b = 0; b < len; b++) { h ^= p[b]; h *= 1099511628211ULL; }
        size_t pos = (size_t) h & mask;
        for (;;) {
            R_xlen_t s = slot[pos];
            if (s == 0) { slot[pos] = i + 1; g[i] = ++ngroup; break; }
            R_xlen_t j = s - 1;
            SEXP rj = VECTOR_ELT(wkb_list, j);
            if (XLENGTH(rj) == len && memcmp(RAW(rj), p, (size_t) len) == 0) {
                g[i] = g[j]; break;
            }
            pos = (pos + 1) & mask;
        }
    }
    R_Free(slot);
    UNPROTECT(1);
    return grp;
}

/* ---- run one batch of jobs ----------------------------------------------- */

/* C_overlay_run(wkb_chunk, job_chunk, rects, n_threads)
 *   wkb_chunk : VECSXP of RAWSXP cleaned WKB (a feature may repeat across tiles)
 *   job_chunk : INTSXP job id per chunk input (1..njobs, dense)
 *   rects     : REALSXP length 4*njobs (xmin,ymin,xmax,ymax per job); NA xmin
 *               means the job is not clipped (a whole small component)
 *   n_threads : INTSXP(1) OpenMP threads (<=0 -> all cores)
 *   prec      : REALSXP(1) noding grid size; >0 nodes at fixed precision
 *               (snap-rounding on that grid), 0 nodes in floating precision
 * returns VECSXP(5): hex-WKB pieces (one per face x covering input), INTSXP
 *                    origin (1-based chunk index of the covering input), piece
 *                    areas, input areas (per chunk input, clipped), INTSXP face
 *                    (1-based id shared by the rows from one polygonised face). */
SEXP C_overlay_run(SEXP wkb_chunk, SEXP job_chunk, SEXP rects_sexp, SEXP nthreads_sexp,
                   SEXP prec_sexp) {
    overlay_geos_init();
    int m = (int) Rf_length(wkb_chunk);
    int nthreads = (Rf_length(nthreads_sexp) > 0) ? INTEGER(nthreads_sexp)[0] : 0;
    int have_rects = (rects_sexp != R_NilValue && Rf_length(rects_sexp) >= 4);
    const double *rects = have_rects ? REAL(rects_sexp) : NULL;
    double prec = (Rf_length(prec_sexp) > 0) ? REAL(prec_sexp)[0] : 0.0;

    const unsigned char **ptrs =
        (const unsigned char **) R_alloc((size_t) m, sizeof(const unsigned char *));
    size_t *lens = (size_t *) R_alloc((size_t) m, sizeof(size_t));
    for (int i = 0; i < m; i++) {
        SEXP raw = VECTOR_ELT(wkb_chunk, i);
        ptrs[i] = (const unsigned char *) RAW(raw);
        lens[i] = (size_t) Rf_length(raw);
    }

    const int *job = INTEGER(job_chunk);
    int njobs = 0;
    for (int i = 0; i < m; i++) if (job[i] > njobs) njobs = job[i];
    int *jsize = (int *) R_Calloc((size_t) (njobs > 0 ? njobs : 1), int);
    for (int i = 0; i < m; i++) jsize[job[i] - 1]++;
    int **jmemb = (int **) R_alloc((size_t) (njobs > 0 ? njobs : 1), sizeof(int *));
    for (int j = 0; j < njobs; j++) jmemb[j] = (int *) R_alloc((size_t) jsize[j], sizeof(int));
    int *jfill = (int *) R_Calloc((size_t) (njobs > 0 ? njobs : 1), int);
    for (int i = 0; i < m; i++) { int j = job[i] - 1; jmemb[j][jfill[j]++] = i; }

    double *inarea = (double *) R_Calloc((size_t) m, double);

#ifdef _OPENMP
    {   /* clamp to the team cap (R CMD check two-core limit), then to job count */
        int cap = omp_get_max_threads();
        if (nthreads <= 0 || nthreads > cap) nthreads = cap;
    }
    if (nthreads > njobs) nthreads = njobs > 0 ? njobs : 1;
#else
    nthreads = 1;
#endif
    int nw = nthreads > 0 ? nthreads : 1;
    OutList *worker = (OutList *) R_alloc((size_t) nw, sizeof(OutList));
    for (int t = 0; t < nw; t++) ol_init(&worker[t]);
    int g_face = 0;   /* dense piece-id source, shared across jobs/threads */

#ifdef _OPENMP
    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, overlay_error_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        GEOSWKBWriter *writer = GEOSWKBWriter_create_r(ctx);
        #pragma omp for schedule(dynamic, 1)
        for (int j = 0; j < njobs; j++) {
            const double *rect = NULL;
            if (have_rects && !ISNA(rects[4 * j])) rect = &rects[4 * j];
            process_tile(ctx, reader, writer, ptrs, lens, jmemb[j], jsize[j],
                         rect, &worker[tid], inarea, &g_face, prec);
        }
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOSWKBWriter_destroy_r(ctx, writer);
        GEOS_finish_r(ctx);
    }
#else
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, overlay_error_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        GEOSWKBWriter *writer = GEOSWKBWriter_create_r(ctx);
        for (int j = 0; j < njobs; j++) {
            const double *rect = NULL;
            if (have_rects && !ISNA(rects[4 * j])) rect = &rects[4 * j];
            process_tile(ctx, reader, writer, ptrs, lens, jmemb[j], jsize[j],
                         rect, &worker[0], inarea, &g_face, prec);
        }
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOSWKBWriter_destroy_r(ctx, writer);
        GEOS_finish_r(ctx);
    }
#endif

    size_t total = 0;
    for (int t = 0; t < nw; t++) total += worker[t].n;
    SEXP geoms  = PROTECT(allocVector(STRSXP,  (R_xlen_t) total));
    SEXP origin = PROTECT(allocVector(INTSXP,  (R_xlen_t) total));
    SEXP parea  = PROTECT(allocVector(REALSXP, (R_xlen_t) total));
    SEXP face   = PROTECT(allocVector(INTSXP,  (R_xlen_t) total));
    int *op = INTEGER(origin); double *pa = REAL(parea); int *fp = INTEGER(face);
    size_t w = 0;
    for (int t = 0; t < nw; t++) {
        OutList *ol = &worker[t];
        for (size_t k = 0; k < ol->n; k++) {
            SET_STRING_ELT(geoms, (R_xlen_t) w, mkChar(ol->a[k].hex));
            op[w] = ol->a[k].origin + 1;     /* 1-based chunk input index */
            pa[w] = ol->a[k].area;
            fp[w] = ol->a[k].face + 1;       /* 1-based piece id within this call */
            free(ol->a[k].hex);
            w++;
        }
        free(ol->a);
    }

    SEXP iarea = PROTECT(allocVector(REALSXP, m));
    memcpy(REAL(iarea), inarea, (size_t) m * sizeof(double));
    R_Free(inarea); R_Free(jfill); R_Free(jsize);

    SEXP res = PROTECT(allocVector(VECSXP, 5));
    SET_VECTOR_ELT(res, 0, geoms);
    SET_VECTOR_ELT(res, 1, origin);
    SET_VECTOR_ELT(res, 2, parea);
    SET_VECTOR_ELT(res, 3, iarea);
    SET_VECTOR_ELT(res, 4, face);
    UNPROTECT(6);
    return res;
}

SEXP C_geos_version(void) {
    overlay_geos_init();
    return mkString(GEOSversion());
}
