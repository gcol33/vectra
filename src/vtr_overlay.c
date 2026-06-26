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
#include <string.h>
#include "libgeos.h"
#include "vtr_geos.h"

#ifdef _OPENMP
#include <omp.h>
#endif

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

typedef struct { char *hex; int *lab; int nlab; double area; } OutPiece;
typedef struct { OutPiece *a; size_t n, cap; } OutList;

static void ol_init(OutList *o) { o->a = NULL; o->n = 0; o->cap = 0; }
static void ol_push(OutList *o, char *hex, int *lab, int nlab, double area) {
    if (o->n == o->cap) {
        o->cap = o->cap ? o->cap * 2 : 256;
        o->a = (OutPiece *) realloc(o->a, o->cap * sizeof(OutPiece));
        if (o->a == NULL) error("vectra overlay: out of memory");
    }
    o->a[o->n].hex = hex; o->a[o->n].lab = lab;
    o->a[o->n].nlab = nlab; o->a[o->n].area = area; o->n++;
}

static void emit_piece(GEOSContextHandle_t ctx, GEOSWKBWriter *writer, OutList *out,
                       const GEOSGeometry *geom, const int *lab, int nlab, double area) {
    size_t len = 0;
    unsigned char *buf = GEOSWKBWriter_writeHEX_r(ctx, writer, geom, &len);
    if (buf == NULL) return;
    char *hex = (char *) malloc(len + 1);
    memcpy(hex, buf, len); hex[len] = '\0';
    GEOSFree_r(ctx, buf);
    int *labcopy = (int *) malloc((size_t) nlab * sizeof(int));
    memcpy(labcopy, lab, (size_t) nlab * sizeof(int));
    ol_push(out, hex, labcopy, nlab, area);
}

/* ---- one tile ------------------------------------------------------------ */

/* Overlay already-cleaned inputs `members` (local chunk indices) clipped to
 * `rect` (NULL for none) into disjoint pieces, appending them to `out`. Inputs
 * are pre-repaired and pre-snapped (see C_overlay_partition), so this only parses,
 * keeps areal parts, and clips -- no makeValid, no setPrecision. */
static void process_tile(GEOSContextHandle_t ctx, GEOSWKBReader *reader, GEOSWKBWriter *writer,
                         const unsigned char **ptrs, const size_t *lens,
                         const int *members, int nmem, const double *rect,
                         OutList *out, double *inarea) {
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
        if (g != NULL && rect != NULL) {
            GEOSGeometry *gc = GEOSClipByRect_r(ctx, g, rect[0], rect[1], rect[2], rect[3]);
            GEOSGeom_destroy_r(ctx, g);
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
            int one = members[k];
            emit_piece(ctx, writer, out, poly[k], &one, 1, inarea[members[k]]);
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
        GEOSGeometry *noded = (coll != NULL) ? GEOSUnaryUnion_r(ctx, coll) : NULL;
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
                    GEOSGeometry *rep = GEOSPointOnSurface_r(ctx, face);
                    if (rep == NULL) continue;
                    cand.n = 0;
                    GEOSSTRtree_query_r(ctx, tree, rep, strtree_cb, &cand);
                    int *labbuf = (int *) malloc((size_t) (cand.n > 0 ? cand.n : 1) * sizeof(int));
                    int nlab = 0;
                    for (int c = 0; c < cand.n; c++) {
                        int k = cand.idx[c];
                        if (poly[k] != NULL && GEOSPreparedIntersects_r(ctx, prep[k], rep))
                            labbuf[nlab++] = members[k];
                    }
                    GEOSGeom_destroy_r(ctx, rep);
                    if (nlab > 0)
                        emit_piece(ctx, writer, out, face, labbuf, nlab, areal_area(ctx, face));
                    free(labbuf);
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

/* C_overlay_partition(wkb_list, grid, n_threads) -> VECSXP(3):
 *   [[1]] INTSXP  dense 1-based connected-component id per feature (overlap from
 *         bounding boxes; a valid superset of true overlap)
 *   [[2]] REALSXP matrix n x 4 (xmin, ymin, xmax, ymax), NA row on parse failure
 *   [[3]] VECSXP  cleaned WKB (raw) per feature: repaired, areal, snapped to grid
 * Each feature is parsed once, in parallel; the cleaned WKB is what the overlay
 * jobs consume, so a feature spanning many tiles is never repaired or snapped
 * more than once. */
SEXP C_overlay_partition(SEXP wkb_list, SEXP grid_sexp, SEXP nthreads_sexp) {
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
    if (nthreads <= 0) nthreads = omp_get_max_threads();
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
                /* setPrecision can fold a ring onto itself and return invalid,
                 * self-overlapping geometry whose GEOSArea double-counts the
                 * overlap; re-validate so the snapped area and the noded pieces
                 * built from its boundary reconstruct each other. */
                GEOSGeometry *gv = (gs != NULL) ? GEOSMakeValid_r(ctx, gs) : NULL;
                if (gs != NULL) GEOSGeom_destroy_r(ctx, gs);
                g = (gv != NULL) ? areal_only(ctx, gv) : NULL;
                if (gv != NULL) GEOSGeom_destroy_r(ctx, gv);
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

    /* serial: cleaned WKB list + STRtree over boxes + connected components */
    GEOSContextHandle_t ctx = GEOS_init_r();
    GEOSContext_setErrorMessageHandler_r(ctx, overlay_error_handler, NULL);
    SEXP clean = PROTECT(allocVector(VECSXP, n));
    GEOSGeometry **rect = (GEOSGeometry **) R_Calloc((size_t) n, GEOSGeometry *);
    int *store = (int *) R_Calloc((size_t) n, int);
    GEOSSTRtree *tree = GEOSSTRtree_create_r(ctx, 10);
    for (int i = 0; i < n; i++) {
        store[i] = i;
        if (cbuf[i] != NULL) {
            SEXP r = allocVector(RAWSXP, (R_xlen_t) clen[i]);
            memcpy(RAW(r), cbuf[i], clen[i]);
            SET_VECTOR_ELT(clean, i, r);
            free(cbuf[i]);
        } else {
            SET_VECTOR_ELT(clean, i, allocVector(RAWSXP, 0));
        }
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
    R_Free(clen); R_Free(cbuf);

    SEXP out = PROTECT(allocVector(VECSXP, 3));
    SET_VECTOR_ELT(out, 0, comp);
    SET_VECTOR_ELT(out, 1, bbox);
    SET_VECTOR_ELT(out, 2, clean);
    UNPROTECT(4);
    return out;
}

/* ---- run one batch of jobs ----------------------------------------------- */

/* C_overlay_run(wkb_chunk, job_chunk, rects, n_threads)
 *   wkb_chunk : VECSXP of RAWSXP cleaned WKB (a feature may repeat across tiles)
 *   job_chunk : INTSXP job id per chunk input (1..njobs, dense)
 *   rects     : REALSXP length 4*njobs (xmin,ymin,xmax,ymax per job); NA xmin
 *               means the job is not clipped (a whole small component)
 *   n_threads : INTSXP(1) OpenMP threads (<=0 -> all cores)
 * returns VECSXP(4): hex-WKB pieces, INTSXP origins (1-based chunk indices),
 *                    piece areas, input areas (per chunk input, clipped). */
SEXP C_overlay_run(SEXP wkb_chunk, SEXP job_chunk, SEXP rects_sexp, SEXP nthreads_sexp) {
    overlay_geos_init();
    int m = (int) Rf_length(wkb_chunk);
    int nthreads = (Rf_length(nthreads_sexp) > 0) ? INTEGER(nthreads_sexp)[0] : 0;
    int have_rects = (rects_sexp != R_NilValue && Rf_length(rects_sexp) >= 4);
    const double *rects = have_rects ? REAL(rects_sexp) : NULL;

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
    if (nthreads <= 0) nthreads = omp_get_max_threads();
    if (nthreads > njobs) nthreads = njobs > 0 ? njobs : 1;
#else
    nthreads = 1;
#endif
    int nw = nthreads > 0 ? nthreads : 1;
    OutList *worker = (OutList *) R_alloc((size_t) nw, sizeof(OutList));
    for (int t = 0; t < nw; t++) ol_init(&worker[t]);

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
                         rect, &worker[tid], inarea);
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
                         rect, &worker[0], inarea);
        }
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOSWKBWriter_destroy_r(ctx, writer);
        GEOS_finish_r(ctx);
    }
#endif

    size_t total = 0;
    for (int t = 0; t < nw; t++) total += worker[t].n;
    SEXP geoms   = PROTECT(allocVector(STRSXP, (R_xlen_t) total));
    SEXP origins = PROTECT(allocVector(VECSXP, (R_xlen_t) total));
    SEXP parea   = PROTECT(allocVector(REALSXP, (R_xlen_t) total));
    double *pa = REAL(parea);
    size_t w = 0;
    for (int t = 0; t < nw; t++) {
        OutList *ol = &worker[t];
        for (size_t k = 0; k < ol->n; k++) {
            SET_STRING_ELT(geoms, (R_xlen_t) w, mkChar(ol->a[k].hex));
            SEXP lab = allocVector(INTSXP, ol->a[k].nlab);
            int *lp = INTEGER(lab);
            for (int jj = 0; jj < ol->a[k].nlab; jj++) lp[jj] = ol->a[k].lab[jj] + 1;
            SET_VECTOR_ELT(origins, (R_xlen_t) w, lab);
            pa[w] = ol->a[k].area;
            free(ol->a[k].hex); free(ol->a[k].lab);
            w++;
        }
        free(ol->a);
    }

    SEXP iarea = PROTECT(allocVector(REALSXP, m));
    memcpy(REAL(iarea), inarea, (size_t) m * sizeof(double));
    R_Free(inarea); R_Free(jfill); R_Free(jsize);

    SEXP res = PROTECT(allocVector(VECSXP, 4));
    SET_VECTOR_ELT(res, 0, geoms);
    SET_VECTOR_ELT(res, 1, origins);
    SET_VECTOR_ELT(res, 2, parea);
    SET_VECTOR_ELT(res, 3, iarea);
    UNPROTECT(5);
    return res;
}

SEXP C_geos_version(void) {
    overlay_geos_init();
    return mkString(GEOSversion());
}
