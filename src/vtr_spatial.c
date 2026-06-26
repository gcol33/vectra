/* GEOS-native streaming spatial verbs.
 *
 * The streamed spatial verbs (spatial_filter / spatial_clip / spatial_dissolve)
 * carry geometry through the engine as hex-WKB in an ordinary string column.
 * These entry points run their fixed GEOS operation straight off that column,
 * with no per-batch round-trip through sf: the small resident side (a locator
 * layer or a clip mask) is parsed once into a GEOSLocator external pointer, and
 * each streamed batch is tested or cut in C.
 *
 *   C_geos_locator_build -- parse the resident WKB once into geometries plus an
 *                           STRtree, returned as an external pointer reused
 *                           across every batch.
 *   C_geos_filter        -- per batch row, keep it when its geometry relates to
 *                           any resident feature under a topological predicate.
 *   C_geos_join          -- per batch row, the indices of every resident feature
 *                           it relates to (the spatial-join match lists).
 *   C_geos_clip          -- per batch row, intersect (clip) or difference
 *                           (erase) its geometry against the resident mask.
 *   C_geos_union_hex     -- union a group's geometries into one (dissolve).
 *   C_geos_locate_xy     -- match raw point coordinates (no WKB round-trip)
 *                           against the locator: the coords= verbs and zonal.
 *   C_geos_points_to_hex -- encode raw point coordinates to point hex-WKB.
 *
 * Geometry math runs in the planar GEOS frame, exactly as the sf path does; the
 * caller aligns CRS on the resident side before building the locator.
 */

#include <R.h>
#include <Rinternals.h>
#include <stdlib.h>
#include <string.h>
#include "libgeos.h"
#include "vtr_geos.h"

#ifdef _OPENMP
#include <omp.h>
#endif

/* ---- predicate codes (x relates to resident y) --------------------------- */
/* Each is evaluated with a prepared geometry on the resident y side, so the
 * direction is expressed through y: e.g. "x within y" is "y contains x".
 * EQUALS is symmetric and has no prepared form, so it is confirmed with the raw
 * GEOSEquals on the resident geometry instead. WITHIN_DISTANCE takes the radius
 * in `dist`; DISJOINT is handled by the filter without the tree (its matches are
 * the boxes the tree prunes away). */
#define VTR_PRED_INTERSECTS      0
#define VTR_PRED_WITHIN          1   /* x within y     -> prepared_y.contains(x)   */
#define VTR_PRED_CONTAINS        2   /* x contains y   -> prepared_y.within(x)     */
#define VTR_PRED_OVERLAPS        3
#define VTR_PRED_COVERS          4   /* x covers y     -> prepared_y.coveredby(x)  */
#define VTR_PRED_COVERED_BY      5   /* x covered by y -> prepared_y.covers(x)     */
#define VTR_PRED_TOUCHES         6
#define VTR_PRED_CROSSES         7
#define VTR_PRED_EQUALS          8   /* x equals y     -> GEOSEquals(y, x)         */
#define VTR_PRED_DISJOINT        9   /* x disjoint y (filter only, via intersects) */
#define VTR_PRED_WITHIN_DISTANCE 10  /* x within `dist` of y                       */
#define VTR_PRED_NEAREST         11  /* single nearest resident feature (locate_xy)*/

static char prep_relates(GEOSContextHandle_t ctx, const GEOSPreparedGeometry *py,
                         const GEOSGeometry *x, int pred, double dist) {
    switch (pred) {
        case VTR_PRED_INTERSECTS:      return GEOSPreparedIntersects_r(ctx, py, x);
        case VTR_PRED_WITHIN:          return GEOSPreparedContains_r(ctx, py, x);
        case VTR_PRED_CONTAINS:        return GEOSPreparedWithin_r(ctx, py, x);
        case VTR_PRED_OVERLAPS:        return GEOSPreparedOverlaps_r(ctx, py, x);
        case VTR_PRED_COVERS:          return GEOSPreparedCoveredBy_r(ctx, py, x);
        case VTR_PRED_COVERED_BY:      return GEOSPreparedCovers_r(ctx, py, x);
        case VTR_PRED_TOUCHES:         return GEOSPreparedTouches_r(ctx, py, x);
        case VTR_PRED_CROSSES:         return GEOSPreparedCrosses_r(ctx, py, x);
        case VTR_PRED_WITHIN_DISTANCE: return GEOSPreparedDistanceWithin_r(ctx, py, x, dist);
        default:                       return 0;
    }
}

/* ---- resident locator (external pointer) --------------------------------- */

typedef struct {
    GEOSContextHandle_t ctx;   /* owns the parsed geometries + tree + finalizer */
    GEOSGeometry **geom;       /* resident geometries, parsed once (may be NULL) */
    int *store;                /* tree payload: the feature index */
    GEOSSTRtree *tree;         /* over the resident geometries, pre-warmed       */
    int n;                     /* feature slots (some geom[i] may be NULL)        */
    int n_live;                /* features that parsed to a non-NULL geometry     */
} GeosLocator;

static void locator_noop_cb(void *item, void *userdata) { (void) item; (void) userdata; }

static void geos_locator_finalize(SEXP ptr) {
    GeosLocator *loc = (GeosLocator *) R_ExternalPtrAddr(ptr);
    if (loc == NULL) return;
    GEOSContextHandle_t ctx = loc->ctx;
    if (loc->tree != NULL) GEOSSTRtree_destroy_r(ctx, loc->tree);
    for (int i = 0; i < loc->n; i++)
        if (loc->geom[i] != NULL) GEOSGeom_destroy_r(ctx, loc->geom[i]);
    free(loc->geom);
    free(loc->store);
    GEOS_finish_r(ctx);
    free(loc);
    R_ClearExternalPtr(ptr);
}

/* C_geos_locator_build(wkb_list): wkb_list is a VECSXP of RAWSXP (resident
 * features' WKB). Returns an external pointer to a GeosLocator reused across all
 * batches. */
SEXP C_geos_locator_build(SEXP wkb_list) {
    vtr_geos_ensure_api();
    int n = (int) Rf_length(wkb_list);

    GEOSContextHandle_t ctx = GEOS_init_r();
    GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
    GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);

    GeosLocator *loc = (GeosLocator *) calloc(1, sizeof(GeosLocator));
    if (loc == NULL) { GEOSWKBReader_destroy_r(ctx, reader); GEOS_finish_r(ctx);
                       error("vectra: out of memory building spatial locator"); }
    loc->ctx = ctx;
    loc->n = n;
    loc->geom = (GEOSGeometry **) calloc((size_t) (n > 0 ? n : 1), sizeof(GEOSGeometry *));
    loc->store = (int *) malloc((size_t) (n > 0 ? n : 1) * sizeof(int));
    loc->tree = GEOSSTRtree_create_r(ctx, 10);
    if (loc->geom == NULL || loc->store == NULL) {
        free(loc->geom); free(loc->store); free(loc);
        if (reader != NULL) GEOSWKBReader_destroy_r(ctx, reader);
        GEOS_finish_r(ctx);
        error("vectra: out of memory building spatial locator");
    }

    int first_live = -1;
    loc->n_live = 0;
    for (int i = 0; i < n; i++) {
        loc->store[i] = i;
        SEXP raw = VECTOR_ELT(wkb_list, i);
        GEOSGeometry *g = NULL;
        if (TYPEOF(raw) == RAWSXP && Rf_length(raw) > 0)
            g = GEOSWKBReader_read_r(ctx, reader, RAW(raw), (size_t) Rf_length(raw));
        loc->geom[i] = g;
        if (g != NULL) {
            GEOSSTRtree_insert_r(ctx, loc->tree, g, &loc->store[i]);
            loc->n_live++;
            if (first_live < 0) first_live = i;
        }
    }
    GEOSWKBReader_destroy_r(ctx, reader);

    /* Force the STRtree to build its index now (it builds lazily on first
     * query), so the per-batch parallel queries are read-only and race-free. */
    if (first_live >= 0)
        GEOSSTRtree_query_r(ctx, loc->tree, loc->geom[first_live],
                            locator_noop_cb, NULL);

    SEXP ptr = PROTECT(R_MakeExternalPtr(loc, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ptr, geos_locator_finalize, TRUE);
    UNPROTECT(1);
    return ptr;
}

/* ---- extract a batch's hex column into C arrays (no R API in threads) ----- */

static void extract_hex(SEXP batch_hex, int m,
                        const unsigned char **hex, size_t *hexlen) {
    for (int r = 0; r < m; r++) {
        SEXP s = STRING_ELT(batch_hex, r);
        if (s == NA_STRING) { hex[r] = NULL; hexlen[r] = 0; }
        else { hex[r] = (const unsigned char *) CHAR(s); hexlen[r] = (size_t) LENGTH(s); }
    }
}

static int resolve_threads(SEXP nthreads_sexp, int work) {
#ifdef _OPENMP
    int nt = (Rf_length(nthreads_sexp) > 0) ? INTEGER(nthreads_sexp)[0] : 0;
    if (nt <= 0) nt = omp_get_max_threads();
    if (nt > work) nt = work > 0 ? work : 1;
    return nt;
#else
    (void) nthreads_sexp; (void) work;
    return 1;
#endif
}

/* ---- per-row matching (shared by filter and join) ------------------------ */

/* small dynamic int vector, used both for STRtree candidates and match lists */
typedef struct { int *idx; int n, cap; } IntVec;
static void intvec_push(IntVec *v, int x) {
    if (v->n == v->cap) {
        v->cap = v->cap ? v->cap * 2 : 16;
        v->idx = (int *) realloc(v->idx, (size_t) v->cap * sizeof(int));
        if (v->idx == NULL) error("vectra: out of memory in spatial match");
    }
    v->idx[v->n++] = x;
}
static void cand_cb(void *item, void *userdata) {
    intvec_push((IntVec *) userdata, *(int *) item);
}
static int int_cmp(const void *a, const void *b) {
    int x = *(const int *) a, y = *(const int *) b;
    return (x > y) - (x < y);
}

/* Resident features relating to `xg` under `pred`. Queries the STRtree for
 * candidates (into the reused `cand` scratch), then confirms each. `equals` is
 * confirmed with the raw resident geometry (no prepared form); the rest use a
 * lazily-built prepared geometry, passing `dist` for within-distance. For
 * within-distance the tree is queried with `xg`'s envelope grown by `dist`, so
 * features within range but not bounding-box-overlapping are still found. With
 * `first_only` it stops at the first match. When `hits` is non-NULL the
 * confirmed feature indices are appended to it. Returns the number of matches. */
static int row_relate(GEOSContextHandle_t ctx, GeosLocator *loc,
                      const GEOSPreparedGeometry **prep, IntVec *cand,
                      const GEOSGeometry *xg, int pred, double dist,
                      int first_only, IntVec *hits) {
    const GEOSGeometry *qg = xg;
    GEOSGeometry *qfree = NULL;
    if (pred == VTR_PRED_WITHIN_DISTANCE) {
        double xmin, ymin, xmax, ymax;
        if (GEOSGeom_getExtent_r(ctx, xg, &xmin, &ymin, &xmax, &ymax)) {
            qfree = GEOSGeom_createRectangle_r(ctx, xmin - dist, ymin - dist,
                                               xmax + dist, ymax + dist);
            if (qfree != NULL) qg = qfree;
        }
    }
    cand->n = 0;
    GEOSSTRtree_query_r(ctx, loc->tree, qg, cand_cb, cand);
    int nh = 0;
    for (int c = 0; c < cand->n; c++) {
        int k = cand->idx[c];
        if (loc->geom[k] == NULL) continue;
        char ok;
        if (pred == VTR_PRED_EQUALS) {
            ok = GEOSEquals_r(ctx, loc->geom[k], xg);
        } else {
            if (prep[k] == NULL) prep[k] = GEOSPrepare_r(ctx, loc->geom[k]);
            ok = (prep[k] != NULL) ? prep_relates(ctx, prep[k], xg, pred, dist) : 0;
        }
        if (ok == 1) {
            if (hits != NULL) intvec_push(hits, k);
            nh++;
            if (first_only) break;
        }
    }
    if (qfree != NULL) GEOSGeom_destroy_r(ctx, qfree);
    return nh;
}

/* ---- filter -------------------------------------------------------------- */

/* C_geos_filter(loc_ptr, batch_hex, pred, negate, dist, nthreads) -> LGLSXP(m):
 * TRUE where the batch geometry relates to any resident feature under `pred`
 * (XOR `negate`). For disjoint, a row matches when it is disjoint from at least
 * one resident feature, i.e. it does not intersect every one of them, which is
 * counted from the intersect matches without enumerating the bbox complement. */
SEXP C_geos_filter(SEXP loc_ptr, SEXP batch_hex, SEXP pred_sexp,
                   SEXP negate_sexp, SEXP dist_sexp, SEXP nthreads_sexp) {
    vtr_geos_ensure_api();
    GeosLocator *loc = (GeosLocator *) R_ExternalPtrAddr(loc_ptr);
    if (loc == NULL) error("vectra: spatial locator was released");
    int m = (int) Rf_length(batch_hex);
    int pred = INTEGER(pred_sexp)[0];
    int negate = LOGICAL(negate_sexp)[0];
    double dist = REAL(dist_sexp)[0];

    const unsigned char **hex =
        (const unsigned char **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(const unsigned char *));
    size_t *hexlen = (size_t *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(size_t));
    extract_hex(batch_hex, m, hex, hexlen);

    SEXP out = PROTECT(allocVector(LGLSXP, m));
    int *res = LOGICAL(out);
    int nt = resolve_threads(nthreads_sexp, m);

#ifdef _OPENMP
    #pragma omp parallel num_threads(nt)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        /* per-thread prepared geometries over the shared resident geoms, built
         * lazily so only candidates ever touched are prepared */
        const GEOSPreparedGeometry **prep = (const GEOSPreparedGeometry **)
            calloc((size_t) (loc->n > 0 ? loc->n : 1), sizeof(const GEOSPreparedGeometry *));
        IntVec cand; cand.idx = NULL; cand.n = 0; cand.cap = 0;
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, 256)
#endif
        for (int r = 0; r < m; r++) {
            int hit = 0;
            if (hex[r] != NULL) {
                GEOSGeometry *xg = GEOSWKBReader_readHEX_r(ctx, reader, hex[r], hexlen[r]);
                if (xg != NULL) {
                    if (pred == VTR_PRED_DISJOINT) {
                        int ni = row_relate(ctx, loc, prep, &cand, xg,
                                            VTR_PRED_INTERSECTS, 0.0, 0, NULL);
                        hit = ni < loc->n_live;
                    } else {
                        hit = row_relate(ctx, loc, prep, &cand, xg, pred, dist,
                                         1, NULL) > 0;
                    }
                    GEOSGeom_destroy_r(ctx, xg);
                }
            }
            res[r] = negate ? !hit : hit;
        }
        for (int k = 0; k < loc->n; k++)
            if (prep[k] != NULL) GEOSPreparedGeom_destroy_r(ctx, prep[k]);
        free((void *) prep);
        free(cand.idx);
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOS_finish_r(ctx);
    }

    UNPROTECT(1);
    return out;
}

/* ---- join (match lists) -------------------------------------------------- */

/* C_geos_join(loc_ptr, batch_hex, pred, dist, nthreads) -> VECSXP(m): for each
 * batch row an INTSXP of the 1-based resident-feature indices it relates to
 * under `pred` (sorted ascending, empty when none). The R driver replicates the
 * left row once per match and attaches the resident attributes. */
SEXP C_geos_join(SEXP loc_ptr, SEXP batch_hex, SEXP pred_sexp,
                 SEXP dist_sexp, SEXP nthreads_sexp) {
    vtr_geos_ensure_api();
    GeosLocator *loc = (GeosLocator *) R_ExternalPtrAddr(loc_ptr);
    if (loc == NULL) error("vectra: spatial locator was released");
    int m = (int) Rf_length(batch_hex);
    int pred = INTEGER(pred_sexp)[0];
    double dist = REAL(dist_sexp)[0];

    const unsigned char **hex =
        (const unsigned char **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(const unsigned char *));
    size_t *hexlen = (size_t *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(size_t));
    extract_hex(batch_hex, m, hex, hexlen);

    /* per-row match arrays (1-based), filled in threads then serialised into a
     * VECSXP afterwards because allocVector allocates. */
    int **mptr = (int **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(int *));
    int *mlen = (int *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(int));
    for (int r = 0; r < m; r++) { mptr[r] = NULL; mlen[r] = 0; }
    int nt = resolve_threads(nthreads_sexp, m);

#ifdef _OPENMP
    #pragma omp parallel num_threads(nt)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        const GEOSPreparedGeometry **prep = (const GEOSPreparedGeometry **)
            calloc((size_t) (loc->n > 0 ? loc->n : 1), sizeof(const GEOSPreparedGeometry *));
        IntVec cand; cand.idx = NULL; cand.n = 0; cand.cap = 0;
        IntVec hits; hits.idx = NULL; hits.n = 0; hits.cap = 0;
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, 256)
#endif
        for (int r = 0; r < m; r++) {
            if (hex[r] == NULL) continue;
            GEOSGeometry *xg = GEOSWKBReader_readHEX_r(ctx, reader, hex[r], hexlen[r]);
            if (xg == NULL) continue;
            hits.n = 0;
            row_relate(ctx, loc, prep, &cand, xg, pred, dist, 0, &hits);
            GEOSGeom_destroy_r(ctx, xg);
            if (hits.n > 0) {
                qsort(hits.idx, (size_t) hits.n, sizeof(int), int_cmp);
                int *a = (int *) malloc((size_t) hits.n * sizeof(int));
                if (a == NULL) error("vectra: out of memory in spatial join");
                for (int j = 0; j < hits.n; j++) a[j] = hits.idx[j] + 1;
                mptr[r] = a; mlen[r] = hits.n;
            }
        }
        for (int k = 0; k < loc->n; k++)
            if (prep[k] != NULL) GEOSPreparedGeom_destroy_r(ctx, prep[k]);
        free((void *) prep);
        free(cand.idx);
        free(hits.idx);
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOS_finish_r(ctx);
    }

    SEXP out = PROTECT(allocVector(VECSXP, m));
    for (int r = 0; r < m; r++) {
        SEXP v = allocVector(INTSXP, mlen[r]);
        if (mlen[r] > 0) {
            memcpy(INTEGER(v), mptr[r], (size_t) mlen[r] * sizeof(int));
            free(mptr[r]);
        }
        SET_VECTOR_ELT(out, r, v);
    }
    UNPROTECT(1);
    return out;
}

/* ---- nearest feature ----------------------------------------------------- */

/* Distance between the query geometry and a resident feature, for the STRtree
 * nearest-neighbour traversal. The two items are the query (passed to
 * nearest_generic) and a tree item (the resident `&store[k]`); their order is
 * not contracted, so the query is told apart by pointer identity. */
typedef struct {
    GEOSContextHandle_t ctx;
    GeosLocator *loc;
    const void *query_item;    /* the query geometry, as the item we passed       */
} NearestCtx;

static int nearest_distfn(const void *item1, const void *item2,
                          double *distance, void *userdata) {
    NearestCtx *nc = (NearestCtx *) userdata;
    const void *qi = nc->query_item;
    const GEOSGeometry *q;
    int k;
    if (item1 == qi) { q = (const GEOSGeometry *) item1; k = *(const int *) item2; }
    else             { q = (const GEOSGeometry *) item2; k = *(const int *) item1; }
    const GEOSGeometry *g = nc->loc->geom[k];
    if (g == NULL) { *distance = 0; return 0; }
    return GEOSDistance_r(nc->ctx, q, g, distance);   /* 1 on success, 0 on error */
}

/* C_geos_nearest(loc_ptr, batch_hex, nthreads) -> INTSXP(m): the 1-based index
 * of the single resident feature nearest to each batch row (NA where the row has
 * no geometry or the tree is empty). One match per row, as st_nearest_feature. */
SEXP C_geos_nearest(SEXP loc_ptr, SEXP batch_hex, SEXP nthreads_sexp) {
    vtr_geos_ensure_api();
    GeosLocator *loc = (GeosLocator *) R_ExternalPtrAddr(loc_ptr);
    if (loc == NULL) error("vectra: spatial locator was released");
    int m = (int) Rf_length(batch_hex);

    const unsigned char **hex =
        (const unsigned char **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(const unsigned char *));
    size_t *hexlen = (size_t *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(size_t));
    extract_hex(batch_hex, m, hex, hexlen);

    SEXP out = PROTECT(allocVector(INTSXP, m));
    int *res = INTEGER(out);
    for (int r = 0; r < m; r++) res[r] = NA_INTEGER;
    int nt = resolve_threads(nthreads_sexp, m);

#ifdef _OPENMP
    #pragma omp parallel num_threads(nt)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, 256)
#endif
        for (int r = 0; r < m; r++) {
            if (loc->n_live == 0 || hex[r] == NULL) continue;
            GEOSGeometry *xg = GEOSWKBReader_readHEX_r(ctx, reader, hex[r], hexlen[r]);
            if (xg == NULL) continue;
            NearestCtx nc; nc.ctx = ctx; nc.loc = loc; nc.query_item = (const void *) xg;
            const void *hit = GEOSSTRtree_nearest_generic_r(
                ctx, loc->tree, (const void *) xg, xg, nearest_distfn, &nc);
            if (hit != NULL) res[r] = *(const int *) hit + 1;
            GEOSGeom_destroy_r(ctx, xg);
        }
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOS_finish_r(ctx);
    }

    UNPROTECT(1);
    return out;
}

/* ---- locate raw point coordinates ---------------------------------------- */

/* C_geos_locate_xy(loc, x, y, pred, dist, want_all, nthreads): match raw point
 * coordinates against the resident locator -- the sf-free counterpart of
 * encoding the points to WKB and running C_geos_join / C_geos_nearest. Each
 * point is built once with GEOSGeom_createPointFromXY, so there is no hex
 * round-trip. Serves the coords= spatial verbs and the vector-zone path of
 * zonal().
 *
 *   want_all == FALSE -> INTSXP(m): the smallest 1-based resident index the point
 *     matches under `pred` (the first index sf's predicates list), or NA when
 *     none; for pred == VTR_PRED_NEAREST the single nearest resident index.
 *   want_all == TRUE  -> VECSXP(m): every 1-based match index, sorted ascending.
 */
SEXP C_geos_locate_xy(SEXP loc_ptr, SEXP x_sexp, SEXP y_sexp, SEXP pred_sexp,
                      SEXP dist_sexp, SEXP want_all_sexp, SEXP nthreads_sexp) {
    vtr_geos_ensure_api();
    GeosLocator *loc = (GeosLocator *) R_ExternalPtrAddr(loc_ptr);
    if (loc == NULL) error("vectra: spatial locator was released");
    int m = (int) Rf_length(x_sexp);
    if ((int) Rf_length(y_sexp) != m)
        error("vectra: x and y must have the same length");
    int pred = INTEGER(pred_sexp)[0];
    double dist = REAL(dist_sexp)[0];
    int want_all = LOGICAL(want_all_sexp)[0];
    const double *xs = REAL(x_sexp), *ys = REAL(y_sexp);

    /* first-hit results land straight in an INTSXP; all-match results collect
     * per-row arrays in threads, serialised afterwards (allocVector allocates). */
    SEXP out_first = R_NilValue;
    int *res = NULL;
    int **mptr = NULL; int *mlen = NULL;
    if (want_all) {
        mptr = (int **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(int *));
        mlen = (int *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(int));
        for (int r = 0; r < m; r++) { mptr[r] = NULL; mlen[r] = 0; }
    } else {
        out_first = PROTECT(allocVector(INTSXP, m));
        res = INTEGER(out_first);
        for (int r = 0; r < m; r++) res[r] = NA_INTEGER;
    }
    int nt = resolve_threads(nthreads_sexp, m);

#ifdef _OPENMP
    #pragma omp parallel num_threads(nt)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
        const GEOSPreparedGeometry **prep = (const GEOSPreparedGeometry **)
            calloc((size_t) (loc->n > 0 ? loc->n : 1), sizeof(const GEOSPreparedGeometry *));
        IntVec cand; cand.idx = NULL; cand.n = 0; cand.cap = 0;
        IntVec hits; hits.idx = NULL; hits.n = 0; hits.cap = 0;
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, 256)
#endif
        for (int r = 0; r < m; r++) {
            double xr = xs[r], yr = ys[r];
            if (ISNAN(xr) || ISNAN(yr)) continue;
            GEOSGeometry *pt = GEOSGeom_createPointFromXY_r(ctx, xr, yr);
            if (pt == NULL) continue;
            if (pred == VTR_PRED_NEAREST) {
                if (!want_all && loc->n_live > 0) {
                    NearestCtx nc; nc.ctx = ctx; nc.loc = loc;
                    nc.query_item = (const void *) pt;
                    const void *hit = GEOSSTRtree_nearest_generic_r(
                        ctx, loc->tree, (const void *) pt, pt, nearest_distfn, &nc);
                    if (hit != NULL) res[r] = *(const int *) hit + 1;
                }
                GEOSGeom_destroy_r(ctx, pt);
                continue;
            }
            hits.n = 0;
            row_relate(ctx, loc, prep, &cand, pt, pred, dist, 0, &hits);
            GEOSGeom_destroy_r(ctx, pt);
            if (hits.n == 0) continue;
            if (want_all) {
                qsort(hits.idx, (size_t) hits.n, sizeof(int), int_cmp);
                int *a = (int *) malloc((size_t) hits.n * sizeof(int));
                if (a == NULL) error("vectra: out of memory in spatial locate");
                for (int j = 0; j < hits.n; j++) a[j] = hits.idx[j] + 1;
                mptr[r] = a; mlen[r] = hits.n;
            } else {
                int mn = hits.idx[0];
                for (int j = 1; j < hits.n; j++) if (hits.idx[j] < mn) mn = hits.idx[j];
                res[r] = mn + 1;
            }
        }
        for (int k = 0; k < loc->n; k++)
            if (prep[k] != NULL) GEOSPreparedGeom_destroy_r(ctx, prep[k]);
        free((void *) prep);
        free(cand.idx);
        free(hits.idx);
        GEOS_finish_r(ctx);
    }

    if (!want_all) { UNPROTECT(1); return out_first; }

    SEXP out = PROTECT(allocVector(VECSXP, m));
    for (int r = 0; r < m; r++) {
        SEXP v = allocVector(INTSXP, mlen[r]);
        if (mlen[r] > 0) {
            memcpy(INTEGER(v), mptr[r], (size_t) mlen[r] * sizeof(int));
            free(mptr[r]);
        }
        SET_VECTOR_ELT(out, r, v);
    }
    UNPROTECT(1);
    return out;
}

/* C_geos_points_to_hex(x, y) -> STRSXP(m): each (x, y) as point hex-WKB, NA where
 * a coordinate is missing. Gives the coords= spatial join the same point geometry
 * output column the sf path emits, without round-tripping through sf. */
SEXP C_geos_points_to_hex(SEXP x_sexp, SEXP y_sexp) {
    vtr_geos_ensure_api();
    int m = (int) Rf_length(x_sexp);
    if ((int) Rf_length(y_sexp) != m)
        error("vectra: x and y must have the same length");
    const double *xs = REAL(x_sexp), *ys = REAL(y_sexp);

    GEOSContextHandle_t ctx = GEOS_init_r();
    GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
    GEOSWKBWriter *writer = GEOSWKBWriter_create_r(ctx);

    SEXP out = PROTECT(allocVector(STRSXP, m));
    for (int r = 0; r < m; r++) {
        SET_STRING_ELT(out, r, NA_STRING);
        if (ISNAN(xs[r]) || ISNAN(ys[r])) continue;
        GEOSGeometry *pt = GEOSGeom_createPointFromXY_r(ctx, xs[r], ys[r]);
        if (pt == NULL) continue;
        size_t len = 0;
        unsigned char *buf = GEOSWKBWriter_writeHEX_r(ctx, writer, pt, &len);
        if (buf != NULL) {
            SET_STRING_ELT(out, r, mkCharLen((const char *) buf, (int) len));
            GEOSFree_r(ctx, buf);
        }
        GEOSGeom_destroy_r(ctx, pt);
    }
    GEOSWKBWriter_destroy_r(ctx, writer);
    GEOS_finish_r(ctx);
    UNPROTECT(1);
    return out;
}

/* ---- clip / erase -------------------------------------------------------- */

/* C_geos_clip(loc_ptr, batch_hex, erase, nthreads) -> STRSXP(m):
 * the clipped (intersection) or erased (difference) geometry as hex-WKB, with
 * NA_STRING where the row is dropped (empty result). loc must hold a single
 * resident mask geometry (geom[0]). */
SEXP C_geos_clip(SEXP loc_ptr, SEXP batch_hex, SEXP erase_sexp, SEXP nthreads_sexp) {
    vtr_geos_ensure_api();
    GeosLocator *loc = (GeosLocator *) R_ExternalPtrAddr(loc_ptr);
    if (loc == NULL) error("vectra: spatial locator was released");
    if (loc->n < 1 || loc->geom[0] == NULL)
        error("vectra: clip mask is empty");
    const GEOSGeometry *mask = loc->geom[0];
    int erase = LOGICAL(erase_sexp)[0];
    int m = (int) Rf_length(batch_hex);

    const unsigned char **hex =
        (const unsigned char **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(const unsigned char *));
    size_t *hexlen = (size_t *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(size_t));
    extract_hex(batch_hex, m, hex, hexlen);

    /* per-row disposition + cut hex (filled in threads, serialised into the
     * STRSXP afterwards because mkChar/SET_STRING_ELT allocate). */
    char *disp = (char *) R_alloc((size_t) (m > 0 ? m : 1), sizeof(char));  /* 0 drop, 1 cut, 2 keep */
    char **cut = (char **) R_alloc((size_t) (m > 0 ? m : 1), sizeof(char *));
    for (int r = 0; r < m; r++) { disp[r] = 0; cut[r] = NULL; }
    int nt = resolve_threads(nthreads_sexp, m);

#ifdef _OPENMP
    #pragma omp parallel num_threads(nt)
#endif
    {
        GEOSContextHandle_t ctx = GEOS_init_r();
        GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
        GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
        GEOSWKBWriter *writer = GEOSWKBWriter_create_r(ctx);
        const GEOSPreparedGeometry *pm = GEOSPrepare_r(ctx, mask);
#ifdef _OPENMP
        #pragma omp for schedule(dynamic, 128)
#endif
        for (int r = 0; r < m; r++) {
            if (hex[r] == NULL) { disp[r] = 0; continue; }
            GEOSGeometry *xg = GEOSWKBReader_readHEX_r(ctx, reader, hex[r], hexlen[r]);
            if (xg == NULL) { disp[r] = 0; continue; }
            char meets = (pm != NULL) ? GEOSPreparedIntersects_r(ctx, pm, xg) : 1;
            GEOSGeometry *g = NULL;
            if (erase) {
                if (meets != 1) { disp[r] = 2; GEOSGeom_destroy_r(ctx, xg); continue; }
                g = GEOSDifference_r(ctx, xg, mask);
            } else {
                if (meets != 1) { disp[r] = 0; GEOSGeom_destroy_r(ctx, xg); continue; }
                g = GEOSIntersection_r(ctx, xg, mask);
            }
            GEOSGeom_destroy_r(ctx, xg);
            if (g == NULL) { disp[r] = (erase ? 2 : 0); continue; }
            if (GEOSisEmpty_r(ctx, g)) { GEOSGeom_destroy_r(ctx, g); disp[r] = 0; continue; }
            size_t len = 0;
            unsigned char *buf = GEOSWKBWriter_writeHEX_r(ctx, writer, g, &len);
            GEOSGeom_destroy_r(ctx, g);
            if (buf == NULL) { disp[r] = 0; continue; }
            char *s = (char *) malloc(len + 1);
            if (s == NULL) error("vectra: out of memory in spatial clip");
            memcpy(s, buf, len); s[len] = '\0';
            GEOSFree_r(ctx, buf);
            cut[r] = s; disp[r] = 1;
        }
        if (pm != NULL) GEOSPreparedGeom_destroy_r(ctx, pm);
        GEOSWKBWriter_destroy_r(ctx, writer);
        GEOSWKBReader_destroy_r(ctx, reader);
        GEOS_finish_r(ctx);
    }

    SEXP out = PROTECT(allocVector(STRSXP, m));
    for (int r = 0; r < m; r++) {
        if (disp[r] == 1)      { SET_STRING_ELT(out, r, mkCharLen(cut[r], (int) strlen(cut[r]))); free(cut[r]); }
        else if (disp[r] == 2) SET_STRING_ELT(out, r, STRING_ELT(batch_hex, r));
        else                   SET_STRING_ELT(out, r, NA_STRING);
    }
    UNPROTECT(1);
    return out;
}

/* ---- union (dissolve) ---------------------------------------------------- */

/* C_geos_union_hex(batch_hex) -> STRSXP(1): the unary union of a group's hex-WKB
 * geometries as one hex-WKB string (NA if the group has no valid geometry). */
SEXP C_geos_union_hex(SEXP batch_hex) {
    vtr_geos_ensure_api();
    int m = (int) Rf_length(batch_hex);

    GEOSContextHandle_t ctx = GEOS_init_r();
    GEOSContext_setErrorMessageHandler_r(ctx, vtr_geos_quiet_handler, NULL);
    GEOSWKBReader *reader = GEOSWKBReader_create_r(ctx);
    GEOSWKBWriter *writer = GEOSWKBWriter_create_r(ctx);

    GEOSGeometry **parts = (GEOSGeometry **) malloc((size_t) (m > 0 ? m : 1) * sizeof(GEOSGeometry *));
    if (parts == NULL) { GEOSWKBReader_destroy_r(ctx, reader); GEOSWKBWriter_destroy_r(ctx, writer);
                         GEOS_finish_r(ctx); error("vectra: out of memory in spatial dissolve"); }
    int np = 0;
    for (int r = 0; r < m; r++) {
        SEXP s = STRING_ELT(batch_hex, r);
        if (s == NA_STRING) continue;
        GEOSGeometry *g = GEOSWKBReader_readHEX_r(ctx, reader,
            (const unsigned char *) CHAR(s), (size_t) LENGTH(s));
        if (g != NULL) parts[np++] = g;
    }
    GEOSWKBReader_destroy_r(ctx, reader);

    SEXP out = PROTECT(allocVector(STRSXP, 1));
    SET_STRING_ELT(out, 0, NA_STRING);
    if (np > 0) {
        GEOSGeometry *coll = GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, parts, (unsigned) np);
        if (coll == NULL) {
            for (int k = 0; k < np; k++) GEOSGeom_destroy_r(ctx, parts[k]);
        } else {
            GEOSGeometry *u = GEOSUnaryUnion_r(ctx, coll);
            GEOSGeom_destroy_r(ctx, coll);
            if (u != NULL) {
                size_t len = 0;
                unsigned char *buf = GEOSWKBWriter_writeHEX_r(ctx, writer, u, &len);
                if (buf != NULL) {
                    SET_STRING_ELT(out, 0, mkCharLen((const char *) buf, (int) len));
                    GEOSFree_r(ctx, buf);
                }
                GEOSGeom_destroy_r(ctx, u);
            }
        }
    }
    free(parts);
    GEOSWKBWriter_destroy_r(ctx, writer);
    GEOS_finish_r(ctx);
    UNPROTECT(1);
    return out;
}
