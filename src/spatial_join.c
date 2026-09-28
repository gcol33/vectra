/* SpatialJoinNode: a streamed spatial join of a left node against a resident
 * layer, as a lazy plan node.
 *
 * The right side (a small sf layer) is parsed once into a GeosLocator (parsed
 * geometries + STRtree, vtr_spatial.c) and its attribute columns are held as one
 * dense resident VecBatch. The left side streams: each pulled batch is matched
 * in one parallel pass (vtr_geos_match_batch, per-thread GEOS contexts), from a
 * hex-WKB geometry column or from two raw coordinate columns, and the joined
 * rows are gathered straight into an output batch. Nothing is spilled and no
 * batch passes through R, so the result streams into the next verb.
 *
 * Output layout is decided in R (naming, suffixes, geometry placement) and
 * handed over as one source per output column: a left column, a right column,
 * or the point geometry built from the coordinates. Each left row is emitted
 * once per match in ascending resident order; with `left`, an unmatched row is
 * emitted once with NA right columns. A batch whose matches fan out is emitted
 * in pieces of about SJ_EMIT_ROWS rows, so an output batch stays bounded.
 */

#include "spatial_join.h"
#include "plan_budget.h"
#include "vtr_spatial.h"
#include "r_bridge_internal.h"
#include "types.h"
#include "array.h"
#include "batch.h"
#include "builder.h"
#include "schema.h"
#include "error.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>

#define SJ_EMIT_ROWS 131072

enum { SJ_SRC_LEFT = 0, SJ_SRC_RIGHT = 1, SJ_SRC_POINT = 2 };

/* Hex-WKB of a 2D point: byte order, type, x, y. */
#define SJ_POINT_HEX_LEN 42

typedef struct {
    VecNode      base;
    VecNode     *child;
    GeosLocator *loc;
    VecBatch    *right;      /* resident attributes (NULL when y has none) */
    int          geom_col;   /* hex-WKB column in the child, or -1 */
    int          x_col, y_col;
    int          pred;
    double       dist;
    int          left;
    int          nthreads;
    int          n_out;
    int         *out_side;   /* SJ_SRC_* per output column */
    int         *out_src;    /* column index on that side (unused for POINT) */

    /* the left batch being emitted */
    VecBatch    *in;
    int          in_rows;    /* logical rows */
    int          cursor;     /* next logical row to emit */
    double      *xs, *ys;    /* per logical row (coords input) */
    int        **mptr;
    int         *mlen;
} SpatialJoinNode;

static void sj_release_input(SpatialJoinNode *sj) {
    if (sj->mptr != NULL)
        for (int r = 0; r < sj->in_rows; r++) free(sj->mptr[r]);
    free(sj->mptr); sj->mptr = NULL;
    free(sj->mlen); sj->mlen = NULL;
    free(sj->xs); sj->xs = NULL;
    free(sj->ys); sj->ys = NULL;
    if (sj->in != NULL) vec_batch_free(sj->in);
    sj->in = NULL;
    sj->in_rows = 0;
    sj->cursor = 0;
}

static double sj_num(const VecArray *a, int64_t p) {
    if (!vec_array_is_valid(a, p)) return NAN;
    if (a->type == VEC_DOUBLE) return a->buf.dbl[p];
    return (double) vec_array_get_int(a, p);
}

/* Take ownership of a pulled left batch and compute its match lists. */
static void sj_load(SpatialJoinNode *sj, VecBatch *b) {
    sj->in = b;
    int64_t m64 = vec_batch_logical_rows(b);
    if (m64 > INT32_MAX) vectra_error("spatial_join: batch too large");
    int m = (int) m64;
    sj->in_rows = m;
    sj->cursor = 0;
    size_t nm = (size_t) (m > 0 ? m : 1);
    sj->mptr = (int **) calloc(nm, sizeof(int *));
    sj->mlen = (int *) calloc(nm, sizeof(int));
    if (sj->mptr == NULL || sj->mlen == NULL)
        vectra_error("spatial_join: out of memory");

    const unsigned char **hex = NULL;
    size_t *hexlen = NULL;
    if (sj->geom_col >= 0) {
        const VecArray *g = &b->columns[sj->geom_col];
        hex = (const unsigned char **) malloc(nm * sizeof(const unsigned char *));
        hexlen = (size_t *) malloc(nm * sizeof(size_t));
        if (hex == NULL || hexlen == NULL) {
            free((void *) hex); free(hexlen);
            vectra_error("spatial_join: out of memory");
        }
        for (int r = 0; r < m; r++) {
            int64_t p = vec_batch_physical_row(b, r);
            if (!vec_array_is_valid(g, p)) { hex[r] = NULL; hexlen[r] = 0; continue; }
            int64_t o = g->buf.str.offsets[p];
            hex[r] = (const unsigned char *) g->buf.str.data + o;
            hexlen[r] = (size_t) (g->buf.str.offsets[p + 1] - o);
        }
    } else {
        sj->xs = (double *) malloc(nm * sizeof(double));
        sj->ys = (double *) malloc(nm * sizeof(double));
        if (sj->xs == NULL || sj->ys == NULL)
            vectra_error("spatial_join: out of memory");
        const VecArray *xa = &b->columns[sj->x_col], *ya = &b->columns[sj->y_col];
        for (int r = 0; r < m; r++) {
            int64_t p = vec_batch_physical_row(b, r);
            sj->xs[r] = sj_num(xa, p);
            sj->ys[r] = sj_num(ya, p);
        }
    }

    int rc = vtr_geos_match_batch(sj->loc, hex, hexlen, sj->xs, sj->ys, m,
                                  sj->pred, sj->dist, sj->nthreads,
                                  sj->mptr, sj->mlen);
    free((void *) hex);
    free(hexlen);
    if (rc != 0) vectra_error("spatial_join: out of memory matching a batch");
}

/* Gather rows `idx` of a resident column; a negative index is an NA row. */
static VecArray sj_gather_right(const VecArray *src, const int32_t *idx,
                                int32_t n, int any_na) {
    if (!any_na) return vec_array_gather(src, idx, n);
    VecArrayBuilder bld = vec_builder_init(src->type);
    vec_builder_reserve(&bld, n);
    for (int32_t j = 0; j < n; j++) {
        if (idx[j] < 0) vec_builder_append_na(&bld);
        else vec_builder_append_one(&bld, src, idx[j]);
    }
    return vec_builder_finish(&bld);
}

static void sj_hex_le64(char *dst, double v) {
    static const char digits[] = "0123456789ABCDEF";
    uint64_t u;
    memcpy(&u, &v, sizeof u);
    for (int k = 0; k < 8; k++) {
        unsigned byte = (unsigned) ((u >> (8 * k)) & 0xFFu);
        dst[2 * k]     = digits[byte >> 4];
        dst[2 * k + 1] = digits[byte & 0xFu];
    }
}

/* Point hex-WKB (little-endian, as GEOS and sf write it on every platform R
 * supports) for the coordinates of logical rows `lrow`; NA where either
 * coordinate is missing. */
static VecArray sj_points_hex(const SpatialJoinNode *sj, const int32_t *lrow,
                              int32_t n) {
    VecArray a = vec_array_alloc(VEC_STRING, n);
    int64_t n_valid = 0;
    for (int32_t j = 0; j < n; j++)
        if (!isnan(sj->xs[lrow[j]]) && !isnan(sj->ys[lrow[j]])) n_valid++;
    free(a.buf.str.data);
    a.buf.str.data = (char *) malloc((size_t) (n_valid > 0 ? n_valid * SJ_POINT_HEX_LEN : 1));
    if (a.buf.str.data == NULL) {
        vec_array_free(&a);
        vectra_error("spatial_join: out of memory building point geometry");
    }
    a.buf.str.data_len = n_valid * SJ_POINT_HEX_LEN;
    int64_t off = 0;
    for (int32_t j = 0; j < n; j++) {
        a.buf.str.offsets[j] = off;
        double x = sj->xs[lrow[j]], y = sj->ys[lrow[j]];
        if (isnan(x) || isnan(y)) continue;
        char *d = a.buf.str.data + off;
        memcpy(d, "0101000000", 10);
        sj_hex_le64(d + 10, x);
        sj_hex_le64(d + 26, y);
        vec_array_set_valid(&a, j);
        off += SJ_POINT_HEX_LEN;
    }
    a.buf.str.offsets[n] = off;
    return a;
}

/* Emit logical rows [start, end) of the current batch, `n_out` output rows. */
static VecBatch *sj_emit(SpatialJoinNode *sj, int start, int end, int32_t n_out) {
    size_t cap = (size_t) (n_out > 0 ? n_out : 1);
    int32_t *lrow = (int32_t *) malloc(cap * sizeof(int32_t));
    int32_t *prow = (int32_t *) malloc(cap * sizeof(int32_t));
    int32_t *rrow = (int32_t *) malloc(cap * sizeof(int32_t));
    if (lrow == NULL || prow == NULL || rrow == NULL) {
        free(lrow); free(prow); free(rrow);
        vectra_error("spatial_join: out of memory");
    }
    int32_t k = 0;
    int any_na = 0;
    for (int r = start; r < end; r++) {
        int32_t p = (int32_t) vec_batch_physical_row(sj->in, r);
        int nm = sj->mlen[r];
        if (nm == 0) {
            if (!sj->left) continue;
            lrow[k] = r; prow[k] = p; rrow[k] = -1; k++;
            any_na = 1;
            continue;
        }
        for (int j = 0; j < nm; j++) {
            lrow[k] = r; prow[k] = p; rrow[k] = sj->mptr[r][j] - 1; k++;
        }
    }

    VecBatch *out = vec_batch_alloc(sj->n_out, n_out);
    for (int c = 0; c < sj->n_out; c++) {
        const char *nm = sj->base.output_schema.col_names[c];
        out->col_names[c] = (char *) malloc(strlen(nm) + 1);
        strcpy(out->col_names[c], nm);
        switch (sj->out_side[c]) {
        case SJ_SRC_LEFT:
            out->columns[c] = vec_array_gather(&sj->in->columns[sj->out_src[c]],
                                               prow, n_out);
            break;
        case SJ_SRC_RIGHT:
            out->columns[c] = sj_gather_right(&sj->right->columns[sj->out_src[c]],
                                              rrow, n_out, any_na);
            break;
        default:
            out->columns[c] = sj_points_hex(sj, lrow, n_out);
        }
    }
    free(lrow); free(prow); free(rrow);
    return out;
}

static VecBatch *sj_next_batch(VecNode *self) {
    SpatialJoinNode *sj = (SpatialJoinNode *) self;
    for (;;) {
        if (sj->in == NULL) {
            VecBatch *b = sj->child->next_batch(sj->child);
            if (b == NULL) return NULL;
            sj_load(sj, b);
        }
        /* take whole left rows until the output reaches SJ_EMIT_ROWS */
        int start = sj->cursor, end = start;
        int64_t n_out = 0;
        while (end < sj->in_rows) {
            int64_t add = sj->mlen[end] > 0 ? sj->mlen[end] : (sj->left ? 1 : 0);
            if (end > start && n_out + add > SJ_EMIT_ROWS) break;
            n_out += add;
            end++;
        }
        if (n_out > INT32_MAX) vectra_error("spatial_join: too many matches for one row");
        VecBatch *out = (n_out > 0) ? sj_emit(sj, start, end, (int32_t) n_out) : NULL;
        sj->cursor = end;
        if (sj->cursor >= sj->in_rows) sj_release_input(sj);
        if (out != NULL) return out;
    }
}

static void sj_free(VecNode *self) {
    SpatialJoinNode *sj = (SpatialJoinNode *) self;
    sj_release_input(sj);
    if (sj->child != NULL) sj->child->free_node(sj->child);
    vtr_geos_locator_free(sj->loc);
    if (sj->right != NULL) vec_batch_free(sj->right);
    free(sj->out_side);
    free(sj->out_src);
    vec_schema_free(&sj->base.output_schema);
    free(sj);
}

static int sj_numeric_type(VecType t) {
    return t == VEC_DOUBLE || vec_type_is_int(t);
}

/* C_spatial_join_node(node, wkb_list, right_df, geom, coords, pred, dist, left,
 *                     out_side, out_src, out_names, nthreads)
 *
 * `geom` names the left hex-WKB column (or NULL) and `coords` the two left
 * coordinate columns (or NULL); exactly one is given. Output column c is
 * `out_names[c]`, taken from side `out_side[c]` (0 left, 1 right, 2 the point
 * geometry of the coordinates) at column `out_src[c]` of that side. `right_df`
 * is the resident attribute data.frame, or NULL when y carries none. */
VEC_ONE_CHILD_FN(spatial_join_children, SpatialJoinNode, child)

SEXP C_spatial_join_node(SEXP node_xptr, SEXP wkb_list, SEXP right_df,
                         SEXP geom_sexp, SEXP coords_sexp, SEXP pred_sexp,
                         SEXP dist_sexp, SEXP left_sexp, SEXP out_side_sexp,
                         SEXP out_src_sexp, SEXP out_names_sexp,
                         SEXP nthreads_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    const VecSchema *cs = &child->output_schema;
    int n_out = Rf_length(out_names_sexp);
    if (Rf_length(out_side_sexp) != n_out || Rf_length(out_src_sexp) != n_out)
        vectra_error("spatial_join: output spec lengths differ");

    int geom_col = -1, x_col = -1, y_col = -1;
    if (!Rf_isNull(geom_sexp)) {
        const char *g = CHAR(STRING_ELT(geom_sexp, 0));
        geom_col = vec_schema_find_col(cs, g);
        if (geom_col < 0)
            vectra_error("geometry column '%s' not found; pass geom= or coords=", g);
        if (cs->col_types[geom_col] != VEC_STRING)
            vectra_error("geometry column '%s' must hold hex-WKB strings", g);
    } else {
        const char *xn = CHAR(STRING_ELT(coords_sexp, 0));
        const char *yn = CHAR(STRING_ELT(coords_sexp, 1));
        x_col = vec_schema_find_col(cs, xn);
        y_col = vec_schema_find_col(cs, yn);
        if (x_col < 0 || y_col < 0)
            vectra_error("coords column(s) not found: %s",
                         x_col < 0 ? xn : yn);
        if (!sj_numeric_type(cs->col_types[x_col]) ||
            !sj_numeric_type(cs->col_types[y_col]))
            vectra_error("coords columns must be numeric");
    }

    VecBatch *right = Rf_isNull(right_df) ? NULL : df_to_batch(right_df);

    int *out_side = (int *) malloc((size_t) (n_out > 0 ? n_out : 1) * sizeof(int));
    int *out_src = (int *) malloc((size_t) (n_out > 0 ? n_out : 1) * sizeof(int));
    char **names = (char **) malloc((size_t) (n_out > 0 ? n_out : 1) * sizeof(char *));
    VecType *types = (VecType *) malloc((size_t) (n_out > 0 ? n_out : 1) * sizeof(VecType));
    if (!out_side || !out_src || !names || !types) {
        free(out_side); free(out_src); free(names); free(types);
        if (right != NULL) vec_batch_free(right);
        vectra_error("spatial_join: out of memory");
    }
    const char *bad = NULL;
    for (int c = 0; c < n_out && bad == NULL; c++) {
        int side = INTEGER(out_side_sexp)[c];
        const char *src = CHAR(STRING_ELT(out_src_sexp, c));
        names[c] = (char *) CHAR(STRING_ELT(out_names_sexp, c));
        out_side[c] = side;
        out_src[c] = -1;
        if (side == SJ_SRC_LEFT) {
            out_src[c] = vec_schema_find_col(cs, src);
            if (out_src[c] >= 0) types[c] = cs->col_types[out_src[c]];
        } else if (side == SJ_SRC_RIGHT && right != NULL) {
            for (int k = 0; k < right->n_cols; k++)
                if (strcmp(right->col_names[k], src) == 0) { out_src[c] = k; break; }
            if (out_src[c] >= 0) types[c] = right->columns[out_src[c]].type;
        } else if (side == SJ_SRC_POINT && x_col >= 0) {
            out_src[c] = 0;
            types[c] = VEC_STRING;
        }
        if (out_src[c] < 0) bad = src;
    }
    if (bad != NULL) {
        free(out_side); free(out_src); free(names); free(types);
        if (right != NULL) vec_batch_free(right);
        vectra_error("spatial_join: output column source not found: %s", bad);
    }

    SpatialJoinNode *sj = (SpatialJoinNode *) calloc(1, sizeof(SpatialJoinNode));
    if (sj == NULL) {
        free(out_side); free(out_src); free(names); free(types);
        if (right != NULL) vec_batch_free(right);
        vectra_error("spatial_join: out of memory");
    }
    sj->base.output_schema = vec_schema_create(n_out, names, types);
    free(names); free(types);
    sj->out_side = out_side;
    sj->out_src = out_src;
    sj->n_out = n_out;
    sj->right = right;
    sj->geom_col = geom_col;
    sj->x_col = x_col;
    sj->y_col = y_col;
    sj->pred = Rf_asInteger(pred_sexp);
    sj->dist = Rf_asReal(dist_sexp);
    sj->left = Rf_asLogical(left_sexp) == TRUE;
    sj->nthreads = Rf_asInteger(nthreads_sexp);
    sj->base.next_batch = sj_next_batch;
    sj->base.free_node = sj_free;
    sj->base.static_rows = NULL;
    sj->base.kind = "SpatialJoinNode";
    sj->base.children = spatial_join_children;

    /* The locator is built last: every earlier step can fail on user input,
       and until the child is taken the input pipeline stays intact. */
    sj->loc = vtr_geos_locator_new(wkb_list);
    R_ClearExternalPtr(node_xptr);
    sj->child = child;
    return wrap_node((VecNode *) sj);
}
