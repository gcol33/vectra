#include "scan.h"
#include "vtri.h"
#include "batch.h"
#include "schema.h"
#include "expr.h"
#include "array.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Pack first 8 bytes of a string into big-endian uint64 for zone map comparison.
 * pad=0x00 for min/query-lo, pad=0xFF for max/query-hi. */
static uint64_t scan_pack_str(const char *s, int64_t len, uint8_t pad) {
    uint64_t r = 0;
    for (int i = 0; i < 8; i++) {
        uint8_t b = (i < len) ? (uint8_t)s[i] : pad;
        r = (r << 8) | b;
    }
    return r;
}

/* Check if a string %in% predicate can be pruned: skip when no set value
 * could fall within the batch's [min, max] prefix range. */
static int str_in_might_match(const VecExpr *pred, const Vtr1ColStat *stats,
                               const VecSchema *schema) {
    /* pred->operand is the col_ref, pred->set_str is the string set */
    if (!pred->operand || pred->operand->kind != EXPR_COL_REF) return 1;
    if (!pred->set_str || pred->n_set == 0) return 1;

    int col_idx = vec_schema_find_col(schema, pred->operand->col_name);
    if (col_idx < 0) return 1;
    if (schema->col_types[col_idx] != VEC_STRING) return 1;

    const Vtr1ColStat *st = &stats[col_idx];
    if (!st->has_stats) return 1;

    uint64_t batch_min = (uint64_t)st->i64.min;
    uint64_t batch_max = (uint64_t)st->i64.max;

    /* Check if any set value's prefix range overlaps with the batch range */
    for (int64_t i = 0; i < pred->n_set; i++) {
        if (!pred->set_str[i]) continue;
        int64_t slen = (int64_t)strlen(pred->set_str[i]);
        uint64_t val_lo = scan_pack_str(pred->set_str[i], slen, 0x00);
        uint64_t val_hi = scan_pack_str(pred->set_str[i], slen, 0xFF);
        /* Overlap: val_lo <= batch_max && val_hi >= batch_min */
        if (val_lo <= batch_max && val_hi >= batch_min)
            return 1; /* at least one value might be in this batch */
    }
    return 0; /* no set value can be in this batch */
}

/* Check if a simple comparison predicate can be pruned using row group stats.
 * Returns 0 if the predicate is definitely false for all rows in this row group,
 * 1 if it might be true (or we can't determine). rg_n_rows is the row count of
 * this group, used for all-null pruning against null_count. */
static int predicate_might_match(const VecExpr *pred, const Vtr1ColStat *stats,
                                  const VecSchema *schema, int64_t rg_n_rows) {
    if (!pred || !stats) return 1;

    /* Handle AND: both sides must possibly match */
    if (pred->kind == EXPR_BOOL && pred->op == '&') {
        return predicate_might_match(pred->left, stats, schema, rg_n_rows) &&
               predicate_might_match(pred->right, stats, schema, rg_n_rows);
    }

    /* Handle OR: at least one side must possibly match */
    if (pred->kind == EXPR_BOOL && pred->op == '|') {
        return predicate_might_match(pred->left, stats, schema, rg_n_rows) ||
               predicate_might_match(pred->right, stats, schema, rg_n_rows);
    }

    /* Handle %in% with string set */
    if (pred->kind == EXPR_IN && pred->set_str) {
        return str_in_might_match(pred, stats, schema);
    }

    /* Handle simple comparison: col <op> literal */
    if (pred->kind != EXPR_CMP) return 1;

    /* Check if one side is a col_ref and the other is a literal */
    const VecExpr *col_expr = NULL;
    const VecExpr *lit_expr = NULL;
    int col_is_left = 0;

    if (pred->left->kind == EXPR_COL_REF &&
        (pred->right->kind == EXPR_LIT_INT64 ||
         pred->right->kind == EXPR_LIT_DOUBLE ||
         pred->right->kind == EXPR_LIT_STRING)) {
        col_expr = pred->left;
        lit_expr = pred->right;
        col_is_left = 1;
    } else if (pred->right->kind == EXPR_COL_REF &&
               (pred->left->kind == EXPR_LIT_INT64 ||
                pred->left->kind == EXPR_LIT_DOUBLE ||
                pred->left->kind == EXPR_LIT_STRING)) {
        col_expr = pred->right;
        lit_expr = pred->left;
        col_is_left = 0;
    } else {
        return 1; /* can't prune complex expressions */
    }

    /* Find column index */
    int col_idx = vec_schema_find_col(schema, col_expr->col_name);
    if (col_idx < 0) return 1;

    const Vtr1ColStat *st = &stats[col_idx];

    /* All-null column: this comparison against a literal is NA for every row,
     * which filter drops, so no row in this group can pass. Sound for every
     * comparison operator. null_count is valid even when min/max are not
     * (has_stats == 0, e.g. a numeric column whose values were all NA). */
    if (rg_n_rows > 0 && st->null_count >= (uint64_t)rg_n_rows)
        return 0;

    if (!st->has_stats) return 1;

    VecType ct = schema->col_types[col_idx];

    /* --- String column zone-map pruning --- */
    if (ct == VEC_STRING && lit_expr->kind == EXPR_LIT_STRING) {
        if (!lit_expr->lit_str) return 1;
        int64_t slen = (int64_t)strlen(lit_expr->lit_str);
        uint64_t val_lo = scan_pack_str(lit_expr->lit_str, slen, 0x00);
        uint64_t val_hi = scan_pack_str(lit_expr->lit_str, slen, 0xFF);
        uint64_t batch_min = (uint64_t)st->i64.min;
        uint64_t batch_max = (uint64_t)st->i64.max;

        char op = pred->op;
        char op2 = pred->op2;
        if (!col_is_left) {
            if (op == '<') op = '>';
            else if (op == '>') op = '<';
        }

        /* Conservative checks using prefix ranges */
        if (op == '=' && op2 == '=')
            return val_lo <= batch_max && val_hi >= batch_min;
        if (op == '!' && op2 == '=')
            return !(batch_min == batch_max && batch_min == val_lo &&
                     batch_max == val_hi);
        /* For <, >, <=, >= on strings: conservative, just allow */
        return 1;
    }

    /* --- Numeric column pruning (existing logic) --- */

    /* Get literal value as double */
    double lit_val;
    if (lit_expr->kind == EXPR_LIT_DOUBLE)
        lit_val = lit_expr->lit_dbl;
    else if (lit_expr->kind == EXPR_LIT_INT64)
        lit_val = (double)lit_expr->lit_i64;
    else
        return 1; /* string literal but numeric column — can't prune */

    /* Get column min/max as double */
    double col_min, col_max;
    if (vec_type_is_int(ct)) {
        col_min = (double)st->i64.min;
        col_max = (double)st->i64.max;
    } else if (ct == VEC_DOUBLE) {
        col_min = st->dbl.min;
        col_max = st->dbl.max;
    } else {
        return 1; /* bool: can't prune */
    }

    char op = pred->op;
    char op2 = pred->op2;

    /* Flip operator if column is on the right: literal <op> column */
    if (!col_is_left) {
        if (op == '<') op = '>';
        else if (op == '>') op = '<';
        /* <= and >= flip similarly */
        if (op2 == '=') {
            /* already handled by the flip of op */
        }
        /* == and != are symmetric, no flip needed */
    }

    /* Now: col <op><op2> lit_val */
    if (op == '>' && op2 == ' ') return col_max > lit_val;
    if (op == '>' && op2 == '=') return col_max >= lit_val;
    if (op == '<' && op2 == ' ') return col_min < lit_val;
    if (op == '<' && op2 == '=') return col_min <= lit_val;
    if (op == '=' && op2 == '=') return col_min <= lit_val && col_max >= lit_val;
    if (op == '!' && op2 == '=') {
        /* Only prune if all values are the same and equal to lit_val */
        return !(col_min == lit_val && col_max == lit_val);
    }

    return 1; /* unknown op, don't prune */
}

/* Apply tombstone filter: build a selection vector that excludes deleted rows.
   Returns the batch unmodified if there are no deletions in this row group. */
static VecBatch *tombstone_filter_batch(VecBatch *batch,
                                        const TombstoneSet *ts,
                                        int64_t rg_row_base) {
    if (!ts || ts->n == 0) return batch;

    int64_t n = batch->n_rows;
    int32_t *sel = (int32_t *)malloc((size_t)n * sizeof(int32_t));
    if (!sel) vectra_error("tombstone_filter_batch: alloc failed");

    int32_t sel_n = 0;
    for (int64_t i = 0; i < n; i++) {
        if (!tombstone_is_deleted(ts, rg_row_base + i))
            sel[sel_n++] = (int32_t)i;
    }

    if (sel_n == (int32_t)n) {
        /* No rows deleted in this batch — discard sel and return as-is */
        free(sel);
        return batch;
    }

    batch->sel   = sel;
    batch->sel_n = sel_n;
    return batch;
}

/* ------------------------------------------------------------------ */
/*  Binary search on sorted row groups                                */
/* ------------------------------------------------------------------ */

/* Extract a simple predicate of the form col <op> literal.
   Returns 1 if extraction succeeded, 0 otherwise.
   Sets *col_idx (in file schema), *op, *op2, and literal value. */
static int extract_simple_pred(const VecExpr *pred, const VecSchema *schema,
                               int *col_idx, char *op, char *op2,
                               double *lit_dbl, int64_t *lit_i64,
                               const char **lit_str, int64_t *lit_str_len,
                               VecType *col_type) {
    if (!pred || pred->kind != EXPR_CMP) return 0;

    const VecExpr *col_expr = NULL;
    const VecExpr *lit_expr = NULL;
    int col_is_left = 0;

    if (pred->left && pred->left->kind == EXPR_COL_REF &&
        pred->right && (pred->right->kind == EXPR_LIT_INT64 ||
                        pred->right->kind == EXPR_LIT_DOUBLE ||
                        pred->right->kind == EXPR_LIT_STRING)) {
        col_expr = pred->left;
        lit_expr = pred->right;
        col_is_left = 1;
    } else if (pred->right && pred->right->kind == EXPR_COL_REF &&
               pred->left && (pred->left->kind == EXPR_LIT_INT64 ||
                              pred->left->kind == EXPR_LIT_DOUBLE ||
                              pred->left->kind == EXPR_LIT_STRING)) {
        col_expr = pred->right;
        lit_expr = pred->left;
        col_is_left = 0;
    } else {
        return 0;
    }

    int ci = vec_schema_find_col(schema, col_expr->col_name);
    if (ci < 0) return 0;
    *col_idx = ci;
    *col_type = schema->col_types[ci];

    *op = pred->op;
    *op2 = pred->op2;
    if (!col_is_left) {
        if (*op == '<') *op = '>';
        else if (*op == '>') *op = '<';
    }

    if (lit_expr->kind == EXPR_LIT_INT64) {
        *lit_i64 = lit_expr->lit_i64;
        *lit_dbl = (double)lit_expr->lit_i64;
    } else if (lit_expr->kind == EXPR_LIT_DOUBLE) {
        *lit_dbl = lit_expr->lit_dbl;
        *lit_i64 = (int64_t)lit_expr->lit_dbl;
        /* Integer column compared against a *fractional* double: the integer-space
           binary search uses *lit_i64 as a hard row-group cutoff. Truncation
           toward zero rounds the wrong way for half the operators (and for all
           fractional negatives), which can drop a row group that actually
           matches. Convert to the operator-appropriate integer bound so the
           search stays conservative. '==' is left truncated: no integer equals a
           fractional literal, so any resulting prune is safe (the filter re-checks). */
        if (vec_type_is_int(*col_type)) {
            double t  = lit_expr->lit_dbl;
            double fl = floor(t), ce = ceil(t);
            if (fl != ce) {
                if      (*op == '>' && *op2 == ' ') *lit_i64 = (int64_t)fl; /* x>t : skip max<=floor */
                else if (*op == '>' && *op2 == '=') *lit_i64 = (int64_t)ce; /* x>=t: skip max< ceil  */
                else if (*op == '<' && *op2 == ' ') *lit_i64 = (int64_t)ce; /* x<t : skip min>=ceil  */
                else if (*op == '<' && *op2 == '=') *lit_i64 = (int64_t)fl; /* x<=t: skip min> floor */
            }
        }
    } else if (lit_expr->kind == EXPR_LIT_STRING) {
        *lit_str = lit_expr->lit_str;
        *lit_str_len = lit_expr->lit_str ? (int64_t)strlen(lit_expr->lit_str) : 0;
    }
    return 1;
}

/* Binary search for first row group where max >= val (for ==, >=, > predicates)
   and last row group where min <= val (for ==, <= predicates).
   Operates on the row group stats array for a sorted column.
   Sets *first_rg and *last_rg (exclusive upper bound). */
static void binary_search_rg_range(const Vtr1TdcFile *file, int col_idx,
                                    char op, char op2, VecType col_type,
                                    double lit_dbl, int64_t lit_i64,
                                    const char *lit_str, int64_t lit_str_len,
                                    uint32_t *first_rg, uint32_t *last_rg) {
    uint32_t n_rgs = vtr1_tdc_n_rowgroups(file);
    *first_rg = 0;
    *last_rg = n_rgs;

    if (n_rgs == 0) return;

    /* For string columns, pack the literal for comparison */
    uint64_t str_lo = 0, str_hi = 0;
    if (col_type == VEC_STRING && lit_str) {
        str_lo = scan_pack_str(lit_str, lit_str_len, 0x00);
        str_hi = scan_pack_str(lit_str, lit_str_len, 0xFF);
    }

    /* Helper macros to get stat values as comparable scalars.
     * vtr1_tdc_rowgroup_col_stats may return NULL for zero-row groups,
     * but binary_search_rg_range is only entered when col_sorted[c]==1
     * which requires every rg to have stats — safe to deref. */
    #define RG_STATS(rg) vtr1_tdc_rowgroup_col_stats(file, rg)[col_idx]
    #define RG_MIN_I64(rg) RG_STATS(rg).i64.min
    #define RG_MAX_I64(rg) RG_STATS(rg).i64.max
    #define RG_MIN_DBL(rg) RG_STATS(rg).dbl.min
    #define RG_MAX_DBL(rg) RG_STATS(rg).dbl.max

    if (op == '=' && op2 == '=') {
        /* Equality: find first rg where max >= lit, last rg where min <= lit */
        uint32_t lo = 0, hi = n_rgs;
        /* Binary search for first_rg: smallest rg where max >= literal */
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            int max_lt_lit;
            if (vec_type_is_int(col_type)) {
                max_lt_lit = RG_MAX_I64(mid) < lit_i64;
            } else if (col_type == VEC_DOUBLE) {
                max_lt_lit = RG_MAX_DBL(mid) < lit_dbl;
            } else { /* VEC_STRING */
                max_lt_lit = (uint64_t)RG_MAX_I64(mid) < str_lo;
            }
            if (max_lt_lit)
                lo = mid + 1;
            else
                hi = mid;
        }
        *first_rg = lo;

        /* Binary search for last_rg: smallest rg where min > literal */
        lo = *first_rg;
        hi = n_rgs;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            int min_gt_lit;
            if (vec_type_is_int(col_type)) {
                min_gt_lit = RG_MIN_I64(mid) > lit_i64;
            } else if (col_type == VEC_DOUBLE) {
                min_gt_lit = RG_MIN_DBL(mid) > lit_dbl;
            } else { /* VEC_STRING */
                min_gt_lit = (uint64_t)RG_MIN_I64(mid) > str_hi;
            }
            if (min_gt_lit)
                hi = mid;
            else
                lo = mid + 1;
        }
        *last_rg = lo;
    } else if ((op == '>' && op2 == ' ') || (op == '>' && op2 == '=')) {
        /* col > lit or col >= lit: find first rg where max >= lit (or > lit) */
        uint32_t lo = 0, hi = n_rgs;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            int skip;
            if (vec_type_is_int(col_type)) {
                skip = (op2 == '=') ? (RG_MAX_I64(mid) < lit_i64)
                                    : (RG_MAX_I64(mid) <= lit_i64);
            } else if (col_type == VEC_DOUBLE) {
                skip = (op2 == '=') ? (RG_MAX_DBL(mid) < lit_dbl)
                                    : (RG_MAX_DBL(mid) <= lit_dbl);
            } else {
                skip = (op2 == '=') ? ((uint64_t)RG_MAX_I64(mid) < str_lo)
                                    : ((uint64_t)RG_MAX_I64(mid) <= str_hi);
            }
            if (skip)
                lo = mid + 1;
            else
                hi = mid;
        }
        *first_rg = lo;
        /* last_rg stays at n_rgs — all subsequent row groups could match */
    } else if ((op == '<' && op2 == ' ') || (op == '<' && op2 == '=')) {
        /* col < lit or col <= lit: find last rg where min <= lit (or < lit) */
        uint32_t lo = 0, hi = n_rgs;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            int skip;
            if (vec_type_is_int(col_type)) {
                skip = (op2 == '=') ? (RG_MIN_I64(mid) > lit_i64)
                                    : (RG_MIN_I64(mid) >= lit_i64);
            } else if (col_type == VEC_DOUBLE) {
                skip = (op2 == '=') ? (RG_MIN_DBL(mid) > lit_dbl)
                                    : (RG_MIN_DBL(mid) >= lit_dbl);
            } else {
                skip = (op2 == '=') ? ((uint64_t)RG_MIN_I64(mid) > str_hi)
                                    : ((uint64_t)RG_MIN_I64(mid) >= str_lo);
            }
            if (skip)
                hi = mid;
            else
                lo = mid + 1;
        }
        *last_rg = lo;
        /* first_rg stays at 0 — all preceding row groups could match */
    }
    /* != doesn't benefit from binary search — leave full range */

    #undef RG_STATS
    #undef RG_MIN_I64
    #undef RG_MAX_I64
    #undef RG_MIN_DBL
    #undef RG_MAX_DBL
}

/* The store's shape, as an index records it at build time. An index whose stamp
   disagrees describes row groups that have since moved. */
static void scan_store_stamp(const ScanNode *sn, int64_t *n_rows,
                             int64_t *n_rowgroups) {
    uint32_t n_rg = vtr1_tdc_n_rowgroups(sn->file);
    int64_t total = 0;
    for (uint32_t rg = 0; rg < n_rg; rg++)
        total += vtr1_tdc_rowgroup_n_rows(sn->file, rg);
    *n_rows = total;
    *n_rowgroups = (int64_t)n_rg;
}

/* Open the single-column .vtri sidecar for one column, if it is usable.
   Opening is deferred to here rather than done when the scan is built, so a
   query pays for the index it filters on and no others -- and so filtering on
   the second indexed column of a store reaches that column's index. */
static VtrIndex *scan_open_col_index(ScanNode *sn, const char *col_name,
                                     int col_idx) {
    if (!sn->vtr_path) return NULL;
    char *vtri_path = vtri_make_path(sn->vtr_path, col_name);
    if (!vtri_path) return NULL;
    int64_t n_rows, n_rgs;
    scan_store_stamp(sn, &n_rows, &n_rgs);
    VtrIndex *idx = vtri_open(vtri_path, vtr1_tdc_schema(sn->file), n_rows, n_rgs);
    free(vtri_path);
    if (!idx) return NULL;
    if (idx->n_cols != 1 || (int)idx->col_idx != col_idx) {
        vtri_close(idx);
        return NULL;
    }
    return idx;
}

/* The equality or %in% predicate an index could be probed with, if any. Shared
   by the probe itself and by the plan description, so what explain() reports and
   what the scan does cannot drift apart. */
static void scan_index_preds(const ScanNode *sn, const VecExpr **out_eq,
                             const VecExpr **out_in) {
    *out_eq = NULL;
    *out_in = NULL;
    const VecExpr *pred = sn->predicate;
    if (!pred) return;

    /* Walk through AND chain looking for an equality predicate */
    const VecExpr *eq_pred = NULL;
    if (pred->kind == EXPR_CMP && pred->op == '=' && pred->op2 == '=') {
        eq_pred = pred;
    } else if (pred->kind == EXPR_BOOL && pred->op == '&') {
        if (pred->left && pred->left->kind == EXPR_CMP &&
            pred->left->op == '=' && pred->left->op2 == '=')
            eq_pred = pred->left;
        else if (pred->right && pred->right->kind == EXPR_CMP &&
                 pred->right->op == '=' && pred->right->op2 == '=')
            eq_pred = pred->right;
    }

    /* Also look for a %in% predicate */
    const VecExpr *in_pred = NULL;
    if (!eq_pred) {
        if (pred->kind == EXPR_IN) {
            in_pred = pred;
        } else if (pred->kind == EXPR_BOOL && pred->op == '&') {
            if (pred->left && pred->left->kind == EXPR_IN)
                in_pred = pred->left;
            else if (pred->right && pred->right->kind == EXPR_IN)
                in_pred = pred->right;
        }
    }

    *out_eq = eq_pred;
    *out_in = in_pred;
}

/* The column whose single-column index this scan can probe, or -1. */
static int scan_index_col(const ScanNode *sn) {
    const VecExpr *eq_pred, *in_pred;
    scan_index_preds(sn, &eq_pred, &in_pred);
    const VecExpr *col_expr = NULL;
    if (in_pred) {
        col_expr = in_pred->operand;
    } else if (eq_pred) {
        if (eq_pred->left && eq_pred->left->kind == EXPR_COL_REF && eq_pred->right)
            col_expr = eq_pred->left;
        else if (eq_pred->right && eq_pred->right->kind == EXPR_COL_REF && eq_pred->left)
            col_expr = eq_pred->right;
    }
    if (!col_expr || col_expr->kind != EXPR_COL_REF) return -1;
    return vec_schema_find_col(vtr1_tdc_schema(sn->file), col_expr->col_name);
}

/* Try to use the hash index to build a row-group bitmap for equality predicates. */
static void try_hash_index(ScanNode *sn) {
    if (!sn->predicate) return;

    const VecSchema *schema = vtr1_tdc_schema(sn->file);
    const VecExpr *eq_pred, *in_pred;
    scan_index_preds(sn, &eq_pred, &in_pred);

    /* Handle %in% with index: probe each set value, OR bitmaps */
    if (in_pred) {
        if (!in_pred->operand || in_pred->operand->kind != EXPR_COL_REF) return;
        int ci = vec_schema_find_col(schema, in_pred->operand->col_name);
        if (ci < 0) return;
        sn->index = scan_open_col_index(sn, schema->col_names[ci], ci);
        if (!sn->index) return;

        uint32_t n_rgs = vtr1_tdc_n_rowgroups(sn->file);
        uint8_t *combined = (uint8_t *)calloc(n_rgs, 1);
        if (!combined) return;
        VecType idx_col_type = schema->col_types[ci];

        for (int64_t s = 0; s < in_pred->n_set; s++) {
            uint8_t *bm = NULL;
            if (idx_col_type == VEC_STRING && in_pred->set_str) {
                bm = vtri_probe_string(sn->index, in_pred->set_str[s],
                                       (int64_t)strlen(in_pred->set_str[s]), n_rgs);
            } else if (vec_type_is_int(idx_col_type) && in_pred->set_i64) {
                bm = vtri_probe_int64(sn->index, in_pred->set_i64[s], n_rgs);
            } else if (idx_col_type == VEC_DOUBLE && in_pred->set_dbl) {
                bm = vtri_probe_double(sn->index, in_pred->set_dbl[s], n_rgs);
            }
            if (bm) {
                for (uint32_t r = 0; r < n_rgs; r++) combined[r] |= bm[r];
                free(bm);
            }
        }
        sn->rg_bitmap = combined;
        return;
    }

    if (!eq_pred) return;

    /* Extract col_ref and literal */
    const VecExpr *col_expr = NULL, *lit_expr = NULL;
    if (eq_pred->left && eq_pred->left->kind == EXPR_COL_REF &&
        eq_pred->right) {
        col_expr = eq_pred->left;
        lit_expr = eq_pred->right;
    } else if (eq_pred->right && eq_pred->right->kind == EXPR_COL_REF &&
               eq_pred->left) {
        col_expr = eq_pred->right;
        lit_expr = eq_pred->left;
    }
    if (!col_expr || !lit_expr) return;

    /* Open this column's index, if it has one */
    int ci = vec_schema_find_col(schema, col_expr->col_name);
    if (ci < 0) return;
    sn->index = scan_open_col_index(sn, schema->col_names[ci], ci);
    if (!sn->index) return;

    uint32_t n_rgs = vtr1_tdc_n_rowgroups(sn->file);
    uint8_t *bitmap = NULL;

    /* Probe must hash with the same type the index was built with.
       The column type determines which hash function to use, not the
       literal type (R's 250 is EXPR_LIT_DOUBLE but the column is VEC_INT64). */
    VecType idx_col_type = schema->col_types[ci];

    if (idx_col_type == VEC_STRING && lit_expr->lit_str) {
        bitmap = vtri_probe_string(sn->index, lit_expr->lit_str,
                                   (int64_t)strlen(lit_expr->lit_str), n_rgs);
    } else if (vec_type_is_int(idx_col_type)) {
        int64_t key;
        if (lit_expr->kind == EXPR_LIT_INT64)
            key = lit_expr->lit_i64;
        else if (lit_expr->kind == EXPR_LIT_DOUBLE)
            key = (int64_t)lit_expr->lit_dbl;
        else
            goto skip_probe;
        bitmap = vtri_probe_int64(sn->index, key, n_rgs);
    } else if (idx_col_type == VEC_DOUBLE) {
        double key;
        if (lit_expr->kind == EXPR_LIT_DOUBLE)
            key = lit_expr->lit_dbl;
        else if (lit_expr->kind == EXPR_LIT_INT64)
            key = (double)lit_expr->lit_i64;
        else
            goto skip_probe;
        bitmap = vtri_probe_double(sn->index, key, n_rgs);
    }
    skip_probe:

    if (bitmap)
        sn->rg_bitmap = bitmap;
}

#define MAX_COMPOSITE_COLS VTRI_MAX_COLS

/* Every equality predicate in a flat AND chain -- (A & B), (A & (B & C)), ... --
   sorted into schema column order, which is the order create_index() names and
   hashes its columns in. Returns how many were found. */
static int scan_eq_chain(const ScanNode *sn, const char **col_names,
                        const VecExpr **lit_exprs, int *col_idxs) {
    const VecExpr *pred = sn->predicate;
    const VecSchema *schema = vtr1_tdc_schema(sn->file);
    int n_eq = 0;

    const VecExpr *stack[MAX_COMPOSITE_COLS * 2];
    int sp = 0;
    stack[sp++] = pred;
    while (sp > 0 && n_eq < MAX_COMPOSITE_COLS) {
        const VecExpr *p = stack[--sp];
        if (p->kind == EXPR_BOOL && p->op == '&') {
            if (p->right && sp < MAX_COMPOSITE_COLS * 2) stack[sp++] = p->right;
            if (p->left && sp < MAX_COMPOSITE_COLS * 2) stack[sp++] = p->left;
        } else if (p->kind == EXPR_CMP && p->op == '=' && p->op2 == '=') {
            const VecExpr *col_e = NULL, *lit_e = NULL;
            if (p->left && p->left->kind == EXPR_COL_REF && p->right) {
                col_e = p->left; lit_e = p->right;
            } else if (p->right && p->right->kind == EXPR_COL_REF && p->left) {
                col_e = p->right; lit_e = p->left;
            }
            if (col_e && lit_e) {
                int ci = vec_schema_find_col(schema, col_e->col_name);
                if (ci >= 0) {
                    col_names[n_eq] = col_e->col_name;
                    lit_exprs[n_eq] = lit_e;
                    col_idxs[n_eq] = ci;
                    n_eq++;
                }
            }
        }
    }

    for (int i = 0; i < n_eq - 1; i++)
        for (int j = i + 1; j < n_eq; j++)
            if (col_idxs[i] > col_idxs[j]) {
                int ti = col_idxs[i]; col_idxs[i] = col_idxs[j]; col_idxs[j] = ti;
                const char *tn = col_names[i]; col_names[i] = col_names[j]; col_names[j] = tn;
                const VecExpr *te = lit_exprs[i]; lit_exprs[i] = lit_exprs[j]; lit_exprs[j] = te;
            }
    return n_eq;
}

/* Open the composite .vtri covering exactly the columns of an AND chain, if
   there is one. */
static VtrIndex *scan_open_composite_index(ScanNode *sn, const char **col_names,
                                           int n_eq) {
    if (n_eq < 2 || !sn->vtr_path) return NULL;
    char *vtri_path = vtri_make_path_composite(sn->vtr_path, col_names, n_eq);
    if (!vtri_path) return NULL;
    int64_t n_rows, n_rgs;
    scan_store_stamp(sn, &n_rows, &n_rgs);
    VtrIndex *idx = vtri_open(vtri_path, vtr1_tdc_schema(sn->file), n_rows, n_rgs);
    free(vtri_path);
    if (idx && idx->n_cols != (uint16_t)n_eq) {
        vtri_close(idx);
        return NULL;
    }
    return idx;
}

/* Try composite index: collect all equality predicates from AND chain,
   check if a composite .vtri exists, and probe it. */
static void try_composite_index(ScanNode *sn) {
    if (!sn->predicate || !sn->vtr_path) return;
    if (sn->rg_bitmap) return; /* already resolved */

    const VecSchema *schema = vtr1_tdc_schema(sn->file);

    const char *col_names[MAX_COMPOSITE_COLS];
    const VecExpr *lit_exprs[MAX_COMPOSITE_COLS];
    int col_idxs[MAX_COMPOSITE_COLS];
    int n_eq = scan_eq_chain(sn, col_names, lit_exprs, col_idxs);

    if (n_eq < 2) return; /* need at least 2 columns for composite */

    VtrIndex *cidx = scan_open_composite_index(sn, col_names, n_eq);
    if (!cidx) return;

    /* Compute per-column hashes from literal values */
    uint64_t col_hashes[MAX_COMPOSITE_COLS];
    for (int c = 0; c < n_eq; c++) {
        VecType ct = schema->col_types[col_idxs[c]];
        const VecExpr *le = lit_exprs[c];
        if (ct == VEC_STRING && le->lit_str) {
            col_hashes[c] = cidx->ci
                ? vtri_fnv1a_ci(le->lit_str, (int64_t)strlen(le->lit_str))
                : vtri_fnv1a((const uint8_t *)le->lit_str, (int64_t)strlen(le->lit_str));
        } else if (vec_type_is_int(ct)) {
            int64_t key = le->kind == EXPR_LIT_INT64 ? le->lit_i64 : (int64_t)le->lit_dbl;
            col_hashes[c] = vtri_hash_int64(key);
        } else if (ct == VEC_DOUBLE) {
            double key = le->kind == EXPR_LIT_DOUBLE ? le->lit_dbl : (double)le->lit_i64;
            col_hashes[c] = vtri_hash_double(key);
        } else {
            vtri_close(cidx);
            return;
        }
    }

    uint8_t *bitmap = vtri_probe_composite(cidx, col_hashes, n_eq,
                                            vtr1_tdc_n_rowgroups(sn->file));
    vtri_close(cidx);
    if (bitmap) sn->rg_bitmap = bitmap;
}

/* Try to narrow the row group scan range using binary search on sorted columns.
   Called once on the first scan_next_batch() invocation. */
static void try_binary_search(ScanNode *sn) {
    sn->rg_range_set = 1;  /* mark as attempted */

    /* A composite index covering every equality in the predicate prunes at least
       as well as a single-column index on one of those columns, since it encodes
       the keys co-occurring in a row group. Try it first; fall back to a
       single-column index when there is no composite for exactly these columns. */
    try_composite_index(sn);
    if (!sn->rg_bitmap) try_hash_index(sn);

    const uint8_t *col_sorted = vtr1_tdc_col_sorted(sn->file);
    if (!sn->predicate || !col_sorted) return;
    uint32_t n_rgs = vtr1_tdc_n_rowgroups(sn->file);
    if (n_rgs <= 1) return;
    const VecSchema *schema = vtr1_tdc_schema(sn->file);

    int col_idx;
    char op, op2;
    double lit_dbl = 0;
    int64_t lit_i64 = 0;
    const char *lit_str = NULL;
    int64_t lit_str_len = 0;
    VecType col_type;

    /* For AND predicates, try to extract and intersect ranges from both sides */
    if (sn->predicate->kind == EXPR_BOOL && sn->predicate->op == '&') {
        /* Try left side */
        uint32_t l_first = 0, l_last = n_rgs;
        if (sn->predicate->left &&
            extract_simple_pred(sn->predicate->left, schema,
                                &col_idx, &op, &op2, &lit_dbl, &lit_i64,
                                &lit_str, &lit_str_len, &col_type) &&
            col_sorted[col_idx]) {
            binary_search_rg_range(sn->file, col_idx, op, op2, col_type,
                                   lit_dbl, lit_i64, lit_str, lit_str_len,
                                   &l_first, &l_last);
        }
        /* Try right side */
        uint32_t r_first = 0, r_last = n_rgs;
        if (sn->predicate->right &&
            extract_simple_pred(sn->predicate->right, schema,
                                &col_idx, &op, &op2, &lit_dbl, &lit_i64,
                                &lit_str, &lit_str_len, &col_type) &&
            col_sorted[col_idx]) {
            binary_search_rg_range(sn->file, col_idx, op, op2, col_type,
                                   lit_dbl, lit_i64, lit_str, lit_str_len,
                                   &r_first, &r_last);
        }
        /* Intersect */
        uint32_t first = l_first > r_first ? l_first : r_first;
        uint32_t last  = l_last < r_last ? l_last : r_last;
        if (first < last && (first > 0 || last < n_rgs)) {
            sn->next_rg = first;
            sn->last_rg = last;
            sn->last_rg_set = 1;
            /* Compute rg_row_base for the starting position */
            sn->rg_row_base = 0;
            for (uint32_t rg = 0; rg < first; rg++)
                sn->rg_row_base += vtr1_tdc_rowgroup_n_rows(sn->file, rg);
        }
        return;
    }

    /* Single predicate */
    if (!extract_simple_pred(sn->predicate, schema,
                             &col_idx, &op, &op2, &lit_dbl, &lit_i64,
                             &lit_str, &lit_str_len, &col_type))
        return;
    if (!col_sorted[col_idx]) return;

    uint32_t first_rg, last_rg;
    binary_search_rg_range(sn->file, col_idx, op, op2, col_type,
                           lit_dbl, lit_i64, lit_str, lit_str_len,
                           &first_rg, &last_rg);

    if (first_rg > 0 || last_rg < n_rgs) {
        sn->next_rg = first_rg;
        sn->last_rg = last_rg;
        sn->last_rg_set = 1;   /* last_rg == 0 (prune everything) is authoritative */
        /* Compute rg_row_base for the starting position */
        sn->rg_row_base = 0;
        for (uint32_t rg = 0; rg < first_rg; rg++)
            sn->rg_row_base += vtr1_tdc_rowgroup_n_rows(sn->file, rg);
    }
}

static VecBatch *scan_next_batch(VecNode *self) {
    ScanNode *sn = (ScanNode *)self;

    /* On first call, try binary search and hash index to narrow range */
    if (!sn->rg_range_set) {
        try_binary_search(sn);
    }

    uint32_t rg_limit = sn->last_rg_set ? sn->last_rg
                                        : vtr1_tdc_n_rowgroups(sn->file);

    while (sn->next_rg < rg_limit) {
        /* Track the physical row base for tombstone checking */
        int64_t rg_base = sn->rg_row_base;
        int64_t rg_n_rows = vtr1_tdc_rowgroup_n_rows(sn->file, sn->next_rg);

        /* Hash index bitmap: skip row groups not in the probe result */
        if (sn->rg_bitmap && !sn->rg_bitmap[sn->next_rg]) {
            sn->next_rg++;
            sn->rg_row_base += rg_n_rows;
            continue;
        }

        /* Predicate pushdown: skip row groups that can't match */
        if (sn->predicate) {
            const Vtr1ColStat *stats =
                vtr1_tdc_rowgroup_col_stats(sn->file, sn->next_rg);
            if (stats && !predicate_might_match(sn->predicate, stats,
                                                 vtr1_tdc_schema(sn->file),
                                                 rg_n_rows)) {
                sn->next_rg++;
                sn->rg_row_base += rg_n_rows;
                continue;
            }
        }

        VecBatch *batch = vtr1_read_rowgroup_tdc(sn->file, sn->next_rg,
                                                  sn->col_mask);
        sn->next_rg++;
        sn->rg_row_base += rg_n_rows;

        /* Apply tombstone filter if needed */
        batch = tombstone_filter_batch(batch, sn->tombstone, rg_base);

        /* If all rows in this batch were deleted, skip to next row group
           instead of returning an empty batch */
        if (batch->sel && batch->sel_n == 0) {
            vec_batch_free(batch);
            continue;
        }

        return batch;
    }
    return NULL;
}

/* Rows this scan will emit, from the row-group index alone. Anything that
   makes the scan emit a subset of the stored rows has to run to be counted:
   a pushed-down predicate, index or zone-map pruning, a binary-search-narrowed
   row-group range, or a scan already part-way through its row groups.
   Deletions are the one exception - the tombstone names exactly which stored
   rows are gone, so they subtract, as long as every index it names is in
   range (a stale .del must not produce a negative count). */
static int64_t scan_static_rows(const VecNode *self) {
    const ScanNode *sn = (const ScanNode *)self;
    if (sn->predicate || sn->rg_bitmap || sn->rg_range_set ||
        sn->last_rg_set || sn->next_rg != 0)
        return -1;

    int64_t total = sn->base.row_count_hint;   /* sum over row groups */
    if (total < 0) return -1;

    const TombstoneSet *ts = sn->tombstone;
    if (ts && ts->n > 0) {
        if (ts->rows[0] < 0 || ts->rows[ts->n - 1] >= total) return -1;
        total -= ts->n;
    }
    return total;
}

static void scan_free(VecNode *self) {
    ScanNode *sn = (ScanNode *)self;
    if (sn->predicate && !sn->pred_borrowed)
        vec_expr_free(sn->predicate);
    tombstone_free(sn->tombstone);
    vtr1_close_tdc(sn->file);
    free(sn->col_mask);
    free(sn->rg_bitmap);
    if (sn->index) vtri_close(sn->index);
    if (sn->delete_on_free && sn->vtr_path) remove(sn->vtr_path);
    free(sn->vtr_path);
    vec_schema_free(&sn->base.output_schema);
    free(sn);
}

ScanNode *scan_node_create(const char *path, int *col_indices, int n_selected) {
    ScanNode *sn = (ScanNode *)calloc(1, sizeof(ScanNode));
    if (!sn) vectra_error("alloc failed for ScanNode");

    sn->file = vtr1_open_tdc(path);
    if (!sn->file) vectra_error("vtr1_open_tdc failed for %s", path);
    sn->next_rg = 0;
    sn->rg_row_base = 0;
    sn->vtr_path = (char *)malloc(strlen(path) + 1);
    if (sn->vtr_path) strcpy(sn->vtr_path, path);

    /* Check for tombstone file: "<path>.del" */
    size_t plen = strlen(path);
    char *del_path = (char *)malloc(plen + 5);
    if (!del_path) vectra_error("alloc failed for del_path");
    memcpy(del_path, path, plen);
    memcpy(del_path + plen, ".del", 5);
    sn->tombstone = tombstone_load(del_path);
    free(del_path);

    const VecSchema *file_schema = vtr1_tdc_schema(sn->file);
    int n_cols = file_schema->n_cols;

    /* Build column mask */
    sn->col_mask = (int *)calloc((size_t)n_cols, sizeof(int));
    if (!sn->col_mask) vectra_error("alloc failed for col_mask");

    if (!col_indices) {
        /* Select all */
        for (int i = 0; i < n_cols; i++) sn->col_mask[i] = 1;
        sn->base.output_schema = vec_schema_copy(file_schema);
    } else {
        for (int i = 0; i < n_selected; i++) {
            if (col_indices[i] < 0 || col_indices[i] >= n_cols)
                vectra_error("column index out of range: %d", col_indices[i]);
            sn->col_mask[col_indices[i]] = 1;
        }
        /* Build output schema with only selected columns */
        char **sel_names = (char **)malloc((size_t)n_selected * sizeof(char *));
        VecType *sel_types = (VecType *)malloc((size_t)n_selected * sizeof(VecType));
        int j = 0;
        for (int i = 0; i < n_cols; i++) {
            if (sn->col_mask[i]) {
                sel_names[j] = file_schema->col_names[i];
                sel_types[j] = file_schema->col_types[i];
                j++;
            }
        }
        sn->base.output_schema = vec_schema_create(n_selected, sel_names, sel_types);
        free(sel_names);
        free(sel_types);
    }

    /* A .vtri sidecar is opened on the first batch, for the column the
       predicate filters on (try_hash_index). */
    sn->index = NULL;

    sn->base.next_batch = scan_next_batch;
    sn->base.free_node = scan_free;
    sn->base.static_rows = scan_static_rows;
    sn->base.kind = "ScanNode";

    /* Compute total row count hint from row group metadata */
    int64_t total = 0;
    uint32_t n_rg = vtr1_tdc_n_rowgroups(sn->file);
    for (uint32_t rg = 0; rg < n_rg; rg++)
        total += vtr1_tdc_rowgroup_n_rows(sn->file, rg);
    sn->base.row_count_hint = total;

    return sn;
}

/* --- Parallel I/O accessors --- */

int scan_node_is_parallel_safe(const VecNode *node) {
    if (!node || strcmp(node->kind, "ScanNode") != 0) return 0;
    const ScanNode *sn = (const ScanNode *)node;
    if (sn->predicate) return 0;
    if (sn->tombstone) return 0;
    if (sn->rg_bitmap) return 0;
    if (sn->rg_range_set) return 0;
    if (vtr1_tdc_n_rowgroups(sn->file) < 4) return 0; /* need enough RGs */
    return 1;
}

const char *scan_node_get_path(const VecNode *node) {
    return ((const ScanNode *)node)->vtr_path;
}

/* Describes the .vtri index this scan's predicate will be probed against, for
   explain(): writes the indexed column names into buf ("id", or "a + b" for a
   composite) and returns 1, or returns 0 when no index applies. Answers by
   opening the same index the scan would, so it reports what will happen rather
   than only that a sidecar file exists. */
int scan_node_index_desc(const VecNode *node, char *buf, int bufsize) {
    if (!buf || bufsize < 1) return 0;
    buf[0] = '\0';
    if (!node || !node->kind || strcmp(node->kind, "ScanNode") != 0) return 0;
    ScanNode *sn = (ScanNode *)node;
    if (!sn->predicate) return 0;
    const VecSchema *schema = vtr1_tdc_schema(sn->file);

    /* Same order of preference as try_binary_search, so what is reported is what
       will be probed. */
    const char *col_names[MAX_COMPOSITE_COLS];
    const VecExpr *lit_exprs[MAX_COMPOSITE_COLS];
    int col_idxs[MAX_COMPOSITE_COLS];
    int n_eq = scan_eq_chain(sn, col_names, lit_exprs, col_idxs);
    VtrIndex *cidx = scan_open_composite_index(sn, col_names, n_eq);
    if (cidx) {
        vtri_close(cidx);
        /* snprintf reports the length it would have written, so clamp rather
           than letting pos run past the end of the buffer. */
        int pos = 0;
        for (int c = 0; c < n_eq && pos < bufsize - 1; c++) {
            int n = snprintf(buf + pos, (size_t)(bufsize - pos), "%s%s",
                             c ? " + " : "", col_names[c]);
            if (n < 0) break;
            pos += n;
            if (pos > bufsize - 1) pos = bufsize - 1;
        }
        return 1;
    }

    int ci = scan_index_col(sn);
    if (ci >= 0) {
        VtrIndex *idx = scan_open_col_index(sn, schema->col_names[ci], ci);
        if (idx) {
            vtri_close(idx);
            snprintf(buf, (size_t)bufsize, "%s", schema->col_names[ci]);
            return 1;
        }
    }
    return 0;
}

Vtr1TdcFile *scan_node_get_file(const VecNode *node) {
    return ((const ScanNode *)node)->file;
}

const int *scan_node_get_col_mask(const VecNode *node) {
    return ((const ScanNode *)node)->col_mask;
}
