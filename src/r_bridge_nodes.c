#include "r_bridge.h"
#include "r_bridge_internal.h"
#include "types.h"
#include "schema.h"
#include "filter.h"
#include "project.h"
#include "group_agg.h"
#include "sort.h"
#include "topn.h"
#include "group_topn.h"
#include "limit.h"
#include "join.h"
#include "fuzzy_join.h"
#include "interval_join.h"
#include "window.h"
#include "concat.h"
#include "expr.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* --- C_filter_node --- */

SEXP C_filter_node(SEXP node_xptr, SEXP expr_list) {
    VecNode *child = unwrap_node(node_xptr);
    /* Clear the external pointer so the old R object can't double-free */
    R_ClearExternalPtr(node_xptr);

    VecExpr *pred = parse_expr(expr_list, &child->output_schema);
    FilterNode *fn = filter_node_create(child, pred);
    return wrap_node((VecNode *)fn);
}

/* --- C_project_node --- */

SEXP C_project_node(SEXP node_xptr, SEXP names, SEXP expr_lists) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;

    /* Build a mutable schema that we extend as mutate entries are parsed,
       so later entries can reference earlier ones */
    int n = Rf_length(names);
    int schema_n = schema->n_cols;
    int schema_cap = schema_n + n;
    char **ext_names = (char **)malloc((size_t)schema_cap * sizeof(char *));
    VecType *ext_types = (VecType *)malloc((size_t)schema_cap * sizeof(VecType));
    for (int i = 0; i < schema_n; i++) {
        ext_names[i] = schema->col_names[i];
        ext_types[i] = schema->col_types[i];
    }
    VecSchema ext_schema;
    memset(&ext_schema, 0, sizeof(ext_schema));
    ext_schema.n_cols = schema_n;
    ext_schema.col_names = ext_names;
    ext_schema.col_types = ext_types;

    ProjEntry *entries = (ProjEntry *)calloc((size_t)n, sizeof(ProjEntry));
    if (!entries) vectra_error("alloc failed for ProjEntry");

    for (int i = 0; i < n; i++) {
        const char *nm = CHAR(STRING_ELT(names, i));
        entries[i].output_name = (char *)malloc(strlen(nm) + 1);
        strcpy(entries[i].output_name, nm);

        SEXP expr = VECTOR_ELT(expr_lists, i);
        if (expr == R_NilValue) {
            entries[i].expr = NULL; /* pass-through */
        } else {
            entries[i].expr = parse_expr(expr, &ext_schema);
            /* Add this entry to the extended schema so later entries
               can reference it */
            ext_names[ext_schema.n_cols] = entries[i].output_name;
            ext_types[ext_schema.n_cols] = entries[i].expr->result_type;
            ext_schema.n_cols++;
        }
    }

    free(ext_names);
    free(ext_types);

    ProjectNode *pn = project_node_create(child, n, entries);
    return wrap_node((VecNode *)pn);
}

/* --- C_group_agg_node --- */

static AggKind parse_agg_kind(const char *s) {
    if (strcmp(s, "n") == 0 || strcmp(s, "count_star") == 0) return AGG_COUNT_STAR;
    if (strcmp(s, "count") == 0) return AGG_COUNT;
    if (strcmp(s, "sum") == 0) return AGG_SUM;
    if (strcmp(s, "mean") == 0) return AGG_MEAN;
    if (strcmp(s, "min") == 0) return AGG_MIN;
    if (strcmp(s, "max") == 0) return AGG_MAX;
    if (strcmp(s, "var") == 0) return AGG_VAR;
    if (strcmp(s, "sd") == 0) return AGG_SD;
    if (strcmp(s, "first") == 0) return AGG_FIRST;
    if (strcmp(s, "last") == 0) return AGG_LAST;
    if (strcmp(s, "any") == 0) return AGG_ANY;
    if (strcmp(s, "all") == 0) return AGG_ALL;
    if (strcmp(s, "n_distinct") == 0) return AGG_N_DISTINCT;
    if (strcmp(s, "median") == 0) return AGG_MEDIAN;
    vectra_error("unknown aggregation function: %s", s);
    return AGG_COUNT; /* unreachable */
}

SEXP C_group_agg_node(SEXP node_xptr, SEXP key_names_sexp, SEXP agg_specs_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    /* Parse key names */
    int n_keys = Rf_length(key_names_sexp);
    char **key_names = (char **)malloc((size_t)n_keys * sizeof(char *));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(key_names_sexp, k));
        key_names[k] = (char *)malloc(strlen(nm) + 1);
        strcpy(key_names[k], nm);
    }

    /* Parse agg specs: list of lists with $name, $kind, $col, $na_rm */
    int n_aggs = Rf_length(agg_specs_sexp);
    AggSpec *specs = (AggSpec *)calloc((size_t)n_aggs, sizeof(AggSpec));
    for (int a = 0; a < n_aggs; a++) {
        SEXP spec = VECTOR_ELT(agg_specs_sexp, a);
        const char *name = list_get_string(spec, "name");
        const char *kind = list_get_string(spec, "kind");
        const char *col = list_get_string(spec, "col");
        SEXP na_rm_sexp = list_get(spec, "na_rm");

        specs[a].output_name = (char *)malloc(strlen(name) + 1);
        strcpy(specs[a].output_name, name);
        specs[a].kind = parse_agg_kind(kind);
        if (col) {
            specs[a].input_col = (char *)malloc(strlen(col) + 1);
            strcpy(specs[a].input_col, col);
        }
        specs[a].na_rm = (na_rm_sexp != R_NilValue) ? Rf_asLogical(na_rm_sexp) : 0;
    }

    GroupAggNode *ga = group_agg_node_create(child, n_keys, key_names,
                                              n_aggs, specs, get_r_tempdir());
    return wrap_node((VecNode *)ga);
}

/* --- C_sort_node --- */

SEXP C_sort_node(SEXP node_xptr, SEXP col_names_sexp, SEXP desc_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;
    int n_keys = Rf_length(col_names_sexp);

    SortKey *keys = (SortKey *)malloc((size_t)n_keys * sizeof(SortKey));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(col_names_sexp, k));
        int idx = vec_schema_find_col(schema, nm);
        if (idx < 0)
            vectra_error("arrange: column not found: %s", nm);
        keys[k].col_index = idx;
        keys[k].descending = LOGICAL(desc_sexp)[k];
    }

    SortNode *sn = sort_node_create(child, n_keys, keys, get_r_tempdir());
    return wrap_node((VecNode *)sn);
}

/* --- C_limit_node --- */

SEXP C_limit_node(SEXP node_xptr, SEXP n_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    int64_t max_rows = (int64_t)Rf_asReal(n_sexp);
    LimitNode *ln = limit_node_create(child, max_rows);
    return wrap_node((VecNode *)ln);
}

/* --- C_topn_node --- */

SEXP C_topn_node(SEXP node_xptr, SEXP col_names_sexp,
                  SEXP desc_sexp, SEXP n_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;
    int n_keys = Rf_length(col_names_sexp);

    SortKey *keys = (SortKey *)malloc((size_t)n_keys * sizeof(SortKey));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(col_names_sexp, k));
        int idx = vec_schema_find_col(schema, nm);
        if (idx < 0)
            vectra_error("topn: column not found: %s", nm);
        keys[k].col_index = idx;
        keys[k].descending = LOGICAL(desc_sexp)[k];
    }

    int64_t limit = (int64_t)Rf_asReal(n_sexp);
    TopNNode *tn = topn_node_create(child, n_keys, keys, limit);
    return wrap_node((VecNode *)tn);
}

/* --- C_group_topn_node --- */

SEXP C_group_topn_node(SEXP node_xptr, SEXP key_names_sexp,
                       SEXP order_sexp, SEXP desc_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    const VecSchema *schema = &child->output_schema;
    int n_keys = Rf_length(key_names_sexp);

    int *key_idx = (int *)malloc((size_t)(n_keys > 0 ? n_keys : 1) * sizeof(int));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(key_names_sexp, k));
        int idx = vec_schema_find_col(schema, nm);
        if (idx < 0)
            vectra_error("group_topn: group column not found: %s", nm);
        key_idx[k] = idx;
    }

    const char *order_nm = CHAR(STRING_ELT(order_sexp, 0));
    int order_idx = vec_schema_find_col(schema, order_nm);
    if (order_idx < 0)
        vectra_error("group_topn: order column not found: %s", order_nm);

    int descending = LOGICAL(desc_sexp)[0];
    GroupTopNNode *gn = group_topn_node_create(child, n_keys, key_idx,
                                               order_idx, descending);
    free(key_idx);
    return wrap_node((VecNode *)gn);
}

/* --- C_join_node --- */

SEXP C_join_node(SEXP left_xptr, SEXP right_xptr,
                 SEXP kind_sexp, SEXP left_keys_sexp, SEXP right_keys_sexp,
                 SEXP suffix_x_sexp, SEXP suffix_y_sexp) {
    VecNode *left = unwrap_node(left_xptr);
    R_ClearExternalPtr(left_xptr);
    VecNode *right = unwrap_node(right_xptr);
    R_ClearExternalPtr(right_xptr);

    const char *kind_str = CHAR(STRING_ELT(kind_sexp, 0));
    JoinKind kind = JOIN_INNER;
    if (strcmp(kind_str, "inner") == 0) kind = JOIN_INNER;
    else if (strcmp(kind_str, "left") == 0) kind = JOIN_LEFT;
    else if (strcmp(kind_str, "full") == 0) kind = JOIN_FULL;
    else if (strcmp(kind_str, "semi") == 0) kind = JOIN_SEMI;
    else if (strcmp(kind_str, "anti") == 0) kind = JOIN_ANTI;
    else vectra_error("unknown join kind: %s", kind_str);

    const VecSchema *lschema = &left->output_schema;
    const VecSchema *rschema = &right->output_schema;

    int n_keys = Rf_length(left_keys_sexp);
    JoinKey *keys = (JoinKey *)malloc((size_t)n_keys * sizeof(JoinKey));
    for (int k = 0; k < n_keys; k++) {
        const char *lk = CHAR(STRING_ELT(left_keys_sexp, k));
        const char *rk = CHAR(STRING_ELT(right_keys_sexp, k));
        keys[k].left_col = vec_schema_find_col(lschema, lk);
        keys[k].right_col = vec_schema_find_col(rschema, rk);
        if (keys[k].left_col < 0)
            vectra_error("join: left key column not found: %s", lk);
        if (keys[k].right_col < 0)
            vectra_error("join: right key column not found: %s", rk);
    }

    const char *sx = CHAR(STRING_ELT(suffix_x_sexp, 0));
    const char *sy = CHAR(STRING_ELT(suffix_y_sexp, 0));

    JoinNode *jn = join_node_create(left, right, kind, n_keys, keys, sx, sy);
    return wrap_node((VecNode *)jn);
}

/* --- C_window_node --- */

static WinKind parse_win_kind(const char *s) {
    if (strcmp(s, "lag") == 0) return WIN_LAG;
    if (strcmp(s, "lead") == 0) return WIN_LEAD;
    if (strcmp(s, "row_number") == 0) return WIN_ROW_NUMBER;
    if (strcmp(s, "rank") == 0) return WIN_RANK;
    if (strcmp(s, "dense_rank") == 0) return WIN_DENSE_RANK;
    if (strcmp(s, "cumsum") == 0) return WIN_CUMSUM;
    if (strcmp(s, "cummean") == 0) return WIN_CUMMEAN;
    if (strcmp(s, "cummin") == 0) return WIN_CUMMIN;
    if (strcmp(s, "cummax") == 0) return WIN_CUMMAX;
    if (strcmp(s, "ntile") == 0) return WIN_NTILE;
    if (strcmp(s, "percent_rank") == 0) return WIN_PERCENT_RANK;
    if (strcmp(s, "cume_dist") == 0) return WIN_CUME_DIST;
    if (strcmp(s, "roll_sum") == 0) return WIN_ROLL_SUM;
    if (strcmp(s, "roll_mean") == 0) return WIN_ROLL_MEAN;
    if (strcmp(s, "roll_min") == 0) return WIN_ROLL_MIN;
    if (strcmp(s, "roll_max") == 0) return WIN_ROLL_MAX;
    if (strcmp(s, "roll_n") == 0) return WIN_ROLL_N;
    vectra_error("unknown window function: %s", s);
    return WIN_LAG; /* unreachable */
}

SEXP C_window_node(SEXP node_xptr, SEXP key_names_sexp, SEXP win_specs_sexp) {
    VecNode *child = unwrap_node(node_xptr);
    R_ClearExternalPtr(node_xptr);

    int n_keys = Rf_length(key_names_sexp);
    char **key_names = (char **)malloc((size_t)n_keys * sizeof(char *));
    for (int k = 0; k < n_keys; k++) {
        const char *nm = CHAR(STRING_ELT(key_names_sexp, k));
        key_names[k] = (char *)malloc(strlen(nm) + 1);
        strcpy(key_names[k], nm);
    }

    int n_wins = Rf_length(win_specs_sexp);
    WinSpec *specs = (WinSpec *)calloc((size_t)n_wins, sizeof(WinSpec));
    for (int w = 0; w < n_wins; w++) {
        SEXP spec = VECTOR_ELT(win_specs_sexp, w);
        const char *name = list_get_string(spec, "name");
        const char *kind = list_get_string(spec, "kind");
        const char *col = list_get_string(spec, "col");
        SEXP offset_sexp = list_get(spec, "offset");
        SEXP default_sexp = list_get(spec, "default");
        SEXP desc_sexp = list_get(spec, "desc");
        const char *order = list_get_string(spec, "order");
        SEXP window_sexp = list_get(spec, "window");

        specs[w].output_name = (char *)malloc(strlen(name) + 1);
        strcpy(specs[w].output_name, name);
        specs[w].kind = parse_win_kind(kind);
        if (col) {
            specs[w].input_col = (char *)malloc(strlen(col) + 1);
            strcpy(specs[w].input_col, col);
        }
        if (order) {
            specs[w].order_col = (char *)malloc(strlen(order) + 1);
            strcpy(specs[w].order_col, order);
        }
        specs[w].window = (window_sexp != R_NilValue && !Rf_isNull(window_sexp))
                          ? Rf_asReal(window_sexp) : 0.0;
        specs[w].offset = (offset_sexp != R_NilValue) ? Rf_asInteger(offset_sexp) : 1;
        if (default_sexp != R_NilValue && !Rf_isNull(default_sexp)) {
            specs[w].default_val = Rf_asReal(default_sexp);
            specs[w].has_default = 1;
        }
        specs[w].desc = (desc_sexp != R_NilValue && !Rf_isNull(desc_sexp))
                        ? Rf_asLogical(desc_sexp) : 0;
    }

    WindowNode *wn = window_node_create(child, n_keys, key_names, n_wins, specs);
    return wrap_node((VecNode *)wn);
}

/* --- C_concat_node --- */

SEXP C_concat_node(SEXP node_xptrs) {
    int n = Rf_length(node_xptrs);
    VecNode **children = (VecNode **)malloc((size_t)n * sizeof(VecNode *));
    for (int i = 0; i < n; i++) {
        SEXP xptr = VECTOR_ELT(node_xptrs, i);
        children[i] = unwrap_node(xptr);
        R_ClearExternalPtr(xptr);
    }
    ConcatNode *cn = concat_node_create(n, children);
    return wrap_node((VecNode *)cn);
}

/* --- C_fuzzy_join_node --- */

SEXP C_fuzzy_join_node(SEXP probe_xptr, SEXP build_xptr,
                       SEXP by_probe_sexp, SEXP by_build_sexp,
                       SEXP block_probe_sexp, SEXP block_build_sexp,
                       SEXP method_sexp, SEXP max_dist_sexp,
                       SEXP n_threads_sexp, SEXP suffix_y_sexp) {
    VecNode *probe = unwrap_node(probe_xptr);
    R_ClearExternalPtr(probe_xptr);
    VecNode *build = unwrap_node(build_xptr);
    R_ClearExternalPtr(build_xptr);

    const VecSchema *pschema = &probe->output_schema;
    const VecSchema *bschema = &build->output_schema;

    /* Resolve key column indices */
    const char *pk = CHAR(STRING_ELT(by_probe_sexp, 0));
    const char *bk = CHAR(STRING_ELT(by_build_sexp, 0));
    int probe_key = vec_schema_find_col(pschema, pk);
    int build_key = vec_schema_find_col(bschema, bk);
    if (probe_key < 0)
        vectra_error("fuzzy_join: probe key column not found: %s", pk);
    if (build_key < 0)
        vectra_error("fuzzy_join: build key column not found: %s", bk);

    /* Resolve optional blocking columns */
    int probe_block = -1, build_block = -1;
    if (block_probe_sexp != R_NilValue && block_build_sexp != R_NilValue) {
        const char *bp = CHAR(STRING_ELT(block_probe_sexp, 0));
        const char *bb = CHAR(STRING_ELT(block_build_sexp, 0));
        probe_block = vec_schema_find_col(pschema, bp);
        build_block = vec_schema_find_col(bschema, bb);
        if (probe_block < 0)
            vectra_error("fuzzy_join: probe block column not found: %s", bp);
        if (build_block < 0)
            vectra_error("fuzzy_join: build block column not found: %s", bb);
    }

    /* Parse method */
    int method_int = INTEGER(method_sexp)[0];
    FuzzyMethod method;
    if (method_int == 0) method = FUZZY_DL;
    else if (method_int == 1) method = FUZZY_LEVENSHTEIN;
    else if (method_int == 2) method = FUZZY_JW;
    else vectra_error("fuzzy_join: unknown method: %d", method_int);

    double max_dist = REAL(max_dist_sexp)[0];
    int n_threads = INTEGER(n_threads_sexp)[0];
    const char *suffix_y = CHAR(STRING_ELT(suffix_y_sexp, 0));

    FuzzyJoinNode *fj = fuzzy_join_node_create(
        probe, build,
        probe_key, build_key,
        probe_block, build_block,
        method, max_dist, n_threads,
        suffix_y
    );
    return wrap_node((VecNode *)fj);
}

/* --- C_interval_join_node --- */

SEXP C_interval_join_node(SEXP probe_xptr, SEXP build_xptr,
                          SEXP start_probe_sexp, SEXP end_probe_sexp,
                          SEXP start_build_sexp, SEXP end_build_sexp,
                          SEXP block_probe_sexp, SEXP block_build_sexp,
                          SEXP kind_sexp, SEXP closed_sexp,
                          SEXP n_threads_sexp, SEXP suffix_y_sexp) {
    VecNode *probe = unwrap_node(probe_xptr);
    R_ClearExternalPtr(probe_xptr);
    VecNode *build = unwrap_node(build_xptr);
    R_ClearExternalPtr(build_xptr);

    const VecSchema *pschema = &probe->output_schema;
    const VecSchema *bschema = &build->output_schema;

    /* Resolve start/end interval columns */
    const char *psn = CHAR(STRING_ELT(start_probe_sexp, 0));
    const char *pen = CHAR(STRING_ELT(end_probe_sexp, 0));
    const char *bsn = CHAR(STRING_ELT(start_build_sexp, 0));
    const char *ben = CHAR(STRING_ELT(end_build_sexp, 0));
    int p_start = vec_schema_find_col(pschema, psn);
    int p_end   = vec_schema_find_col(pschema, pen);
    int b_start = vec_schema_find_col(bschema, bsn);
    int b_end   = vec_schema_find_col(bschema, ben);
    if (p_start < 0) vectra_error("interval_join: probe start column not found: %s", psn);
    if (p_end   < 0) vectra_error("interval_join: probe end column not found: %s", pen);
    if (b_start < 0) vectra_error("interval_join: build start column not found: %s", bsn);
    if (b_end   < 0) vectra_error("interval_join: build end column not found: %s", ben);

    /* Interval columns must be numeric */
    if (pschema->col_types[p_start] == VEC_STRING ||
        pschema->col_types[p_end]   == VEC_STRING ||
        bschema->col_types[b_start] == VEC_STRING ||
        bschema->col_types[b_end]   == VEC_STRING)
        vectra_error("interval_join: start/end columns must be numeric");

    /* Optional blocking columns (must be string, like the fuzzy join) */
    int probe_block = -1, build_block = -1;
    if (block_probe_sexp != R_NilValue && block_build_sexp != R_NilValue) {
        const char *bp = CHAR(STRING_ELT(block_probe_sexp, 0));
        const char *bb = CHAR(STRING_ELT(block_build_sexp, 0));
        probe_block = vec_schema_find_col(pschema, bp);
        build_block = vec_schema_find_col(bschema, bb);
        if (probe_block < 0) vectra_error("interval_join: probe block column not found: %s", bp);
        if (build_block < 0) vectra_error("interval_join: build block column not found: %s", bb);
        if (pschema->col_types[probe_block] != VEC_STRING ||
            bschema->col_types[build_block] != VEC_STRING)
            vectra_error("interval_join: blocking 'by' columns must be string");
    }

    const char *kind_str = CHAR(STRING_ELT(kind_sexp, 0));
    IntervalJoinKind kind;
    if (strcmp(kind_str, "inner") == 0)      kind = IJOIN_INNER;
    else if (strcmp(kind_str, "left") == 0)  kind = IJOIN_LEFT;
    else vectra_error("interval_join: unknown kind: %s", kind_str);

    int closed = Rf_asLogical(closed_sexp) == TRUE ? 1 : 0;
    int n_threads = INTEGER(n_threads_sexp)[0];
    const char *suffix_y = CHAR(STRING_ELT(suffix_y_sexp, 0));

    IntervalJoinNode *ij = interval_join_node_create(
        probe, build,
        p_start, p_end, b_start, b_end,
        probe_block, build_block,
        kind, closed, n_threads, suffix_y
    );
    return wrap_node((VecNode *)ij);
}
