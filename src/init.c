#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include "r_bridge.h"

static const R_CallMethodDef CallEntries[] = {
    {"C_write_vtr",    (DL_FUNC) &C_write_vtr,    3},
    {"C_scan_node",    (DL_FUNC) &C_scan_node,     1},
    {"C_collect",      (DL_FUNC) &C_collect,       1},
    {"C_node_schema",  (DL_FUNC) &C_node_schema,   1},
    {"C_node_plan",    (DL_FUNC) &C_node_plan,     1},
    {"C_filter_node",  (DL_FUNC) &C_filter_node,   2},
    {"C_project_node",   (DL_FUNC) &C_project_node,  3},
    {"C_group_agg_node", (DL_FUNC) &C_group_agg_node, 3},
    {"C_sort_node",      (DL_FUNC) &C_sort_node,       3},
    {"C_limit_node",     (DL_FUNC) &C_limit_node,      2},
    {"C_topn_node",      (DL_FUNC) &C_topn_node,       4},
    {"C_join_node",      (DL_FUNC) &C_join_node,       7},
    {"C_window_node",    (DL_FUNC) &C_window_node,     3},
    {"C_concat_node",   (DL_FUNC) &C_concat_node,    1},
    {"C_write_csv",     (DL_FUNC) &C_write_csv,      2},
    {"C_csv_scan_node", (DL_FUNC) &C_csv_scan_node,  2},
    {"C_sql_scan_node", (DL_FUNC) &C_sql_scan_node,  3},
    {"C_write_sqlite",  (DL_FUNC) &C_write_sqlite,   3},
    {"C_tiff_scan_node", (DL_FUNC) &C_tiff_scan_node, 2},
    {"C_tiff_scan_meta", (DL_FUNC) &C_tiff_scan_meta, 1},
    {"C_write_tiff",     (DL_FUNC) &C_write_tiff,     3},
    {NULL, NULL, 0}
};

void R_init_vectra(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
