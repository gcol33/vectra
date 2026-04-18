#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <R.h>
#include <Rinternals.h>
#include "r_bridge.h"
#include "vtr_append.h"

/* block.c R bridge functions */
SEXP C_block_materialize(SEXP node_xptr);
SEXP C_block_lookup(SEXP block_xptr, SEXP col_name, SEXP keys, SEXP ci);
SEXP C_block_fuzzy_lookup(SEXP block_xptr, SEXP match_col, SEXP keys,
                          SEXP method, SEXP max_dist, SEXP block_col,
                          SEXP block_keys, SEXP n_threads);
#include "vtr_delete.h"
#include "vtr_diff.h"
#include "vtr_codec.h"

/* P2a/P2b tdc encode/decode bridge round-trip test entries (vtr_codec_tdc.c). */
SEXP C_tdc_encode_column(SEXP x_sexp, SEXP comp_level_sexp);
SEXP C_tdc_decode_column(SEXP raw_sexp, SEXP n_sexp, SEXP r_type_sexp);

/* P3 tdc-backed row-group container round-trip test entries (vtr1_tdc.c). */
SEXP C_write_vtr_tdc(SEXP path_sexp, SEXP df_sexp,
                     SEXP rowgroup_size_sexp, SEXP comp_level_sexp);
SEXP C_read_vtr_tdc(SEXP path_sexp);

/* Profiling bridge */
static SEXP C_codec_profile_reset(void) {
    vtr_codec_profile_reset();
    return R_NilValue;
}
extern uint64_t g_prof_sse2_unshuffle_calls;
static SEXP C_codec_profile_get(void) {
    uint64_t dec_ns, unsh_ns, enc_ns, calls;
    vtr_codec_profile_get(&dec_ns, &unsh_ns, &enc_ns, &calls);
    SEXP out = PROTECT(allocVector(REALSXP, 5));
    REAL(out)[0] = (double)dec_ns;
    REAL(out)[1] = (double)unsh_ns;
    REAL(out)[2] = (double)enc_ns;
    REAL(out)[3] = (double)calls;
    REAL(out)[4] = (double)g_prof_sse2_unshuffle_calls;
    SEXP nms = PROTECT(allocVector(STRSXP, 5));
    SET_STRING_ELT(nms, 0, mkChar("decompress_ns"));
    SET_STRING_ELT(nms, 1, mkChar("unshuffle_ns"));
    SET_STRING_ELT(nms, 2, mkChar("decode_ns"));
    SET_STRING_ELT(nms, 3, mkChar("calls"));
    SET_STRING_ELT(nms, 4, mkChar("sse2_unshuffle_calls"));
    setAttrib(out, R_NamesSymbol, nms);
    UNPROTECT(2);
    return out;
}

static const R_CallMethodDef CallEntries[] = {
    {"C_write_vtr",    (DL_FUNC) &C_write_vtr,    7},
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
    {"C_tiff_scan_meta",         (DL_FUNC) &C_tiff_scan_meta,         1},
    {"C_tiff_extract_points",    (DL_FUNC) &C_tiff_extract_points,    3},
    {"C_write_tiff",             (DL_FUNC) &C_write_tiff,             3},
    {"C_write_tiff_typed",       (DL_FUNC) &C_write_tiff_typed,       5},
    {"C_tiff_read_metadata",     (DL_FUNC) &C_tiff_read_metadata,     1},
    {"C_write_vtr_node", (DL_FUNC) &C_write_vtr_node, 7},
    {"C_append_vtr",     (DL_FUNC) &C_append_vtr,     2},
    {"C_delete_vtr",     (DL_FUNC) &C_delete_vtr,      2},
    {"C_diff_vtr",       (DL_FUNC) &C_diff_vtr,        3},
    {"C_fuzzy_join_node", (DL_FUNC) &C_fuzzy_join_node, 10},
    {"C_block_materialize", (DL_FUNC) &C_block_materialize, 1},
    {"C_block_lookup",        (DL_FUNC) &C_block_lookup,        4},
    {"C_block_fuzzy_lookup",  (DL_FUNC) &C_block_fuzzy_lookup,  8},
    {"C_create_index",      (DL_FUNC) &C_create_index,      3},
    {"C_has_index",         (DL_FUNC) &C_has_index,         2},
    {"C_codec_profile_reset", (DL_FUNC) &C_codec_profile_reset, 0},
    {"C_codec_profile_get",   (DL_FUNC) &C_codec_profile_get,   0},
    {"C_tdc_encode_column",   (DL_FUNC) &C_tdc_encode_column,   2},
    {"C_tdc_decode_column",   (DL_FUNC) &C_tdc_decode_column,   3},
    {"C_write_vtr_tdc",       (DL_FUNC) &C_write_vtr_tdc,       4},
    {"C_read_vtr_tdc",        (DL_FUNC) &C_read_vtr_tdc,        1},
    {NULL, NULL, 0}
};

void R_init_vectra(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
