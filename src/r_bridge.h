#ifndef VECTRA_R_BRIDGE_H
#define VECTRA_R_BRIDGE_H

#include <R.h>
#include <Rinternals.h>

/* .Call entry points */
SEXP C_write_vtr(SEXP df, SEXP path, SEXP batch_size, SEXP compress, SEXP col_types,
                 SEXP quantize, SEXP spatial);
SEXP C_scan_node(SEXP path);
SEXP C_collect(SEXP node_xptr);
SEXP C_node_schema(SEXP node_xptr);
SEXP C_node_plan(SEXP node_xptr);
SEXP C_filter_node(SEXP node_xptr, SEXP expr_list);
SEXP C_project_node(SEXP node_xptr, SEXP names, SEXP expr_lists);
SEXP C_group_agg_node(SEXP node_xptr, SEXP key_names, SEXP agg_specs);
SEXP C_sort_node(SEXP node_xptr, SEXP col_names, SEXP desc);
SEXP C_limit_node(SEXP node_xptr, SEXP n);
SEXP C_topn_node(SEXP node_xptr, SEXP col_names, SEXP desc, SEXP n);
SEXP C_join_node(SEXP left_xptr, SEXP right_xptr,
                 SEXP kind, SEXP left_keys, SEXP right_keys,
                 SEXP suffix_x, SEXP suffix_y);
SEXP C_window_node(SEXP node_xptr, SEXP key_names, SEXP win_specs);
SEXP C_concat_node(SEXP node_xptrs);
SEXP C_write_csv(SEXP node_xptr, SEXP path);
SEXP C_csv_scan_node(SEXP path, SEXP batch_size);
SEXP C_sql_scan_node(SEXP path, SEXP table, SEXP batch_size);
SEXP C_write_sqlite(SEXP node_xptr, SEXP path, SEXP table_name);
SEXP C_tiff_scan_node(SEXP path, SEXP batch_size);
SEXP C_tiff_scan_meta(SEXP node_xptr);
SEXP C_tiff_extract_points(SEXP path, SEXP x, SEXP y);
SEXP C_write_tiff(SEXP node_xptr, SEXP path, SEXP compress);
SEXP C_write_tiff_typed(SEXP node_xptr, SEXP path, SEXP compress,
                        SEXP pixel_type, SEXP metadata);
SEXP C_tiff_read_metadata(SEXP path);
SEXP C_write_vtr_node(SEXP node_xptr, SEXP path, SEXP batch_size, SEXP compress,
                      SEXP col_types, SEXP quantize, SEXP spatial);
SEXP C_append_vtr(SEXP node_xptr, SEXP path);
SEXP C_delete_vtr(SEXP path, SEXP row_indices);
SEXP C_fuzzy_join_node(SEXP probe_xptr, SEXP build_xptr,
                       SEXP by_probe, SEXP by_build,
                       SEXP block_probe, SEXP block_build,
                       SEXP method, SEXP max_dist,
                       SEXP n_threads, SEXP suffix_y);

/* Hash index */
SEXP C_create_index(SEXP path, SEXP col_name, SEXP ci);
SEXP C_has_index(SEXP path, SEXP col_name);

#endif /* VECTRA_R_BRIDGE_H */
