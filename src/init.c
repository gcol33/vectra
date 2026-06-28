#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <R.h>
#include <Rinternals.h>
#include "r_bridge.h"
#include "vtr_append.h"
#include "vec_omp.h"

/* block.c R bridge functions */
SEXP C_block_materialize(SEXP node_xptr);
SEXP C_block_lookup(SEXP block_xptr, SEXP col_name, SEXP keys, SEXP ci);
SEXP C_block_fuzzy_lookup(SEXP block_xptr, SEXP match_col, SEXP keys,
                          SEXP method, SEXP max_dist, SEXP block_col,
                          SEXP block_keys, SEXP n_threads);
#include "vtr_delete.h"
#include "vtr_diff.h"

/* tdc encode/decode bridge round-trip test entries (vtr_codec_tdc.c). */
SEXP C_tdc_encode_column(SEXP x_sexp, SEXP comp_level_sexp);
SEXP C_tdc_decode_column(SEXP raw_sexp, SEXP n_sexp, SEXP r_type_sexp);

/* tdc-backed row-group container entries (vtr1_tdc.c). */
SEXP C_write_vtr_tdc(SEXP path_sexp, SEXP df_sexp,
                     SEXP rowgroup_size_sexp, SEXP comp_level_sexp,
                     SEXP annotations_sexp);
SEXP C_read_vtr_tdc(SEXP path_sexp);
SEXP C_read_vtr_tdc_annotations(SEXP path_sexp);
SEXP C_read_vtr_tdc_stats(SEXP path_sexp);

/* VECR raster entries (r_bridge_raster.c). */
SEXP C_vec_write_raster(SEXP path_sexp, SEXP data_sexp, SEXP dims_sexp,
                        SEXP dtype_sexp, SEXP tile_size_sexp,
                        SEXP gt_sexp, SEXP epsg_sexp, SEXP nodata_sexp,
                        SEXP band_names_sexp, SEXP compression_sexp);
SEXP C_vec_open_raster(SEXP path_sexp);
SEXP C_vec_read_window(SEXP ptr_sexp, SEXP band_sexp, SEXP level_sexp,
                       SEXP col_min_sexp, SEXP row_min_sexp,
                       SEXP col_max_sexp, SEXP row_max_sexp);
SEXP C_vec_extract_points(SEXP ptr_sexp, SEXP x_sexp, SEXP y_sexp);
SEXP C_vec_close_raster(SEXP ptr_sexp);
SEXP C_vec_build_overviews(SEXP path_sexp, SEXP n_levels_sexp,
                           SEXP resampling_sexp, SEXP compression_sexp);
SEXP C_vec_to_tiff(SEXP vec_path_sexp, SEXP tiff_path_sexp,
                   SEXP use_deflate_sexp);
SEXP C_vec_write_time_cube(SEXP path_sexp, SEXP data_sexp, SEXP dims_sexp,
                           SEXP times_sexp, SEXP dtype_sexp,
                           SEXP tile_size_sexp,
                           SEXP gt_sexp, SEXP epsg_sexp, SEXP nodata_sexp,
                           SEXP band_names_sexp, SEXP compression_sexp);
SEXP C_vec_read_time_slice(SEXP ptr_sexp, SEXP band_sexp, SEXP level_sexp,
                           SEXP time_sexp,
                           SEXP col_min_sexp, SEXP row_min_sexp,
                           SEXP col_max_sexp, SEXP row_max_sexp);
SEXP C_vec_write_pixel_cube(SEXP path_sexp, SEXP data_sexp, SEXP dims_sexp,
                            SEXP times_sexp, SEXP dtype_sexp,
                            SEXP tile_size_sexp,
                            SEXP gt_sexp, SEXP epsg_sexp, SEXP nodata_sexp,
                            SEXP band_names_sexp, SEXP compression_sexp);
SEXP C_vec_read_pixel_series(SEXP ptr_sexp, SEXP col_sexp, SEXP row_sexp,
                             SEXP band_sexp, SEXP level_sexp);
SEXP C_vec_raster_times(SEXP ptr_sexp, SEXP band_sexp, SEXP level_sexp);
SEXP C_vec_raster_layout(SEXP ptr_sexp);

/* Streaming vector-to-raster accumulator (rasterize.c). */
SEXP C_rasterize_new(SEXP dims_sexp, SEXP gt_sexp, SEXP fun_sexp);
SEXP C_rasterize_push(SEXP ptr_sexp, SEXP x_sexp, SEXP y_sexp, SEXP val_sexp);
SEXP C_rasterize_finish(SEXP ptr_sexp, SEXP bg_sexp);

/* Focal / terrain kernels and the streaming tile-row writer (focal.c). */
SEXP C_focal_strip(SEXP in_sexp, SEXP dims_sexp, SEXP w_sexp, SEXP kdims_sexp,
                   SEXP fun_sexp, SEXP na_rm_sexp, SEXP top_sexp,
                   SEXP out_h_sexp);
SEXP C_terrain_strip(SEXP in_sexp, SEXP dims_sexp, SEXP which_sexp,
                     SEXP top_sexp, SEXP out_h_sexp, SEXP res_sexp,
                     SEXP unit_sexp, SEXP sun_sexp);
SEXP C_vecr_writer_open(SEXP path_sexp, SEXP dims_sexp, SEXP dtype_sexp,
                        SEXP tile_size_sexp, SEXP gt_sexp, SEXP epsg_sexp,
                        SEXP nodata_sexp, SEXP band_names_sexp,
                        SEXP compression_sexp);
SEXP C_vecr_writer_write_strip(SEXP ptr_sexp, SEXP band_sexp, SEXP ty_sexp,
                               SEXP strip_sexp);
SEXP C_vecr_writer_finish(SEXP ptr_sexp);

/* Resample/reproject sampler kernel (warp.c). */
SEXP C_warp_strip(SEXP win_sexp, SEXP win_dims_sexp, SEXP origin_sexp,
                  SEXP sx_sexp, SEXP sy_sexp, SEXP method_sexp,
                  SEXP out_dims_sexp);

/* Euclidean distance transform kernel (edt.c). */
SEXP C_edt_strip(SEXP mat_sexp, SEXP dims_sexp, SEXP scale_sexp);

/* GEOS-native vector overlay (vtr_overlay.c). */
SEXP C_geos_version(void);
SEXP C_overlay_parse(SEXP wkb_list, SEXP grid_sexp, SEXP nthreads_sexp);
SEXP C_overlay_components(SEXP bbox_sexp);
SEXP C_overlay_group(SEXP wkb_list);
SEXP C_overlay_run(SEXP wkb_chunk, SEXP job_chunk, SEXP rects_sexp, SEXP nthreads_sexp, SEXP prec_sexp);

/* GEOS-native streaming spatial verbs (vtr_spatial.c). */
SEXP C_geos_locator_build(SEXP wkb_list);
SEXP C_geos_filter(SEXP loc_ptr, SEXP batch_hex, SEXP pred_sexp, SEXP negate_sexp, SEXP dist_sexp, SEXP nthreads_sexp);
SEXP C_geos_join(SEXP loc_ptr, SEXP batch_hex, SEXP pred_sexp, SEXP dist_sexp, SEXP nthreads_sexp);
SEXP C_geos_nearest(SEXP loc_ptr, SEXP batch_hex, SEXP nthreads_sexp);
SEXP C_geos_clip(SEXP loc_ptr, SEXP batch_hex, SEXP erase_sexp, SEXP nthreads_sexp);
SEXP C_geos_union_hex(SEXP batch_hex);
SEXP C_geos_locate_xy(SEXP loc_ptr, SEXP x_sexp, SEXP y_sexp, SEXP pred_sexp, SEXP dist_sexp, SEXP want_all_sexp, SEXP nthreads_sexp);
SEXP C_geos_points_to_hex(SEXP x_sexp, SEXP y_sexp);

static const R_CallMethodDef CallEntries[] = {
    {"C_write_vtr",    (DL_FUNC) &C_write_vtr,    7},
    {"C_scan_node",    (DL_FUNC) &C_scan_node,     1},
    {"C_collect",      (DL_FUNC) &C_collect,       1},
    {"C_node_optimize",   (DL_FUNC) &C_node_optimize,   1},
    {"C_node_next_batch", (DL_FUNC) &C_node_next_batch, 1},
    {"C_node_schema",  (DL_FUNC) &C_node_schema,   1},
    {"C_node_plan",    (DL_FUNC) &C_node_plan,     1},
    {"C_filter_node",  (DL_FUNC) &C_filter_node,   2},
    {"C_project_node",   (DL_FUNC) &C_project_node,  3},
    {"C_group_agg_node", (DL_FUNC) &C_group_agg_node, 3},
    {"C_sort_node",      (DL_FUNC) &C_sort_node,       3},
    {"C_limit_node",     (DL_FUNC) &C_limit_node,      2},
    {"C_topn_node",      (DL_FUNC) &C_topn_node,       4},
    {"C_group_topn_node",(DL_FUNC) &C_group_topn_node, 4},
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
    {"C_write_tiff_typed",       (DL_FUNC) &C_write_tiff_typed,       11},
    {"C_tiff_read_metadata",     (DL_FUNC) &C_tiff_read_metadata,     1},
    {"C_tiff_read_crs",          (DL_FUNC) &C_tiff_read_crs,          1},
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
    {"C_tdc_encode_column",   (DL_FUNC) &C_tdc_encode_column,   2},
    {"C_tdc_decode_column",   (DL_FUNC) &C_tdc_decode_column,   3},
    {"C_write_vtr_tdc",            (DL_FUNC) &C_write_vtr_tdc,            5},
    {"C_read_vtr_tdc",             (DL_FUNC) &C_read_vtr_tdc,             1},
    {"C_read_vtr_tdc_annotations", (DL_FUNC) &C_read_vtr_tdc_annotations, 1},
    {"C_read_vtr_tdc_stats",       (DL_FUNC) &C_read_vtr_tdc_stats,       1},
    {"C_vec_write_raster",         (DL_FUNC) &C_vec_write_raster,         10},
    {"C_vec_open_raster",          (DL_FUNC) &C_vec_open_raster,          1},
    {"C_vec_read_window",          (DL_FUNC) &C_vec_read_window,          7},
    {"C_vec_extract_points",       (DL_FUNC) &C_vec_extract_points,       3},
    {"C_vec_close_raster",         (DL_FUNC) &C_vec_close_raster,         1},
    {"C_vec_build_overviews",      (DL_FUNC) &C_vec_build_overviews,      4},
    {"C_vec_to_tiff",              (DL_FUNC) &C_vec_to_tiff,              3},
    {"C_vec_write_time_cube",      (DL_FUNC) &C_vec_write_time_cube,      11},
    {"C_vec_read_time_slice",      (DL_FUNC) &C_vec_read_time_slice,      8},
    {"C_vec_write_pixel_cube",     (DL_FUNC) &C_vec_write_pixel_cube,     11},
    {"C_vec_read_pixel_series",    (DL_FUNC) &C_vec_read_pixel_series,    5},
    {"C_vec_raster_times",         (DL_FUNC) &C_vec_raster_times,         3},
    {"C_vec_raster_layout",        (DL_FUNC) &C_vec_raster_layout,        1},
    {"C_rasterize_new",            (DL_FUNC) &C_rasterize_new,            3},
    {"C_rasterize_push",           (DL_FUNC) &C_rasterize_push,           4},
    {"C_rasterize_finish",         (DL_FUNC) &C_rasterize_finish,         2},
    {"C_focal_strip",              (DL_FUNC) &C_focal_strip,              8},
    {"C_terrain_strip",            (DL_FUNC) &C_terrain_strip,            8},
    {"C_warp_strip",               (DL_FUNC) &C_warp_strip,               7},
    {"C_edt_strip",                (DL_FUNC) &C_edt_strip,                3},
    {"C_geos_version",             (DL_FUNC) &C_geos_version,             0},
    {"C_overlay_parse",            (DL_FUNC) &C_overlay_parse,            3},
    {"C_overlay_components",       (DL_FUNC) &C_overlay_components,       1},
    {"C_overlay_group",            (DL_FUNC) &C_overlay_group,            1},
    {"C_overlay_run",              (DL_FUNC) &C_overlay_run,              5},
    {"C_geos_locator_build",       (DL_FUNC) &C_geos_locator_build,       1},
    {"C_geos_filter",              (DL_FUNC) &C_geos_filter,              6},
    {"C_geos_join",                (DL_FUNC) &C_geos_join,                5},
    {"C_geos_nearest",             (DL_FUNC) &C_geos_nearest,             3},
    {"C_geos_clip",                (DL_FUNC) &C_geos_clip,                4},
    {"C_geos_union_hex",           (DL_FUNC) &C_geos_union_hex,           1},
    {"C_geos_locate_xy",           (DL_FUNC) &C_geos_locate_xy,           7},
    {"C_geos_points_to_hex",       (DL_FUNC) &C_geos_points_to_hex,       2},
    {"C_vecr_writer_open",         (DL_FUNC) &C_vecr_writer_open,         9},
    {"C_vecr_writer_write_strip",  (DL_FUNC) &C_vecr_writer_write_strip,  4},
    {"C_vecr_writer_finish",       (DL_FUNC) &C_vecr_writer_finish,       1},
    {NULL, NULL, 0}
};

void R_init_vectra(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
    vec_omp_apply_core_limit();
}
