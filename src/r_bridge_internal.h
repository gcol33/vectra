#ifndef VECTRA_R_BRIDGE_INTERNAL_H
#define VECTRA_R_BRIDGE_INTERNAL_H

#include <R.h>
#include <Rinternals.h>
#include "types.h"
#include "schema.h"
#include "expr.h"
#include "vtr_codec.h"

/* --- r_bridge_core.c: external pointer helpers --- */
void       node_finalizer(SEXP xptr);
SEXP       wrap_node(VecNode *node);
VecNode   *unwrap_node(SEXP xptr);

/* --- r_bridge_core.c: R column type detection --- */
int        r_has_class(SEXP col, const char *cls_name);
VecType    r_col_type(SEXP col);
char      *r_col_annotation(SEXP col);

/* --- r_bridge_core.c: data.frame -> VecBatch --- */
VecBatch  *df_to_batch(SEXP df);

/* --- r_bridge_core.c: expression parser --- */
const char *list_get_string(SEXP lst, const char *field);
SEXP        list_get(SEXP lst, const char *field);
VecExpr    *parse_expr(SEXP lst, const VecSchema *schema);

/* --- r_bridge_core.c: tempdir helper --- */
const char *get_r_tempdir(void);

/* --- r_bridge_io.c: quantize + spatial spec parsers --- */
VtrQuantizeSpec *parse_quantize(SEXP quantize_sexp, SEXP col_names, int n_cols);
VtrSpatialSpec *parse_spatial(SEXP spatial_sexp, SEXP col_names, int n_cols);

#endif /* VECTRA_R_BRIDGE_INTERNAL_H */
