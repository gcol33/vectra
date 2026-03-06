#ifndef VECTRA_EXPR_H
#define VECTRA_EXPR_H

#include "types.h"

typedef enum {
    EXPR_COL_REF,
    EXPR_LIT_INT64,
    EXPR_LIT_DOUBLE,
    EXPR_LIT_BOOL,
    EXPR_LIT_STRING,
    EXPR_LIT_NA,
    EXPR_ARITH,      /* +, -, *, /, %% */
    EXPR_CMP,        /* ==, !=, <, <=, >, >= */
    EXPR_BOOL,       /* &, |, ! */
    EXPR_IS_NA,
    EXPR_IF_ELSE,
    EXPR_CAST,
    EXPR_NEGATE,     /* unary minus */
    EXPR_NCHAR,      /* nchar(x) -> int64 */
    EXPR_SUBSTR,     /* substr(x, start, stop) -> string */
    EXPR_GREPL       /* grepl(pattern, x) -> bool (fixed match) */
} VecExprKind;

typedef struct VecExpr VecExpr;

struct VecExpr {
    VecExprKind kind;
    VecType     result_type;

    /* EXPR_COL_REF */
    char *col_name;

    /* EXPR_LIT_* */
    int64_t  lit_i64;
    double   lit_dbl;
    uint8_t  lit_bln;
    char    *lit_str;

    /* EXPR_ARITH, EXPR_CMP, EXPR_BOOL (binary) */
    char     op;        /* '+', '-', '*', '/', '%' for arith; '<', '>', '=', '!' for cmp */
    char     op2;       /* second char for <=, >=, ==, != */
    VecExpr *left;
    VecExpr *right;

    /* EXPR_BOOL (unary !) */
    VecExpr *operand;

    /* EXPR_IS_NA */
    /* uses operand */

    /* EXPR_IF_ELSE */
    VecExpr *cond;
    VecExpr *then_expr;
    VecExpr *else_expr;

    /* EXPR_CAST */
    VecType  cast_to;
    /* uses operand */

    /* EXPR_NEGATE */
    /* uses operand */
};

/* Allocate a new expression node */
VecExpr *vec_expr_alloc(VecExprKind kind);

/* Free expression tree */
void vec_expr_free(VecExpr *expr);

/* Evaluate an expression against a batch, return a new VecArray.
   Caller must free the result. */
VecArray *vec_expr_eval(const VecExpr *expr, const VecBatch *batch);

/* Walk an expression tree and mark all referenced column names.
   needed[i] is set to 1 if column col_names[i] is referenced. */
void vec_expr_collect_colrefs(const VecExpr *expr, char **col_names,
                              int n_cols, uint8_t *needed);

#endif /* VECTRA_EXPR_H */
