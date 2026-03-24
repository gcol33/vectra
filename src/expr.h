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
    EXPR_GREPL,      /* grepl(pattern, x) -> bool (fixed match) */
    EXPR_MATH_UNARY, /* abs, sqrt, log, exp, floor, ceiling, round */
    EXPR_TOLOWER,    /* tolower(x) -> string */
    EXPR_TOUPPER,    /* toupper(x) -> string */
    EXPR_TRIMWS,     /* trimws(x) -> string */
    EXPR_IN,         /* x %in% c(...) -> bool */
    EXPR_PASTE0,     /* paste0(a, b) -> string */
    EXPR_STARTSWITH, /* startsWith(x, prefix) -> bool */
    EXPR_ENDSWITH,   /* endsWith(x, suffix) -> bool */
    EXPR_GSUB,       /* gsub(pattern, replacement, x) -> string (fixed) */
    EXPR_SUB,        /* sub(pattern, replacement, x) -> string (fixed, first only) */
    EXPR_PMIN,       /* pmin(x, y) -> numeric */
    EXPR_PMAX,       /* pmax(x, y) -> numeric */
    EXPR_DATE_PART,  /* year/month/day/hour/minute/second extraction */
    EXPR_AS_DATE     /* as.Date(string) -> double (days since epoch) */
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

    /* EXPR_MATH_UNARY */
    char math_fn;  /* 'a'=abs, 's'=sqrt, 'l'=log, 'e'=exp, 'f'=floor, 'c'=ceiling, 'r'=round */
    /* uses operand */

    /* EXPR_IN */
    int64_t  n_set;
    double  *set_dbl;
    int64_t *set_i64;
    char   **set_str;
    /* uses operand */

    /* EXPR_DATE_PART */
    char date_part;  /* 'Y'=year, 'M'=month, 'D'=day, 'h'=hour, 'm'=minute, 's'=second */

    /* EXPR_GSUB / EXPR_SUB */
    char *gsub_pattern;
    char *gsub_replacement;
    /* uses operand for the input string */
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
