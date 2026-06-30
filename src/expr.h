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
    EXPR_AS_DATE,    /* as.Date(string) -> double (days since epoch) */
    EXPR_FLOOR_TIME, /* floor_time(t, unit): truncate epoch to a calendar grid */
    EXPR_LEVENSHTEIN,      /* levenshtein(x, pattern) -> int64 edit distance */
    EXPR_LEVENSHTEIN_NORM, /* levenshtein_norm(x, pattern) -> double 0.0-1.0 */
    EXPR_DL_DIST,          /* dl_dist(x, pattern) -> int64 Damerau-Levenshtein distance */
    EXPR_DL_DIST_NORM,     /* dl_dist_norm(x, pattern) -> double 0.0-1.0 */
    EXPR_JARO_WINKLER,     /* jaro_winkler(x, pattern) -> double 0.0-1.0 similarity */
    EXPR_RESOLVE,          /* resolve(fk, pk, val) -> FK lookup within same table */
    EXPR_PROPAGATE,        /* propagate(parent_fk, pk, seed) -> iterative tree fill */
    EXPR_CASE_WHEN,        /* case_when(cond1 ~ val1, ...) */
    EXPR_COALESCE,         /* coalesce(a, b, c, ...) -> first non-NA */
    EXPR_PASTE,            /* paste(a, b, ..., sep) or paste0(a, b, ...) */
    EXPR_STR_EXTRACT,      /* str_extract(x, pattern) -> first regex match */
    EXPR_GEOM,             /* libgeos op on a hex-WKB geometry column */
    EXPR_VEC_DIST          /* cosine/l2/dot over a hex float32 embedding column */
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

    /* EXPR_FLOOR_TIME reuses date_part for the unit ('s' second, 'n' minute,
       'h' hour, 'd' day, 'w' week, 'M' month, 'q' quarter, 'y' year) and
       lit_i64 for the integer multiple. */

    /* EXPR_GSUB / EXPR_SUB */
    char *gsub_pattern;
    char *gsub_replacement;
    /* uses operand for the input string */

    /* EXPR_LEVENSHTEIN / EXPR_LEVENSHTEIN_NORM */
    int64_t max_dist;  /* -1 = no bound; >= 0 = early termination threshold */

    /* Variable-arity children (case_when, coalesce, paste, etc.) */
    int64_t   n_children;
    VecExpr **children;

    /* EXPR_PASTE: separator string (NULL = paste0, i.e. no separator) */
    char *paste_sep;

    /* EXPR_GREPL / EXPR_GSUB / EXPR_SUB: 1 = fixed match (default), 0 = regex */
    int fixed;

    /* EXPR_GEOM: which libgeos op (see expr_geom.c). The geometry argument is
       `operand`; a binary op's second geometry or a parameterized transform's
       scalar argument is `right`. */
    char geom_fn;

    /* EXPR_VEC_DIST: which distance (see expr_vec.c). The embedding column is
       `operand`; a second embedding column rides on `left`, or a constant
       query vector on `lit_str` (both hex float32). */
    char vec_fn;
};

/* Allocate a new expression node */
VecExpr *vec_expr_alloc(VecExprKind kind);

/* Free expression tree */
void vec_expr_free(VecExpr *expr);

/* Evaluate an expression against a batch, return a new VecArray.
   Caller must free the result. */
VecArray *vec_expr_eval(const VecExpr *expr, const VecBatch *batch);

/* Sub-dispatcher for string operations (nchar, substr, grepl, tolower,
   toupper, trimws, %in%, paste0, startsWith, endsWith, gsub/sub,
   levenshtein, dl_dist, jaro_winkler). */
VecArray *vec_expr_eval_string(VecExprKind op, const VecExpr *expr,
                                const VecBatch *batch);

/* Sub-dispatcher for datetime/extended operations (pmin, pmax, date_part,
   as.Date, if_else, resolve, propagate). */
VecArray *vec_expr_eval_extended(VecExprKind op, const VecExpr *expr,
                                  const VecBatch *batch);

/* Sub-dispatcher for geometry operations: a libgeos op (selected by
   expr->geom_fn) over a hex-WKB geometry column. See expr_geom.c. */
VecArray *vec_expr_eval_geom(const VecExpr *expr, const VecBatch *batch);

/* Sub-dispatcher for embedding distance ops (cosine/l2/dot, selected by
   expr->vec_fn) over a hex float32 embedding column. See expr_vec.c. */
VecArray *vec_expr_eval_vec(const VecExpr *expr, const VecBatch *batch);

/* Result column type for a geometry op given its geom_fn discriminator. */
VecType vec_expr_geom_result_type(char geom_fn);

/* Walk an expression tree and mark all referenced column names.
   needed[i] is set to 1 if column col_names[i] is referenced. */
void vec_expr_collect_colrefs(const VecExpr *expr, char **col_names,
                              int n_cols, uint8_t *needed);

#endif /* VECTRA_EXPR_H */
