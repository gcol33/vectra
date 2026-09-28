#include "expr.h"
#include "array.h"
#include "coerce.h"
#include "error.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

/* Elementwise numeric functions, one table per arity. Each entry is the R
 * function name the serializer emits and a scalar kernel; parse time resolves
 * the name to a table index, evaluation runs the kernel over the batch. NA
 * (validity) skips the unary kernels and is passed to the binary ones as R's
 * NA payload; NaN propagates through the kernels, as it does in R. */

typedef double (*VecMathUnaryFn)(double);
typedef double (*VecMathBinaryFn)(double, double);

static double math_sign(double v) {
    if (isnan(v)) return v;
    return (v > 0) ? 1.0 : (v < 0) ? -1.0 : 0.0;
}

/* A binary kernel has to tell NA from NaN, because R does: an NA argument
   enters as a NaN carrying R's NA payload (low word 1954, as R_IsNA tests),
   and a result carrying that payload leaves as NA. */
static double math_na(void) {
    union { double d; uint64_t u; } v;
    v.u = 0x7FF00000000007A2ULL;
    return v.d;
}

static int math_is_na(double x) {
    union { double d; uint64_t u; } v;
    v.d = x;
    return isnan(x) && (uint32_t)v.u == 1954u;
}

/* An NA or NaN argument yields NA if either is NA, else NaN. */
static double math_nan_of(double a, double b) {
    return (math_is_na(a) || math_is_na(b)) ? math_na() : NAN;
}

/* R's pmin/pmax: a later NA/NaN argument replaces the running result, an
   earlier one is kept (pmax(NA, NaN) is NaN, pmin(NaN, NA) is NA). */
static double math_pmin(double a, double b) {
    if (isnan(b)) return b;
    if (isnan(a)) return a;
    return a < b ? a : b;
}

static double math_pmax(double a, double b) {
    if (isnan(b)) return b;
    if (isnan(a)) return a;
    return a > b ? a : b;
}

/* R_pow: 1^y and x^0 are 1 even for NA/NaN. */
static double math_pow(double a, double b) {
    if (a == 1.0 || b == 0.0) return 1.0;
    if (isnan(a) || isnan(b)) return math_nan_of(a, b);
    return pow(a, b);
}

static double math_atan2(double a, double b) {
    if (isnan(a) || isnan(b)) return math_nan_of(a, b);
    return atan2(a, b);
}

static const struct { const char *name; VecMathUnaryFn fn; } math_unary[] = {
    { "abs",     fabs  },
    { "sqrt",    sqrt  },
    { "exp",     exp   },
    { "expm1",   expm1 },
    { "log",     log   },
    { "log1p",   log1p },
    { "log2",    log2  },
    { "log10",   log10 },
    { "floor",   floor },
    { "ceiling", ceil  },
    { "round",   rint  }, /* round half to even, like R */
    { "trunc",   trunc },
    { "sign",    math_sign },
    { "sin",     sin   },
    { "cos",     cos   },
    { "tan",     tan   },
    { "asin",    asin  },
    { "acos",    acos  },
    { "atan",    atan  },
    { "sinh",    sinh  },
    { "cosh",    cosh  },
    { "tanh",    tanh  },
    { "asinh",   asinh },
    { "acosh",   acosh },
    { "atanh",   atanh },
};

static const struct { const char *name; VecMathBinaryFn fn; } math_binary[] = {
    { "pmin",  math_pmin },
    { "pmax",  math_pmax },
    { "atan2", math_atan2 },
    { "^",     math_pow   },
};

#define N_MATH_UNARY  ((int)(sizeof(math_unary)  / sizeof(math_unary[0])))
#define N_MATH_BINARY ((int)(sizeof(math_binary) / sizeof(math_binary[0])))

int vec_math_lookup(const char *name, int arity) {
    if (name == NULL) return -1;
    if (arity == 1) {
        for (int k = 0; k < N_MATH_UNARY; k++)
            if (strcmp(math_unary[k].name, name) == 0) return k;
    } else if (arity == 2) {
        for (int k = 0; k < N_MATH_BINARY; k++)
            if (strcmp(math_binary[k].name, name) == 0) return k;
    }
    return -1;
}

/* Evaluate an operand as a double array. Any non-double operand (int64, bool)
   is coerced: reading a narrower buffer through buf.dbl would over-read, and a
   string operand errors cleanly in vec_coerce. */
static VecArray *eval_double(const VecExpr *e, const VecBatch *batch) {
    VecArray *a = vec_expr_eval(e, batch);
    if (a->type == VEC_DOUBLE) return a;
    VecArray *d = vec_coerce(a, VEC_DOUBLE);
    vec_array_free(a); free(a);
    return d;
}

VecArray *vec_expr_eval_math(const VecExpr *expr, const VecBatch *batch) {
    VecArray *out = (VecArray *)malloc(sizeof(VecArray));
    if (!out) vectra_error("out of memory");

    if (expr->kind == EXPR_MATH_UNARY) {
        if (expr->math_fn < 0 || expr->math_fn >= N_MATH_UNARY)
            vectra_error("unknown math function index: %d", expr->math_fn);
        VecMathUnaryFn fn = math_unary[expr->math_fn].fn;
        VecArray *x = eval_double(expr->operand, batch);
        *out = vec_array_alloc(VEC_DOUBLE, x->length);
        for (int64_t i = 0; i < x->length; i++) {
            if (!vec_array_is_valid(x, i)) { vec_array_set_null(out, i); continue; }
            vec_array_set_valid(out, i);
            out->buf.dbl[i] = fn(x->buf.dbl[i]);
        }
        vec_array_free(x); free(x);
        return out;
    }

    if (expr->math_fn < 0 || expr->math_fn >= N_MATH_BINARY)
        vectra_error("unknown math function index: %d", expr->math_fn);
    VecMathBinaryFn fn = math_binary[expr->math_fn].fn;
    VecArray *l = eval_double(expr->left, batch);
    VecArray *r = eval_double(expr->right, batch);
    int64_t n = l->length;
    *out = vec_array_alloc(VEC_DOUBLE, n);
    for (int64_t i = 0; i < n; i++) {
        double a = vec_array_is_valid(l, i) ? l->buf.dbl[i] : math_na();
        double b = vec_array_is_valid(r, i) ? r->buf.dbl[i] : math_na();
        double res = fn(a, b);
        if (math_is_na(res)) { vec_array_set_null(out, i); continue; }
        vec_array_set_valid(out, i);
        out->buf.dbl[i] = res;
    }
    vec_array_free(l); free(l);
    vec_array_free(r); free(r);
    return out;
}
