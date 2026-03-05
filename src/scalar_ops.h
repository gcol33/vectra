#ifndef VECTRA_SCALAR_OPS_H
#define VECTRA_SCALAR_OPS_H

#include "types.h"

/* Arithmetic: result = left op right. Both arrays must have same length.
   op: '+', '-', '*', '/', '%'
   Returns a new VecArray (caller frees). */
VecArray *vec_arith(const VecArray *left, const VecArray *right, char op);

/* Comparison: result = left op right. Returns VEC_BOOL array.
   op+op2 encode: "< ", "> ", "<=", ">=", "==", "!=" */
VecArray *vec_cmp(const VecArray *left, const VecArray *right, char op, char op2);

/* Boolean: result = left op right. op: '&' or '|'. Both must be VEC_BOOL. */
VecArray *vec_bool_binary(const VecArray *left, const VecArray *right, char op);

/* Boolean NOT. Input must be VEC_BOOL. */
VecArray *vec_bool_not(const VecArray *arr);

/* Unary negate (- x). Works on int64 and double. */
VecArray *vec_negate(const VecArray *arr);

#endif /* VECTRA_SCALAR_OPS_H */
