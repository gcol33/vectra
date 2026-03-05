#ifndef VECTRA_COERCE_H
#define VECTRA_COERCE_H

#include "types.h"

/* Coerce an array to a target type. Returns a new VecArray (caller frees).
   Supported: bool->int64->double. */
VecArray *vec_coerce(const VecArray *arr, VecType target);

/* Determine common type for binary operation. */
VecType vec_common_type(VecType a, VecType b);

#endif /* VECTRA_COERCE_H */
