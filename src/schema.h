#ifndef VECTRA_SCHEMA_H
#define VECTRA_SCHEMA_H

#include "types.h"

/* Create a schema (deep copies names) */
VecSchema vec_schema_create(int n_cols, char **col_names, VecType *col_types);

/* Free schema internals */
void vec_schema_free(VecSchema *schema);

/* Deep copy a schema */
VecSchema vec_schema_copy(const VecSchema *src);

/* Find column index by name, returns -1 if not found */
int vec_schema_find_col(const VecSchema *schema, const char *name);

/* Deep copy of a's columns followed by b's. Raises a vectra_error if a name
   in b collides with one in a or with another name in b -- column names are
   how every verb identifies a column, so a collision is an error rather
   than something to disambiguate. */
VecSchema vec_schema_concat(const VecSchema *a, const VecSchema *b);

#endif /* VECTRA_SCHEMA_H */
