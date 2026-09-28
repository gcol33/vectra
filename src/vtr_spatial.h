/* Resident GEOS locator and batch matching, shared between the streamed spatial
 * verbs (vtr_spatial.c) and the SpatialJoinNode (spatial_join.c). */
#ifndef VTR_SPATIAL_H
#define VTR_SPATIAL_H

#include <stddef.h>
#include <R.h>
#include <Rinternals.h>

/* Predicate code for the single nearest resident feature (st_nearest_feature). */
#define VTR_SPATIAL_PRED_NEAREST 11

/* Parsed resident geometries plus a pre-built STRtree, safe to query from many
 * threads at once. */
typedef struct GeosLocator GeosLocator;

/* Parse a VECSXP of RAWSXP WKB into a locator. Raises on allocation failure. */
GeosLocator *vtr_geos_locator_new(SEXP wkb_list);
void vtr_geos_locator_free(GeosLocator *loc);

/* Match `m` rows against the locator. The geometry of row r is the hex-WKB
 * hex[r] (length hexlen[r], NULL for NA) when `hex` is non-NULL, else the point
 * (xs[r], ys[r]) (NaN for NA). On return mptr[r] is a malloc'd array of the
 * 1-based resident indices row r relates to under `pred`, ascending, of length
 * mlen[r] (NULL / 0 for none); the caller frees each. VTR_SPATIAL_PRED_NEAREST
 * gives each row its single nearest feature. `nthreads` <= 0 means the OpenMP
 * default. Returns 0, or -1 on allocation failure with every mptr[r] freed.
 * Opens its own parallel region, so it is called from the master thread. */
int vtr_geos_match_batch(GeosLocator *loc, const unsigned char **hex,
                         const size_t *hexlen, const double *xs, const double *ys,
                         int m, int pred, double dist, int nthreads,
                         int **mptr, int *mlen);

#endif
