# node.R - construction of derived plan nodes.
#
# A verb wraps the C node it built in a new R-level `vectra_node`. The fields
# that describe the data rather than the plan travel with it: the source path,
# the grouping, the spill registry keeping run files alive, and the spatial
# metadata (`.crs`, and `.geom`, the name of the geometry column). Building the
# wrapper in one place is what lets `collect()` see that the result is spatial
# after any number of filters, selects and mutates.

.derive_node <- function(parent, xptr, groups = NULL, path = parent$.path) {
  node <- structure(list(.node = xptr, .path = path, .groups = groups),
                    class = "vectra_node")
  node$.reg <- parent$.reg
  node$.crs <- parent$.crs
  geom <- parent$.geom
  if (!is.null(geom) && geom %in% .Call(C_node_schema, xptr)$name)
    node$.geom <- geom
  node
}
