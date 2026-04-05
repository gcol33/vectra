#' Define a link between a fact table and a dimension table
#'
#' Creates a link descriptor that specifies how to join a dimension table
#' to a fact table via one or more key columns.
#'
#' @param key A character vector or named character vector specifying join keys.
#'   Unnamed: same column name in both tables. Named: `c("fact_col" = "dim_col")`.
#' @param node A `vectra_node` object (the dimension table). Must be file-backed
#'   (created via [tbl()], [tbl_csv()], or [tbl_sqlite()]).
#'
#' @return A `vectra_link` object.
#'
#' @examples
#' \donttest{
#' f_obs <- tempfile(fileext = ".vtr")
#' f_sp  <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(sp_id = 1:3, value = c(10, 20, 30)), f_obs)
#' write_vtr(data.frame(sp_id = 1:3, name = c("A", "B", "C")), f_sp)
#' lnk <- link("sp_id", tbl(f_sp))
#' unlink(c(f_obs, f_sp))
#' }
#'
#' @export
link <- function(key, node) {
  if (!is.character(key) || length(key) == 0)
    stop("key must be a non-empty character vector")
  if (!inherits(node, "vectra_node"))
    stop(sprintf("node must be a vectra_node, got %s", class(node)[1]))
  if (is.null(node$.path))
    stop("schema links require file-backed nodes (created via tbl, tbl_csv, etc.)")
  structure(list(key = key, node = node), class = "vectra_link")
}

#' Create a star schema over linked vectra tables
#'
#' Registers a fact table with named dimension links. The schema enables
#' [lookup()] to resolve columns from dimension tables without writing
#' explicit joins.
#'
#' @param fact A `vectra_node` object (the central fact table). Must be
#'   file-backed (created via [tbl()], [tbl_csv()], or [tbl_sqlite()]).
#' @param ... Named `vectra_link` objects created by [link()]. Names become
#'   the dimension aliases used in [lookup()] (e.g., `species$name`).
#'
#' @return A `vectra_schema` object.
#'
#' @examples
#' \donttest{
#' f_obs <- tempfile(fileext = ".vtr")
#' f_sp  <- tempfile(fileext = ".vtr")
#' f_ct  <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(sp_id = 1:3, ct_code = c("AT", "DE", "FR"),
#'                       value = 10:12), f_obs)
#' write_vtr(data.frame(sp_id = 1:3,
#'                       name = c("Oak", "Beech", "Pine")), f_sp)
#' write_vtr(data.frame(ct_code = c("AT", "DE", "FR"),
#'                       gdp = c(400, 3800, 2700)), f_ct)
#'
#' s <- vtr_schema(
#'   fact    = tbl(f_obs),
#'   species = link("sp_id", tbl(f_sp)),
#'   country = link("ct_code", tbl(f_ct))
#' )
#' print(s)
#' unlink(c(f_obs, f_sp, f_ct))
#' }
#'
#' @export
vtr_schema <- function(fact, ...) {
  if (!inherits(fact, "vectra_node"))
    stop(sprintf("fact must be a vectra_node, got %s", class(fact)[1]))
  if (is.null(fact$.path))
    stop("fact must be a file-backed node (created via tbl, tbl_csv, etc.)")

  dims <- list(...)
  if (length(dims) == 0)
    stop("at least one dimension link is required")

  dim_names <- names(dims)
  if (is.null(dim_names) || any(dim_names == ""))
    stop("all dimension links must be named (e.g., species = link(...))")

  for (nm in dim_names) {
    if (!inherits(dims[[nm]], "vectra_link"))
      stop(sprintf("'%s' must be a vectra_link created by link(), got %s",
                    nm, class(dims[[nm]])[1]))
  }

  structure(
    list(fact = fact, dims = dims),
    class = "vectra_schema"
  )
}

#' @export
print.vectra_schema <- function(x, ...) {
  fact_schema <- .Call(C_node_schema, x$fact$.node)
  cat("vectra schema\n")
  cat(sprintf("Fact table: %d columns\n", length(fact_schema$name)))

  for (nm in names(x$dims)) {
    lnk <- x$dims[[nm]]
    dim_schema <- .Call(C_node_schema, lnk$node$.node)
    key_str <- format_key_str(lnk$key)
    cat(sprintf("  %s: %d columns (key: %s)\n",
                nm, length(dim_schema$name), key_str))
  }
  invisible(x)
}

# Format a join key spec for display
format_key_str <- function(key) {
  if (is.null(names(key))) return(paste(key, collapse = ", "))
  parts <- character(length(key))
  for (i in seq_along(key)) {
    k <- names(key)[i]
    if (k == "") k <- key[i]
    parts[i] <- if (k == key[i]) k else paste0(k, " = ", key[i])
  }
  paste(parts, collapse = ", ")
}

# Re-open a node from its stored file path (fresh external pointer)
reopen_node <- function(node) {
  path <- node$.path
  ext <- tolower(tools::file_ext(path))
  if (ext == "gz" && grepl("\\.csv\\.gz$", tolower(path))) ext <- "csv"
  switch(ext,
    vtr = tbl(path),
    csv = tbl_csv(path),
    sqlite = , db = tbl_sqlite(path, node$.table),
    stop(sprintf("cannot re-open node from .%s file", ext))
  )
}

#' Look up columns from linked dimension tables
#'
#' Resolves columns from dimension tables registered in a [vtr_schema()],
#' automatically building the necessary join tree. Reports unmatched keys
#' as a diagnostic message.
#'
#' Column references use `dimension$column` syntax (e.g., `species$name`).
#' Columns from the fact table can be referenced by name directly.
#'
#' @param .schema A `vectra_schema` object.
#' @param ... Column references: bare names for fact columns, or
#'   `dimension$column` for dimension columns.
#' @param .join Join type: `"left"` (default, keeps all fact rows) or
#'   `"inner"` (drops unmatched fact rows).
#' @param .report Logical. If `TRUE` (default), print a message with
#'   the number of unmatched keys per dimension.
#'
#' @return A `vectra_node` with the selected columns.
#'
#' @details
#' When `.report = TRUE`, each needed dimension is checked for unmatched keys
#' by opening fresh scans of the fact and dimension tables. This adds one
#' extra read pass per dimension but does not affect the lazy result node.
#'
#' Only dimensions referenced in `...` are joined. Unreferenced dimensions
#' are never scanned.
#'
#' @examples
#' \donttest{
#' f_obs <- tempfile(fileext = ".vtr")
#' f_sp  <- tempfile(fileext = ".vtr")
#' f_ct  <- tempfile(fileext = ".vtr")
#' write_vtr(data.frame(sp_id = 1:4, ct_code = c("AT", "DE", "FR", "XX"),
#'                       value = 10:13), f_obs)
#' write_vtr(data.frame(sp_id = 1:3,
#'                       name = c("Oak", "Beech", "Pine")), f_sp)
#' write_vtr(data.frame(ct_code = c("AT", "DE", "FR"),
#'                       gdp = c(400, 3800, 2700)), f_ct)
#'
#' s <- vtr_schema(
#'   fact    = tbl(f_obs),
#'   species = link("sp_id", tbl(f_sp)),
#'   country = link("ct_code", tbl(f_ct))
#' )
#'
#' # Pull columns from any linked dimension
#' result <- lookup(s, value, species$name, country$gdp)
#' collect(result)
#'
#' unlink(c(f_obs, f_sp, f_ct))
#' }
#'
#' @export
lookup <- function(.schema, ..., .join = "left", .report = TRUE) {
  UseMethod("lookup")
}

#' @export
lookup.vectra_schema <- function(.schema, ..., .join = "left", .report = TRUE) {
  if (!.join %in% c("left", "inner"))
    stop(sprintf(".join must be 'left' or 'inner', got '%s'", .join))

  exprs <- eval(substitute(alist(...)))
  if (length(exprs) == 0)
    stop("at least one column reference is required")

  # Parse each expression into {dim, col} pairs
  requests <- parse_lookup_refs(exprs, names(.schema$dims))

  # Determine which dimensions are needed
  needed_dims <- unique(vapply(
    Filter(function(r) !is.null(r$dim), requests),
    function(r) r$dim, character(1)
  ))

  # Report unmatched keys (uses fresh nodes, does not affect the join tree)
  if (.report && length(needed_dims) > 0) {
    report_unmatched(.schema, needed_dims)
  }

  # Build join tree from fresh nodes
  node <- reopen_node(.schema$fact)
  for (dim_name in needed_dims) {
    lnk <- .schema$dims[[dim_name]]
    dim_node <- reopen_node(lnk$node)
    keys <- parse_join_keys(node, dim_node, lnk$key)
    new_xptr <- .Call(C_join_node, node$.node, dim_node$.node,
                      .join, keys$left, keys$right, ".x", ".y")
    node <- structure(list(.node = new_xptr, .path = NULL),
                      class = "vectra_node")
  }

  # Select only the requested columns
  out_names <- vapply(requests, function(r) r$col, character(1))
  expr_lists <- vector("list", length(out_names))
  new_xptr <- .Call(C_project_node, node$.node, out_names, expr_lists)
  structure(list(.node = new_xptr, .path = NULL), class = "vectra_node")
}

# Parse lookup column references: bare names or dim$col expressions
parse_lookup_refs <- function(exprs, dim_names) {
  requests <- vector("list", length(exprs))
  for (i in seq_along(exprs)) {
    e <- exprs[[i]]
    if (is.call(e) && identical(e[[1]], as.name("$"))) {
      dim_name <- as.character(e[[2]])
      col_name <- as.character(e[[3]])
      if (!dim_name %in% dim_names)
        stop(sprintf("dimension '%s' not found in schema (available: %s)",
                      dim_name, paste(dim_names, collapse = ", ")))
      requests[[i]] <- list(dim = dim_name, col = col_name)
    } else {
      requests[[i]] <- list(dim = NULL, col = as.character(e))
    }
  }
  requests
}

# Report unmatched keys for each dimension (opens fresh nodes)
report_unmatched <- function(schema, needed_dims) {
  for (dim_name in needed_dims) {
    lnk <- schema$dims[[dim_name]]

    # Fresh nodes for the anti_join
    fact_node <- reopen_node(schema$fact)
    dim_node <- reopen_node(lnk$node)
    keys <- parse_join_keys(fact_node, dim_node, lnk$key)

    anti_xptr <- .Call(C_join_node, fact_node$.node, dim_node$.node,
                       "anti", keys$left, keys$right, ".x", ".y")
    anti_node <- structure(list(.node = anti_xptr, .path = NULL),
                           class = "vectra_node")
    anti_df <- .Call(C_collect, anti_node$.node)
    n_unmatched <- nrow(anti_df)

    # Count total unique keys in fact table
    fact_node2 <- reopen_node(schema$fact)
    fact_df <- .Call(C_collect, fact_node2$.node)
    n_total <- nrow(fact_df)

    if (n_unmatched > 0) {
      unmatched_keys <- unique(anti_df[[keys$left[1]]])
      preview <- if (length(unmatched_keys) <= 5) {
        paste(format(unmatched_keys, trim = TRUE), collapse = ", ")
      } else {
        paste(c(format(head(unmatched_keys, 5), trim = TRUE), "..."),
              collapse = ", ")
      }
      message(sprintf("%s: %d/%d unmatched keys (%s)",
                      dim_name, n_unmatched, n_total, preview))
    } else {
      message(sprintf("%s: all %d keys matched", dim_name, n_total))
    }
  }
}
