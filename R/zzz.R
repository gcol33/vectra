#' @useDynLib vectra, .registration = TRUE
#' @importFrom rlang expr enquo is_formula as_function
NULL

# Build a 0-row data.frame proxy from schema, enabling where() etc.
# Maps vectra types to R column types so tidyselect predicates work.
# Annotations override the base type (Date, POSIXct -> double with class;
# factor -> integer with levels).
schema_proxy <- function(schema) {
  n <- length(schema$name)
  ann <- if (!is.null(schema$annotation)) schema$annotation else rep(NA_character_, n)
  cols <- vector("list", n)
  for (i in seq_len(n)) {
    a <- ann[i]
    if (!is.na(a) && startsWith(a, "factor")) {
      cols[[i]] <- factor(character(0))
    } else if (!is.na(a) && a == "Date") {
      cols[[i]] <- structure(double(0), class = "Date")
    } else if (!is.na(a) && startsWith(a, "POSIXct")) {
      cols[[i]] <- structure(double(0), class = c("POSIXct", "POSIXt"))
    } else {
      cols[[i]] <- switch(schema$type[i],
        int64  = integer(0),
        double = double(0),
        bool   = logical(0),
        string = character(0),
        double(0)
      )
    }
  }
  names(cols) <- schema$name
  structure(cols, class = "data.frame", row.names = integer(0))
}

.onLoad <- function(libname, pkgname) {
  # Nothing needed; .Call registration is done in init.c
}

.onUnload <- function(libpath) {
  library.dynam.unload("vectra", libpath)
}
