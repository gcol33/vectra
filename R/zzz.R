#' @useDynLib vectra, .registration = TRUE
#' @importFrom rlang expr enquo is_formula as_function
NULL

.onLoad <- function(libname, pkgname) {
  # Nothing needed; .Call registration is done in init.c
}

.onUnload <- function(libpath) {
  library.dynam.unload("vectra", libpath)
}
