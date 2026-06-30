# Time-series resampling: bucket a datetime column to a calendar grid and
# aggregate within each bucket. Composes the existing mutate / group_by /
# summarise verbs, with floor_time() (see expr.R / expr_datetime.c) deriving
# the bucket key inside the engine.

#' Floor a datetime column to a calendar grid
#'
#' Truncates a `Date` or `POSIXct` column down to a unit boundary -- the basis
#' for time bucketing. Usable inside [mutate()] / [filter()]; [resample()]
#' calls it for you.
#'
#' @param t A `Date` or `POSIXct` column (stored as days / seconds since the
#'   epoch).
#' @param unit A bucket size as a string: a count and a unit, e.g. `"hour"`,
#'   `"15 min"`, `"day"`, `"week"`, `"3 months"`, `"quarter"`, `"year"`.
#'
#' @details
#' The result is returned in the column's own scale (days for `Date`, seconds
#' for `POSIXct`), so it remains a valid instant. The engine does not re-attach
#' the `Date`/`POSIXct` class to a computed column, so a floored column
#' collects as the underlying numeric epoch; wrap with `as.POSIXct(x, origin =
#' "1970-01-01", tz = "UTC")` or `as.Date(x, origin = "1970-01-01")` if you
#' need the class back.
#'
#' @return A numeric (epoch) column.
#' @seealso [resample()]
#' @name floor_time
#' @export
floor_time <- function(t, unit)
  stop("floor_time() is only usable inside a vectra mutate()/filter()/resample()")

#' Time-based rolling aggregates
#'
#' Trailing rolling aggregates over a datetime window, usable inside [mutate()].
#' For each row, the aggregate covers the rows whose `time` falls in
#' `(time - every, time]` -- the row itself and everything within one window
#' before it. With an upstream [group_by()], windows are computed within each
#' group. `NA` values are skipped.
#'
#' `time` must be a `Date` or `POSIXct` column; `every` is a fixed-width
#' duration string (`"15 min"`, `"1 hour"`, `"7 days"`) -- calendar units
#' (month, year) are not allowed because their length varies.
#'
#' @param x Value column to aggregate (not needed for `roll_n`).
#' @param time Datetime column defining the window.
#' @param every Window span as a fixed-width duration string.
#'
#' @return A double column.
#'
#' @examples
#' \dontrun{
#' tbl("readings.vtr") |>
#'   group_by(sensor) |>
#'   mutate(avg_1h = roll_mean(temp, t, "1 hour"),
#'          n_1h   = roll_n(t, "1 hour")) |>
#'   collect()
#' }
#'
#' @name rolling
#' @seealso [resample()]
NULL

#' @rdname rolling
#' @export
roll_sum <- function(x, time, every) stop("roll_sum() is only usable inside a vectra mutate()")
#' @rdname rolling
#' @export
roll_mean <- function(x, time, every) stop("roll_mean() is only usable inside a vectra mutate()")
#' @rdname rolling
#' @export
roll_min <- function(x, time, every) stop("roll_min() is only usable inside a vectra mutate()")
#' @rdname rolling
#' @export
roll_max <- function(x, time, every) stop("roll_max() is only usable inside a vectra mutate()")
#' @rdname rolling
#' @export
roll_n <- function(time, every) stop("roll_n() is only usable inside a vectra mutate()")

#' Resample a time series to a calendar grid
#'
#' Buckets a datetime column to a grid (`every`) and aggregates within each
#' bucket -- the time-series form of `group_by()` + `summarise()`. Equivalent
#' to `mutate(<time> = floor_time(<time>, every))`, `group_by(<time>)`, then
#' `summarise(...)`.
#'
#' @param .data A `vectra_node`.
#' @param time The datetime column to bucket (unquoted).
#' @param every Bucket size as a string, e.g. `"1 hour"`, `"15 min"`, `"day"`,
#'   `"month"` (see [floor_time()]).
#' @param ... Named aggregation expressions, as in [summarise()] (e.g.
#'   `mean_temp = mean(temp)`).
#' @param .name Optional name for the bucket column. Defaults to the name of
#'   `time` (the original column, replaced by its floored value).
#'
#' @return A `vectra_node` with one row per occupied bucket: the bucket column
#'   followed by the aggregates. The bucket collects as a numeric epoch value
#'   (see [floor_time()] on restoring the date class).
#'
#' @examples
#' \dontrun{
#' tbl("readings.vtr") |>
#'   resample(timestamp, "1 hour", mean_temp = mean(temp), n = n()) |>
#'   collect()
#' }
#'
#' @seealso [floor_time()], [summarise()]
#' @export
resample <- function(.data, time, every, ..., .name = NULL) {
  UseMethod("resample")
}

#' @export
resample.vectra_node <- function(.data, time, every, ..., .name = NULL) {
  time_sym <- substitute(time)
  if (is.character(time_sym)) time_sym <- as.name(time_sym)
  if (!is.name(time_sym))
    stop("`time` must be a column name")
  tcol <- as.character(time_sym)

  if (!is.character(every) || length(every) != 1L)
    stop("`every` must be a single string, e.g. \"1 hour\"")

  dots <- eval(substitute(alist(...)))
  if (length(dots) == 0L || is.null(names(dots)) || any(names(dots) == ""))
    stop("resample() needs at least one named aggregation, e.g. mean_x = mean(x)")

  bucket <- if (is.null(.name)) tcol else .name

  # Build: summarise(group_by(mutate(.data, <bucket> = floor_time(<time>, every)),
  #                            <bucket>),
  #                  ...dots...)
  floor_call <- bquote(floor_time(.(as.name(tcol)), .(every)))
  mutate_call <- as.call(c(as.name("mutate"), list(quote(.data)),
                           stats::setNames(list(floor_call), bucket)))
  group_call  <- as.call(list(as.name("group_by"), mutate_call, as.name(bucket)))
  summ_call   <- as.call(c(as.name("summarise"), list(group_call), dots))

  env <- new.env(parent = parent.frame())
  env$.data <- .data
  eval(summ_call, env)
}
