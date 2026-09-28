# ---------------------------------------------------------------------------
# Memory budget: single source of truth
# ---------------------------------------------------------------------------
#
# Every subsystem that buffers rows before spilling or flushing -- the external
# sort, self-overlay tiling, spatial run-file flushes, partition routing, and the
# spilling hash join -- derives its working budget from one number: vectra_mem().
# Within one query plan the budget is one pool: before execution the C engine
# (plan_budget.c) gives each budgeted node a grant on it, and each node reserves
# what it actually allocates (buffer capacity, sort permutation and scratch,
# hash table) as it grows, spilling when the pool refuses.
# Set it for the session with options(vectra.memory = "8GB") (a string with a
# K/M/G/T suffix) or options(vectra.memory = 8e9) (a byte count). Row-group size
# (batch_size) is a separate, orthogonal cache-locality knob, not a memory cap.

.VECTRA_MEM_FLOOR <- 1073741824  # 1 GB: no subsystem may budget below this

# Parse a memory size given as bytes or a string like "8GB", "512MB", "1.5g".
# Bare positive numbers pass through as bytes.
.parse_bytes <- function(x) {
  if (is.numeric(x)) {
    if (length(x) != 1L || !is.finite(x) || x <= 0)
      stop("memory limit must be a single positive number of bytes", call. = FALSE)
    return(as.numeric(x))
  }
  if (!is.character(x) || length(x) != 1L)
    stop('memory limit must be a number of bytes or a string like "8GB"', call. = FALSE)
  m <- regmatches(x, regexec("^\\s*([0-9]*\\.?[0-9]+)\\s*([kKmMgGtT]?)[bB]?\\s*$", x))[[1L]]
  if (length(m) != 3L)
    stop(sprintf('cannot parse memory limit "%s"; use e.g. "8GB" or a byte count', x),
         call. = FALSE)
  mult <- switch(toupper(m[3L]), K = 1024, M = 1024^2, G = 1024^3, T = 1024^4, 1)
  as.numeric(m[2L]) * mult
}

#' Resolve the vectra memory budget
#'
#' The single memory ceiling every buffering subsystem derives its working budget
#' from: the external sort's spill threshold, the self-overlay tile cap, spatial
#' run-file flushes, partition routing, and the spilling hash join.
#'
#' The budget bounds a whole query, not each step of it. When a query holds
#' several buffering steps at once (a join feeding a grouped `summarise()`,
#' an `arrange()` over a join), they draw on one shared pool of this size. A
#' step reserves what it actually allocates as it grows (buffered rows at their
#' allocated capacity, the permutation and scratch a sort needs, the hash table
#' a join builds) and spills when the pool refuses. A step that needs little
#' leaves the rest to the others; each step is always guaranteed
#' `1 / (4 * steps)` of the budget, so none is starved. The process peak is
#' then about the budget plus a fixed allowance for R itself, the batches in
#' flight and the collected result.
#'
#' Resolution order is an explicit `limit`, then `getOption("vectra.memory")`, then a default
#' of half the detected system RAM. The auto-detected default is floored at 1 GB;
#' an explicit `limit` or option is honored as given (down to 1 KB), so a smaller
#' budget can be requested deliberately.
#'
#' Set the session budget with `options(vectra.memory = "8GB")` (a string with a
#' K/M/G/T suffix) or `options(vectra.memory = 8e9)` (a byte count). Row-group
#' size (`batch_size`) is a separate cache-locality knob and is not affected.
#'
#' @param limit Optional per-call override: a byte count or a string such as
#'   `"8GB"`. `NULL` (the default) falls back to the option, then to
#'   auto-detection.
#' @return The budget in bytes, as a numeric scalar: at least 1 GB when
#'   auto-detected, and as given (down to 1 KB) when set explicitly.
#' @export
#' @examples
#' vectra_mem()
#' vectra_mem("4GB")
vectra_mem <- function(limit = NULL) {
  raw <- if (!is.null(limit)) limit else getOption("vectra.memory", NULL)
  if (is.null(raw)) {
    # Auto-detected default: half of RAM, floored at 1 GB so a tiny or
    # undetectable machine still gets a workable budget.
    ram <- .sys_ram_bytes()
    bytes <- if (is.na(ram)) 4 * 1024^3 else 0.5 * ram
    max(bytes, .VECTRA_MEM_FLOOR)
  } else {
    # An explicit budget is honored as given (a small 1 KB sanity floor only),
    # so a user who asks for "512MB" gets it and tests can force spill paths.
    max(.parse_bytes(raw), 1024)
  }
}

# Convert a byte budget to a row count given an estimated bytes-per-row, for
# subsystems whose natural flush unit is rows. Floored at 1 so a very wide row
# never yields a zero-row (non-terminating) flush.
.mem_rows <- function(bytes, est_row_bytes) {
  max(1L, as.integer(bytes / max(est_row_bytes, 1)))
}

# Working-set budget in bytes for a single streaming run-file buffer (the spatial
# flush accumulator and the partition routers). A bounded fraction of the overall
# ceiling, so several buffers plus the rest of the pipeline stay within
# vectra_mem(); scales with the one knob. A run-file buffer flushes once its
# accumulated bytes cross this.
.stream_bytes <- function() vectra_mem() / 8

# Peak resident set size of this R process in bytes (peak working set on
# Windows), or -1 where the platform does not report it. Used by the tests
# that check vectra_mem() is a bound.
.peak_rss <- function() .Call(C_peak_rss)
