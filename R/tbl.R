#' Create a lazy table reference from a .vtr file
#'
#' Opens a vectra1 file and returns a lazy query node. No data is read until
#' [collect()] is called.
#'
#' @param path Path to a `.vtr` file.
#'
#' @return A `vectra_node` object representing a lazy scan of the file.
#'
#' @examples
#' f <- tempfile(fileext = ".vtr")
#' write_vtr(mtcars, f)
#' node <- tbl(f)
#' print(node)
#' unlink(f)
#'
#' @export
tbl <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_scan_node, path)
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a delimited text file
#'
#' Opens a delimited text file (CSV, TSV, or any single-character separator)
#' for lazy, streaming query execution. Column types are inferred from the
#' first 1000 rows. No data is read until [collect()] is called.
#' Gzip-compressed files (`.csv.gz`, `.tsv.gz`) are supported transparently.
#'
#' The field separator is set by `delim`, so tab-separated files (GBIF
#' occurrence exports, TSV dumps) and semicolon-separated files (many European
#' exports) are read natively without a transcode step. Quoting follows RFC
#' 4180 for every delimiter: a field wrapped in double quotes may contain the
#' delimiter, newlines, and doubled quotes.
#'
#' @param path Path to a delimited text file, optionally gzip-compressed.
#' @param batch_size Number of rows per batch (default 65536).
#' @param delim Single-character field separator (default `","`). Use `"\t"`
#'   for tab-separated and `";"` for semicolon-separated files.
#'
#' @return A `vectra_node` object representing a lazy scan of the file.
#'
#' @examples
#' f <- tempfile(fileext = ".csv")
#' write.csv(mtcars, f, row.names = FALSE)
#' node <- tbl_csv(f)
#' print(node)
#' unlink(f)
#'
#' # Tab-separated (e.g. a GBIF occurrence export)
#' g <- tempfile(fileext = ".tsv")
#' write.table(mtcars, g, sep = "\t", row.names = FALSE, quote = FALSE)
#' tbl_csv(g, delim = "\t") |> collect() |> head()
#' unlink(g)
#'
#' @export
tbl_csv <- function(path, batch_size = .DEFAULT_BATCH_SIZE, delim = ",") {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!is.character(delim) || length(delim) != 1 || is.na(delim))
    stop("delim must be a single character string")
  if (nchar(delim, type = "bytes") != 1)
    stop("delim must be a single-byte character (e.g. \",\", \"\\t\", \";\")")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_csv_scan_node, path, as.double(batch_size), delim)
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a FASTA file
#'
#' Streams a FASTA file of biological sequences as rows: one row per record
#' with columns `id`, `desc`, and `seq`. `id` is the first whitespace-delimited
#' token of the header line, `desc` is the remainder (an empty string when the
#' header carries no description), and `seq` is the sequence with line breaks
#' removed. Records stream one batch at a time, so a read set larger than RAM
#' never fully materializes. Gzip-compressed files (`.fasta.gz`, `.fa.gz`) are
#' read transparently. No data is read until [collect()] is called.
#'
#' The `seq_*` expression family (`seq_revcomp()`, `seq_gc()`, `seq_translate()`,
#' `seq_dist()`, ...) operates on the `seq` column directly inside
#' [mutate()]/[filter()]. See [seq_expressions].
#'
#' A record cut short is a loud error, not a silent drop: a header that is not
#' the first non-blank token, or a byte where a `>` is expected, stops the scan.
#' When the scan reaches the end of the file it reports the number of records
#' read (suppress with `quiet = TRUE`).
#'
#' @param path Path to a `.fasta`/`.fa` file, optionally gzip-compressed.
#' @param batch_size Number of records per batch (default 65536).
#' @param quiet If `FALSE` (default), report the record count when the scan
#'   completes.
#'
#' @return A `vectra_node` object representing a lazy scan of the FASTA file.
#'
#' @examples
#' f <- tempfile(fileext = ".fasta")
#' writeLines(c(">seq1 first", "ACGTACGT", ">seq2 second", "GGGGCCCC"), f)
#' node <- tbl_fasta(f, quiet = TRUE)
#' node |> mutate(gc = seq_gc(seq)) |> collect()
#' unlink(f)
#'
#' @seealso [tbl_fastq()], [seq_expressions]
#' @export
tbl_fasta <- function(path, batch_size = .DEFAULT_BATCH_SIZE, quiet = FALSE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_fasta_scan_node, path, as.double(batch_size),
                FALSE, isTRUE(quiet))
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a FASTQ file
#'
#' Streams a FASTQ file as rows: one row per record with columns `id`, `desc`,
#' `seq`, and `qual` (the raw quality string, same length as `seq`). `id` and
#' `desc` are split from the header as in [tbl_fasta()]. Records stream one
#' batch at a time, so a read set larger than RAM never fully materializes.
#' Gzip-compressed files (`.fastq.gz`, `.fq.gz`) are read transparently. No data
#' is read until [collect()] is called.
#'
#' Records are parsed in the standard four-line form (header, sequence, `+`
#' separator, quality). A record cut short --- a missing sequence, separator, or
#' quality line, or a quality string whose length does not match the sequence
#' --- is a loud error, not a silent drop. When the scan reaches the end of the
#' file it reports the number of records read (suppress with `quiet = TRUE`).
#'
#' @param path Path to a `.fastq`/`.fq` file, optionally gzip-compressed.
#' @param batch_size Number of records per batch (default 65536).
#' @param quiet If `FALSE` (default), report the record count when the scan
#'   completes.
#'
#' @return A `vectra_node` object representing a lazy scan of the FASTQ file.
#'
#' @examples
#' f <- tempfile(fileext = ".fastq")
#' writeLines(c("@r1 read one", "ACGT", "+", "IIII",
#'              "@r2 read two", "GGCC", "+", "!!!!"), f)
#' node <- tbl_fastq(f, quiet = TRUE)
#' node |> mutate(len = seq_length(seq)) |> collect()
#' unlink(f)
#'
#' @seealso [tbl_fasta()], [seq_expressions]
#' @export
tbl_fastq <- function(path, batch_size = .DEFAULT_BATCH_SIZE, quiet = FALSE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_fasta_scan_node, path, as.double(batch_size),
                TRUE, isTRUE(quiet))
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a BED file
#'
#' Streams a BED (Browser Extensible Data) file of genomic features as rows,
#' one row per feature. Columns follow the BED specification in order:
#' `chrom`, `start`, `end`, `name`, `score`, `strand`, `thickStart`,
#' `thickEnd`, `itemRgb`, `blockCount`, `blockSizes`, `blockStarts`; any
#' fields beyond the twelfth (a "BED N+" file) are read as strings named
#' `V13`, `V14`, ... The column count is fixed by the first feature line and
#' every later line must match it. Features stream one batch at a time, so a
#' genome-scale interval set never fully materializes. Gzip-compressed files
#' (`.bed.gz`) are read transparently. No data is read until [collect()] is
#' called.
#'
#' Fields are whitespace-delimited (tab or space), and blank lines, `#`
#' comments, and UCSC `track`/`browser` directive lines are skipped. `chrom`,
#' `start`, and `end` are required; `start` and `end` are integers (a
#' non-integer there stops the scan). Optional integer fields (`score`,
#' `thickStart`, `thickEnd`, `blockCount`) accept `.` or `NA` as missing.
#' When the scan reaches the end of the file it reports the number of features
#' read (suppress with `quiet = TRUE`).
#'
#' **Coordinates are read faithfully.** BED `start` is 0-based and `end` is
#' half-open (the first position past the feature), and both are returned
#' exactly as stored --- no coordinate is rewritten. Two features that abut
#' (`end` of one equal to `start` of the next) share no base. To overlap-join
#' BED tables with those half-open semantics --- matching bedtools and
#' `GenomicRanges::findOverlaps()` --- pass `closed = FALSE` to
#' [interval_join()], which requires a strictly positive overlap and so does
#' not pair abutting features:
#'
#' ```r
#' peaks <- tbl_bed("peaks.bed")
#' genes <- tbl_bed("genes.bed")
#' interval_join(peaks, genes, start = "start", end = "end",
#'               by = "chrom", closed = FALSE)
#' ```
#'
#' @param path Path to a `.bed` file, optionally gzip-compressed.
#' @param batch_size Number of features per batch (default 65536).
#' @param quiet If `FALSE` (default), report the feature count when the scan
#'   completes.
#'
#' @return A `vectra_node` object representing a lazy scan of the BED file.
#'
#' @examples
#' f <- tempfile(fileext = ".bed")
#' writeLines(c("chr1\t100\t200\tfeatA\t0\t+",
#'              "chr1\t150\t400\tfeatB\t0\t-",
#'              "chr2\t500\t900\tfeatC\t0\t+"), f)
#' node <- tbl_bed(f, quiet = TRUE)
#' node |> filter(chrom == "chr1") |> collect()
#' unlink(f)
#'
#' @seealso [interval_join()], [tbl_fasta()]
#' @export
tbl_bed <- function(path, batch_size = .DEFAULT_BATCH_SIZE, quiet = FALSE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_bed_scan_node, path, as.double(batch_size), isTRUE(quiet))
  structure(list(.node = xptr, .path = path), class = "vectra_node")
}

#' Create a lazy table reference from a SQLite database
#'
#' Opens a SQLite database and lazily scans a table. Column types are inferred
#' from declared types in the CREATE TABLE statement. All filtering, grouping,
#' and aggregation is handled by vectra's C engine --- no SQL parsing needed.
#' No data is read until [collect()] is called.
#'
#' @param path Path to a SQLite database file.
#' @param table Name of the table to scan.
#' @param batch_size Number of rows per batch (default 65536).
#'
#' @return A `vectra_node` object representing a lazy scan of the table.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".sqlite")
#' write_sqlite(mtcars, f, "cars")
#' node <- tbl_sqlite(f, "cars")
#' node |> filter(cyl == 6) |> collect()
#' unlink(f)
#' }
#'
#' @export
tbl_sqlite <- function(path, table, batch_size = .DEFAULT_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!is.character(table) || length(table) != 1)
    stop("table must be a single character string")

  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_sql_scan_node, path, table, as.double(batch_size))
  structure(list(.node = xptr, .path = path, .table = table),
            class = "vectra_node")
}

#' Create a lazy table reference from an Excel (.xlsx) file
#'
#' Reads a sheet from an Excel workbook into a vectra node for lazy query
#' execution. The sheet is read into memory via
#' \code{\link[openxlsx2:read_xlsx]{openxlsx2::read_xlsx()}} and then converted
#' to vectra's internal format. Requires the \pkg{openxlsx2} package.
#'
#' @param path Path to an `.xlsx` file.
#' @param sheet Sheet to read: either a name (character) or 1-based index
#'   (integer). Default `1L` (first sheet).
#' @param batch_size Number of rows per batch (default 65536).
#'
#' @return A `vectra_node` object representing a lazy scan of the sheet.
#'
#' @examples
#' \donttest{
#' if (requireNamespace("openxlsx2", quietly = TRUE)) {
#'   f <- tempfile(fileext = ".xlsx")
#'   openxlsx2::write_xlsx(mtcars, f)
#'   node <- tbl_xlsx(f)
#'   node |> filter(cyl == 6) |> collect()
#'   unlink(f)
#' }
#' }
#'
#' @export
tbl_xlsx <- function(path, sheet = 1L, batch_size = .DEFAULT_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  if (!(is.character(sheet) || is.numeric(sheet)) || length(sheet) != 1)
    stop("sheet must be a single character string or integer")

  if (!requireNamespace("openxlsx2", quietly = TRUE))
    stop("Package 'openxlsx2' is required for tbl_xlsx(). ",
         "Install it with: install.packages('openxlsx2')")

  path <- normalizePath(path, mustWork = TRUE)
  df <- openxlsx2::read_xlsx(path, sheet = sheet)
  df_to_node(df)
}

#' Create a lazy table reference from a GeoTIFF raster
#'
#' Opens a GeoTIFF file and returns a lazy query node. Each pixel becomes a row
#' with columns `x`, `y`, `band1`, `band2`, etc. Coordinates are pixel centers
#' derived from the affine geotransform. NoData values become `NA`.
#'
#' Use `filter(x >= ..., y <= ...)` for extent-based cropping and
#' `filter(band1 > ...)` for value-based cropping. Results can be converted
#' back to a raster with `terra::rast(df, type = "xyz")`.
#'
#' @param path Path to a GeoTIFF file.
#' @param batch_size Number of raster rows per batch (default 256).
#'
#' @return A `vectra_node` object representing a lazy scan of the raster.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = as.double(rep(1:4, 3)),
#'                  y = as.double(rep(1:3, each = 4)),
#'                  band1 = as.double(1:12))
#' write_tiff(df, f)
#' node <- tbl_tiff(f)
#' node |> filter(band1 > 6) |> collect()
#' unlink(f)
#' }
#'
#' @export
tbl_tiff <- function(path, batch_size = .TIFF_BATCH_SIZE) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  xptr <- .Call(C_tiff_scan_node, path, as.double(batch_size))
  meta <- .Call(C_tiff_scan_meta, xptr)
  structure(list(.node = xptr, .path = path, .tiff_meta = meta),
            class = "vectra_node")
}

#' Extract raster values at point coordinates
#'
#' Samples band values from a GeoTIFF at specific (x, y) locations using the
#' file's affine geotransform. Only the strips containing query points are read,
#' making this efficient for sparse point sets on large rasters.
#'
#' Points that fall outside the raster extent return `NA` for all bands.
#' Pixel assignment uses nearest-pixel rounding (i.e., the point is assigned to
#' the pixel whose center is closest).
#'
#' @param path Path to a GeoTIFF file.
#' @param x Numeric vector of x coordinates, or a data.frame / matrix with
#'   columns named `x` and `y`.
#' @param y Numeric vector of y coordinates (ignored if `x` is a data.frame).
#'
#' @return A data.frame with columns `x`, `y`, `band1`, `band2`, etc.
#'   One row per input point, in the same order as the input.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = as.double(rep(1:4, 3)),
#'                  y = as.double(rep(1:3, each = 4)),
#'                  band1 = as.double(1:12))
#' write_tiff(df, f)
#'
#' # Sample at specific locations via data.frame
#' pts <- data.frame(x = c(2, 3), y = c(1, 2))
#' tiff_extract_points(f, pts)
#'
#' # Or pass x and y separately
#' tiff_extract_points(f, x = c(2, 3), y = c(1, 2))
#' unlink(f)
#' }
#'
#' @export
tiff_extract_points <- function(path, x, y = NULL) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)

  if (is.data.frame(x) || is.matrix(x)) {
    if (!all(c("x", "y") %in% colnames(x)))
      stop("when x is a data.frame/matrix it must have columns 'x' and 'y'")
    y <- as.double(x[, "y"])
    x <- as.double(x[, "x"])
  } else {
    if (is.null(y))
      stop("y is required when x is a numeric vector")
    x <- as.double(x)
    y <- as.double(y)
  }

  if (length(x) != length(y))
    stop(sprintf("x and y must have the same length (got %d and %d)",
                 length(x), length(y)))

  .Call(C_tiff_extract_points, path, x, y)
}

#' Read GDAL_METADATA from a GeoTIFF
#'
#' Returns the GDAL_METADATA XML string (TIFF tag 42112) embedded in a
#' GeoTIFF file. Returns `NA` if the tag is not present.
#'
#' @param path Path to a GeoTIFF file.
#' @return A single character string containing the XML, or `NA_character_`.
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = as.double(1:4))
#' write_tiff(df, f, metadata = "<GDALMetadata></GDALMetadata>")
#' tiff_metadata(f)
#' unlink(f)
#' }
#'
#' @export
tiff_metadata <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  .Call(C_tiff_read_metadata, path)
}

#' Read per-band names from a GeoTIFF
#'
#' Returns the band names embedded in the file's GDAL_METADATA XML
#' (TIFF tag 42112). GDAL writes per-band names as
#' `<Item name="DESCRIPTION" sample="N" role="description">...</Item>` entries,
#' where `sample` is the 0-based band index. Bands without a name in the XML
#' are reported as `NA`. Files with no GDAL_METADATA tag at all return a
#' length-`nbands` vector of `NA_character_`.
#'
#' This is a small, dependency-free scanner intended for the common case
#' (`terra::names(r) <- ...` and similar). For arbitrary XML, parse the raw
#' string from [tiff_metadata()] yourself.
#'
#' @param path Path to a GeoTIFF file.
#' @return A character vector of length `nbands`. Element `i` is the name
#'   of band `i` (or `NA_character_` if the file does not name it).
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = rep(1:2, 2), y = rep(1:2, each = 2),
#'                  band1 = as.double(1:4), band2 = as.double(5:8))
#' xml <- paste0(
#'   "<GDALMetadata>",
#'   "<Item name=\"DESCRIPTION\" sample=\"0\" role=\"description\">temperature</Item>",
#'   "<Item name=\"DESCRIPTION\" sample=\"1\" role=\"description\">humidity</Item>",
#'   "</GDALMetadata>")
#' write_tiff(df, f, metadata = xml)
#' tiff_band_names(f)
#' unlink(f)
#' }
#'
#' @export
tiff_band_names <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)

  # We need nbands to size the output. Open via tbl_tiff() to reuse the
  # existing C scan node.
  node <- tbl_tiff(path)
  nb <- node$.tiff_meta$nbands
  out <- rep(NA_character_, nb)

  meta <- tiff_metadata(path)
  if (is.na(meta) || !nzchar(meta)) return(out)

  items <- .parse_gdal_metadata_items(meta)
  if (nrow(items) == 0) return(out)

  desc <- items[items$name == "DESCRIPTION" & !is.na(items$sample), ,
                drop = FALSE]
  if (nrow(desc) == 0) return(out)

  # sample is 0-based; clamp out-of-range entries silently.
  ok <- desc$sample >= 0 & desc$sample < nb
  for (i in which(ok)) out[desc$sample[i] + 1L] <- desc$value[i]
  out
}

# Internal: scan a GDAL_METADATA XML blob for <Item ...>value</Item> entries.
# Returns a data.frame with columns name (chr), sample (int, NA if absent),
# role (chr, NA if absent), value (chr). Built for the well-constrained
# format GDAL emits — not a general XML parser.
.parse_gdal_metadata_items <- function(xml) {
  m <- gregexpr("<Item\\b([^>]*)>(.*?)</Item>", xml, perl = TRUE)[[1]]
  if (length(m) == 1 && m == -1)
    return(data.frame(name = character(0), sample = integer(0),
                      role = character(0), value = character(0),
                      stringsAsFactors = FALSE))

  starts <- as.integer(m)
  lens   <- attr(m, "match.length")
  cap_starts <- attr(m, "capture.start")
  cap_lens   <- attr(m, "capture.length")

  n <- length(starts)
  attrs_chr  <- substring(xml, cap_starts[, 1], cap_starts[, 1] + cap_lens[, 1] - 1L)
  values_chr <- substring(xml, cap_starts[, 2], cap_starts[, 2] + cap_lens[, 2] - 1L)

  get_attr <- function(s, key) {
    pat <- paste0(key, "\\s*=\\s*\"([^\"]*)\"")
    m2  <- regmatches(s, regexec(pat, s, perl = TRUE))[[1]]
    if (length(m2) == 2) m2[2] else NA_character_
  }

  name   <- vapply(attrs_chr, get_attr, character(1), key = "name")
  sample <- vapply(attrs_chr, function(s) {
    v <- get_attr(s, "sample")
    if (is.na(v)) NA_integer_ else suppressWarnings(as.integer(v))
  }, integer(1))
  role   <- vapply(attrs_chr, get_attr, character(1), key = "role")

  data.frame(name   = unname(name),
             sample = unname(sample),
             role   = unname(role),
             value  = unname(.unescape_xml(values_chr)),
             stringsAsFactors = FALSE)
}

.unescape_xml <- function(x) {
  x <- gsub("&lt;",   "<",  x, fixed = TRUE)
  x <- gsub("&gt;",   ">",  x, fixed = TRUE)
  x <- gsub("&quot;", "\"", x, fixed = TRUE)
  x <- gsub("&apos;", "'",  x, fixed = TRUE)
  gsub("&amp;",  "&",  x, fixed = TRUE)
}

#' Read CRS metadata from a GeoTIFF
#'
#' Returns the spatial reference system embedded in a GeoTIFF, parsed from
#' the GeoKey directory (TIFF tag 34735). The projected CRS EPSG
#' (`PCSTypeGeoKey` 3072) is preferred over the geographic CRS EPSG
#' (`GeographicTypeGeoKey` 2048). Citation strings are read from
#' `GeoAsciiParams` (tag 34737) with priority PCS > GeoTIFF > geographic.
#'
#' Files written without a GeoKey directory return `NA` for both fields.
#'
#' @param path Path to a GeoTIFF file.
#' @return A list with elements `epsg` (integer or `NA_integer_`) and
#'   `citation` (character or `NA_character_`).
#'
#' @examples
#' \donttest{
#' f <- tempfile(fileext = ".tif")
#' df <- data.frame(x = 1:4, y = rep(1:2, each = 2), band1 = as.double(1:4))
#' write_tiff(df, f)
#' tiff_crs(f)  # epsg = NA, citation = NA — vectra writer omits GeoKeys
#' unlink(f)
#' }
#'
#' @export
tiff_crs <- function(path) {
  if (!is.character(path) || length(path) != 1)
    stop("path must be a single character string")
  path <- normalizePath(path, mustWork = TRUE)
  .Call(C_tiff_read_crs, path)
}
