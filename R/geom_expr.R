#' Geometry functions inside mutate(), filter(), and summarise()
#'
#' vectra recognizes a family of `st_*` geometry functions directly inside
#' expression verbs. They run on the GEOS C library straight off the hex-WKB
#' geometry column, one row at a time, with no per-batch round-trip through
#' \pkg{sf}: `tbl(f) |> filter(st_area(geometry) > 1e6)` prunes the stream in C,
#' and `mutate(centroid = st_centroid(geometry))` adds a new hex-WKB geometry
#' column. The computation is planar, in the geometry's own coordinate units,
#' exactly as the streaming spatial verbs are.
#'
#' These names are interpreted by the expression engine; they are not exported R
#' functions and are not called as such. They are available only inside
#' [mutate()], [transmute()], [filter()], and a grouped [summarise()] over a
#' `vectra_node`. The geometry argument is the hex-WKB column (named `geometry`
#' by convention). A function that returns a geometry produces another hex-WKB
#' column; materialize it with [collect_sf()] (point it at the column with
#' `geom =`), or write it with [write_tiff()]/`sf::st_write()`.
#'
#' @section Measures (return a number):
#' \describe{
#'   \item{`st_area(g)`}{Area of polygonal geometry (0 for lines and points).}
#'   \item{`st_length(g)`, `st_perimeter(g)`}{Length of a line, or the perimeter
#'     (boundary length) of a polygon. The two names are aliases.}
#'   \item{`st_x(g)`, `st_y(g)`}{Coordinate of a point geometry; `NA` for a
#'     non-point.}
#'   \item{`st_npoints(g)`}{Number of coordinates in the geometry.}
#'   \item{`st_ngeometries(g)`}{Number of sub-geometries in a collection or
#'     multi-geometry (1 for a single geometry).}
#'   \item{`st_distance(a, b)`}{Shortest planar distance between two geometries
#'     (see the binary second argument below).}
#' }
#'
#' @section Predicates (return TRUE / FALSE):
#' Unary: `st_is_valid(g)`, `st_is_empty(g)`, `st_is_simple(g)`.
#'
#' Binary topological relations, each taking a second geometry: `st_intersects`,
#' `st_within`, `st_contains`, `st_overlaps`, `st_touches`, `st_crosses`,
#' `st_equals`, `st_disjoint`, `st_covers`, `st_covered_by`. Used in [filter()]
#' they keep the rows where the relation holds.
#'
#' @section Type (returns a string):
#' `st_geometry_type(g)` gives the GEOS geometry type name (`"Point"`,
#' `"Polygon"`, `"MultiPolygon"`, ...).
#'
#' @section Transforms (return a geometry):
#' \describe{
#'   \item{`st_centroid(g)`}{Area (or length, or vertex) centroid.}
#'   \item{`st_point_on_surface(g)`}{A point guaranteed to lie on the geometry.}
#'   \item{`st_boundary(g)`}{The topological boundary.}
#'   \item{`st_envelope(g)`}{The axis-aligned bounding rectangle.}
#'   \item{`st_convex_hull(g)`}{The convex hull.}
#'   \item{`st_make_valid(g)`}{Repair an invalid geometry.}
#'   \item{`st_buffer(g, dist)`}{Buffer by `dist` (round joins, 8 segments per
#'     quadrant).}
#'   \item{`st_simplify(g, tol)`}{Topology-preserving Douglas-Peucker
#'     simplification with tolerance `tol`.}
#' }
#'
#' @section The second geometry of a binary op:
#' For `st_distance` and the binary predicates, the second argument can be
#' another geometry column (compared row by row), a constant `sf`/`sfc` object
#' (a multi-feature object is unioned to one geometry), or a hex-WKB string. A
#' constant is parsed once and reused across every row, so testing a whole
#' stream against one area of interest stays cheap.
#'
#' @section Missing geometry:
#' A missing (`NA`) or unparseable geometry, or an operation that has no answer
#' (a coordinate of a non-point, distance to a missing geometry), yields `NA`
#' for that row rather than an error.
#'
#' @name geom_expressions
#' @seealso [mutate()], [filter()], [collect_sf()] to materialize a geometry
#'   result as `sf`; [spatial_map()] for an arbitrary per-feature \pkg{sf}
#'   transform; [spatial_filter()] and [spatial_join()] for relating a stream to
#'   a resident reference layer.
#' @examples
#' if (requireNamespace("sf", quietly = TRUE)) {
#'   nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'   f <- tempfile(fileext = ".vtr")
#'   write_vtr(data.frame(
#'     NAME     = nc$NAME,
#'     geometry = sf::st_as_binary(sf::st_geometry(nc), hex = TRUE)
#'   ), f)
#'
#'   # measures: add area and perimeter columns
#'   tbl(f) |>
#'     mutate(area = st_area(geometry), perim = st_perimeter(geometry)) |>
#'     select(NAME, area, perim) |>
#'     collect() |>
#'     head()
#'
#'   # predicate: keep counties intersecting an area of interest
#'   aoi <- sf::st_as_sfc(sf::st_bbox(
#'     c(xmin = -81.5, ymin = 36.2, xmax = -80.5, ymax = 36.6)))
#'   tbl(f) |> filter(st_intersects(geometry, aoi)) |> collect() |> nrow()
#'
#'   # transform: replace each county with its centroid, materialize as sf
#'   tbl(f) |>
#'     mutate(geometry = st_centroid(geometry)) |>
#'     select(NAME, geometry) |>
#'     collect_sf()
#'
#'   unlink(f)
#' }
NULL
