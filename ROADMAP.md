# vectra roadmap

vectra is a streaming, larger-than-RAM columnar engine. Everything it does well
follows one shape: an **opaque self-describing blob per cell**, decoded and
computed on **per row in C**, dispatched through `expr`, parallelized with
OpenMP, and **streamed** so the working set never has to fit in RAM. Hex-WKB
geometry (`st_*`) and hex-float32 embeddings (`cosine`/`l2`/`dot`) are the two
existing proofs. The roadmap below extends that shape to new domains and closes
the remaining edges of the spatial layer.

This document is forward-looking. Each **initiative** is a stack of **phases**;
every phase ships independently on `main` behind its own version tag, with its
own recovery tests, so the tree is always releasable. Version targets are
indicative, not contractual -- ordering may shift with need.

---

## Status ledger (shipped)

The vector-GIS verb roadmap is complete. Recorded here so the forward plan does
not repeat it; detail lives in `NEWS.md` and git history.

| Area | Shipped | Version |
|------|---------|---------|
| Scalar geometry expressions (`st_*`: measures, 10 binary predicates, 3 unary, 8 transforms) | native GEOS off the WKB column, OpenMP per row | through 0.9.x |
| Two-layer overlay (`spatial_overlay`, `how=`) | intersection / union / identity / symdiff, component-tiled, streamed | 0.9.2 |
| Set-wise constructions (`spatial_construct`) | hull / concave / voronoi / delaunay / bbox family / pole | 0.9.2 |
| Snap + topology cleanup (`spatial_snap`, `spatial_snap_grid`, `spatial_explode`) | | 0.9.2 |
| Analysis verbs (`spatial_knn`, `spatial_split`, `spatial_smooth`) | | 0.9.2 |
| Coverage + linear referencing (`spatial_polygonize`, `spatial_line_merge`, `spatial_simplify`, `spatial_locate`, `spatial_centerline`, `spatial_topology`, `spatial_eliminate`) | | 0.9.3-0.9.5 |
| Network routing (`spatial_network`, `spatial_route`, `spatial_service_area`) | native-C binary-heap Dijkstra over CSR, directed + one-way, OpenMP across origins | 0.9.6 |
| Raster engine (GeoTIFF + tiled `.vec`, `zonal`, `focal`, `terrain`, `warp` with PROJ reprojection, `rasterize`, `polygonize`, `contours`, `mask`, `mosaic`, `rast_calc`, `proximity`) | native C, no GDAL | 0.9.x |
| Interval overlap joins (`interval_join`) | sweep-line, output-sensitive | 0.9.8 |
| Embedding columns (`as_embedding`, `cosine`, `l2`, `dot`) | hex-float32 blob, per-row decode | 0.9.x |
| Time resampling + rolling (`floor_time`, `resample`, `roll_*`) | | 0.9.8 |
| Engine memory (`vectra.memory`, grace-hash join spill) | one budget knob, joins spill to disk | 0.9.10 |
| Compression (`compress="small"` adaptive + parallel sweep, dict-defer string collect, all-null row-group prune) | | 0.9.11-0.9.12 |

**What remains in GIS is deliberate, not a missing tier** -- see Initiative C for
the small edges (vector reprojection ergonomics, CRS breadth, BigTIFF write,
gap-fill) and "Out of scope" for what belongs in a different package
(interpolation, spatial statistics, geocoding).

---

## Priority ordering

1. **Initiative A -- Genomics / sequence ops.** Cleanest architectural fit; two
   existing precedents (`st_*`, embeddings) to copy; **no new external
   dependency**, so it keeps the self-contained-tarball property and can start
   immediately.
2. **Initiative B -- Climate cubes (NetCDF / Zarr).** Highest leverage for the
   actual climate-ecology workflow (the "I can't open this CMIP6 file" problem),
   but it touches an external-library decision that cuts against the
   self-contained tarball, so it opens with a design pass (Phase B0) before any
   engine code.
3. **Initiative C -- GIS completeness polish.** Small, high-clarity closes of the
   known spatial edges; each is a day-scale phase, slot between larger work.
4. **Initiative D -- Time-series / embedding follow-ons.** Nice-to-haves that
   round out already-shipped verbs.
5. **Initiative E -- Network follow-ons.** Only when a real network shows plain
   Dijkstra is the bottleneck.

---

## Initiative A -- Genomics: sequence ops + format backends

**Goal.** Make vectra a larger-than-RAM engine for biological sequence data. A
sequence (DNA / RNA / protein) is a self-describing ASCII string per cell -- the
exact hex-WKB precedent -- so `seq_*` maps onto `expr` the way `st_*` does:
decode per row, compute in C, OpenMP over rows. `levenshtein` / `dl_dist` are
already parallelized, so edit-distance alignment is the same kernel shape.

**Why not a per-feature `mutate`.** Reverse-complement, GC content, and translate
are per-row and *do* fit `mutate`, but they need a native C kernel (not R string
juggling) to stream at engine speed, and k-mer / alignment are set-wise or
windowed and need their own node.

### Phase A1 -- `seq_*` scalar expression family (0.10.0) -- SHIPPED

The smallest correct slice: operate on a sequence held in an ordinary string
column, no new backend required. Shipped in 0.10.0 (`src/expr_seq.c`,
`.serialize_seq` in `R/expr.R`, `?seq_expressions`), recovery-tested cell-for-cell
against Biostrings and stringdist in `tests/testthat/test-seq-expr.R`.

```r
tbl_csv("reads.csv") |>
  mutate(rc  = seq_revcomp(seq),
         gc  = seq_gc(seq),
         aa  = seq_translate(seq),          # frame 1, standard genetic code
         d   = seq_dist(seq, ref_seq))      # edit distance to a reference column/literal
```

- Functions: `seq_length`, `seq_revcomp`, `seq_complement`, `seq_reverse`,
  `seq_gc`, `seq_translate` (codon table arg, default standard), `seq_transcribe`
  (DNA<->RNA), `seq_dist` (reuses the levenshtein kernel; DL / Hamming variants
  via arg), `seq_subseq(seq, start, width)`.
- IUPAC ambiguity codes handled in complement / GC (documented behaviour).
- Missing / non-sequence input -> `NA`, never an error (the `st_*` contract).
- **Files:** `src/expr_seq.c` (kernel, per-row, `#pragma omp` above threshold via
  `vec_omp.h` -- never include `<omp.h>`), name->discriminator maps in `R/expr.R`
  (alongside the `st_*` block), doc topic `?seq_expressions` in `R/seq_expr.R`.
- **Tests (recovery, not smoke):** every op checked cell-for-cell against
  `Biostrings` (Suggests, tests only) on a fixture -- `reverseComplement`,
  `translate`, `letterFrequency` for GC, `stringdist` for `seq_dist`. Random
  sequences across seeds; ambiguity-code cases explicit.

### Phase A2 -- FASTA / FASTQ scan backend (0.10.1) -- SHIPPED

`tbl_fasta(path)` / `tbl_fastq(path)` stream records as rows (`id`, `desc`,
`seq`, and for FASTQ `qual`), one row group per N records, so a 40 GB read set
never materializes. Mirrors the `csv_scan` backend; both share the renamed
`byte_reader` (plain + gzip). Shipped in 0.10.1.

- **Files:** `src/fasta_scan.c` / `.h`, shared `src/byte_reader.c` / `.h` (the
  former `csv_reader`), entry point in `src/r_bridge_io.c`, `R/tbl.R` front
  doors. Gzip input rides the vendored miniz path CSV already uses.
- **Input-totals sanity check:** a truncated / malformed record (missing FASTQ
  line, seq/qual length mismatch, missing `>`/`@`) fails loudly rather than
  silently dropping; the scan reports its record count on completion (streaming,
  so the total is known at end-of-file, not open; `quiet = TRUE` suppresses it).
- **Tests:** `tests/testthat/test-fasta-scan.R` round-trips known multi-record
  FASTA/FASTQ against `Biostrings::readDNAStringSet` and `ShortRead`; gzipped
  and plain; streaming invariance across batch sizes; deliberately truncated /
  malformed files error.

### Phase A3 -- k-mer spectrum node (0.10.2) -- SHIPPED

k-mer counting is set-wise (one row per distinct k-mer per group), so it is a
node, not a scalar expr -- the `group_agg` shape. Shipped in 0.10.2.

```r
tbl_fasta("genome.fa") |> kmer(seq, k = 6, by = id)   # -> id, kmer, count
```

- Canonical-k-mer option (collapse a k-mer with its reverse-complement).
- **Files:** `src/kmer.c` node (open-addressing hash over the 2-bit packed k-mer,
  keyed on (group id, packed k-mer); k in 1..32, non-ACGT windows skipped),
  `R/seq_verbs.R`, bridge `C_kmer_node` in `src/r_bridge_nodes.c`. The group-key
  store is the shared `src/key_arena.c` (extracted from `group_agg`); the 2-bit
  base encoding is the shared `src/seq_util.h`.
- **Tests:** `tests/testthat/test-kmer.R` -- counts match a hand-rolled R k-mer
  tabulation (ungrouped and by-group), canonical option verified, non-ACGT
  window skipping, streaming invariance (multi-batch == single-batch), k = 32
  packing, and out-of-range k rejected.

### Phase A4 -- BED interval scan (0.10.3)

BED is a tab file of `[chrom, start, end, ...]`; scanning it as rows makes the
already-shipped `interval_join()` a genome-interval overlap engine for free (BED
x BED, BED x annotation), keyed on `chrom`.

- **Files:** thin `src/bed_scan.c` (or a `tbl_bed()` wrapper over `csv_scan` with
  the BED dialect + 0-based half-open convention made explicit), `R/tbl.R`.
- **Tests:** overlap result matches `GenomicRanges::findOverlaps` on a fixture;
  half-open / 0-based boundary cases explicit (off-by-one is the classic BED bug).

### Phase A5 -- VCF scan + pairwise alignment (0.11.0)

The heavier tail, split out so A1-A4 ship first.

- `tbl_vcf(path)`: variant records as rows (`chrom`, `pos`, `ref`, `alt`,
  `qual`, INFO/FORMAT as columns); bgzip via miniz.
- `seq_align(a, b, ...)`: Smith-Waterman / Needleman-Wunsch local/global
  alignment score + optional CIGAR, same OpenMP-per-row shape as `levenshtein`
  (a banded DP kernel, well under the 200-line native bar).
- **Tests:** VCF fields vs `VariantAnnotation`; alignment score vs `Biostrings::pairwiseAlignment`.

**Out of scope for this initiative:** a full aligner index (BWA/minimap2-class),
assembly, and probabilistic variant calling -- those are their own tools, not a
columnar per-row kernel.

---

## Initiative B -- Climate cubes: NetCDF / Zarr scan

**Goal.** Turn an N-dimensional climate cube (lon, lat, time, level) into a lazy
`tbl` whose rows are `(coord columns..., value)`, with a bbox / time-range
predicate pruning chunks the way `==` already prunes via the hash index. The
`tiff_scan` backend (pixels -> rows with x, y, band) already proves the model;
this generalizes it to more dimensions.

**Why it needs a design pass first.** Every other vectra backend is native C with
no external link (self-contained tarball -> one CRAN artifact). NetCDF-4 is HDF5
underneath, a heavy C library; adding it as a hard dependency breaks the
self-contained property and complicates CRAN. This tension is a real decision,
not an implementation detail, so Phase B0 resolves it before any engine code.

### Phase B0 -- design pass (no code): the dependency decision

Write `dev_notes/netcdf_zarr_design.md` deciding, with the "No Dependency
Shortcuts" and self-contained-tarball principles as the frame:

- **Option 1 -- native readers for the self-contained subset.** NetCDF-3 classic
  is a simple self-describing format readable natively (a few hundred lines).
  **Zarr** is even friendlier: chunks are independent compressed blobs
  (zstd / blosc / gzip) plus JSON metadata (`.zarray` / `.zattrs`); vectra
  already vendors tdc (zstd-family entropy coders) and miniz (gzip/deflate), so a
  pure-C Zarr v2/v3 reader for the common codecs is plausible with no new link.
  This keeps the tarball self-contained.
- **Option 2 -- optional HDF5/netcdf-c linkage** for NetCDF-4, gated by a
  `configure` probe (build the backend only when the system library is present,
  like an optional feature), so the default install stays dependency-free and
  power users with HDF5 get NetCDF-4.
- **Recommendation to validate in B0:** native Zarr + native NetCDF-3 as the
  first-class path (Phases B1-B3), NetCDF-4/HDF5 as optional-linkage follow-on
  (deferred). Confirm the codec coverage (which blosc/zstd variants real CMIP6 /
  ERA5 Zarr stores use) before committing.
- Deliverable: the decision doc + a one-file spike reading a single chunk of a
  real store, committed to `dev_notes/`. No package code yet.

### Phase B1 -- `nc_scan` / `zarr_scan`: one variable to rows

`tbl_zarr(path, var=)` / `tbl_netcdf(path, var=)` streams one variable, emitting
dimension-coordinate columns plus the value column, **one Zarr/NetCDF chunk per
`.vtr` row group** so decode and pruning align with storage.

- **Files:** `src/zarr_scan.c` (+ native NetCDF-3 in `src/nc_scan.c`), entry
  points in `src/r_bridge_io.c`, front doors in `R/tbl.R`.
- **Input-totals sanity check (mandatory):** assert loaded shape equals the
  header's declared dims; log `loaded <n> chunks (var=..., dims=...)` every open.
- **Tests:** values + coordinates match `ncdf4` / `stars` / `terra` on a fixture
  cube; chunked read == whole-array read.

### Phase B2 -- chunk pruning via zone maps

A bbox / time-range / level predicate prunes chunks before decode, reusing the
`scan.c` zone-map + null-count machinery. Each chunk carries its coordinate
min/max as stats; a `filter(time >= t0, lat > 40)` skips chunks that cannot
contribute -- the same pruning that makes `.vtr` scans fast.

- **Tests:** a predicate over a multi-chunk cube reads only the covering chunks
  (assert via a decode counter) and returns the identical rows to the unpruned
  scan.

### Phase B3 -- multi-variable and CF conventions

- Multiple variables sharing a grid -> multiple value columns in one scan.
- CF metadata: `units`, `calendar` (non-Gregorian climate calendars), `scale_factor` /
  `add_offset` unpacking, `_FillValue` -> `NA`. Time axis reclassed to the
  engine's date annotation so `floor_time` / `resample` compose directly.
- **Tests:** unpacked values and decoded times match `CFtime` / `terra` on a
  fixture with scale/offset and a 360-day calendar.

### Phase B4 -- write path (deferred)

Materialize a query back to Zarr / NetCDF-3 (chunk-aligned), so vectra is a cube
transform tool, not only a reader. Deferred behind read + the raster tier.

**Optional follow-on (own phase, gated by B0):** NetCDF-4 / HDF5 via configure-probed
optional linkage.

---

## Initiative C -- GIS completeness polish

Small closes of the known spatial edges. None is a new tier; each is a
day-to-few-days phase.

### Phase C1 -- vector reprojection ergonomics (0.9.x)

Reprojection currently only reads as `spatial_map(~ sf::st_transform(.x, crs))`.
Keep the engine free of a PROJ link for vectors, but:

- Document the streamed-reproject recipe prominently in `?spatial_map` and the
  spatial vignette (it is the single most-asked transform).
- Evaluate a thin `spatial_transform(x, crs)` convenience that wraps the streamed
  `sf::st_transform` and threads the CRS metadata -- **only if** it earns its
  keep against "few front doors"; if not, ship the documented recipe instead.
- Turn the current CRS-mismatch *error* into an actionable message naming the
  exact `spatial_transform` / `spatial_map` call to run.

### Phase C2 -- CRS breadth: WKT / PROJ, not EPSG-only (0.9.x)

Raster headers store an integer EPSG; a WKT/PROJ-only CRS collapses to `0`, so
`warp()` silently declines to reproject a custom projection lacking an EPSG code
-- a **silent** wrong-scope, the exact failure the sanity-check rule targets.

- Carry the full CRS string (WKT2 / PROJ) in the `.vec` header and GeoTIFF
  metadata, EPSG as a fast-path cache.
- When a reprojection is requested but the CRS cannot be resolved to something
  PROJ accepts, **fail loudly** rather than pass geometry through unprojected.
- **Tests:** `warp()` across two custom (non-EPSG) CRS matches `terra::project`;
  an unresolvable CRS errors instead of silently not-reprojecting.

### Phase C3 -- tiled BigTIFF write (0.9.x)

`write_tiff` reads tiled BigTIFF but does not write it (`write.R:139`). Close the
asymmetry so a > 4 GB raster round-trips.

- **Tests:** a tiled BigTIFF write reads back byte-identical pixels via
  `tbl_tiff` and via `terra::rast`.

### Phase C4 -- coverage gap-fill (0.9.x)

`spatial_eliminate(fill_gaps = TRUE)`: the empty slivers between polygons that
should tile are recovered from the boundary arcs (`spatial_topology` /
`spatial_polygonize` of the unioned boundary minus the input) and merged into a
neighbour by the existing `spatial_eliminate` union-find -- an argument on the
existing verb, not a sibling (the two-layer-overlay model). "Delete holes" stays
a documented `spatial_map(~ ...)` recipe, no verb.

- **Tests:** a coverage with known gaps tiles exactly after fill; total area
  conserved to the coverage tolerance.

---

## Initiative D -- Time-series & embedding follow-ons

Round out shipped verbs; each is small.

- **D1 -- reclass computed date columns.** The project node does not re-attach the
  `Date`/`POSIXct` class to a floored/resampled bucket column, so it collects as
  numeric epoch. Thread the date annotation through the project node so a
  `resample` bucket comes back a date. **Test:** class + values survive a
  `floor_time |> collect` round-trip.
- **D2 -- time-series gap-filling.** `resample(..., fill = )` (or a `fill_gaps`
  verb) inserts missing calendar buckets with `NA` / carry-forward / interpolate,
  so a downsample over a sparse series has a regular grid. **Test:** filled grid
  matches a hand-built regular index; each fill mode recovers a known series.
- **D3 -- fused nearest-neighbour node.** `mutate(d = cosine(emb, q)) |> slice_min(d, n = k)`
  already works via the `topn` node; add a fused NN node **only if** the
  distance-column memory ever measurably matters (profile first -- no speculative
  node). **Test:** identical top-k to the two-step form.

---

## Initiative E -- Network follow-ons

Only once a real network shows the current native Dijkstra is the bottleneck.

- **E1 -- snap-to-edge origins.** Off-network origins/destinations snap to the
  nearest point on the nearest edge (virtual edge split for one query), not only
  to the nearest node. **Test:** snapped route matches `sfnetworks` with
  edge-blending.
- **E2 -- bidirectional Dijkstra** for single (origin, destination) pairs --
  meet-in-the-middle, roughly halving the settled set. **Test:** identical
  cost + path to the unidirectional solver on a fixture.
- **E3 -- contraction hierarchies** (`spatial_network(prepare = TRUE)`): one-time
  preprocessing for orders-of-magnitude-faster repeat queries on a large static
  graph. Default stays plain Dijkstra. **Test:** CH query cost matches plain
  Dijkstra cell-for-cell; build-time component counts reported.

**Deferred to a further tier:** turn restrictions / turn costs (need an
edge-expanded graph), time-dependent / multimodal routing (different graph
model), map-matching GPS traces (external reference data + probabilistic model).

---

## Out of scope (a different package)

These sit on top of geometry / data but are not per-row streamable kernels; they
fit a statistics or solver package better than a columnar engine.

- **Spatial interpolation and surfaces** -- IDW, kriging, kernel-density. A solver
  over the whole point set; the raster tier covers the output format, not the
  estimator.
- **Spatial statistics / point-pattern** -- Moran's I, Getis-Ord, Ripley's K.
  Global estimators over a layer, not element-by-element streamable.
- **Geocoding, conflation, map-matching** -- need external services or reference
  data.
- **Text corpora / full-text** (tokenization, TF-IDF, postings lists) -- fits the
  string machinery, but less differentiated from existing tools than the tiers
  above.
- **Audio / signal frames** (spectrograms, MFCCs) -- windowed across rows rather
  than per-row, so they fight the pull-based model; doable as a windowed node,
  not as cheap as the `st_*` analogy suggests.

---

## Engineering standards (cross-cutting, apply to every phase)

- **Streaming contract.** Every verb keeps peak memory tracking the result or the
  per-group working set, never the input length. A streamed result must equal the
  resident result; divergence is a bug, not a rounding difference.
- **Recovery tests, not smoke tests.** New statistical / decode kernels are
  validated against an established ground truth (Biostrings, GenomicRanges,
  terra, sfnetworks, igraph -- Suggests, tests only), cell-for-cell on a fixture
  across seeds -- never a shape/NaN check called a test.
- **Input-totals sanity check.** Every new scan backend asserts loaded totals
  against the header/manifest on open and logs `loaded <n> units (...)`; a flag
  selecting a subset must change what is loaded, not only the label. Fail loudly
  on mismatch.
- **No dependency shortcuts.** Native C under ~200 lines beats a new link; the
  self-contained tarball is a hard property (Initiative B's whole design pass
  exists to protect it). Geometry goes through the linked libgeos C API, not sf;
  sf / terra / igraph stay Suggests (ground truth + vector I/O only).
- **OpenMP discipline.** Never `#include <omp.h>` in `src/*.c`; include
  `"vec_omp.h"`. Two CRAN clang flavors have broken on a direct include.
- **Few front doors.** Prefer an argument on an existing verb over a new sibling
  (`spatial_overlay(y=, how=)` is the model). Split only when the return genuinely
  differs in kind.
- **Ship independently on main.** Each phase is releasable on its own tag with its
  tests green; no long-lived feature branches.
