# Changelog

## vectra 0.11.9

### Bug fixes

- [`filter()`](https://gillescolling.com/vectra/reference/filter.md) no
  longer drops rows when a `%in%` predicate runs against an indexed
  column. The scan probed the `.vtri` sidecar with whichever
  representation the predicate happened to carry, and contributed
  nothing when none matched the column’s own type – so the row-group
  bitmap came back empty and every row group was pruned.
  `filter(k %in% c(5, 9))` on an integer column returned zero rows where
  the same store without an index returned all of them, and since R
  writes a bare numeric literal as a double whatever the column holds,
  that is the ordinary way of writing the predicate rather than a corner
  case. The same went for a logical column, and for an `NA` in the set,
  whose matching rows an index files under a key of its own.

  Every set element is now probed by the column’s type, and a key that
  cannot be probed leaves the scan unpruned rather than being passed
  over. Where a double names no single integer the two possible readings
  are now separated: a fractional key matches no row, so every row group
  can go, while a key past 2^53 stands for several integers at once, so
  nothing is pruned. `filter(x == -1)` also reaches the index now: R
  parses a negative literal as a call rather than a constant, and
  neither the index nor the zone maps could read it, so the predicate
  scanned the store – 1.59 s over 60 million rows against 0.03 s once
  the constant is folded.

  The test file now answers 21 predicate shapes against both an indexed
  store and a plain copy of the same data and requires the two to agree.

- Building a `.vtri` index no longer holds the whole index in memory.
  Entries had to be chained into hash buckets, which cannot be done
  before every entry is known, so a build cost about what the sidecar
  cost: 2.23 GB resident for a 2.12 GB index over 60 million distinct
  keys, and an index too large for memory could be read but not made.

  The entries are now sorted rather than chained, through the same
  streaming budget everything else spills against
  ([`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md)),
  and written in a single forward pass. On that store the build peaks at
  0.32 GB under a 128 MB budget, in the same time as before. Extending
  an index after
  [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  is bounded the same way: the sidecar on disk is already a sorted
  stream, so it is merged with the appended row groups’ entries as the
  new file is written rather than gathered first.

  Sorted entries also drop the chain pointer and the bucket array,
  taking the same index from 2.12 GB to 0.69 GB, and make the file a
  function of the data rather than of the scan – an index that took in
  an append is byte-identical to one rebuilt from the whole store, and
  one built under a small budget to one built under a large one. A probe
  reads a directory slot and binary-searches the few entries it spans,
  so a fetch stays flat as the index grows.

  `.vtri` files written by earlier versions of vectra read as absent, as
  any unusable index does;
  [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  rebuilds them.

- `append_vtr(along = "rows")` no longer rewrites the store on every
  call. Because the container keeps its row-group index in the trailer,
  the row path used to restream every existing row group through a fresh
  writer into a temp file and swap it over the original, so a call cost
  a full pass over whatever was already on disk. Building a store the
  natural way – append a batch, append the next – was therefore
  quadratic in the number of batches, and the degradation was invisible
  until the store was large: on 30 appends of 100,000 rows x 13 columns,
  per-call time grew with the preceding store size at 0.0215 s/MB (R^2 =
  0.996), and a real 486-million-row build decayed from 8.0M rows/min to
  0.58M rows/min as it passed 4 GB.

  The existing row groups are now neither read nor moved: the new blocks
  and a rebuilt index are written past the container’s trailer and the
  header is patched last, exactly as `along = "cols"` already worked.
  The index entries describing the existing row groups are carried over
  verbatim, which is sound because nothing before the old trailer moves.
  On the same measurement, per-call time is now flat – 0.28 s with 4 MB
  on disk, 0.28 s with 112 MB, slope 0.00006 s/MB – so building a store
  by appending costs one pass over the rows written, however many calls
  it takes.

  Two consequences follow. A row append is now interruption-safe:
  everything is written past the existing data and the header last, so a
  crash leaves the store readable exactly as it was, where the old
  temp-file path could leave a half-written file. And a store grown in
  place is stamped so that readers predating this format refuse it
  rather than misread it.

- `append_vtr(along = "rows")` now honours `compress`. The row path
  ignored the argument and always re-encoded at `"fast"`, whatever the
  file was written with.

- A row append no longer rebuilds every `.vtri` the store carries.
  Rebuilding read the whole store, which would have kept an indexed
  store’s append quadratic even with the container fix. Since a row
  append moves no existing row group, the entries an index already holds
  stay true, so each index now takes in only the row groups just
  appended.

- A `.vtri` sidecar that cannot be read no longer makes the store
  unreadable. `vtri_open()` sized the index file with `ftell()` into a
  `long`, which is 32 bits on Windows, so any index past 2 GB got a
  meaningless size, tripped the sanity guard, and raised
  `corrupt .vtri: entry/slot counts exceed file size` from
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) – on an
  intact index, and with no way to read the store at all, not even by
  falling back to a scan. Offsets now go through the 64-bit calls the
  rest of the package already uses.

  Independently of the sizing, every way of failing to read a sidecar
  now reports no index rather than raising: absent, superseded, written
  by a newer vectra, stale against the store, or malformed. An index
  only ever saves a scan work, so an unusable one costs speed and never
  rows, and raising turned a readable store into an unopenable one.
  [`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
  reports `FALSE` for these and
  [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  rebuilds.

- Opening a `.vtri` no longer costs a copy of the whole index.
  `vtri_open()` read all four of the index’s arrays into memory, so
  every lookup that reached an index first paid for the entire file – a
  223 MB sidecar cost 67 ms and 223 MB per open, and the same read
  repeated on each fetch, which is exactly the cost an index exists to
  avoid. An index past `VTRI_RESIDENT_MAX_BYTES` (4 MB) is now mapped
  read-only instead, and the probe reads the handful of entries a
  chained-hash lookup touches out of the mapping in place. Both backings
  are read through the same accessors, so the probe and the rebuild are
  written once.

  What a lookup costs is now the pages it touches rather than the size
  of the index. On a 5-million-key store, opening a 223 MB sidecar went
  from 67 ms to under the timer’s resolution, and an indexed fetch from
  71.4 ms to 4.75 ms. The effect grows with the index: a 17 GB sidecar
  had needed 17 GB of resident memory per fetch.

  This is also what makes an index past 2 GB usable rather than merely
  readable. A 2.12 GB sidecar over 60 million distinct keys now opens in
  2 ms and answers equality and `%in%` lookups; the same file previously
  had to be allocated in full before it could be probed at all. An index
  too large even to map reports absent, like any other unusable index,
  so a lookup falls back to reading the store rather than exhausting
  memory.

  Below the threshold nothing changes: a small index is still read
  whole, which is cheaper than faulting pages in for a single probe and
  leaves no handle open. Because a mapped index does hold its file open,
  [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  now swaps the rebuilt sidecar into place through the same atomic
  replace the writers use, which waits out a sharing violation rather
  than failing on one.

## vectra 0.11.8

CRAN release: 2026-07-30

### Bug fixes

- A `.vtri` index left behind by `append_vtr(along = "rows")` no longer
  drops rows from a filter on the indexed column. A row append rewrites
  every row group, so the index mapped its keys to row groups that had
  moved; probing it pruned groups that held matching rows, and the query
  returned a subset with no error or warning. On a five-row store,
  `filter(id == "a")` returned one of two matching rows and a key added
  by the append returned none.

  A row append now rebuilds each index the store carries. Independently
  of that, an index records the row and row-group counts it was built
  against, and a query that finds them disagreeing reads the store
  rather than trusting the index – so an index invalidated some other
  way costs the acceleration, never the rows.
  [`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
  reports `FALSE` for such an index, having previously reported on no
  more than the sidecar file existing.

- [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  now makes `filter(col == value)` faster rather than slower
  ([\#9](https://github.com/gcol33/vectra/issues/9)). The index held one
  entry per row, which made it larger than the store it indexed – 131 MB
  against a 49 MB store of 3.2M rows – and
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) read all
  of it eagerly. Lookup cost therefore tracked the size of the store,
  which is what an index is for avoiding, and an indexed store was
  slower than the same store without one.

  An index now holds one entry per distinct key per row group. Over
  8,000 keys in 3.2M rows the sidecar is 0.29 MB rather than 131 MB, and
  fetching one key’s 400 rows takes 5 ms rather than 427 ms – flat in
  the size of the store (5.2 ms at 3.2M rows against 4.7 ms at 200k,
  previously a 10-15x rise), and 20x faster than the same query with no
  index. Building an index is also no longer bounded by holding one
  entry per row in memory.

- A query filtering on the second indexed column of a store now reaches
  that column’s index. A scan loaded the first index it found in schema
  order and could probe no other, so any further index was read on every
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) and never
  used. Indexes are now opened for the column a predicate actually
  filters on, which also means a query reads no index it has no use for.

- [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  accepts the columns of a composite index in any order. Naming them in
  other than schema order wrote a sidecar under a name the scan does not
  look for, leaving the index unused while
  [`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
  reported it present.

- A composite index over a narrow integer column is now probed rather
  than skipped.

- A store carrying both a composite index and a single-column index on
  one of its columns now uses the composite for a predicate the
  composite covers. The single-column index was probed first and stopped
  the composite being consulted, so the more selective index went unused
  on exactly the queries it was built for.

- The `indexing` and `engine` vignettes described the index internals
  incorrectly: open addressing with a stored row-group bitmap per value
  and a 70% load factor, where the format is a chained table at 50%, and
  a claim that the sidecar is memory-mapped, which it is not.
  [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  was also shown being passed `tbl(path)` rather than a path, which
  cannot work.

- A filter reaches a value that is computed rather than held in a
  variable: `filter(x, id == keys[i])`, `filter(x, day > range$hi)`, and
  the like. A bare name was resolved in the calling environment, but any
  larger expression that named no column was rejected as an unsupported
  function. An expression that does name a column still reports an
  unsupported operation as one.

- A failed index write no longer replaces a working index with a
  truncated file; the index is written to a temporary path and moved
  into place.

### New features

- [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
  reports the index a scan will probe, as `hash index (id)` or
  `hash index (a + b)` for a composite. It answers by opening the index
  the scan would open, so it distinguishes an index that will be used
  from a sidecar file that merely exists.

### Breaking changes

- The `.vtri` index format has changed, and indexes written by 0.11.7
  and earlier read as absent: queries and
  [`has_index()`](https://gillescolling.com/vectra/reference/has_index.md)
  behave as though the store has no index. Call
  [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  again to rebuild – the new index is much smaller and is what makes an
  indexed lookup faster than an unindexed one. The `.vtr` format itself
  is unchanged.

## vectra 0.11.7

### New features

- [`nrow()`](https://rdrr.io/r/base/nrow.html),
  [`ncol()`](https://rdrr.io/r/base/nrow.html), and
  [`dim()`](https://rdrr.io/r/base/dim.html) work on a lazy query, via a
  [`dim()`](https://rdrr.io/r/base/dim.html) method for `vectra_node`.
  Both counts come from plan metadata, so the query is neither run nor
  consumed: a `.vtr` table reports the row count held in its row-group
  index (less any rows
  [`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
  has tombstoned), and the row-preserving verbs carry it through –
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md),
  [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
  [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
  window functions, [`head()`](https://rdrr.io/r/utils/head.html),
  [`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)/[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md),
  and
  [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  over counted inputs.

  Verbs whose output length depends on the data –
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  the joins,
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
  [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md)
  – report `NA` rows, as do CSV, SQLite, and TIFF sources, which carry
  no stored row count. Counting those means a full pass over data that
  may be larger than RAM, so
  [`nrow()`](https://rdrr.io/r/base/nrow.html) reports what it knows
  rather than starting one; `count() |> collect()` gives the exact
  number.

  Previously [`nrow()`](https://rdrr.io/r/base/nrow.html) fell through
  to [`base::nrow()`](https://rdrr.io/r/base/nrow.html), which returned
  `NULL` because a node had no
  [`dim()`](https://rdrr.io/r/base/dim.html). A `NULL` row count passed
  to [`sprintf()`](https://rdrr.io/r/base/sprintf.html) produces
  `character(0)`, which [`cat()`](https://rdrr.io/r/base/cat.html)
  prints as nothing at all, so a loop reporting row counts printed blank
  lines instead of failing.

- `append_vtr(x, path, along = "cols")` attaches whole new columns to
  the rows already in a `.vtr` store. The existing columns are never
  read or rewritten: the new columns are encoded and attached on their
  own, so the cost tracks what is being added rather than the size of
  the store.

  This is what lets a table too wide to hold in memory be built a block
  of columns at a time – write the first block with
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
  then append each later block as it is produced, with a peak of one
  block instead of the whole table. `x` must have exactly as many rows
  as the store holds, and column names that do not collide with the
  existing ones; its rows are matched to the store’s rows by position.

  Existing row-group boundaries and column data are untouched, so a
  `.vtri` index built with
  [`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
  over the original columns stays valid across a column append. Unlike a
  row append, a column append writes everything past the end of the
  existing data and patches the file header last, so an interruption –
  or a rejected append, such as a row-count mismatch – leaves the store
  readable exactly as it was.

  `along = "rows"` remains the default and is unchanged
  ([\#8](https://github.com/gcol33/vectra/issues/8)).

### Bug fixes

- [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md)
  names each column’s type again. It mapped the schema’s type codes
  through a numeric lookup table, but the schema bridge reports type
  names, so every column printed as `<NA>`. It also prints the row count
  in its header when the count is known, in place of the former `?`.

### Internals

- Plan nodes gained an optional `static_rows` hook: the exact number of
  rows the node will emit, read off metadata without pulling a batch. It
  is what [`dim()`](https://rdrr.io/r/base/dim.html) reads. A node kind
  that does not implement it reports “unknown”, so a node added later is
  over-cautious rather than wrong, and a scan opts out whenever pruning,
  a pushed-down predicate, or a narrowed row-group range means the
  stored row count is only an upper bound.

- The vendored tdc container gained a widening encoder
  (`tdc_stream_encoder_open_widen`), which is what makes the above
  possible. Block records were already located solely by the trailing
  index, so new blocks can be appended anywhere in the file; the schema
  section, pinned immediately before the first block, could not grow in
  place, so a widened container relocates its schema to the tail and is
  stamped with a new container version. `.vtr` files that are never
  widened are byte-identical to before, and remain readable by any
  reader that could read them.

  A widened container is random-access only, which is how vectra reads
  every `.vtr` anyway.

## vectra 0.11.6

### New features

- [`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md)
  now streams both files through the external sort and merges them in a
  single bounded pass, instead of holding every distinct key of the old
  file resident. Peak memory follows the sort’s spill budget
  ([`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md)),
  so a diff whose key cardinality exceeds RAM no longer blows up.

- `first()` / `last()` work on string columns (previously numeric-only).

- [`lag()`](https://rdrr.io/r/stats/lag.html) / `lead()` preserve a
  string column (previously silently returned a column of zeros);
  integer / double columns are unchanged.

- `rank(ties.method = "average")` computes base R’s average rank. Bare
  [`rank()`](https://rdrr.io/r/base/rank.html) keeps its established
  min-rank behaviour (dplyr `min_rank()`).

- `n()` works inside
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  returning the partition (or group) size repeated per row, in addition
  to its existing use as a
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  aggregate.

- [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  accepts expressions over aggregates
  (`summarise(rate = sum(hits) / sum(at_bats))`) and evaluates its
  arguments sequentially, so a later output can reference an earlier one
  (`summarise(m = mean(x), z = x_dev / m)`), matching dplyr.

### Bug fixes (audit pass)

#### Crashes / memory safety

- A namespace-qualified function head (`pkg::fn(...)` / `pkg:::fn(...)`)
  no longer crashes the expression serializer with “the condition has
  length \> 1” under R \>= 4.2. `serialize_expr` (the shared `mutate` /
  `filter` / post-`summarise` serializer) unwraps `::` / `:::` to the
  bare name, and a namespace-qualified top-level
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  call now routes to the aggregation parser, so an unknown one reports
  `unknown aggregation function: <name>` instead of the cryptic length
  error.

- Hash join no longer duplicates rows or loops forever on a many-to-many
  key whose build-chain length lines up with the internal 65536-row emit
  cap: the resumable probe conflated “chain exhausted” with the “not
  resuming” sentinel at the cap boundary. Both the hash and
  block-nested-loop probe paths are fixed.

- Reading a crafted GeoTIFF no longer corrupts the heap:
  `read_tag_ascii` now bounds the tag element count like its sibling tag
  readers (a negative BigTIFF count reached a `memcpy` with a huge
  size).

- Opening a crafted SQLite database no longer reads past a page buffer:
  the header `page_size` is validated (power of two in \[512, 65536\]),
  the reserved region is bounded, and a page’s declared cell count must
  fit the page.

- [`pmin()`](https://rdrr.io/r/base/Extremes.html) /
  [`pmax()`](https://rdrr.io/r/base/Extremes.html), the date/time
  helpers, and the unary math functions (`round`, `abs`, `sqrt`,
  `floor`, …) no longer read past a `logical` operand’s buffer (the
  operand was type-punned through the double buffer unless it was
  already `int64`).

- Reading a crafted GeoTIFF’s GeoKey directory no longer over-reads the
  heap: the key count and citation offset/length are bounded without an
  intermediate multiply/add that overflowed `int64`, and a BigTIFF IFD
  with an implausible entry count is rejected instead of spinning.

- Opening a crafted `.vecr` raster no longer overflows the index
  allocation: the declared tile count is bounded against the file size
  before allocating.

- [`substr()`](https://rdrr.io/r/base/substr.html) with a huge negative
  `start` no longer triggers signed-overflow UB.

- The parallel column copy in
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  no longer risks a `longjmp` out of an OpenMP region: the builder input
  validation is hoisted to the serial master before the parallel append.

#### Larger-than-RAM bounds

- GeoTIFF export
  ([`vec_to_tiff()`](https://gillescolling.com/vectra/reference/vec_to_tiff.md))
  streams the raster in row strips instead of materializing every band
  as doubles in RAM.

- A grouped
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  / [`count()`](https://gillescolling.com/vectra/reference/count.md) on
  a high-cardinality key streams its output in bounded batches instead
  of one batch sized to the number of groups.

- A very large TIFF (\> 2 GB, BigTIFF) now seeks with a 64-bit offset on
  Windows.

#### Correctness

- `arrange(desc(x))` places `NA` last (dplyr `na.last = TRUE`)
  consistently on both the in-memory and the spilled path; sort NA
  placement is now a per-key option so window value sorts (`cume_dist`,
  rank) keep treating `NA` as the largest value.

- [`as.character()`](https://rdrr.io/r/base/character.html) /
  [`paste()`](https://rdrr.io/r/base/paste.html) of a `double` keep full
  precision (15 significant digits) instead of truncating to 6.

- [`paste()`](https://rdrr.io/r/base/paste.html) /
  [`paste0()`](https://rdrr.io/r/base/paste.html) stringify `NA` to
  `"NA"` (base R) instead of returning `NA`.

- [`as.character()`](https://rdrr.io/r/base/character.html) /
  [`paste()`](https://rdrr.io/r/base/paste.html) of a computed `NaN` /
  `Inf` / `-Inf` format as R does (`"NaN"` / `"Inf"` / `"-Inf"`), not
  the platform’s lowercase `%g` output.

- [`as.integer()`](https://rdrr.io/r/base/integer.html) /
  [`as.numeric()`](https://rdrr.io/r/base/numeric.html) /
  [`as.logical()`](https://rdrr.io/r/base/logical.html) support the
  `double`, string, and logical source types base R does
  (e.g. `as.integer(2.7)` is `2`).

- Grouped `first()` / `last()` return the literal first / last element
  of the group instead of `NA` when the group contains any `NA`.

- Overview `bilinear` / `gauss` resampling no longer shifts pixels half
  a cell to the north-west.

#### dplyr compatibility

- `.by` (and `.keep`) are honored in
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  and
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  instead of being turned into a stray column / predicate.

- `arrange(-x)`, `if_any()` / `if_all()`,
  [`across()`](https://gillescolling.com/vectra/reference/across.md)
  with anonymous lambdas (`\(x) ...`) and `{.fn}` in `.names`, the
  `.data[[var]]` pronoun, and
  [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  list splicing with a character `.id` now work.

- [`select()`](https://gillescolling.com/vectra/reference/select.md) and
  [`across()`](https://gillescolling.com/vectra/reference/across.md)
  resolve tidyselect helpers that reference an external variable
  (`all_of(v)`, `any_of(cols)`) in the caller’s environment.

- `if_any()` / `if_all()` accept an anonymous `\(x) ...` lambda, not
  only a `~ .x` formula.

- [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
  sorts by an expression (`arrange(x + y)`, `arrange(desc(x * 2))`), and
  a window function may reference a column created earlier in the same
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
  (evaluation now follows dplyr’s left-to-right order).

- [`median()`](https://rdrr.io/r/stats/median.html) / `n_distinct()`
  accept an expression or the `.data[[var]]` pronoun (materialized like
  [`mean()`](https://rdrr.io/r/base/mean.html) /
  [`sum()`](https://rdrr.io/r/base/sum.html) already were).

- [`across()`](https://gillescolling.com/vectra/reference/across.md)
  errors on a duplicate output name instead of silently dropping a
  result;
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  rejects `.keep` / `.preserve`.

#### Robustness

- A GEOS constant-geometry expression (`st_distance(geom, const)`) warms
  the shared geometry’s envelope before the parallel loop, removing a
  data race.

- [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  warns when a component of mutually-overlapping features cannot be
  tiled within the memory budget, instead of silently exceeding it.

- Reading a CSV warns (once, naming the column) when a value past the
  type-inference window does not match the inferred type and is read as
  `NA`.

## vectra 0.11.5

### Bug fixes

- Windowed rolling
  [`roll_min()`](https://gillescolling.com/vectra/reference/rolling.md)
  /
  [`roll_max()`](https://gillescolling.com/vectra/reference/rolling.md)
  no longer corrupt the heap on a long partition with a short time
  window (the monotonic-deque index could run past its buffer).

- [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
  with a unary math function
  ([`sqrt()`](https://rdrr.io/r/base/MathFun.html),
  [`abs()`](https://rdrr.io/r/base/MathFun.html),
  [`log()`](https://rdrr.io/r/base/Log.html),
  [`round()`](https://rdrr.io/r/base/Round.html), …) on a `double`
  column no longer leaks memory on every batch, which could exhaust
  memory during a large streamed
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md).

- Row-group pruning is more accurate: a
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md)
  with a fractional threshold on a sorted integer column
  (`filter(x < 2.9)`), an interior all-`NaN` row group, and a quantized
  column no longer drop rows that actually match.

- Date/time: `year()`, `month()`,
  [`floor_time()`](https://gillescolling.com/vectra/reference/floor_time.md),
  and friends now use the column’s stored `Date` / `POSIXct` class to
  decide days-vs-seconds instead of guessing by magnitude (a near-epoch
  `POSIXct` was misread), and compute calendar fields with portable
  arithmetic that is correct for pre-1970 dates on Windows.
  [`as.Date()`](https://rdrr.io/r/base/as.Date.html) returns `NA` for an
  invalid date (`"2021-02-30"`) instead of a normalized one.

- GeoTIFF reading now inverts the horizontal predictor (tag 317), so
  DEFLATE-compressed files written with `PREDICTOR=2` (a GDAL/terra
  default) decode correctly instead of as differenced garbage.

- Window functions match dplyr: `ntile()` front-loads the remainder,
  `row_number(desc(x))` keeps ties in first-arrival order, and a logical
  column feeds [`cumsum()`](https://rdrr.io/r/base/cumsum.html) / rank
  windows correctly.

- [`min()`](https://rdrr.io/r/base/Extremes.html) /
  [`max()`](https://rdrr.io/r/base/Extremes.html) propagate `NaN`
  regardless of position; [`any()`](https://rdrr.io/r/base/any.html) /
  [`all()`](https://rdrr.io/r/base/all.html) treat `NaN` as `NA`; `NaN`
  join keys match each other; `x %in% set` always returns a logical (an
  `NA` operand is `FALSE`, or `TRUE` if the set contains `NA`).

- Hash joins now emit many-to-many output in bounded chunks: a hot key
  matched by a large probe batch no longer materializes the whole cross
  product in one resident batch (the probe resumes mid-chain across
  batches, on both the in-memory and spilled block-nested-loop paths).

- [`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
  errors on a non-string key/blocking column instead of crashing; a join
  on more than 16 key columns is rejected rather than overrunning
  internal buffers.

- [`right_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  suffixes a non-key column present on both sides (`.x` / `.y`) instead
  of emitting two columns with the same name.

- A column with no declared type in a SQLite table (BLOB affinity) reads
  its numeric cells as text instead of dropping the whole column to
  `NA`; the reader bounds-checks on-disk offsets so a corrupt database
  cannot over-read. The GeoTIFF and SQLite readers reject crafted files
  with overflowing sizes.

- `int64` values above 2^53 warn about precision loss on the common
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  path (previously only a rarer path warned).

### New features

- [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
  gains a `col_types` argument to force specific column types
  (`c(zip = "character")`), so a zero-padded identifier column is not
  numericized by type inference.

## vectra 0.11.4

### Behaviour changes

- [`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  /
  [`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  /
  [`right_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  /
  [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  /
  [`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  /
  [`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  gain an explicit `na_matches` argument (`"na"`, the default, matches
  `NA` to `NA` as in dplyr; `"never"` uses SQL NULL semantics).

- [`grepl()`](https://rdrr.io/r/base/grep.html),
  [`gsub()`](https://rdrr.io/r/base/grep.html), and
  [`sub()`](https://rdrr.io/r/base/grep.html) now treat the pattern as a
  regular expression by default (`fixed = FALSE`), matching base R. Pass
  `fixed = TRUE` for literal matching. They also honour
  `ignore.case = TRUE`; `perl = TRUE` is rejected with a clear error
  (the engine uses POSIX extended regexps).

- [`round()`](https://rdrr.io/r/base/Round.html) now rounds halves to
  even (`round(2.5)` is `2`), matching base R.

### Bug fixes

- SQLite: reading a `BLOB` column, or a `TEXT` value larger than 64 KB,
  no longer reads past the reader’s buffer (a crash on ordinary input);
  a non-text value in a text column reads as `NA`. Writing a row larger
  than a page (for example a long text value) no longer overflows the
  page buffer – the writer now emits SQLite overflow pages, so large
  cells round-trip. Database files larger than 2 GB are seeked with
  64-bit offsets on Windows. A text/blob value in a numeric column reads
  as `NA` rather than a fake `0`.

- CSV: a leading UTF-8 byte-order mark is stripped from the header, so
  the first column name is no longer corrupted (common in “CSV UTF-8”
  exports). A value that disagrees with the inferred column type past
  the inference window becomes `NA` rather than silently `FALSE` for
  logical columns. New `guess_max` argument to
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
  (default 1000; `Inf` scans the whole file) for columns whose type only
  becomes apparent later in the file.

- [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
  [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
  evaluate expressions left to right, so an expression may reference a
  column created earlier in the same call
  (`mutate(a = x + 1, b = a * 2)`).
  [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
  also keeps the grouping columns.

- Integer-dtype raster output
  ([`focal()`](https://gillescolling.com/vectra/reference/focal.md),
  [`terrain()`](https://gillescolling.com/vectra/reference/terrain.md),
  [`warp()`](https://gillescolling.com/vectra/reference/warp.md),
  [`mask()`](https://gillescolling.com/vectra/reference/mask.md),
  [`proximity()`](https://gillescolling.com/vectra/reference/proximity.md),
  [`vec_write_raster()`](https://gillescolling.com/vectra/reference/vec_write_raster.md),
  …) round-trips `NA` instead of writing it as a valid `0`, by recording
  a per-dtype nodata sentinel when the data contains `NA`.

- [`across()`](https://gillescolling.com/vectra/reference/across.md)
  accepts purrr-style formula lambdas (`~ .x + 1`,
  `~ mean(.x, na.rm = TRUE)`).

- Window functions accept `min_rank()` and a compound argument
  (`cumsum(x + y)`, `rank(desc(a * b))`).

- [`count()`](https://gillescolling.com/vectra/reference/count.md)
  groups by the existing
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
  keys plus the counted columns, and `count(wt = )` / `tally(wt = )` sum
  weights with `na.rm = TRUE`, matching dplyr.

- [`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md),
  and [`slice()`](https://gillescolling.com/vectra/reference/slice.md)
  are group-aware on grouped input.

- Corrupt or truncated `.vtr` / `.vtri` files are rejected cleanly
  instead of over-reading, over-writing, or looping: the `.vtri` reader
  validates its entry and slot counts against the file size and bounds
  its probe chains, and the bundled tdc decoder bounds its LZ sequence
  cursor and back-references and computes the dictionary offset-table
  size in 64-bit.

- [`focal()`](https://gillescolling.com/vectra/reference/focal.md)
  reports a clean error instead of a null-pointer dereference if a
  per-thread scratch allocation fails.

## vectra 0.11.3

CRAN release: 2026-07-17

### Bug fixes

- [`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
  and
  [`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
  now consume their input node, matching every other terminal
  ([`collect()`](https://gillescolling.com/vectra/reference/collect.md),
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)).
  The streaming batch cursor previously drained the plan without
  invalidating the handle, so collecting the same node again re-drove an
  already-drained spill plan and returned wrong or empty data instead of
  raising the documented “already consumed” error.

- The holistic aggregates
  ([`median()`](https://rdrr.io/r/stats/median.html), `n_distinct()`)
  and [`kmer()`](https://gillescolling.com/vectra/reference/kmer.md) now
  bound the fan-in of their external record merge. The shared record
  sort-merge opened every spilled run at once, so a genuinely
  larger-than-RAM aggregate could grow its resident read buffers with
  the run count and exhaust the process file-handle table. It now
  reduces the runs to a bounded fan-in over multiple passes first, as
  the row sort behind
  [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)/grouped
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  already did, keeping peak memory and open handles bounded regardless
  of input size.

- `propagate()` no longer stops at a fixed 20 levels of hierarchy. A
  parent-child chain deeper than 20 within a batch left the deepest rows
  `NA`; propagation now runs to convergence, so an arbitrarily deep
  hierarchy resolves fully.

- `resolve()` and `propagate()` coerce their foreign-key and primary-key
  columns to a common type before matching. A key pair stored in
  different numeric types (for example a `double` foreign key against an
  integer primary key) could silently fail to match; they are now
  compared like with like.

- `lookup(.report = TRUE)`, the default, no longer materializes the
  whole fact table. It collected the entire fact table into memory
  purely to count its rows for a diagnostic message; the count and the
  unmatched-key preview now stream in bounded memory.

## vectra 0.11.2

### Bug fixes

- The gzip (`.gz`) reader now streams. It previously read the whole
  compressed file into memory and inflated it whole into a second
  buffer, so the readable size was capped at available RAM, and its size
  query used a 32-bit `ftell`, so a `.gz` past 2 GB compressed failed to
  open at all on Windows. It now feeds the raw deflate stream through
  miniz’s `tinfl` coroutine into a 32 KB wrapping window (which doubles
  as the LZ dictionary) and serves bytes from that window, with 64-bit
  file offsets throughout; peak memory is the window plus one input
  block, independent of file size. A `.gz` whose inflated size exceeds
  RAM (and a compressed size past 2 GB) now reads fine. Enables
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
  on multi-GB compressed streams.

- The gzip reader now follows concatenated gzip members, so a
  multi-member `.gz` (as produced by `bgzip` and `cat a.gz b.gz`) reads
  whole instead of stopping at the first member. The header is parsed
  field by field with no fixed size cap. This affects
  [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md),
  [`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md),
  [`tbl_fastq()`](https://gillescolling.com/vectra/reference/tbl_fastq.md),
  and
  [`tbl_bed()`](https://gillescolling.com/vectra/reference/tbl_bed.md)
  on any `.gz` input.

- A truncated or corrupt `.gz` now fails loudly. The scanners
  distinguish a hard decode error from a clean end of stream, so a
  partial compressed file raises an error instead of silently returning
  a short read.

## vectra 0.11.1

CRAN release: 2026-07-10

### Bug fixes

- A query is now consumed by exactly one terminal operation.
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  and
  [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  join `write_*()` in invalidating the node once its pull cursor is
  drained, so a second terminal op on the same node (for example
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)-ing
  a pipeline to inspect it and then
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)-ing
  the same object) raises a clear “already consumed” error instead of
  re-driving an exhausted plan. Previously the second pass returned
  empty or, on a multi-spill plan, silently reinterpreted a string
  column’s bytes as doubles
  ([\#5](https://github.com/gcol33/vectra/issues/5)).

- [`offload()`](https://gillescolling.com/vectra/reference/offload.md)
  shards are re-collectable: a shard rebuilds a fresh scan on each
  access, so a partition stays an iterable list of shards under the
  consume-once rule.

- `vec_builder_*` now errors on a type-mismatched or dictionary-deferred
  array instead of reinterpreting raw bytes, matching the guard the
  writer already applied at its own boundary.

## vectra 0.11.0

### Delimited-file reader gains a `delim` argument

- [`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
  takes a `delim` argument (default `","`), so tab- and
  semicolon-separated files read natively without a transcode step.
  `delim = "\t"` streams a GBIF occurrence export (SIMPLE_CSV) straight
  through; `delim = ";"` reads the semicolon exports common in European
  data. Quoting stays RFC 4180 for any delimiter, so a quoted field may
  still contain the delimiter, newlines, and doubled quotes.

### Feature-space nearest-neighbour tools

- New
  [`feature_knn()`](https://gillescolling.com/vectra/reference/feature_knn.md):
  nearest-neighbour search in *predictor* space rather than on
  coordinates. For each streamed query row it returns the mean distance
  to the nearest `k` (or nearest `percentage`%) of a resident reference
  cloud, with a Euclidean or Mahalanobis metric. The query side streams
  one batch at a time so the projection side can exceed memory; the
  reference cloud is materialized once, whitened for the chosen metric,
  and scanned in parallel (a bounded max-heap keeps peak memory at O(k)
  per thread). This is the environmental-novelty counterpart to the
  coordinate-based
  [`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md).
- New
  [`rast_feature_distance()`](https://gillescolling.com/vectra/reference/rast_feature_distance.md):
  the same distance computed out-of-core over a projection raster. The
  reference raster is read once and indexed; the projection raster
  streams one tile-row strip at a time and the distance surface is
  written aligned to its grid. This is the streaming distance surface
  behind an environmental-novelty / transferability diagnostic such as
  MOP (Owens et al. 2013); the strict non-analogous-conditions layers
  compose from a per-band range reduce plus
  [`rast_calc()`](https://gillescolling.com/vectra/reference/rast_calc.md).
- The Species Distribution Models vignette gains a transferability /
  novelty section covering both.

### Bounded memory across the remaining streaming paths

- The streaming operations that still grew resident state with the input
  size or with key skew are now bounded. Interval joins run as a serial
  sweep-merge over externally sorted sides; k-mer counting streams
  through an external sort-merge (`rec_spill`) instead of a hash table
  that grew with the input; grouped top-1 (`slice_min`/`slice_max`,
  `n = 1`) keeps one champion per open group via a `(key, row-id)` sort;
  fuzzy joins stream the probe side and spill the build side when it
  overflows the budget; and ungrouped windows with mixed orderings
  decompose into a chain of single-spec streaming nodes rather than
  materializing the table. A shared `rec_spill` external merge and a
  shared `key_snap` group-boundary detector back these paths, so there
  is one implementation of each rather than several.

## vectra 0.10.8

### Bounded-memory joins under key skew

- Hash joins now keep a bounded memory peak regardless of how skewed the
  join keys are. When the materialized build side exceeds the memory
  budget
  ([`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md)),
  both sides grace-hash spill into 64 run-file partitions and join one
  partition at a time. A partition that is itself still over budget is
  re-partitioned by its sub-join with a depth-salted hash (a murmur3
  finalizer, not a bare XOR, so colliding keys actually redistribute
  across levels rather than landing in the same bucket again). A
  partition that a single dominant key value makes un-splittable –
  hashing cannot separate identical keys at any depth – drops at the
  third level to a block-nested-loop: the build side is read in
  budget-sized blocks and the probe side is re-scanned once per block.
  Peak memory is one build block plus one probe batch plus a
  one-bit-per-row matched bitset, so no join retains an unbounded
  resident partition. Applies to all five kinds (inner, left, right,
  full, semi, anti). Partition files are opened lazily on first row, so
  a hot key no longer creates 63 empty spill files per level.

### Fixes

- An empty build partition no longer corrupts the heap on a
  [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md).
  The hash-table constructor floors its slot allocation to one row; the
  true build count is now recorded separately, so the finalize pass over
  a build-empty partition reads zero rows instead of a nonexistent
  row 0. Surfaced by the one-sided partitions the recursive spill
  routinely produces.
- The sorted-input merge-join path is now consistent with the hash path
  in three cases it previously disagreed on: a match group beginning at
  build row 0 no longer spins forever (the group cursor used a
  positive-only sentinel that conflated position 0 with “inactive”); an
  unmatched build row under
  [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md)
  is emitted exactly once rather than doubled by the finalize pass; and
  an `NA` key never matches (both-`NA` at an equal-compare point is
  treated as unmatched, as in the hash path).

## vectra 0.10.7

### Fixes

- The overlay engine again builds without OpenMP (e.g. the default macOS
  CRAN toolchain). The serial fallback in `C_overlay_run` called
  `process_tile()` with one argument short of its signature, so the
  no-OpenMP branch failed to compile; it now passes the point-in-polygon
  flag like the parallel branch.
- Encoding a string column of duplicate empty strings no longer triggers
  undefined behavior. The DICT_1D dictionary encoder compared a
  hash-table cache candidate with `memcmp(str, s, len)`; when every
  string is empty the heap data pointer is `NULL` and `len` is 0, so
  `memcmp(NULL, NULL, 0)` tripped the UBSan nonnull check on CRAN’s
  ASAN/UBSAN runner. The comparison is now short-circuited on
  `len == 0`, where the already-checked length equality makes the
  strings equal. Output is unchanged. Fixed in vendored tdc
  (`gcol33/tdc`).
- An audit swept the rest of this undefined-behavior class (a `NULL`
  pointer with length 0 passed to `memcmp`/`memcpy`) across the engine
  and vendored tdc. Two more sites are fixed: a blocked
  [`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
  whose probe block column is all empty strings compared block keys with
  `memcmp(build, NULL, 0)`, and tdc’s string min/max stats copied a
  zero-length prefix from a `NULL` all-empty-string heap. Both are now
  length-guarded; results are unchanged.

## vectra 0.10.6

### Bounded-memory top-N and fuzzy join

- [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)/`slice_max(..., with_ties = FALSE)`
  no longer materialize their input. The streaming top-N node keeps at
  most `k` rows in a size-`k` max-heap (fixed-width values overwritten
  in place, strings held as per-slot owned copies freed on eviction); a
  non-winning row costs one comparison and no copy, so peak memory is
  `min(k, n)` rows rather than the whole child. NA in the order column
  now always sorts last, independent of direction, matching dplyr and
  the `with_ties = TRUE` path – an earlier version treated NA as the
  maximum under
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md).
- [`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
  now streams the probe side instead of materializing both inputs and
  the whole cross-product of matches. The build side is materialized
  once and, with a blocking column, indexed by exact block key; the
  probe side streams one batch at a time, and each batch’s matches are
  computed, ordered, and emitted in chunks before the next batch is
  pulled. Peak memory is the build side plus one probe batch plus that
  batch’s matches, and the `(probe, distance)` output order is preserved
  without a global sort. These were the last two operators that buffered
  their whole input, so every verb is now bounded-memory.

## vectra 0.10.5

### Spill-safe window functions

- Ordered ungrouped windows that need the whole table sorted
  ([`rank()`](https://rdrr.io/r/base/rank.html), `dense_rank()`,
  `percent_rank()`, `cume_dist()`, `row_number(col)`, `roll_*()`,
  [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`, `ntile()`) now
  stream, closing the last window case that materialized the whole
  table. A single spill-safe global sort is inserted below the window,
  then one forward pass computes each spec from bounded running state,
  so peak memory is one batch plus the sort’s own spill buffer rather
  than the whole table. Two ordering tricks keep the awkward cases
  single-pass: `cume_dist()` sorts descending so `count(<= v)` is known
  when each value group opens, and `lead()` is computed as
  [`lag()`](https://rdrr.io/r/stats/lag.html) on the row-id-reversed
  stream. When one
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
  mixes specs that need conflicting sort orders (for example `rank(x)`
  and `rank(desc(x))` together), the node falls back to the in-memory
  path; splitting them into separate
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
  calls keeps each streaming.

## vectra 0.10.4

### Spill-safe window functions

- Grouped window functions
  ([`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
  followed by
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
  with `row_number()`, [`rank()`](https://rdrr.io/r/base/rank.html),
  [`lag()`](https://rdrr.io/r/stats/lag.html),
  [`cumsum()`](https://rdrr.io/r/base/cumsum.html),
  [`roll_sum()`](https://gillescolling.com/vectra/reference/rolling.md),
  and the rest) no longer materialize the whole table in memory. The
  window node now sorts on the group keys (external, spill-safe),
  processes one group at a time, and restores the original row order, so
  peak memory is a single group rather than the full input – the same
  bound `group_by() |> summarise()` already had. Results are unchanged:
  rows come back in original order, and cumulative windows still run in
  arrival order within each group.

- Ungrouped cumulative windows (`mutate(cs = cumsum(x))` and the rest of
  the `cumsum`/`cummean`/`cummin`/`cummax`/`row_number()` family, with
  no
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md))
  now stream one batch at a time with O(1) running state, so a running
  aggregate over a larger-than-RAM table holds only one batch. Ordered
  ungrouped windows that need the whole table sorted
  ([`rank()`](https://rdrr.io/r/base/rank.html), `dense_rank()`,
  `percent_rank()`, `cume_dist()`, `row_number(col)`, `roll_*()`,
  [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`, `ntile()`) keep
  the in-memory path.

## vectra 0.10.3

### Bug fixes

- The parallel `.vtr` reader no longer risks an intermittent crash when
  a read fails mid-collect. Row groups are decoded on OpenMP worker
  threads; on an I/O or decode failure (a truncated or removed file, a
  short read, a corrupt block) the reader used to raise the error
  directly from a worker thread, where the R error mechanism’s longjmp
  corrupts the master thread’s stack. The reader now allocates every
  batch on the master thread and only fills them from disk in parallel,
  capturing the first failure and re-raising it once the parallel region
  joins, so a failed read is a clean, catchable R error.

### BED streaming scan backend

- [`tbl_bed()`](https://gillescolling.com/vectra/reference/tbl_bed.md)
  streams a BED (Browser Extensible Data) file of genomic features as a
  lazy table, one feature per row, with the standard BED columns in
  order (`chrom`, `start`, `end`, `name`, `score`, `strand`,
  `thickStart`, `thickEnd`, `itemRgb`, `blockCount`, `blockSizes`,
  `blockStarts`; extra fields past the twelfth as `V13`, `V14`, …). The
  column count is fixed by the first feature line and every later line
  must match. Fields are whitespace-delimited (tab or space); blank,
  `#`, `track`, and `browser` lines are skipped; gzip (`.bed.gz`) input
  is read transparently, and the scan reports its feature count on
  completion (`quiet = TRUE` suppresses it).
- Coordinates are read faithfully: `start` is 0-based and `end`
  half-open, both returned exactly as stored. Paired with the existing
  [`interval_join()`](https://gillescolling.com/vectra/reference/interval_join.md),
  this makes vectra a streaming genome-interval overlap engine. For
  base-overlap semantics matching bedtools and
  [`GenomicRanges::findOverlaps()`](https://rdrr.io/pkg/IRanges/man/findOverlaps-methods.html),
  use `interval_join(..., closed = FALSE)`, which requires a strictly
  positive overlap and so does not pair abutting features.
  Recovery-tested against `findOverlaps` and on explicit half-open
  boundary (off-by-one) cases.
- A malformed feature line is a loud error, not a silent drop: an
  inconsistent field count, a non-integer `start`/`end`, or fewer than
  three fields stops the scan. Optional integer fields (`score`,
  `thickStart`, `thickEnd`, `blockCount`) accept `.` or `NA` as missing.

## vectra 0.10.2

### `kmer()` k-mer spectrum node

- `kmer(x, seq, k, by = , canonical = )` counts every k-mer of a
  nucleotide column, grouped by zero or more key columns, returning one
  row per distinct (group, k-mer) with a `kmer` string and an integer
  `count`. It is the set-wise companion to the per-row `seq_*` family: a
  blocking step like
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
  but only the k-mer table is held, not the input, so a spectrum over a
  larger-than-RAM read set stays bounded. Each k-mer is packed into 2
  bits per base and counted in a native open-addressing hash (k in
  `1:32`); a window containing any non-`ACGT` base is skipped, matching
  dedicated k-mer counters. `canonical = TRUE` collapses a k-mer with
  its reverse complement. Recovery-tested against a hand-rolled
  tabulation (ungrouped, by-group, canonical, non-ACGT skipping,
  streaming invariance).

- Internal: the group-key store (`KeyArena`) shared by
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  and [`kmer()`](https://gillescolling.com/vectra/reference/kmer.md),
  and the 2-bit base encoding shared by `seq_*` and
  [`kmer()`](https://gillescolling.com/vectra/reference/kmer.md), are
  now single-sourced (`key_arena`, `seq_util`).

### Fixes

- Reading a `.vtr` no longer holds an OS file handle open for the scan
  node’s whole lifetime. The reader loads the row-group index into
  memory at open and reopens the file per read (as the parallel reader
  already did), so an idle scan node – one created or already collected
  but not yet garbage-collected – keeps no descriptor. A tight
  `tbl(f) |> collect()` loop previously leaked one handle per iteration
  until the OS refused further opens and `vtr1_open_tdc` failed
  (crashing when the failure landed mid-decode); it now runs unbounded.
- A data.frame lifted into a lazy node
  ([`tbl_xlsx()`](https://gillescolling.com/vectra/reference/tbl_xlsx.md),
  and the data.frame inputs to
  [`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md)
  /
  [`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md)
  /
  [`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md))
  now owns its temporary `.vtr` for the node’s lifetime and unlinks it
  when the node is freed, instead of deleting it when the creating call
  returned.

## vectra 0.10.1

### FASTA / FASTQ streaming scan backends

- [`tbl_fasta()`](https://gillescolling.com/vectra/reference/tbl_fasta.md)
  and
  [`tbl_fastq()`](https://gillescolling.com/vectra/reference/tbl_fastq.md)
  stream a biological-sequence file as a lazy table, one record per row:
  `id`, `desc`, `seq` for FASTA and an additional `qual` for FASTQ. `id`
  is the first whitespace-delimited token of the header and `desc` is
  the remainder (an empty string when absent). Records stream one batch
  at a time, so a read set larger than RAM never fully materializes, and
  the `seq_*` expression family works directly on the `seq` column. Gzip
  input (`.fasta.gz`, `.fq.gz`, …) is read transparently through the
  same vendored miniz path CSV uses. A record cut short — a header where
  a `>`/`@` is expected, a FASTQ record missing a line, or a quality
  string whose length does not match its sequence — is a loud error
  rather than a silent drop, and the scan reports how many records it
  read on completion (`quiet = TRUE` suppresses it). Recovery-tested
  against `Biostrings` and `ShortRead`.

- The byte reader that backs the streaming text scans (plain and gzip)
  is now a shared `byte_reader` used by both the CSV and FASTA/FASTQ
  backends.

## vectra 0.10.0

### `seq_*` biological-sequence expressions

- A family of `seq_*` functions now works directly inside
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  and
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  over a sequence held in an ordinary string column, computed per row in
  C and parallelized across rows: `seq_length`, `seq_gc`, `seq_revcomp`,
  `seq_complement`, `seq_reverse`, `seq_transcribe` (DNA\<-\>RNA),
  `seq_translate` (standard genetic code), `seq_subseq`, and `seq_dist`
  (Levenshtein / Damerau-Levenshtein / Hamming edit distance to a
  reference column or constant). A missing or unparseable cell yields
  `NA`, the same contract as the `st_*` geometry and embedding-distance
  families. Complement handles the IUPAC ambiguity codes. See
  [`?seq_expressions`](https://gillescolling.com/vectra/reference/seq_expressions.md).

## vectra 0.9.12

### `compress = "small"` parallelizes the candidate sweep

- The try-all-pick-smallest encoder now trial-encodes a column’s
  candidate specs across threads: each thread encodes a disjoint slice
  of candidates into its own scratch buffer and the smallest record is
  reduced. Large row groups (the ones where the sweep dominates encode
  time) compress markedly faster on multi-core machines. The chosen spec
  is independent of thread count — ties break to the lowest candidate
  index — so `"small"` files are byte-identical regardless of how many
  cores encode them, and never larger than `"fast"`. The sweep stays
  serial for small blocks and inside an existing parallel region.

### All-null row groups are pruned during scan

- A filter comparison against a column whose values are all `NA` in a
  row group (`x > 5`, `x == "a"`, `x != 5`, …) is `NA` for every row,
  which the filter drops. The scan now recognizes this from the row
  group’s null count and skips the whole group without reading it, even
  for numeric columns that carry no min/max (an all-`NA` column has
  none). Results are unchanged; the pruning only avoids reading groups
  that could not have produced a row.

## vectra 0.9.11

### `compress = "small"` now does adaptive per-column encoding

- `write_vtr(..., compress = "small")` previously behaved identically to
  `"fast"`. It now performs try-all-pick-smallest: each column is
  trial-encoded under a set of candidate tdc specs — alternative models
  (delta, second-order and FCM/DFCM float predictors, numeric and string
  dictionaries, sparse-zero) crossed with stronger entropy coders
  (optimal-parse LZ, split-stream LZ, FSE, 4-stream Huffman, per-lane) —
  and the smallest block record is kept. The `"fast"` encoding is always
  a candidate, so `"small"` files are never larger than `"fast"` (about
  16-40% smaller on mixed data). Encode is slower in proportion to the
  number of candidates; decode and the on-disk format are unchanged.
- `compress = "none"` now works on string columns (previously errored).

### Faster `collect()` on dictionary-encoded string columns

- [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  on a string column now interns each unique value once and fills the
  result by index, instead of hashing every row. The direct-read path
  decodes the on-disk dictionary block into (unique values + per-row
  indices) via the new `tdc_decode_block_dict` primitive and carries it
  through a `VecArray` dictionary side-channel to the fill. On 5M rows
  of wide, heavily- duplicated strings the collect drops from ~0.34s to
  ~0.03s. Results are unchanged; NA, empty, and UTF-8 values round-trip
  identically.

### Fixes

- Fixed two correctness bugs in the tdc codec that surfaced through the
  new `"small"` encoder: sparse-zero blocks and single-element all-zero
  columns could fail to decode. Both are covered by new regression
  tests.

## vectra 0.9.10

### One memory knob for the whole engine

- A single ceiling, `options(vectra.memory = "8GB")`, now governs every
  part of the engine that buffers before spilling. It is resolved by the
  new exported
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md)
  (accepts a byte count or a `"512MB"` / `"8GB"` string). The
  auto-detected default is half of system RAM, floored at 1 GB; an
  explicit value is honored as given. Row-group size (`batch_size`) is a
  separate cache-locality control and is unaffected.
- The external sort’s spill threshold, the self-overlay working-set cap,
  and the streaming spatial flush / partition-routing buffers all derive
  their budget from
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md)
  instead of separate constants. The per-subsystem options
  `vectra.spatial_flush`, `vectra.partition_budget`,
  `vectra.overlay_mem_limit`, and `vectra.overlay_parse_chunk` are
  removed; per-call `flush_rows` (an explicit row cap) remains the
  override on the streaming spatial verbs and
  [`offload()`](https://gillescolling.com/vectra/reference/offload.md).

### Joins spill to disk instead of running out of memory

- When a join’s build (right) side outgrows
  [`vectra_mem()`](https://gillescolling.com/vectra/reference/vectra_mem.md),
  the engine switches to a grace-hash join: both sides are
  hash-partitioned by key into run-files and joined one partition at a
  time, so peak memory stays bounded. The result is identical to the
  in-memory join for every kind (inner, left, right, full, semi, anti),
  including composite keys and many-to-many matches.

## vectra 0.9.9

### Faster `spatial_overlay()`

- Each distinct input geometry is decoded from its stored WKB once per
  overlay batch and shared, read-only, across every tile it falls in. A
  feature that spans many tiles was previously decoded again in each of
  them; on a dense world protected-area union a single large feature can
  recur in thousands of tiles, which made WKB decoding the largest
  single cost of the overlay. The per-tile clipping, noding, and
  attribution are unchanged, so the result is identical.
- A piece is a face of the arrangement of all input boundaries, so it
  lies wholly inside or outside every input up to snap-rounding slivers
  along the boundary.
  [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  now credits each whole face to the inputs whose interior contains the
  face’s representative point, and the piece geometry is that face. This
  replaces intersecting every face with each partially covering input,
  the largest remaining cost once decoding is shared; per-input covered
  area stays within about the noding precision times the face perimeter
  of the exact value (well inside the 1e-4 coverage tolerance), and thin
  boundary slivers no longer appear as separate pieces. Pass
  `exact = TRUE` to restore the previous behaviour, where each face is
  intersected with every covering input and credited that exact area.
- Together these take the end-to-end ~470k-feature world protected-area
  union from about 15 minutes to about 5, with the coverage invariant
  still holding exactly (0 offenders), on a 32-thread desktop.

## vectra 0.9.8

CRAN release: 2026-07-01

### New features

- Embedding columns.
  [`as_embedding()`](https://gillescolling.com/vectra/reference/as_embedding.md)
  packs numeric vectors into a hex float32 blob held in an ordinary
  character column. The distance functions
  [`cosine()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
  (cosine distance),
  [`l2()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
  (Euclidean distance), and
  [`dot()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
  (inner product) decode the blob inside the engine and run inside
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) /
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  parallelized over rows. Pair them with
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  /
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  for nearest-neighbour search.
- Time-series resampling.
  [`resample()`](https://gillescolling.com/vectra/reference/resample.md)
  buckets a `Date` / `POSIXct` column to a calendar grid and aggregates
  within each bucket, the time-series form of
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md) +
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md).
  [`floor_time()`](https://gillescolling.com/vectra/reference/floor_time.md)
  exposes the bucket key on its own for use inside
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) /
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md).
- Time-based rolling aggregates inside
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md):
  [`roll_sum()`](https://gillescolling.com/vectra/reference/rolling.md),
  [`roll_mean()`](https://gillescolling.com/vectra/reference/rolling.md),
  [`roll_min()`](https://gillescolling.com/vectra/reference/rolling.md),
  [`roll_max()`](https://gillescolling.com/vectra/reference/rolling.md),
  and
  [`roll_n()`](https://gillescolling.com/vectra/reference/rolling.md)
  over a trailing datetime window `(time - every, time]`, respecting an
  upstream
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md).
- [`interval_join()`](https://gillescolling.com/vectra/reference/interval_join.md)
  joins two tables on range overlap: a row of `x` matches a row of `y`
  when their `[start, end]` intervals overlap. Supports an optional
  equality key, inner or left output, and open or closed interval ends.

### Bug fixes

- [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
  no longer triggers an UndefinedBehaviorSanitizer report when the
  written data frame has a zero-row double column (CRAN M1 sanitizer
  check). The bulk-copy fast path in `df_to_batch()` called `memcpy()`
  over the column unconditionally; for an empty column `REAL()` yields a
  degenerate pointer that clang’s alignment sanitizer flagged. The copy
  is now skipped when there are no rows.

## vectra 0.9.7

CRAN release: 2026-06-29

### Geometry functions in mutate(), filter(), and summarise()

- A family of `st_*` geometry functions now runs inside the expression
  verbs, on the GEOS C library straight off the hex-WKB geometry column,
  with no per-batch round-trip through `sf`.
  `tbl(f) |> filter(st_area(geometry) > 1e6)` prunes the stream in C,
  and `mutate(geometry = st_centroid(geometry))` adds a new hex-WKB
  geometry column. See
  [`?geom_expressions`](https://gillescolling.com/vectra/reference/geom_expressions.md).
- Measures (return a number):
  [`st_area()`](https://r-spatial.github.io/sf/reference/geos_measures.html),
  [`st_length()`](https://r-spatial.github.io/sf/reference/geos_measures.html)
  /
  [`st_perimeter()`](https://r-spatial.github.io/sf/reference/geos_measures.html),
  `st_x()`, `st_y()`, `st_npoints()`, `st_ngeometries()`, and the binary
  [`st_distance()`](https://r-spatial.github.io/sf/reference/geos_measures.html).
- Predicates (return TRUE/FALSE): unary
  [`st_is_valid()`](https://r-spatial.github.io/sf/reference/valid.html),
  [`st_is_empty()`](https://r-spatial.github.io/sf/reference/geos_query.html),
  [`st_is_simple()`](https://r-spatial.github.io/sf/reference/geos_query.html),
  and the binary topological relations
  [`st_intersects()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_within()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_contains()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_overlaps()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_touches()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_crosses()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_equals()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_disjoint()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_covers()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  [`st_covered_by()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html).
  The second geometry is another geometry column, a constant `sf`/`sfc`
  object (parsed once and reused across the stream), or a hex-WKB
  string.
- Transforms (return a geometry as hex-WKB):
  [`st_centroid()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
  [`st_point_on_surface()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
  [`st_boundary()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
  `st_envelope()`,
  [`st_convex_hull()`](https://r-spatial.github.io/sf/reference/geos_unary.html),
  [`st_make_valid()`](https://r-spatial.github.io/sf/reference/valid.html),
  the parameterized `st_buffer(g, dist)` and `st_simplify(g, tol)`, and
  the type name
  [`st_geometry_type()`](https://r-spatial.github.io/sf/reference/st_geometry_type.html).
- The per-row decode is parallelized with OpenMP. A missing or
  unparseable geometry yields `NA` for that row rather than an error.

### Documentation

- New vignettes covering the spatial surface added since 0.9.1:
  “Geometry functions in expressions” (the `st_*` functions inside
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)/[`filter()`](https://gillescolling.com/vectra/reference/filter.md)/
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)),
  “Coverage and topology” (the set-wise verbs:
  [`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md),
  [`spatial_line_merge()`](https://gillescolling.com/vectra/reference/spatial_line_merge.md),
  [`spatial_simplify()`](https://gillescolling.com/vectra/reference/spatial_simplify.md),
  [`spatial_eliminate()`](https://gillescolling.com/vectra/reference/spatial_eliminate.md),
  [`spatial_explode()`](https://gillescolling.com/vectra/reference/spatial_explode.md),
  [`spatial_topology()`](https://gillescolling.com/vectra/reference/spatial_topology.md),
  [`spatial_centerline()`](https://gillescolling.com/vectra/reference/spatial_centerline.md),
  [`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md),
  [`spatial_snap_grid()`](https://gillescolling.com/vectra/reference/spatial_snap_grid.md),
  [`spatial_locate()`](https://gillescolling.com/vectra/reference/spatial_locate.md)),
  and “Network analysis”
  ([`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md),
  [`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md),
  [`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)).

### Bug fixes

- Fixed installation failure on R-devel with clang 22 (CRAN’s
  `r-devel-linux-x86_64-fedora-clang`). Six source files included
  `<omp.h>` directly after R’s headers; clang 22’s `omp.h` begins with a
  `declare variant match(...)` clause that R’s `match` -\> `Rf_match`
  macro rewrote into invalid syntax. All OpenMP usage now routes through
  `vec_omp.h`, which forward-declares the runtime functions instead of
  including the wrapper.

## vectra 0.9.6

### Network analysis

- [`spatial_network()`](https://gillescolling.com/vectra/reference/spatial_network.md)
  builds a routable graph from a line layer: nodes at line endpoints
  (snapped within `tolerance`), edges weighted by geometry length or a
  `weight` column, optionally directed with one-way codes (`direction`,
  `weight_to`). The graph and the shortest-path solver are native C (a
  binary-heap Dijkstra over a CSR adjacency, no `igraph` dependency);
  the graph is held resident in a `vectra_network` object, the network
  counterpart of a resident `sf` `y`, while the query verbs stream.
- [`spatial_route()`](https://gillescolling.com/vectra/reference/spatial_route.md)
  streams a layer of origins past a resident network and returns the
  shortest path from each origin to one or more destinations `to`, one
  row per (origin, destination) with the cost and the route geometry.
  With `geometry = FALSE` it returns only the cost, so a destination set
  per origin is the origin-destination cost matrix in long form.
  Unreachable pairs return an infinite cost rather than dropping the
  row.
- [`spatial_service_area()`](https://gillescolling.com/vectra/reference/spatial_service_area.md)
  streams origins and, per origin, returns the part of the network
  reachable within a cost budget – the convex-hull service area
  (`output = "polygon"`), the reachable edges (`"lines"`), or the
  reachable nodes (`"nodes"`). A vector `cost` returns nested
  travel-cost isochrone bands, one row per (origin, band).
- The solver parallelises over a batch of origins with OpenMP; the graph
  is the resident budget while the query side scales by streaming.

## vectra 0.9.5

### Coverage cleanup

- [`spatial_eliminate()`](https://gillescolling.com/vectra/reference/spatial_eliminate.md)
  cleans a polygon coverage by absorbing every feature smaller than
  `max_area` into a neighbour (the QGIS “Eliminate”): each sliver joins
  the neighbour it shares the longest border with, or the largest-area
  neighbour with `into = "largest_area"`. An area-rooted union-find
  collapses chains of slivers so a connected run flows to its single
  largest member, whose attributes survive, and a sliver with no
  neighbour is kept unchanged. Rides the partition tier alongside
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md).

## vectra 0.9.4

### Centerline and planar topology

- [`spatial_centerline()`](https://gillescolling.com/vectra/reference/spatial_centerline.md)
  traces the centerline (medial axis) of each streamed polygon from the
  Voronoi diagram of its densified boundary: the Voronoi edges that fall
  inside the polygon are its skeleton, merged into maximal lines.
  `density` sets the boundary sampling and `prune` drops the short spurs
  the skeleton grows toward convex corners. The usual approximation for
  river or road centerlines from a filled shape; non-polygon geometry
  passes through unchanged.
- [`spatial_topology()`](https://gillescolling.com/vectra/reference/spatial_topology.md)
  decomposes a polygon coverage into the arcs of its planar topology:
  the unioned boundaries are noded so a shared border becomes one arc,
  tagged with the identifiers of the (up to two) polygons on either side
  – two for an internal shared edge, one for an outer edge. Rides the
  partition tier and is the inverse of
  [`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md).

## vectra 0.9.3

### Set-wise topology verbs and linear referencing

- [`spatial_polygonize()`](https://gillescolling.com/vectra/reference/spatial_polygonize.md)
  builds the polygonal faces enclosed by a line network (the QGIS
  “Polygonize”, the inverse of taking polygon boundaries): a group’s
  lines are unioned and noded, then the faces of that arrangement are
  returned, one per row. Like
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
  and
  [`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
  it rides the partition tier, with an optional `by` to polygonize
  within groups.
- [`spatial_line_merge()`](https://gillescolling.com/vectra/reference/spatial_line_merge.md)
  sews line segments that meet end to end into maximal linestrings (the
  line counterpart of a dissolve), one row per chain; segments meeting
  at a junction of degree greater than two stay separate.
- [`spatial_simplify()`](https://gillescolling.com/vectra/reference/spatial_simplify.md)
  simplifies a polygon coverage without tearing shared edges: boundaries
  are unioned so a shared border is one line, noded into arcs, each arc
  simplified once with its junction endpoints pinned, and
  re-polygonized, so adjacent polygons stay edge-matched with no
  slivers. This is the topology-preserving simplification a per-feature
  `spatial_map(~ sf::st_simplify())` cannot give, because that
  simplifies each polygon’s copy of a shared border independently. Each
  simplified face keeps its source polygon’s attributes.
- [`spatial_locate()`](https://gillescolling.com/vectra/reference/spatial_locate.md)
  locates streamed points along a resident line layer (linear
  referencing): each point gets its nearest line’s identifier, the
  measure (distance along that line), and the perpendicular offset, with
  an optional `snap` onto the line. The inverse (a measure back to a
  point) is
  [`sf::st_line_interpolate()`](https://r-spatial.github.io/sf/reference/st_line_project_point.html)
  through
  [`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md).
- The partition tier shared by
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md),
  [`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md),
  and the three new set-wise verbs is now a single internal
  `.partition_each` router rather than re-inlined in each verb.

## vectra 0.9.2

### Two-layer `spatial_overlay()`

- [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  gains a second layer `y`: instead of self-unioning one layer it nodes
  two layers into one planar partition and carries the attributes of the
  covering `x`-record and `y`-record onto each piece. A `how` argument
  selects which pieces to keep – `"intersection"` (covered by both),
  `"union"` (every piece of either, the absent side filled with `NA`),
  `"identity"` (all of `x` split by `y`), or `"symdiff"` (pieces in
  exactly one layer). `vars_y` selects the carried `y` columns, and a
  name shared with an `x` column is disambiguated with a `.x` / `.y`
  suffix. `y` accepts an `sf` object or a file path (`layer_y` /
  `query_y`) read in batches, and must share the CRS of `x`. With
  `y = NULL` (the default) the behaviour is unchanged. This reuses the
  existing noding, deduplication, component-tiling, and streaming
  machinery, so a two-layer overlay scales the same way the self-union
  does.

### `spatial_explode()`

- New
  [`spatial_explode()`](https://gillescolling.com/vectra/reference/spatial_explode.md)
  streams a query and splits every multipart geometry into its
  single-part components – a `MULTIPOLYGON` into one row per polygon, a
  `MULTILINESTRING` into linestrings, a `MULTIPOINT` into points, and a
  `GEOMETRYCOLLECTION` into its members (recursively) – copying the
  source attributes onto each part. Single-part and empty geometries
  pass through as one row. An optional `part` argument names a 1-based
  part-index column. It is the streaming counterpart of the QGIS
  “multipart to singleparts” tool, processing one batch at a time, and
  the inverse of
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md).

### `spatial_construct()`

- New
  [`spatial_construct()`](https://gillescolling.com/vectra/reference/spatial_construct.md)
  builds a set-wise geometry construction from a whole feature set – the
  constructions a per-feature
  [`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
  cannot express. A `kind` argument selects it: `"convex_hull"`,
  `"concave_hull"`, `"envelope"`, `"oriented_box"`,
  `"enclosing_circle"`, `"inscribed_circle"`, `"pole"` (the pole of
  inaccessibility, the centre of the maximum inscribed circle),
  `"voronoi"`, and `"delaunay"`. Like
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
  it rides the partition tier: a `by` argument routes the layer into one
  shard per group and builds one construction per group, with `NULL`
  constructing from the whole layer. The enclosing kinds emit one
  feature per group; the tessellations emit one polygon per cell, each
  carrying the group’s `by` values.

### `spatial_snap_grid()` and `spatial_snap()`

- New
  [`spatial_snap_grid()`](https://gillescolling.com/vectra/reference/spatial_snap_grid.md)
  rounds a streamed layer’s coordinates to a regular grid of a given
  spacing and repairs the result, one batch at a time. It is the
  fixed-precision snap-rounding
  [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  applies internally, exposed as a standalone verb, so a layer can be
  cleaned of slivers or pre-noded to a common precision without running
  a full overlay. The snap runs in C straight off the hex-WKB column,
  one cleaned geometry per input feature.
- New
  [`spatial_snap()`](https://gillescolling.com/vectra/reference/spatial_snap.md)
  snaps a streamed layer’s vertices and edges toward a resident
  reference layer when they lie within a tolerance (the QGIS “snap
  geometries to layer”), closing the small gaps and overshoots between
  two layers that should share a boundary. The reference layer stays
  resident while the left stream flows past one batch at a time.

### `spatial_knn()`

- New
  [`spatial_knn()`](https://gillescolling.com/vectra/reference/spatial_knn.md)
  finds, for each feature of a streamed layer, the `k` nearest features
  of a small resident layer, returning one row per (left, neighbour)
  pair with the neighbour’s rank, identifier, and distance. Where
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
  with `st_nearest_feature` attaches only the single nearest match, this
  returns the top `k` and the distances – the nearest-`k` query and the
  building block of a distance matrix. Distances follow
  [`sf::st_distance()`](https://r-spatial.github.io/sf/reference/geos_measures.html)
  (planar in CRS units, or great-circle metres when spherical geometry
  is on).

### `spatial_smooth()`

- New
  [`spatial_smooth()`](https://gillescolling.com/vectra/reference/spatial_smooth.md)
  rounds the corners of streamed lines and polygons by Chaikin
  corner-cutting, one batch at a time. Each iteration replaces every
  vertex with two points a quarter and three-quarters along its adjacent
  edges; open lines keep their endpoints, polygon rings are cut
  cyclically. The smoothing is computed directly on the coordinates (no
  GEOS call), so it is dependency-light. Densifying and sampling points
  along a line stay
  [`spatial_map()`](https://gillescolling.com/vectra/reference/spatial_map.md)
  recipes (`~ sf::st_segmentize(.x, dfMaxLength)`,
  `~ sf::st_line_sample(.x, n)`).

### `spatial_split()`

- New
  [`spatial_split()`](https://gillescolling.com/vectra/reference/spatial_split.md)
  cuts a streamed layer against a small resident `blade` layer (the QGIS
  “split with lines”), one batch at a time: a polygon is divided into
  the faces the blade carves out, a line into the arcs between
  crossings, and each piece is emitted as its own row with the source
  attributes copied. A feature the blade misses passes through as a
  single piece. With `extract = "points"` it instead returns the points
  where each feature meets the blade (the “line intersections” tool),
  dropping features that do not cross. The split is built from /GEOS
  noding and polygonization and expects planar coordinates.

## vectra 0.9.1

CRAN release: 2026-06-29

### `spatial_overlay()` noding and deduplication

- [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  now nodes each tile with fixed-precision snap-rounding
  (`GEOSUnaryUnionPrec`) at a grid derived from the layer extent,
  instead of floating-point noding. Floating noding throws on dense
  overlapping linework and falls back to a full snap-rounding retry of
  the whole component, which on large protected-area layers dominated
  the run. Fixed-precision noding is deterministic and single-pass, so
  the per-tile cost is flat and the overlap coverage invariant holds
  (`maxerr < 1e-4`) without the previous coverage warning. A new
  `precision` argument overrides the derived grid size.
- Byte-identical input geometries are now deduplicated before the
  overlay (`dedup = TRUE`, the default): each distinct geometry is
  overlaid once and its attributes fanned back to every duplicate
  source, so a layer with repeated sites does the topology work once. On
  a ~470k-feature world protected-area union this cut the distinct
  geometry count by about three quarters and the end-to-end run from
  roughly 50 to 17 minutes. Set `dedup = FALSE` to disable.

### Streaming GeoPackage output

- An \[sf::st_write()\] method for a `vectra_node` (also reached via
  [`sf::write_sf()`](https://r-spatial.github.io/sf/reference/st_write.html))
  writes a result to a vector file one batch at a time, appending each,
  so a multi-million-feature output is never held in memory as one `sf`
  object the way `collect_sf() |> st_write()` would. Resolving a dense
  overlay and writing the ~3M-piece GeoPackage this way keeps peak
  memory near the overlay’s own (a few GB) instead of spiking on the
  write.
- Grouped
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  /
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  (`n = 1`) now emits its winners in bounded row batches rather than one
  block, so a downstream streaming writer sees the result incrementally.

### Streaming grouped `slice_min()` / `slice_max()`

- Grouped
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  /
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  with `n = 1, with_ties = FALSE` now streams: it holds only the running
  winner per group, so peak memory scales with the number of groups (the
  result size), not the input length. The previous path ranked every
  input row through the window operator, which materialized all columns
  – including a large geometry string column – and could exhaust memory
  (`builder realloc failed (str data)`) when resolving a dense overlay
  whose geometry dwarfs RAM. The whole winning row, geometry and all
  attributes included, is still kept. Other grouped cases (`n > 1` or
  `with_ties = TRUE`) are unchanged.

### Lower-memory `spatial_overlay()`

- [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  now encodes and parses the input geometry a feature batch at a time
  rather than materializing the whole layer’s WKB at once. Connected
  components are derived from the bounding boxes after parsing, so the
  result is byte-identical; only the transient input copy is bounded.
  The batch size scales with available RAM (`read_chunk`, or
  `getOption("vectra.overlay_parse_chunk")`), and the default
  working-set budget is capped at half of total RAM when it can be
  detected, so a many-core machine cannot scale the overlay past what it
  can hold.
- [`spatial_overlay()`](https://gillescolling.com/vectra/reference/spatial_overlay.md)
  can read its input directly from a vector file (`x` a path, with
  `layer =` or `query =`) instead of a pre-loaded `sf` object, reading
  the layer in feature batches. The full layer is never held in memory,
  so peak usage tracks the cleaned geometry rather than the source size:
  a world protected-area layer that needs ~11 GB to load with
  [`sf::st_read()`](https://r-spatial.github.io/sf/reference/st_read.html)
  overlays in ~5 GB this way, bringing a larger-than-RAM layer within
  reach of a 16 GB machine.

### Raster and vector toolbox

- `polygonize(raster)` vectorises a raster into polygon features, the
  inverse of
  [`rasterize()`](https://gillescolling.com/vectra/reference/rasterize.md):
  cells are read one tile-row strip at a time and (by default) dissolved
  by value into one polygon per value through
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md).
- `contours(raster, levels)` traces iso-lines with marching squares over
  a haloed strip pass, then joins each level’s segments into continuous
  lines.
- `mask(raster, polygon)` clips a raster to an `sf` polygon layer one
  strip at a time, keeping the pixels whose centre falls inside (or,
  with `inverse = TRUE`, outside) it. It is the raster counterpart of
  [`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md).
- `rast_calc(rasters, expr)` evaluates a cellwise expression across
  aligned rasters (map algebra): band indices like
  `(nir - red) / (nir + red)`, reclassification, and arithmetic across
  layers, streamed strip by strip.
- `mosaic(rasters, fun)` merges rasters sharing a resolution and cell
  grid onto their union, resolving overlap with `first` / `last` /
  `mean` / `sum` / `min` / `max`, one output strip at a time.
- `proximity(raster, target)` computes the exact Euclidean distance from
  every cell to the nearest feature (non-NA, or matching `target`) in
  CRS units, via the separable Felzenszwalb-Huttenlocher distance
  transform: a row pass, an out-of-core transpose, a column pass, and a
  transpose back, each over tile-row strips so the whole grid is never
  resident. Squared distances scale by the x and y resolution, so the
  result is exact on anisotropic cells.

### Native libgeos compute paths

- [`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md),
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md),
  [`spatial_clip()`](https://gillescolling.com/vectra/reference/spatial_clip.md),
  and
  [`spatial_dissolve()`](https://gillescolling.com/vectra/reference/spatial_dissolve.md)
  now run their geometry operation natively on the GEOS C API (via
  `libgeos`) straight off the hex-WKB geometry column, with no per-batch
  round-trip through `sf`. The resident side – the locator layer, the
  join target, the clip mask – is parsed once into a GEOS spatial index
  and each streamed batch is tested, matched, or cut in C, parallel
  across rows.
  [`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md)
  and
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
  cover the topological predicates (intersects, within, contains,
  overlaps, covers, covered by, touches, crosses);
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
  returns the per-row match lists from C and attaches the resident
  attributes in R without decoding the left side.
- The native predicate set extends beyond the topological ones:
  `equals`, within-distance
  ([`sf::st_is_within_distance`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html),
  radius passed as `dist =`, found by querying the index with each
  feature’s envelope grown by the radius), and, for
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md),
  nearest feature
  ([`sf::st_nearest_feature`](https://r-spatial.github.io/sf/reference/st_nearest_feature.html),
  one resident match per row via the index’s nearest-neighbour
  traversal).
  [`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md)
  also runs `disjoint` natively (a row matches when it is disjoint from
  at least one resident feature). A disjoint *join* keeps the `sf` path,
  since its matches are the bounding-box complement a spatial index
  cannot prune.
- Coordinate-assembled (`coords`) point input runs natively too: each
  point is built in C from its x/y columns and matched against the
  index, instead of being assembled into an `sf` layer per batch. This
  covers
  [`spatial_filter()`](https://gillescolling.com/vectra/reference/spatial_filter.md)
  (every predicate but disjoint, which stays on `sf` as it does for the
  join) and
  [`spatial_join()`](https://gillescolling.com/vectra/reference/spatial_join.md)
  (topological, within-distance, and nearest, with the emitted point
  geometry also built in C).
- [`zonal()`](https://gillescolling.com/vectra/reference/zonal.md) with
  polygon zones now assigns each pixel centre to its polygon natively:
  the polygons are parsed once into the index and every tile-row strip’s
  centres are located in C, so `sf` is touched only to read the polygons
  in. Geographic polygons with spherical geometry on
  ([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html))
  keep the `sf` point-in-polygon path.
- The native paths run on projected or unprojected planar data, where
  they equal the previous `sf` result exactly. Geographic coordinates
  with spherical geometry on
  ([`sf::sf_use_s2()`](https://r-spatial.github.io/sf/reference/s2.html)),
  a disjoint join, and extra
  [`sf::st_union()`](https://r-spatial.github.io/sf/reference/geos_combine.html)
  /
  [`sf::st_join()`](https://r-spatial.github.io/sf/reference/st_join.html)
  arguments keep the `sf` path, so its semantics are unchanged.

### Documentation

- New
  [`vignette("spatial")`](https://gillescolling.com/vectra/articles/spatial.md)
  walks the out-of-core GIS toolbox as one workflow, with inline canvas
  animations for the raster-to-points bridge, select by location,
  rasterization, and the cost-model tiers.
- The quickstart vignette leads with animated views of the streaming
  memory envelope, what has to fit in RAM, and the lazy pull-based plan,
  and its on-disk-format description now matches the tdc codec.

### Two-sided streamed spatial join

- `spatial_join(x, y, partition = grid(cellsize))` joins two
  larger-than-RAM layers by binning both to a uniform spatial grid and
  joining one shard at a time, for the case where neither side fits in
  memory as a resident `sf` object. `y` becomes a streamed
  `vectra_node`; each left feature is assigned to the single grid cell
  of its reference point while each right feature is replicated to every
  cell its bounding box overlaps, so a left row is emitted exactly once
  and the result equals the resident join. This is exact for point left
  geometries (the dominant case – tagging a huge point set with the
  polygon it falls in). `grid(cellsize, origin)` defines the partition
  grid. The partition path serves the topological predicates
  (intersects, within, contains, …) and
  [`sf::st_nearest_feature`](https://r-spatial.github.io/sf/reference/st_nearest_feature.html),
  for which each left feature searches its own cell and the eight around
  it (the nearest is found when it lies within one cell of the left
  reference cell).

### Streamed warp (resample / reproject)

- `warp(raster, template, method)` resamples or reprojects a `.vec`
  raster onto a target grid, walking the *output* one tile-row strip at
  a time. Each strip’s target pixel centres are projected into the
  source CRS (via PROJ through `sf` only when the two CRSs differ),
  mapped through the source geotransform, and sampled from the bounded
  source window they fall in – so the whole output grid is never
  resident and the source is read in windows rather than held whole.
  `method` is `"near"`, `"bilinear"`, or `"cubic"` (Catmull-Rom),
  following the GDAL /
  [`terra::project()`](https://rspatial.github.io/terra/reference/project.html)
  convention; kernels that reach off the source extent or touch nodata
  return `NA`. `template` borrows a grid from another raster or is given
  as `list(crs =, extent =, res =, dims =)`. The C sampler keeps the
  interpolation native; projection stays in PROJ.

### Streamed focal and terrain

- `focal(raster, w, fun)` applies a moving window to a `.vec` raster,
  reading the input one tile-row strip at a time – each strip expanded
  by the kernel radius (a halo read) so window neighbours are available
  without ever holding the whole grid resident. When `path` is given the
  output is streamed straight back to a new `.vec` one tile-row at a
  time, so neither the input nor the output band is ever fully in
  memory: the raster op that runs out of core where an in-memory engine
  needs the whole raster at once. The window is a weight matrix (or a
  single odd integer); `fun` is one of `"sum"`, `"mean"`, `"min"`,
  `"max"`, `"sd"`, `"median"`, computed in C, with `na.rm` matching the
  resident behaviour at edges.
- `terrain(raster, v)` derives DEM products with Horn’s 3x3 method on
  the same haloed strip pass: `"slope"`, `"aspect"`, `"hillshade"`,
  `"TPI"`, `"roughness"`, `"TRI"`. The return follows the input – one
  matrix for a single `v`, a named list (or a multi-band `.vec`) for
  several – and matches
  [`terra::terrain()`](https://rspatial.github.io/terra/reference/terrain.html)
  /
  [`terra::shade()`](https://rspatial.github.io/terra/reference/shade.html).

### Streamed dissolve

- `spatial_dissolve(x, by, .fun)` unions the geometries within each `by`
  group into a single feature (the GIS “Dissolve” tool), optionally
  summarising attributes through a named list of functions. Dissolve
  needs every geometry of a group together, so it rides the partition
  tier: `x` is spilled once and routed into one shard per group in a
  single bounded pass, then each shard is unioned with `sf`. With no
  `by` the whole layer dissolves into one feature.

### Streamed zonal statistics

- `zonal(raster, zones, fun)` summarises a raster within zones one
  tile-row strip at a time, so the whole grid never has to be resident.
  Zones come from a second raster aligned to the value grid (the
  [`terra::zonal()`](https://rspatial.github.io/terra/reference/zonal.html)
  pattern) or from an `sf` polygon layer (each pixel assigned the
  polygon its centre falls in). The per-zone moments are folded in
  memory as strips arrive – peak memory is one strip plus the small
  per-zone table – and `fun` may name several of `"mean"`, `"sum"`,
  `"count"`, `"min"`, `"max"`, `"sd"` at once. Raster zones are
  `sf`-free; `sd` is derived from the streamed moments with no second
  pass.

### Streamed vector-to-raster

- `rasterize(x, template, field, fun)` folds a larger-than-RAM point
  stream into a fixed raster grid one batch at a time. The grid is held
  resident while the points flow past, so peak memory is the grid plus
  one batch – the streaming counterpart to
  [`terra::rasterize()`](https://rspatial.github.io/terra/reference/rasterize.html)
  on a point set that has to fit in RAM. The per-cell reduction
  (`"count"`, `"sum"`, `"mean"`, `"min"`, `"max"`) is accumulated in C.
  Points arrive either as two coordinate columns (the default, `sf`-free
  path) or from a hex-WKB point-geometry column. The result is an
  in-memory georeferenced matrix, or a `.vec` raster when `path` is
  given.

### Streamed select-by-location and clip/erase

- `spatial_filter(x, y, predicate)` keeps the rows of a streamed layer
  `x` whose geometry satisfies an `sf` binary predicate against a small
  resident layer `y` (select by location), filtering the billion-row
  stream one batch at a time while `y` stays in memory. Rows are
  filtered, never duplicated, and the output carries `x`’s schema
  unchanged; `negate = TRUE` keeps the non-matching rows (select by
  location, inverted).

- `spatial_clip(x, mask, erase)` cuts a streamed layer’s geometry
  against a small resident `mask`: the intersection by default (the GIS
  “Clip” tool), or the difference with `erase = TRUE` (the “Erase”
  tool). The mask is dissolved once and held resident while the stream
  flows past one batch at a time.

- The run-file spill machinery shared by the streamed spatial verbs
  (`spatial_map`/`join`/`filter`/`clip`/`overlay`) is now a single
  internal accumulator, so all of them flush, finalize, and clean up
  identically.

## vectra 0.8.2

### Bug fixes

- [`ifelse()`](https://rdrr.io/r/base/ifelse.html) (and `if_else()`) now
  returns the correct type when its two branches differ. Previously
  `ifelse(int64_col, x, y)` with a `double` or `NA` other branch
  labelled the result column int64 while the evaluator produced doubles,
  so the kept int64 values came back as ~4.6e18 garbage (and triggered a
  spurious “int64 value exceeds 2^53” warning). The result column now
  adopts the common type of the two branches, matching the evaluator. In
  particular `ifelse(year > 0, year, NA)` is a clean way to blank out
  sentinel years.

## vectra 0.8.1

### Polygon self-overlay

- `spatial_overlay(x)` splits a polygon `sf` layer along all its own
  overlaps into disjoint pieces (the “Union (single layer)” overlay),
  returning a lazy node with one row per piece per covering polygon.
  Resolve the duplicates with a grouped
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)/[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  – e.g. earliest designation year wins,
  `group_by(piece_id) |> slice_min(year)`. The overlay runs in C on the
  GEOS C API (via `libgeos`). Each feature is parsed once, in parallel –
  repaired and snapped to a fixed-precision grid – then features are
  grouped into connected components from their bounding boxes. Each
  component is one overlay job whose boundary linework is noded once and
  polygonised into faces (a single noding pass, so cost tracks the
  number of pieces, not how deeply polygons overlap); the few components
  too large for the memory budget are tiled over their own extent and
  clipped, so no single noding pass is ever large. Jobs run one per
  OpenMP thread (`threads`) and stream to a `.vtr` in batches sized to a
  `mem_limit` budget, so peak memory stays bounded regardless of layer
  size. The snapping grid is derived from the data’s coordinate
  magnitude and checked against a coverage invariant (the piece areas
  covering an input sum to its area), so pieces come out disjoint and
  their areas reconstruct the union. Scales to layers a single
  [`sf::st_intersection()`](https://r-spatial.github.io/sf/reference/geos_binary_ops.html)
  cannot hold at once (a 470k marine-protection layer overlays in
  bounded memory where the in-memory call exhausts RAM).

## vectra 0.8.0

### Group-aware slicing

- [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  and
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  now respect
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md):
  they keep the n smallest/largest rows *within each group* and return
  the whole winning row (every column, including geometry carried as a
  string), rather than a global top-n. `with_ties = FALSE` returns
  exactly n per group via a deterministic ordered `row_number()`;
  `with_ties = TRUE` keeps rows tied at the nth value. Previously a
  grouped
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)/[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  silently ignored the grouping and returned a single global result.
- `row_number()` accepts an order column: `row_number(col)` and
  `row_number(desc(col))` assign a deterministic 1..n within each
  partition, ordered by the column (the unordered `row_number()` is
  unchanged). `rank(desc(col))` is also supported.

### Streamed spatial operations

- `spatial_map(x, fn)` streams a lazy query through an `sf` transform
  (buffer, centroid, CRS transform, simplify, …) one batch at a time and
  returns a new lazy node, so a per-feature geometry operation runs on a
  table larger than RAM at one-batch peak memory.
- `spatial_join(x, y, join)` joins a streamed left side `x` against a
  small resident `sf` object `y` with an `sf` binary predicate
  (`st_intersects` by default): the spatial analogue of a hash join with
  the small side resident. The dominant use is tagging a huge point set
  with the polygon it falls in. Both-sides-huge joins compose with
  `offload(by = ...)`: partition on a spatial grid key, join each shard,
  recombine.
- `collect_sf(x)` materializes a spatial query as an `sf` object.
- Geometry rides through the engine as hex-encoded WKB in an ordinary
  string column (no new column type), losslessly round-tripped; the CRS
  is carried on the node. Topology stays with `sf`/GEOS — `sf` is an
  optional dependency (Suggests).

## vectra 0.7.1

CRAN release: 2026-06-11

- Cap the OpenMP team to two threads under `R CMD check`. When CRAN’s
  `_R_CHECK_LIMIT_CORES_` is set, the package now lowers its default
  team size to two so the parallel string, fuzzy-join, sort, and window
  kernels stay within the check farm’s two-core limit. The fuzzy-join
  match phase also clamps its requested thread count to the available
  maximum, matching the blocked fuzzy-lookup path. Outside a check the
  package still uses every available core.

## vectra 0.7.0

### Streaming consumption

- `collect_chunked(x, f, .init)` folds a function over a query one batch
  at a time. The engine pulls a single batch into R, applies
  `f(acc, chunk)`, frees the batch, and moves on, so a result larger
  than RAM can be reduced to a small summary (a running count, per-group
  sufficient statistics, the cross-products behind a linear fit) in one
  bounded-memory pass.
- `chunk_feeder(.source)` turns a query into a resettable generator
  following the `data(reset)` protocol that
  [`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html)
  expects, so a generalized linear model can be fitted out-of-core: each
  iteratively reweighted pass streams through the engine without ever
  holding the full design matrix. `.source` is a factory returning a
  fresh node, replayed on every reset.
- New C pull interface (`C_node_optimize`, `C_node_next_batch`) backs
  both verbs; per-batch conversion reuses the existing column converter,
  so the chunked and materializing paths share one code path.

### Offloading and out-of-core fits

- [`offload()`](https://gillescolling.com/vectra/reference/offload.md)
  is one verb with two return shapes. `offload(x)` materializes a query
  once to a `.vtr` and returns a node that streams from that file: it
  holds the same rows as `x` (an identity on values) and changes only
  the cost profile, since replaying it is a disk scan instead of a
  re-run of the upstream pipeline.
  [`chunk_feeder()`](https://gillescolling.com/vectra/reference/chunk_feeder.md)
  accepts an offloaded node directly, so an iterative consumer such as
  [`biglm::bigglm()`](https://rdrr.io/pkg/biglm/man/bigglm.html) reads
  the prepared columns from disk on every reweighted pass rather than
  rebuilding them each time.
- `offload(x, by = ...)` splits a query into disjoint shards in a single
  streaming pass, one per key value (`method = "level"`), per value
  range (`"range"`), or per hash bucket (`"hash"`); `"auto"` picks level
  for a discrete key and range for a numeric one. The result is
  list-like: [`length()`](https://rdrr.io/r/base/length.html),
  [`names()`](https://rdrr.io/r/base/names.html) (the keys),
  `p[["key"]]`, and `lapply(p, ...)` all work, turning a model that
  couples within a group into independent per-shard fits. The union of
  the shards reproduces the input; row totals are checked.
- [`group_map()`](https://gillescolling.com/vectra/reference/group_map.md)
  and
  [`group_modify()`](https://gillescolling.com/vectra/reference/group_map.md)
  run a function on each shard of a partition.
  [`group_map()`](https://gillescolling.com/vectra/reference/group_map.md)
  reads each shard into a data.frame, hands it to the function with its
  key, and returns the results keyed by shard (one fit per group).
  [`group_modify()`](https://gillescolling.com/vectra/reference/group_map.md)
  binds per-shard data.frames into one table and restores the key as a
  column. A purrr-style `~` formula works for either.
- [`collect_chunked()`](https://gillescolling.com/vectra/reference/collect_chunked.md)
  is now a generic and gains a `combine` argument: supplying it declares
  the reduction a monoid (with `.init` as identity), which lets the fold
  run over the shards of a partition and merge the partial results. A
  `commutative` flag declares the merge order-free.
- Offloaded streams carry a cost grade (passes over the data, peak
  memory, I/O class), shown by
  [`print()`](https://rdrr.io/r/base/print.html) and
  [`explain()`](https://gillescolling.com/vectra/reference/explain.md) –
  the label a plan reads to choose between a one-pass fold, an external
  sort
  ([`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)),
  and a partition.

## vectra 0.6.3

### Fixes

- [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
  /
  [`summarize()`](https://gillescolling.com/vectra/reference/summarise.md)
  now accept namespace-qualified aggregation calls (`vectra::n()`,
  `vectra::sum(x)`, `vectra:::mean(x)`). Previously `parse_agg_expr` ran
  [`as.character()`](https://rdrr.io/r/base/character.html) on the call
  head and dispatched on its result; for a `pkg::fn` call that yielded
  the length-3 vector `c("::", "pkg", "fn")`, and the subsequent
  `if (!fn %in% valid_aggs)` triggered “the condition has length \> 1”
  under R \>= 4.2. The parser now unwraps `::` / `:::` and uses the bare
  function name.

## vectra 0.6.2

CRAN release: 2026-05-08

### CRAN archive-issue fixes

Resolves the three findings the auto-check email surfaced for the
2026-05-06 archived 0.5.1 release.

- DESCRIPTION: replaced “gridded” (flagged as a possibly-misspelled word
  in the CRAN incoming pretest) with “raster”.
- gcc-ASAN heap-buffer-overflow in the LZ decode path
  (`tdc/src/api/decode_impl.c`, surfaced through `read_rg_tdc_with_fp`
  in `vtr1_tdc.c`): the consolidated decode pipeline now always
  allocates scratch buffers with a +16-byte wildcopy slack, so
  `tdc_match_copy`’s SIMD overshoot stays within the allocation. The
  `decode_ex.c` variant that was missing this slack on 0.5.1 is gone
  (folded into the shared `driver_decode_block_impl`). The
  ASAN-under-vignettes regression check is now part of the GitHub
  Actions sanitizer workflow so a future drift would be caught locally
  instead of at CRAN’s BDR memcheck.
- rchk PROTECT findings in `src/r_bridge.c`, `src/r_bridge_io.c`,
  `src/vtr1_tdc.c`, and `src/collect.c`: every `Rf_getAttrib` /
  `Rf_mkString` result that crossed an allocating call (`R_alloc`,
  `Rf_warning`, `Rf_setAttrib`, `Rf_asReal`, `Rf_asInteger`, `parse_*`)
  is now `PROTECT`ed and balanced with a matching `UNPROTECT`. Touches
  `apply_annotation`, `C_write_vtr`, `C_write_vtr_tdc`,
  `parse_quantize`, and `parse_spatial`.

## vectra 0.6.1

### Fixes

- `src/vec_omp.h` and call sites: stop including `<omp.h>` and
  forward-declare the three OpenMP runtime functions vectra calls
  (`omp_get_max_threads`, `omp_get_thread_num`, `omp_in_parallel`).
  clang 21’s bundled omp.h wrapper contains an unbalanced
  `#pragma omp end declare variant` that breaks compilation of `block.c`
  (and any other vectra TU that includes the wrapper) under
  r-devel-linux-x86_64-debian-clang. The bug is in the wrapper itself,
  so an `#ifdef _OPENMP` guard around `#include <omp.h>` is not enough —
  when `-fopenmp` is on the compile line, `_OPENMP` is defined and the
  broken wrapper is pulled in. Skipping the wrapper avoids the bug; the
  `#pragma omp ...` directives elsewhere in `src/` are still recognised
  and the runtime symbols resolve at link time via `libomp`. Fixes the
  compilation error that caused vectra 0.5.1 to be archived from CRAN.

## vectra 0.6.0

### Raster format (`.vec`)

A new tiled raster format and accompanying API for larger-than-RAM
gridded data. Each tile is encoded as a self-describing tdc block
(PRED_2D + BYTE_SHUFFLE + LZ); decoding is parallel across tiles.

- `vec_write_raster(x, path, ...)`: write a numeric matrix or 3D
  `(rows, cols, bands)` array to `.vec`. Storage dtypes: `f64`, `f32`,
  `i8`/`u8`, `i16`/`u16`, `i32`/`u32`, `i64`/`u64`. `compression`
  controls per-tile codec probing — `"fast"`, `"balanced"`, or `"max"`
  (six-spec probe per tile). Decode cost is unchanged across levels
  because each tile records its own codec spec.
- `vec_open_raster(path)` / `vec_close_raster(r)`: lazy open returning a
  metadata + handle list (`vectra_raster`). The handle is auto-finalized
  on garbage collection.
- `vec_read_window(r, band, level, cols, rows)`: decode a window of a
  chosen band, with overview-level support. Pixels outside the raster
  come back as `NA`. Tile decode is parallelized across worker threads
  (Phase 5a).
- `vec_extract_points(r, x, y)`: sample band values at `(x, y)` points.
- `vec_build_overviews(path, levels, resampling)`: append `n_levels - 1`
  reduced-resolution copies in place. Resampling kernels: `"nearest"`,
  `"average"`, `"bilinear"`, `"mode"`, `"gauss"`. The file’s `n_levels`
  is updated atomically.
- `vec_to_tiff(path, output, compression)`: export `.vec` level-0 pixels
  to GeoTIFF. Compression is `"none"`, `"deflate"`, or `"lzw"`; LZW also
  applies horizontal differencing (Predictor 2) for integer pixel types,
  matching the layout libtiff/GDAL produce by default. Inherits dtype,
  geotransform, EPSG, and nodata from the source.

### Time cubes

- `vec_write_time_cube(x, times, path, layout, ...)`: write a 4D
  `(rows, cols, bands, time)` array. Two layouts:
  - `"image"` (default): one tile per `(band, time, ty, tx)` — optimal
    for “give me one full image at time T” reads.
  - `"pixel"`: one tile per `(band, ty, tx)` holding the full time stack
    as `[tw*th, n_time]` — optimal for “give me the time series at pixel
    `(x, y)`” reads.
- `vec_read_pixel_series(r, x, y, band)`: full time series at a single
  pixel as a numeric vector. On pixel-major files this is one tile
  decode; on image-major files the reader scans the index for distinct
  time stamps and decodes one tile per stamp.
- `vec_read_time_slice(r, time, band, level, cols, rows)`: read a single
  time slice as a matrix.
- `vec_raster_times(r, band, level)`: distinct time stamps, in ascending
  order.
- `vec_raster_layout(r)`: query whether an open raster is `"image"` or
  `"pixel"` layout.
- `print.vectra_raster()`: prints dimensions, dtype, geotransform, EPSG,
  nodata, and band names.

### GeoTIFF reader and writer

- Reader: tiled and Cloud-Optimized GeoTIFF (COG) inputs go through the
  same block abstraction as strip TIFFs (strips collapse to
  `n_blocks_x = 1`). Edge-block padding is handled in
  `block_stored_rows()`.
- [`tiff_band_names()`](https://gillescolling.com/vectra/reference/tiff_band_names.md):
  parse `<Item role="description">` entries from `GDAL_METADATA` (tag
  42112). Pure-R scanner, no `xml2` dependency.
- `tiff_crs(path)`: read the EPSG code, geographic-vs-projected flag,
  and citation string from the GeoKey directory (tags 34735/34737).
- [`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md)
  gains `tiled`, `tile_size`, `bigtiff`, and `crs` arguments.
  - `tiled = TRUE` emits TIFF tags 322/323/324/325 in place of strip
    tags. `tile_size` accepts a single integer (square) or a length-2
    `c(w, h)`; both dimensions must be positive multiples of 16.
    Default 256. Tiled output is the layout required for Cloud-Optimized
    GeoTIFF.
  - `bigtiff = "auto"` (default) auto-promotes to BigTIFF (magic
    `0x002B`, 64-bit offsets) when the expected raw payload exceeds the
    classic-TIFF 4 GB ceiling; `TRUE` forces BigTIFF; `FALSE` forces
    classic TIFF. Tiled BigTIFF is not yet supported.
  - `crs` accepts an integer EPSG code, an `"EPSG:xxxx"` string, or a
    list with `$epsg`, `$geographic`, and optional `$citation`. Outputs
    round-trip through
    [`terra::rast()`](https://rspatial.github.io/terra/reference/rast.html)
    for 4326, 3857, and 31287.

### Fixes

- [`collect()`](https://gillescolling.com/vectra/reference/collect.md) /
  `block_array_gather`: empty-string slots now shortcut to
  `R_BlankString`. Previously the gather paths called
  `Rf_mkCharLenCE(NULL, 0, ...)` and the dedup cache called
  `memcmp(NULL, ...)` when a batch happened to contain only empty/`NA`
  strings, tripping UBSAN’s nonnull check even though the length was
  zero.

### Internal

- C-side `*_push` helpers (`vec_buf_push`, `vec_array_push`, …)
  consolidated into a single `vec_grow_to` growth primitive.

## vectra 0.5.1

CRAN release: 2026-04-21

### CRAN resubmission fixes (0.5.0 incoming pretest feedback)

- `configure` / `configure.win`: rewritten as POSIX `/bin/sh`
  (previously `#!/usr/bin/env bash` with `set -o pipefail` and
  `[[ ... ]]`). Bash is not guaranteed on all CRAN build hosts.
- `src/window.c`: the OpenMP task-parallel merge sort helper was defined
  unconditionally but called only from `#ifdef _OPENMP` branches,
  producing a clang `-Wunneeded-internal-declaration` warning under
  Debian’s no-OpenMP build. The definition now shares the guard.
- Vendored `tdc`: all `fprintf(stderr, ...)` debug/timing prints are
  routed through a `TDC_LOG(...)` macro that is a no-op unless
  `TDC_ENABLE_STDERR_LOG` is defined at build time, so the released
  `.so` contains no `stderr` / `fprintf` symbols. Addresses the WRE
  §1.6.4 policy forbidding compiled code from writing to stdout/stderr.

### Fixes

- [`collect()`](https://gillescolling.com/vectra/reference/collect.md):
  fix use-after-free in the cross-batch CHARSXP dedup cache. Each slot
  stored a raw pointer into the decoder’s heap buffer, which is freed
  when the batch is consumed; the next batch’s hash-collision `memcmp`
  then dereferenced freed memory. Manifested as segfaults on the second
  consecutive
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  of a large multi-rowgroup string-heavy `.vtr` (register, backbones),
  more likely under the parallel reader where batches accumulate before
  the serial consumer loop. Now verifies cache hits against
  `CHAR(sexp)`, which points into the still-alive interned CHARSXP body.

## vectra 0.5.0

### Compression backend rewire

- Replaced the bespoke v4 codec with `tdc`, a standalone
  typed-dimensional compression library vendored into `src/tdc/`. Encode
  and decode go through a self-describing block record (model +
  transform chain + entropy) rather than per-column tag constants.
  Deleted `vtr_codec.c`, `vtr_encodings.c`, `vtr_compress.c`, `vtr1.c`,
  and `vtr_codec_internal.h`.
- The `.vtr` on-disk format is a deliberate breaking change: pre-0.5
  files are not readable.
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
  and
  [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  write the new container;
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) reads
  only the new container.
- Per-row-group column statistics (min/max) are carried in the container
  index so the scan layer can still prune unreachable row groups.
- Parallel row-group reads are preserved.
- Custom vendoring via `tools/vendor_tdc.sh` and `configure` /
  `configure.win` pull the latest upstream `tdc` on every install when
  the source checkout is present; the pre-vendored copy is used
  otherwise.

### Known regression

- The v4 dict-defer CHARSXP fast path is gone — duplicate strings now
  hit R’s CHARSXP hash per row. Will be re-implemented on top of `tdc`’s
  dictionary-encoded varlen output when it becomes a hot spot.

### Fixes

- `man/write_vtr.Rd`: replaced a literal percent sign in the `compress`
  argument description that produced malformed Rd output on build.
- Windows:
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
  [`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
  and
  [`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
  now use `MoveFileEx` with a short retry loop for the final
  temp-to-target swap. Previously, a preceding
  [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) read
  could leave the target file mmap’d pending GC, and the swap would fail
  with a sharing violation.

## vectra 0.4.1

### Star schema and lookup

- New
  [`vtr_schema()`](https://gillescolling.com/vectra/reference/vtr_schema.md),
  [`link()`](https://gillescolling.com/vectra/reference/link.md), and
  [`lookup()`](https://gillescolling.com/vectra/reference/lookup.md)
  functions for star-schema workflows. Register a fact table with named
  dimension links once, then pull columns from any dimension without
  writing explicit joins. Only referenced dimensions are scanned.
- [`lookup()`](https://gillescolling.com/vectra/reference/lookup.md)
  reports unmatched keys per dimension by default, catching referential
  integrity issues before they propagate NAs silently.
- Supports both `"left"` (default) and `"inner"` join modes, named keys
  for differing column names, and reusable schema objects across
  multiple queries.

## vectra 0.3.2

- Fix misaligned `int64_t` memory access in `vtr_codec.c` (UBSAN).
  Dictionary encoding wrote and read 8-byte offsets through an unaligned
  pointer; delta decoding had the same issue. All fixed with `memcpy`.

## vectra 0.3.1

- CRAN submission fixes: title case, quoted technical terms in
  DESCRIPTION, corrected documentation URLs.

## vectra 0.3.0

### File operations

- `append_vtr(df, path)`: append a data.frame as a new row group to an
  existing `.vtr` file. Existing row groups are never rewritten.
- `delete_vtr(path, row_ids)`: logically delete rows by 0-based physical
  index. Writes a tombstone side file (`<path>.del`); the `.vtr` file is
  never modified. Deletions are cumulative and excluded automatically on
  the next [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md)
  call.
- `diff_vtr(old_path, new_path, key_col)`: key-based logical diff
  between two `.vtr` files. Returns a list with `added` (a lazy
  `vectra_node`) and `deleted` (a vector of key values). Implemented as
  a single-pass C streaming engine with O(n_unique_keys) memory.

### Expressions

- [`tolower()`](https://rdrr.io/r/base/chartr.html),
  [`toupper()`](https://rdrr.io/r/base/chartr.html),
  [`trimws()`](https://rdrr.io/r/base/trimws.html): case conversion and
  whitespace trimming for string columns in
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md).
- `levenshtein(x, y)` / `levenshtein_norm(x, y)`: Levenshtein edit
  distance and normalised variant (0–1). Supports column-vs-column and
  column-vs-literal comparisons. Optional `max_dist` argument for early
  termination.
- `dl_dist(x, y)` / `dl_dist_norm(x, y)`: Damerau-Levenshtein distance
  (counts transpositions as cost 1) and normalised variant.
- `jaro_winkler(x, y)`: Jaro-Winkler similarity (0–1, higher = more
  similar). All string-similarity functions propagate `NA` and work in
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md).
- `resolve(fk, pk, value)`: scalar self-join — looks up `value` where
  `pk == fk` within the same batch. Useful for denormalising
  parent-child tables without a join.
- `propagate(parent_id, id, seed)`: tree-traversal aggregation —
  propagates non-NA `seed` values down a parent-child hierarchy until
  all reachable nodes are filled. Converges in O(depth) passes.

### Format

- `.vtr` format version 4 with a two-layer codec (no external
  dependencies):
  - Encoding: `PLAIN` (default), `DICTIONARY` (string columns with \<
    50% unique values), `DELTA` (monotonically increasing `int64`
    columns).
  - Compression: custom LZ77 byte compressor (`LZ_VTR`, ~120 lines of
    C). Applied after encoding; skipped for buffers \< 64 bytes or when
    compression does not reduce size. Files written with v4 are
    typically 30–60% smaller than v3.
    [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) reads
    v1–v4 files;
    [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
    always writes v4.

## vectra 0.2.2

### Query optimizer

- Column pruning: scan nodes only read columns needed by the query plan.
- Predicate pushdown: filter predicates are attached to scan nodes and
  use `.vtr` v3 per-rowgroup min/max statistics to skip entire row
  groups.

### Engine

- `.vtr` format version 3 with per-column per-rowgroup statistics
  (min/max).
- O(n log n) [`rank()`](https://rdrr.io/r/base/rank.html) and
  `dense_rank()` (replaces O(n²) comparison-based).
- Nested expressions in
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md):
  `summarise(m = mean(x + y))` auto-inserts a hidden mutate.

### Expressions

- `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`:
  date/time component extraction for Date and POSIXct columns.
- [`as.Date()`](https://rdrr.io/r/base/as.Date.html) and
  [`as.POSIXct()`](https://rdrr.io/r/base/as.POSIXlt.html) literals in
  filter expressions (e.g. `filter(date > as.Date("2020-01-01"))`).
- `as.Date(string_col)`: convert ISO-format date strings to Date values.
- [`nchar()`](https://rdrr.io/r/base/nchar.html): returns string length
  as integer.
- `substr(x, start, stop)`: substring extraction (1-based, like R).
- `grepl(pattern, x)`: fixed string matching (no regex).
- `paste0(a, b)`: two-argument string concatenation.
- `gsub(pattern, replacement, x)` /
  [`sub()`](https://rdrr.io/r/base/grep.html): fixed-string replacement.
- [`startsWith()`](https://rdrr.io/r/base/startsWith.html) /
  [`endsWith()`](https://rdrr.io/r/base/startsWith.html): string
  prefix/suffix matching.
- [`pmin()`](https://rdrr.io/r/base/Extremes.html) /
  [`pmax()`](https://rdrr.io/r/base/Extremes.html): element-wise
  minimum/maximum.
- [`log2()`](https://rdrr.io/r/base/Log.html),
  [`log10()`](https://rdrr.io/r/base/Log.html),
  [`sign()`](https://rdrr.io/r/base/sign.html),
  [`trunc()`](https://rdrr.io/r/base/Round.html): additional math
  functions.

### Aggregation

- [`sd()`](https://rdrr.io/r/stats/sd.html) and
  [`var()`](https://rdrr.io/r/stats/cor.html): sample standard deviation
  and variance via Welford’s online algorithm. Returns NA for groups
  with fewer than 2 values (R semantics).
- `first()` and `last()`: first and last non-NA value per group. Both
  support `na.rm = TRUE`.

### Verbs

- [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
  and
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
  gain a working `with_ties` parameter (default `TRUE`). Ties at the
  boundary are now included by default; use `with_ties = FALSE` for
  exactly `n` rows.
- [`count()`](https://gillescolling.com/vectra/reference/count.md) and
  [`tally()`](https://gillescolling.com/vectra/reference/count.md) gain
  a working `sort` parameter. `sort = TRUE` returns results in
  descending order of the count column.
- [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
  and
  [`reframe()`](https://gillescolling.com/vectra/reference/reframe.md)
  now support
  [`across()`](https://gillescolling.com/vectra/reference/across.md).
- `distinct(.keep_all = TRUE)` with a column subset now emits a message
  when falling back to R.

### Utilities

- [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md):
  preview column names, types, and first few values without collecting
  the full result.
- [`collect()`](https://gillescolling.com/vectra/reference/collect.md)
  now works on data.frames (no-op), so `slice_min(...) |> collect()`
  works regardless of the `with_ties` path.

### Documentation

- New quickstart vignette:
  [`vignette("quickstart")`](https://gillescolling.com/vectra/articles/quickstart.md).
- `@details` sections added to
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
  [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
  [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md),
  [`count()`](https://gillescolling.com/vectra/reference/count.md), and
  join functions.

## vectra 0.2.1

### Engine

- External merge sort with 1 GB memory budget and automatic
  spill-to-disk.
- Sort-based `group_by() |> summarise()` path for spill-safe
  aggregation.
- Chunked FULL join finalize (65,536 rows per batch).
- Automatic type coercion (`int64 <-> double`) in join keys and
  [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md).
- [`rank()`](https://rdrr.io/r/base/rank.html) and `dense_rank()` window
  functions.

### Type system

- `.vtr` format version 2 with per-column annotations.
- Date, POSIXct, and factor columns roundtrip through
  [`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
  /
  [`collect()`](https://gillescolling.com/vectra/reference/collect.md).
- `where()` predicates work in
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md),
  [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
  and
  [`across()`](https://gillescolling.com/vectra/reference/across.md).

### Infrastructure

- Engine reference vignette
  ([`vignette("engine")`](https://gillescolling.com/vectra/articles/engine.md)).
- 17-scenario benchmark suite with baseline snapshots and regression
  thresholds.
- ASAN/UBSAN CI job on Linux.
- Benchmark smoke job on PRs.

## vectra 0.1.0

- Initial release.
- Custom columnar on-disk format (`.vtr`) with multi-row-group support.
- dplyr-compatible verbs:
  [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
  [`select()`](https://gillescolling.com/vectra/reference/select.md),
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
  [`transmute()`](https://gillescolling.com/vectra/reference/transmute.md),
  [`rename()`](https://gillescolling.com/vectra/reference/rename.md),
  [`relocate()`](https://gillescolling.com/vectra/reference/relocate.md),
  [`group_by()`](https://gillescolling.com/vectra/reference/group_by.md),
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
  [`count()`](https://gillescolling.com/vectra/reference/count.md),
  [`tally()`](https://gillescolling.com/vectra/reference/count.md),
  [`distinct()`](https://gillescolling.com/vectra/reference/distinct.md),
  [`reframe()`](https://gillescolling.com/vectra/reference/reframe.md),
  [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md),
  [`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md),
  [`pull()`](https://gillescolling.com/vectra/reference/pull.md).
- Hash joins:
  [`left_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`right_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`full_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md),
  [`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md).
- [`bind_rows()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  and
  [`bind_cols()`](https://gillescolling.com/vectra/reference/bind_rows.md)
  for combining queries.
- Window functions: `row_number()`,
  [`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
  [`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
  [`cummin()`](https://rdrr.io/r/base/cumsum.html),
  [`cummax()`](https://rdrr.io/r/base/cumsum.html).
- [`across()`](https://gillescolling.com/vectra/reference/across.md)
  support in
  [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
  [`summarise()`](https://gillescolling.com/vectra/reference/summarise.md).
- [`explain()`](https://gillescolling.com/vectra/reference/explain.md)
  for inspecting the execution plan.
- `tidyselect` integration for column selection helpers.
- Data sources: `.vtr`, CSV, SQLite, GeoTIFF.
- Data sinks:
  [`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md),
  [`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md),
  [`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md).
