# Getting Started with vectra

## Introduction

vectra is a columnar query engine for R. It stores data in a binary
columnar format (`.vtr`), reads it lazily, and processes it batch by
batch through a C11 execution engine. The API mirrors dplyr: chain
[`filter()`](https://gillescolling.com/vectra/reference/filter.md),
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md),
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
and the rest, and nothing runs until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).
That call pulls row groups through a tree of plan nodes, each consuming
and producing fixed-size batches, so memory use stays roughly constant
as the file grows.

The execution model is pull-based. When
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
fires, the root node calls `next_batch()` on its child, down to the scan
node that reads from disk; each node transforms one batch and passes it
upstream. Filtering uses selection vectors, so a filter over a
billion-row file touches only the matching indices. Sorting spills to
temporary files and merges with a k-way heap. Aggregation uses hash
tables sized to the number of groups. The engine carries four column
types (64-bit integers, doubles, booleans, variable-length strings),
each with a validity bitmap for NA.

Compression and encoding run through the vendored tdc codec at write
time, per column per row group. Per-row-group statistics (min, max, null
count) are attached to the container index, so the scan node skips row
groups a predicate excludes before decoding them. Optional `.vtri` hash
indexes accelerate equality predicates to O(1) row-group lookup. The
whole C codebase compiles from C11 with no external library link, so a
20 GB `.vtr` processes on a laptop because the scan reads one row group
at a time and the pipeline holds at most two batches at once.

Three pictures before the code: what streaming buys, what has to fit in
RAM, and how a lazy plan pulls one batch through.

The in-memory route holds the whole working set, so its footprint climbs
with the file and eventually meets the RAM limit. The streamed route
holds one batch, so the line stays flat. What has to fit at once is the
difference.

An in-memory pipeline keeps the source, a working copy, and the R
session live together. A streamed pipeline keeps one batch and the R
session. The plan that decides which one you get is lazy until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).

The verbs below build that plan. Each section introduces a feature area
with runnable examples, then points to the deep-dive vignette for full
coverage. All code uses tempfiles and built-in datasets, so every chunk
runs on any machine with vectra installed.

## Writing and reading data

vectra uses a binary columnar format with the `.vtr` extension. A `.vtr`
file is a typed container: a magic header, an attached column schema,
one self-describing record per column per row group, and a trailing
row-group index for fast seeking. Each column within a row group is
stored as a typed array with a validity bitmap for NA values. Encoding
and compression are handled per column per row group by the vendored tdc
codec, which derives a specification (a model, a transform chain, and an
entropy stage) from the requested compression level and emits one
self-describing record. Per-row-group statistics (min, max, null count)
are attached to the index so the scan node can prune row groups a
predicate excludes before decoding them.

Write any data.frame to `.vtr` with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md).
Open it lazily with
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md), which
returns a `vectra_node` object. No data is read at this point. The
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) call reads
only the file header to learn the schema and row group layout.

``` r

library(vectra)
#> 
#> Attaching package: 'vectra'
#> The following object is masked from 'package:stats':
#> 
#>     filter
#> The following object is masked from 'package:graphics':
#> 
#>     grid

f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

node <- tbl(f)
node
#> vectra query node
#> Columns (11):
#>   mpg <double>
#>   cyl <double>
#>   disp <double>
#>   hp <double>
#>   drat <double>
#>   wt <double>
#>   qsec <double>
#>   vs <double>
#>   am <double>
#>   gear <double>
#>   carb <double>
```

The print method shows the schema and file path but does not scan any
data. Call
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) to
pull all batches through the plan and materialize the result as a
data.frame.

``` r

tbl(f) |> collect() |> head()
#>    mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
#> 4 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
#> 5 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
#> 6 18.1   6  225 105 2.76 3.460 20.22  1  0    3    1
```

For large datasets,
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
accepts a `batch_size` parameter that controls how many rows go into
each row group. Smaller row groups give finer-grained zone-map pruning
(the scan node skips row groups whose min/max statistics exclude your
filter predicate) but add per-group overhead. The default, one row group
per data.frame, works well for most in-memory writes. For streaming
writes from a pipeline, the upstream batch size is preserved.

``` r

f_batched <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f_batched, batch_size = 10)
tbl(f_batched) |> collect() |> nrow()
#> [1] 32
```

The row count is the same. The difference is internal layout: 4 row
groups of approximately 10 rows each instead of 1 row group of 32 rows.
With smaller row groups, zone-map pruning becomes more selective. If
`mpg` ranges from 10.4 to 33.9 across the full dataset but only 10.4 to
15.2 within one particular row group, a `filter(mpg > 20)` can skip that
row group entirely without decompressing any column data. The tradeoff
is overhead: each row group carries its own encoding metadata and
compression frame, so very small row groups (under a few hundred rows)
waste space on headers. For most analytical workloads, row groups
between 10,000 and 100,000 rows strike a good balance.

## Filtering and selecting

[`filter()`](https://gillescolling.com/vectra/reference/filter.md) keeps
rows matching one or more conditions. Comma-separated conditions inside
a single
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) call
are combined with AND. Use `|` for OR within a single expression. You
can also chain multiple
[`filter()`](https://gillescolling.com/vectra/reference/filter.md)
calls, which stacks FilterNodes in the plan. The engine evaluates them
sequentially, one batch at a time, so the second filter only sees rows
that passed the first.

``` r

tbl(f) |>
  filter(cyl == 6, mpg > 19) |>
  select(mpg, cyl, hp, wt) |>
  collect()
#>    mpg cyl  hp    wt
#> 1 21.0   6 110 2.620
#> 2 21.0   6 110 2.875
#> 3 21.4   6 110 3.215
#> 4 19.2   6 123 3.440
#> 5 19.7   6 175 2.770
```

The C engine evaluates the predicate batch-by-batch and sets a selection
vector on each `VecBatch`. The selection vector is an integer array of
indices that passed the predicate. Downstream nodes (project, sort,
aggregation) read only those indices, so no column data is copied or
compacted at the filter stage. This zero-copy approach means filtering a
50 GB file with a 1% hit rate allocates memory proportional to 1% of one
batch, not 1% of the entire file.

NA values in comparisons follow SQL semantics: `NA == 6` evaluates to
NA, not FALSE. Rows where the predicate evaluates to NA are excluded
from the selection vector, the same as rows that evaluate to FALSE. To
explicitly test for NA, use [`is.na()`](https://rdrr.io/r/base/NA.html)
in the predicate.

OR conditions and parenthesized expressions work as expected.

``` r

tbl(f) |>
  filter(cyl == 4 | cyl == 8) |>
  select(mpg, cyl) |>
  collect() |>
  head()
#>    mpg cyl
#> 1 22.8   4
#> 2 18.7   8
#> 3 14.3   8
#> 4 24.4   4
#> 5 22.8   4
#> 6 16.4   8
```

For set membership, `%in%` is accelerated with a hash set when the
right-hand side has more than a handful of values. On a column with 10
million rows and a set of 500 target values, the hash-based `%in%` is
roughly 10x faster than repeated OR comparisons.

``` r

tbl(f) |>
  filter(cyl %in% c(4, 6)) |>
  select(mpg, cyl) |>
  collect() |>
  head()
#>    mpg cyl
#> 1 21.0   6
#> 2 21.0   6
#> 3 22.8   4
#> 4 21.4   6
#> 5 18.1   6
#> 6 24.4   4
```

[`select()`](https://gillescolling.com/vectra/reference/select.md) picks
columns by name or by tidyselect helper. The full tidyselect vocabulary
works: `starts_with()`, `ends_with()`, `contains()`, `matches()`
(regex), `where()` (type predicate), `everything()`, `last_col()`, and
negation via `-` or `!`. Column renaming inside
[`select()`](https://gillescolling.com/vectra/reference/select.md) also
works (`select(miles = mpg)` renames on the fly). Under the hood,
[`select()`](https://gillescolling.com/vectra/reference/select.md)
inserts a ProjectNode that prunes columns from the batch before they
reach the next verb, so columns you drop are never decompressed from
disk in subsequent row groups.

``` r

tbl(f) |>
  select(starts_with("d"), mpg) |>
  collect() |>
  head()
#>   disp drat  mpg
#> 1  160 3.90 21.0
#> 2  160 3.90 21.0
#> 3  108 3.85 22.8
#> 4  258 3.08 21.4
#> 5  360 3.15 18.7
#> 6  225 2.76 18.1
```

Negation drops columns.

``` r

tbl(f) |>
  select(-am, -vs, -gear, -carb) |>
  collect() |>
  head()
#>    mpg cyl disp  hp drat    wt  qsec
#> 1 21.0   6  160 110 3.90 2.620 16.46
#> 2 21.0   6  160 110 3.90 2.875 17.02
#> 3 22.8   4  108  93 3.85 2.320 18.61
#> 4 21.4   6  258 110 3.08 3.215 19.44
#> 5 18.7   8  360 175 3.15 3.440 17.02
#> 6 18.1   6  225 105 2.76 3.460 20.22
```

[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
prints the query plan tree. We can see how
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
[`select()`](https://gillescolling.com/vectra/reference/select.md) map
to plan nodes, and where predicate pushdown applies.

``` r

tbl(f) |>
  filter(cyl > 4) |>
  select(mpg, cyl, hp) |>
  explain()
#> vectra execution plan
#> 
#> ProjectNode [streaming] 
#>   FilterNode [streaming] 
#>     ScanNode [streaming, 3/11 cols (pruned), predicate pushdown, tdc stats] 
#> 
#> Output columns (3):
#>   mpg <double>
#>   cyl <double>
#>   hp <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

The ProjectNode prunes columns before the data reaches R. The FilterNode
sits below it, and the ScanNode reads from disk. When a hash index
exists on the filter column, the scan node probes the `.vtri` sidecar
index and builds a bitmap of row groups that could contain matching
rows. Row groups not in the bitmap are skipped entirely. This composes
with zone-map pruning: even without an explicit index, the scan node
compares the filter predicate against per-row-group min/max statistics
and skips row groups that cannot match. Between these two mechanisms, a
point query on an indexed column over a 10 GB file with 1000 row groups
might read only 1 row group.

## Transforming columns

[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) adds
or replaces columns. All expression evaluation happens in the C engine:
the R layer captures the expression as an abstract syntax tree (via
[`substitute()`](https://rdrr.io/r/base/substitute.html)), serializes it
into a nested list structure, and passes it across the
[`.Call()`](https://rdrr.io/r/base/CallExternal.html) bridge. The C side
walks the expression tree and evaluates it element-wise over the batch.
Supported operators include arithmetic (`+`, `-`, `*`, `/`, `%%`),
comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`), boolean (`&`, `|`, `!`),
and unary negation. There is no R-level evaluation per row, which is why
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) on
100 million rows runs at C speed regardless of expression complexity.

``` r

tbl(f) |>
  mutate(kpl = mpg * 0.425144, hp_per_wt = hp / wt) |>
  select(mpg, kpl, hp, wt, hp_per_wt) |>
  collect() |>
  head()
#>    mpg      kpl  hp    wt hp_per_wt
#> 1 21.0 8.928024 110 2.620  41.98473
#> 2 21.0 8.928024 110 2.875  38.26087
#> 3 22.8 9.693283  93 2.320  40.08621
#> 4 21.4 9.098082 110 3.215  34.21462
#> 5 18.7 7.950193 175 3.440  50.87209
#> 6 18.1 7.695106 105 3.460  30.34682
```

Math functions are evaluated natively in C:
[`abs()`](https://rdrr.io/r/base/MathFun.html),
[`sqrt()`](https://rdrr.io/r/base/MathFun.html),
[`log()`](https://rdrr.io/r/base/Log.html),
[`log2()`](https://rdrr.io/r/base/Log.html),
[`log10()`](https://rdrr.io/r/base/Log.html),
[`exp()`](https://rdrr.io/r/base/Log.html),
[`floor()`](https://rdrr.io/r/base/Round.html),
[`ceiling()`](https://rdrr.io/r/base/Round.html),
[`round()`](https://rdrr.io/r/base/Round.html),
[`sign()`](https://rdrr.io/r/base/sign.html),
[`trunc()`](https://rdrr.io/r/base/Round.html). For element-wise binary
min/max, [`pmin()`](https://rdrr.io/r/base/Extremes.html) and
[`pmax()`](https://rdrr.io/r/base/Extremes.html) take two arguments
(columns or literals) and return the smaller or larger of each pair.
These compose with arithmetic to build clamping expressions.

``` r

tbl(f) |>
  mutate(
    log_hp = log(hp),
    hp_floor = floor(hp / 10) * 10,
    bounded = pmin(pmax(mpg, 15), 25)
  ) |>
  select(hp, log_hp, hp_floor, mpg, bounded) |>
  collect() |>
  head()
#>    hp   log_hp hp_floor  mpg bounded
#> 1 110 4.700480      110 21.0    21.0
#> 2 110 4.700480      110 21.0    21.0
#> 3  93 4.532599       90 22.8    22.8
#> 4 110 4.700480      110 21.4    21.4
#> 5 175 5.164786      170 18.7    18.7
#> 6 105 4.653960      100 18.1    18.1
```

[`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
works like
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) but
drops all columns except the new ones.

``` r

tbl(f) |>
  transmute(
    efficiency = mpg / wt,
    power_ratio = hp / disp
  ) |>
  collect() |>
  head()
#>   efficiency power_ratio
#> 1   8.015267   0.6875000
#> 2   7.304348   0.6875000
#> 3   9.827586   0.8611111
#> 4   6.656299   0.4263566
#> 5   5.436047   0.4861111
#> 6   5.231214   0.4666667
```

Type casting converts between vectra’s four internal column types.
[`as.numeric()`](https://rdrr.io/r/base/numeric.html) and
[`as.double()`](https://rdrr.io/r/base/double.html) cast to double,
[`as.integer()`](https://rdrr.io/r/base/integer.html) casts to int64,
[`as.character()`](https://rdrr.io/r/base/character.html) casts to
string, and [`as.logical()`](https://rdrr.io/r/base/logical.html) casts
to bool. Casting follows R semantics:
[`as.integer()`](https://rdrr.io/r/base/integer.html) on a double
truncates toward zero,
[`as.numeric()`](https://rdrr.io/r/base/numeric.html) on a string parses
the number or returns NA, and
[`as.character()`](https://rdrr.io/r/base/character.html) on a numeric
formats it as a decimal string. These casts happen per-element in C and
respect the validity bitmap, so NA values stay NA after casting.

``` r

tbl(f) |>
  mutate(cyl_str = as.character(cyl)) |>
  select(cyl, cyl_str) |>
  collect() |>
  head(3)
#>   cyl cyl_str
#> 1   6       6
#> 2   6       6
#> 3   4       4
```

[`is.na()`](https://rdrr.io/r/base/NA.html) tests for missing values by
checking the validity bitmap directly, without touching the data buffer.
`if_else()` and `case_when()` handle conditional logic. Unlike base R’s
[`ifelse()`](https://rdrr.io/r/base/ifelse.html), `if_else()` is
type-strict: both branches must have the same type, and the result
preserves NA from the condition (matching dplyr semantics).
`case_when()` evaluates formulas top to bottom and assigns the value
from the first matching condition. `coalesce()` returns the first non-NA
value from an arbitrary number of columns or literals. `between()` is
shorthand for `x >= left & x <= right`, compiled to a single AND
expression internally.

``` r

tbl(f) |>
  mutate(
    size = case_when(
      cyl == 4 ~ "small",
      cyl == 6 ~ "medium",
      cyl == 8 ~ "large"
    ),
    mpg_class = if_else(mpg > 20, "high", "low"),
    in_range = between(hp, 100, 200)
  ) |>
  select(cyl, size, mpg, mpg_class, hp, in_range) |>
  collect() |>
  head()
#>   cyl   size  mpg mpg_class  hp in_range
#> 1   6 medium 21.0      high 110     TRUE
#> 2   6 medium 21.0      high 110     TRUE
#> 3   4  small 22.8      high  93    FALSE
#> 4   6 medium 21.4      high 110     TRUE
#> 5   8  large 18.7       low 175     TRUE
#> 6   6 medium 18.1       low 105     TRUE
```

`case_when()` accepts a `.default` argument for the fallback value.
Formulas are evaluated top to bottom; the first matching condition wins.

`coalesce()` takes any number of arguments and returns the first non-NA
value for each row. It works with both column references and literal
values.

``` r

df_na <- data.frame(
  a = c(NA, 2, NA, 4),
  b = c(10, NA, NA, 40),
  stringsAsFactors = FALSE
)
f_na <- tempfile(fileext = ".vtr")
write_vtr(df_na, f_na)

tbl(f_na) |>
  mutate(filled = coalesce(a, b, 0)) |>
  collect()
#>    a  b filled
#> 1 NA 10     10
#> 2  2 NA      2
#> 3 NA NA      0
#> 4  4 40      4
```

## String operations

vectra evaluates string functions in C using a two-pass strategy. The
first pass walks the input strings and computes the total output byte
count. The second pass allocates a single contiguous buffer of that size
and fills it. This avoids per-row `malloc`/`realloc` overhead that would
otherwise dominate runtime on columns with millions of short strings.
For regex operations (`grepl` with `fixed = FALSE`, `gsub`, `sub`,
`str_extract`), the engine uses POSIX regex compiled once per batch (or
once per thread under OpenMP), supporting full backreference syntax
(`\\1`, `\\2`, etc.) and capture groups.

We will use a small dataset with character columns for the examples. The
patterns here are representative of what you would do on larger string
columns; the C engine processes them identically regardless of batch
size.

``` r

people <- data.frame(
  name = c("  Alice  ", "Bob", "Charlie Brown", "Diana"),
  city = c("Amsterdam", "Berlin", "Chicago", "Dublin"),
  email = c("alice@example.com", "bob@test.org",
            "charlie.b@work.net", "diana@example.com"),
  stringsAsFactors = FALSE
)
fs <- tempfile(fileext = ".vtr")
write_vtr(people, fs)
```

[`nchar()`](https://rdrr.io/r/base/nchar.html) returns the number of
characters in each string (not bytes; the engine is UTF-8 aware for
multi-byte characters). [`substr()`](https://rdrr.io/r/base/substr.html)
extracts a substring by 1-indexed start and stop positions, matching
base R semantics. [`trimws()`](https://rdrr.io/r/base/trimws.html)
strips leading and trailing whitespace (spaces, tabs, newlines).

``` r

tbl(fs) |>
  mutate(
    name_trimmed = trimws(name),
    name_len = nchar(trimws(name)),
    city_prefix = substr(city, 1, 3)
  ) |>
  select(name_trimmed, name_len, city_prefix) |>
  collect()
#>    name_trimmed name_len city_prefix
#> 1         Alice        5         Ams
#> 2           Bob        3         Ber
#> 3 Charlie Brown       13         Chi
#> 4         Diana        5         Dub
```

[`toupper()`](https://rdrr.io/r/base/chartr.html) and
[`tolower()`](https://rdrr.io/r/base/chartr.html) convert case.
[`startsWith()`](https://rdrr.io/r/base/startsWith.html) and
[`endsWith()`](https://rdrr.io/r/base/startsWith.html) test prefixes and
suffixes, returning a boolean column.

``` r

tbl(fs) |>
  mutate(
    city_upper = toupper(city),
    is_example = endsWith(email, "example.com"),
    starts_a = startsWith(city, "A")
  ) |>
  select(city_upper, email, is_example, starts_a) |>
  collect()
#>   city_upper              email is_example starts_a
#> 1  AMSTERDAM  alice@example.com       TRUE     TRUE
#> 2     BERLIN       bob@test.org      FALSE    FALSE
#> 3    CHICAGO charlie.b@work.net      FALSE    FALSE
#> 4     DUBLIN  diana@example.com       TRUE    FALSE
```

[`grepl()`](https://rdrr.io/r/base/grep.html) tests for pattern matches.
The default is fixed matching (literal substring search), which is the
fastest path and uses a simple `strstr()` internally. Pass
`fixed = FALSE` for POSIX regex, which compiles the pattern once per
batch and applies it element-wise. On columns with more than 1000 rows,
regex `grepl` is OpenMP-parallelized with per-thread regex compilation
for thread safety.

``` r

tbl(fs) |>
  mutate(has_at = grepl("@example", email)) |>
  select(email, has_at) |>
  collect()
#>                email has_at
#> 1  alice@example.com   TRUE
#> 2       bob@test.org  FALSE
#> 3 charlie.b@work.net  FALSE
#> 4  diana@example.com   TRUE
```

[`gsub()`](https://rdrr.io/r/base/grep.html) and
[`sub()`](https://rdrr.io/r/base/grep.html) perform string replacement.
[`gsub()`](https://rdrr.io/r/base/grep.html) replaces all occurrences of
the pattern; [`sub()`](https://rdrr.io/r/base/grep.html) replaces only
the first. Like [`grepl()`](https://rdrr.io/r/base/grep.html), the
default is fixed matching. Pass `fixed = FALSE` for regex with full
backreference support. In regex mode, the replacement string can
reference capture groups with `\\1`, `\\2`, and so on, following the
POSIX extended regex specification.

``` r

tbl(fs) |>
  mutate(domain = gsub(".*@", "", email, fixed = FALSE)) |>
  select(email, domain) |>
  collect()
#>                email      domain
#> 1  alice@example.com example.com
#> 2       bob@test.org    test.org
#> 3 charlie.b@work.net    work.net
#> 4  diana@example.com example.com
```

`str_extract()` pulls out the first match of a regex pattern and returns
it as a string. When there is no match, the result is NA. If the pattern
contains a capture group (parentheses), `str_extract()` returns the
captured portion rather than the full match, which makes it possible to
extract structured substrings like domains from URLs or species epithets
from binomial names.

``` r

tbl(fs) |>
  mutate(user = str_extract(email, "^[^@]+")) |>
  select(email, user) |>
  collect()
#>                email      user
#> 1  alice@example.com     alice
#> 2       bob@test.org       bob
#> 3 charlie.b@work.net charlie.b
#> 4  diana@example.com     diana
```

[`paste()`](https://rdrr.io/r/base/paste.html) and
[`paste0()`](https://rdrr.io/r/base/paste.html) concatenate columns and
literals. Both are N-ary: they accept any number of arguments, mixing
column references and string literals freely.
[`paste()`](https://rdrr.io/r/base/paste.html) takes a `sep` argument
(default `" "`) that is inserted between each pair of arguments;
[`paste0()`](https://rdrr.io/r/base/paste.html) concatenates with no
separator. Internally, these use the two-pass allocation strategy: the
first pass sums up the output lengths across all arguments, allocates
once, then the second pass writes the concatenated strings into the
pre-sized buffer.

``` r

tbl(fs) |>
  mutate(
    greeting = paste0("Hello, ", trimws(name), "!"),
    label = paste(trimws(name), city, sep = " - ")
  ) |>
  select(greeting, label) |>
  collect()
#>                greeting                   label
#> 1         Hello, Alice!       Alice - Amsterdam
#> 2           Hello, Bob!            Bob - Berlin
#> 3 Hello, Charlie Brown! Charlie Brown - Chicago
#> 4         Hello, Diana!          Diana - Dublin
```

For a complete reference of string operations, regex patterns, and
performance characteristics on large columns, see
[`vignette("string-ops")`](https://gillescolling.com/vectra/articles/string-ops.md).

## Aggregation

[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
marks columns as grouping keys. It is metadata-only: no plan node is
created, no data is scanned, and the grouping information simply
attaches to the `vectra_node` as an R-level attribute. The C engine only
learns about the groups when
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
(or equivalently
[`summarize()`](https://gillescolling.com/vectra/reference/summarise.md))
is called, at which point a GroupAggNode is created in the plan. This
node builds a hash table keyed on the group columns and accumulates
aggregation results in a single pass through the input batches. Each
batch’s rows are hashed, looked up in the hash table, and their values
are folded into the running aggregation state. Once all batches are
consumed, the hash table entries are flushed as the output.

vectra supports 14 aggregation functions, all evaluated in C: `n()`,
[`sum()`](https://rdrr.io/r/base/sum.html),
[`mean()`](https://rdrr.io/r/base/mean.html),
[`min()`](https://rdrr.io/r/base/Extremes.html),
[`max()`](https://rdrr.io/r/base/Extremes.html),
[`sd()`](https://rdrr.io/r/stats/sd.html),
[`var()`](https://rdrr.io/r/stats/cor.html), `first()`, `last()`,
[`any()`](https://rdrr.io/r/base/any.html),
[`all()`](https://rdrr.io/r/base/all.html),
[`median()`](https://rdrr.io/r/stats/median.html), `n_distinct()`. All
except `n()` take a column name as their first argument. All except
`n()` and `n_distinct()` accept `na.rm = TRUE` to skip NA values.
Without `na.rm`, any NA in a group poisons the result:
[`sum()`](https://rdrr.io/r/base/sum.html) returns NA,
[`mean()`](https://rdrr.io/r/base/mean.html) returns NA, and so on. This
matches base R semantics. With `na.rm = TRUE`, the behavior also matches
R: [`sum()`](https://rdrr.io/r/base/sum.html) on all-NA returns 0,
[`mean()`](https://rdrr.io/r/base/mean.html) on all-NA returns NaN, and
[`min()`](https://rdrr.io/r/base/Extremes.html)/[`max()`](https://rdrr.io/r/base/Extremes.html)
on all-NA return Inf/-Inf with a warning.

``` r

tbl(f) |>
  group_by(cyl) |>
  summarise(
    count = n(),
    avg_mpg = mean(mpg),
    total_hp = sum(hp),
    best_mpg = max(mpg)
  ) |>
  collect()
#>   cyl count  avg_mpg total_hp best_mpg
#> 1   4    11 26.66364      909     33.9
#> 2   6     7 19.74286      856     21.4
#> 3   8    14 15.10000     2929     19.2
```

The result has one row per group. Groups are not guaranteed to be in any
particular order unless you add an
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
afterward.

[`sd()`](https://rdrr.io/r/stats/sd.html) and
[`var()`](https://rdrr.io/r/stats/cor.html) compute sample standard
deviation and variance using the Welford one-pass algorithm (denominator
`n - 1`, matching R’s [`sd()`](https://rdrr.io/r/stats/sd.html) and
[`var()`](https://rdrr.io/r/stats/cor.html)). `first()` and `last()`
return the first and last non-NA values encountered in scan order within
each group. [`any()`](https://rdrr.io/r/base/any.html) and
[`all()`](https://rdrr.io/r/base/all.html) reduce boolean (or
boolean-coercible) columns, returning TRUE/FALSE per group. These are
useful for flagging groups that contain at least one positive
observation or checking whether all rows in a group satisfy a condition.

``` r

tbl(f) |>
  group_by(cyl) |>
  summarise(
    mpg_sd = sd(mpg),
    mpg_var = var(mpg),
    first_hp = first(hp),
    last_hp = last(hp)
  ) |>
  collect()
#>   cyl   mpg_sd   mpg_var first_hp last_hp
#> 1   4 4.509828 20.338545       93     109
#> 2   6 1.453567  2.112857      110     175
#> 3   8 2.560048  6.553846      175     335
```

[`median()`](https://rdrr.io/r/stats/median.html) uses a per-group
dynamic array and quicksort internally. `n_distinct()` uses a per-group
open-addressing hash set with FNV-1a hashing at 70% load factor.

``` r

tbl(f) |>
  group_by(cyl) |>
  summarise(
    med_mpg = median(mpg),
    unique_gears = n_distinct(gear)
  ) |>
  collect()
#>   cyl med_mpg unique_gears
#> 1   4    26.0            3
#> 2   6    19.7            3
#> 3   8    15.2            2
```

For quick counts,
[`count()`](https://gillescolling.com/vectra/reference/count.md) and
[`tally()`](https://gillescolling.com/vectra/reference/count.md) are
convenient shorthands.
[`count()`](https://gillescolling.com/vectra/reference/count.md) takes
grouping columns inline;
[`tally()`](https://gillescolling.com/vectra/reference/count.md) uses
existing groups.

``` r

tbl(f) |>
  count(cyl, sort = TRUE) |>
  collect()
#>   cyl  n
#> 1   8 14
#> 2   4 11
#> 3   6  7
```

``` r

tbl(f) |>
  group_by(gear) |>
  tally() |>
  collect()
#>   gear  n
#> 1    3 15
#> 2    4 12
#> 3    5  5
```

[`across()`](https://gillescolling.com/vectra/reference/across.md)
applies the same function or set of functions to multiple columns at
once. It works inside both
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
and [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
accepting tidyselect column specifications (including helpers like
`where(is.numeric)`, `starts_with()`, etc.) and functions as either a
single function, a formula (`~ .x + 1`), or a named list of functions.
The `.names` argument controls output column naming with `{.col}` and
`{.fn}` glue-style placeholders, so you get predictable names like
`mpg_avg`, `hp_total` without manual repetition. Under the hood,
[`across()`](https://gillescolling.com/vectra/reference/across.md) is
expanded at the R level into individual aggregation expressions before
the plan reaches C, so there is no runtime overhead compared to writing
each expression by hand.

``` r

tbl(f) |>
  group_by(cyl) |>
  summarise(across(c(mpg, hp, wt), mean)) |>
  collect()
#>   cyl      mpg        hp       wt
#> 1   4 26.66364  82.63636 2.285727
#> 2   6 19.74286 122.28571 3.117143
#> 3   8 15.10000 209.21429 3.999214
```

With multiple functions and a naming pattern:

``` r

tbl(f) |>
  group_by(cyl) |>
  summarise(across(
    c(mpg, hp),
    list(avg = mean, total = sum),
    .names = "{.col}_{.fn}"
  )) |>
  collect()
#>   cyl  mpg_avg    hp_avg mpg_total hp_total
#> 1   4 26.66364  82.63636     293.3      909
#> 2   6 19.74286 122.28571     138.2      856
#> 3   8 15.10000 209.21429     211.4     2929
```

[`ungroup()`](https://gillescolling.com/vectra/reference/ungroup.md)
removes grouping metadata from a node. This is occasionally needed when
you want to apply a second
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
with different groups, or when you want window functions to operate on
the full dataset rather than per-group.

``` r

tbl(f) |>
  group_by(cyl, gear) |>
  summarise(n = n(), .groups = "keep") |>
  ungroup() |>
  arrange(desc(n)) |>
  collect()
#>   cyl gear  n
#> 1   8    3 12
#> 2   4    4  8
#> 3   6    4  4
#> 4   4    5  2
#> 5   6    3  2
#> 6   8    5  2
#> 7   4    3  1
#> 8   6    5  1
```

The `.groups` argument to
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
controls what happens to grouping after aggregation. `"drop_last"` (the
default) peels off the last grouping column, `"drop"` removes all
groups, and `"keep"` preserves them.

## Sorting and slicing

[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
sorts rows. Wrap column names in
[`desc()`](https://gillescolling.com/vectra/reference/desc.md) for
descending order. Sort is stable: rows with equal sort keys preserve
their original order.

``` r

tbl(f) |>
  select(mpg, cyl, hp) |>
  arrange(cyl, desc(mpg)) |>
  collect() |>
  head(8)
#>    mpg cyl  hp
#> 1 33.9   4  65
#> 2 32.4   4  66
#> 3 30.4   4  52
#> 4 30.4   4 113
#> 5 27.3   4  66
#> 6 26.0   4  91
#> 7 24.4   4  62
#> 8 22.8   4  93
```

Under the hood,
[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
uses an external merge sort with a 1 GB memory budget. When data exceeds
that limit, sorted runs spill to temporary `.vtr` files and merge via a
k-way min-heap. NAs sort last in ascending order and first in descending
order.

[`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md)
and
[`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md)
return the first or last `n` rows.
[`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md)
is streaming (it creates a LimitNode that stops pulling after `n` rows).
[`slice_tail()`](https://gillescolling.com/vectra/reference/slice_head.md)
must materialize all rows to know the total count.

``` r

tbl(f) |>
  slice_head(n = 5) |>
  collect()
#>    mpg cyl disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
#> 3 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
#> 4 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
#> 5 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
```

[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
and
[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
use an optimized top-N algorithm (a bounded heap) that avoids a full
sort. This matters when you want the 10 smallest values from a
100-million-row dataset: the heap holds at most 10 entries at any point.

``` r

tbl(f) |>
  select(mpg, cyl, hp) |>
  slice_min(order_by = mpg, n = 3) |>
  collect()
#>     mpg cyl  hp
#> 15 10.4   8 205
#> 16 10.4   8 215
#> 24 13.3   8 245
```

By default,
[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
and
[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
include ties. The result may contain more than `n` rows if multiple rows
share the boundary value. Use `with_ties = FALSE` for exactly `n` rows.

``` r

tbl(f) |>
  select(mpg, cyl) |>
  slice_min(order_by = cyl, n = 3, with_ties = FALSE) |>
  collect()
#>    mpg cyl
#> 1 22.8   4
#> 2 24.4   4
#> 3 22.8   4
```

``` r

tbl(f) |>
  select(mpg, cyl, hp) |>
  slice_max(order_by = hp, n = 4, with_ties = FALSE) |>
  collect()
#>    mpg cyl  hp
#> 1 15.0   8 335
#> 2 15.8   8 264
#> 3 14.3   8 245
#> 4 13.3   8 245
```

## Joins

vectra implements hash joins using a build-right, probe-left strategy.
The entire right-side table is consumed and stored in a hash table keyed
on the join columns. Then left-side batches stream through one at a
time, probing the hash table for matches. Memory cost is proportional to
the number of rows in the right-side table, so the standard practice is
to put the smaller table on the right. For a left table with 500 million
rows and a right table with 50,000 rows, the hash table holds 50,000
entries and the left side streams through in constant memory.

Seven join types are available.
[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps all left rows and fills unmatched right columns with NA.
[`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps only rows with matches on both sides.
[`right_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps all right rows.
[`full_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps everything from both sides, filling NA where there is no match.
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps left rows that have at least one match on the right, without
adding any right-side columns.
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps left rows that have no match.
[`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md)
produces the Cartesian product (every left row paired with every right
row). NA keys never match in any join type, following SQL NULL
semantics.

``` r

cyl_info <- data.frame(
  cyl = c(4, 6, 8),
  engine_type = c("inline", "v-type", "v-type"),
  stringsAsFactors = FALSE
)
f_cyl <- tempfile(fileext = ".vtr")
write_vtr(cyl_info, f_cyl)
```

[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps all rows from the left table. Unmatched right-side columns are
filled with NA.

``` r

tbl(f) |>
  select(mpg, cyl, hp) |>
  left_join(tbl(f_cyl), by = "cyl") |>
  collect() |>
  head()
#>    mpg cyl  hp engine_type
#> 1 21.0   6 110      v-type
#> 2 21.0   6 110      v-type
#> 3 22.8   4  93      inline
#> 4 21.4   6 110      v-type
#> 5 18.7   8 175      v-type
#> 6 18.1   6 105      v-type
```

[`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps only rows with matching keys on both sides.
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps left-side rows that have a match on the right but does not add any
right-side columns.
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps left-side rows that have no match.

``` r

tbl(f) |>
  select(mpg, cyl) |>
  anti_join(
    tbl(f_cyl) |> filter(engine_type == "v-type"),
    by = "cyl"
  ) |>
  collect() |>
  head()
#>    mpg cyl
#> 1 22.8   4
#> 2 24.4   4
#> 3 22.8   4
#> 4 32.4   4
#> 5 30.4   4
#> 6 33.9   4
```

That returns only the 4-cylinder cars, since both 6- and 8-cylinder
entries are in the “v-type” subset.

For joins on differently named columns, use a named vector in `by`.
Multi-column keys work by passing multiple names.

``` r

ratings <- data.frame(
  cylinders = c(4, 6, 8),
  rating = c("A", "B", "C"),
  stringsAsFactors = FALSE
)
f_rat <- tempfile(fileext = ".vtr")
write_vtr(ratings, f_rat)

tbl(f) |>
  select(mpg, cyl) |>
  inner_join(tbl(f_rat), by = c("cyl" = "cylinders")) |>
  collect() |>
  head()
#>    mpg cyl rating
#> 1 21.0   6      B
#> 2 21.0   6      B
#> 3 22.8   4      A
#> 4 21.4   6      B
#> 5 18.7   8      C
#> 6 18.1   6      B
```

The `suffix` argument (default `c(".x", ".y")`) controls how non-key
columns with the same name on both sides are disambiguated. For example,
if both tables have a column called `value`, the output will contain
`value.x` from the left and `value.y` from the right. Multi-column keys
work by passing a character vector to `by`, either unnamed (same column
names on both sides) or named (mapping left names to right names).

Key types are auto-coerced following the `bool < int64 < double`
promotion hierarchy. If the left key is int64 and the right key is
double, the int64 values are promoted to double before hashing. Joining
a string column against a numeric column is an error and fails
immediately with a descriptive message.

vectra also supports
[`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
for approximate string matching between tables. It computes string
distances between key columns and keeps pairs within a normalized
distance threshold.

``` r

ref_species <- data.frame(
  canonical = c("Quercus robur", "Quercus petraea",
                 "Fagus sylvatica"),
  code = c("QR", "QP", "FS"),
  stringsAsFactors = FALSE
)
query_species <- data.frame(
  name = c("Quercus robur", "Qurecus petraea",
           "Fagus sylvatca"),
  stringsAsFactors = FALSE
)
f_ref <- tempfile(fileext = ".vtr")
f_query <- tempfile(fileext = ".vtr")
write_vtr(ref_species, f_ref)
write_vtr(query_species, f_query)

tbl(f_query) |>
  fuzzy_join(
    tbl(f_ref),
    by = c("name" = "canonical"),
    method = "dl",
    max_dist = 0.15
  ) |>
  collect()
#>              name       canonical code fuzzy_dist
#> 1   Quercus robur   Quercus robur   QR 0.00000000
#> 2 Qurecus petraea Quercus petraea   QP 0.06666667
#> 3  Fagus sylvatca Fagus sylvatica   FS 0.06666667
```

The `method` argument accepts `"dl"` (Damerau-Levenshtein),
`"levenshtein"`, or `"jw"` (Jaro-Winkler). The `block_by` parameter
restricts comparisons to rows sharing an exact-match value on a second
column, which can reduce runtime by orders of magnitude on large
reference tables. For full documentation, see
[`vignette("joins")`](https://gillescolling.com/vectra/articles/joins.md).

## Window functions

Window functions compute values row-by-row in the context of the full
dataset or a per-group partition. Unlike aggregation, which collapses
groups to single rows, window functions preserve the original row count.
They are detected inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) by
function name and automatically routed to a dedicated WindowNode in the
execution plan. The WindowNode is a materializing operation: it consumes
all input rows (per partition) before producing output, because
functions like [`rank()`](https://rdrr.io/r/base/rank.html) need to see
all values before assigning ranks.

vectra supports 12 window functions organized in three families. Ranking
functions: `row_number()` (no argument, sequential 1..n),
[`rank()`](https://rdrr.io/r/base/rank.html) and `dense_rank()` (take a
column argument, with [`rank()`](https://rdrr.io/r/base/rank.html)
leaving gaps on ties and `dense_rank()` not), `ntile(n)` (divides rows
into `n` roughly equal buckets), `percent_rank()` and `cume_dist()`
(take a column argument, return normalized rank as a double between 0
and 1). Offset functions: [`lag()`](https://rdrr.io/r/stats/lag.html)
and `lead()`. Cumulative functions:
[`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
[`cummin()`](https://rdrr.io/r/base/cumsum.html),
[`cummax()`](https://rdrr.io/r/base/cumsum.html).

``` r

tbl(f) |>
  select(mpg, cyl, hp) |>
  slice_head(n = 8) |>
  mutate(
    rn = row_number(),
    mpg_rank = rank(mpg),
    mpg_dense = dense_rank(mpg)
  ) |>
  collect()
#>    mpg cyl  hp rn mpg_rank mpg_dense
#> 1 21.0   6 110  1        4         4
#> 2 21.0   6 110  2        4         4
#> 3 22.8   4  93  3        7         6
#> 4 21.4   6 110  4        6         5
#> 5 18.7   8 175  5        3         3
#> 6 18.1   6 105  6        2         2
#> 7 14.3   8 245  7        1         1
#> 8 24.4   4  62  8        8         7
```

[`lag()`](https://rdrr.io/r/stats/lag.html) and `lead()` shift a column
forward or backward by `n` positions (default 1). `lag(mpg)` gives each
row the `mpg` value from the previous row; `lead(mpg)` gives the value
from the next row. The `default` argument specifies the fill value when
the offset falls outside the partition boundary (the first row for
`lag`, the last row for `lead`). Without `default`, these positions are
NA.

``` r

tbl(f) |>
  select(mpg, hp) |>
  slice_head(n = 6) |>
  mutate(
    prev_mpg = lag(mpg),
    next_mpg = lead(mpg),
    prev2_hp = lag(hp, n = 2, default = 0)
  ) |>
  collect()
#>    mpg  hp prev_mpg next_mpg prev2_hp
#> 1 21.0 110       NA     21.0        0
#> 2 21.0 110     21.0     22.8        0
#> 3 22.8  93     21.0     21.4      110
#> 4 21.4 110     22.8     18.7      110
#> 5 18.7 175     21.4     18.1       93
#> 6 18.1 105     18.7       NA      110
```

Cumulative functions: [`cumsum()`](https://rdrr.io/r/base/cumsum.html),
`cummean()`, [`cummin()`](https://rdrr.io/r/base/cumsum.html),
[`cummax()`](https://rdrr.io/r/base/cumsum.html).

``` r

tbl(f) |>
  select(mpg, hp) |>
  slice_head(n = 6) |>
  mutate(
    running_hp = cumsum(hp),
    running_avg = cummean(mpg),
    running_min = cummin(mpg)
  ) |>
  collect()
#>    mpg  hp running_hp running_avg running_min
#> 1 21.0 110        110       21.00        21.0
#> 2 21.0 110        220       21.00        21.0
#> 3 22.8  93        313       21.60        21.0
#> 4 21.4 110        423       21.55        21.0
#> 5 18.7 175        598       20.98        18.7
#> 6 18.1 105        703       20.50        18.1
```

Window functions respect grouping. When a node is grouped, partitions
are computed independently.

``` r

tbl(f) |>
  select(mpg, cyl) |>
  group_by(cyl) |>
  mutate(rn = row_number(), pct = percent_rank(mpg)) |>
  slice_head(n = 10) |>
  collect()
#>     mpg cyl rn       pct
#> 1  21.0   6  1 0.6666667
#> 2  21.0   6  2 0.6666667
#> 3  22.8   4  1 0.2000000
#> 4  21.4   6  3 1.0000000
#> 5  18.7   8  1 0.9230769
#> 6  18.1   6  4 0.1666667
#> 7  14.3   8  2 0.2307692
#> 8  24.4   4  2 0.4000000
#> 9  22.8   4  3 0.2000000
#> 10 19.2   6  5 0.3333333
```

Each `cyl` group gets its own `row_number()` sequence starting from 1
and its own `percent_rank()` distribution. The grouped window node
partitions the data by the group keys, materializes each partition
independently, computes the window functions within the partition, and
then concatenates the results. If a
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) call
mixes window functions with regular expressions (e.g.,
`mutate(rn = row_number(), double_mpg = mpg * 2)`), the R layer splits
them: window functions go to a WindowNode, regular expressions go to a
ProjectNode stacked on top. The split is invisible to the caller.

## Dates and times

Date and POSIXct columns roundtrip through vectra. Internally, dates are
stored as doubles: days since the Unix epoch (1970-01-01) for Date
objects, seconds since epoch for POSIXct. The R class attribute is
preserved in the `.vtr` schema metadata, so when you
[`collect()`](https://gillescolling.com/vectra/reference/collect.md),
the output columns have the correct R class restored. Component
extraction functions work in
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) and
[`filter()`](https://gillescolling.com/vectra/reference/filter.md):
`year()`, `month()`, `day()` for date parts, and `hour()`, `minute()`,
`second()` for time parts on POSIXct columns. These are evaluated in C
by computing the Gregorian calendar decomposition from the epoch offset,
so there is no R-level overhead per row.

``` r

events <- data.frame(
  event_date = as.Date(c("2020-03-15", "2020-07-01",
                          "2021-01-15", "2021-06-30")),
  event_time = as.POSIXct(c("2020-03-15 09:30:00",
                             "2020-07-01 14:00:00",
                             "2021-01-15 08:15:00",
                             "2021-06-30 17:45:00"),
                           tz = "UTC"),
  value = c(10, 20, 30, 40)
)
fd <- tempfile(fileext = ".vtr")
write_vtr(events, fd)
```

Extract date components and aggregate by year.

``` r

tbl(fd) |>
  mutate(
    yr = year(event_date),
    mo = month(event_date),
    dy = day(event_date)
  ) |>
  group_by(yr) |>
  summarise(total = sum(value)) |>
  collect()
#>     yr total
#> 1 2020    30
#> 2 2021    70
```

Extract time components from POSIXct.

``` r

tbl(fd) |>
  mutate(
    hr = hour(event_time),
    mn = minute(event_time)
  ) |>
  select(event_time, hr, mn) |>
  collect()
#>   event_time hr mn
#> 1 1584264600  9 30
#> 2 1593612000 14  0
#> 3 1610698500  8 15
#> 4 1625075100 17 45
```

Filter by date using [`as.Date()`](https://rdrr.io/r/base/as.Date.html)
to convert a string literal to a date value at parse time. The C engine
receives the numeric date value and compares directly.

``` r

tbl(fd) |>
  filter(event_date >= as.Date("2021-01-01")) |>
  collect()
#>   event_date          event_time value
#> 1 2021-01-15 2021-01-15 08:15:00    30
#> 2 2021-06-30 2021-06-30 17:45:00    40
```

Date arithmetic works through the numeric representation. Adding 30 to a
Date column advances it by 30 days. Subtracting two date columns gives a
difference in days. You can combine date extraction with aggregation:
`group_by(yr = year(event_date)) |> summarise(total = sum(value))` works
as expected because
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
expressions are allowed inline inside
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
through the standard dplyr convention of named expressions.

``` r

tbl(fd) |>
  mutate(plus_30 = event_date + 30) |>
  select(event_date, plus_30) |>
  collect()
#>   event_date plus_30
#> 1      18336   18366
#> 2      18444   18474
#> 3      18642   18672
#> 4      18808   18838
```

The `plus_30` column is numeric (days since epoch). To interpret it as a
Date in R after collecting, wrap it with
`as.Date(x, origin = "1970-01-01")`.

## String similarity

vectra computes edit distances directly in the C engine, with OpenMP
parallelization for columns exceeding 1000 rows. Three distance
algorithms are available.

`levenshtein()` counts insertions, deletions, and substitutions.
`dl_dist()` (Damerau-Levenshtein) additionally counts transpositions of
adjacent characters as a single edit, which better captures common typos
like “hte” for “the”. `jaro_winkler()` returns a similarity score from 0
to 1, where 1 means identical. It weights early-character matches more
heavily, making it well-suited for short strings like personal names.

``` r

species <- data.frame(
  name = c("Quercus robur", "Quercus rubra",
           "Fagus sylvatica", "Acer platanoides",
           "Quercus petraea"),
  stringsAsFactors = FALSE
)
fs2 <- tempfile(fileext = ".vtr")
write_vtr(species, fs2)
```

Compute all three metrics against a reference string and filter by edit
distance.

``` r

tbl(fs2) |>
  mutate(
    lev = levenshtein(name, "Quercus robur"),
    dl = dl_dist(name, "Quercus robur"),
    jw = jaro_winkler(name, "Quercus robur")
  ) |>
  filter(lev <= 5) |>
  arrange(lev) |>
  collect()
#>            name lev dl        jw
#> 1 Quercus robur   0  0 1.0000000
#> 2 Quercus rubra   3  3 0.9525641
```

Normalized variants `levenshtein_norm()` and `dl_dist_norm()` divide the
raw distance by the length of the longer string, returning a value
between 0 and 1. This makes thresholds transferable across strings of
different lengths.

``` r

tbl(fs2) |>
  mutate(
    lev_norm = levenshtein_norm(name, "Quercus robur"),
    dl_norm = dl_dist_norm(name, "Quercus robur")
  ) |>
  collect()
#>               name  lev_norm   dl_norm
#> 1    Quercus robur 0.0000000 0.0000000
#> 2    Quercus rubra 0.2307692 0.2307692
#> 3  Fagus sylvatica 0.9333333 0.9333333
#> 4 Acer platanoides 0.8125000 0.8125000
#> 5  Quercus petraea 0.4666667 0.4666667
```

Damerau-Levenshtein catches transposition errors that pure Levenshtein
scores as two edits.

``` r

tbl(fs2) |>
  mutate(
    lev = levenshtein(name, "Qurecus robur"),
    dl = dl_dist(name, "Qurecus robur")
  ) |>
  collect()
#>               name lev dl
#> 1    Quercus robur   2  1
#> 2    Quercus rubra   5  4
#> 3  Fagus sylvatica  14 14
#> 4 Acer platanoides  14 14
#> 5  Quercus petraea   9  8
```

The “ue” / “re” swap in “Qurecus” counts as 2 edits for Levenshtein
(delete + insert) but 1 for Damerau-Levenshtein (transposition). For
full coverage of distance algorithms, threshold tuning, and blocking
strategies, see
[`vignette("string-ops")`](https://gillescolling.com/vectra/articles/string-ops.md).

## Tree traversal

`resolve()` and `propagate()` are scalar self-join functions designed
for hierarchical (parent-child) data stored in a single table. They
operate within a batch by matching a foreign key column to a primary key
column in the same batch, which means the entire hierarchy must fit
within one row group (or the data must be collected first). These
functions are particularly useful for taxonomic backbones,
organizational charts, and bill-of-materials structures where each row
has a `parent_id` pointing to another row’s `id`.

`resolve(fk, pk, value)` returns the `value` column from the row where
`pk == fk`. It is a single-level lookup, equivalent to a self left-join
on `(fk, pk)`, but without the overhead of materializing a hash table.
The lookup is O(n) per batch (linear scan), so it works well for
hierarchies with up to a few million rows.

``` r

taxa <- data.frame(
  id        = c(1L, 2L, 3L, 4L),
  name      = c("Fagaceae", "Quercus", "Q. robur", "Q. petraea"),
  parent_id = c(NA, 1L, 2L, 2L),
  stringsAsFactors = FALSE
)
ft <- tempfile(fileext = ".vtr")
write_vtr(taxa, ft)

tbl(ft) |>
  mutate(parent_name = resolve(parent_id, id, name)) |>
  collect()
#>   id       name parent_id parent_name
#> 1  1   Fagaceae        NA        <NA>
#> 2  2    Quercus         1    Fagaceae
#> 3  3   Q. robur         2     Quercus
#> 4  4 Q. petraea         2     Quercus
```

Row 1 (Fagaceae) has `parent_id = NA`, so `parent_name` is NA. Row 3 (Q.
robur) has `parent_id = 2`, which matches `id = 2` (Quercus), so
`parent_name` is “Quercus”.

`propagate(parent_fk, pk, seed)` walks the tree upward from each row and
fills every node with the nearest non-NA ancestor value of `seed`.
Unlike `resolve()`, which does a single-level lookup, `propagate()`
traverses the full ancestor chain until it finds a row where the seed
expression is non-NA. This denormalizes a hierarchy in a single pass,
which would otherwise require a recursive CTE in SQL or repeated
self-joins in dplyr.

``` r

tbl(ft) |>
  mutate(family = propagate(
    parent_id, id,
    if_else(is.na(parent_id), name, NA_character_)
  )) |>
  collect()
#>   id       name parent_id   family
#> 1  1   Fagaceae        NA Fagaceae
#> 2  2    Quercus         1 Fagaceae
#> 3  3   Q. robur         2 Fagaceae
#> 4  4 Q. petraea         2 Fagaceae
```

The seed expression assigns “Fagaceae” to the root (the row with no
parent) and NA everywhere else. `propagate()` then walks each row’s
ancestry until it finds “Fagaceae” and fills it in. All four rows now
carry the family name.

## Format backends

vectra reads and writes several file formats beyond `.vtr`. Each backend
produces a `vectra_node` that plugs into the same verb pipeline, so you
can [`filter()`](https://gillescolling.com/vectra/reference/filter.md),
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md),
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md),
and
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md)
on CSV files, SQLite databases, Excel workbooks, and GeoTIFF rasters
using identical syntax. The key advantage is streaming: format
conversion pipelines (e.g., CSV to `.vtr`) process data batch-by-batch
without loading the entire file into memory.

[`tbl_csv()`](https://gillescolling.com/vectra/reference/tbl_csv.md)
scans a CSV file lazily. Column types (int64, double, bool, string) are
inferred from the first 1000 rows by probing each field for numeric,
boolean, and date patterns. Gzip-compressed files (`.csv.gz`) are
detected automatically by file extension and decompressed on the fly.
The `batch_size` parameter controls how many rows are read per batch
(default 65,536).

``` r

csv_in <- tempfile(fileext = ".csv")
write.csv(mtcars, csv_in, row.names = FALSE)

tbl_csv(csv_in) |>
  filter(cyl == 6) |>
  select(mpg, cyl, hp) |>
  collect()
#>    mpg cyl  hp
#> 1 21.0   6 110
#> 2 21.0   6 110
#> 3 21.4   6 110
#> 4 18.1   6 105
#> 5 19.2   6 123
#> 6 17.8   6 123
#> 7 19.7   6 175
```

[`tbl_sqlite()`](https://gillescolling.com/vectra/reference/tbl_sqlite.md)
scans a SQLite table. Column types are derived from the CREATE TABLE
schema. All filtering and aggregation happens in vectra’s C engine, not
in SQLite.

``` r

db <- tempfile(fileext = ".sqlite")
f_src <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f_src)
tbl(f_src) |> write_sqlite(db, "cars")

tbl_sqlite(db, "cars") |>
  filter(mpg > 25) |>
  collect()
#>    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> 1 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
#> 2 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
#> 3 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
#> 4 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1
#> 5 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2
#> 6 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2
```

[`tbl_xlsx()`](https://gillescolling.com/vectra/reference/tbl_xlsx.md)
reads an Excel sheet via the openxlsx2 package (a Suggests dependency,
not required for other backends). The sheet is read into memory as a
data.frame, then converted to a vectra node. This is not a streaming
operation (the full sheet is loaded), but it makes it possible to chain
vectra verbs on Excel data without writing intermediate files. The
`sheet` argument accepts a name or 1-based integer index.

[`tbl_tiff()`](https://gillescolling.com/vectra/reference/tbl_tiff.md)
reads GeoTIFF rasters. Each pixel becomes a row with `x`, `y`, and
`band1`, `band2`, … columns. Coordinates are pixel centers derived from
the affine geotransform stored in the TIFF metadata. NoData values
become NA. This enables spatial queries like
`filter(band1 > 25, x >= xmin, x <= xmax)` on rasters that are too large
to load as matrices. Results can be converted back to a raster with
`terra::rast(df, type = "xyz")`. The corresponding
[`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md)
function reconstructs the grid from x/y columns and writes a GeoTIFF
with optional DEFLATE compression.

Streaming format conversion is a common pattern. The entire pipeline
runs batch-by-batch, so converting a 20 GB CSV file to `.vtr` stays
within a few hundred MB of memory. Write functions
([`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md),
[`write_csv()`](https://gillescolling.com/vectra/reference/write_csv.md),
[`write_sqlite()`](https://gillescolling.com/vectra/reference/write_sqlite.md),
[`write_tiff()`](https://gillescolling.com/vectra/reference/write_tiff.md))
all accept `vectra_node` inputs and consume batches from the upstream
pipeline, writing each batch to the output format as it arrives.

``` r

csv_file <- tempfile(fileext = ".csv")
vtr_file <- tempfile(fileext = ".vtr")
csv_out <- tempfile(fileext = ".csv")

write.csv(mtcars, csv_file, row.names = FALSE)
tbl_csv(csv_file) |> write_vtr(vtr_file)

tbl(vtr_file) |>
  filter(cyl == 6) |>
  write_csv(csv_out)

read.csv(csv_out) |> head()
#>    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4
#> 3 21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1
#> 4 18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1
#> 5 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4
#> 6 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
```

For format-specific options, batch sizes, and performance
considerations, see
[`vignette("formats")`](https://gillescolling.com/vectra/articles/formats.md).

## Indexes

Hash indexes accelerate equality predicates on `.vtr` files. An index is
stored as a `.vtri` sidecar file alongside the `.vtr` file (named
`<path>.<column>.vtri` for single-column indexes). It maps key hashes to
row group indices using FNV-1a hashing, enabling O(1) row group
identification for `filter(col == value)` and `filter(col %in% values)`.
When [`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) opens
a `.vtr` file, it automatically detects and loads any `.vtri` sidecar
files present in the same directory. The scan node then consults the
index before reading data, skipping row groups that cannot contain
matching keys. This composes with zone-map pruning: the index narrows
down candidate row groups, and zone-map statistics further eliminate row
groups where the column’s min/max range excludes the filter value.

Create an index with
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md).
Check for an existing index with
[`has_index()`](https://gillescolling.com/vectra/reference/has_index.md).
Indexes persist on disk and survive R sessions.

``` r

f_idx <- tempfile(fileext = ".vtr")
write_vtr(
  data.frame(id = letters, val = 1:26, stringsAsFactors = FALSE),
  f_idx,
  batch_size = 5
)

has_index(f_idx, "id")  # FALSE
#> [1] FALSE
create_index(f_idx, "id")
has_index(f_idx, "id")  # TRUE
#> [1] TRUE
```

With 6 row groups of 5 rows each, a filter on `id` now skips row groups
that cannot contain the target value.

``` r

tbl(f_idx) |>
  filter(id == "m") |>
  collect()
#>   id val
#> 1  m  13
```

Composite indexes cover AND-combined equality predicates on multiple
columns. Pass a character vector to
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md).

``` r

f_comp <- tempfile(fileext = ".vtr")
write_vtr(
  data.frame(
    region = rep(c("north", "south"), each = 13),
    id = letters,
    val = 1:26,
    stringsAsFactors = FALSE
  ),
  f_comp,
  batch_size = 5
)
create_index(f_comp, c("region", "id"))

tbl(f_comp) |>
  filter(region == "north", id == "c") |>
  collect()
#>   region id val
#> 1  north  c   3
```

The `ci` argument to
[`create_index()`](https://gillescolling.com/vectra/reference/create_index.md)
builds a case-insensitive index by lowercasing all key values before
hashing. This is useful for string columns with inconsistent casing
(e.g., species names entered by different data providers). When a
case-insensitive index is present, `filter(col == "Quercus")` will match
“quercus”, “QUERCUS”, and any other casing variant.

Index pushdown composes with binary search on sorted columns and
zone-map pruning. A well-indexed `.vtr` file with many small row groups
can answer point queries over hundreds of millions of rows by reading a
single row group.
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
shows whether an index was used and how many row groups were pruned. For
details on index internals, composite index hash combining, and tuning
batch size for optimal index selectivity, see
[`vignette("indexing")`](https://gillescolling.com/vectra/articles/indexing.md).

## Incremental operations

Three functions support incremental workflows on `.vtr` files without
full rewrites. These are designed for append-heavy pipelines where new
data arrives in batches (daily sensor readings, transaction logs,
biodiversity occurrence records) and rewriting the entire file on each
update would be prohibitively slow.

[`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
adds new rows as additional row groups at the end of an existing file.
The schema of the new data must match the existing file exactly: same
column names, same types, same order. Existing row groups are untouched;
the function writes the new row groups after the last existing one and
patches the header to update the row group count. This is not fully
atomic: if the process is interrupted between writing the row groups and
patching the header, the file may be corrupted. For write-once workloads
where atomicity matters, use
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
(which writes to a temp file and renames).

``` r

fa <- tempfile(fileext = ".vtr")
write_vtr(mtcars[1:16, ], fa)
append_vtr(mtcars[17:32, ], fa)
tbl(fa) |> collect() |> nrow()
#> [1] 32
```

[`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
marks rows as deleted by writing a tombstone sidecar file
(`<path>.del`). The `.vtr` file itself is never modified. The next
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md) call
automatically excludes tombstoned rows. Tombstones are cumulative across
multiple
[`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
calls.

``` r

delete_vtr(fa, c(0, 1, 2))  # 0-based row indices
tbl(fa) |> collect() |> nrow()
#> [1] 29
unlink(c(fa, paste0(fa, ".del")))
```

[`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md)
computes a key-based logical diff between two snapshots of the same
dataset. It returns a list with `$deleted` (a vector of key values
present in the old file but not the new) and `$added` (a lazy
`vectra_node` of rows present in the new file but not the old). This is
a set-level difference based on key identity, not a binary file diff or
row-level comparison. Rows with the same key that have changed values
appear in both `$deleted` and `$added`, which makes it possible to
detect updates by intersecting the two sets. The implementation streams
both files: pass 1 builds a hash set from the old file’s key column,
pass 2 streams all columns from the new file and writes added rows to a
temporary `.vtr` file. The temporary file is cleaned up automatically
when the returned node is garbage-collected.

``` r

fd1 <- tempfile(fileext = ".vtr")
fd2 <- tempfile(fileext = ".vtr")
old <- data.frame(id = 1:5, val = letters[1:5],
                  stringsAsFactors = FALSE)
new <- data.frame(id = c(3L, 4L, 5L, 6L, 7L),
                  val = c("C", "d", "e", "f", "g"),
                  stringsAsFactors = FALSE)
write_vtr(old, fd1)
write_vtr(new, fd2)

d <- diff_vtr(fd1, fd2, "id")
d$deleted
#> [1] 1 2
collect(d$added)
#>   id val
#> 1  6   f
#> 2  7   g
unlink(c(fd1, fd2))
```

Keys 1 and 2 were deleted. Keys 6 and 7 were added. Key 3 appears with a
changed value (“c” to “C”), which shows up in both deleted and added.
For incremental ETL patterns and large-data workflows, see
[`vignette("large-data")`](https://gillescolling.com/vectra/articles/large-data.md).

## Materialized blocks

A `vectra_node` is single-use: once
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) or
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
consumes it, the C-level external pointer is invalidated and the plan
cannot be reused. This is a consequence of the pull-based execution
model, where each node’s `next_batch()` advances internal state that
cannot be rewound. For repeated lookups against the same dataset (e.g.,
matching a stream of queries against a reference table),
[`materialize()`](https://gillescolling.com/vectra/reference/materialize.md)
pulls all batches and stores the result as a persistent columnar block
in memory. The block lives until it is garbage-collected or the R
session ends.

``` r

blk_data <- data.frame(
  taxonID = c("T1", "T2", "T3", "T4", "T5"),
  name = c("Quercus robur", "Pinus sylvestris",
           "Fagus sylvatica", "Acer campestre",
           "Betula pendula"),
  stringsAsFactors = FALSE
)
f_blk <- tempfile(fileext = ".vtr")
write_vtr(blk_data, f_blk)

blk <- materialize(tbl(f_blk))
blk
#> vectra_block [materialized in memory]
```

[`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
performs a hash-based exact lookup on a string column of the
materialized block. The hash index is built lazily on the first call to
[`block_lookup()`](https://gillescolling.com/vectra/reference/block_lookup.md)
for a given column and cached inside the block for all subsequent calls.
This means the first lookup pays a one-time O(n) indexing cost and all
subsequent lookups are O(k) where k is the number of query keys. The
`ci` argument enables case-insensitive matching by lowercasing keys
before hashing.

``` r

block_lookup(blk, "name", c("Quercus robur", "Betula pendula"))
#>   query_idx taxonID           name
#> 1         1      T1  Quercus robur
#> 2         2      T5 Betula pendula
```

[`block_fuzzy_lookup()`](https://gillescolling.com/vectra/reference/block_fuzzy_lookup.md)
computes string distances between query keys and a column in the block,
returning all pairs within the `max_dist` threshold. It supports
Damerau-Levenshtein (`"dl"`), Levenshtein (`"levenshtein"`), and
Jaro-Winkler (`"jw"`). The `block_col` and `block_keys` arguments enable
exact-match blocking: only rows where the blocking column matches the
corresponding blocking key are compared, which can reduce the number of
distance computations by orders of magnitude. For example, blocking on
genus before fuzzy-matching species names restricts comparisons to rows
within the same genus. The distance computation is OpenMP-parallelized
via the `n_threads` argument.

``` r

block_fuzzy_lookup(
  blk, "name",
  c("Qurecus robur", "Pinus silvestris"),
  method = "dl",
  max_dist = 0.2
)
#>   query_idx fuzzy_dist taxonID             name
#> 1         1 0.07692308      T1    Quercus robur
#> 2         2 0.06250000      T2 Pinus sylvestris
```

The `query_idx` column (1-based) maps each result row back to the input
query vector. The `fuzzy_dist` column gives the normalized distance for
each match.

## Inspecting the plan

Every vectra pipeline builds a lazy execution plan as a tree of nodes.
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
prints this tree before any data moves, which makes it the primary
debugging tool for understanding what the engine will do when you call
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).
The output shows node types (ScanNode, FilterNode, ProjectNode,
SortNode, GroupAggNode, WindowNode, JoinNode, etc.), their parent-child
relationships, and the output column schema at each level.

``` r

tbl(f) |>
  filter(cyl > 4) |>
  select(mpg, cyl, hp) |>
  arrange(desc(mpg)) |>
  explain()
#> vectra execution plan
#> 
#> SortNode [materializes] 
#>   ProjectNode [streaming] 
#>     FilterNode [streaming] 
#>       ScanNode [streaming, 3/11 cols (pruned), predicate pushdown, tdc stats] 
#> 
#> Output columns (3):
#>   mpg <double>
#>   cyl <double>
#>   hp <double>
#> 
#> <offload grade: streaming scan>
#>   passes over data : 1 per consumption (lazy)
#>   peak memory      : O(one batch)
#>   I/O cost         : O(n) per pass
#>   note             : plain query node; re-reading re-runs the upstream pipeline
```

Several things to look for in the plan:

**Streaming vs. materializing.** FilterNode and ProjectNode are
streaming: they process one batch at a time in constant memory.
SortNode, GroupAggNode, and WindowNode are materializing: they consume
all input before producing output. A plan with only streaming nodes
above the scan will use memory proportional to one batch, regardless of
file size.

**Column pruning.** ProjectNode appears when
[`select()`](https://gillescolling.com/vectra/reference/select.md) or
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) is
used. It lists only the columns that pass through. Columns not listed
are never decoded from disk in the scan node, saving I/O and
decompression work.

**Predicate pushdown.** When a hash index exists on the filter column,
the ScanNode consults the `.vtri` index before reading row groups.
[`explain()`](https://gillescolling.com/vectra/reference/explain.md)
shows the index file path when pushdown is active. Zone-map pruning
(based on per-row-group min/max statistics) applies to all columns
automatically, with or without an explicit index. Combining both
mechanisms gives the fastest possible scan: the index identifies
candidate row groups by key hash, and zone-map statistics further narrow
the set.

**Node order.** The plan is printed bottom-up (leaf to root). The
ScanNode is always at the bottom. Reading the plan from bottom to top
traces the path data takes through the pipeline: scan from disk, filter
rows, project columns, sort, limit.

[`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md)
provides a quick preview of column types and a sample of values without
collecting the full result. It reads enough data to show one line per
column.

``` r

tbl(f) |> glimpse()
#> vectra lazy table [? x 11]
#> $ mpg             <NA> 21.0, 21.0, 22.8, 21.4, 18.7
#> $ cyl             <NA> 6, 6, 4, 6, 8
#> $ disp            <NA> 160, 160, 108, 258, 360
#> $ hp              <NA> 110, 110, 93, 110, 175
#> $ drat            <NA> 3.90, 3.90, 3.85, 3.08, 3.15
#> $ wt              <NA> 2.620, 2.875, 2.320, 3.215, 3.440
#> $ qsec            <NA> 16.46, 17.02, 18.61, 19.44, 17.02
#> $ vs              <NA> 0, 0, 1, 1, 0
#> $ am              <NA> 1, 1, 1, 0, 0
#> $ gear            <NA> 4, 4, 4, 3, 3
#> $ carb            <NA> 4, 4, 1, 1, 2
```

## Where to go next

This vignette is the fast path through the engine. Each area has a
dedicated vignette with full coverage:

- [Out-of-core
  GIS](https://gillescolling.com/vectra/articles/spatial.md) for the
  streamed spatial toolbox (filter, clip, rasterize, zonal, focal,
  terrain, warp, partitioned spatial join).

- [The execution
  engine](https://gillescolling.com/vectra/articles/engine.md) for the
  pull-based plan in depth.

- [Joins](https://gillescolling.com/vectra/articles/joins.md), [string
  operations](https://gillescolling.com/vectra/articles/string-ops.md),
  [indexing](https://gillescolling.com/vectra/articles/indexing.md), and
  [larger-than-RAM
  strategy](https://gillescolling.com/vectra/articles/large-data.md) for
  the verb families this tour samples.

- The function reference for the full API.

## Cleanup

``` r

unlink(c(f, f_batched, f_na, fs, fs2, f_cyl, f_rat, f_ref, f_query, fd,
         ft, csv_in, csv_out, csv_file, vtr_file, db, f_src, f_idx,
         paste0(f_idx, ".id.vtri"), f_comp,
         paste0(f_comp, ".region_id.vtri"), f_blk))
```
