# Getting Started with vectra

vectra is a columnar query engine for R that handles larger-than-RAM
datasets with familiar dplyr-style verbs. All operations are lazy –
nothing runs until you call
[`collect()`](https://gillescolling.com/vectra/reference/collect.md).

## Installation

Install the released version from CRAN:

``` r

install.packages("vectra")
```

Or the development version from GitHub:

``` r

# install.packages("devtools")
devtools::install_github("gcol33/vectra")
```

## Writing and reading data

vectra uses its own binary columnar format (`.vtr`) for fast,
memory-efficient storage. Write any data.frame with
[`write_vtr()`](https://gillescolling.com/vectra/reference/write_vtr.md)
and open it lazily with
[`tbl()`](https://gillescolling.com/vectra/reference/tbl.md):

``` r

library(vectra)
#> 
#> Attaching package: 'vectra'
#> The following object is masked from 'package:stats':
#> 
#>     filter

f <- tempfile(fileext = ".vtr")
write_vtr(mtcars, f)

# tbl() returns a lazy reference -- no data is loaded yet
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

Call
[`collect()`](https://gillescolling.com/vectra/reference/collect.md) to
materialize the result as a data.frame:

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

## Filtering and selecting

Use [`filter()`](https://gillescolling.com/vectra/reference/filter.md)
to keep rows matching a condition, and
[`select()`](https://gillescolling.com/vectra/reference/select.md) to
pick columns. Tidyselect helpers like `starts_with()` and `everything()`
work inside
[`select()`](https://gillescolling.com/vectra/reference/select.md).

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

Multiple conditions in
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) are
combined with AND. Use `|` for OR:

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

Use `%in%` for set membership and `if_else()` inside
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) for
conditional values:

``` r

tbl(f) |>
  filter(cyl %in% c(4, 8)) |>
  mutate(size = if_else(cyl > 6, "large", "small")) |>
  select(mpg, cyl, size) |>
  collect() |>
  head()
#>    mpg cyl  size
#> 1 22.8   4 small
#> 2 18.7   8 large
#> 3 14.3   8 large
#> 4 24.4   4 small
#> 5 22.8   4 small
#> 6 16.4   8 large
```

## Transforming columns

[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) adds
or replaces columns. Arithmetic, comparison, and boolean operators all
work:

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

[`transmute()`](https://gillescolling.com/vectra/reference/transmute.md)
works like
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md) but
keeps only the new columns:

``` r

tbl(f) |>
  transmute(efficiency = mpg / wt, power_ratio = hp / disp) |>
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

For string operations, write a small dataset with character columns:

``` r

people <- data.frame(
  name = c("Alice", "Bob", "Charlie", "Diana"),
  city = c("Amsterdam", "Berlin", "Chicago", "Dublin"),
  stringsAsFactors = FALSE
)
fs <- tempfile(fileext = ".vtr")
write_vtr(people, fs)

tbl(fs) |>
  mutate(
    name_len = nchar(name),
    short_city = substr(city, 1, 3),
    has_a = grepl("a", city)
  ) |>
  collect()
#>      name      city name_len short_city has_a
#> 1   Alice Amsterdam        5        Ams  TRUE
#> 2     Bob    Berlin        3        Ber FALSE
#> 3 Charlie   Chicago        7        Chi  TRUE
#> 4   Diana    Dublin        5        Dub FALSE
```

## Aggregation

Group data with
[`group_by()`](https://gillescolling.com/vectra/reference/group_by.md)
and compute summaries with
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md).
Supported aggregation functions: `n()`,
[`sum()`](https://rdrr.io/r/base/sum.html),
[`mean()`](https://rdrr.io/r/base/mean.html),
[`min()`](https://rdrr.io/r/base/Extremes.html),
[`max()`](https://rdrr.io/r/base/Extremes.html),
[`sd()`](https://rdrr.io/r/stats/sd.html),
[`var()`](https://rdrr.io/r/stats/cor.html), `first()`, `last()`,
[`any()`](https://rdrr.io/r/base/any.html),
[`all()`](https://rdrr.io/r/base/all.html),
[`median()`](https://rdrr.io/r/stats/median.html), `n_distinct()`.

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

For quick counts, use
[`count()`](https://gillescolling.com/vectra/reference/count.md) or
[`tally()`](https://gillescolling.com/vectra/reference/count.md):

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

Use [`sd()`](https://rdrr.io/r/stats/sd.html),
[`var()`](https://rdrr.io/r/stats/cor.html), `first()`, and `last()` for
more detailed summaries:

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

## Sorting and slicing

[`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
sorts rows. Wrap column names in
[`desc()`](https://gillescolling.com/vectra/reference/desc.md) for
descending order:

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

[`slice_head()`](https://gillescolling.com/vectra/reference/slice_head.md)
returns the first n rows.
[`slice_min()`](https://gillescolling.com/vectra/reference/slice_head.md)
and
[`slice_max()`](https://gillescolling.com/vectra/reference/slice_head.md)
use an optimized top-N algorithm to avoid a full sort:

``` r

# First 5 rows
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

``` r

# 3 cars with lowest mpg
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
include ties. Use `with_ties = FALSE` for exactly `n` rows:

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

## Joins

Join two vectra tables with
[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`right_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`full_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md),
or
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md).

``` r

# Create a lookup table
cyl_info <- data.frame(
  cyl = c(4, 6, 8),
  engine_type = c("inline", "v-type", "v-type"),
  stringsAsFactors = FALSE
)
f_cyl <- tempfile(fileext = ".vtr")
write_vtr(cyl_info, f_cyl)

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

For joins on differently named columns, use a named vector:

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

## Window functions

Window functions operate row-by-row within
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md).
vectra supports `row_number()`,
[`rank()`](https://rdrr.io/r/base/rank.html), `dense_rank()`,
[`lag()`](https://rdrr.io/r/stats/lag.html), `lead()`,
[`cumsum()`](https://rdrr.io/r/base/cumsum.html), `cummean()`,
[`cummin()`](https://rdrr.io/r/base/cumsum.html), and
[`cummax()`](https://rdrr.io/r/base/cumsum.html).

``` r

tbl(f) |>
  select(mpg, cyl, hp) |>
  slice_head(n = 8) |>
  mutate(
    rn = row_number(),
    prev_mpg = lag(mpg),
    next_mpg = lead(mpg),
    running_total = cumsum(hp)
  ) |>
  collect()
#>    mpg cyl  hp rn prev_mpg next_mpg running_total
#> 1 21.0   6 110  1       NA     21.0           110
#> 2 21.0   6 110  2     21.0     22.8           220
#> 3 22.8   4  93  3     21.0     21.4           313
#> 4 21.4   6 110  4     22.8     18.7           423
#> 5 18.7   8 175  5     21.4     18.1           598
#> 6 18.1   6 105  6     18.7     14.3           703
#> 7 14.3   8 245  7     18.1     24.4           948
#> 8 24.4   4  62  8     14.3       NA          1010
```

[`rank()`](https://rdrr.io/r/base/rank.html) and `dense_rank()` take a
column argument:

``` r

tbl(f) |>
  select(mpg, cyl) |>
  slice_head(n = 8) |>
  mutate(mpg_rank = rank(mpg)) |>
  collect()
#>    mpg cyl mpg_rank
#> 1 21.0   6        4
#> 2 21.0   6        4
#> 3 22.8   4        7
#> 4 21.4   6        6
#> 5 18.7   8        3
#> 6 18.1   6        2
#> 7 14.3   8        1
#> 8 24.4   4        8
```

## Working with dates

Date columns roundtrip through vectra. Extract components with `year()`,
`month()`, and `day()`:

``` r

dates_df <- data.frame(
  event_date = as.Date(c("2020-03-15", "2020-07-01", "2021-01-15", "2021-06-30")),
  value = c(10, 20, 30, 40)
)
fd <- tempfile(fileext = ".vtr")
write_vtr(dates_df, fd)

tbl(fd) |>
  mutate(yr = year(event_date), mo = month(event_date)) |>
  group_by(yr) |>
  summarise(total = sum(value)) |>
  collect()
#>     yr total
#> 1 2020    30
#> 2 2021    70
```

Filter by date using [`as.Date()`](https://rdrr.io/r/base/as.Date.html):

``` r

tbl(fd) |>
  filter(event_date >= as.Date("2021-01-01")) |>
  collect()
#>   event_date value
#> 1 2021-01-15    30
#> 2 2021-06-30    40
```

## String similarity

vectra implements fuzzy string matching directly in the C engine.
`levenshtein()` counts the minimum edit distance between two strings;
`jaro_winkler()` returns a similarity score from 0 to 1 (higher = more
similar). Both work in
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) and
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md):

``` r

species <- data.frame(
  name = c("Quercus robur", "Quercus rubra", "Fagus sylvatica",
           "Acer platanoides", "Quercus petraea"),
  stringsAsFactors = FALSE
)
fs2 <- tempfile(fileext = ".vtr")
write_vtr(species, fs2)

# Find names within edit distance 3 of "Quercus robur"
tbl(fs2) |>
  mutate(dist = levenshtein(name, "Quercus robur"),
         sim  = jaro_winkler(name, "Quercus robur")) |>
  filter(dist <= 5) |>
  arrange(dist) |>
  collect()
#>            name dist       sim
#> 1 Quercus robur    0 1.0000000
#> 2 Quercus rubra    3 0.9525641
```

`dl_dist()` counts transpositions as a single operation
(Damerau-Levenshtein), useful for catching common typos:

``` r

tbl(fs2) |>
  mutate(d = dl_dist(name, "Qurecus robur")) |>  # transposed 'r' and 'e'
  collect()
#>               name  d
#> 1    Quercus robur  1
#> 2    Quercus rubra  4
#> 3  Fagus sylvatica 14
#> 4 Acer platanoides 14
#> 5  Quercus petraea  8
```

## Tree traversal: resolve() and propagate()

`resolve(fk, pk, value)` is a scalar self-join — it looks up `value`
where `pk == fk` within the same batch, denormalising a parent-child
table without an explicit join:

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

`propagate(parent_id, id, seed)` walks the tree and fills every node
with the nearest non-NA ancestor value of `seed`:

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

## Incremental file operations

[`append_vtr()`](https://gillescolling.com/vectra/reference/append_vtr.md)
adds new rows as a row group without rewriting existing data:

``` r

fa <- tempfile(fileext = ".vtr")
write_vtr(mtcars[1:16, ], fa)
append_vtr(mtcars[17:32, ], fa)
tbl(fa) |> collect() |> nrow()   # 32
#> [1] 32
```

[`delete_vtr()`](https://gillescolling.com/vectra/reference/delete_vtr.md)
marks rows as deleted via a tombstone side file — the `.vtr` file is
never modified:

``` r

delete_vtr(fa, c(0, 1, 2))   # delete first 3 rows (0-based)
tbl(fa) |> collect() |> nrow()   # 29
#> [1] 29
unlink(c(fa, paste0(fa, ".del")))
```

[`diff_vtr()`](https://gillescolling.com/vectra/reference/diff_vtr.md)
computes a key-based logical diff between two snapshots:

``` r

fd1 <- tempfile(fileext = ".vtr")
fd2 <- tempfile(fileext = ".vtr")
old <- data.frame(id = 1:5, val = letters[1:5], stringsAsFactors = FALSE)
new <- data.frame(id = c(3L, 4L, 5L, 6L, 7L),
                  val = c("C", "d", "e", "f", "g"),
                  stringsAsFactors = FALSE)
write_vtr(old, fd1)
write_vtr(new, fd2)

d <- diff_vtr(fd1, fd2, "id")
d$deleted          # key values removed
#> [1] 1 2
collect(d$added)   # new rows
#>   id val
#> 1  6   f
#> 2  7   g
unlink(c(fd1, fd2))
```

## Format conversion

vectra can read and write CSV files, making it easy to convert between
formats. The entire pipeline streams batch-by-batch without loading all
data into memory:

``` r

# CSV -> VTR
csv_in <- tempfile(fileext = ".csv")
write.csv(mtcars, csv_in, row.names = FALSE)

vtr_file <- tempfile(fileext = ".vtr")
tbl_csv(csv_in) |> write_vtr(vtr_file)

# VTR -> CSV (with a filter applied)
csv_out <- tempfile(fileext = ".csv")
tbl(vtr_file) |>
  filter(cyl == 6) |>
  write_csv(csv_out)

# Verify the round-trip
read.csv(csv_out) |> head()
#>    mpg cyl  disp  hp drat    wt  qsec vs am gear carb
#> 1 21.0   6 160.0 110 3.90 2.620 16.46  0  1    4    4
#> 2 21.0   6 160.0 110 3.90 2.875 17.02  0  1    4    4
#> 3 21.4   6 258.0 110 3.08 3.215 19.44  1  0    3    1
#> 4 18.1   6 225.0 105 2.76 3.460 20.22  1  0    3    1
#> 5 19.2   6 167.6 123 3.92 3.440 18.30  1  0    4    4
#> 6 17.8   6 167.6 123 3.92 3.440 18.90  1  0    4    4
```

## Inspecting the plan

Every vectra pipeline builds a lazy execution plan. Use
[`explain()`](https://gillescolling.com/vectra/reference/explain.md) to
print the plan tree before collecting, which is helpful for debugging:

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
#>       ScanNode [streaming, 3/11 cols (pruned), predicate pushdown, v3 stats] 
#> 
#> Output columns (3):
#>   mpg <double>
#>   cyl <double>
#>   hp <double>
```

## Previewing data

Use [`glimpse()`](https://gillescolling.com/vectra/reference/glimpse.md)
to see column types and a preview without collecting everything:

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

## Cleanup

``` r

unlink(c(f, fs, fs2, f_cyl, f_rat, fd, ft, csv_in, csv_out, vtr_file))
```
