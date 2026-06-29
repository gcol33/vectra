# Joins

## Introduction

Most analytical workflows combine data from multiple sources.
Observations arrive in one file, site metadata lives in another, and a
taxonomic reference list sits in a third. Joins are the operation that
stitches them together.

vectra implements joins as C-level hash join nodes. Like every other
verb, a join call builds a plan node; nothing executes until
[`collect()`](https://gillescolling.com/vectra/reference/collect.md)
pulls batches through the tree. The engine follows a **build-right,
probe-left** model. When execution starts, the right-side table is fully
materialized into a hash table in memory. The left side then streams
through batch by batch, probing the hash table for matches. This
asymmetry has a practical consequence: the left side can be arbitrarily
large (it never lives in memory all at once), but the right side must
fit. So we put the smaller table on the right.

vectra provides seven join verbs:
[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`right_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`full_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md),
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md),
and
[`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md).
For approximate string matching there is also
[`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md).
All are S3 methods on `vectra_node`, share the same `by` syntax for key
specification, and compose freely with other verbs in a pipeline.

We will work with ecological data throughout: species observations, site
metadata, and a taxonomic reference table. Let us set those up.

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

obs_path <- tempfile(fileext = ".vtr")
sites_path <- tempfile(fileext = ".vtr")
ref_path <- tempfile(fileext = ".vtr")

observations <- data.frame(
  site_id = c(1, 1, 2, 2, 3, 3, 4, 4),
  species = c("Quercus robur", "Pinus sylvestris",
              "Quercus robur", "Fagus sylvatica",
              "Pinus sylvestris", "Betula pendula",
              "Fagus sylvatica", "Acer pseudoplatanus"),
  date = c("2024-06-01", "2024-06-01", "2024-06-15",
           "2024-06-15", "2024-07-01", "2024-07-01",
           "2024-07-10", "2024-07-10"),
  count = c(12, 5, 8, 3, 7, 14, 6, 2),
  stringsAsFactors = FALSE
)

sites <- data.frame(
  site_id = c(1, 2, 3, 5),
  latitude = c(48.21, 47.07, 46.62, 48.85),
  longitude = c(16.37, 15.44, 14.31, 13.02),
  habitat = c("forest", "wetland", "grassland", "forest"),
  stringsAsFactors = FALSE
)

reference <- data.frame(
  species_name = c("Quercus robur", "Pinus sylvestris",
                   "Fagus sylvatica", "Betula pendula",
                   "Alnus glutinosa"),
  family = c("Fagaceae", "Pinaceae", "Fagaceae",
             "Betulaceae", "Betulaceae"),
  conservation_status = c("LC", "LC", "LC", "LC", "LC"),
  stringsAsFactors = FALSE
)

write_vtr(observations, obs_path)
write_vtr(sites, sites_path)
write_vtr(reference, ref_path)
```

## Key specification

Every join except
[`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md)
takes a `by` argument that defines which columns to match on. There are
four ways to specify keys.

**Same-name column.** The simplest case: both tables share a column
name, and we pass it as a string.

``` r

left_join(tbl(obs_path), tbl(sites_path), by = "site_id") |>
  collect()
#>   site_id             species       date count latitude longitude   habitat
#> 1       1       Quercus robur 2024-06-01    12    48.21     16.37    forest
#> 2       1    Pinus sylvestris 2024-06-01     5    48.21     16.37    forest
#> 3       2       Quercus robur 2024-06-15     8    47.07     15.44   wetland
#> 4       2     Fagus sylvatica 2024-06-15     3    47.07     15.44   wetland
#> 5       3    Pinus sylvestris 2024-07-01     7    46.62     14.31 grassland
#> 6       3      Betula pendula 2024-07-01    14    46.62     14.31 grassland
#> 7       4     Fagus sylvatica 2024-07-10     6       NA        NA      <NA>
#> 8       4 Acer pseudoplatanus 2024-07-10     2       NA        NA      <NA>
```

The result contains all columns from both tables. The key column
`site_id` appears once; the remaining columns from each side sit beside
it.

**Different-name columns.** When the key has a different name in each
table, we use a named character vector. The name is the left-side
column; the value is the right-side column.

``` r

inner_join(
  tbl(obs_path),
  tbl(ref_path),
  by = c("species" = "species_name")
) |> collect()
#>   site_id          species       date count     family conservation_status
#> 1       1    Quercus robur 2024-06-01    12   Fagaceae                  LC
#> 2       1 Pinus sylvestris 2024-06-01     5   Pinaceae                  LC
#> 3       2    Quercus robur 2024-06-15     8   Fagaceae                  LC
#> 4       2  Fagus sylvatica 2024-06-15     3   Fagaceae                  LC
#> 5       3 Pinus sylvestris 2024-07-01     7   Pinaceae                  LC
#> 6       3   Betula pendula 2024-07-01    14 Betulaceae                  LC
#> 7       4  Fagus sylvatica 2024-07-10     6   Fagaceae                  LC
```

Here `species` in the observations table maps to `species_name` in the
reference table. The output keeps the left-side name (`species`).

**Natural join.** When `by` is `NULL` (the default), vectra identifies
all columns that appear in both tables and joins on all of them. A
message tells us which columns were selected.

``` r

left_join(tbl(obs_path), tbl(sites_path)) |>
  collect()
#> Joining by: site_id
#>   site_id             species       date count latitude longitude   habitat
#> 1       1       Quercus robur 2024-06-01    12    48.21     16.37    forest
#> 2       1    Pinus sylvestris 2024-06-01     5    48.21     16.37    forest
#> 3       2       Quercus robur 2024-06-15     8    47.07     15.44   wetland
#> 4       2     Fagus sylvatica 2024-06-15     3    47.07     15.44   wetland
#> 5       3    Pinus sylvestris 2024-07-01     7    46.62     14.31 grassland
#> 6       3      Betula pendula 2024-07-01    14    46.62     14.31 grassland
#> 7       4     Fagus sylvatica 2024-07-10     6       NA        NA      <NA>
#> 8       4 Acer pseudoplatanus 2024-07-10     2       NA        NA      <NA>
```

Natural joins are convenient for exploratory work. For production
pipelines, explicit keys are safer because they survive schema changes
in upstream tables.

**Multi-column keys.** We pass multiple column names as a character
vector. This defines a composite key where rows must match on every
column simultaneously.

``` r

survey1_path <- tempfile(fileext = ".vtr")
survey2_path <- tempfile(fileext = ".vtr")

write_vtr(data.frame(
  site_id = c(1, 1, 2, 2),
  year = c(2023, 2024, 2023, 2024),
  richness = c(5, 7, 3, 4)
), survey1_path)

write_vtr(data.frame(
  site_id = c(1, 2, 2),
  year = c(2024, 2023, 2024),
  temperature = c(18.2, 16.1, 17.5)
), survey2_path)

inner_join(
  tbl(survey1_path), tbl(survey2_path),
  by = c("site_id", "year")
) |> collect()
#>   site_id year richness temperature
#> 1       1 2024        7        18.2
#> 2       2 2023        3        16.1
#> 3       2 2024        4        17.5
```

Only the three rows where both `site_id` and `year` match appear. Site 1
in 2023 had a richness measurement but no temperature record, so it
drops from the inner join. Multi-column keys can also use the named form
when column names differ: `by = c("site" = "site_id", "yr" = "year")`.

## Left join

[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps every row from the left table. Where a match exists in the right
table, the right-side columns are filled in. Where no match exists, they
become `NA`.

This is the workhorse join for data enrichment. We have a large
observation table and want to attach site coordinates to each row.

``` r

enriched <- left_join(
  tbl(obs_path), tbl(sites_path), by = "site_id"
) |> collect()
enriched
#>   site_id             species       date count latitude longitude   habitat
#> 1       1       Quercus robur 2024-06-01    12    48.21     16.37    forest
#> 2       1    Pinus sylvestris 2024-06-01     5    48.21     16.37    forest
#> 3       2       Quercus robur 2024-06-15     8    47.07     15.44   wetland
#> 4       2     Fagus sylvatica 2024-06-15     3    47.07     15.44   wetland
#> 5       3    Pinus sylvestris 2024-07-01     7    46.62     14.31 grassland
#> 6       3      Betula pendula 2024-07-01    14    46.62     14.31 grassland
#> 7       4     Fagus sylvatica 2024-07-10     6       NA        NA      <NA>
#> 8       4 Acer pseudoplatanus 2024-07-10     2       NA        NA      <NA>
```

Site 4 has observations but does not appear in the sites table. Its
`latitude`, `longitude`, and `habitat` columns are `NA`. Site 5 is in
the sites table but has no observations; it does not appear at all
because left join preserves the left side, not the right.

Internally, the engine builds a hash table from the right-side table
(sites) during the build phase. Every row in sites is hashed on
`site_id` and stored with its payload columns (`latitude`, `longitude`,
`habitat`). Then the left side (observations) streams through one batch
at a time. For each left row, the engine computes the hash of `site_id`,
probes the hash table, and either copies the matching right-side columns
into the output or fills them with `NA` if the probe misses. The left
row is emitted regardless. This is why left join guarantees that the
output has at least as many rows as the left table: every left row
produces exactly one output row per match, or one NA-padded row if there
is no match.

Left join is the natural default when we have a primary dataset and want
to enrich it with attributes from a secondary source. Inner join, by
contrast, drops every row that lacks a match on the right side, which
can silently shrink the result. In our example, inner join would discard
both observations at site 4. Whether that is acceptable depends on the
analysis. If we plan to model species richness as a function of habitat,
losing site 4 might be fine because we cannot use those rows without
habitat data anyway. But if we are computing total species counts across
all sites, dropping site 4 would undercount. Left join keeps the data
intact and lets us decide later what to do with the gaps.

Those `NA` values in the output carry meaning: they tell us which
observations lack metadata. We can filter them out, impute them, or flag
them for data collection. A common pattern is to follow a left join with
`filter(!is.na(habitat))` when we want inner-join semantics but also
want to inspect the unmatched rows first. Running the left join,
checking the `NA` rate, and then filtering is a safer workflow than
jumping straight to inner join because it makes the data loss visible.

The row count of a left join is always at least the row count of the
left table. It can be larger if the right table has duplicate keys. If
site_id 1 appeared twice in the sites table, each observation at site 1
would produce two output rows. A one-to-one key relationship keeps the
row count equal; a one-to-many relationship on the right side inflates
it. Checking `nrow(result) == nrow(left)` after a left join is a quick
way to confirm the key relationship is one-to-one, which is often what
we assume but rarely verify.

**Suffix for duplicate column names.** When both tables share a non-key
column name, vectra appends suffixes to disambiguate. The default is
`c(".x", ".y")`.

``` r

extra_path <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  site_id = c(1, 2, 3),
  count = c(100, 200, 300)
), extra_path)

left_join(
  tbl(obs_path), tbl(extra_path),
  by = "site_id", suffix = c("_obs", "_site")
) |> collect()
#>   site_id             species       date count_obs count_site
#> 1       1       Quercus robur 2024-06-01        12        100
#> 2       1    Pinus sylvestris 2024-06-01         5        100
#> 3       2       Quercus robur 2024-06-15         8        200
#> 4       2     Fagus sylvatica 2024-06-15         3        200
#> 5       3    Pinus sylvestris 2024-07-01         7        300
#> 6       3      Betula pendula 2024-07-01        14        300
#> 7       4     Fagus sylvatica 2024-07-10         6         NA
#> 8       4 Acer pseudoplatanus 2024-07-10         2         NA
```

Both tables have a `count` column. The left side becomes `count_obs`,
the right side `count_site`. Choosing descriptive suffixes avoids the
generic `.x`/`.y` that can be hard to interpret in downstream code.

## Inner join

[`inner_join()`](https://gillescolling.com/vectra/reference/left_join.md)
returns only the rows that match on both sides. It is the intersection
of the two key sets.

``` r

inner_join(
  tbl(obs_path), tbl(sites_path), by = "site_id"
) |> collect()
#>   site_id          species       date count latitude longitude   habitat
#> 1       1    Quercus robur 2024-06-01    12    48.21     16.37    forest
#> 2       1 Pinus sylvestris 2024-06-01     5    48.21     16.37    forest
#> 3       2    Quercus robur 2024-06-15     8    47.07     15.44   wetland
#> 4       2  Fagus sylvatica 2024-06-15     3    47.07     15.44   wetland
#> 5       3 Pinus sylvestris 2024-07-01     7    46.62     14.31 grassland
#> 6       3   Betula pendula 2024-07-01    14    46.62     14.31 grassland
```

Site 4 (in observations but not in sites) and site 5 (in sites but not
in observations) both vanish. The result has six rows instead of the
eight we started with.

Inner join is the right choice when unmatched rows are meaningless for
the analysis. If we only want observations where we have spatial
metadata, inner join gives us exactly that. Compared to a left join
followed by `filter(!is.na(latitude))`, the inner join does it in a
single hash probe pass with no intermediate `NA` rows to filter out.

When the key relationship is one-to-one on both sides, inner join and
left join produce the same row count. The difference shows up when there
are unmatched keys. Checking row counts after each join is a quick
sanity test for whether the key relationship is what we expected.

## Right and full joins

**Right join.**
[`right_join()`](https://gillescolling.com/vectra/reference/left_join.md)
is the mirror of
[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md):
it keeps every row from the right table and fills `NA` on the left side
where there is no match. Internally vectra implements it by swapping the
two inputs and reordering the output columns to match the expected
layout.

``` r

right_join(
  tbl(obs_path), tbl(sites_path), by = "site_id"
) |> collect()
#>   site_id          species       date count latitude longitude   habitat
#> 1       1 Pinus sylvestris 2024-06-01     5    48.21     16.37    forest
#> 2       1    Quercus robur 2024-06-01    12    48.21     16.37    forest
#> 3       2  Fagus sylvatica 2024-06-15     3    47.07     15.44   wetland
#> 4       2    Quercus robur 2024-06-15     8    47.07     15.44   wetland
#> 5       3   Betula pendula 2024-07-01    14    46.62     14.31 grassland
#> 6       3 Pinus sylvestris 2024-07-01     7    46.62     14.31 grassland
#> 7       5             <NA>       <NA>    NA    48.85     13.02    forest
```

Site 5 now appears with `NA` values for `species`, `date`, and `count`.
Site 4 drops because it has no entry in the right table (sites). A right
join with the tables in order `(A, B)` produces the same matches as a
left join with `(B, A)`, but the column order follows the original call:
left-side columns first, then right-side columns.

**Full join.**
[`full_join()`](https://gillescolling.com/vectra/reference/left_join.md)
keeps every row from both sides. Rows with a match get combined;
unmatched rows from either side are padded with `NA`.

``` r

full_join(
  tbl(obs_path), tbl(sites_path), by = "site_id"
) |> collect()
#>   site_id             species       date count latitude longitude   habitat
#> 1       1       Quercus robur 2024-06-01    12    48.21     16.37    forest
#> 2       1    Pinus sylvestris 2024-06-01     5    48.21     16.37    forest
#> 3       2       Quercus robur 2024-06-15     8    47.07     15.44   wetland
#> 4       2     Fagus sylvatica 2024-06-15     3    47.07     15.44   wetland
#> 5       3    Pinus sylvestris 2024-07-01     7    46.62     14.31 grassland
#> 6       3      Betula pendula 2024-07-01    14    46.62     14.31 grassland
#> 7       4     Fagus sylvatica 2024-07-10     6       NA        NA      <NA>
#> 8       4 Acer pseudoplatanus 2024-07-10     2       NA        NA      <NA>
#> 9       5                <NA>       <NA>    NA    48.85     13.02    forest
```

Both site 4 (left-only) and site 5 (right-only) appear. Full join is
useful when we need to see the complete picture and handle gaps
explicitly downstream.

The engine processes full joins in three phases. First, it builds the
hash table from the right side. Second, it streams the left side,
emitting matched and unmatched left rows. Third, it walks the hash table
to emit any right rows that were never probed, flushing them in
65536-row chunks. This third phase adds a memory overhead equal to the
unmatched portion of the right side.

## Filtering joins: semi and anti

Filtering joins use the right table purely as a lookup, deciding which
left rows to keep or discard without bringing in any of its columns.

**Semi join.**
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md)
answers: “which of my left rows have at least one match in the right
table?” The output schema is identical to the left table.

Consider a scenario where we have a list of target species and want to
keep only observations of those species.

``` r

targets_path <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  species = c("Quercus robur", "Betula pendula"),
  stringsAsFactors = FALSE
), targets_path)
```

``` r

semi_join(
  tbl(obs_path), tbl(targets_path), by = "species"
) |> collect()
#>   site_id        species       date count
#> 1       1  Quercus robur 2024-06-01    12
#> 2       2  Quercus robur 2024-06-15     8
#> 3       3 Betula pendula 2024-07-01    14
```

Three observations match. The result has the same columns as the
observations table; no columns from the target list leak through. If a
species appeared multiple times in the targets table, each observation
would still appear only once. Semi join deduplicates by design: the
right table is a set, not a mapping.

Semi join shows up frequently in spatial and temporal subsetting.
Suppose we have a table of sites that passed a quality control check,
and we want to restrict a large observation dataset to only those sites.
An inner join would work, but it would also attach the QC columns to
every output row, widening the result for no reason. Semi join keeps the
observation schema untouched. In pipelines where the output feeds into
another join or a group-by aggregation, avoiding unnecessary columns
reduces both memory use and cognitive load when reading the schema.

**Anti join.**
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
is the complement: “which left rows have no match in the right table?”
This is the diagnostic workhorse for finding orphan records, missing
reference entries, and data quality gaps.

The classic alternative to anti join is
`left_join() |> filter(is.na(key))`. Both produce the same rows, but
anti join is more efficient: the engine never copies right-side columns
into the output, and it never produces intermediate NA-padded rows that
the filter then discards. For a left table with 10 million rows and a
right table with 50,000 rows, the left-join-then-filter approach
allocates space for right-side payload columns in every output batch,
fills most of them with real data, and then throws those rows away. Anti
join skips all of that. It probes the hash table, checks for a miss, and
emits the left row as-is.

Beyond performance, anti join communicates intent. A reader seeing
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
immediately knows the goal is to find what is missing. A
[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
followed by `filter(is.na(...))` requires reading two lines and mentally
combining them.

Which observed species are missing from our taxonomic reference list?

``` r

anti_join(
  tbl(obs_path),
  tbl(ref_path),
  by = c("species" = "species_name")
) |> collect()
#>   site_id             species       date count
#> 1       4 Acer pseudoplatanus 2024-07-10     2
```

*Acer pseudoplatanus* is in the observations but not in the reference
table. In a real workflow, this flags an incomplete reference list or a
data entry error.

The reverse question is equally useful. Which reference species have
never been observed?

``` r

anti_join(
  tbl(ref_path),
  tbl(obs_path),
  by = c("species_name" = "species")
) |> collect()
#>      species_name     family conservation_status
#> 1 Alnus glutinosa Betulaceae                  LC
```

*Alnus glutinosa* is in the reference but absent from observations.
Running anti joins in both directions is a standard check when
validating relational integrity between tables.

Filtering joins are cheaper than mutating joins when all we need is the
filter effect. The hash table is built the same way, but no columns are
copied from the right side, and the output batch can reuse the left
batch’s memory directly. For wide right-side tables with many columns,
the savings can be substantial: a mutating join copies every payload
column for every matched row, while a filtering join touches none of
them.

## Cross join

[`cross_join()`](https://gillescolling.com/vectra/reference/cross_join.md)
produces the Cartesian product of two tables. Every row on the left is
paired with every row on the right. There is no `by` parameter.

A common use is generating a complete grid of combinations. Suppose we
want every site-year pair for three years of monitoring.

``` r

years_path <- tempfile(fileext = ".vtr")
write_vtr(data.frame(year = c(2022, 2023, 2024)), years_path)

grid <- cross_join(tbl(sites_path), tbl(years_path))
grid
#>    site_id latitude longitude   habitat year
#> 1        1    48.21     16.37    forest 2022
#> 2        1    48.21     16.37    forest 2023
#> 3        1    48.21     16.37    forest 2024
#> 4        2    47.07     15.44   wetland 2022
#> 5        2    47.07     15.44   wetland 2023
#> 6        2    47.07     15.44   wetland 2024
#> 7        3    46.62     14.31 grassland 2022
#> 8        3    46.62     14.31 grassland 2023
#> 9        3    46.62     14.31 grassland 2024
#> 10       5    48.85     13.02    forest 2022
#> 11       5    48.85     13.02    forest 2023
#> 12       5    48.85     13.02    forest 2024
```

The result has `4 * 3 = 12` rows (4 sites times 3 years). From here we
could left join actual survey data onto this grid to identify which
site-year combinations lack records.

Cross joins scale as `O(n * m)`, so both tables must be small enough for
the output to fit in memory. vectra collects both sides before
expanding. Use this for generating lookup grids, parameter sweeps, or
small combinatorial tables. For large tables it is almost never what we
want.

## Fuzzy joins

Ecological datasets are full of messy names. Field observers write
“Quercus robar” instead of “Quercus robur”, or “Pinus sylvestris L.”
instead of “Pinus sylvestris”. Exact joins miss these.
[`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
matches rows by approximate string distance.

``` r

messy_path <- tempfile(fileext = ".vtr")
clean_path <- tempfile(fileext = ".vtr")

write_vtr(data.frame(
  obs_id = c(1, 2, 3, 4, 5),
  name = c("Quercus robar", "Pinus sylvestris L.",
           "Fagus silvatica", "Betula pendla",
           "Acer pseudoplatanus"),
  stringsAsFactors = FALSE
), messy_path)

write_vtr(data.frame(
  species_name = c("Quercus robur", "Pinus sylvestris",
                   "Fagus sylvatica", "Betula pendula",
                   "Acer pseudoplatanus"),
  family = c("Fagaceae", "Pinaceae", "Fagaceae",
             "Betulaceae", "Sapindaceae"),
  stringsAsFactors = FALSE
), clean_path)
```

``` r

fuzzy_join(
  tbl(messy_path),
  tbl(clean_path),
  by = c("name" = "species_name"),
  method = "dl",
  max_dist = 0.2
) |> collect()
#>   obs_id                name        species_name      family fuzzy_dist
#> 1      1       Quercus robar       Quercus robur    Fagaceae 0.07692308
#> 2      2 Pinus sylvestris L.    Pinus sylvestris    Pinaceae 0.15789474
#> 3      3     Fagus silvatica     Fagus sylvatica    Fagaceae 0.06666667
#> 4      4       Betula pendla      Betula pendula  Betulaceae 0.07142857
#> 5      5 Acer pseudoplatanus Acer pseudoplatanus Sapindaceae 0.00000000
```

The output includes a `fuzzy_dist` column with the normalized distance
between the matched strings. Rows that exceed `max_dist` are excluded.

Choosing the right `max_dist` threshold requires balancing recall
against precision. A threshold of 0.1 catches only very minor typos (one
character in a ten-character string). A threshold of 0.3 catches more
variation but starts admitting false matches: “Pinus mugo” and “Pinus
nigra” differ by only a few characters and could match if the threshold
is too loose. The cost of a false match in a species lookup is a wrong
family or conservation status propagating silently through the analysis.
Start with a conservative threshold (0.1 or 0.15), inspect the
`fuzzy_dist` column in the output, and widen only if legitimate matches
are being missed. Sorting the result by `fuzzy_dist` in descending order
is a quick way to audit the worst matches and decide whether the
threshold needs tightening.

The three available methods have different characteristics:

- **`"dl"`** (Damerau-Levenshtein, the default): counts insertions,
  deletions, substitutions, and transpositions. Good general-purpose
  metric for typos.

- **`"levenshtein"`**: same as DL but without transpositions. Slightly
  stricter.

- **`"jw"`** (Jaro-Winkler): gives more weight to matching prefixes.
  Designed for short strings like personal names; works well for
  genus-level matching.

**Blocking for performance.** Fuzzy joins compare every left row against
every right row (within the distance threshold). For large tables this
is expensive. The `block_by` parameter restricts comparisons to rows
that share an exact match on a blocking column.

``` r

messy2_path <- tempfile(fileext = ".vtr")
clean2_path <- tempfile(fileext = ".vtr")

write_vtr(data.frame(
  genus = c("Quercus", "Pinus", "Fagus", "Betula"),
  name = c("Quercus robar", "Pinus sylvestris L.",
           "Fagus silvatica", "Betula pendla"),
  stringsAsFactors = FALSE
), messy2_path)

write_vtr(data.frame(
  genus = c("Quercus", "Pinus", "Fagus",
            "Betula", "Alnus"),
  species_name = c("Quercus robur", "Pinus sylvestris",
                   "Fagus sylvatica", "Betula pendula",
                   "Alnus glutinosa"),
  stringsAsFactors = FALSE
), clean2_path)
```

``` r

fuzzy_join(
  tbl(messy2_path),
  tbl(clean2_path),
  by = c("name" = "species_name"),
  method = "dl",
  max_dist = 0.25,
  block_by = c("genus" = "genus")
) |> collect()
#>     genus                name genus.y     species_name fuzzy_dist
#> 1 Quercus       Quercus robar Quercus    Quercus robur 0.07692308
#> 2   Pinus Pinus sylvestris L.   Pinus Pinus sylvestris 0.15789474
#> 3   Fagus     Fagus silvatica   Fagus  Fagus sylvatica 0.06666667
#> 4  Betula       Betula pendla  Betula   Betula pendula 0.07142857
```

With blocking, each *Quercus* name is only compared against other
*Quercus* entries, each *Pinus* name against other *Pinus* entries, and
so on. Without blocking, a fuzzy join between a left table with N rows
and a right table with M rows computes N \* M distance comparisons. Each
comparison involves walking both strings character by character, so the
total cost is O(N \* M \* L) where L is the average string length. For N
= 10,000 and M = 100,000, that is a billion comparisons. Blocking
partitions both tables by the blocking column and runs the fuzzy
comparison only within matching partitions. If the blocking column
splits the data into k roughly equal groups, the comparison count drops
to N \* M / k. With 200 genera, that billion becomes five million.

The blocking column must be an exact-match column that is already clean
on both sides. Genus is a natural choice for species names because it is
shorter, less prone to typos, and partitions the data well. Other good
blocking columns include country codes, first letters of names, or any
categorical variable that coarsely groups the data. If the blocking
column itself has errors, rows will land in the wrong partition and miss
their true match entirely, so it is worth cleaning the blocking column
before relying on it.

The fuzzy distance computation within each block is parallelized with
OpenMP, and the `n_threads` parameter controls the thread count.

## Multi-column keys

When a single column does not uniquely identify the relationship, we
join on a composite key. This is common with spatial and temporal data
where the natural key is a site-year or site-date combination.

``` r

counts_path <- tempfile(fileext = ".vtr")
effort_path <- tempfile(fileext = ".vtr")

write_vtr(data.frame(
  site_id = c(1, 1, 2, 2, 3),
  year = c(2023, 2024, 2023, 2024, 2024),
  n_species = c(12, 15, 8, 10, 20)
), counts_path)

write_vtr(data.frame(
  site_id = c(1, 1, 2, 3, 3),
  year = c(2023, 2024, 2024, 2023, 2024),
  hours = c(4.5, 5.0, 3.0, 6.0, 4.0)
), effort_path)
```

``` r

left_join(
  tbl(counts_path), tbl(effort_path),
  by = c("site_id", "year")
) |> collect()
#>   site_id year n_species hours
#> 1       1 2023        12   4.5
#> 2       1 2024        15   5.0
#> 3       2 2024        10   3.0
#> 4       3 2024        20   4.0
#> 5       2 2023         8    NA
```

Site 2 in 2023 has a species count but no matching effort record, so the
effort column is `NA` for that row. Site 3 in 2023 has effort data but
no count, and because this is a left join it does not appear in the
output at all.

Composite keys work with all join types. The hash table uses a combined
hash of all key columns (FNV-1a hash combining), so lookup cost does not
increase linearly with the number of key columns. There is a single hash
probe per row regardless of whether the key has one column or five.

When column names differ between tables, the named form works for
composites too:

``` r

effort2_path <- tempfile(fileext = ".vtr")
write_vtr(data.frame(
  loc = c(1, 1, 2, 3, 3),
  survey_year = c(2023, 2024, 2024, 2023, 2024),
  hours = c(4.5, 5.0, 3.0, 6.0, 4.0)
), effort2_path)

left_join(
  tbl(counts_path), tbl(effort2_path),
  by = c("site_id" = "loc", "year" = "survey_year")
) |> collect()
#>   site_id year n_species hours
#> 1       1 2023        12   4.5
#> 2       1 2024        15   5.0
#> 3       2 2024        10   3.0
#> 4       3 2024        20   4.0
#> 5       2 2023         8    NA
```

## Key coercion and NA handling

vectra’s join keys follow a type hierarchy: `bool < int64 < double`.
When the left key is `int64` and the right key is `double`, or vice
versa, both sides are coerced to `double` before hashing. This happens
transparently.

``` r

int_path <- tempfile(fileext = ".vtr")
dbl_path <- tempfile(fileext = ".vtr")

write_vtr(data.frame(id = c(1L, 2L, 3L), x = c(10, 20, 30)), int_path)
write_vtr(data.frame(id = c(1.0, 2.0, 4.0), y = c(100, 200, 400)), dbl_path)

inner_join(tbl(int_path), tbl(dbl_path), by = "id") |> collect()
#>   id  x   y
#> 1  1 10 100
#> 2  2 20 200
```

The integer `1L` matches the double `1.0` after coercion. Boolean
columns coerce to int64 (FALSE = 0, TRUE = 1) if the other side is
numeric.

Joining a string column against a numeric column is an error. vectra
does not attempt to parse strings as numbers, because that coercion is
ambiguous (“01” vs. 1, leading zeros, locale-dependent formatting). If
we need to join string IDs against numeric IDs, we convert explicitly
with [`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
before the join.

**NA keys never match.** This follows SQL NULL semantics. A row with
`NA` in the key column will not match any other row, including another
`NA`.

``` r

na_left <- tempfile(fileext = ".vtr")
na_right <- tempfile(fileext = ".vtr")

write_vtr(data.frame(
  id = c(1, NA, 3), x = c(10, 20, 30)
), na_left)
write_vtr(data.frame(
  id = c(1, NA, 4), y = c(100, 200, 400)
), na_right)

left_join(tbl(na_left), tbl(na_right), by = "id") |> collect()
#>   id  x   y
#> 1  1 10 100
#> 2 NA 20  NA
#> 3  3 30  NA
```

Row 2 on the left has `id = NA`. Even though row 2 on the right also has
`id = NA`, they do not match. The left row is preserved (because this is
a left join) but with `NA` for column `y`. In an inner join, both
NA-keyed rows would drop entirely.

This behavior is consistent and predictable, but it catches people off
guard when they expect NA-to-NA matching. The consequence for left joins
is that a left row with an `NA` key will always appear in the output
with `NA`-filled right columns, even if the right table also has rows
with `NA` keys. Those right-side `NA`-keyed rows sit unused in the hash
table. For inner joins, `NA` keys on either side mean those rows vanish
from the output entirely. For anti joins, every `NA`-keyed left row is
guaranteed to survive because it can never find a match.

In practice, `NA` keys usually signal missing data rather than a
deliberate category. A site_id of `NA` means “we don’t know which site
this observation belongs to,” not “this observation belongs to the NA
site.” SQL NULL semantics align with that interpretation: unknown values
cannot be equal. If matching on missing values is genuinely the goal
(rare, but it happens when `NA` represents a shared “unknown” category),
replace `NA` with a sentinel value via
[`mutate()`](https://gillescolling.com/vectra/reference/mutate.md)
before joining.

## Memory and performance

Understanding the memory profile of joins helps with sizing. vectra’s
hash join has three cost components:

1.  **Build phase.** The entire right-side table is collected into a
    hash table. Memory cost is proportional to
    `nrow(right) * avg_row_width`. Each row in the hash table stores all
    columns (keys + payload), plus hash bucket pointers and overflow
    chains.

2.  **Probe phase.** The left side streams batch by batch. Only one
    batch lives in memory at a time, regardless of how large the left
    table is. For a table with 100 million rows and 10,000 rows per
    batch, only 10,000 rows of the left side are in memory at any given
    moment.

3.  **Finalize phase** (full join only). After probing, the engine walks
    the hash table to find unmatched right rows. These are flushed in
    65536-row chunks. Memory overhead is small since the hash table
    already exists.

The practical rule: **put the smaller table on the right.** If we join a
500 million row observation table against a 10,000 row lookup table, the
lookup table goes on the right. Memory usage is then roughly
`10,000 * row_width` bytes plus overhead for the hash table structure.
The 500 million row left side streams through without ever being fully
in memory.

To put concrete numbers on “right-side memory,” consider a typical
lookup table with 5 columns: one int64 key, two double-precision floats,
and two short strings (average 20 bytes each). Each row occupies roughly
8 + 8 + 8 + 20 + 20 = 64 bytes of payload, plus ~24 bytes of hash table
overhead per slot (hash value, next-pointer, validity flags). That gives
us roughly 90 bytes per row. A right table with 100,000 rows costs about
9 MB; one with 1 million rows costs about 90 MB. String-heavy tables
cost more because each string value is stored inline. A table with a
200-character text column per row could easily triple the per-row cost.

When both tables are large and neither fits comfortably, pre-filtering
the right side before the join is the single most effective
optimization. A
[`select()`](https://gillescolling.com/vectra/reference/select.md) that
drops columns not needed downstream reduces row width directly. A
[`filter()`](https://gillescolling.com/vectra/reference/filter.md) that
removes irrelevant rows reduces row count. Both shrink the hash table.
If the right side has many duplicate keys and we only need one row per
key (say, the most recent record), aggregating with
`group_by() |> summarise()` or deduplicating with a window function
before the join can cut the right-side size by an order of magnitude.

**Filtering joins are cheaper** than mutating joins when all we need is
the existence check.
[`semi_join()`](https://gillescolling.com/vectra/reference/left_join.md)
and
[`anti_join()`](https://gillescolling.com/vectra/reference/left_join.md)
still build the right-side hash table, but they skip copying right-side
payload columns into the output. The output reuses the left batch’s
memory layout. This makes them faster and more memory-efficient than the
equivalent pattern of
[`left_join()`](https://gillescolling.com/vectra/reference/left_join.md)
followed by `filter(is.na(...))` or `filter(!is.na(...))`.

**Hash table sizing.** The engine uses an open-addressing hash table
with a 70% load factor threshold. For a right table with N rows, the
table allocates roughly `N / 0.7` slots. The hash function is FNV-1a,
which distributes well for both numeric and string keys. Composite keys
combine per-column hashes with FNV-1a mixing, so multi-column keys have
the same probe cost as single column keys.

## Practical guidance

Choosing the right join type is often the first design decision in a
pipeline. Here is a decision framework.

**Data enrichment: left join.** We have a primary table and want to
attach attributes from a lookup table. Not every row will match, and we
want to keep all primary rows. This is the most common pattern in
analytical work.

``` r

# Attach habitat type to every observation
left_join(tbl(obs_path), tbl(sites_path), by = "site_id") |>
  select(site_id, species, habitat) |>
  collect()
#>   site_id             species   habitat
#> 1       1       Quercus robur    forest
#> 2       1    Pinus sylvestris    forest
#> 3       2       Quercus robur   wetland
#> 4       2     Fagus sylvatica   wetland
#> 5       3    Pinus sylvestris grassland
#> 6       3      Betula pendula grassland
#> 7       4     Fagus sylvatica      <NA>
#> 8       4 Acer pseudoplatanus      <NA>
```

**Exact intersection: inner join.** We only want rows where both sides
have data. Useful for restricting analysis to a validated subset.

**Completeness check: full join.** We want to see everything and handle
gaps explicitly. Good for reconciliation between two data sources.

**Set membership: semi join.** We want to filter a table to rows that
exist in a reference set, without adding any columns. More efficient
than inner join when we do not need the right-side columns.

**Finding gaps: anti join.** Orphan records, missing entries, incomplete
coverage. Anti join is the diagnostic tool for relational integrity. Run
it in both directions to get the full picture.

**All combinations: cross join.** Generating a grid of factor levels for
downstream left joins. Use sparingly; output grows multiplicatively.

**Approximate matching: fuzzy join.** When keys have typos, inconsistent
formatting, or minor spelling variations that prevent exact matching.
Always try preprocessing first (trimming whitespace, case normalization)
because exact joins are faster by orders of magnitude.

``` r

# Preprocessing before an exact join
tbl(obs_path) |>
  mutate(species_clean = trimws(tolower(species))) |>
  select(site_id, species_clean, count) |>
  collect()
#>   site_id       species_clean count
#> 1       1       quercus robur    12
#> 2       1    pinus sylvestris     5
#> 3       2       quercus robur     8
#> 4       2     fagus sylvatica     3
#> 5       3    pinus sylvestris     7
#> 6       3      betula pendula    14
#> 7       4     fagus sylvatica     6
#> 8       4 acer pseudoplatanus     2
```

If [`tolower()`](https://rdrr.io/r/base/chartr.html) and
[`trimws()`](https://rdrr.io/r/base/trimws.html) bring the keys into
alignment, an exact join is the better choice. Reserve
[`fuzzy_join()`](https://gillescolling.com/vectra/reference/fuzzy_join.md)
for genuine spelling variation that normalization cannot resolve.

**Many-to-many relationships.** When both tables have duplicate keys,
the join produces a row for every combination of matching rows. If the
left table has 3 rows with `id = 1` and the right table has 4 rows with
`id = 1`, the output has 12 rows for that key. This multiplicative
blowup can be surprising and, for large tables, dangerous: a key that
appears 1,000 times on the left and 1,000 times on the right produces a
million output rows for that single key value.

Many-to-many joins usually signal a modelling error. The most common
causes are: duplicate rows in a lookup table that was supposed to have
unique keys, a join key that is too coarse (joining on `year` when the
correct key is `site_id + year`), or a table that represents a genuinely
many-to-many relationship but needs an intermediate mapping table. To
detect the problem, check for duplicate keys on each side before
joining. A quick `group_by(key) |> summarise(n = n()) |> filter(n > 1)`
on the right table reveals duplicates. If the relationship is genuinely
many-to-many and the blowup is intended (e.g., generating all pairwise
combinations within groups), a cross join within groups may be more
explicit about what is happening.

**Join ordering in multi-join pipelines.** When a pipeline chains
several joins, the order matters for both correctness and performance.
Consider an observation table that needs site metadata, taxonomic
classification, and sampling effort. We have three right-side tables to
attach. Performance-wise, the most selective join should go first: if
the taxonomic reference covers only 80% of the observed species and we
are doing an inner join on taxonomy, placing that join first reduces the
row count before the subsequent joins build their hash tables. Each
downstream join then processes fewer left-side rows. For left joins,
order does not affect row count (every left row survives), but it still
affects width: each join adds columns, and wider rows cost more to hash
and copy in subsequent joins. Attaching the narrowest lookup table first
keeps intermediate rows slim.

**Self-joins for deduplication.** Anti-joining a table against itself
(with adjusted keys or filters) can identify duplicate records. For
instance, finding observations that share a site, species, and date but
have different counts.
