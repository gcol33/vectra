# Resolve the vectra memory budget

The single memory ceiling every buffering subsystem derives its working
budget from: the external sort's spill threshold, the self-overlay tile
cap, spatial run-file flushes, partition routing, and the spilling hash
join.

## Usage

``` r
vectra_mem(limit = NULL)
```

## Arguments

- limit:

  Optional per-call override: a byte count or a string such as `"8GB"`.
  `NULL` (the default) falls back to the option, then to auto-detection.

## Value

The budget in bytes, as a numeric scalar: at least 1 GB when
auto-detected, and as given (down to 1 KB) when set explicitly.

## Details

The budget bounds a whole query, not each step of it. When a query holds
several buffering steps at once (a join feeding a grouped
[`summarise()`](https://gillescolling.com/vectra/reference/summarise.md),
an [`arrange()`](https://gillescolling.com/vectra/reference/arrange.md)
over a join), they draw on one shared pool of this size. A step reserves
what it actually allocates as it grows (buffered rows at their allocated
capacity, the permutation and scratch a sort needs, the hash table a
join builds) and spills when the pool refuses. A step that needs little
leaves the rest to the others; each step is always guaranteed
`1 / (4 * steps)` of the budget, so none is starved. The process peak is
then about the budget plus a fixed allowance for R itself, the batches
in flight and the collected result.

Resolution order is an explicit `limit`, then
`getOption("vectra.memory")`, then a default of half the detected system
RAM. The auto-detected default is floored at 1 GB; an explicit `limit`
or option is honored as given (down to 1 KB), so a smaller budget can be
requested deliberately.

Set the session budget with `options(vectra.memory = "8GB")` (a string
with a K/M/G/T suffix) or `options(vectra.memory = 8e9)` (a byte count).
Row-group size (`batch_size`) is a separate cache-locality knob and is
not affected.

## Examples

``` r
vectra_mem()
vectra_mem("4GB")
```
