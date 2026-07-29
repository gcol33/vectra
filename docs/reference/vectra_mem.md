# Resolve the vectra memory budget

The single memory ceiling every buffering subsystem derives its working
budget from: the external sort's spill threshold, the self-overlay tile
cap, spatial run-file flushes, partition routing, and the spilling hash
join. Resolution order is an explicit `limit`, then
`getOption("vectra.memory")`, then a default of half the detected system
RAM. The auto-detected default is floored at 1 GB; an explicit `limit`
or option is honored as given (down to 1 KB), so a smaller budget can be
requested deliberately.

## Usage

``` r
vectra_mem(limit = NULL)
```

## Arguments

- limit:

  Optional per-call override: a byte count or a string such as `"8GB"`.
  `NULL` (the default) falls back to the option, then to auto-detection.

## Value

The budget in bytes, as a numeric scalar, never below 1 GB.

## Details

Set the session budget with `options(vectra.memory = "8GB")` (a string
with a K/M/G/T suffix) or `options(vectra.memory = 8e9)` (a byte count).
Row-group size (`batch_size`) is a separate cache-locality knob and is
not affected.

## Examples

``` r
vectra_mem()
vectra_mem("4GB")
```
