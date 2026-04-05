# vectra — C Engine Performance TODO

## Quick wins

### ~~1. Insertion sort for small groups in median~~ DONE
- **Measured:** -9% on 1000 groups / 1M rows. Bigger wins expected with 100k+ small groups.

### ~~2. memcpy in decompression match loop~~ DONE
- **Measured:** -2% on VTR round-trip. Match lengths are short in practice; bottleneck is elsewhere.

### ~~3. Thread-local buffers for regex~~ DONE
- **Measured:** No change on 1M rows. `regexec()` dominates; malloc overhead is negligible. Code is cleaner (no per-row alloc/free in hot loops).

### ~~4. Single-pass numeric-to-string coercion~~ DONE
- **Measured:** -2% on paste0 coercion. `snprintf` is the real bottleneck, not the second loop.

## High-impact structural changes

### ~~5. All-valid fast path for arithmetic and comparison~~ DONE
- **Measured:** Arithmetic 0.300→0.304 s/iter (noise), filter 0.186→0.184 s/iter (noise). Both inputs already all-valid but the bottleneck is VTR I/O and `collect()` overhead, not the arithmetic loop itself. Code is cleaner: tight auto-vectorizable loops for the common no-NA case. Skip-coercion (item 10) also applied here.

### ~~6. All-valid fast path for filter mask~~ DONE
- **Measured:** 0.186→0.184 s/iter (noise). Validity check is cheap relative to selection vector construction.

### ~~7. Prefetch in hash aggregation~~ DONE
- **Measured:** 10k-group sum agg 0.290→0.274 s/iter (**-6%**). Prefetches upcoming hash table entries 8 rows ahead.

### ~~8. Cache-friendly hash table layout~~ DONE
- **Measured:** Combined with item 7. Merged `slots[]` + `hashes[]` into `VecHTEntry` struct for co-located access. JoinHT in `join.c` not yet updated.

### ~~9. Robin hood hashing~~ REJECTED
- **Measured:** Implemented Robin Hood with separate `dists[]` array for both `VecHashTable` (group_agg) and `JoinHT` (joins). Benchmarked against baseline at 1M rows.
- **group_agg:** +12-17% regression at 10k-50k groups. Displacement cascade overhead exceeds early termination benefit. At 70% load factor, average probe length is ~1.7 — too short for Robin Hood to help. Most lookups are positive (feeding existing groups), so early termination on negative lookups rarely fires.
- **joins:** No improvement across inner/left/semi/anti at 1-100% match rates with 500k probe × 100k build. JoinHT at 50% load factor has even shorter probes. Extra `dists[]` access on every probe step costs more than it saves.
- **Root cause:** FNV-1a distributes well enough that probe chains are already short at these load factors. Robin Hood reduces probe variance (worst-case) but average stays similar, and the per-probe overhead of checking/maintaining displacement distances is not free.

### ~~10. Skip coercion when types already match~~ DONE
- **Measured:** Combined with item 5. In `vec_arith`/`vec_cmp`, operands that already match the common type are used directly instead of being copied. Saves 2 allocations + 2 memcpys per binary op when both operands are same type (the common case).

### ~~11. Merge sort buffer toggling~~ DONE
- **Measured:** No change on 1M rows (0.278→0.276 s/iter numeric, noise on all key types). The memcpy of the 8 MB index array is trivial relative to the random-access comparisons (`compare_rows_cross` chasing pointers into column data) and the gather phase. Code is cleaner: no copy-back per merge level.

### ~~12. Prefetch in gather/scatter~~ DONE
- **Measured:** Sort string 0.552→0.500 s/iter (**-9%**), sort multi-key 0.806→0.744 s/iter (**-8%**), sort numeric -3% (noise). Filter gather unchanged (VTR I/O-bound). `__builtin_prefetch` 8 iterations ahead in sort gather (`sort.c`) and selection-vector gather (`array.c`) for int64/double columns.

## Algorithmic improvements

### ~~13. Merge join path for sorted data~~ DONE
- **Measured:** inner_join 5M×1M: 1.470→0.520 s/iter (**2.8x**), left_join: 1.557→0.723 s/iter (**2.2x**). At 500k×100k: marginal (hash join already cheap at that scale). Detects SortNode children and ScanNode with `col_sorted` zone maps. Skips hash table construction entirely; O(n+m) sequential merge with M:N cross-product support. All 5 join kinds (inner/left/full/semi/anti) supported.

### ~~14. Predicate reordering by selectivity~~ DONE
- **Measured:** Multi-predicate filter with grepl + selective numeric: 0.216→0.194 s/iter (**-10%**) when expensive predicate listed first (reordering learns selectivity). 0.214→0.202 (**-6%**) when already optimal order. Flattens AND chains in FilterNode, evaluates sequentially with short-circuit on all-zero masks, tracks runtime selectivity (EMA) and reorders most-selective-first.

### ~~15. Radix sort for numeric keys~~ DONE
- **Measured:** 20M row single-key double: 5.657→5.530 s/iter (**-2%**). End-to-end gain is small because the gather phase (random-access column reordering) dominates arrange time, not the sort itself. LSD radix sort (8-bit, 8 passes) for single-key int64/double. Falls back to merge sort for multi-key, string, or n < 256. No regression on multi-key (9.547→9.557, noise).

### ~~16. Validity bitmap bulk operations~~ DONE
- **Measured:** No measurable end-to-end change. Bitmap copy/set is a negligible fraction of pipeline time (I/O and data memcpy dominate). Code is cleaner: `vec_validity_set_bits`, `vec_validity_clear_bits`, `vec_validity_copy_bits` with byte-aligned memcpy fast path replace bit-by-bit loops in builder. Will compound with future optimizations that reduce I/O overhead.

### ~~17. Dictionary-aware batch decode~~ DONE
- **Measured:** No change on 5M dict-encoded strings (0.270→0.272 s/iter, noise). Rewrote `dict_decode` to process RLE runs directly instead of expanding to a flat `n_rows * 4` index array. Eliminates the intermediate allocation and per-row index lookup, but the bottleneck is string memcpy, not index expansion. Code is cleaner: two-pass over RLE data (size then fill) with no intermediate buffer.

### ~~18. Window function parallelization~~ DONE
- **Measured:** No change on ungrouped 2M rank (1.320→1.327 s/iter, noise), -3% on grouped-10 large groups (1.080→1.043). Replaced thread-unsafe global `qsort` with thread-safe merge sort. OMP task parallelism for ungrouped top-level sort, sequential merge sort for grouped path (avoids nested parallelism overhead). No regression on cumsum/grouped-1k.
