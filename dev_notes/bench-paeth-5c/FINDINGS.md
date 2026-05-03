# Phase 5c bench results — PAETH SIMD is not the bottleneck

## Setup
- 4096×4096 single-band rasters, tile_size = 256
- Host: i9-14900K, Windows 11, Rtools45 / R 4.6.0
- Full-stack `vec_read_window` (mmap → tdc decode → dtype cast → R matrix)
- 5 reads averaged, 1 warmup

## Measured throughput (raw bytes / wall time)

| dtype | data    | comp     | ratio  | read_ms | MB/s |
|-------|---------|----------|--------|---------|------|
| u16   | smooth  | balanced | 65×    | 310     | 108  |
| u16   | smooth  | max      | 65×    | 306     | 110  |
| u16   | noisy   | balanced | 1.0×   | 296     | 113  |
| u16   | noisy   | max      | 312    | 108     | 108  |
| f32   | smooth  | balanced | 143×   | 324     | 207  |
| f32   | smooth  | max      | 143×   | 338     | 199  |
| f32   | noisy   | balanced | 1.1×   | 340     | 197  |
| f32   | noisy   | max      | 1.1×   | 346     | 194  |
| u8    | smooth  | balanced | 194×   | 300     |  56  |
| u8    | smooth  | max      | 194×   | 276     |  61  |
| u8    | noisy   | balanced | 1.0×   | 286     |  59  |
| u8    | noisy   | max      | 1.0×   | 282     |  59  |

## Reference (isolated-tile decode, upstream tdc/bench/RESULTS.md)

- u16 PAETH: ~378–468 MB/s
- u16 UP:    ~608 MB/s

## Reading

Full-stack vectra throughput is **3–5× below** the upstream isolated-tile
PAETH ceiling. That gap is the per-pixel double cast, the R matrix alloc,
the tile assembly pass, and the mmap copy — *not* PAETH inverse.

Notice that `noisy` data (where PAETH residuals carry full signal entropy
and codec selection picks something other than PAETH) reads at the same
speed as `smooth` (where PAETH wins). If PAETH inverse were the bottleneck,
the smooth path would be visibly slower than the noisy one. It isn't.

## Conclusion

Phase 5c (SIMD PAETH inverse in tdc) buys little for vectra raster reads.
Even a hypothetical 2× PAETH inverse speedup would shave at most ~20% off
end-to-end throughput in the best case, and only when the codec picked
PAETH (not always). Time better spent elsewhere — e.g. eliminating the
double cast in hot read paths, or per-tile overhead in the decode loop.

**Recommendation: defer 5c indefinitely.** Re-open if a real workload
surfaces u16 PAETH-heavy reads as a measured bottleneck — the upstream
tdc reopen criterion (`tdc/SPEEDUP-TODO.md` N4) hasn't been met by any
vectra raster path observed so far.
