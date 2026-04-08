# tdc benchmark results

Recorded 2026-04-07 on the-beast (Windows 11, R 4.5.2). Single source of truth
for tdc vs zstd / fst / parquet / terra. Re-run via `bench_vs_zstd.R` and
`bench_compress_final.R` from the repo root.

## Byte-stream: shuffle + tdc LZ2 vs zstd CLI

5M rows x 8 double cols = 320 MB raw. Same shuffled byte stream fed to both
sides (zstd CLI runs over the raw VTR file produced by `compress = "none"`).

| Compressor          |   Ratio | Size MB | Comp ms | Decomp ms |
|---------------------|--------:|--------:|--------:|----------:|
| zstd -1             |  67.2%  |   218.5 |     220 |       300 |
| zstd -3             |  63.1%  |   205.2 |     400 |       340 |
| zstd -6             |  61.5%  |   199.8 |     840 |       310 |
| zstd -9             |  61.4%  |   199.6 |    1480 |       330 |
| zstd -19            |  58.2%  |   189.2 |   49630 |       350 |
| **vectra LZ2 fast** | **60.3%** | **196.0** | **880** | **125** |

Read this as:

- **Ratio**: tdc LZ2 beats zstd -6 (60.3% vs 61.5%) and ties zstd -9. Only
  zstd -19 (50 s compress) gets meaningfully smaller (58.2%). The byte-shuffle
  pre-pass is doing its job — we beat zstd -6 ratio without an entropy stage.
- **Compress speed**: 880 ms sits next to zstd -6 (840 ms). Roughly tied with
  zstd at the same ratio.
- **Decompress speed**: **125 ms vs zstd's flat ~300-350 ms** — tdc decode is
  now ~2.5x **faster** than zstd CLI on the same byte stream. The decode is
  parallelized across row groups (OpenMP), the wildcopy fast path stays in L2,
  and the SIMD unshuffle finishes the job before zstd's serial entropy stage
  can catch up.

## End-to-end round-trip vs fst / parquet / CSV

Same 5M x 8 doubles, full write -> read cycle (median of 10 reads).

| Format               | RGs | Size MB | Write ms | Read ms |
|----------------------|----:|--------:|---------:|--------:|
| **vectra none**      |  39 |   325.0 |      360 |  **65** |
| **vectra fast**      |  39 |   196.0 |      830 | **100** |
| fst (compress=0)     |   - |   320.0 |      140 |     135 |
| fst (compress=50)    |   - |   301.6 |      160 |     145 |
| fst (compress=100)   |   - |   194.5 |     1200 |     140 |
| parquet (snappy)     |   - |   278.8 |     1890 |     420 |
| parquet (zstd)       |   - |   229.9 |     2020 |     520 |
| fread/fwrite CSV     |   - |   671.6 |      300 |     215 |

Read this as:

- **vs fst at the same ratio** (`fst compress=100` is the closest competitor,
  194.5 MB vs our 196.0 MB): vectra reads in **100 ms vs fst's 140 ms**, i.e.
  we are now **~1.4x faster than fst on decode** at matched ratio. vectra
  writes in 830 ms vs fst's 1200 ms, so we're also ~1.4x faster on write.
  vectra fast beats *every* fst level on read time.
- **vs parquet zstd**: tdc fast wins on every axis — smaller file (196 vs 230
  MB), ~2.4x faster write, ~5x faster read.
- **vs vectra none**: paying 35 ms of decode (65 -> 100 ms) buys 40% size
  reduction (325 -> 196 MB). At any I/O speed short of a fast NVMe with hot
  cache, the compressed path wins on wall time *and* size.

The 4x decode gap to fst documented in earlier notes is gone. Two changes
got us there:

1. **Lifted the `n_selected > 2` cap on the parallel-read path** in
   `scan_node_is_parallel_safe`. The 8-col benchmark was never taking the
   parallel branch — every column was being decoded on the calling thread.
   Once the cap was lifted, OpenMP parallelism across row groups dropped read
   time from 530 ms to ~170 ms.
2. **Direct-write parallel decode into pre-allocated R vectors**. The
   `vtr1_read_parallel_into` API hands each thread a slice of the final R
   storage; PLAIN+SHUFFLE_LZ2 columns unshuffle straight into REAL/INTEGER
   storage with `owns_data=0`. This eliminates the per-row-group malloc and
   the sequential fill-from-temporary phase, taking read time from ~170 ms
   down to ~100 ms. Other encodings (DIFF/DELTA/QUANTIZE/SPATIAL) still
   allocate their own buffers and fall back to memcpy via the standard path.

## Raster: 2000 x 2000 x 5 f64 vs terra DEFLATE

Spatially correlated cumsum stack (~76 MB raw). 5-iteration totals from
`bench_compress_final.R`.

| Format            | Size MB | Write 5x (s) | Read 5x (s) |
|-------------------|--------:|-------------:|------------:|
| terra DEFLATE     |    67.8 |         6.74 |        1.84 |
| VTR none          |   227.5 |         1.25 |        0.72 |
| **VTR fast**      |    70.8 |         2.21 |        1.80 |

- **Ratio**: 70.8 MB vs 67.8 MB — within 4% of GDAL's DEFLATE on a workload
  GDAL is built for. Encouraging given that DEFLATE *does* have an entropy
  stage and we don't.
- **Write**: 2.21 s vs 6.74 s -> tdc is ~3x faster on encode.
- **Read**: 1.80 s vs 1.84 s -> roughly tied. terra's filter read (full scan +
  R-side subset) is 2.31 s vs vectra's pushdown filter at 3.10 s on this
  particular query, where the predicate is ~50% selective and the filter
  overhead exceeds the I/O savings.

## Where tdc stands today

- **Ratio**: beats zstd -6, ties zstd -9, ties fst zstd / GDAL DEFLATE on f64
  columns. Done.
- **Encode speed**: faster than fst zstd, faster than parquet, faster than
  terra DEFLATE. Done.
- **Decode speed**: now **faster than fst** end-to-end at matched ratio
  (100 ms vs 140 ms on 5M x 8 doubles) and **~2.5x faster than zstd CLI** on
  the raw byte stream. The remaining headroom would come from an entropy
  stage (FSE/Huffman) over the match/literal streams — that would shrink
  files further without slowing decode much, since the parallel + direct-write
  path is no longer the bottleneck. No longer urgent for read speed, but
  still the right next codec move for ratio.
