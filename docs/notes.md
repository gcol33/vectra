# Performance Notes

## Arena Allocator vs Scratch Buffers (2026-04-06)

### Why arena doesn’t fit current architecture

VecArray buffers have independent lifetimes — each column’s `buf.i64`,
`validity`, etc. is individually malloc’d and individually free’d by
`vec_array_free()`. An arena requires batch-scoped lifetimes (all
buffers freed at once), which conflicts with how the pipeline operates:
filter, project, join, sort all create/move/copy column buffers
independently.

Making arena work would require redesigning around batch-scoped memory
pools (like Arrow/DuckDB), threading the arena through the entire
pipeline. That’s a v2 architecture decision.

### Current approach: scratch buffers + eliminate temp allocs

The existing `scratch_enc`/`scratch_dec` pattern handles the biggest
reuse case (compressed/decompressed intermediates). Remaining wins:

1.  **scratch_shuf** — third scratch buffer to eliminate per-call malloc
    in `vtr_byte_unshuffle()` (currently mallocs+frees a full copy every
    call in the general path)
2.  **Bulk I/O buffer** — `scratch_bulk` for single-fread-per-RG,
    replacing per-column fread/fseek
3.  All scratch buffers grow-but-never-shrink across row groups

### What arena would additionally save (the 20% we’re leaving on the table)

Per-RG final buffer allocations that currently must be malloc’d
individually: - `calloc` for each validity bitmap (~n_selected per RG) -
`malloc` for each column data buffer (~n_selected per RG) - String
column: separate `offsets` + `data` allocations - Spatial/quantize
intermediate buffers (`res_i64`, `values`, `int_buf`)

For a 10-column, 100-RG file: ~2000 malloc/free pairs that could be ~200
arena resets. At ~50ns per malloc, that’s ~100μs — negligible compared
to I/O and decompression time. The arena win is real but small in
absolute terms.

### Baseline benchmark (2026-04-06, pre-optimization)

2M rows, 8 cols (2x double, 1x int, 1x bool, 2x string, 1x int-id, 1x
runif), 20 RGs of 100K rows.

    === Full read (all columns) ===
      none          median:  220 ms   503 MB/s   (111 MB file)
      fast          median:  480 ms   124 MB/s   ( 59 MB file)
      ratio         median:  510 ms    91 MB/s   ( 46 MB file)

    === Select 2/8 columns (x, grp) ===
      none          median:  100 ms
      fast          median:  130 ms
      ratio         median:  140 ms

    === File open (20 RGs) ===
      none/fast/ratio: 0-10 ms (Windows proc.time resolution = 10ms)

    === Reference ===
      fread CSV     median:  170 ms   942 MB/s   (160 MB CSV)

    === 200 RGs (10K rows each) ===
      open          median:   20 ms

**Key findings:**

1.  **Decompression+unshuffle dominates:** none→fast adds 260ms (54% of
    total). That’s the LZ decompress + byte unshuffle cost. SIMD
    unshuffle targets this directly.
2.  **File open is negligible at 20 RGs** but scales to 20ms at 200 RGs.
    Footer index matters for many-RG files (1000+ RGs).
3.  **Column skipping works:** select 2/8 is ~2x faster for
    uncompressed. Compressed gains less because we still seek past
    column data.
4.  **The gap vs fread:** 2.8x on compressed data. Decompression is the
    dominant cost, not malloc overhead.
5.  **Arena impact estimate:** At ~2000 malloc/free pairs per full read,
    even at 100ns each = 200μs. That’s \<0.1% of the 480ms total.
    **Arena would be invisible in benchmarks.** The “80% of arena win
    via scratch buffers” claim is correct — but only because both arena
    and scratch give \<1% improvement on the actual bottleneck. The real
    wins are SIMD unshuffle and LZ decompression speed.

### Revised priority

1.  **SIMD byte-unshuffle** — targets the 260ms decompress+unshuffle
    cost directly
2.  **LZ decompression speed** — the other half of that 260ms
3.  **Bulk I/O** — eliminates per-column syscalls, matters more at scale
4.  **Footer index** — matters for many-RG files, negligible for typical
    20-RG files
5.  ~~Arena allocator~~ — confirmed irrelevant: \<0.1% of read time
