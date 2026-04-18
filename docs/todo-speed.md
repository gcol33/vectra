# Speed TODO

## Current state (2026-04-07, after P0 win)

Benchmark: 5M rows x 8 double cols = 320 MB raw

### End-to-end read (full round-trip incl. R data.frame materialization)

| Format           | Size MB | Write ms | Read ms |
|------------------|---------|----------|---------|
| vectra none      | 325     | 200      | 180     |
| vectra fast (LZ) | 196     | 1000     | 415     |
| vectra ratio     | 171     | 2410     | 690     |
| fst compress=0   | 320     | 170      | 180     |
| fst compress=50  | 302     | 200      | 205     |
| fst compress=100 | 195     | 1210     | 210     |

Deltas from prior run: vectra none write 410→200ms (2x), LZ read
550→415ms (24% faster). Vectra none read now ties fst 0. Remaining big
gaps: LZ read vs fst zstd (415 vs 210ms), LZ write vs fst 100 (1000 vs
1210 — actually competitive).

### Byte-stream compression (same raw VTR bytes, different compressors)

| Compressor     | Ratio | Size MB | Compress ms | Decompress ms |
|----------------|-------|---------|-------------|---------------|
| zstd -1        | 65.8% | 214     | 140         | 230           |
| zstd -3        | 61.7% | 201     | 300         | 270           |
| zstd -6        | 60.3% | 196     | 640         | 220           |
| zstd -9        | 60.2% | 196     | 1170        | 250           |
| zstd -19       | 57.2% | 186     | 39360       | 290           |
| vectra LZ      | 60.3% | 196     | 760         | 580           |
| vectra deflate | 52.8% | 172     | 2220        | 770           |

## Observations

1.  **Uncompressed reads are slower than fst**: vectra none (175ms) vs
    fst compress=0 (130ms). 35% gap with no compression involved — this
    is pure I/O + column materialization overhead.

2.  **LZ decompression is 2.5x slower than zstd**: 580ms vs 230ms for
    the same compression ratio (60.3%). LZ adds 375ms of decode
    overhead; zstd adds ~35ms via fst. The gap is entropy coding
    (ANS/FSE) and multi-stream parallelism.

3.  **Compression ratio is on par**: LZ matches zstd -6 exactly (196
    MB). Deflate beats everything at 172 MB but is slow to decode.

4.  **Write speed is slow**: vectra none (410ms) vs fst compress=0
    (150ms). 2.7x slower even without compression.

## Priorities

### P0: Uncompressed read path — DONE (2026-04-07)

175ms → 90ms (49% faster, beats fst’s 130ms).

### P1: LZ read speed (415ms -\> target ~250ms)

**Profile (5M x 8 dbl, fused PLAIN+SHUFFLE_LZ path, 312 decode
calls/iter):**

| Component                      | -O2 baseline | -O3 (current)         |
|--------------------------------|--------------|-----------------------|
| lz decompress (core)           | 128 ms       | 117 ms                |
| byte unshuffle (sd → dst)      | 57 ms        | 53 ms                 |
| other (fread + R mat + allocs) | 241 ms       | ~200 ms\*             |
| **total**                      | **426 ms**   | **~370 ms** (noisy\*) |

\*The “other” component has high run-to-run variance (~30-50ms) due to
file I/O and OS state. Codec timings (decompress, unshuffle) are stable.

**What helped (2026-04-07):** - **Bumped Makevars to
`-O3 -funroll-loops`** (was `-O2`). - Decompress: 128 → 117 ms (-9%,
stable) - Unshuffle: 57 → 53 ms (-7%, stable) - Side benefit: also
speeds up R materialization and other hot loops. - Caveat: CRAN may flag
explicit -O3 in PKG_CFLAGS — strip before submission.

**What did not help:** - `-funroll-loops` alone vs -O3: no measurable
difference. - `-march=native`: no measurable difference (codec is
memory-bound, not compute-bound). Plus it breaks portability for
distributed builds. - Inline 16-byte literal wildcopy in
`lz_decompress_fast`: regressed by 2.5x. The new bail check
`lp + lit_len + 15 > literals_size` triggered constant safe-path
fallbacks. memcpy() in glibc is already well-optimized for short
sizes. - Explicit `_mm_prefetch` T0 hints in `byte_unshuffle_8_sse2`:
slight regression (52 → 56 ms). Modern CPUs prefetch adequately on their
own.

**Verified:** - SSE2 unshuffle path is live
(`g_prof_sse2_unshuffle_calls = 312/iter`, matching column count). 53ms
/ 312 calls = 167µs per 1MB call = **6 GB/s**. Versus memcpy ceiling
~10-12 GB/s — narrow remaining headroom for unshuffle.

Reference: vectra none = 180ms total for the same data. So the LZ path
adds - 128ms decompress + 57ms unshuffle = 185ms of pure decode work -
~61ms of extra “other” overhead (vs none-path’s 180ms baseline)

The 61ms overhead is unaccounted — likely extra memory traffic
(decompress writes sd, unshuffle reads sd writes dst — twice the memory
bandwidth of the none path which reads disk straight into dst). Worth a
finer breakdown (time fread, time dst malloc) to confirm before
optimizing.

**Levers (highest expected payoff first):** 1. **Fuse decompress +
unshuffle** — eliminate the sd intermediate. Hard because LZ match
copies reference earlier *contiguous* bytes; unshuffled positions would
break match offsets. Possible workaround: decompress in 8-element strips
small enough that SD lives in L1 ($`\approx`$ 32KB) while unshuffle
reads it. 2. **Faster decompress** — currently 128ms for 320MB → 2.5
GB/s. zstd-1 decodes at ~5-6 GB/s on similar HW. The fast/safe split
with wildcopy is already in place. Remaining levers: 4-byte tag reads,
prefetch, larger wildcopy step. 3. **Faster unshuffle** — 57ms for 320MB
→ 5.6 GB/s. memcpy on this hardware is ~10+ GB/s. SIMD path exists but
is “negligible per CLAUDE.md notes”. Worth re-checking: could be that
the SSE2 path isn’t actually being entered, or the 3-stage transpose has
stalls.

**Profile instrumentation in place:** - `vtr_lz_decompress_into` and
`vtr_byte_unshuffle_to` accumulate ns into global counters
(`vtr_codec.c`). Reset/get exposed via R as
`.Call("C_codec_profile_reset")` / `.Call("C_codec_profile_get")`. -
Bench script: `bench_profile_lz.R`. - Overhead is one `clock_gettime`
pair per call (~50ns). Negligible at current call counts but worth
gating behind `#ifdef VTR_PROFILE` before release.

### P2: Write speed (410ms -\> target ~200ms)

Uncompressed writes are 2.7x slower than fst. Likely causes: -
Per-column per-row-group overhead (header writes, seeks) - R-to-C data
extraction (REAL(), INTEGER(), STRING_ELT() loops) - Atomic write (temp
file + rename) overhead
