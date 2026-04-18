# Optimal Parsing for LZ — Implementation Plan

## Why

vectra’s `compress = "ratio"` mode (LZ + Huffman) is ~70% larger than
fst (zstd backend) on structured tabular data:

| Structured tabular     | size        |
|------------------------|-------------|
| vectra fast (LZ)       | 68.3 MB     |
| vectra ratio (LZ+Huff) | 56.8 MB     |
| fst zstd               | **33.8 MB** |

The two big architectural levers we have left:

1.  **FSE instead of Huffman** — closes ~10–15% of the entropy gap
2.  **Optimal parsing** — closes ~15–25% of the parsing gap

This plan covers (2). FSE is a separate, smaller plan and should land
first.

## What “optimal parsing” actually means

LZ77-family compressors decide at each position whether to emit a
literal or a match, and if a match, which length to take. Greedy parsing
(current LZ) takes the **longest match available right now**. That is
locally optimal but globally suboptimal: a shorter match now can let a
much cheaper match start one byte later, and the literal-vs-match
overheads are not symmetric.

Optimal parsing computes the *globally* minimum-cost parse via dynamic
programming over byte positions. The cost function is the **exact bit
cost** each token would contribute to the encoded stream. With a static
cost model (no entropy stage), this is purely structural. With an
entropy stage in front, the literal cost depends on the Huffman/FSE
table, which depends on the parse — a chicken-and-egg problem solved by
1–2 iterations.

zstd’s `btopt` (level 17+) and `btultra` (level 19+) implement this. The
gain on structured data is typically 5–15% additional compression on top
of greedy.

## Cost model (LZ-specific)

This is the part that needs to be exactly right or the parser is wrong.
From `tdc/src/entropy/lz.c` lines 137–181:

**Per-sequence header:** - 1 byte tag `[LLLLMMMM]` - 2 bytes offset
(uint16, offset_m1 little-endian) - Literal-length extension: 0 bytes if
`lit_len < 15`, else `(lit_len - 15) / 255 + 1` bytes (chained-255
varint) - Match-length extension: 0 bytes if `match_len - 3 < 15`, else
`leb128_size(match_len - 3 - 15)` bytes

**Per-literal byte:** 1 byte in the literal stream (= 8 bits) when there
is no entropy stage in front. With Huffman/FSE in front, the cost
becomes whatever the per-symbol code length is (typically 4–6 bits for
shuffled numeric data).

**Trailing literals** (after the last match) live in the literal stream
only, no sequence header. The optimal parser needs to model this: if the
run from position `p` to end-of-block is cheaper as trailing literals
than as a final match, take the literal path.

**Defining `cost(literal)` and `cost(match, len, off)`:**

    cost_literal(byte b)
      = 8                                    // no entropy stage
      | huffman_code_length[b]               // with Huffman
      | fse_state_bits[b]                    // with FSE (fractional, scale ×256)

    cost_match(len, off)
      = 8 * (3                               // tag byte + 2 offset bytes
           + (len - 3 - 15 >= 0
                ? leb128_size(len - 3 - 15)
                : 0))

Note: literal-length extension bytes are **not** charged to the match —
they are charged to the literal run that *precedes* the match. This
matters for the DP transition (see below).

## Algorithm

Forward DP, identical in shape to zstd’s `btopt`:

    state:  cost[0..N]    // bits, INF initially except cost[0] = 0
            prev[0..N]    // backtrack: (token_type, len, off)

    cost[0] = 0
    for p in 0..N:
      if cost[p] == INF: continue

      // Transition 1: emit one literal
      c = cost[p] + cost_literal(src[p])
      if c < cost[p+1]:
        cost[p+1] = c
        prev[p+1] = LITERAL

      // Transition 2: emit a match starting at p
      for each (len, off) in matches_at(p):     // all plausible matches
        // For matches the literal_run before this match is determined by
        // backtracking from prev[]; we account for the run-length-extension
        // overhead here when it crosses the 15-byte threshold.
        seq_overhead = match_seq_cost(len, off, run_len_at(p))
        c = cost[p] + seq_overhead
        if c < cost[p+len]:
          cost[p+len] = c
          prev[p+len] = MATCH(len, off)

    backtrack from prev[N] to recover the parse

**Run-length crossing:** the 15-byte literal-extension threshold means a
match’s “true” cost depends on whether emitting it pushes the preceding
literal run from \<15 to ≥15 bytes (or 270, 525, etc.). The clean fix is
to fold the literal-run extension cost into `cost_literal()` when
crossing a threshold:

    cost_literal_with_run(p)
      = cost_literal(src[p])
      + 8 * literal_extension_delta(run_len(p) → run_len(p)+1)

This keeps the DP transitions independent and additive — exactly what
the shortest-path formulation requires. Track `run_len` in the DP state
alongside `cost`, or encode it positionally (the run length at position
`p` is `p - last_match_end[p]`, which is recoverable from the backtrack
chain).

The simplest correct version: **add literal run length to the DP
state**, so `cost[p][r]` = best cost to reach position `p` with a
pending literal run of length `r`. The state space grows by ×16 in the
common case (runs ≥15 are rare and can be capped). Forward transitions:

- Literal:
  `cost[p+1][r+1] = min(., cost[p][r] + literal_byte_cost(src[p]))`
- Match:
  `cost[p+len][0] = min(., cost[p][r] + run_extension_cost(r) + match_seq_cost(len, off))`

This is the formulation zstd uses. The 16× state inflation is real but
the inner work per state is tiny.

## Match finder

Optimal parsing requires *all* plausible matches at each position, not
just one. The current LZ match finder is a single-slot hash table — at
most one candidate per 4-byte hash. We need a structure that supports
“give me up to K matches starting at position p, sorted by length
descending.”

Two viable options, in order of effort:

### Option A: Hash chain (deflate-style)

Each hash bucket is a chain of positions. Walk the chain up to a depth
limit (e.g. 32 or 128 candidates), measuring match length at each. Stop
when: - Chain depth limit reached - Position falls outside the 64K
window - A match of `nice_match_len` bytes is found (early termination)

**Implementation cost:** ~150 lines of C, replaces the current htab.
**Memory:** `prev[64K]` array for chain links + the existing 16-bit hash
table = 256 KB. **Time:** ~5–10× greedy encode time at depth 32, ~20× at
depth 128.

### Option B: Binary tree (zstd btopt-style)

A binary search tree over suffixes, keyed by 4-byte prefix. Insertion
and lookup are both O(log n) average, and lookup naturally returns
matches sorted by length.

**Implementation cost:** ~400 lines of C. **Memory:** ~1.5 MB per encode
block (two `prev[]` arrays for left/right children). **Time:** ~10–20×
greedy encode time, but with better cache behavior and a shorter ramp-up
than long hash chains.

**Recommendation:** start with hash chains (Option A). They are widely
used (deflate, lz4hc), well-understood, and good enough to validate the
cost model and DP transitions before sinking effort into a tree. Promote
to binary tree if profiling shows the match finder is the bottleneck.

## Two-pass entropy interaction

When LZ sits in front of an entropy coder (Huffman or FSE), the literal
cost in the DP is *not* a constant — it is the bit length of that
literal in the entropy table. But the entropy table is built from the
literal stream that the parser produces. Chicken-and-egg.

**Standard solution (zstd):**

    pass 1: greedy parse           → produces literal stream L1
            build entropy table from L1
    pass 2: optimal parse with L1's table  → produces literal stream L2
            build entropy table from L2
            (optional) pass 3: optimal parse with L2's table → L3

Two passes captures most of the gain. Three passes is rarely worth it.
The literal distribution doesn’t shift much between passes 1 and 2
because the greedy parse already covers \>90% of byte positions
correctly.

**For ratio mode specifically:** the entropy stage is Huffman today, FSE
soon. The cost model in the DP needs to call into the entropy stage to
ask “what is the bit cost of byte b under your current table?” This is a
small addition to the entropy vtable:

``` c
typedef struct {
  // existing fields...
  // NEW:
  void   *(*build_table)(const uint8_t *src, size_t n, void *user);
  void    (*free_table)(void *table);
  uint8_t (*symbol_bits)(const void *table, uint8_t sym);  // for Huffman
  uint16_t (*symbol_bits_q8)(const void *table, uint8_t sym); // for FSE
                                                              // (fixed-point ×256)
} tdc_entropy_vt;
```

The fixed-point ×256 cost lets FSE participate without losing fractional
bits. The DP runs in `q8` units throughout (`uint32_t cost[]`).

## Where it lives

This is **tdc** code, not vectra code. The plan touches three files in
the tdc repo and three in vectra:

### tdc

| File | Action |
|----|----|
| `src/entropy/lz_opt.c` | New file: optimal parser + match finder |
| `src/entropy/lz.c` | Add second vtable entry: `tdc_entropy_lz_opt_vt` |
| `include/tdc/entropy.h` | New entropy id `TDC_ENTROPY_LZ_OPT`, extend vtable with `symbol_bits` if going the proper two-pass route |
| `bench/RESULTS.md` | Add LZ_OPT row |

The new file should be a sibling of `lz.c`, not a replacement. The
greedy encoder is the right default for `compress = "fast"` and stays
untouched. The decoder is **shared**: optimal-parsed and greedy-parsed
streams use the exact same on-disk format, so `lz_decode_core` handles
both with no changes. This is critical — optimal parsing must be a pure
encode-side optimization. Existing .vtr files round-trip without
touching the decoder.

### vectra

| File | Action |
|----|----|
| `src/vtr_codec.h` | New compression tag `VTR_COMP_SHUFFLE_LZOPT_HUFF` (or rename existing tags) + new compress level `VTR_COMPRESS_MAX = 3` |
| `src/vtr_codec.c` | `vtr_compress_shuffled()` learns a third branch: at MAX, use the LZ_OPT vtable instead of LZ + still chain Huffman/FSE |
| `R/write.R` | `compress = c("fast", "ratio", "max", "none")` |
| `src/r_bridge.c` and `src/r_bridge_io.c` | Parse “max” → comp_level 3 |
| `tests/testthat/test-compression.R` | Round-trip tests for max mode + size comparison vs ratio |

**Naming note:** “max” is one option. Alternatives: -
`compress = c("fast", "ratio", "max")` -
`compress = c("fast", "ratio", "best")` ← matches `gzip -9` convention -
`compress = c("fast", "balanced", "high")` ← bigger rename, breaks
“ratio”

Recommend `"max"` because it’s short, unambiguous, and orders naturally
alongside “fast” and “ratio”.

## Step-by-step execution

### Phase 1: Static cost model + optimal parser, no entropy interaction

Goal: prove the DP and match finder are correct against the existing LZ
on-disk format, with literal cost fixed at 8 bits/byte.

1.  Create `tdc/src/entropy/lz_opt.c` with hash-chain match finder
    (Option A)
2.  Implement forward DP with the 16-state-per-position formulation
    (track pending literal run length)
3.  Implement backtrack pass that emits the same `LZSeq` array shape the
    greedy encoder uses, then call into the existing `lz_encode_core`
    serializer (refactored to take a pre-built sequence array as input)
4.  Add `tdc_entropy_lz_opt_vt` exposing this as a new entropy id
5.  Bench against greedy LZ on tdc/bench data — expected gain is 5–15%
    on structured data even without the entropy hookup
6.  Round-trip tests: encode with opt, decode with the existing greedy
    decoder, byte-for-byte match the input

Phase 1 alone is shippable and gives us a measurable gain on
`compress = "fast"` for a write-time slowdown that users opt into via
`compress = "max"`. It is a clean checkpoint before tackling the entropy
interaction.

### Phase 2: Entropy-aware cost model

1.  Extend `tdc_entropy_vt` with the `build_table` / `symbol_bits_q8`
    calls
2.  Implement them for Huffman (trivial — code lengths are already
    there)
3.  Implement them for FSE (needs the FSE plan to land first)
4.  Wire two-pass parsing: greedy-then-optimal with the table from pass
    1
5.  Bench: expected additional 5–10% gain on structured data; test on
    the “structured tabular” dataset from `_bench_structured.R` which is
    the workload we are optimizing for
6.  Optional pass 3 if pass 2 gain is large enough to suggest the table
    is still drifting

### Phase 3: vectra integration

1.  Add `VTR_COMPRESS_MAX = 3` and the new compression tag to
    `vtr_codec.h`
2.  `vtr_compress_shuffled()` dispatches level 3 to LZ_OPT + Huffman/FSE
3.  R API + bridge updates
4.  Tests: round-trip on all data types, file-size comparison
5.  CLAUDE.md update documenting the new mode and its tradeoffs
6.  NEWS.md entry

### Phase 4 (optional): binary-tree match finder

If profiling shows the hash chain is the bottleneck (likely on long
inputs with high redundancy), implement Option B as `lz_opt_btree.c`.
Wire it as a separate vtable entry or as an internal switch behind a
`params` field on `lz_opt`.

## Testing strategy

- **Round-trip parity:** every optimal-parsed block must decode
  byte-for-byte identical to the input under the existing decoder
- **Greedy ≤ optimal in size:** for every test input, optimal output ≤
  greedy output. If optimal is ever larger, the cost model is wrong
- **Cost matches reality:** after parsing, sum the cost-model bits and
  compare to actual encoded byte count × 8. They should match exactly in
  Phase 1 and match within 1% in Phase 2 (FSE has fractional rounding)
- **Random fuzz:** generate random byte streams + structured streams
  (DIFF, PLANE2D outputs from tdc) and run round-trip in CI
- **Existing test corpus:** all current vtr1.c tests pass with
  `compress = "max"` substituted for `compress = "fast"`

## Risk register

| Risk | Mitigation |
|----|----|
| DP cost model has an off-by-one and produces *larger* output than greedy | Phase 1 tests greedy ≤ optimal in size as a hard invariant; CI fails on regression |
| Encode time blows up on long inputs | Match finder has a chain depth limit (default 32, configurable). Profile against tdc/bench inputs before declaring done |
| Memory usage spikes on large blocks | DP arrays are O(N×16), so a 1 MB block needs 64 MB of state. Cap block size at 256 KB for the DP and chain blocks if needed (the existing LZ already operates per-block) |
| Two-pass entropy interaction never converges | Cap at 2 passes; pass 1 is greedy + table-build, pass 2 is optimal + final emit. zstd does this and it works |
| The 64 KB window limits the gain | The plan does NOT widen the window. That is a separate, format-breaking change and has its own plan |
| FSE plan slips and Phase 2 is blocked | Phase 1 ships standalone with the static cost model. Users get a smaller “max” mode immediately even before FSE lands |

## Out of scope

These belong in separate plans:

- **FSE replacement for Huffman in ratio mode** — different file,
  different plan, lands first
- **Window size larger than 64 KB** — format-breaking, requires a new
  compression tag and a different match finder data structure
- **Repcode optimization** (zstd’s 3 most-recent offsets) —
  format-breaking, meaningful win but complicates the DP transitions
- **Per-stream entropy** (separate Huffman tables for literals vs match
  lengths vs offsets) — meaningful win but requires extending the LZ
  on-disk format with multiple entropy headers

## Success metric

`_bench_structured.R` shows `compress = "max"` within **30% of fst
zstd** on the structured tabular dataset (~44 MB target vs current 56.8
MB), with encode time ≤ 5× of `compress = "ratio"` and decode time
within 10% of `compress = "ratio"` (decoder is unchanged so decode time
should be identical modulo cache effects).

If Phase 1 alone hits within 15% of greedy (~58 MB → ~50 MB on the
structured tabular case), the architecture is proven and Phase 2 is
straightforward FSE plumbing.
