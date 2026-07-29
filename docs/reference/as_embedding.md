# Encode vectors as an embedding column

Packs numeric vectors into the hex float32 blobs vectra stores embedding
columns as. Write the result as an ordinary character column; the
distance functions
[`cosine()`](https://gillescolling.com/vectra/reference/embedding_distance.md),
[`l2()`](https://gillescolling.com/vectra/reference/embedding_distance.md),
and
[`dot()`](https://gillescolling.com/vectra/reference/embedding_distance.md)
decode it inside the engine.

## Usage

``` r
as_embedding(x)
```

## Arguments

- x:

  A numeric matrix (one embedding per row), a list of equal-length
  numeric vectors, or a single numeric vector (one embedding).

## Value

A character vector with one hex-encoded blob per embedding.

## See also

[`cosine()`](https://gillescolling.com/vectra/reference/embedding_distance.md),
[`l2()`](https://gillescolling.com/vectra/reference/embedding_distance.md),
[`dot()`](https://gillescolling.com/vectra/reference/embedding_distance.md)

## Examples

``` r
m <- matrix(rnorm(12), nrow = 3)        # 3 embeddings of length 4
emb <- as_embedding(m)
df <- data.frame(id = 1:3, emb = emb)
```
