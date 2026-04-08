/*
 * src/entropy/deflate.c
 *
 * TDC_ENTROPY_DEFLATE — zlib deflate wrapper. Optional dependency,
 * gated by a TDC_HAVE_ZLIB compile flag. When zlib is not linked, the
 * vtable returns NULL from tdc_entropy_get(TDC_ENTROPY_DEFLATE) and
 * encode paths must avoid selecting it.
 *
 * Kept ONLY for the "ratio" mode (slower but smaller). vectra's policy
 * is no external compression libraries in the .vtr core; deflate
 * survives because zlib is already linked for CSV.gz and TIFF, and
 * because some users genuinely want max ratio over max speed. It will
 * be removed once a native entropy coder (huffman / FSE / ANS) lands
 * that hits comparable ratios.
 *
 * Source today: SHUFFLE_DEFLATE path in vectra/src/vtr_codec.c.
 */

#include "tdc/entropy.h"
