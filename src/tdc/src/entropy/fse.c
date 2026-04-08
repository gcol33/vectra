/*
 * src/entropy/fse.c
 *
 * Reserved post-v0. Finite-State Entropy / tabled-ANS coder, intended
 * as the eventual replacement for the LZ2 entropy stage. The fse.h
 * placeholder header in vectra/src/codec/entropy/ predates this file
 * and will be moved here when implementation begins.
 *
 * Per the design notes (vectra/CLAUDE.md):
 *   "Don't re-attempt batched LZ2 parse/copy decoupling until a real
 *    entropy stage (FSE / Huffman) lives in front of LZ2, at which
 *    point the parse phase becomes expensive enough that decoupling
 *    is a clear win."
 *
 * Vtable not registered until implementation lands.
 */

#include "tdc/entropy.h"
