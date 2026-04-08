/*
 * src/entropy/huffman.c
 *
 * Reserved post-v0. Static-Huffman entropy stage, intended as the first
 * native replacement for deflate's literal compression once the LZ2
 * matcher and a Huffman literal coder are connected.
 *
 * Vtable not registered until implementation lands.
 */

#include "tdc/entropy.h"
