/*
 * src/core/error.c
 *
 * tdc_strerror — static string lookup for tdc_status codes.
 *
 * The contract (tdc/error.h):
 *   - never NULL
 *   - never allocates
 *   - returns a static string literal
 *
 * Unknown codes fall through to "unknown tdc_status" rather than asserting,
 * because tdc_strerror is the function callers reach for WHEN something has
 * already gone wrong; it must not itself be a source of new failures.
 */

#include "tdc/error.h"

const char *tdc_strerror(tdc_status s) {
    switch (s) {
        case TDC_OK:              return "ok";
        case TDC_E_INVAL:         return "invalid argument";
        case TDC_E_NOMEM:         return "out of memory";
        case TDC_E_UNSUPPORTED:   return "unsupported model/transform/entropy id";
        case TDC_E_DTYPE:         return "dtype not accepted by stage";
        case TDC_E_LAYOUT:        return "layout not accepted by stage";
        case TDC_E_SHAPE:         return "shape rank or dims invalid";
        case TDC_E_BUF_TOO_SMALL: return "destination buffer too small";
        case TDC_E_CORRUPT:       return "on-disk header or payload failed validation";
        case TDC_E_VERSION:       return "container or block version not understood";
        case TDC_E_IO:            return "i/o error";
        default:                  return "unknown tdc_status";
    }
}
