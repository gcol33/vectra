#include "error.h"
#include <stdarg.h>
#include <stdio.h>

void vectra_error(const char *fmt, ...) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    Rf_error("%s", buf);
}
