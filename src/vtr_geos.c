/* Shared GEOS setup (see vtr_geos.h). */

#include "vtr_geos.h"

static int vtr_geos_api_ready = 0;

void vtr_geos_ensure_api(void) {
    if (!vtr_geos_api_ready) { libgeos_init_api(); vtr_geos_api_ready = 1; }
}

void vtr_geos_quiet_handler(const char *message, void *userdata) {
    (void) message; (void) userdata;
}
