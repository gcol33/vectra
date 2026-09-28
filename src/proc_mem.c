#include "proc_mem.h"

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>

double vec_peak_rss_bytes(void) {
    PROCESS_MEMORY_COUNTERS pmc;
    if (!K32GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
        return -1;
    return (double)pmc.PeakWorkingSetSize;
}

#else
#include <sys/resource.h>

double vec_peak_rss_bytes(void) {
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) != 0) return -1;
#ifdef __APPLE__
    return (double)ru.ru_maxrss;           /* bytes */
#else
    return (double)ru.ru_maxrss * 1024.0;  /* kilobytes */
#endif
}
#endif
