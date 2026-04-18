#include "vtr_atomic_rename.h"
#include <stdio.h>

#ifdef _WIN32
#include <windows.h>

int vtr_atomic_replace(const char *tmp_path, const char *path) {
    /* Cumulative wait ~1 s across 7 tries, which comfortably covers the
       typical R GC/mmap-release window after a preceding tbl() read. */
    const DWORD backoffs_ms[] = {0, 25, 50, 100, 150, 250, 400};
    const size_t n_tries = sizeof(backoffs_ms) / sizeof(backoffs_ms[0]);

    for (size_t i = 0; i < n_tries; i++) {
        if (backoffs_ms[i]) Sleep(backoffs_ms[i]);
        if (MoveFileExA(tmp_path, path, MOVEFILE_REPLACE_EXISTING))
            return 0;
        DWORD err = GetLastError();
        if (err != ERROR_SHARING_VIOLATION && err != ERROR_ACCESS_DENIED)
            return -1;
    }
    return -1;
}

#else

int vtr_atomic_replace(const char *tmp_path, const char *path) {
    return rename(tmp_path, path);
}

#endif
