#include "file_map.h"

#include <stddef.h>
#include <string.h>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <sys/types.h>
#  include <sys/stat.h>
#  include <sys/mman.h>
#  include <fcntl.h>
#  include <unistd.h>
#endif

static void map_reset(VecFileMap *m) {
    memset(m, 0, sizeof(*m));
    m->fd = -1;
}

int vec_file_map_open(VecFileMap *m, const char *path) {
    if (!m) return 0;
    map_reset(m);
    if (!path) return 0;

#ifdef _WIN32
    /* FILE_SHARE_DELETE lets a writer rename a replacement over this file while
       the mapping is live, as far as the share mode is concerned; the section
       itself can still refuse, which is why the writer retries. */
    HANDLE hf = CreateFileA(path, GENERIC_READ,
                            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                            NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hf == INVALID_HANDLE_VALUE) return 0;

    LARGE_INTEGER sz;
    if (!GetFileSizeEx(hf, &sz) || sz.QuadPart <= 0) {
        CloseHandle(hf);
        return 0;
    }
    /* A 32-bit build cannot address a large file, and MapViewOfFile with a
       length of 0 maps to the end of the file, so refuse rather than truncate. */
    if ((unsigned long long)sz.QuadPart > (unsigned long long)((size_t)-1)) {
        CloseHandle(hf);
        return 0;
    }

    HANDLE hm = CreateFileMappingA(hf, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!hm) {
        CloseHandle(hf);
        return 0;
    }
    void *view = MapViewOfFile(hm, FILE_MAP_READ, 0, 0, 0);
    if (!view) {
        CloseHandle(hm);
        CloseHandle(hf);
        return 0;
    }

    m->base   = (const uint8_t *)view;
    m->size   = (int64_t)sz.QuadPart;
    m->h_file = (void *)hf;
    m->h_map  = (void *)hm;
    return 1;
#else
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return 0;
    }
    if ((unsigned long long)st.st_size > (unsigned long long)((size_t)-1)) {
        close(fd);
        return 0;
    }

    void *p = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) {
        close(fd);
        return 0;
    }

    m->base = (const uint8_t *)p;
    m->size = (int64_t)st.st_size;
    m->fd   = fd;
    return 1;
#endif
}

void vec_file_map_close(VecFileMap *m) {
    if (!m) return;
    if (!m->base) {
        map_reset(m);
        return;
    }
#ifdef _WIN32
    UnmapViewOfFile((LPCVOID)m->base);
    if (m->h_map)  CloseHandle((HANDLE)m->h_map);
    if (m->h_file) CloseHandle((HANDLE)m->h_file);
#else
    munmap((void *)m->base, (size_t)m->size);
    if (m->fd >= 0) close(m->fd);
#endif
    map_reset(m);
}
