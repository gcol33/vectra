#ifndef VTR_FILEOPS_H
#define VTR_FILEOPS_H

#include <stdio.h>
#include <stdint.h>

/*
 * Platform shims for the file operations the .vtr writers need beyond
 * fread / fwrite.
 */

/*
 * vtr_atomic_replace(tmp_path, path)
 *
 * Atomically replace `path` with `tmp_path`. Returns 0 on success, -1 on
 * failure. Used by write_vtr / append_vtr / delete_vtr to finish a
 * "write to side file, then swap" sequence.
 *
 * POSIX: single rename(2) call — atomic and overwrites the target.
 *
 * Windows: MoveFileExA with MOVEFILE_REPLACE_EXISTING, retried briefly on
 * ERROR_SHARING_VIOLATION / ERROR_ACCESS_DENIED. Vectra readers memory-map
 * the file and the mmap can outlive the R-visible handle until GC runs,
 * so a recently-read file can still block the replace for a few hundred
 * milliseconds after the reader closed.
 */
int vtr_atomic_replace(const char *tmp_path, const char *path);

/* Length of the open file in bytes, or -1 if it cannot be determined.
   Leaves the stream position where it found it. */
int64_t vtr_file_size(FILE *fp);

/*
 * Truncate the open file to `length` bytes. Returns 0 on success, -1 on
 * failure. Used to roll back an aborted column append: that path writes
 * past the container's trailing index before the header that would
 * reference those bytes is patched in, so discarding them keeps a
 * repeatedly-failing append from growing the file without bound.
 *
 * The caller is responsible for flushing the stream first.
 */
int vtr_file_truncate(FILE *fp, int64_t length);

#endif
