#ifndef VTR_ATOMIC_RENAME_H
#define VTR_ATOMIC_RENAME_H

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

#endif
