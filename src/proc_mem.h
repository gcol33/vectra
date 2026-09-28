#ifndef VECTRA_PROC_MEM_H
#define VECTRA_PROC_MEM_H

/* Peak resident set size of this process in bytes (Windows: peak working
   set; Linux: VmHWM via ru_maxrss; macOS: ru_maxrss), or -1 where the
   platform does not report it. */
double vec_peak_rss_bytes(void);

#endif /* VECTRA_PROC_MEM_H */
