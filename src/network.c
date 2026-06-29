/*
 * network.c — native shortest-path engine over a line-network graph.
 *
 * The compute kernel behind spatial_network() / spatial_route() /
 * spatial_service_area(). The graph is built once from a line layer and held
 * resident in an external pointer; the R side then streams origin (and
 * destination) batches past it. The graph is the resident budget (bounded by the
 * network size, like a resident sf `y`); each query adds one Dijkstra's label
 * arrays per worker thread, so the query side scales while the graph stays put.
 *
 * Entry points (all .Call):
 *   C_network_build(from, to, w, n_nodes) -> externalptr to a CSR graph
 *   C_network_route(ptr, src, dst, want_path) -> list(cost, paths)
 *   C_network_service(ptr, src, budget)        -> list per origin (node, cost)
 *   C_network_stats(ptr) -> integer(3) = c(n_nodes, n_edges, n_components)
 *
 * The graph is directed: R expands an undirected or two-way line into both
 * forward and reverse directed edges before building, so this file only ever
 * sees a directed edge list. Each directed edge carries its 1:1 id (its row in
 * R's directed-edge table) so a reconstructed route comes back as a sequence of
 * edge ids the R side maps to source geometry.
 *
 * The solver is plain Dijkstra with a lazy-deletion binary min-heap over a CSR
 * adjacency (no igraph dependency — the heap and the relaxation loop are a few
 * dozen lines). A per-thread generation stamp avoids an O(n) reset between the
 * many independent single-source runs, so a batch of origins parallelises with
 * one OpenMP loop over read-only shared CSR and disjoint output slots.
 *
 * Threading discipline: R's allocator and Rf_error are not thread-safe, so the
 * parallel regions touch only plain C buffers (malloc/realloc are fine) and a
 * shared error flag; every SEXP is allocated serially before or after the region.
 */

#include <R.h>
#include <Rinternals.h>

#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#include "vec_omp.h"

/* ---------- graph ------------------------------------------------------- */

typedef struct {
    int     n_nodes;
    int     n_edges;       /* number of directed edges */
    int     n_components;  /* weakly-connected components (undirected view) */
    int    *head;          /* size n_nodes+1, CSR row offsets */
    int    *adj_to;        /* size n_edges, target node of each adjacency slot */
    double *adj_w;         /* size n_edges, edge weight */
    int    *adj_eid;       /* size n_edges, directed-edge id (R row, 0-based) */
} NetGraph;

static void net_free(NetGraph *g) {
    if (!g) return;
    free(g->head);
    free(g->adj_to);
    free(g->adj_w);
    free(g->adj_eid);
    free(g);
}

static void net_finalize(SEXP ext) {
    NetGraph *g = (NetGraph *)R_ExternalPtrAddr(ext);
    if (g) {
        net_free(g);
        R_ClearExternalPtr(ext);
    }
}

static NetGraph *net_unwrap(SEXP ext) {
    if (TYPEOF(ext) != EXTPTRSXP)
        Rf_error("expected an external pointer to a network graph");
    NetGraph *g = (NetGraph *)R_ExternalPtrAddr(ext);
    if (!g) Rf_error("network graph is closed");
    return g;
}

/* ---------- union-find (weakly-connected component count) --------------- */

static int uf_find(int *p, int x) {
    while (p[x] != x) { p[x] = p[p[x]]; x = p[x]; }
    return x;
}

static void uf_union(int *p, int a, int b) {
    a = uf_find(p, a);
    b = uf_find(p, b);
    if (a != b) p[a] = b;
}

/* ---------- C_network_build -------------------------------------------- */

/*
 * from, to : integer, 0-based node ids of each directed edge
 * w        : numeric, edge weight (>= 0)
 * n_nodes  : integer scalar
 */
SEXP C_network_build(SEXP from_sexp, SEXP to_sexp, SEXP w_sexp,
                     SEXP nnodes_sexp) {
    R_xlen_t ne = Rf_xlength(from_sexp);
    if (Rf_xlength(to_sexp) != ne || Rf_xlength(w_sexp) != ne)
        Rf_error("network: from / to / weight must have equal length");
    if (ne > INT_MAX)
        Rf_error("network: too many edges");
    int n_edges = (int)ne;
    int n_nodes = Rf_asInteger(nnodes_sexp);
    if (n_nodes < 0) Rf_error("network: n_nodes must be non-negative");

    const int    *from = INTEGER(from_sexp);
    const int    *to   = INTEGER(to_sexp);
    const double *w    = REAL(w_sexp);

    NetGraph *g = (NetGraph *)calloc(1, sizeof(NetGraph));
    if (!g) Rf_error("network: out of memory");
    g->n_nodes = n_nodes;
    g->n_edges = n_edges;
    g->head    = (int *)calloc((size_t)n_nodes + 1, sizeof(int));
    g->adj_to  = n_edges ? (int *)malloc((size_t)n_edges * sizeof(int))    : NULL;
    g->adj_w   = n_edges ? (double *)malloc((size_t)n_edges * sizeof(double)) : NULL;
    g->adj_eid = n_edges ? (int *)malloc((size_t)n_edges * sizeof(int))    : NULL;
    if (!g->head || (n_edges && (!g->adj_to || !g->adj_w || !g->adj_eid))) {
        net_free(g);
        Rf_error("network: out of memory");
    }

    /* validate + count out-degree per node */
    for (int i = 0; i < n_edges; i++) {
        int s = from[i], t = to[i];
        if (s < 0 || s >= n_nodes || t < 0 || t >= n_nodes) {
            net_free(g);
            Rf_error("network: edge node id out of range");
        }
        if (!(w[i] >= 0.0)) {       /* also rejects NA / NaN */
            net_free(g);
            Rf_error("network: edge weights must be finite and non-negative");
        }
        g->head[s + 1]++;
    }
    for (int v = 0; v < n_nodes; v++) g->head[v + 1] += g->head[v];

    /* scatter into CSR */
    int *pos = (int *)malloc((size_t)(n_nodes ? n_nodes : 1) * sizeof(int));
    if (!pos) { net_free(g); Rf_error("network: out of memory"); }
    for (int v = 0; v < n_nodes; v++) pos[v] = g->head[v];
    for (int i = 0; i < n_edges; i++) {
        int slot = pos[from[i]]++;
        g->adj_to[slot]  = to[i];
        g->adj_w[slot]   = w[i];
        g->adj_eid[slot] = i;
    }
    free(pos);

    /* weakly-connected components */
    if (n_nodes > 0) {
        int *p = (int *)malloc((size_t)n_nodes * sizeof(int));
        if (!p) { net_free(g); Rf_error("network: out of memory"); }
        for (int v = 0; v < n_nodes; v++) p[v] = v;
        for (int i = 0; i < n_edges; i++) uf_union(p, from[i], to[i]);
        int nc = 0;
        for (int v = 0; v < n_nodes; v++) if (uf_find(p, v) == v) nc++;
        g->n_components = nc;
        free(p);
    } else {
        g->n_components = 0;
    }

    SEXP ext = PROTECT(R_MakeExternalPtr(g, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ext, net_finalize, TRUE);
    UNPROTECT(1);
    return ext;
}

/* ---------- per-thread Dijkstra scratch -------------------------------- */

typedef struct {
    int     n;
    double *dist;   /* valid iff gen[v] == cur */
    int    *gen;    /* "has a tentative distance this run" stamp */
    int    *fin;    /* "finalized this run" stamp */
    int    *tgt;    /* "is a target this run" stamp (route mode) */
    int    *pnode;  /* predecessor node on the shortest path */
    int    *peid;   /* directed-edge id used to reach this node */
    /* binary min-heap of (key, node), lazy deletion */
    double *hk;
    int    *hn;
    int     hcap;
    int     hsize;
    int     cur;    /* current run generation */
    int     err;    /* set on a heap allocation failure inside a parallel run */
} Scratch;

static int scratch_init(Scratch *s, int n) {
    int cap = n ? n : 1;
    s->n = n;
    s->cur = 0;
    s->err = 0;
    s->dist  = (double *)malloc((size_t)cap * sizeof(double));
    s->gen   = (int *)calloc((size_t)cap, sizeof(int));
    s->fin   = (int *)calloc((size_t)cap, sizeof(int));
    s->tgt   = (int *)calloc((size_t)cap, sizeof(int));
    s->pnode = (int *)malloc((size_t)cap * sizeof(int));
    s->peid  = (int *)malloc((size_t)cap * sizeof(int));
    s->hcap  = 1024;
    s->hk    = (double *)malloc((size_t)s->hcap * sizeof(double));
    s->hn    = (int *)malloc((size_t)s->hcap * sizeof(int));
    s->hsize = 0;
    if (!s->dist || !s->gen || !s->fin || !s->tgt || !s->pnode || !s->peid ||
        !s->hk || !s->hn)
        return 0;
    return 1;
}

static void scratch_free(Scratch *s) {
    free(s->dist); free(s->gen); free(s->fin); free(s->tgt);
    free(s->pnode); free(s->peid); free(s->hk); free(s->hn);
}

/* push onto the heap; on allocation failure set err and drop the entry (the
 * caller's err check aborts the run, so a dropped entry never yields a wrong
 * answer that is reported as success). */
static void heap_push(Scratch *s, double k, int node) {
    if (s->hsize == s->hcap) {
        int nc = s->hcap * 2;
        double *nk = (double *)realloc(s->hk, (size_t)nc * sizeof(double));
        if (!nk) { s->err = 1; return; }
        s->hk = nk;
        int *nn = (int *)realloc(s->hn, (size_t)nc * sizeof(int));
        if (!nn) { s->err = 1; return; }
        s->hn = nn;
        s->hcap = nc;
    }
    int i = s->hsize++;
    s->hk[i] = k; s->hn[i] = node;
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (s->hk[parent] <= s->hk[i]) break;
        double tk = s->hk[parent]; s->hk[parent] = s->hk[i]; s->hk[i] = tk;
        int tn = s->hn[parent]; s->hn[parent] = s->hn[i]; s->hn[i] = tn;
        i = parent;
    }
}

/* pop the minimum into *outk/*outn; returns 0 when empty */
static int heap_pop(Scratch *s, double *outk, int *outn) {
    if (s->hsize == 0) return 0;
    *outk = s->hk[0]; *outn = s->hn[0];
    s->hsize--;
    if (s->hsize > 0) {
        s->hk[0] = s->hk[s->hsize];
        s->hn[0] = s->hn[s->hsize];
        int i = 0;
        for (;;) {
            int l = 2 * i + 1, r = 2 * i + 2, m = i;
            if (l < s->hsize && s->hk[l] < s->hk[m]) m = l;
            if (r < s->hsize && s->hk[r] < s->hk[m]) m = r;
            if (m == i) break;
            double tk = s->hk[m]; s->hk[m] = s->hk[i]; s->hk[i] = tk;
            int tn = s->hn[m]; s->hn[m] = s->hn[i]; s->hn[i] = tn;
            i = m;
        }
    }
    return 1;
}

/*
 * One single-source Dijkstra. `cur` must already be bumped (and route targets
 * marked) by the caller. Stops early when `remaining` distinct targets are all
 * finalized (route; budget = +Inf) or when the next node exceeds `budget`
 * (service). Results land in the scratch dist/pnode/peid arrays, valid where
 * gen[v] == cur.
 *
 * remaining: number of distinct unsettled targets to wait for; < 0 disables the
 *            target early-stop (service mode).
 * budget:    expand only while the popped key <= budget; +Inf disables it.
 */
static void dijkstra(const NetGraph *g, Scratch *s, int src,
                     int remaining, double budget) {
    int cur = s->cur;
    s->hsize = 0;
    s->dist[src]  = 0.0;
    s->gen[src]   = cur;
    s->pnode[src] = -1;
    s->peid[src]  = -1;
    heap_push(s, 0.0, src);

    double key; int u;
    while (heap_pop(s, &key, &u)) {
        if (s->err) return;
        if (s->gen[u] != cur || key > s->dist[u]) continue;  /* stale entry */
        if (s->fin[u] == cur) continue;                       /* already done */
        if (key > budget) break;                              /* service cutoff */
        s->fin[u] = cur;
        if (remaining >= 0 && s->tgt[u] == cur) {
            if (--remaining == 0) break;                      /* all targets in */
        }
        int end = g->head[u + 1];
        for (int slot = g->head[u]; slot < end; slot++) {
            int v = g->adj_to[slot];
            double nd = s->dist[u] + g->adj_w[slot];
            if (s->gen[v] != cur || nd < s->dist[v]) {
                s->dist[v]  = nd;
                s->gen[v]   = cur;
                s->pnode[v] = u;
                s->peid[v]  = g->adj_eid[slot];
                heap_push(s, nd, v);
                if (s->err) return;
            }
        }
    }
}

/* ---------- C_network_route -------------------------------------------- */

/* qsort comparator on a (src, idx) pair array, by src ascending */
typedef struct { int src; int idx; } Pair;
static int pair_cmp(const void *a, const void *b) {
    int sa = ((const Pair *)a)->src, sb = ((const Pair *)b)->src;
    return (sa > sb) - (sa < sb);
}

/*
 * ptr       : the graph
 * src, dst  : integer, 0-based node ids of each (origin, destination) pair
 * want_path : logical(1)
 *
 * Returns list(cost = numeric(m), paths = list(m) | NULL). cost is +Inf for an
 * unreachable pair and 0 for src == dst. Each path is an integer vector of
 * directed-edge ids (0-based) in travel order, empty when unreachable or trivial.
 */
SEXP C_network_route(SEXP ptr, SEXP src_sexp, SEXP dst_sexp, SEXP want_path_sexp) {
    NetGraph *g = net_unwrap(ptr);
    R_xlen_t m = Rf_xlength(src_sexp);
    if (Rf_xlength(dst_sexp) != m)
        Rf_error("network: src and dst must have equal length");
    int want_path = Rf_asLogical(want_path_sexp) == TRUE;
    const int *src = INTEGER(src_sexp);
    const int *dst = INTEGER(dst_sexp);
    int mm = (int)m;

    /* validate endpoints up front (serial, may Rf_error) */
    for (int i = 0; i < mm; i++)
        if (src[i] < 0 || src[i] >= g->n_nodes ||
            dst[i] < 0 || dst[i] >= g->n_nodes)
            Rf_error("network: route endpoint out of range");

    SEXP cost = PROTECT(Rf_allocVector(REALSXP, m));
    double *costp = REAL(cost);

    /* per-pair path buffers filled in parallel, materialized to SEXP after */
    int **pathbuf = NULL;
    int  *pathlen = NULL;
    if (want_path) {
        pathbuf = (int **)calloc((size_t)(mm ? mm : 1), sizeof(int *));
        pathlen = (int *)calloc((size_t)(mm ? mm : 1), sizeof(int));
        if (!pathbuf || !pathlen) {
            free(pathbuf); free(pathlen); UNPROTECT(1);
            Rf_error("network: out of memory");
        }
    }

    /* group pairs by source so one Dijkstra serves all of a source's targets */
    Pair *ord = (Pair *)malloc((size_t)(mm ? mm : 1) * sizeof(Pair));
    int  *gstart = (int *)malloc((size_t)(mm + 1) * sizeof(int));
    if (!ord || !gstart) {
        free(ord); free(gstart); free(pathbuf); free(pathlen); UNPROTECT(1);
        Rf_error("network: out of memory");
    }
    for (int i = 0; i < mm; i++) { ord[i].src = src[i]; ord[i].idx = i; }
    qsort(ord, (size_t)mm, sizeof(Pair), pair_cmp);
    int ngroups = 0;
    for (int i = 0; i < mm; ) {
        gstart[ngroups++] = i;
        int s = ord[i].src;
        while (i < mm && ord[i].src == s) i++;
    }
    gstart[ngroups] = mm;

    int nthreads = 1;
#ifdef _OPENMP
    if (mm >= 256) nthreads = vec_omp_threads();
#endif
    int alloc_fail = 0;

    #pragma omp parallel num_threads(nthreads)
    {
        Scratch sc;
        int ok = scratch_init(&sc, g->n_nodes);
        if (!ok) {
            #pragma omp atomic write
            alloc_fail = 1;
        }
        if (ok) {
            #pragma omp for schedule(dynamic, 8)
            for (int gi = 0; gi < ngroups; gi++) {
                int a = gstart[gi], b = gstart[gi + 1];
                int s = ord[a].src;
                sc.cur++;
                int cur = sc.cur;

                int remaining = 0;
                for (int j = a; j < b; j++) {
                    int d = dst[ord[j].idx];
                    if (d == s) continue;
                    if (sc.tgt[d] != cur) { sc.tgt[d] = cur; remaining++; }
                }

                dijkstra(g, &sc, s, remaining, R_PosInf);
                if (sc.err) {
                    #pragma omp atomic write
                    alloc_fail = 1;
                    continue;
                }

                for (int j = a; j < b; j++) {
                    int i = ord[j].idx;
                    int d = dst[i];
                    if (d == s) { costp[i] = 0.0; continue; }
                    if (sc.gen[d] == cur && R_FINITE(sc.dist[d])) {
                        costp[i] = sc.dist[d];
                        if (want_path) {
                            int len = 0;
                            for (int v = d; v != s; v = sc.pnode[v]) len++;
                            int *pe = (int *)malloc((size_t)(len ? len : 1)
                                                    * sizeof(int));
                            if (!pe) {
                                #pragma omp atomic write
                                alloc_fail = 1;
                            } else {
                                int idx = len - 1;
                                for (int v = d; v != s; v = sc.pnode[v])
                                    pe[idx--] = sc.peid[v];
                                pathbuf[i] = pe;
                                pathlen[i] = len;
                            }
                        }
                    } else {
                        costp[i] = R_PosInf;
                    }
                }
            }
            scratch_free(&sc);
        }
    }

    free(ord);
    free(gstart);

    if (alloc_fail) {
        if (pathbuf) for (int i = 0; i < mm; i++) free(pathbuf[i]);
        free(pathbuf); free(pathlen); UNPROTECT(1);
        Rf_error("network: out of memory");
    }

    /* materialize paths serially */
    SEXP paths = R_NilValue;
    int nprot = 1;
    if (want_path) {
        paths = PROTECT(Rf_allocVector(VECSXP, m)); nprot++;
        for (int i = 0; i < mm; i++) {
            SEXP pth = Rf_allocVector(INTSXP, pathlen[i]);
            SET_VECTOR_ELT(paths, i, pth);
            if (pathlen[i])
                memcpy(INTEGER(pth), pathbuf[i],
                       (size_t)pathlen[i] * sizeof(int));
            free(pathbuf[i]);
        }
        free(pathbuf); free(pathlen);
    }

    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2)); nprot++;
    SET_VECTOR_ELT(out, 0, cost);
    SET_VECTOR_ELT(out, 1, paths);
    SEXP nms = PROTECT(Rf_allocVector(STRSXP, 2)); nprot++;
    SET_STRING_ELT(nms, 0, Rf_mkChar("cost"));
    SET_STRING_ELT(nms, 1, Rf_mkChar("paths"));
    Rf_setAttrib(out, R_NamesSymbol, nms);
    UNPROTECT(nprot);
    return out;
}

/* ---------- C_network_service ------------------------------------------ */

/*
 * ptr    : the graph
 * src    : integer, 0-based origin node ids
 * budget : numeric(1), maximum cumulative cost to reach a node
 *
 * Returns a list, one element per origin: list(node = integer(), cost =
 * numeric()) of every node reachable within `budget` (origin included, cost 0),
 * in non-decreasing cost order.
 */
SEXP C_network_service(SEXP ptr, SEXP src_sexp, SEXP budget_sexp) {
    NetGraph *g = net_unwrap(ptr);
    R_xlen_t m = Rf_xlength(src_sexp);
    const int *src = INTEGER(src_sexp);
    double budget = Rf_asReal(budget_sexp);
    if (!(budget >= 0.0)) Rf_error("network: cost budget must be non-negative");
    int mm = (int)m;

    for (int i = 0; i < mm; i++)
        if (src[i] < 0 || src[i] >= g->n_nodes)
            Rf_error("network: service origin out of range");

    /* per-origin reached-node / cost buffers, filled in parallel */
    int    **nodebuf = (int **)calloc((size_t)(mm ? mm : 1), sizeof(int *));
    double **costbuf = (double **)calloc((size_t)(mm ? mm : 1), sizeof(double *));
    int     *cnt     = (int *)calloc((size_t)(mm ? mm : 1), sizeof(int));
    if (!nodebuf || !costbuf || !cnt) {
        free(nodebuf); free(costbuf); free(cnt);
        Rf_error("network: out of memory");
    }

    int nthreads = 1;
#ifdef _OPENMP
    if (mm >= 64) nthreads = vec_omp_threads();
#endif
    int alloc_fail = 0;

    #pragma omp parallel num_threads(nthreads)
    {
        Scratch sc;
        int ok = scratch_init(&sc, g->n_nodes);
        if (!ok) {
            #pragma omp atomic write
            alloc_fail = 1;
        }
        if (ok) {
            #pragma omp for schedule(dynamic, 8)
            for (int i = 0; i < mm; i++) {
                int s = src[i];
                sc.cur++;
                int cur = sc.cur;

                int rcap = 256, nr = 0;
                int *rbuf = (int *)malloc((size_t)rcap * sizeof(int));
                if (!rbuf) {
                    #pragma omp atomic write
                    alloc_fail = 1;
                    continue;
                }

                sc.hsize = 0;
                sc.dist[s] = 0.0; sc.gen[s] = cur;
                sc.pnode[s] = -1; sc.peid[s] = -1;
                heap_push(&sc, 0.0, s);
                double key; int u;
                int failed = 0;
                while (heap_pop(&sc, &key, &u)) {
                    if (sc.err) { failed = 1; break; }
                    if (sc.gen[u] != cur || key > sc.dist[u]) continue;
                    if (sc.fin[u] == cur) continue;
                    if (key > budget) break;
                    sc.fin[u] = cur;
                    if (nr == rcap) {
                        rcap *= 2;
                        int *nb = (int *)realloc(rbuf, (size_t)rcap * sizeof(int));
                        if (!nb) { failed = 1; break; }
                        rbuf = nb;
                    }
                    rbuf[nr++] = u;
                    int end = g->head[u + 1];
                    for (int slot = g->head[u]; slot < end; slot++) {
                        int v = g->adj_to[slot];
                        double nd = sc.dist[u] + g->adj_w[slot];
                        if (sc.gen[v] != cur || nd < sc.dist[v]) {
                            sc.dist[v]  = nd;
                            sc.gen[v]   = cur;
                            sc.pnode[v] = u;
                            sc.peid[v]  = g->adj_eid[slot];
                            heap_push(&sc, nd, v);
                            if (sc.err) { failed = 1; break; }
                        }
                    }
                    if (failed) break;
                }
                if (failed) {
                    free(rbuf);
                    #pragma omp atomic write
                    alloc_fail = 1;
                    continue;
                }

                double *cb = (double *)malloc((size_t)(nr ? nr : 1)
                                              * sizeof(double));
                if (!cb) {
                    free(rbuf);
                    #pragma omp atomic write
                    alloc_fail = 1;
                    continue;
                }
                for (int k = 0; k < nr; k++) cb[k] = sc.dist[rbuf[k]];
                nodebuf[i] = rbuf;
                costbuf[i] = cb;
                cnt[i] = nr;
            }
            scratch_free(&sc);
        }
    }

    if (alloc_fail) {
        for (int i = 0; i < mm; i++) { free(nodebuf[i]); free(costbuf[i]); }
        free(nodebuf); free(costbuf); free(cnt);
        Rf_error("network: out of memory");
    }

    SEXP out = PROTECT(Rf_allocVector(VECSXP, m));
    for (int i = 0; i < mm; i++) {
        int nr = cnt[i];
        /* anchor elt in out first, then fill its slots, so an allocation that
         * triggers GC cannot collect a not-yet-referenced child */
        SEXP elt = Rf_allocVector(VECSXP, 2);
        SET_VECTOR_ELT(out, i, elt);
        SET_VECTOR_ELT(elt, 0, Rf_allocVector(INTSXP, nr));
        SET_VECTOR_ELT(elt, 1, Rf_allocVector(REALSXP, nr));
        if (nr) {
            memcpy(INTEGER(VECTOR_ELT(elt, 0)), nodebuf[i],
                   (size_t)nr * sizeof(int));
            memcpy(REAL(VECTOR_ELT(elt, 1)), costbuf[i],
                   (size_t)nr * sizeof(double));
        }
        free(nodebuf[i]);
        free(costbuf[i]);
    }
    free(nodebuf); free(costbuf); free(cnt);
    UNPROTECT(1);
    return out;
}

/* ---------- C_network_stats -------------------------------------------- */

SEXP C_network_stats(SEXP ptr) {
    NetGraph *g = net_unwrap(ptr);
    SEXP out = PROTECT(Rf_allocVector(INTSXP, 3));
    int *p = INTEGER(out);
    p[0] = g->n_nodes;
    p[1] = g->n_edges;
    p[2] = g->n_components;
    UNPROTECT(1);
    return out;
}
