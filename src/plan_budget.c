#include "plan_budget.h"
#include "error.h"
#include <stdint.h>
#include <stdlib.h>

struct VecMemPool {
    int64_t total;
    int64_t shared_total;   /* total minus every grant's floor */
    int64_t shared_used;
    int     refs;           /* one per grant */
};

static int64_t excess(const VecMemGrant *g, int64_t held) {
    return held > g->floor ? held - g->floor : 0;
}

VecMemGrant *vec_mem_grant_ref(VecMemGrant *g) {
    if (g) g->refs++;
    return g;
}

void vec_mem_grant_unref(VecMemGrant *g) {
    if (!g || --g->refs > 0) return;
    VecMemPool *p = g->pool;
    p->shared_used -= excess(g, g->held);
    if (--p->refs == 0) free(p);
    free(g);
}

/* Move grant g's reservation by d bytes; refused (0) if growing past its
   floor would overdraw the shared pool. */
static int grant_move(VecMemGrant *g, int64_t d, int force) {
    int64_t before = excess(g, g->held);
    int64_t after  = excess(g, g->held + d);
    VecMemPool *p = g->pool;
    if (!force && after > before &&
        p->shared_used + (after - before) > p->shared_total)
        return 0;
    p->shared_used += after - before;
    g->held += d;
    return 1;
}

VecMemAcct vec_mem_acct(VecMemGrant *grant, int64_t cap) {
    VecMemAcct a;
    a.grant = vec_mem_grant_ref(grant);
    a.cap = cap;
    a.held = 0;
    return a;
}

int vec_mem_try(VecMemAcct *a, int64_t want) {
    if (want < 0) want = 0;
    if (want <= a->held) { vec_mem_set(a, want); return 1; }
    if (!a->grant) {
        if (a->cap > 0 && want > a->cap) return 0;
        a->held = want;
        return 1;
    }
    if (!grant_move(a->grant, want - a->held, 0)) return 0;
    a->held = want;
    return 1;
}

void vec_mem_set(VecMemAcct *a, int64_t bytes) {
    if (bytes < 0) bytes = 0;
    if (a->grant) grant_move(a->grant, bytes - a->held, 1);
    a->held = bytes;
}

int64_t vec_mem_allowance(const VecMemAcct *a) {
    if (!a->grant)
        return a->cap > 0 ? a->cap : INT64_MAX;
    const VecMemGrant *g = a->grant;
    const VecMemPool *p = g->pool;
    int64_t own = g->floor > g->held ? g->floor - g->held : 0;
    int64_t shared = p->shared_total - p->shared_used;
    if (shared < 0) shared = 0;
    return a->held + own + shared;
}

void vec_mem_acct_rebind(VecMemAcct *a, VecMemGrant *grant) {
    int64_t held = a->held;
    vec_mem_set(a, 0);
    vec_mem_grant_unref(a->grant);
    a->grant = vec_mem_grant_ref(grant);
    vec_mem_set(a, held);
}

void vec_mem_acct_free(VecMemAcct *a) {
    vec_mem_set(a, 0);
    vec_mem_grant_unref(a->grant);
    a->grant = NULL;
}

void vec_node_set_budgeted(VecNode *node, int64_t request,
                           SetBudgetFn set_grant) {
    node->set_mem_budget = set_grant;
    node->mem_request = request;
}

void vec_node_clear_budgeted(VecNode *node) {
    node->set_mem_budget = NULL;
    node->mem_request = 0;
}

typedef struct {
    VecNode **nodes;
    int       n, cap;
} NodeList;

static void list_push(NodeList *l, VecNode *node) {
    if (l->n == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 16;
        VecNode **grown = (VecNode **)realloc(l->nodes,
                                              (size_t)l->cap * sizeof(VecNode *));
        if (!grown) { free(l->nodes); vectra_error("alloc failed in plan walk"); }
        l->nodes = grown;
    }
    l->nodes[l->n++] = node;
}

/* Every node reachable from root; iterative, so plan depth is not bounded by
   the C stack. */
static NodeList plan_nodes(VecNode *root) {
    NodeList all = {0}, stack = {0};
    list_push(&stack, root);
    while (stack.n > 0) {
        VecNode *node = stack.nodes[--stack.n];
        list_push(&all, node);
        if (!node->children) continue;
        int k = node->children(node, NULL, 0);
        if (k <= 0) continue;
        VecNode **kids = (VecNode **)malloc((size_t)k * sizeof(VecNode *));
        if (!kids) vectra_error("alloc failed in plan walk");
        node->children(node, kids, k);
        for (int i = 0; i < k; i++)
            if (kids[i]) list_push(&stack, kids[i]);
        free(kids);
    }
    free(stack.nodes);
    return all;
}

void vec_plan_assign_budgets(VecNode *root) {
    if (!root) return;
    NodeList all = plan_nodes(root);
    int n = 0;
    int64_t total = 0;
    for (int i = 0; i < all.n; i++) {
        VecNode *node = all.nodes[i];
        /* <= 0 means unbounded (never spills): not part of the pool. */
        if (!node->set_mem_budget || node->mem_request <= 0) continue;
        n++;
        if (node->mem_request > total) total = node->mem_request;
    }
    if (n == 0) { free(all.nodes); return; }

    VecMemPool *pool = (VecMemPool *)calloc(1, sizeof(VecMemPool));
    if (!pool) { free(all.nodes); vectra_error("alloc failed for memory pool"); }
    int64_t floor = total / (4 * (int64_t)n);
    pool->total = total;
    pool->shared_total = total - floor * n;
    pool->refs = 1;
    for (int i = 0; i < all.n; i++) {
        VecNode *node = all.nodes[i];
        if (!node->set_mem_budget || node->mem_request <= 0) continue;
        VecMemGrant *g = (VecMemGrant *)calloc(1, sizeof(VecMemGrant));
        if (!g) { free(all.nodes); vectra_error("alloc failed for memory grant"); }
        g->pool = pool;
        g->floor = floor;
        g->refs = 1;
        pool->refs++;
        node->set_mem_budget(node, g);
        vec_mem_grant_unref(g);   /* the node holds its own reference */
    }
    /* Drop the creation reference: the pool now lives as long as a grant. */
    if (--pool->refs == 0) free(pool);
    free(all.nodes);
}
