#include "plan_budget.h"
#include "error.h"
#include <stdlib.h>

void vec_node_set_budgeted(VecNode *node, int64_t request,
                           SetBudgetFn set_budget) {
    node->set_mem_budget = set_budget;
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
    int n_budgeted = 0;
    for (int i = 0; i < all.n; i++)
        if (all.nodes[i]->set_mem_budget) n_budgeted++;
    for (int i = 0; i < all.n; i++) {
        VecNode *node = all.nodes[i];
        if (!node->set_mem_budget) continue;
        int64_t req = node->mem_request;
        /* <= 0 means unbounded (never spills); that is not divisible. */
        int64_t share = req > 0 ? req / n_budgeted : req;
        if (req > 0 && share < 1) share = 1;
        node->set_mem_budget(node, share);
    }
    free(all.nodes);
}
