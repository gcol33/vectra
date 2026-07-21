/*
 * node_rows.c - static (metadata-only) row counts for a plan.
 *
 * Backs nrow() on a lazy query. Each node kind that can name its output row
 * count without reading data implements VecNode.static_rows: a scan reads it
 * off the row-group index, the row-preserving verbs delegate to their child,
 * limit and top-n clamp the child's count, concat sums its children. Verbs
 * whose output length depends on the data (filter, joins, grouped aggregation,
 * distinct) leave the hook NULL and are reported as unknown.
 *
 * The default is unknown, so a node kind added later reports NA until it opts
 * in - never a wrong count.
 */

#include "types.h"

int64_t vec_node_static_rows(const VecNode *node) {
    if (!node || !node->static_rows) return -1;
    return node->static_rows(node);
}
