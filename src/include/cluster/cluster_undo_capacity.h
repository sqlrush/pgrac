/* Local invalidation of read-only undo capacity observations. */
#ifndef CLUSTER_UNDO_CAPACITY_H
#define CLUSTER_UNDO_CAPACITY_H

#include "port/atomics.h"

typedef struct ClusterUndoCapacityChangeFence {
	pg_atomic_uint64 change_sequence;
	pg_atomic_uint64 inflight_writers;
} ClusterUndoCapacityChangeFence;

StaticAssertDecl(sizeof(ClusterUndoCapacityChangeFence) == 16,
				 "capacity invalidation fence must remain two atomic counters");
StaticAssertDecl(offsetof(ClusterUndoCapacityChangeFence, inflight_writers) == 8,
				 "capacity counters must retain atomic alignment");

extern void cluster_undo_capacity_fence_init(ClusterUndoCapacityChangeFence *fence);
extern void cluster_undo_capacity_fence_attach(ClusterUndoCapacityChangeFence *fence);
/* Reference-counted scopes may overlap and end in any order. Lazy mark must
 * precede the first actual mutation under its original protection. Merely
 * scanning under an exclusive lock does not invalidate a negative sample. */
extern bool cluster_undo_capacity_write_scope_begin(void);
extern void cluster_undo_capacity_write_mark(void);
extern void cluster_undo_capacity_write_scope_end(bool entered);
extern bool cluster_undo_capacity_change_sample(uint64 *sequence);
extern bool cluster_undo_capacity_change_current(uint64 sequence);

#endif
