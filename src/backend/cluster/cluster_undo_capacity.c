/* Read-only capacity certificates never authorize a write or release. */
#include "postgres.h"

#include "cluster/cluster_undo_capacity.h"
#ifndef CLUSTER_UNDO_CAPACITY_UNIT_TEST
#include "storage/ipc.h"
#endif

static ClusterUndoCapacityChangeFence *capacity_fence;
static unsigned capacity_scopes;
static bool capacity_writer_registered;
#ifndef CLUSTER_UNDO_CAPACITY_UNIT_TEST
static bool capacity_exit_registered;
#endif

static void
capacity_sequence_advance(void)
{
	uint64 sequence = pg_atomic_read_u64(&capacity_fence->change_sequence);

	while (sequence != UINT64_MAX
		   && !pg_atomic_compare_exchange_u64(&capacity_fence->change_sequence, &sequence,
											  sequence + 1))
		;
}

static void
capacity_writer_leave(void)
{
	if (capacity_writer_registered) {
		/* The final visible mutation precedes retirement. Sequence exhaustion
		 * poisons observations for this entire original shared-memory domain. */
		capacity_sequence_advance();
		pg_atomic_fetch_sub_u64(&capacity_fence->inflight_writers, 1);
		capacity_writer_registered = false;
	}
}

#ifndef CLUSTER_UNDO_CAPACITY_UNIT_TEST
static void
capacity_writer_exit(int code, Datum arg)
{
	/* Original before_shmem_exit owners finish first. This retires only this
	 * process's invalidation registration, never its mutation/release debt.
	 * A hard crash instead retires the original postmaster shared domain. */
	capacity_scopes = 0;
	capacity_writer_leave();
}
#endif

void
cluster_undo_capacity_fence_init(ClusterUndoCapacityChangeFence *fence)
{
	pg_atomic_init_u64(&fence->change_sequence, 1);
	pg_atomic_init_u64(&fence->inflight_writers, 0);
}

void
cluster_undo_capacity_fence_attach(ClusterUndoCapacityChangeFence *fence)
{
	Assert(capacity_scopes == 0 && !capacity_writer_registered);
	capacity_fence = fence;
}

bool
cluster_undo_capacity_write_scope_begin(void)
{
	if (capacity_fence == NULL)
		return false;
	if (capacity_scopes == UINT_MAX) {
		pg_atomic_write_u64(&capacity_fence->change_sequence, UINT64_MAX);
		return false;
	}
#ifndef CLUSTER_UNDO_CAPACITY_UNIT_TEST
	if (!capacity_exit_registered) {
		on_shmem_exit(capacity_writer_exit, (Datum)0);
		capacity_exit_registered = true;
	}
#endif
	capacity_scopes++;
	return true;
}

void
cluster_undo_capacity_write_mark(void)
{
	uint64 writers;

	/* Raw private-array algorithms also use this hook. Only a production
	 * shared writer's scope can register; tests/private codecs stay private. */
	if (capacity_fence == NULL || capacity_scopes == 0 || capacity_writer_registered)
		return;
	writers = pg_atomic_read_u64(&capacity_fence->inflight_writers);
	for (;;) {
		if (writers == UINT64_MAX) {
			pg_atomic_write_u64(&capacity_fence->change_sequence, UINT64_MAX);
			return;
		}
		if (pg_atomic_compare_exchange_u64(&capacity_fence->inflight_writers, &writers,
										   writers + 1))
			break;
	}
	capacity_writer_registered = true;
	capacity_sequence_advance();
}

void
cluster_undo_capacity_write_scope_end(bool entered)
{
	if (!entered)
		return;
	Assert(capacity_scopes != 0);
	if (capacity_scopes == 0) {
		pg_atomic_write_u64(&capacity_fence->change_sequence, UINT64_MAX);
		return;
	}
	if (--capacity_scopes == 0)
		capacity_writer_leave();
}

bool
cluster_undo_capacity_change_sample(uint64 *sequence)
{
	uint64 before;

	if (capacity_fence == NULL || sequence == NULL)
		return false;
	before = pg_atomic_read_u64(&capacity_fence->change_sequence);
	pg_read_barrier();
	if (before == 0 || before == UINT64_MAX
		|| pg_atomic_read_u64(&capacity_fence->inflight_writers) != 0)
		return false;
	pg_read_barrier();
	if (before != pg_atomic_read_u64(&capacity_fence->change_sequence))
		return false;
	*sequence = before;
	return true;
}

bool
cluster_undo_capacity_change_current(uint64 sequence)
{
	uint64 current;

	return cluster_undo_capacity_change_sample(&current) && sequence == current;
}
