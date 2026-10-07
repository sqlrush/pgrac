/* Actual QVOTEC selector, original voting I/O and original PGFM codec.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <fcntl.h>
#include <unistd.h>
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_storage_quorum.h"
#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#undef strerror
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = false;
int cluster_node_id = 0;
bool cluster_storage_quorum_allows_node(int node);
bool
cluster_storage_quorum_allows_node(int node)
{
	return node == 0;
}

#include "utils/memutils.h"
MemoryContext TopMemoryContext;
void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	(void)context;
	return calloc(1, size);
}

#include "test_cluster_formation_restart.inc"

static int fds[3];
static char paths[3][128];
static ClusterVotingSlot matrix[3 * CLUSTER_MAX_NODES];
static ClusterFormationDiskSnapshot snapshot;
static uint8 committed[CLUSTER_VOTING_SLOT_BYTES];

static void
fixture(void)
{
	uint8 empty[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES] = { 0 };
	ClusterFenceMarker fence;
	cluster_fence_marker_build_baseline(&fence, 0, empty, 0, 0, -1);
	for (int d = 0; d < 3; ++d) {
		snprintf(paths[d], sizeof(paths[d]), "/tmp/p3b-formation-%ld-%d-XXXXXX", (long)getpid(), d);
		fds[d] = mkstemp(paths[d]);
		UT_ASSERT(fds[d] >= 0);
		UT_ASSERT_EQ(cluster_voting_disk_format(fds[d], CLUSTER_MAX_NODES, d),
					 CLUSTER_VOTING_DISK_IO_OK);
		UT_ASSERT_EQ(ftruncate(fds[d], CLUSTER_VOTING_PGRD_FILE_BYTES_MIN), 0);
		UT_ASSERT_EQ(fdatasync(fds[d]), 0);
		UT_ASSERT_EQ(cluster_voting_disk_read_slot(fds[d], d, 0, &matrix[d * CLUSTER_MAX_NODES]),
					 CLUSTER_VOTING_DISK_IO_OK);
		cluster_fence_marker_pack(matrix[d * CLUSTER_MAX_NODES]._reserved1, &fence);
		UT_ASSERT_EQ(cluster_voting_disk_write_slot(fds[d], &matrix[d * CLUSTER_MAX_NODES]),
					 CLUSTER_VOTING_DISK_IO_OK);
	}
}

static void
cleanup(void)
{
	for (int d = 0; d < 3; ++d) {
		close(fds[d]);
		unlink(paths[d]);
	}
}

static void
write_fence(int d, int node, uint64 epoch)
{
	ClusterVotingSlot slot;
	ClusterFenceMarker fence;
	uint8 empty[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES] = { 0 };
	UT_ASSERT_EQ(cluster_voting_disk_read_slot(fds[d], d, node, &slot), CLUSTER_VOTING_DISK_IO_OK);
	cluster_fence_marker_build_baseline(&fence, epoch, empty, 0, 0, -1);
	cluster_fence_marker_pack(slot._reserved1, &fence);
	UT_ASSERT_EQ(cluster_voting_disk_write_slot(fds[d], &slot), CLUSTER_VOTING_DISK_IO_OK);
}

static void
write_formation(int d, int node, uint64 generation, uint64 epoch, uint64 nonce)
{
	uint64 incs[CLUSTER_MAX_NODES] = { 66, 67 };
	uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
	ClusterFormationCommitMarker m = { 0 };
	m.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	m.version = CLUSTER_FORMATION_MARKER_VERSION;
	m.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	m.formation_generation = generation;
	m.formation_epoch = epoch;
	m.arbiter_node = 0;
	m.arbiter_incarnation = 66;
	m.commit_nonce = nonce;
	m.admitted_nodes[0] = 3;
	m.n_admitted = 2;
	UT_ASSERT(cluster_formation_marker_encode(&m, incs, bytes));
	UT_ASSERT_EQ(cluster_voting_disk_write_formation_slot(fds[d], node, bytes),
				 CLUSTER_VOTING_DISK_IO_OK);
}

static bool
scan(void)
{
	bool all = true;
	for (int d = 0; d < 3; ++d)
		for (int n = 0; n < CLUSTER_MAX_NODES; ++n)
			if (cluster_voting_disk_read_slot(fds[d], d, n, &matrix[d * CLUSTER_MAX_NODES + n])
				!= CLUSTER_VOTING_DISK_IO_OK)
				all = false;
	return qvotec_formation_disk_snapshot(fds, 3, matrix, all, 1000, 77, &snapshot, committed);
}

UT_TEST(fresh_disk_has_only_real_epoch_zero_majority)
{
	fixture();
	UT_ASSERT(scan());
	UT_ASSERT(snapshot.complete);
	UT_ASSERT_EQ(snapshot.fence.agree_disk_count, 3);
	UT_ASSERT_EQ(snapshot.max_epoch, 0);
	UT_ASSERT_EQ(snapshot.max_generation, 0);
	UT_ASSERT_EQ(committed[0], 0);
	cleanup();
}

UT_TEST(restart_recovers_original_committed_generation_and_fence)
{
	fixture();
	for (int d = 0; d < 3; ++d) {
		write_fence(d, 0, 7);
		for (int n = 0; n < 2; ++n)
			write_formation(d, n, 3, 7, 12);
	}
	UT_ASSERT(scan());
	UT_ASSERT_EQ(snapshot.max_epoch, 7);
	UT_ASSERT_EQ(snapshot.max_generation, 3);
	UT_ASSERT_EQ(snapshot.fence.marker.fence_epoch, 7);
	{
		ClusterFormationCommitMarker m;
		uint64 incs[CLUSTER_MAX_NODES];
		UT_ASSERT(cluster_formation_marker_decode(committed, &m, incs));
		UT_ASSERT_EQ(m.commit_nonce, 12);
		UT_ASSERT_EQ(incs[1], 67);
	}
	/* Same media, subsequent formation; never reformat between rounds. */
	for (int d = 0; d < 3; ++d) {
		write_fence(d, 0, 8);
		for (int n = 0; n < 2; ++n)
			write_formation(d, n, 4, 8, 13);
	}
	UT_ASSERT(scan());
	UT_ASSERT_EQ(snapshot.max_epoch, 8);
	UT_ASSERT_EQ(snapshot.max_generation, 4);
	cleanup();
}

UT_TEST(fence_only_crash_retains_successor_floor)
{
	fixture();
	for (int d = 0; d < 3; ++d)
		write_fence(d, 0, 1);
	UT_ASSERT(scan());
	UT_ASSERT_EQ(snapshot.max_epoch, 1);
	UT_ASSERT_EQ(snapshot.max_generation, 0);
	UT_ASSERT_EQ(committed[0], 0);
	cleanup();
}

UT_TEST(minority_fence_raises_floor_without_becoming_authority)
{
	fixture();
	for (int d = 0; d < 3; ++d)
		write_fence(d, 0, 7);
	write_fence(2, 0, 9);
	UT_ASSERT(scan());
	UT_ASSERT_EQ(snapshot.max_epoch, 9);
	UT_ASSERT_EQ(snapshot.fence.marker.fence_epoch, 7);
	UT_ASSERT_EQ(snapshot.fence.agree_disk_count, 2);
	cleanup();
}

UT_TEST(partial_marker_is_not_a_whole_cohort_commit)
{
	fixture();
	for (int d = 0; d < 3; ++d)
		write_formation(d, 0, 4, 10, 14);
	write_formation(0, 1, 4, 10, 14);
	UT_ASSERT(scan());
	UT_ASSERT_EQ(snapshot.max_generation, 4);
	UT_ASSERT_EQ(snapshot.max_epoch, 10);
	UT_ASSERT_EQ(committed[0], 0);
	write_formation(1, 1, 4, 10, 14);
	UT_ASSERT(scan());
	UT_ASSERT_NE(committed[0], 0);
	cleanup();
}

UT_TEST(conflicting_generation_is_not_selected)
{
	fixture();
	write_formation(0, 0, 4, 10, 14);
	write_formation(1, 0, 4, 10, 15);
	UT_ASSERT(!scan());
	UT_ASSERT(!snapshot.complete);
	UT_ASSERT_EQ(committed[0], 0);
	cleanup();
}

UT_TEST(incomplete_or_corrupt_reads_do_not_publish_a_bound)
{
	for (int bad = 0; bad < 4; ++bad) {
		uint8 byte = 0xff;
		fixture();
		write_formation(0, 0, 4, 10, 14);
		if (bad == 0)
			UT_ASSERT_EQ(pwrite(fds[0], &byte, 1, CLUSTER_VOTING_SLOT_OFFSET(0) + 12), 1);
		if (bad == 1)
			UT_ASSERT_EQ(pwrite(fds[0], &byte, 1, CLUSTER_VOTING_FORMATION_SLOT_OFFSET(0) + 24), 1);
		if (bad == 2)
			UT_ASSERT_EQ(ftruncate(fds[0], CLUSTER_VOTING_FORMATION_SLOT_OFFSET(1) + 8), 0);
		if (bad == 3) {
			close(fds[0]);
			fds[0] = -1;
		}
		UT_ASSERT(!scan());
		UT_ASSERT(!snapshot.complete);
		UT_ASSERT_EQ(committed[0], 0);
		cleanup();
	}
}

UT_TEST(unknown_predecessor_fence_cannot_be_erased_by_heartbeat)
{
	ClusterVotingSlot slot = { 0 };
	ClusterFenceMarker fence;
	uint8 empty[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES] = { 0 };
	UT_ASSERT(qvotec_formation_prior_fence_valid(&slot));
	cluster_fence_marker_build_baseline(&fence, 7, empty, 0, 0, -1);
	cluster_fence_marker_pack(slot._reserved1, &fence);
	UT_ASSERT(qvotec_formation_prior_fence_valid(&slot));
	fence.version++;
	memcpy(slot._reserved1, &fence, sizeof(fence));
	UT_ASSERT(!qvotec_formation_prior_fence_valid(&slot));
	fence.magic = 0;
	memcpy(slot._reserved1, &fence, sizeof(fence));
	UT_ASSERT(!qvotec_formation_prior_fence_valid(&slot));
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(fresh_disk_has_only_real_epoch_zero_majority);
	UT_RUN(restart_recovers_original_committed_generation_and_fence);
	UT_RUN(fence_only_crash_retains_successor_floor);
	UT_RUN(minority_fence_raises_floor_without_becoming_authority);
	UT_RUN(partial_marker_is_not_a_whole_cohort_commit);
	UT_RUN(conflicting_generation_is_not_selected);
	UT_RUN(incomplete_or_corrupt_reads_do_not_publish_a_bound);
	UT_RUN(unknown_predecessor_fence_cannot_be_erased_by_heartbeat);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
