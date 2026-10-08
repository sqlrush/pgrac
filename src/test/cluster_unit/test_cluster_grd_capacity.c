/*-------------------------------------------------------------------------
 * test_cluster_grd_capacity.c -- exact owners on the real GRD/GES path.
 *
 * Allocation, formation and transport reuse the handoff fixture. Capacity,
 * compatible grants, LMON decisions and exact reclamation execute product C.
 * The four node IDs below are offline identities, not a running cluster.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define PGRAC_HW_HANDOFF_EMBEDDED
#include "test_cluster_hw_handoff.c"
#include "cluster/cluster_ic_router.h"

/* Retain the standalone fixture's fail-stop control-service boundaries.
 * This suite never sends control retirement messages or uses that authority. */
bool
cluster_recovery_transport_components_current(void)
{
	return false;
}

bool
cluster_ges_dedup_retire_control_request(uint32 node pg_attribute_unused(),
										 uint32 procno pg_attribute_unused(),
										 uint64 epoch pg_attribute_unused(),
										 uint64 request pg_attribute_unused())
{
	abort();
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type pg_attribute_unused(), int32 dest pg_attribute_unused(),
						 const void *payload pg_attribute_unused(),
						 uint32 len pg_attribute_unused())
{
	abort();
}

typedef struct CapacityCounts {
	int entries;
	int holders;
	int waiters;
	int converts;
} CapacityCounts;

static void
capacity_count_row(void *context, const int32 fields[11])
{
	CapacityCounts *counts = context;

	counts->entries++;
	counts->holders += fields[7];
	counts->waiters += fields[8];
	counts->converts += fields[9];
}

static void
capacity_expect_counts(int entries, int holders)
{
	CapacityCounts counts = { 0 };

	cluster_grd_entries_walk(capacity_count_row, &counts);
	UT_ASSERT_EQ(counts.entries, entries);
	UT_ASSERT_EQ(counts.holders, holders);
	UT_ASSERT_EQ(counts.waiters, 0);
	UT_ASSERT_EQ(counts.converts, 0);
	UT_ASSERT_EQ(cluster_grd_entry_count(), entries);
}

static void
capacity_setup(ClusterLockAcquireRequest *request, ClusterGrdHolderId *lmon_holder)
{
	ClusterGrdShared *shared;
	bool found;

	queued_cut_case = true;
	queued_cut_prepare(GES_REQ_OPCODE_REQUEST, false, 0, request, lmon_holder);
	request->resid = (ClusterResId){ .field1 = 5,
									 .field2 = 16385,
									 .type = LOCKTAG_RELATION,
									 .lockmethodid = DEFAULT_LOCKMETHOD };
	cluster_node_id = 1;
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found && shared != NULL);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&request->resid)], 1);
	memcpy(master_request.resid, &request->resid, sizeof(request->resid));
	master_request.lockmode = RowExclusiveLock;
	master_work_pending = false;
	capacity_expect_counts(0, 0);
}

static ClusterGrdHolderId
capacity_holder(int index)
{
	ClusterGrdHolderId holder = grd_lifecycle_holder(index % 4, 100 + index, 1000 + index);

	holder.cluster_epoch = ut_mock_epoch;
	return holder;
}

static ClusterGrdGrantAction
capacity_acquire(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	ClusterGrdGrantAction action;
	int conflicts = -1;

	action = cluster_grd_entry_enqueue_or_grant(resid, holder, holder->node_id, holder->request_id,
												9, GES_REQ_OPCODE_REQUEST, RowExclusiveLock, NULL,
												&conflicts);
	UT_ASSERT_EQ(conflicts, 0);
	return action;
}

static void
capacity_release_present(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	if (cluster_grd_holder_mode_by_id(resid, holder, NULL)) {
		UT_ASSERT_EQ(cluster_grd_release_holder_by_id(resid, holder), CLUSTER_GRD_ENTRY_OK);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(resid, holder, NULL));
	}
}

/* A compatible owner must not disappear at the old per-resource boundary.
 * Keep scanning and clean actual owners even on RED, so the same run proves
 * both the missing grants and whether exact release leaves residual state. */
static void
capacity_compatible_owners(int count)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId lmon_holder;
	ClusterGrdHolderId holders[256];
	bool present[256] = { false };
	int granted = 0;
	int first_refused = 0;

	HW_CHECK(count > 0 && count <= lengthof(holders));
	capacity_setup(&request, &lmon_holder);
	for (int i = 0; i < count; i++) {
		ClusterGrdGrantAction action;
		LOCKMODE mode = NoLock;

		holders[i] = capacity_holder(i);
		action = capacity_acquire(&request.resid, &holders[i]);
		present[i] = cluster_grd_holder_mode_by_id(&request.resid, &holders[i], &mode);
		UT_ASSERT_EQ(present[i], action == CLUSTER_GRD_GRANT_NOW);
		if (present[i])
			UT_ASSERT_EQ(mode, RowExclusiveLock);
		if (action == CLUSTER_GRD_GRANT_NOW)
			granted++;
		else if (first_refused == 0)
			first_refused = i + 1;
	}
	printf("# compatible requested=%d granted=%d first_refused=%d\n", count, granted,
		   first_refused);
	UT_ASSERT_EQ(granted, count);
	capacity_expect_counts(1, count);
	for (int i = count - 1; i >= 0; i--) {
		capacity_release_present(&request.resid, &holders[i]);
		if (i > 0 && present[i - 1])
			UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[i - 1], NULL));
	}
	capacity_expect_counts(0, 0);
	printf("# compatible requested=%d cleanup_entries=%d\n", count, cluster_grd_entry_count());
	queued_cut_case = false;
	MyProc = NULL;
}

UT_TEST(compatible_16_control_releases_exact_owners)
{
	capacity_compatible_owners(16);
}

UT_TEST(compatible_17_owners_are_all_granted)
{
	capacity_compatible_owners(17);
}

UT_TEST(compatible_32_owners_are_all_granted)
{
	capacity_compatible_owners(32);
}

UT_TEST(compatible_256_owners_are_all_granted)
{
	capacity_compatible_owners(256);
}

/* Assert the required grant, not the baseline's generic refusal. The reply
 * is diagnostic evidence for this offline path, not a claim about the sole
 * source of any live rejection. Freeing one slot must also allow exact reuse. */
UT_TEST(real_lmon_grants_the_17th_compatible_owner)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId lmon_holder;
	ClusterGrdHolderId holders[16];
	LOCKMODE mode = NoLock;
	bool installed;

	capacity_setup(&request, &lmon_holder);
	for (int i = 0; i < lengthof(holders); i++) {
		holders[i] = capacity_holder(i);
		UT_ASSERT_EQ(capacity_acquire(&request.resid, &holders[i]), CLUSTER_GRD_GRANT_NOW);
	}
	capacity_expect_counts(1, 16);
	stage_master_work();
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(master_reply_count, 1);
	installed = cluster_grd_holder_mode_by_id(&request.resid, &lmon_holder, &mode);
	printf("# lmon owner=17 opcode=%u reason=%u installed=%d\n", master_reply.opcode,
		   master_reply.reject_reason, installed);
	UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(master_reply.reject_reason, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(master_reply.reply_for_opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(master_reply.holder_node_id, lmon_holder.node_id);
	UT_ASSERT_EQ(master_reply.holder_procno, lmon_holder.procno);
	UT_ASSERT_EQ(master_reply.holder_cluster_epoch_lo, (uint32)lmon_holder.cluster_epoch);
	UT_ASSERT_EQ(master_reply.holder_cluster_epoch_hi, (uint32)(lmon_holder.cluster_epoch >> 32));
	UT_ASSERT_EQ(master_reply.holder_request_id_lo, (uint32)lmon_holder.request_id);
	UT_ASSERT_EQ(master_reply.holder_request_id_hi, (uint32)(lmon_holder.request_id >> 32));
	UT_ASSERT(memcmp(master_reply.resid, &request.resid, sizeof(request.resid)) == 0);
	UT_ASSERT(installed);
	if (installed)
		UT_ASSERT_EQ(mode, RowExclusiveLock);
	capacity_expect_counts(1, 17);

	/* Reissue the same identity after release, with no deadline change. */
	capacity_release_present(&request.resid, &lmon_holder);
	capacity_release_present(&request.resid, &holders[0]);
	stage_master_work();
	master_reply_count = 0;
	memset(&master_reply, 0, sizeof(master_reply));
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(master_reply_count, 1);
	UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(master_reply.reject_reason, GES_REJECT_REASON_NONE);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &lmon_holder, &mode));
	UT_ASSERT_EQ(mode, RowExclusiveLock);
	capacity_expect_counts(1, 16);
	capacity_release_present(&request.resid, &lmon_holder);
	for (int i = 1; i < lengthof(holders); i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_expect_counts(0, 0);
	printf("# lmon exact_reuse_granted=%d cleanup_entries=%d\n",
		   master_reply.opcode == GES_REPLY_OPCODE_GRANT, cluster_grd_entry_count());
	queued_cut_case = false;
	MyProc = NULL;
}

int
main(void)
{
	MyBackendType = B_BACKEND;
	setvbuf(stdout, NULL, _IONBF, 0);
	alarm(30); /* Same standalone watchdog; no product deadline is changed. */
	UT_PLAN(5);
	UT_RUN(compatible_16_control_releases_exact_owners);
	UT_RUN(compatible_17_owners_are_all_granted);
	UT_RUN(compatible_32_owners_are_all_granted);
	UT_RUN(compatible_256_owners_are_all_granted);
	UT_RUN(real_lmon_grants_the_17th_compatible_owner);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
