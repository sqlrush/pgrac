/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_native_census.inc
 *     Actual ROOT, native checkpoint and pair input under the original scope.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_native_census.inc
 *
 * NOTES
 *     Native files and all input/census producers execute. Startup, GES and
 *     the unused legacy cold-owner authority remain controlled lower leaves.
 *     Successful observations supply no native lifetime or COMPLETE grant.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_undo_recovery.h"
extern bool cluster_relmap_retention_census_execute_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, size_t root_budget,
	size_t memory_budget, size_t payload_budget, ClusterRelmapRetentionCensus **out);
extern void scope_native_census_destroy(ClusterRelmapRetentionCensus *census);
extern void cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census);

bool cluster_shared_catalog = true;
static int scope_native_fd = -1;
static unsigned scope_native_destroyed, scope_native_legacy_calls;

bool cluster_recovery_merge_claim_is_held(void);
bool cluster_relmap_authority_x_held(void);

bool
cluster_recovery_merge_claim_is_held(void)
{
	scope_native_legacy_calls++;
	return false;
}

bool
cluster_relmap_authority_x_held(void)
{
	scope_native_legacy_calls++;
	return false;
}

bool
cluster_undo_recovery_cold_single_source_v1(const ClusterWalSourceRef *source,
	const ClusterControlRootSnapshot *checkpoint, XLogRecPtr native_redo, uint64 scope_serial)
{
	(void)source;
	(void)checkpoint;
	(void)native_redo;
	(void)scope_serial;
	scope_native_legacy_calls++;
	return false;
}

void
cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census)
{
	if (census != NULL) {
		UT_ASSERT_EQ(ctrc_set_held, 2);
		UT_ASSERT(inputs_pin_held);
		UT_ASSERT(fcntl(scope_native_fd, F_GETFD) >= 0);
		scope_native_destroyed++;
	}
	scope_native_census_destroy(census);
}

static void
scope_native_write_pair(Oid dbid, bool pending, bool corrupt)
{
	ClusterRelmapV2Image image = {0};
	uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
	char path[MAXPGPATH];
	RelMapFile *map = &image.committed;

	image.identity = (ClusterRelmapV2Namespace){ TEST_SYSID,
		tx_inputs_typed.sources[0].claim.database_incarnation, dbid, dbid == InvalidOid };
	image.committed_generation = 1;
	map->magic = RELMAPPER_FILEMAGIC;
	map->num_mappings = 1;
	map->mappings[0].mapoid = 1259;
	map->mappings[0].mapfilenumber = 40000;
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
	for (unsigned copy = 0; copy < 2; copy++) {
		int fd;

		if (copy == 1 && pending) {
			image.pending_generation = 2;
			image.pending_owner = (ClusterRelmapV2Owner){1, 50, 0, 17, 909, 31};
			image.pending = image.committed;
		}
		UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
		if (copy == 1 && corrupt) bytes[sizeof(bytes) - 1] ^= 1;
		if (dbid == InvalidOid)
			snprintf(path, sizeof(path), "global/pgrac_relmap_authority%s", copy ? ".bak" : "");
		else
			snprintf(path, sizeof(path), "base/%u/pgrac_relmap_authority%s", dbid, copy ? ".bak" : "");
		fd = openat(scope_native_fd, path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
		UT_ASSERT(fd >= 0);
		if (fd >= 0) {
			UT_ASSERT_EQ(write(fd, bytes, sizeof(bytes)), sizeof(bytes));
			UT_ASSERT_EQ(close(fd), 0);
		}
	}
}

static ClusterRecoverySerialAcquireResult
scope_native_acquire(const ClusterRecoverySerialRequest *request, ClusterRecoverySerialGuard *guard)
{
	ClusterRecoverySerialAcquireResult result = ctrc_set_native_acquire(request, guard);
	if (request->duty.origin_thread_id == 2 && result == CLUSTER_RECOVERY_SERIAL_GRANTED)
		relmap_sibling = guard;
	return result;
}

static void
scope_native_second_origin(uint8 bytes[66048], ClusterRecoveryAnchorV2 *anchor,
	ControlRootImage *root)
{
	ControlFileData native = {0};
	char path[MAXPGPATH];

	/* Original broad ROOT fixtures use origin127 deliberately. The native
	 * window has a different original64-origin domain: create a real second
	 * claim/anchor/WAL at origin1, without relaxing either product boundary. */
	root->records[1] = root->records[127];
	root->records[1].identity.origin_node_id = 1;
	root->records[1].identity.origin_thread_id = 2;
	root->refs[1] = root->refs[127];
	root->startup[1] = root->startup[127];
	root->publisher_incarnation[1] = root->publisher_incarnation[127];
	root->publisher_node[1] = root->publisher_node[127];
	root->present[1] = true;
	root->present[127] = false;
	root->publisher_incarnation[127] = root->publisher_node[127] = 0;
	memset(&root->refs[127], 0, sizeof(root->refs[127]));
	memset(&root->records[127], 0, sizeof(root->records[127]));
	memset(&root->startup[127], 0, sizeof(root->startup[127]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(root), 0);
	memcpy(bytes, root->bytes, 66048);
	v2_claim_object(bytes, 1, root);
	anchor->identity = root->records[1].identity;
	memcpy(anchor->claim_sha256, root->refs[1].claim_sha256, 32);
	native.state = anchor->state;
	native.checkPoint = anchor->checkpoint;
	native.checkPointCopy = anchor->checkpoint_copy;
	snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path),
		"%s/thread_2/generation_" UINT64_FORMAT "/durable_prefix/current",
		cluster_wal_threads_dir, anchor->identity.origin_owner_incarnation);
	v2_checkpoint_wal_record(&anchor->identity, &native, 8);
	v2_anchor_object(bytes, anchor, &anchor->identity, path);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, 66048, v2_storage, TEST_SYSID, root), 0);
	root->records[1].checkpoint_record_crc32c = root->records[1].tail_last_record_crc32c
		= test_checkpoint_crc;
	root->records[1].validated_tail_lsn_exclusive = test_checkpoint_end;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(root), 0);
	memcpy(bytes, root->bytes, 66048);
}

static void
scope_native_setup(ClusterWalInputsV1 **inputs)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ClusterWalInputsV1 *original = NULL;
	char path[MAXPGPATH];

	setup_fixture();
	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	scope_native_second_origin(bytes, &anchors[1], &root);
	for (unsigned i = 0; i < 2; i++) {
		unsigned node = i;
		ClusterRelmapCheckpointWindow window = {0};

		window.system_identifier = TEST_SYSID;
		window.database_incarnation = anchors[i].database_incarnation;
		window.origin = node;
		window.epoch = 17;
		window.admitted_incarnation = anchors[i].identity.origin_owner_incarnation;
		window.boot_incarnation = 31;
		window.native_floor = 3;
		window.last_issued = 90;
		window.allocation_next = 100;
		UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(&window, &anchors[i].checkpoint_copy,
			anchors[i].checkpoint, root.records[node].validated_tail_lsn_exclusive,
			95, anchors[i].native_window));
		v2_anchor_object(bytes, &anchors[i], &anchors[i].identity, path);
	}
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	tx_ir_complete(&root.records[0]);
	tx_ir_complete(&root.records[1]);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &original), 0);
	tx_inputs_capture(original, NULL);
	cluster_wal_inputs_release_v1(&original);
	tx_inputs_activate();
	ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs;
	ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0;
	ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = scope_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_sibling = NULL;
	relmap_declared = true;
	relmap_failed = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(*inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_namespace_v1(*inputs, &scope_native_fd), 0);
	UT_ASSERT_EQ(mkdirat(scope_native_fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(scope_native_fd, "base/5", 0700), 0);
	scope_native_write_pair(InvalidOid, false, false);
	scope_native_write_pair(5, false, false);
	scope_native_destroyed = scope_native_legacy_calls = 0;
}

static bool
scope_native_prepare(ClusterWalInputsV1 *inputs, const ClusterRelmapRetentionCensus **out)
{
	return cluster_wal_inputs_relmap_census_prepare_v1(inputs,
		16 * 1024 * 1024, 1024 * 1024, 128 * 1024, out);
}

static void
scope_native_finish(ClusterWalInputsV1 **inputs)
{
	ctrc_set_finish(inputs);
	UT_ASSERT_EQ(fcntl(scope_native_fd, F_GETFD), -1);
	relmap_declared = false;
}

UT_TEST(test_scope_native_census_uses_original_episode_and_retires_before_holders)
{
	ClusterWalInputsV1 *inputs = NULL;
	const ClusterRelmapRetentionCensus *borrowed = NULL, *again = NULL;

	scope_native_setup(&inputs);
	UT_ASSERT(!RecoveryInProgress());
	UT_ASSERT(scope_native_prepare(inputs, &borrowed));
	UT_ASSERT(borrowed != NULL);
	if (borrowed != NULL)
		UT_ASSERT_EQ(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)borrowed),
			CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT(!scope_native_prepare(inputs, &again));
	UT_ASSERT(again == NULL);
	UT_ASSERT_EQ(scope_native_legacy_calls, 0);
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(ctrc_set_held, 2);
	UT_ASSERT(inputs_pin_held);
	UT_ASSERT_EQ(scope_native_destroyed, 0);
	scope_native_finish(&inputs);
	UT_ASSERT_EQ(scope_native_destroyed, 1);
}

UT_TEST(test_scope_native_census_missing_corrupt_pending_pairs_keep_original_debt)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		const ClusterRelmapRetentionCensus *borrowed = NULL;

		scope_native_setup(&inputs);
		if (fault == 0)
			UT_ASSERT_EQ(unlinkat(scope_native_fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
		if (fault == 1) scope_native_write_pair(5, false, true);
		if (fault == 2) scope_native_write_pair(5, true, false);
		UT_ASSERT(!scope_native_prepare(inputs, &borrowed));
		UT_ASSERT(borrowed == NULL);
		UT_ASSERT(relmap_failed > 0);
		UT_ASSERT(ctrc_set_held == 2 && inputs_pin_held);
		UT_ASSERT(fcntl(scope_native_fd, F_GETFD) >= 0);
		UT_ASSERT(!scope_native_prepare(inputs, &borrowed));
		scope_native_finish(&inputs);
	}
}

UT_TEST(test_scope_native_census_late_source_loss_and_error_do_not_replace_custody)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs = NULL;
		const ClusterRelmapRetentionCensus *borrowed = NULL;
		volatile bool caught = false;

		scope_native_setup(&inputs);
		UT_ASSERT(scope_native_prepare(inputs, &borrowed));
		if (borrowed != NULL) {
			test_capture_error_level = true;
			if (fault == 0) relmap_sibling->release_uncertain = true;
			else ctrc_replay_current_override = relmap_episode_terminal_error_current;
			PG_TRY();
			{
				UT_ASSERT(cluster_relmap_retention_census_next((ClusterRelmapRetentionCensus *)borrowed)
					> CLUSTER_RELMAP_CENSUS_DRAINED);
			}
			PG_CATCH(); { caught = true; } PG_END_TRY();
			test_capture_error_level = false;
			ctrc_replay_current_override = ctrc_set_original_current;
			if (fault == 0) relmap_sibling->release_uncertain = false;
			UT_ASSERT_EQ(caught, fault == 1);
			UT_ASSERT(relmap_failed > 0);
			UT_ASSERT_EQ(scope_native_destroyed, 0);
			UT_ASSERT(ctrc_set_held == 2 && inputs_pin_held);
		}
		scope_native_finish(&inputs);
		UT_ASSERT_EQ(scope_native_destroyed, 1);
	}
}

UT_TEST(test_scope_native_census_entry_roles_do_not_borrow_legacy_cold_authority)
{
	ClusterWalInputsV1 *inputs = NULL;
	const ClusterRelmapRetentionCensus *borrowed = NULL;

	scope_native_setup(&inputs);
	CritSectionCount = 1;
	UT_ASSERT(!scope_native_prepare(inputs, &borrowed));
	CritSectionCount = 0;
	MyBackendType = B_BG_WORKER;
	UT_ASSERT(!scope_native_prepare(inputs, &borrowed));
	MyBackendType = B_STARTUP;
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT(scope_native_prepare(inputs, &borrowed));
	UT_ASSERT(borrowed != NULL);
	UT_ASSERT_EQ(scope_native_legacy_calls, 0);
	scope_native_finish(&inputs);
}
