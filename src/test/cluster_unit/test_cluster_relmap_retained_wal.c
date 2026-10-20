/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_retained_wal.c
 *    Pending relmap against original ROOT/claim files and native XLogReader.
 *
 * Copyright (c) 2026, pgrac contributors
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Copyright (c) 1994, The Regents of the University of California
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_retained_wal.c
 *
 * NOTES
 *    WALR/CF transport and recovery-role providers remain fixture boundaries.
 *
 *-------------------------------------------------------------------------
 */
int control_fixture_main(int argc, char **argv);
#define main control_fixture_main
#include "test_cluster_control_root.c"
#undef main
#include "cluster/cluster_relmap_owner.h"
#include "catalog/pg_tablespace_d.h"
#include "utils/relmapper.h"

static ClusterWalSourceRef relmap_test_source[2];
static WalTestRecord relmap_test_record[2];
static unsigned relmap_test_fault;

static void
relmap_test_recheck_fault(void)
{
	test_stop_share_hook = NULL;
	if (relmap_test_fault == 1)
		v2_checkpoint_root_race();
	else if (relmap_test_fault == 2)
		inputs_pin_current = false;
	else
		test_throw_root_read = true;
}

static void
relmap_test_crc(RelMapFile *map)
{
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
}

/* Append actual short, single-page native records to the checkpoint segment. */
static WalTestRecord
relmap_test_append(const ClusterWalSourceRef *source, XLogRecPtr at,
	XLogRecPtr previous, RmgrId rmid, const void *payload, uint32 length)
{
	uint8 bytes[2048] = { 0 };
	XLogRecord record = { 0 };
	size_t framing = length <= UINT8_MAX ? 2 : 5;
	char path[MAXPGPATH];
	int fd;

	UT_ASSERT(length + SizeOfXLogRecord + framing < sizeof(bytes));
	record.xl_tot_len = SizeOfXLogRecord + framing + length;
	record.xl_prev = previous;
	record.xl_rmid = rmid;
	record.xl_info = rmid == RM_RELMAP_ID ? XLOG_RELMAP_UPDATE : XLOG_NOOP;
	record.xl_xid = 4195136;
	bytes[SizeOfXLogRecord] = length <= UINT8_MAX ? XLR_BLOCK_ID_DATA_SHORT : XLR_BLOCK_ID_DATA_LONG;
	if (length <= UINT8_MAX)
		bytes[SizeOfXLogRecord + 1] = length;
	else
		memcpy(bytes + SizeOfXLogRecord + 1, &length, sizeof(length));
	memcpy(bytes + SizeOfXLogRecord + framing, payload, length);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, framing + length);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	v2_checkpoint_wal_path(&source->claim.identity, at, source->timeline, path);
	UT_ASSERT(at % XLOG_BLCKSZ + record.xl_tot_len < XLOG_BLCKSZ);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, record.xl_tot_len, at % wal_segment_size), record.xl_tot_len);
	UT_ASSERT_EQ(close(fd), 0);
	return (WalTestRecord){ 1, MAXALIGN(at + record.xl_tot_len), at, record.xl_crc };
}

static ClusterWalInputsV1 *
relmap_test_fixture(ClusterRelmapV2Image *image, unsigned selected, bool shared,
	bool cold, bool open_source)
{
	uint8 bytes[66048], payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterWalInputsV1 *inputs = NULL;
	ControlRootImage root;
	xl_relmap_update update = { 0 };

	inputs_claim_observe = inputs_checkpoint_observe = false;
	inputs_claim_fault = 0;
	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	memset(image, 0, sizeof(*image));
	image->identity.system_identifier = TEST_SYSID;
	image->identity.database_incarnation = root.header.v2.database_incarnation;
	image->identity.shared = shared;
	image->identity.dbid = shared ? InvalidOid : 42;
	image->committed_generation = 7;
	image->pending_generation = 8;
	image->pending_owner.node = anchors[selected].identity.origin_node_id;
	image->pending_owner.admitted_incarnation = anchors[selected].identity.origin_owner_incarnation;
	image->pending_owner.top_full_xid = UINT64CONST(0x100000000) + 4195136;
	image->pending_owner.membership_epoch = 91;
	image->pending_owner.boot_incarnation = 202;
	image->committed.magic = RELMAPPER_FILEMAGIC;
	image->committed.num_mappings = 1;
	image->committed.mappings[0].mapoid = 1259;
	image->committed.mappings[0].mapfilenumber = 40000;
	relmap_test_crc(&image->committed);
	image->pending = image->committed;
	image->pending.mappings[0].mapfilenumber++;
	relmap_test_crc(&image->pending);
	update.dbid = image->identity.dbid;
	update.tsid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	update.nbytes = sizeof(RelMapFile);
	memcpy(payload, &update, MinSizeOfRelmapUpdate);
	memcpy(payload + MinSizeOfRelmapUpdate, &image->pending, sizeof(RelMapFile));
	for (unsigned i = 0; i < 2; i++) {
		ClusterControlRootSnapshot *snapshot = &root.records[i == 0 ? 0 : 127];
		ClusterWalSourceRef *source = &relmap_test_source[i];
		WalTestRecord tail;

		memset(source, 0, sizeof(*source));
		source->claim.identity = snapshot->identity;
		source->claim.database_incarnation = root.header.v2.database_incarnation;
		source->claim.max_config_generation = root.header.v2.config_generation;
		memcpy(source->claim.claim_sha256, root.refs[i == 0 ? 0 : 127].claim_sha256, 32);
		source->timeline = snapshot->tail_tli;
		tail = relmap_test_append(source, snapshot->validated_tail_lsn_exclusive,
			snapshot->tail_last_record_lsn, RM_RELMAP_ID, payload, sizeof(payload));
		relmap_test_record[i] = tail;
		if (i == selected)
			image->pending_owner.wal_end = tail.exclusive_end;
		/* A valid suffix after the match is still mandatory input. */
		tail = relmap_test_append(source, tail.exclusive_end, tail.record_start, RM_XLOG_ID, "abc", 3);
		snapshot->tail_last_record_lsn = tail.record_start;
		snapshot->tail_last_record_crc32c = tail.record_crc;
		snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	}
	if (open_source)
		root.records[127].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	v2_write_roots(root.bytes);
	test_inputs_startup_cf = cold;
	UT_ASSERT_EQ(inputs_begin_for_role(cold, &inputs), 0);
	return inputs;
}

UT_TEST(test_retained_relmap_matches_each_selected_namespace)
{
	for (unsigned i = 0; i < 4; i++) {
		ClusterRelmapV2Image image, before;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, i % 2, i < 2, true, false);
		before = image;
		UT_ASSERT(inputs != NULL);
		if (inputs == NULL) return;
		for (unsigned pass = 0; pass < 2; pass++) {
			UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
			UT_ASSERT_EQ(memcmp(&image, &before, sizeof(image)), 0);
		}
		UT_ASSERT_EQ(inputs_peer_begins + inputs_native_begin_calls + inputs_native_flush_calls, 0);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_requires_exact_pending)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		if (fault == 0) image.pending_owner.wal_end++;
		if (fault == 1) image.pending_owner.top_full_xid++;
		if (fault == 2) image.pending_owner.admitted_incarnation++;
		if (fault == 3) image.identity.database_incarnation++;
		if (fault == 4) { image.pending.mappings[0].mapfilenumber++; relmap_test_crc(&image.pending); }
		if (fault == 5) image.identity.dbid++;
		if (fault == 6) { image.identity.shared = true; image.identity.dbid = InvalidOid; }
		if (fault == 7) image.pending_owner.node = 64;
		UT_ASSERT_NE(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_scans_suffix_and_other_source)
{
	for (unsigned selected = 0; selected < 2; selected++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
		char path[MAXPGPATH];
		uint8 bad = 0x5a;
		int fd;
		v2_checkpoint_wal_path(&relmap_test_source[selected].claim.identity,
			relmap_test_record[selected].exclusive_end, relmap_test_source[selected].timeline, path);
		fd = open(path, O_WRONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pwrite(fd, &bad, 1, relmap_test_record[selected].exclusive_end % wal_segment_size + SizeOfXLogRecord + 2), 1);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_NE(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_waits_for_open_and_rejects_online)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, mode == 0, mode == 0);
		inputs_claim_calls = 0;
		inputs_claim_observe = true;
		UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), mode == 0 ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT : CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		inputs_claim_observe = false;
		UT_ASSERT_EQ(inputs_claim_calls, 0);
		UT_ASSERT(inputs_pin_held);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_original_recheck_and_error_keep_pin)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
		volatile bool threw = false;

		/* Original cuts check, two pre/post visits, then whole-set recheck.
		 * Inject after the native records in both sources have been read. */
		relmap_test_fault = fault;
		test_stop_share_call = test_cf_lock_calls + 6;
		test_stop_share_hook = relmap_test_recheck_fault;
		PG_TRY();
		{
			UT_ASSERT_NE(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		UT_ASSERT_EQ(threw, fault == 3);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		UT_ASSERT_NE(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_retained_relmap_invalid_input_is_read_only)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, true, false);
	inputs_claim_calls = 0;
	inputs_claim_observe = true;
	UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(NULL, inputs), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	image.pending.magic++;
	UT_ASSERT_EQ(cluster_relmap_owner_retained_wal_v2(&image, inputs), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	inputs_claim_observe = false;
	UT_ASSERT_EQ(inputs_claim_calls, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cold_census_done(&inputs);
}

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	UT_PLAN(6);
	UT_RUN(test_retained_relmap_matches_each_selected_namespace);
	UT_RUN(test_retained_relmap_requires_exact_pending);
	UT_RUN(test_retained_relmap_scans_suffix_and_other_source);
	UT_RUN(test_retained_relmap_waits_for_open_and_rejects_online);
	UT_RUN(test_retained_relmap_original_recheck_and_error_keep_pin);
	UT_RUN(test_retained_relmap_invalid_input_is_read_only);
	UT_DONE();
	return ut_failed_count != 0;
}
