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
#include "cluster/cluster_relmap_retention_census.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "utils/relmapper.h"

static ClusterWalSourceRef relmap_test_source[2];
static WalTestRecord relmap_test_record[2];
static unsigned relmap_test_fault;
static bool relmap_test_side_error;
static unsigned relmap_test_side_errors;

ClusterRelmapPendingResult test_relmap_pending_collect_side(
	const ClusterRelmapV2Namespace *expected, const uint8 storage_uuid[16],
	const ClusterRelmapPendingSideSource *sources, size_t source_count,
	ClusterRelmapPendingDescriptor *out, size_t capacity, size_t *count_out);

/* The real collector can ERROR through the ABORT accessor's production
 * palloc0. Inject that boundary; all nonfault calls use the original collector. */
ClusterRelmapPendingResult
test_relmap_pending_collect_side(const ClusterRelmapV2Namespace *expected,
	const uint8 storage_uuid[16], const ClusterRelmapPendingSideSource *sources,
	size_t source_count, ClusterRelmapPendingDescriptor *out, size_t capacity,
	size_t *count_out)
{
	if (relmap_test_side_error) {
		relmap_test_side_error = false;
		relmap_test_side_errors++;
		ereport(ERROR, (errmsg("injected SIDE collector ERROR")));
	}
	return cluster_relmap_pending_collect_side(expected, storage_uuid, sources,
		source_count, out, capacity, count_out);
}

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

typedef struct RelmapCensusWalFixture
{
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryRootSet *roots;
	ClusterRelmapRetentionCensus *census;
	ClusterRelmapRetentionDirectory origin[2];
	ClusterRelmapRetentionDirectory *directories[2];
} RelmapCensusWalFixture;

static void
relmap_census_file(int fd, const char *path, const ClusterRelmapV2Image *image)
{
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	int file;

	UT_ASSERT_EQ(cluster_relmap_v2_encode(image, wire, sizeof(wire)), CLUSTER_RELMAP_V2_VALID);
	file = openat(fd, path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	UT_ASSERT(file >= 0);
	UT_ASSERT_EQ(write(file, wire, sizeof(wire)), sizeof(wire));
	UT_ASSERT_EQ(close(file), 0);
}

/* Actual physical namespace and authority bytes, distinct from WAL ROOT files.
 * Caller qualification of floor/issued and isolation remains a fixture input. */
static void
relmap_census_begin(RelmapCensusWalFixture *f, const ClusterRelmapV2Image *pending,
	bool published)
{
	ClusterRelmapV2Image clean = *pending, primary = *pending;
	ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;

	memset(f, 0, sizeof(*f));
	strlcpy(f->path, "/tmp/pgrac-relmap-census-wal-XXXXXX", sizeof(f->path));
	UT_ASSERT(mkdtemp(f->path) != NULL);
	f->fd = open(f->path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(f->fd >= 0);
	UT_ASSERT_EQ(mkdirat(f->fd, "global", 0700), 0);
	UT_ASSERT_EQ(mkdirat(f->fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(f->fd, "base/42", 0700), 0);
	clean.identity.dbid = pending->identity.shared ? 42 : InvalidOid;
	clean.identity.shared = !pending->identity.shared;
	clean.pending_generation = 0;
	memset(&clean.pending_owner, 0, sizeof(clean.pending_owner));
	memset(&clean.pending, 0, sizeof(clean.pending));
	if (published) {
		primary.committed_generation = pending->pending_generation;
		primary.committed = pending->pending;
		primary.pending_generation = 0;
		memset(&primary.pending_owner, 0, sizeof(primary.pending_owner));
		memset(&primary.pending, 0, sizeof(primary.pending));
	}
	relmap_census_file(f->fd, "global/pgrac_relmap_authority",
		pending->identity.shared ? &primary : &clean);
	relmap_census_file(f->fd, "global/pgrac_relmap_authority.bak",
		pending->identity.shared ? pending : &clean);
	relmap_census_file(f->fd, "base/42/pgrac_relmap_authority",
		pending->identity.shared ? &clean : &primary);
	relmap_census_file(f->fd, "base/42/pgrac_relmap_authority.bak",
		pending->identity.shared ? &clean : pending);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_relmap_retention_build_start(&f->origin[i], TEST_SYSID,
			pending->identity.database_incarnation, i == 0 ? 0 : 127, 1,
			UINT64CONST(0x100000003), UINT64CONST(0x100800000)), CLUSTER_RELMAP_RETENTION_OK);
		f->directories[1 - i] = &f->origin[i];
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(f->fd, TEST_SYSID,
		pending->identity.database_incarnation, v2_storage, 4 * 1024 * 1024, &f->roots), result);
	for (unsigned i = 0; i < 20000 && (result == CLUSTER_RECOVERY_ROOTS_MORE
		|| result == CLUSTER_RECOVERY_ROOTS_ENTRY || result == CLUSTER_RECOVERY_ROOTS_BYTES); i++) {
		ClusterRecoveryRootsEvent event;
		result = cluster_recovery_root_set_step(f->roots, &event);
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_relmap_retention_census_begin(f->roots, TEST_SYSID,
		pending->identity.database_incarnation, v2_storage, f->directories, 2,
		128 * 1024, &f->census), CLUSTER_RELMAP_CENSUS_MORE);
}

static ClusterRelmapCensusCommitted
relmap_census_emission(RelmapCensusWalFixture *f, Oid dbid)
{
	ClusterRelmapCensusCommitted out = { 0 };
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++) {
		result = cluster_relmap_retention_census_next_committed(f->census, &out);
		if (out.present && out.identity.dbid == dbid)
			return out;
	}
	UT_ASSERT(false);
	return out;
}

static ClusterRelmapCensusResult
relmap_census_drain(RelmapCensusWalFixture *f)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++)
		result = cluster_relmap_retention_census_next(f->census);
	UT_ASSERT_NE(result, CLUSTER_RELMAP_CENSUS_MORE);
	return result;
}

static ClusterRelmapPendingDescriptor
relmap_census_descriptor(const ClusterRelmapV2Image *image)
{
	ClusterRelmapPendingDescriptor descriptor = { 0 };

	descriptor.kind = CLUSTER_RELMAP_PENDING_DROP;
	descriptor.identity.system_identifier = TEST_SYSID;
	descriptor.identity.database_incarnation = image->identity.database_incarnation;
	memcpy(descriptor.identity.storage_uuid, v2_storage, 16);
	descriptor.identity.locator = (RelFileLocator){ image->identity.shared
		? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID, image->identity.dbid, 70000 };
	return descriptor;
}

static void
relmap_census_pin_retained(RelmapCensusWalFixture *f, const ClusterRelmapV2Image *image,
	bool refused)
{
	unsigned pins = 0;
	uint8 digest[32];

	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(f->origin[i].state, refused ? CLUSTER_RELMAP_RETENTION_UNKNOWN
			: CLUSTER_RELMAP_RETENTION_BUILDING);
		for (unsigned p = 0; p < lengthof(f->origin[i].pins); p++) {
			const ClusterRelmapRetentionPin *pin = &f->origin[i].pins[p];
			if (!pin->occupied)
				continue;
			pins++;
			UT_ASSERT_EQ(pin->key.origin, image->pending_owner.node);
			UT_ASSERT_EQ(pin->key.top_full_xid, image->pending_owner.top_full_xid);
			UT_ASSERT_EQ(pin->wal_end, image->pending_owner.wal_end);
		}
	}
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(cluster_recovery_root_set_digest(f->roots, digest), !refused);
}

static void
relmap_census_end(RelmapCensusWalFixture *f)
{
	cluster_relmap_retention_census_destroy(f->census);
	cluster_recovery_root_set_destroy(f->roots);
	UT_ASSERT_EQ(unlinkat(f->fd, "global/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "global/pgrac_relmap_authority.bak", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base/42/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base/42/pgrac_relmap_authority.bak", 0), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base/42", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "base", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(f->fd, "global", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(f->fd), 0);
	UT_ASSERT_EQ(rmdir(f->path), 0);
}

static ClusterRelmapRetentionDirectory *relmap_census_drift_directory;

static void
relmap_census_borrowed_drift(void)
{
	test_stop_share_hook = NULL;
	relmap_census_drift_directory->generation++;
}

UT_TEST(test_census_wal_rebuild_stage_and_publish_cuts)
{
	for (unsigned cut = 0; cut < 8; cut++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, cut % 2, cut % 4 < 2, true, false);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending;
		ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);
		ClusterRelmapCensusCommitted out;

		relmap_census_begin(&f, &image, cut >= 4);
		out = relmap_census_emission(&f, image.identity.dbid);
		UT_ASSERT_EQ(out.generation, cut >= 4 ? image.pending_generation : image.committed_generation);
		UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, cut >= 4 ? 40001 : 40000);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptor.kind, &descriptor.identity));
		UT_ASSERT(pending.entries[0].main_placeholder && !pending.entries[0].copies_cleared_readback
			&& !pending.entries[0].native_terminal && !pending.entries[0].tt_complete
			&& !pending.entries[0].space_complete);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		relmap_census_pin_retained(&f, &image, false);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_mismatch_is_atomic_and_sticky)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 1, false, true, false);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;
		ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);

		if (fault == 0) image.pending_owner.wal_end++;
		if (fault == 1) image.pending_owner.top_full_xid++;
		if (fault == 2) image.pending_owner.admitted_incarnation++;
		if (fault == 3) { image.pending.mappings[0].mapfilenumber++; relmap_test_crc(&image.pending); }
		relmap_census_begin(&f, &image, true);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_wait_online_and_missing_scope_refuse)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, true, fault != 1, fault == 0);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;

		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
			&pending, fault == 2 ? NULL : inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_original_recheck_error_retains_both_pins)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapCensusWalFixture f;
		ClusterRelmapPendingSet pending, before;
		volatile bool threw = false;

		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		relmap_test_fault = fault;
		test_stop_share_call = test_cf_lock_calls + 6;
		test_stop_share_hook = relmap_test_recheck_fault;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
				&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
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
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_wal_structural_conflict_leaves_destination)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending, before;
	ClusterRelmapPendingDescriptor descriptors[2];

	descriptors[0] = relmap_census_descriptor(&image);
	descriptors[1] = descriptors[0];
	relmap_census_begin(&f, &image, true);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	before = pending;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, descriptors, 2), CLUSTER_RELMAP_PENDING_CONFLICT);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT(inputs_pin_held);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_wal_rechecks_borrowed_directory_after_scan)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending, before;
	ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);

	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	before = pending;
	relmap_census_drift_directory = &f.origin[1];
	test_stop_share_call = test_cf_lock_calls + 6;
	test_stop_share_hook = relmap_census_borrowed_drift;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT(test_stop_share_hook == NULL);
	test_stop_share_hook = NULL;
	relmap_census_drift_directory = NULL;
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_CHANGED);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(inputs_pin_held);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_wal_physical_root_recheck_still_required)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending;
	ClusterRelmapPendingDescriptor descriptor = relmap_census_descriptor(&image);

	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptor.kind, &descriptor.identity));
	image.pending_owner.boot_incarnation++;
	relmap_census_file(f.fd, "base/42/pgrac_relmap_authority.bak", &image);
	UT_ASSERT_NE(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	relmap_census_pin_retained(&f, &image, true);
	/* The caller must discard the provisional set after that refusal. */
	UT_ASSERT(!pending.entries[0].copies_cleared_readback && !pending.entries[0].native_terminal);
	UT_ASSERT(inputs_pin_held);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_wal_clean_pair_and_missing_window)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	ClusterRelmapPendingSet pending, before;
	ClusterRelmapPendingDescriptor descriptor;

	relmap_census_begin(&f, &image, false);
	cluster_relmap_pending_init(&pending);
	before = pending;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	/* The other namespace is clean, but structural descriptors remain independent. */
	(void)relmap_census_emission(&f, InvalidOid);
	image.identity.dbid = InvalidOid;
	image.identity.shared = true;
	descriptor = relmap_census_descriptor(&image);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_wal(f.census,
		&pending, inputs, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptor.kind, &descriptor.identity));
	UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	UT_ASSERT(inputs_pin_held && inputs_pin_current);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

typedef struct RelmapCensusSideFixture
{
	ClusterRelmapPendingSideSource source;
	RfSideOnlinePlanV1 *plan;
	ClusterSpaceIdentityKey key;
} RelmapCensusSideFixture;

/* Controlled decoded SIDE record, owned and sealed by the original plan.
 * Source selection/cut correspondence to the held WAL scope is still the
 * original caller's qualification, not a claim made by this fixture. */
static void
relmap_census_side(RelmapCensusSideFixture *f, const ClusterRelmapV2Image *image,
	unsigned selected, bool seal)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterSpaceReservation before = { 0 };
	ClusterSpaceStructureChange change = { 0 };
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	XLogReaderState reader = { 0 };
	DecodedXLogRecord decoded = { 0 };
	RfPageOnlineRecordIdentityV1 identity = { 0 };
	RfDetachedRecordPlanV1 record = { 0 };
	XLogRecPtr redo = 200;

	memset(f, 0, sizeof(*f));
	f->source.source = relmap_test_source[selected];
	f->source.cut.failed_thread = f->source.source.claim.identity.origin_thread_id;
	f->source.cut.timeline_id = f->source.source.timeline;
	f->source.cut.origin_owner_incarnation =
		f->source.source.claim.identity.origin_owner_incarnation;
	f->source.cut.scan_begin_inclusive = 100;
	f->source.cut.scan_end_exclusive = 200;
	f->source.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	request.system_identifier = image->identity.system_identifier;
	memcpy(request.storage_uuid, v2_storage, 16);
	request.physical_cuts = &f->source.cut;
	request.participant_count = 1;
	request.purpose = RF_SIDE_PLAN_RECOVERY;
	request.redo_starts = &redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->plan, &f->source.source, 1));
	f->source.plan = f->plan;
	f->key = relmap_census_descriptor(image).identity;
	before.identity.key = f->key;
	memset(before.identity.incarnation, 0x17, 16);
	before.identity.sequence = before.identity.operation = 1;
	before.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	before.next_block = 7;
	change.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
	change.identity.nblocks = 2;
	change.identity.expected = change.identity.result = before.identity;
	memset(change.identity.result.incarnation, 0x29, 16);
	change.identity.result.sequence++;
	change.identity.result.operation++;
	change.identity.before_token = 123;
	change.identity.result_token = 70;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
	change.reservation.before = change.reservation.result = before;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result.next_block = change.reservation.first_block = 2;
	change.reservation.before_token = 80;
	change.reservation.result_token = 70;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	decoded.header.xl_rmid = RM_SMGR_ID;
	decoded.header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded.header.xl_crc = 0xabc123;
	decoded.header.xl_xid = 4195136;
	decoded.main_data = (char *)wal;
	decoded.main_data_len = sizeof(wal);
	decoded.max_block_id = -1;
	decoded.lsn = reader.ReadRecPtr = 100;
	decoded.next_lsn = reader.EndRecPtr = 200;
	reader.system_identifier = image->identity.system_identifier;
	reader.record = &decoded;
	identity.record.system_identifier = reader.system_identifier;
	memcpy(identity.record.storage_uuid, v2_storage, 16);
	identity.record.origin_thread = f->source.cut.failed_thread;
	identity.record.timeline_id = f->source.cut.timeline_id;
	identity.record.read_rec_ptr = 100;
	identity.record.end_rec_ptr = 200;
	identity.record.record_crc = decoded.header.xl_crc;
	identity.record.rmid = decoded.header.xl_rmid;
	identity.record.info = decoded.header.xl_info;
	record.source_record = &reader;
	record.route.rmid = RM_SMGR_ID;
	record.route.normalized_info = XLOG_SMGR_SPACE_IDENTITY;
	record.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record.preflight_complete = true;
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(f->plan, &record, &identity),
		RF_PAGE_PROOF_DETAIL_OK);
	memset(wal, 0xee, sizeof(wal));
	if (seal) {
		UT_ASSERT(rf_side_online_plan_bind_database_v1(f->plan, image->identity.database_incarnation));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
		/* This operation is retained history, not a redo execution target. */
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(f->plan), 0);
	}
}

UT_TEST(test_census_side_wal_same_window_stage_publish_and_history)
{
	for (unsigned cut = 0; cut < 8; cut++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, cut % 2, cut % 4 < 2, true, false);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, installed;

		relmap_census_side(&side, &image, cut % 2, true);
		relmap_census_begin(&f, &image, cut >= 4);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, &side.source, 1), CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
		UT_ASSERT(pending.entries[0].main_placeholder && !pending.entries[0].copies_cleared_readback
			&& !pending.entries[0].native_terminal && !pending.entries[0].tt_complete
			&& !pending.entries[0].space_complete);
		UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
		installed = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, &side.source, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &installed, sizeof(pending)), 0);
		UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		relmap_census_pin_retained(&f, &image, false);
		UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
		rf_side_online_plan_destroy_v1(&side.plan);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_side_refusal_precedes_scan)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_side(&side, &image, 0, fault != 1);
		if (fault == 2) side.source.source.claim.claim_sha256[0]++;
		if (fault == 3) side.source.cut.scan_end_exclusive++;
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
			&side.key, true), CLUSTER_RELMAP_PENDING_OK);
		before = pending;
		inputs_claim_calls = 0;
		inputs_claim_observe = true;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, fault == 0 ? NULL : &side.source, fault == 0 ? 0 : 1),
			CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(inputs_claim_calls, 0);
		inputs_claim_observe = false;
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		rf_side_online_plan_destroy_v1(&side.plan);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_wal_refusal_never_installs_side)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 1, true, fault != 3, fault == 2);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		if (fault == 0) image.pending_owner.wal_end++;
		relmap_census_side(&side, &image, 1, true);
		relmap_census_begin(&f, &image, true);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, fault == 1 ? NULL : inputs, &side.source, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		rf_side_online_plan_destroy_v1(&side.plan);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_recheck_and_error_keep_window_invalid)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;
		volatile bool threw = false;

		relmap_census_side(&side, &image, 0, true);
		relmap_census_begin(&f, &image, false);
		(void)relmap_census_emission(&f, image.identity.dbid);
		cluster_relmap_pending_init(&pending);
		before = pending;
		relmap_census_drift_directory = &f.origin[1];
		relmap_test_fault = fault == 1 ? 2 : 3;
		test_stop_share_call = test_cf_lock_calls + 6;
		test_stop_share_hook = fault == 0 ? relmap_census_borrowed_drift : relmap_test_recheck_fault;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
				&pending, inputs, &side.source, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_throw_root_read = false;
		UT_ASSERT(test_stop_share_hook == NULL);
		test_stop_share_hook = NULL;
		relmap_census_drift_directory = NULL;
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		relmap_census_pin_retained(&f, &image, true);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT(inputs_pin_held && test_actual_cf == NoLock);
		rf_side_online_plan_destroy_v1(&side.plan);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

UT_TEST(test_census_side_wal_late_physical_root_rejects_provisional)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	RelmapCensusSideFixture side;
	ClusterRelmapPendingSet pending;

	relmap_census_side(&side, &image, 0, true);
	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
		&pending, inputs, &side.source, 1), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
	image.pending_owner.boot_incarnation++;
	relmap_census_file(f.fd, "base/42/pgrac_relmap_authority.bak", &image);
	UT_ASSERT_NE(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
	UT_ASSERT(inputs_pin_held);
	rf_side_online_plan_destroy_v1(&side.plan);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_side_wal_collector_error_invalidates_before_wal)
{
	ClusterRelmapV2Image image;
	ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
	RelmapCensusWalFixture f;
	RelmapCensusSideFixture side;
	ClusterRelmapPendingSet pending, before;
	volatile bool threw = false;

	relmap_census_side(&side, &image, 0, true);
	relmap_census_begin(&f, &image, false);
	(void)relmap_census_emission(&f, image.identity.dbid);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
		&side.key, true), CLUSTER_RELMAP_PENDING_OK);
	before = pending;
	inputs_claim_calls = relmap_test_side_errors = 0;
	inputs_claim_observe = relmap_test_side_error = true;
	test_last_error_level = 0;
	test_capture_error_level = true;
	PG_TRY();
	{
		(void)cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, &side.source, 1);
	}
	PG_CATCH();
	{
		threw = true;
	}
	PG_END_TRY();
	test_capture_error_level = false;
	UT_ASSERT(threw && !relmap_test_side_error);
	UT_ASSERT_EQ(test_last_error_level, ERROR);
	UT_ASSERT_EQ(relmap_test_side_errors, 1);
	UT_ASSERT_EQ(inputs_claim_calls, 0);
	inputs_claim_observe = relmap_test_side_error = false;
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	relmap_census_pin_retained(&f, &image, true);
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
		&pending, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_revalidate_v1(inputs), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(inputs_pin_held && inputs_pin_current && test_actual_cf == NoLock);
	rf_side_online_plan_destroy_v1(&side.plan);
	relmap_census_end(&f);
	cold_census_done(&inputs);
}

UT_TEST(test_census_side_wal_window_clean_pair_and_destination)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapV2Image image;
		ClusterWalInputsV1 *inputs = relmap_test_fixture(&image, 0, false, true, false);
		RelmapCensusWalFixture f;
		RelmapCensusSideFixture side;
		ClusterRelmapPendingSet pending, before;

		relmap_census_begin(&f, &image, false);
		cluster_relmap_pending_init(&pending);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(NULL,
			&pending, inputs, NULL, 0), CLUSTER_RELMAP_PENDING_INVALID_INPUT);
		(void)relmap_census_emission(&f, fault == 0 ? InvalidOid : image.identity.dbid);
		if (fault == 0) {
			image.identity.dbid = InvalidOid;
			image.identity.shared = true;
		}
		relmap_census_side(&side, &image, 0, true);
		if (fault == 1)
			UT_ASSERT_EQ(cluster_relmap_pending_add(&pending, CLUSTER_RELMAP_PENDING_DROP,
				&side.key, true), CLUSTER_RELMAP_PENDING_OK);
		before = pending;
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side_wal(f.census,
			fault == 2 ? NULL : &pending, fault == 0 ? NULL : inputs, &side.source, 1),
			fault == 0 ? CLUSTER_RELMAP_PENDING_OK : fault == 1 ? CLUSTER_RELMAP_PENDING_CONFLICT
			: CLUSTER_RELMAP_PENDING_INVALID_INPUT);
		if (fault == 0) {
			/* No pending image: this success claims no native WAL coverage. */
			UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_TRUNCATE, &side.key));
			UT_ASSERT_EQ(relmap_census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		} else {
			UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
			relmap_census_pin_retained(&f, &image, true);
		}
		UT_ASSERT(inputs_pin_held);
		rf_side_online_plan_destroy_v1(&side.plan);
		relmap_census_end(&f);
		cold_census_done(&inputs);
	}
}

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	UT_PLAN(21);
	UT_RUN(test_retained_relmap_matches_each_selected_namespace);
	UT_RUN(test_retained_relmap_requires_exact_pending);
	UT_RUN(test_retained_relmap_scans_suffix_and_other_source);
	UT_RUN(test_retained_relmap_waits_for_open_and_rejects_online);
	UT_RUN(test_retained_relmap_original_recheck_and_error_keep_pin);
	UT_RUN(test_retained_relmap_invalid_input_is_read_only);
	UT_RUN(test_census_wal_rebuild_stage_and_publish_cuts);
	UT_RUN(test_census_wal_mismatch_is_atomic_and_sticky);
	UT_RUN(test_census_wal_wait_online_and_missing_scope_refuse);
	UT_RUN(test_census_wal_original_recheck_error_retains_both_pins);
	UT_RUN(test_census_wal_structural_conflict_leaves_destination);
	UT_RUN(test_census_wal_rechecks_borrowed_directory_after_scan);
	UT_RUN(test_census_wal_physical_root_recheck_still_required);
	UT_RUN(test_census_wal_clean_pair_and_missing_window);
	UT_RUN(test_census_side_wal_same_window_stage_publish_and_history);
	UT_RUN(test_census_side_wal_side_refusal_precedes_scan);
	UT_RUN(test_census_side_wal_wal_refusal_never_installs_side);
	UT_RUN(test_census_side_wal_recheck_and_error_keep_window_invalid);
	UT_RUN(test_census_side_wal_late_physical_root_rejects_provisional);
	UT_RUN(test_census_side_wal_collector_error_invalidates_before_wal);
	UT_RUN(test_census_side_wal_window_clean_pair_and_destination);
	UT_DONE();
	return ut_failed_count != 0;
}
