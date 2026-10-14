/* Native WAL targets and real physical files through the same root union.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define RECOVERY_ROOT_SET_FIXTURE_ONLY
#include "test_cluster_recovery_root_set.c"
#include "catalog/storage_xlog.h"
#include "../../backend/cluster/cluster_recovery_native_scan.c"

static XLogReaderState native_reader;
static DecodedXLogRecord native_decoded;
static ClusterWalSourceRef native_source;
static RfContributorStreamCutV1 native_cut;
static union {
	uint64 align;
	char bytes[256];
} native_payload;

static void
native_fixture(uint8 rmid, uint8 info, uint32 length)
{
	memset(&native_reader, 0, sizeof(native_reader));
	memset(&native_decoded, 0, sizeof(native_decoded));
	memset(&native_source, 0, sizeof(native_source));
	memset(&native_payload, 0, sizeof(native_payload));
	native_source.claim.identity.system_identifier = 817;
	memcpy(native_source.claim.identity.storage_uuid, root_uuid, 16);
	native_source.claim.identity.authority_uuid[0] = 6;
	native_source.claim.identity.origin_thread_id = 2;
	native_source.claim.identity.origin_node_id = 1;
	native_source.claim.identity.thread_claim_created_at = 103;
	native_source.claim.identity.origin_owner_incarnation = 71;
	native_source.claim.identity.root_lineage_seq = 3;
	native_source.claim.database_incarnation = 91;
	native_source.claim.max_config_generation = 2;
	native_source.claim.claim_sha256[0] = 18;
	native_source.timeline = 7;
	native_cut = (RfContributorStreamCutV1){ 0 };
	native_cut.failed_thread = 2;
	native_cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	native_cut.timeline_id = 7;
	native_cut.origin_owner_incarnation = 71;
	native_cut.scan_begin_inclusive = 0x3000;
	native_cut.scan_end_exclusive = 0x3100;
	native_cut.contributor_count = 1;
	native_reader.system_identifier = 817;
	native_reader.ReadRecPtr = native_decoded.lsn = 0x3000;
	native_reader.EndRecPtr = native_decoded.next_lsn = 0x3100;
	native_reader.record = &native_decoded;
	native_decoded.header.xl_rmid = rmid;
	native_decoded.header.xl_info = info;
	native_decoded.header.xl_xid = 4195136;
	native_decoded.header.xl_prev = 0x2000;
	native_decoded.main_data = native_payload.bytes;
	native_decoded.main_data_len = length;
	native_decoded.max_block_id = -1;
}

static ClusterRecoveryNativeScan *
native_create(RelFileLocator locator, ForkNumber forknum)
{
	ClusterRecoveryNativeScan *scan = NULL;
	xl_smgr_create create = { locator, forknum };

	native_fixture(RM_SMGR_ID, XLOG_SMGR_CREATE, sizeof(create));
	memcpy(native_payload.bytes, &create, sizeof(create));
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
													65536, &scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	return scan;
}

UT_TEST(test_create_preserves_missing_and_zero_length_without_a_fake_block)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryNativeScan *scan;
	ClusterRecoveryNativeEffect effect;
	unsigned found = 0;
	ClusterRecoveryRootsResult result;

	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	root_file(fd, "base/5/16385", "", 0);
	roots = begin_roots(fd, 1024 * 1024);
	for (unsigned i = 0; i < 2; i++) {
		scan = native_create((RelFileLocator){ 1663, 5, 16385 + i }, MAIN_FORKNUM);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		cluster_recovery_native_scan_destroy(scan);
	}
	result = finish_roots(roots, NULL);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED) {
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
		return;
	}
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELATION)
			continue;
		found++;
		UT_ASSERT_EQ(entry->file.size, 0);
		UT_ASSERT_EQ(entry->required_blocks, 0);
		UT_ASSERT(entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL);
		UT_ASSERT_EQ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0,
					 entry->file.locator.relNumber == 16385);
	}
	UT_ASSERT_EQ(found, 2);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_terminal_deletion_adds_every_shared_fork_and_keeps_page_requirement)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	xl_xact_commit commit = { 1234 };
	uint32 xinfo = XACT_XINFO_HAS_RELFILELOCATORS;
	int count = 1;
	RelFileLocator locator = { 1664, 0, 1260 };
	RfPageIdentityV1 page = target(1260, VISIBILITYMAP_FORKNUM, 7);
	unsigned forks = 0;
	ClusterRecoveryRootsResult result;
	size_t offset = 0;

	page.locator = locator;
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page), CLUSTER_RECOVERY_ROOTS_MORE);
	native_fixture(RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO,
				   sizeof(commit) + sizeof(xinfo) + sizeof(count) + sizeof(locator));
	memcpy(native_payload.bytes + offset, &commit, sizeof(commit));
	offset += sizeof(commit);
	memcpy(native_payload.bytes + offset, &xinfo, sizeof(xinfo));
	offset += sizeof(xinfo);
	memcpy(native_payload.bytes + offset, &count, sizeof(count));
	offset += sizeof(count);
	memcpy(native_payload.bytes + offset, &locator, sizeof(locator));
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
													65536, &scan),
				 CLUSTER_RECOVERY_NATIVE_MORE);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_RELATION_DROP);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_MORE);
	cluster_recovery_native_scan_destroy(scan);
	result = finish_roots(roots, NULL);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_OBSERVED);
	if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED) {
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 4);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
		UT_ASSERT_EQ(entry->file.locator.dbOid, 0);
		UT_ASSERT_STR_CONTAINS(entry->file.path, "global/1260");
		forks |= 1U << entry->file.forknum;
		UT_ASSERT_EQ(entry->required_blocks, entry->file.forknum == VISIBILITYMAP_FORKNUM ? 8 : 0);
	}
	UT_ASSERT_EQ(forks, (1U << MAIN_FORKNUM) | (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM)
							| (1U << SPACE_FORKNUM));
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_foreign_namespace_malformed_effect_or_late_add_poison_observation)
{
	int fd = new_root();
	ClusterRecoveryNativeScan *scan
		= native_create((RelFileLocator){ 1663, 5, 16385 }, MAIN_FORKNUM);
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeRecord record;
	uint8 digest[32];

	UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	record = *effect.record;
	for (unsigned variant = 0; variant < 5; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryNativeEffect changed = effect;

		if (variant == 0) {
			record.source.claim.database_incarnation++;
			changed.record = &record;
		}
		if (variant == 1)
			changed.length--;
		if (variant == 2)
			changed.kind = CLUSTER_RECOVERY_NATIVE_TERMINAL;
		if (variant == 3)
			changed.bytes = NULL;
		if (variant == 4)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &changed),
					 CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_root_set_destroy(roots);
	}
	cluster_recovery_native_scan_destroy(scan);
	remove_root(fd);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_create_preserves_missing_and_zero_length_without_a_fake_block);
	UT_RUN(test_terminal_deletion_adds_every_shared_fork_and_keeps_page_requirement);
	UT_RUN(test_foreign_namespace_malformed_effect_or_late_add_poison_observation);
	UT_DONE();
	return ut_failed_count != 0;
}
