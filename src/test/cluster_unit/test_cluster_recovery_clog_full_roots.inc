/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_clog_full_roots.inc
 *    Native FULL CLOG page births in the original physical ROOT union.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_clog_full_roots.inc
 *
 * NOTES
 *    PGRAC-original component cases use original source/root fixtures and
 *    constructed native effects. File observations do not certify an era,
 *    native lifetime, replay permission or joint completion.
 *-------------------------------------------------------------------------
 */

#include "access/slru.h"
#include "common/cryptohash.h"

static void
clog_full_root_effect(ClusterWalInputsV1 *inputs, uint32 source_index,
					 xl_clog_zeropage_full *birth, ClusterRecoveryNativeRecord *record,
					 ClusterRecoveryNativeEffect *effect)
{
	memset(record, 0, sizeof(*record));
	record->source = cluster_wal_inputs_at_v1(inputs, source_index)->source;
	record->header.xl_rmid = RM_CLOG_ID;
	record->header.xl_info = CLOG_ZEROPAGE_FULL;
	record->payload = (const char *)birth;
	record->payload_length = sizeof(*birth);
	*effect = (ClusterRecoveryNativeEffect){ .kind = CLUSTER_RECOVERY_NATIVE_CLOG_ZERO,
		.record = record, .bytes = birth, .length = sizeof(*birth), .ordinal = 0 };
}

static ClusterRecoveryRootsResult
clog_full_root_observe(ClusterRecoveryRootSet *roots)
{
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result;
	unsigned calls = 0;

	do {
		result = cluster_recovery_root_set_step(roots, &event);
		UT_ASSERT(++calls < 10000);
	} while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED);
	return result;
}

UT_TEST(test_clog_full_roots_keep_original_origin_and_page_extent)
{
	const uint32 pages[] = { 0, 33, UINT32_MAX / CLUSTER_NATIVE_CLOG_PER_PAGE };
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);

	for (unsigned source = 0; source < 2; source++) {
		for (unsigned shape = 0; shape < lengthof(pages); shape++) {
			uint32 page = pages[shape];
			uint32 raw = page == 0 ? FirstNormalTransactionId : page * CLUSTER_NATIVE_CLOG_PER_PAGE;
			xl_clog_zeropage_full birth = { page, 0, (UINT64_C(7) << 32) | raw };
			ClusterRecoveryNativeRecord record;
			ClusterRecoveryNativeEffect effect;
			ClusterRecoveryRootSet *roots;
			char path[MAXPGPATH];
			int fd;
			unsigned found = 0;

			roots = inventory_roots(inputs, path, &fd, false);
			clog_full_root_effect(inputs, source, &birth, &record, &effect);
			UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_MORE);
			UT_ASSERT_EQ(clog_full_root_observe(roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
				const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
				UT_ASSERT(entry != NULL);
				if (entry == NULL)
					continue;
				if (entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
					continue;
				found++;
				UT_ASSERT_EQ(entry->file.native_origin_node, record.source.claim.identity.origin_node_id);
				UT_ASSERT_EQ(entry->file.native_family, CLUSTER_RECOVERY_NATIVE_FAMILY_XACT);
				UT_ASSERT_EQ(entry->file.native_segment, page / SLRU_PAGES_PER_SEGMENT);
				UT_ASSERT_EQ(entry->required_blocks, page % SLRU_PAGES_PER_SEGMENT + 1);
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
				UT_ASSERT_EQ(entry->file.size, 0);
			}
			UT_ASSERT_EQ(found, 1);
			UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
			UT_ASSERT_EQ(clog_full_root_observe(roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT_EQ(birth.first_full_xid, (UINT64_C(7) << 32) | raw);
			inventory_roots_done(roots, fd, path);
		}
	}
	cold_census_done(&inputs);
}

UT_TEST(test_clog_full_roots_reject_malformed_or_detached_births_stickily)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);

	for (unsigned fault = 0; fault < 13; fault++) {
		xl_clog_zeropage_full birth = { 33, 0, (UINT64_C(8) << 32) | (33 * CLUSTER_NATIVE_CLOG_PER_PAGE) };
		xl_clog_zeropage_full copy;
		ClusterRecoveryNativeRecord record;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;

		roots = inventory_roots(inputs, path, &fd, false);
		clog_full_root_effect(inputs, 0, &birth, &record, &effect);
		copy = birth;
		switch (fault) {
		case 0: record.payload_length = effect.length = 0; break;
		case 1: record.payload_length = effect.length = sizeof(birth) - 1; break;
		case 2: record.payload_length = effect.length = sizeof(birth) + 1; break;
		case 3: birth.reserved_zero = 1; break;
		case 4: birth.pageno = -1; break;
		case 5: birth.first_full_xid = 0; break;
		case 6: birth.first_full_xid = (UINT64_C(8) << 32) | 1; break;
		case 7: birth.first_full_xid = (UINT64_C(8) << 32) | 2; break;
		case 8: birth.first_full_xid++; break;
		case 9: birth.pageno++; break;
		case 10: effect.bytes = (const uint8 *)&copy; break;
		case 11: effect.ordinal = 1; break;
		case 12: record.header.xl_rmid = RM_COMMIT_TS_ID; break;
		}
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_INVALID);
		clog_full_root_effect(inputs, 0, &copy, &record, &effect);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT_EQ(clog_full_root_observe(roots), CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		inventory_roots_done(roots, fd, path);
	}
	cold_census_done(&inputs);
}

UT_TEST(test_clog_full_roots_read_whole_original_file_and_revoke_on_drift)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
	xl_clog_zeropage_full birth = { 33, 0, (UINT64_C(7) << 32) | (33 * CLUSTER_NATIVE_CLOG_PER_PAGE) };
	ClusterRecoveryNativeRecord record;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryRootSet *roots;
	char path[MAXPGPATH];
	uint8 bytes[BLCKSZ] = { 0 }, expected[32];
	pg_cryptohash_ctx *hash = pg_cryptohash_create(PG_SHA256);
	int fd, file;
	unsigned found = 0;

	roots = inventory_roots(inputs, path, &fd, false);
	/* Finish constructing this fixture before taking its first observation. */
	cluster_recovery_root_set_destroy(roots);
	roots = NULL;
	clog_full_root_effect(inputs, 0, &birth, &record, &effect);
	UT_ASSERT_EQ(record.source.claim.identity.origin_node_id, 0);
	UT_ASSERT_EQ(mkdirat(fd, "native_side", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "native_side/origin_0", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "native_side/origin_0/pg_xact", 0700), 0);
	file = openat(fd, "native_side/origin_0/pg_xact/0001", O_CREAT | O_RDWR, 0600);
	UT_ASSERT(file >= 0 && hash != NULL);
	UT_ASSERT_EQ(pg_cryptohash_init(hash), 0);
	for (unsigned page = 0; page < 2; page++) {
		bytes[0] = page + 1;
		UT_ASSERT_EQ(write(file, bytes, sizeof(bytes)), sizeof(bytes));
		UT_ASSERT_EQ(pg_cryptohash_update(hash, bytes, sizeof(bytes)), 0);
	}
	UT_ASSERT_EQ(pg_cryptohash_final(hash, expected, sizeof(expected)), 0);
	pg_cryptohash_free(hash);
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, record.source.claim.identity.system_identifier,
				 record.source.claim.database_incarnation, record.source.claim.identity.storage_uuid,
				 1024 * 1024, &roots), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(clog_full_root_observe(roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		UT_ASSERT(entry != NULL);
		if (entry == NULL)
			continue;
		if (entry->file.kind != CLUSTER_RECOVERY_FILE_ORIGIN_SLRU)
			continue;
		found++;
		UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL | CLUSTER_RECOVERY_ROOT_PHYSICAL);
		UT_ASSERT_EQ(entry->required_blocks, 2);
		UT_ASSERT_EQ(entry->file.size, 2 * BLCKSZ);
		UT_ASSERT(memcmp(entry->content_sha256, expected, sizeof(expected)) == 0);
	}
	UT_ASSERT_EQ(found, 1);
	UT_ASSERT_EQ(pwrite(file, "x", 1, BLCKSZ + 7), 1);
	UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(clog_full_root_observe(roots), CLUSTER_RECOVERY_ROOTS_CHANGED);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	UT_ASSERT_EQ(close(file), 0);
	UT_ASSERT_EQ(unlinkat(fd, "native_side/origin_0/pg_xact/0001", 0), 0);
	UT_ASSERT_EQ(unlinkat(fd, "native_side/origin_0/pg_xact", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "native_side/origin_0", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(unlinkat(fd, "native_side", AT_REMOVEDIR), 0);
	inventory_roots_done(roots, fd, path);
	cold_census_done(&inputs);
}

UT_TEST(test_clog_full_roots_keep_original_source_and_unstarted_union_guards)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);

	for (unsigned fault = 0; fault < 4; fault++) {
		xl_clog_zeropage_full birth = { 0, 0, (UINT64_C(8) << 32) | FirstNormalTransactionId };
		ClusterRecoveryNativeRecord record;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryRootSet *roots;
		char path[MAXPGPATH];
		int fd;

		roots = inventory_roots(inputs, path, &fd, false);
		clog_full_root_effect(inputs, 0, &birth, &record, &effect);
		if (fault == 0) record.source.claim.database_incarnation++;
		if (fault == 1) record.source.claim.identity.storage_uuid[0] ^= 1;
		if (fault == 2) record.source.claim.identity.system_identifier++;
		if (fault == 3)
			UT_ASSERT_EQ(clog_full_root_observe(roots), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		inventory_roots_done(roots, fd, path);
	}
	cold_census_done(&inputs);
}
