/* Physical native maps and exact reference edges through real root readers.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define RECOVERY_ROOT_SET_FIXTURE_ONLY
#include "test_cluster_recovery_root_set.c"
#include "test_cluster_recovery_relmap_fixture.h"
#include "../../backend/cluster/cluster_recovery_relmap_scan.c"

static RelMapFile
physical_map(int count)
{
	char payload[TEST_RELMAP_LENGTH];
	RelMapFile map;
	test_relmap_payload(payload, false, count);
	memcpy(&map, payload + MinSizeOfRelmapUpdate, sizeof(map));
	return map;
}

static void
physical_crc(RelMapFile *map)
{
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
}

static int
map_root(void)
{
	int fd = new_root();
	UT_ASSERT_EQ(mkdirat(fd, "global", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	return fd;
}

static ClusterRecoveryRelmapScan *
physical_begin(ClusterRecoveryRootSet *roots)
{
	ClusterRecoveryRelmapScan *scan = NULL;
	UT_ASSERT_EQ(cluster_recovery_relmap_scan_begin(roots, 817, 91, root_uuid, 65536, &scan),
				 CLUSTER_RECOVERY_RELMAP_MORE);
	UT_ASSERT(scan != NULL);
	return scan;
}

static ClusterRecoveryRelmapResult
physical_drain(ClusterRecoveryRelmapScan *scan)
{
	ClusterRecoveryRelmapEntry out;
	for (unsigned i = 0; i < 20000; ++i) {
		ClusterRecoveryRelmapResult result = cluster_recovery_relmap_scan_next(scan, &out);
		if (result > CLUSTER_RECOVERY_RELMAP_ENTRY)
			return result;
	}
	UT_ASSERT(false);
	return CLUSTER_RECOVERY_RELMAP_INVALID;
}

static void
physical_sticky(ClusterRecoveryRelmapScan *scan, ClusterRecoveryRootSet *roots,
				ClusterRecoveryRelmapResult expected)
{
	ClusterRecoveryRelmapEntry out, zero = { 0 };
	ClusterRecoveryRelmapDiag diag, empty = { 0 };
	uint8 digest[32];
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_recovery_relmap_scan_next(scan, &out), expected);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(!cluster_recovery_relmap_scan_diag(scan, &diag));
	UT_ASSERT_EQ(memcmp(&diag, &empty, sizeof(diag)), 0);
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
}

UT_TEST(test_physical_maps_all_copies_and_reference_edges)
{
	int fd = map_root();
	RelMapFile map = physical_map(2), empty = physical_map(0), full = physical_map(MAX_MAPPINGS);
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryRelmapScan *scan;
	ClusterRecoveryRelmapEntry out;
	ClusterRecoveryRelmapDiag diag = { 0 };
	ClusterRecoveryRelmapResult result = CLUSTER_RECOVERY_RELMAP_MORE;
	unsigned images = 0, mappings = 0, targets = 0, shorts = 0, missing = 0, global = 0,
			 wal_only = 0;
	PGAlignedBlock page = { 0 };
	RfPageIdentityV1 need = target(16385, MAIN_FORKNUM, 2);
	root_file(fd, "global/pg_filenode.map", &empty, sizeof(empty));
	root_file(fd, "global/pg_filenode.map.bak", &full, sizeof(full));
	root_file(fd, "base/5/pg_filenode.map", &map, sizeof(map));
	map.mappings[0].mapfilenumber = 99999;
	physical_crc(&map);
	root_file(fd, "base/5/pg_filenode.map.bak", &map,
			  sizeof(map)); /* disagreement is not a winner */
	root_file(fd, "base/5/16385", page.data, BLCKSZ);
	root_file(fd, "base/5/16385_vm", page.data, 0);
	root_file(fd, "global/16385", page.data, 0);
	root_file(fd, "base/5/pgrac_relmap_authority", "unknown-version", 15);
	roots = begin_roots(fd, 2 * 1024 * 1024);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &need), CLUSTER_RECOVERY_ROOTS_MORE);
	need = target(16386, FSM_FORKNUM, 99);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &need), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = physical_begin(roots);
	for (unsigned i = 0; scan != NULL && i < 20000; ++i) {
		result = cluster_recovery_relmap_scan_next(scan, &out);
		if (result > CLUSTER_RECOVERY_RELMAP_ENTRY)
			break;
		if (result != CLUSTER_RECOVERY_RELMAP_ENTRY)
			continue;
		UT_ASSERT(out.image != NULL);
		if (!out.image)
			break;
		UT_ASSERT_EQ(out.key.system_identifier, 817);
		UT_ASSERT_EQ(out.key.database_incarnation, 91);
		UT_ASSERT_EQ(memcmp(out.key.storage_uuid, root_uuid, 16), 0);
		UT_ASSERT_EQ(out.key.locator.spcOid, out.key.locator.dbOid == 0 ? 1664 : 1663);
		if (out.kind == CLUSTER_RECOVERY_RELMAP_IMAGE) {
			images++;
			UT_ASSERT(out.file_kind == CLUSTER_RECOVERY_FILE_RELMAP
					  || out.file_kind == CLUSTER_RECOVERY_FILE_RELMAP_BACKUP);
			if (out.image->num_mappings < MAX_MAPPINGS)
				UT_ASSERT_EQ(((uint8 *)&out.image->mappings[out.image->num_mappings])[0], 0xa5);
		} else if (out.kind == CLUSTER_RECOVERY_RELMAP_MAPPING) {
			mappings++;
			UT_ASSERT(out.ordinal < (uint32)out.image->num_mappings);
			UT_ASSERT_EQ(
				memcmp(&out.mapping, &out.image->mappings[out.ordinal], sizeof(out.mapping)), 0);
		} else {
			UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_RELMAP_TARGET);
			targets++;
			UT_ASSERT(out.forknum == MAIN_FORKNUM || out.forknum == FSM_FORKNUM
					  || out.forknum == VISIBILITYMAP_FORKNUM || out.forknum == SPACE_FORKNUM);
			if (out.key.locator.dbOid == 0)
				global++;
			if (!out.physical)
				missing++;
			if (out.target != NULL && !out.physical) {
				wal_only++;
				UT_ASSERT_EQ(out.target->required_blocks, 100);
				UT_ASSERT(!out.extent_present);
			}
			if (out.physical && !out.extent_present)
				shorts++;
			if (out.key.locator.dbOid == 5 && out.mapping.mapfilenumber == 16385
				&& out.forknum == MAIN_FORKNUM) {
				UT_ASSERT(out.target != NULL);
				if (out.target)
					UT_ASSERT_EQ(out.target->required_blocks, 3);
				UT_ASSERT(!out.extent_present);
			}
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_DRAINED);
	UT_ASSERT_EQ(images, 4);
	UT_ASSERT_EQ(mappings, 68);
	UT_ASSERT_EQ(targets, 272);
	UT_ASSERT_EQ(wal_only, 2);
	UT_ASSERT_EQ(global, 256);
	UT_ASSERT_EQ(shorts, 1);
	UT_ASSERT_EQ(missing, 269);
	UT_ASSERT(cluster_recovery_relmap_scan_diag(scan, &diag));
	UT_ASSERT_EQ(diag.images, images);
	UT_ASSERT_EQ(diag.mappings, mappings);
	UT_ASSERT_EQ(diag.targets, targets);
	UT_ASSERT_EQ(diag.short_targets, shorts);
	UT_ASSERT_EQ(diag.missing_targets, missing);
	UT_ASSERT_EQ(diag.rereads, 4);
	cluster_recovery_relmap_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_physical_maps_reject_bad_bytes_and_shape)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		int fd = map_root();
		RelMapFile map = physical_map(2);
		size_t size = sizeof(map);
		char large[sizeof(map) + 1] = { 0 };
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRelmapScan *scan;
		if (fault == 0)
			size = 0;
		if (fault == 1)
			size--;
		if (fault == 2)
			map.magic++;
		if (fault == 3)
			map.num_mappings = -1;
		if (fault == 4)
			map.num_mappings = MAX_MAPPINGS + 1;
		if (fault == 5)
			map.mappings[0].mapfilenumber = 0;
		if (fault == 6)
			map.mappings[0].mapoid = 0;
		if (fault != 7)
			physical_crc(&map);
		else
			map.crc ^= 1;
		memcpy(large, &map, sizeof(map));
		if (fault == 8)
			size++;
		root_file(fd, "base/5/pg_filenode.map", large, size);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = physical_begin(roots);
		if (scan) {
			ClusterRecoveryRelmapResult expected = fault < 2 ? CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED
															 : CLUSTER_RECOVERY_RELMAP_CORRUPT;
			UT_ASSERT_EQ(physical_drain(scan), expected);
			physical_sticky(scan, roots, expected);
		}
		cluster_recovery_relmap_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_physical_maps_full_reread_and_final_root_drift)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		int fd = map_root();
		RelMapFile map = physical_map(1);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRelmapScan *scan;
		ClusterRecoveryRelmapEntry out;
		root_file(fd, "base/5/pg_filenode.map", &map, sizeof(map));
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = physical_begin(roots);
		if (scan) {
			ClusterRecoveryRelmapResult result;
			do {
				result = cluster_recovery_relmap_scan_next(scan, &out);
			} while (result == CLUSTER_RECOVERY_RELMAP_MORE);
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_ENTRY);
			UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_RELMAP_IMAGE);
			if (mode == 2)
				UT_ASSERT_EQ(physical_drain(scan), CLUSTER_RECOVERY_RELMAP_DRAINED);
			map.mappings[63].mapoid ^= 1;
			physical_crc(&map); /* inactive bytes, valid native CRC */
			if (mode == 1)
				UT_ASSERT_EQ(unlinkat(fd, "base/5/pg_filenode.map", 0), 0);
			root_file(fd, "base/5/pg_filenode.map", &map, sizeof(map));
			if (mode == 2) {
				UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
							 CLUSTER_RECOVERY_ROOTS_MORE);
				UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CHANGED);
			} else
				UT_ASSERT_EQ(physical_drain(scan), CLUSTER_RECOVERY_RELMAP_CHANGED);
			physical_sticky(scan, roots, CLUSTER_RECOVERY_RELMAP_CHANGED);
		}
		cluster_recovery_relmap_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_physical_maps_io_short_read_and_namespace_refuse)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		int fd = map_root();
		RelMapFile map = physical_map(1);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRelmapScan *scan;
		root_file(fd, "global/pg_filenode.map", &map, sizeof(map));
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		scan = physical_begin(roots);
		inject_pread_fault = fault <= 2 ? fault : 0;
		if (fault == 3)
			cluster_recovery_root_set_invalidate(roots);
		if (scan) {
			ClusterRecoveryRelmapResult result = physical_drain(scan);
			UT_ASSERT(result == CLUSTER_RECOVERY_RELMAP_IO
					  || result == CLUSTER_RECOVERY_RELMAP_CHANGED);
			physical_sticky(scan, roots, result);
		}
		inject_pread_fault = 0;
		cluster_recovery_relmap_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_physical_maps_missing_map_is_not_empty)
{
	int fd = map_root();
	char payload[TEST_RELMAP_LENGTH];
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryRelmapScan *scan;
	ClusterRecoveryNativeRecord record = { 0 };
	ClusterRecoveryNativeEffect effect = { 0 };
	test_relmap_payload(payload, false, 1);
	record.source.claim.identity.system_identifier = 817;
	record.source.claim.database_incarnation = 91;
	memcpy(record.source.claim.identity.storage_uuid, root_uuid, 16);
	record.source.claim.identity.origin_thread_id = 1;
	record.source.claim.identity.thread_claim_created_at = 1;
	record.source.claim.identity.origin_owner_incarnation = 1;
	record.source.claim.identity.root_lineage_seq = 1;
	record.source.claim.identity.authority_uuid[0] = 1;
	record.source.claim.max_config_generation = 1;
	record.source.claim.claim_sha256[0] = 1;
	record.header.xl_rmid = RM_RELMAP_ID;
	record.header.xl_info = XLOG_RELMAP_UPDATE;
	record.payload = payload;
	record.payload_length = sizeof(payload);
	record.nmappings = 1;
	record.mappings_offset = MinSizeOfRelmapUpdate + offsetof(RelMapFile, mappings);
	record.tablespace = 1663;
	record.database = 5;
	effect.kind = CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE;
	effect.record = &record;
	effect.bytes = payload;
	effect.length = sizeof(payload);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	scan = physical_begin(roots);
	if (scan) {
		UT_ASSERT_EQ(physical_drain(scan), CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
		physical_sticky(scan, roots, CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
	}
	cluster_recovery_relmap_scan_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_physical_maps_budget_identity_and_bounded_selection)
{
	for (unsigned mode = 0; mode < 4; ++mode) {
		int fd = map_root();
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRelmapScan *scan = NULL;
		for (unsigned i = 0; i < 260; ++i) {
			char name[80];
			snprintf(name, sizeof(name), "unknown_%u", i);
			root_file(fd, name, "x", 1);
		}
		roots = begin_roots(fd, 2 * 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		if (mode < 3) {
			UT_ASSERT_EQ(cluster_recovery_relmap_scan_begin(roots, mode == 1 ? 818 : 817,
															mode == 2 ? 92 : 91, root_uuid,
															mode == 0 ? 1 : 65536, &scan),
						 mode == 0 ? CLUSTER_RECOVERY_RELMAP_CAPACITY
								   : CLUSTER_RECOVERY_RELMAP_INVALID);
			UT_ASSERT(scan == NULL);
			UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		} else {
			ClusterRecoveryRelmapEntry out;
			ClusterRecoveryRelmapDiag diag;
			scan = physical_begin(roots);
			if (scan) {
				UT_ASSERT_EQ(cluster_recovery_relmap_scan_next(scan, &out),
							 CLUSTER_RECOVERY_RELMAP_MORE);
				UT_ASSERT(cluster_recovery_relmap_scan_diag(scan, &diag));
				UT_ASSERT_EQ(diag.skipped_roots, 128);
				UT_ASSERT_EQ(physical_drain(scan), CLUSTER_RECOVERY_RELMAP_DRAINED);
				cluster_recovery_root_set_invalidate(roots);
				physical_sticky(scan, roots, CLUSTER_RECOVERY_RELMAP_CHANGED);
			}
		}
		cluster_recovery_relmap_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_physical_maps_all_copies_and_reference_edges);
	UT_RUN(test_physical_maps_reject_bad_bytes_and_shape);
	UT_RUN(test_physical_maps_full_reread_and_final_root_drift);
	UT_RUN(test_physical_maps_io_short_read_and_namespace_refuse);
	UT_RUN(test_physical_maps_missing_map_is_not_empty);
	UT_RUN(test_physical_maps_budget_identity_and_bounded_selection);
	UT_DONE();
	return ut_failed_count != 0;
}
