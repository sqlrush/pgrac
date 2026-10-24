/* Real ROOT readers and physical relmap pairs, not authority-return mocks.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#define RECOVERY_ROOT_SET_FIXTURE_ONLY
#include "test_cluster_recovery_root_set.c"
#include "cluster/cluster_relmap_retention_census.h"
#include "cluster/cluster_relmap_pending.h"

#define CENSUS_BUDGET (128 * 1024)
#define CENSUS_FULL UINT64CONST(0x200000123)

typedef struct CensusFixture {
	int fd;
	ClusterRecoveryRootSet *roots;
	ClusterRelmapRetentionCensus *census;
	ClusterRelmapRetentionDirectory origin[2];
	ClusterRelmapRetentionDirectory *directories[2];
} CensusFixture;

static RelMapFile
census_map(RelFileNumber number)
{
	RelMapFile map = { 0 };

	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0].mapoid = 1259;
	map.mappings[0].mapfilenumber = number;
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	return map;
}

static void
census_wire(uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE], Oid dbid, uint16 owner,
	bool pending, bool published)
{
	ClusterRelmapV2Image image = { 0 };

	image.identity = (ClusterRelmapV2Namespace){ 817, 91, dbid, dbid == InvalidOid };
	image.committed_generation = published ? 2 : 1;
	image.committed = census_map(published ? 40001 : 40000);
	if (pending) {
		image.pending_generation = 2;
		image.pending_owner = (ClusterRelmapV2Owner){ owner, CENSUS_FULL, 0, 101, 909, 202 };
		image.pending = census_map(40001);
	}
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, CLUSTER_RELMAP_V2_FILE_SIZE),
		CLUSTER_RELMAP_V2_VALID);
}

/* primary clean + backup pending is a legal discard cut. */
static void
census_pair(int fd, Oid dbid, uint16 owner, bool primary_pending, bool backup_pending,
	bool published)
{
	char path[MAXPGPATH], directory[32];
	uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];

	if (dbid == InvalidOid)
		strlcpy(directory, "global", sizeof(directory));
	else
		snprintf(directory, sizeof(directory), "base/%u", dbid);
	census_wire(bytes, dbid, owner, primary_pending, published);
	snprintf(path, sizeof(path), "%s/pgrac_relmap_authority", directory);
	root_file(fd, path, bytes, sizeof(bytes));
	census_wire(bytes, dbid, owner, backup_pending, false);
	snprintf(path, sizeof(path), "%s/pgrac_relmap_authority.bak", directory);
	root_file(fd, path, bytes, sizeof(bytes));
}

static void
census_fixture(CensusFixture *f, bool pending)
{
	memset(f, 0, sizeof(*f));
	f->fd = new_root();
	if (f->fd < 0 || mkdirat(f->fd, "global", 0700) != 0
		|| mkdirat(f->fd, "base", 0700) != 0 || mkdirat(f->fd, "base/5", 0700) != 0)
		abort();
	census_pair(f->fd, InvalidOid, 0, false, false, false);
	census_pair(f->fd, 5, 3, false, pending, false);
	root_file(f->fd, "unrelated", "cold", 4);
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(cluster_relmap_retention_build_start(&f->origin[i], 817, 91,
			i == 0 ? 3 : 7, 1, UINT64CONST(0x200000003), UINT64CONST(0x200000400)),
			CLUSTER_RELMAP_RETENTION_OK);
		/* The caller array order must not select the pending origin. */
		f->directories[1 - i] = &f->origin[i];
	}
}

static bool
census_observe_begin(CensusFixture *f)
{
	f->roots = begin_roots(f->fd, 4 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(f->roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_relmap_retention_census_begin(f->roots, 817, 91, root_uuid,
		f->directories, 2, CENSUS_BUDGET, &f->census), CLUSTER_RELMAP_CENSUS_MORE);
	UT_ASSERT(f->census != NULL);
	return f->census != NULL;
}

static ClusterRelmapCensusResult
census_drain(CensusFixture *f)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++)
		result = cluster_relmap_retention_census_next(f->census);
	UT_ASSERT_NE(result, CLUSTER_RELMAP_CENSUS_MORE);
	return result;
}

static const ClusterRelmapRetentionPin *
census_pin(const ClusterRelmapRetentionDirectory *directory, Oid dbid)
{
	for (unsigned i = 0; i < lengthof(directory->pins); i++)
		if (directory->pins[i].occupied && directory->pins[i].key.identity.dbid == dbid)
			return &directory->pins[i];
	return NULL;
}

static bool
census_until_pin(CensusFixture *f)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++) {
		if (census_pin(&f->origin[0], 5) != NULL)
			return true;
		result = cluster_relmap_retention_census_next(f->census);
	}
	UT_ASSERT(false);
	return false;
}

static void
census_refused(CensusFixture *f)
{
	uint8 digest[32];

	UT_ASSERT(!cluster_recovery_root_set_digest(f->roots, digest));
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT_EQ(f->origin[i].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		UT_ASSERT_NE(cluster_relmap_retention_build_finish(&f->origin[i]),
			CLUSTER_RELMAP_RETENTION_OK);
	}
}

static void
census_cleanup(CensusFixture *f)
{
	cluster_relmap_retention_census_destroy(f->census);
	cluster_recovery_root_set_destroy(f->roots);
	remove_root(f->fd);
	UT_ASSERT_EQ(rmdir(test_path), 0);
}

UT_TEST(test_all_physical_namespaces_and_original_backup_only_pending)
{
	CensusFixture f;
	const ClusterRelmapRetentionPin *pin;
	uint8 before[32], after[32];

	census_fixture(&f, true);
	UT_ASSERT_EQ(mkdirat(f.fd, "base/6", 0700), 0);
	census_pair(f.fd, 6, 7, true, true, false);
	if (census_observe_begin(&f)) {
		UT_ASSERT(cluster_recovery_root_set_digest(f.roots, before));
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(cluster_recovery_root_set_digest(f.roots, after));
		UT_ASSERT_EQ(memcmp(before, after, sizeof(before)), 0);
		for (unsigned i = 0; i < 2; i++) {
			pin = census_pin(&f.origin[i], i == 0 ? 5 : 6);
			UT_ASSERT(pin != NULL);
			if (pin != NULL) {
				UT_ASSERT_EQ(pin->key.identity.system_identifier, 817);
				UT_ASSERT_EQ(pin->key.identity.database_incarnation, 91);
				UT_ASSERT(!pin->key.identity.shared);
				UT_ASSERT_EQ(pin->key.origin, i == 0 ? 3 : 7);
				UT_ASSERT_EQ(pin->key.generation, 2);
				UT_ASSERT_EQ(pin->key.top_full_xid, CENSUS_FULL);
				UT_ASSERT_EQ(pin->key.membership_epoch, 0);
				UT_ASSERT_EQ(pin->key.admitted_incarnation, 101);
				UT_ASSERT_EQ(pin->key.boot_incarnation, 202);
				UT_ASSERT_EQ(pin->wal_end, 909);
				UT_ASSERT(pin->may_be_staged);
			}
			UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
			UT_ASSERT_NE(cluster_relmap_retention_allocation_check(&f.origin[i],
				UINT64CONST(0x200000401)), CLUSTER_RELMAP_RETENTION_OK);
		}
	}
	census_cleanup(&f);
}

UT_TEST(test_all_legal_pair_cuts_keep_pending_without_publishing_complete)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		CensusFixture f;

		census_fixture(&f, true);
		census_pair(f.fd, 5, 3, cut == 0, cut != 0, cut == 1);
		if (census_observe_begin(&f)) {
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			cluster_relmap_retention_census_destroy(f.census);
			f.census = NULL;
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_clean_census_stays_unpublished)
{
	CensusFixture f;

	census_fixture(&f, false);
	if (census_observe_begin(&f)) {
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		for (unsigned i = 0; i < 2; i++) {
			UT_ASSERT(census_pin(&f.origin[i], 5) == NULL);
			UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
	}
	census_cleanup(&f);
}

UT_TEST(test_missing_copies_and_unreferenced_database_never_mean_empty)
{
	const char *missing[] = { "global/pgrac_relmap_authority",
		"global/pgrac_relmap_authority.bak", "base/5/pgrac_relmap_authority",
		"base/5/pgrac_relmap_authority.bak", NULL };

	for (unsigned i = 0; i < lengthof(missing); i++) {
		CensusFixture f;

		census_fixture(&f, false);
		if (missing[i] != NULL)
			UT_ASSERT_EQ(unlinkat(f.fd, missing[i], 0), 0);
		else
			UT_ASSERT_EQ(mkdirat(f.fd, "base/999", 0700), 0);
		if (census_observe_begin(&f)) {
			UT_ASSERT_NE(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_global_and_base_roots_are_required)
{
	for (unsigned i = 0; i < 2; i++) {
		CensusFixture f;

		census_fixture(&f, false);
		if (i == 0) {
			UT_ASSERT_EQ(unlinkat(f.fd, "global/pgrac_relmap_authority", 0), 0);
			UT_ASSERT_EQ(unlinkat(f.fd, "global/pgrac_relmap_authority.bak", 0), 0);
			UT_ASSERT_EQ(unlinkat(f.fd, "global", AT_REMOVEDIR), 0);
		} else {
			UT_ASSERT_EQ(unlinkat(f.fd, "base/5/pgrac_relmap_authority", 0), 0);
			UT_ASSERT_EQ(unlinkat(f.fd, "base/5/pgrac_relmap_authority.bak", 0), 0);
			UT_ASSERT_EQ(unlinkat(f.fd, "base/5", AT_REMOVEDIR), 0);
			UT_ASSERT_EQ(unlinkat(f.fd, "base", AT_REMOVEDIR), 0);
		}
		if (census_observe_begin(&f)) {
			UT_ASSERT_NE(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_degraded_foreign_short_and_conflicting_pairs_refuse)
{
	for (unsigned bad = 0; bad < 5; bad++) {
		CensusFixture f;
		uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];

		census_fixture(&f, true);
		census_wire(bytes, bad == 1 ? 6 : 5, bad == 2 ? 7 : 3, true, false);
		if (bad == 0)
			bytes[0] ^= 1;
		if (bad == 4)
			bytes[CLUSTER_RELMAP_V2_PENDING_OFFSET] ^= 1;
		root_file(f.fd, "base/5/pgrac_relmap_authority", bytes,
			bad == 3 ? sizeof(bytes) - 1 : sizeof(bytes));
		if (census_observe_begin(&f)) {
			UT_ASSERT_NE(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_unknown_origin_native_gone_and_fullxid_window_refuse)
{
	for (unsigned bad = 0; bad < 3; bad++) {
		CensusFixture f;

		census_fixture(&f, true);
		if (bad == 0)
			census_pair(f.fd, 5, 8, true, true, false);
		else if (bad == 1)
			f.origin[0].native_floor = CENSUS_FULL + 1;
		else
			f.origin[0].last_issued = CENSUS_FULL + UINT64CONST(0x80000000);
		if (census_observe_begin(&f)) {
			UT_ASSERT_NE(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_later_failure_preserves_earlier_pending_and_is_sticky)
{
	CensusFixture f;
	ClusterRelmapCensusResult result;

	census_fixture(&f, true);
	UT_ASSERT_EQ(mkdirat(f.fd, "base/6", 0700), 0);
	if (census_observe_begin(&f)) {
		UT_ASSERT(census_until_pin(&f));
		result = census_drain(&f);
		UT_ASSERT_NE(result, CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), result);
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
		census_refused(&f);
	}
	census_cleanup(&f);
}

UT_TEST(test_whole_root_recheck_rejects_new_namespace_inode_and_unrelated_change)
{
	for (unsigned change = 0; change < 3; change++) {
		CensusFixture f;
		uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];

		census_fixture(&f, true);
		if (census_observe_begin(&f) && census_until_pin(&f)) {
			if (change == 0) {
				UT_ASSERT_EQ(mkdirat(f.fd, "base/8", 0700), 0);
				census_pair(f.fd, 8, 0, false, false, false);
			} else if (change == 1) {
				census_wire(bytes, 5, 3, true, false);
				root_file(f.fd, "replacement", bytes, sizeof(bytes));
				UT_ASSERT_EQ(renameat(f.fd, "replacement", f.fd,
					"base/5/pgrac_relmap_authority.bak"), 0);
			} else
				root_file(f.fd, "unrelated", "warm", 4);
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_CHANGED);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_origin_qualification_cannot_change_during_scan)
{
	for (unsigned change = 0; change < 3; change++) {
		CensusFixture f;

		census_fixture(&f, true);
		if (census_observe_begin(&f) && census_until_pin(&f)) {
			if (change == 0)
				f.origin[0].generation++;
			else if (change == 1)
				f.origin[0].native_floor++;
			else
				f.origin[0].last_issued++;
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_CHANGED);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_interrupted_cursor_invalidates_without_losing_pending)
{
	CensusFixture f;

	census_fixture(&f, true);
	if (census_observe_begin(&f) && census_until_pin(&f)) {
		cluster_relmap_retention_census_destroy(f.census);
		f.census = NULL;
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
		census_refused(&f);
	}
	census_cleanup(&f);
}

UT_TEST(test_read_io_short_read_and_corruption_never_drain)
{
	for (unsigned fault = 1; fault < 4; fault++) {
		CensusFixture f;

		census_fixture(&f, true);
		if (census_observe_begin(&f)) {
			inject_pread_fault = fault < 3 ? fault : 0;
			inject_read_corruption = fault == 3;
			UT_ASSERT_NE(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			inject_pread_fault = 0;
			inject_read_corruption = false;
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_invalid_begin_poisoning_and_oversized_array_bound)
{
	for (unsigned bad = 0; bad < 7; bad++) {
		CensusFixture f;
		uint8 uuid[16];
		size_t count = 2;
		ClusterRelmapCensusResult result;

		census_fixture(&f, false);
		f.roots = begin_roots(f.fd, 4 * 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(f.roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		memcpy(uuid, root_uuid, sizeof(uuid));
		if (bad == 1)
			f.directories[1] = f.directories[0];
		if (bad == 2)
			f.directories[0] = NULL;
		if (bad == 3)
			uuid[0]++;
		if (bad == 6)
			count = CLUSTER_RELMAP_RETENTION_MAX_ORIGINS + 1;
		result = cluster_relmap_retention_census_begin(f.roots, bad == 4 ? 818 : 817,
			91, uuid, f.directories, count, bad == 0 ? 0 : CENSUS_BUDGET,
			bad == 5 ? NULL : &f.census);
		UT_ASSERT_NE(result, CLUSTER_RELMAP_CENSUS_MORE);
		UT_ASSERT(f.census == NULL);
		if (bad == 6) {
			/* count exceeds the actual two-element array: no element may be read. */
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
			UT_ASSERT_EQ(f.origin[1].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		} else
			for (unsigned i = 0; i < 2; i++)
				if (f.directories[i] != NULL)
					UT_ASSERT_EQ(f.directories[i]->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		census_cleanup(&f);
	}
}

static void
census_committed_empty(const ClusterRelmapCensusCommitted *out)
{
	ClusterRelmapCensusCommitted empty = { 0 };

	UT_ASSERT_EQ(memcmp(out, &empty, sizeof(empty)), 0);
}

UT_TEST(test_committed_reader_all_legal_cuts_and_original_pending_origin)
{
	for (unsigned cut = 0; cut < 5; cut++) {
		CensusFixture f;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		ClusterRelmapCensusCommitted out;
		unsigned seen = 0;
		bool pending = cut != 0;

		census_fixture(&f, pending);
		census_pair(f.fd, 5, 3, cut == 1 || cut == 4,
			cut == 2 || cut == 3 || cut == 4, cut == 3);
		if (census_observe_begin(&f)) {
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				tick++) {
				RelMapFile expected;
				unsigned bit;

				memset(&out, 0xa5, sizeof(out));
				result = cluster_relmap_retention_census_next_committed(f.census, &out);
				if (!out.present) {
					census_committed_empty(&out);
					continue;
				}
				UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_MORE);
				UT_ASSERT_EQ(out.identity.system_identifier, 817);
				UT_ASSERT_EQ(out.identity.database_incarnation, 91);
				UT_ASSERT(out.identity.dbid == 5 || out.identity.dbid == InvalidOid);
				UT_ASSERT_EQ(out.identity.shared, out.identity.dbid == InvalidOid);
				bit = out.identity.shared ? 1 : 2;
				UT_ASSERT_EQ(seen & bit, 0);
				seen |= bit;
				expected = census_map(!out.identity.shared && cut == 3 ? 40001 : 40000);
				UT_ASSERT_EQ(memcmp(&out.map, &expected, sizeof(expected)), 0);
				UT_ASSERT_EQ(out.generation, !out.identity.shared && cut == 3 ? 2 : 1);
				if (!out.identity.shared)
					UT_ASSERT_EQ(census_pin(&f.origin[0], 5) != NULL, pending);
				for (unsigned i = 0; i < 2; i++)
					UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
			}
			UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(seen, 3);
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT_EQ(cluster_relmap_retention_census_next_committed(f.census, &out),
				CLUSTER_RELMAP_CENSUS_DRAINED);
			census_committed_empty(&out);
			UT_ASSERT_EQ(census_pin(&f.origin[0], 5) != NULL, pending);
			UT_ASSERT(census_pin(&f.origin[1], 5) == NULL);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_committed_reader_requires_pending_restore_before_emission)
{
	for (unsigned failure = 0; failure < 2; failure++) {
		CensusFixture f;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		ClusterRelmapCensusCommitted out;

		census_fixture(&f, true);
		if (failure == 0)
			f.origin[0].native_floor = CENSUS_FULL + 1;
		else
			census_pair(f.fd, 5, 4, true, true, false);
		if (census_observe_begin(&f)) {
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				tick++) {
				memset(&out, 0xa5, sizeof(out));
				result = cluster_relmap_retention_census_next_committed(f.census, &out);
				if (out.present)
					UT_ASSERT_NE(out.identity.dbid, 5);
				else
					census_committed_empty(&out);
			}
			UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			census_committed_empty(&out);
			UT_ASSERT(census_pin(&f.origin[0], 5) == NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_committed_reader_emitted_maps_are_provisional_until_whole_recheck)
{
	for (unsigned change = 0; change < 3; change++) {
		CensusFixture f;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		ClusterRelmapCensusCommitted out;
		bool emitted = false;

		census_fixture(&f, true);
		if (census_observe_begin(&f)) {
			for (unsigned tick = 0; tick < 20000 && !emitted
				&& result == CLUSTER_RELMAP_CENSUS_MORE; tick++) {
				result = cluster_relmap_retention_census_next_committed(f.census, &out);
				emitted = out.present && out.identity.dbid == 5;
			}
			UT_ASSERT(emitted);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			if (change == 0)
				UT_ASSERT_EQ(unlinkat(f.fd, "global/pgrac_relmap_authority.bak", 0), 0);
			else if (change == 1) {
				UT_ASSERT_EQ(mkdirat(f.fd, "base/8", 0700), 0);
				census_pair(f.fd, 8, 0, false, false, false);
			} else
				f.origin[0].last_issued++;
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				tick++) {
				memset(&out, 0xa5, sizeof(out));
				result = cluster_relmap_retention_census_next_committed(f.census, &out);
				if (!out.present)
					census_committed_empty(&out);
			}
			UT_ASSERT(result > CLUSTER_RELMAP_CENSUS_DRAINED);
			census_committed_empty(&out);
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT_EQ(cluster_relmap_retention_census_next_committed(f.census, &out),
				result);
			census_committed_empty(&out);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_committed_reader_io_refusal_clears_output)
{
	for (unsigned fault = 1; fault < 4; fault++) {
		CensusFixture f;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		ClusterRelmapCensusCommitted out;

		census_fixture(&f, true);
		if (census_observe_begin(&f)) {
			inject_pread_fault = fault < 3 ? fault : 0;
			inject_read_corruption = fault == 3;
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				tick++) {
				memset(&out, 0xa5, sizeof(out));
				result = cluster_relmap_retention_census_next_committed(f.census, &out);
				census_committed_empty(&out);
			}
			inject_pread_fault = 0;
			inject_read_corruption = false;
			UT_ASSERT(result > CLUSTER_RELMAP_CENSUS_DRAINED);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_committed_reader_legacy_and_optional_output_share_one_cursor)
{
	for (unsigned reader = 0; reader < 2; reader++) {
		CensusFixture f;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		ClusterRelmapCensusCommitted out;

		census_fixture(&f, true);
		if (census_observe_begin(&f)) {
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				tick++)
				result = reader == 0 ? cluster_relmap_retention_census_next(f.census)
					: cluster_relmap_retention_census_next_committed(f.census, NULL);
			UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_DRAINED);
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT_EQ(cluster_relmap_retention_census_next_committed(f.census, &out),
				CLUSTER_RELMAP_CENSUS_DRAINED);
			census_committed_empty(&out);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_committed_reader_invalid_cursor_clears_output)
{
	ClusterRelmapCensusCommitted out;

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_relmap_retention_census_next_committed(NULL, &out),
		CLUSTER_RELMAP_CENSUS_INVALID);
	census_committed_empty(&out);
	UT_ASSERT_EQ(cluster_relmap_retention_census_next_committed(NULL, NULL),
		CLUSTER_RELMAP_CENSUS_INVALID);
}

static ClusterRelmapPendingDescriptor
census_descriptor(Oid dbid, ClusterRelmapPendingKind kind, RelFileNumber number)
{
	ClusterRelmapPendingDescriptor descriptor = { 0 };

	descriptor.kind = kind;
	descriptor.identity.system_identifier = 817;
	descriptor.identity.database_incarnation = 91;
	memcpy(descriptor.identity.storage_uuid, root_uuid, 16);
	descriptor.identity.locator = (RelFileLocator){ dbid == InvalidOid
		? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID, dbid, number };
	return descriptor;
}

static bool
census_until_committed(CensusFixture *f, Oid dbid)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; tick++) {
		ClusterRelmapCensusCommitted out;

		result = cluster_relmap_retention_census_next_committed(f->census, &out);
		if (out.present && out.identity.dbid == dbid)
			return true;
	}
	UT_ASSERT(false);
	return false;
}

UT_TEST(test_root_committed_rebuild_uses_exact_view_and_preserves_all_obligations)
{
	for (unsigned cut = 0; cut < 5; cut++) {
		CensusFixture f;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		unsigned seen = 0;

		census_fixture(&f, cut != 0);
		census_pair(f.fd, 5, 3, cut == 1 || cut == 4,
			cut == 2 || cut == 3 || cut == 4, cut == 3);
		if (census_observe_begin(&f)) {
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				tick++) {
				ClusterRelmapCensusCommitted out;
				ClusterRelmapPendingSet set;
				ClusterRelmapPendingDescriptor descriptors[2];
				size_t count;

				result = cluster_relmap_retention_census_next_committed(f.census, &out);
				if (!out.present)
					continue;
				descriptors[0] = census_descriptor(out.identity.dbid, CLUSTER_RELMAP_PENDING_DROP, 70000);
				descriptors[1] = census_descriptor(out.identity.dbid, CLUSTER_RELMAP_PENDING_TRUNCATE, 70001);
				/* An empty structural stream is independent of a clean relmap pair. */
				count = out.identity.shared && cut == 0 ? 0 : 2;
				cluster_relmap_pending_init(&set);
				UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
					&set, count == 0 ? NULL : descriptors, count), CLUSTER_RELMAP_PENDING_OK);
				for (size_t i = 0; i < count; i++) {
					ClusterSpaceIdentityKey alias = descriptors[i].identity;

					UT_ASSERT(cluster_relmap_pending_contains(&set, descriptors[i].kind,
						&descriptors[i].identity));
					alias.storage_uuid[0]++;
					UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&set, &alias));
					UT_ASSERT(set.entries[i].main_placeholder);
					UT_ASSERT(!set.entries[i].copies_cleared_readback);
					UT_ASSERT(!set.entries[i].native_terminal);
					UT_ASSERT(!set.entries[i].tt_complete);
					UT_ASSERT(!set.entries[i].space_complete);
					UT_ASSERT(!cluster_relmap_pending_can_release(&set, descriptors[i].kind,
						&descriptors[i].identity));
				}
				seen |= out.identity.shared ? 1 : 2;
				UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber,
					!out.identity.shared && cut == 3 ? 40001 : 40000);
			}
			UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(seen, 3);
			UT_ASSERT_EQ(census_pin(&f.origin[0], 5) != NULL, cut != 0);
			for (unsigned i = 0; i < 2; i++)
				UT_ASSERT_EQ(f.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_root_rebuild_window_is_current_emission_only_and_single_use)
{
	for (unsigned window = 0; window < 6; window++) {
		CensusFixture f;
		ClusterRelmapPendingSet set, before;
		ClusterRelmapPendingDescriptor descriptor = census_descriptor(5,
			CLUSTER_RELMAP_PENDING_DROP, 70000);

		census_fixture(&f, true);
		cluster_relmap_pending_init(&set);
		if (census_observe_begin(&f)) {
			if (window != 0)
				UT_ASSERT(census_until_committed(&f, 5));
			if (window == 1 || window == 2) {
				UT_ASSERT_EQ(window == 1 ? cluster_relmap_retention_census_next(f.census)
					: cluster_relmap_retention_census_next_committed(f.census, NULL),
					CLUSTER_RELMAP_CENSUS_MORE);
			} else if (window == 3) {
				UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
					&set, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
			} else if (window == 4)
				UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			else if (window == 5) {
				/* A non-emitting MORE call also expires the previous namespace. */
				ClusterRelmapCensusCommitted out;

				UT_ASSERT_EQ(cluster_relmap_retention_census_next_committed(f.census, &out),
					CLUSTER_RELMAP_CENSUS_MORE);
				UT_ASSERT(!out.present);
			}
			before = set;
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
				&set, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_root_rebuild_descriptor_refusal_is_atomic_and_poisoned_with_pins_retained)
{
	for (unsigned bad = 0; bad < 11; bad++) {
		CensusFixture f;
		ClusterRelmapPendingSet set, before;
		ClusterRelmapPendingDescriptor descriptors[2] = {
			census_descriptor(5, CLUSTER_RELMAP_PENDING_DROP, 70000),
			census_descriptor(5, CLUSTER_RELMAP_PENDING_TRUNCATE, 70001)
		};
		ClusterRelmapPendingResult expected = CLUSTER_RELMAP_PENDING_CONFLICT;

		census_fixture(&f, true);
		cluster_relmap_pending_init(&set);
		if (census_observe_begin(&f) && census_until_committed(&f, 5)) {
			switch (bad) {
				case 0: descriptors[1].identity.system_identifier++; break;
				case 1: descriptors[1].identity.database_incarnation++; break;
				case 2: descriptors[1].identity.locator.dbOid++; break;
				case 3: descriptors[1].identity.locator.spcOid = GLOBALTABLESPACE_OID; break;
				case 4: descriptors[1].kind = CLUSTER_RELMAP_PENDING_INVALID;
					expected = CLUSTER_RELMAP_PENDING_INVALID_INPUT; break;
				case 5: memset(descriptors[1].identity.storage_uuid, 0, 16);
					expected = CLUSTER_RELMAP_PENDING_INVALID_INPUT; break;
				case 6: descriptors[1].identity.locator = descriptors[0].identity.locator;
					descriptors[1].identity.storage_uuid[0]++; break;
				case 7: expected = CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED; break;
				case 8: UT_ASSERT_EQ(cluster_relmap_pending_add(&set, descriptors[0].kind,
					&descriptors[0].identity, true), CLUSTER_RELMAP_PENDING_OK); break;
				case 9: case 10: expected = CLUSTER_RELMAP_PENDING_INVALID_INPUT; break;
			}
			before = set;
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
				bad == 10 ? NULL : &set, bad == 9 ? NULL : descriptors,
				bad == 7 ? CLUSTER_RELMAP_PENDING_CAPACITY + 1 : 2), expected);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census), bad == 7
				? CLUSTER_RELMAP_CENSUS_CAPACITY : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_root_rebuild_rechecks_borrowed_qualification_before_consuming)
{
	for (unsigned change = 0; change < 3; change++) {
		CensusFixture f;
		ClusterRelmapPendingSet set, before;
		ClusterRelmapPendingDescriptor descriptor = census_descriptor(5,
			CLUSTER_RELMAP_PENDING_DROP, 70000);

		census_fixture(&f, true);
		cluster_relmap_pending_init(&set);
		before = set;
		if (census_observe_begin(&f) && census_until_committed(&f, 5)) {
			if (change == 0)
				cluster_recovery_root_set_invalidate(f.roots);
			else if (change == 1)
				f.origin[1].last_issued++;
			else
				cluster_relmap_retention_invalidate(&f.origin[0]);
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
				&set, &descriptor, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census),
				CLUSTER_RELMAP_CENSUS_CHANGED);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_root_rebuild_remains_provisional_until_whole_set_recheck)
{
	CensusFixture f;
	ClusterRelmapPendingSet set;
	ClusterRelmapPendingDescriptor descriptor = census_descriptor(5,
		CLUSTER_RELMAP_PENDING_DROP, 70000);

	census_fixture(&f, true);
	cluster_relmap_pending_init(&set);
	if (census_observe_begin(&f) && census_until_committed(&f, 5)) {
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
			&set, &descriptor, 1), CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT_EQ(unlinkat(f.fd, "global/pgrac_relmap_authority.bak", 0), 0);
		UT_ASSERT(census_drain(&f) > CLUSTER_RELMAP_CENSUS_DRAINED);
		UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
		census_refused(&f);
		/* Like a previously copied map, the cursor cannot revoke a caller's
		 * set. The owner discards it; no terminal or release proof was given. */
		UT_ASSERT(cluster_relmap_pending_contains(&set, descriptor.kind, &descriptor.identity));
		UT_ASSERT(!cluster_relmap_pending_can_release(&set, descriptor.kind, &descriptor.identity));
		cluster_relmap_pending_init(&set);
		UT_ASSERT(!cluster_relmap_pending_rel_number_reserved(&set, &descriptor.identity));
	}
	census_cleanup(&f);
}

UT_TEST(test_root_rebuild_null_cursor_preserves_destination)
{
	ClusterRelmapPendingSet set, before;

	cluster_relmap_pending_init(&set);
	before = set;
	UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(NULL, &set, NULL, 0),
		CLUSTER_RELMAP_PENDING_INVALID_INPUT);
	UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
}

UT_TEST(test_root_rebuild_preserves_existing_proofs_and_exact_pins)
{
	const unsigned proof_masks[] = { 0, 5, 15 };

	for (unsigned shared = 0; shared < 2; shared++) {
		for (unsigned p = 0; p < lengthof(proof_masks); p++) {
			for (unsigned count = 0; count <= 2; count += 2) {
				CensusFixture f;
				ClusterRelmapPendingSet set, before, empty;
				ClusterRelmapPendingDescriptor existing[2], incoming[2];
				ClusterRelmapRetentionDirectory origins_before[2];
				Oid dbid = shared ? InvalidOid : 5;
				unsigned proofs = proof_masks[p];

				census_fixture(&f, true);
				census_pair(f.fd, InvalidOid, 3, false, true, false);
				cluster_relmap_pending_init(&set);
				for (unsigned i = 0; i < lengthof(existing); i++) {
					ClusterSpaceIdentityKey *key = &existing[i].identity;

					existing[i] = census_descriptor(dbid, i == 0
						? CLUSTER_RELMAP_PENDING_DROP : CLUSTER_RELMAP_PENDING_TRUNCATE,
						71000 + i);
					UT_ASSERT_EQ(cluster_relmap_pending_add(&set, existing[i].kind,
						key, true), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 1)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_copies_cleared(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 2)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_native_terminal(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 4)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_tt_complete(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 8)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_space_complete(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					incoming[i] = existing[i];
					incoming[i].identity.locator.relNumber += 100;
				}
				memcpy(&before, &set, sizeof(before));
				if (census_observe_begin(&f) && census_until_committed(&f, dbid)) {
					UT_ASSERT(census_pin(&f.origin[0], dbid) != NULL);
					memcpy(origins_before, f.origin, sizeof(origins_before));
					UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
						&set, count == 0 ? NULL : incoming, count),
						CLUSTER_RELMAP_PENDING_CONFLICT);
					UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
					for (unsigned i = 0; i < lengthof(f.origin); i++)
						UT_ASSERT_EQ(memcmp(f.origin[i].pins, origins_before[i].pins,
							sizeof(f.origin[i].pins)), 0);
					for (unsigned i = 0; i < lengthof(existing); i++) {
						ClusterSpaceIdentityKey alias = existing[i].identity;

						alias.storage_uuid[0]++;
						UT_ASSERT(cluster_relmap_pending_contains(&set, existing[i].kind,
							&existing[i].identity));
						UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&set, &alias));
						UT_ASSERT_EQ(cluster_relmap_pending_can_release(&set, existing[i].kind,
							&existing[i].identity), proofs == 15);
					}
					UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census),
						CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
					census_refused(&f);
					/* A fresh destination cannot retry through the poisoned cursor. */
					cluster_relmap_pending_init(&empty);
					UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending(f.census,
						&empty, NULL, 0), CLUSTER_RELMAP_PENDING_NOT_READY);
					UT_ASSERT(!empty.has_namespace);
					UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
				}
				census_cleanup(&f);
			}
		}
	}
}

#ifndef RELMAP_CENSUS_FIXTURE_ONLY
#include "test_cluster_relmap_census_recovery.inc"

int
main(void)
{
	UT_PLAN(34);
	UT_RUN(test_census_recovery_prepares_only_after_whole_root_drained);
	UT_RUN(test_census_recovery_exact_namespace_and_partial_publish_base);
	UT_RUN(test_census_recovery_requires_exact_already_restored_pin);
	UT_RUN(test_census_recovery_native_refusal_and_error_retain_original_pins);
	UT_RUN(test_census_recovery_post_native_borrowed_drift_discards_local_handle);
	UT_RUN(test_census_recovery_changed_pair_and_absent_pending_refuse);
	UT_RUN(test_census_recovery_full_sha_rejects_valid_crc_read_substitution);
	UT_RUN(test_census_recovery_requires_original_wal_storage_scope);
	UT_RUN(test_all_physical_namespaces_and_original_backup_only_pending);
	UT_RUN(test_all_legal_pair_cuts_keep_pending_without_publishing_complete);
	UT_RUN(test_clean_census_stays_unpublished);
	UT_RUN(test_missing_copies_and_unreferenced_database_never_mean_empty);
	UT_RUN(test_global_and_base_roots_are_required);
	UT_RUN(test_degraded_foreign_short_and_conflicting_pairs_refuse);
	UT_RUN(test_unknown_origin_native_gone_and_fullxid_window_refuse);
	UT_RUN(test_later_failure_preserves_earlier_pending_and_is_sticky);
	UT_RUN(test_whole_root_recheck_rejects_new_namespace_inode_and_unrelated_change);
	UT_RUN(test_origin_qualification_cannot_change_during_scan);
	UT_RUN(test_interrupted_cursor_invalidates_without_losing_pending);
	UT_RUN(test_read_io_short_read_and_corruption_never_drain);
	UT_RUN(test_invalid_begin_poisoning_and_oversized_array_bound);
	UT_RUN(test_committed_reader_all_legal_cuts_and_original_pending_origin);
	UT_RUN(test_committed_reader_requires_pending_restore_before_emission);
	UT_RUN(test_committed_reader_emitted_maps_are_provisional_until_whole_recheck);
	UT_RUN(test_committed_reader_io_refusal_clears_output);
	UT_RUN(test_committed_reader_legacy_and_optional_output_share_one_cursor);
	UT_RUN(test_committed_reader_invalid_cursor_clears_output);
	UT_RUN(test_root_committed_rebuild_uses_exact_view_and_preserves_all_obligations);
	UT_RUN(test_root_rebuild_window_is_current_emission_only_and_single_use);
	UT_RUN(test_root_rebuild_descriptor_refusal_is_atomic_and_poisoned_with_pins_retained);
	UT_RUN(test_root_rebuild_rechecks_borrowed_qualification_before_consuming);
	UT_RUN(test_root_rebuild_remains_provisional_until_whole_set_recheck);
	UT_RUN(test_root_rebuild_null_cursor_preserves_destination);
	UT_RUN(test_root_rebuild_preserves_existing_proofs_and_exact_pins);
	UT_DONE();
	return ut_failed_count != 0;
}
#endif
