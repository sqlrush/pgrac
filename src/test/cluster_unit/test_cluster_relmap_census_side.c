/* Real ROOT/pair readers + original immutable SIDE feed/seal + pending core.
 * Native mutation/authority providers remain the existing SIDE fixture.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#define RELMAP_CENSUS_FIXTURE_ONLY
#include "test_cluster_relmap_retention_census.c"

/* Both original fixtures share one TAP state. Keep their assertion reporters
 * separate; neither changes a production proof or admission result. */
#undef UT_DEFINE_GLOBALS
#define UT_DEFINE_GLOBALS()
void side_fixture_ExceptionalCondition(const char *condition, const char *file, int line);
int side_xact_fixture_main(void);
#define ExceptionalCondition side_fixture_ExceptionalCondition
#define main side_xact_fixture_main
#include "test_cluster_side_xact.c"
#undef main
#undef ExceptionalCondition

typedef struct CensusSideFixture {
	ClusterRelmapPendingSideSource source;
	RfSideOnlinePlanV1 *plan;
} CensusSideFixture;

static void
census_side_plan(CensusSideFixture *f, XLogRecPtr end, bool history,
	RfSidePlanPurposeV1 purpose)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterWalThreadClaimRefV2 *claim;
	XLogRecPtr redo = end;

	memset(f, 0, sizeof(*f));
	claim = &f->source.source.claim;
	claim->identity.system_identifier = 817;
	memcpy(claim->identity.storage_uuid, root_uuid, 16);
	memset(claim->identity.authority_uuid, 0x55, 16);
	claim->identity.origin_thread_id = 3;
	claim->identity.origin_node_id = 2;
	claim->identity.origin_owner_incarnation = 33;
	claim->identity.root_lineage_seq = 1;
	claim->identity.thread_claim_created_at = 42;
	claim->database_incarnation = 91;
	claim->max_config_generation = 9;
	memset(claim->claim_sha256, 0x21, 32);
	f->source.source.timeline = 7;
	f->source.cut.failed_thread = 3;
	f->source.cut.timeline_id = 7;
	f->source.cut.origin_owner_incarnation = 33;
	f->source.cut.scan_begin_inclusive = 100;
	f->source.cut.scan_end_exclusive = end;
	f->source.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE |
		(end == 100 ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY : 0);
	request.system_identifier = 817;
	memcpy(request.storage_uuid, root_uuid, 16);
	request.physical_cuts = &f->source.cut;
	request.participant_count = 1;
	request.purpose = purpose;
	request.redo_starts = history ? &redo : NULL;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->plan, &f->source.source, 1));
	f->source.plan = f->plan;
}

static void
census_side_seal(CensusSideFixture *f)
{
	UT_ASSERT(rf_side_online_plan_bind_database_v1(f->plan, 91));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
}

static ClusterSpaceIdentityKey
census_side_key(RelFileNumber number, Oid dbid)
{
	ClusterSpaceIdentityKey key = { 0 };

	key.system_identifier = 817;
	key.database_incarnation = 91;
	memcpy(key.storage_uuid, root_uuid, 16);
	key.locator = (RelFileLocator){ dbid == InvalidOid
		? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID, dbid, number };
	return key;
}

static void
census_side_feed(CensusSideFixture *f, FakeXactRecord *fake, XLogRecPtr begin, bool xact)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memcpy(uuid, root_uuid, 16);
	identity = make_identity(fake, uuid);
	identity.record.system_identifier = fake->reader.system_identifier = 817;
	set_identity_range(fake, &identity, begin, begin + 100);
	record = xact ? make_record_plan(fake) : make_projection_record_plan(fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(f->plan, &record, &identity),
		RF_PAGE_PROOF_DETAIL_OK);
}

static void
census_side_truncate(CensusSideFixture *f, RelFileNumber number, Oid dbid, XLogRecPtr begin)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange change;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	FakeXactRecord fake;

	advance.result.identity.key = census_side_key(number, dbid);
	change = space_truncate_fixture(&advance.result, 80, 123, 70, 0x29);
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	make_projection_record(&fake, RM_SMGR_ID,
		XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE, wal, sizeof(wal));
	fake.u.decoded.max_block_id = -1;
	census_side_feed(f, &fake, begin, false);
}

static void
census_side_drops(CensusSideFixture *f, XLogRecPtr begin)
{
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	FakeXactRecord fake;

	for (unsigned i = 0; i < 2; i++) {
		ClusterSpaceIdentityKey key = census_side_key(16384 + i, 5);

		drops[i].identity.expected.key = drops[i].identity.result.key = key;
		drops[i].reservation.before.identity.key = drops[i].reservation.result.identity.key = key;
	}
	make_space_commit(&fake, drops, 2);
	census_side_feed(f, &fake, begin, true);
	memset(fake.data, 0xee, sizeof(fake.data));
}

static void
census_side_entry(const ClusterRelmapPendingSet *set, unsigned index,
	ClusterRelmapPendingKind kind, RelFileNumber number, Oid dbid)
{
	ClusterSpaceIdentityKey key = census_side_key(number, dbid), alias = key;

	UT_ASSERT_EQ(set->entries[index].kind, kind);
	UT_ASSERT(cluster_relmap_pending_contains(set, kind, &key));
	alias.storage_uuid[0]++;
	UT_ASSERT(cluster_relmap_pending_rel_number_reserved(set, &alias));
	UT_ASSERT(set->entries[index].main_placeholder);
	UT_ASSERT(!set->entries[index].copies_cleared_readback);
	UT_ASSERT(!set->entries[index].native_terminal);
	UT_ASSERT(!set->entries[index].tt_complete);
	UT_ASSERT(!set->entries[index].space_complete);
	UT_ASSERT(!cluster_relmap_pending_can_release(set, kind, &key));
}

UT_TEST(census_side_real_pair_history_and_every_commit_drop)
{
	for (unsigned cut = 0; cut < 3; cut++) {
		CensusFixture root;
		CensusSideFixture side;
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		unsigned seen = 0;

		reset_prepare_apply();
		census_fixture(&root, true);
		census_pair(root.fd, 5, 3, cut == 0, cut != 0, cut == 1);
		census_side_plan(&side, 400, true, RF_SIDE_PLAN_RECOVERY);
		census_side_truncate(&side, 16386, 5, 100);
		census_side_drops(&side, 200);
		census_side_truncate(&side, 16387, InvalidOid, 300);
		census_side_seal(&side);
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(side.plan), 0);
		if (census_observe_begin(&root)) {
			for (unsigned tick = 0; tick < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; tick++) {
				ClusterRelmapCensusCommitted out;
				ClusterRelmapPendingSet set, before;

				result = cluster_relmap_retention_census_next_committed(root.census, &out);
				if (!out.present)
					continue;
				cluster_relmap_pending_init(&set);
				UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side(root.census,
					&set, &side.source, 1), CLUSTER_RELMAP_PENDING_OK);
				if (out.identity.shared) {
					census_side_entry(&set, 0, CLUSTER_RELMAP_PENDING_TRUNCATE, 16387, InvalidOid);
					UT_ASSERT(!set.entries[1].occupied);
					seen |= 1;
				} else {
					census_side_entry(&set, 0, CLUSTER_RELMAP_PENDING_TRUNCATE, 16386, 5);
					census_side_entry(&set, 1, CLUSTER_RELMAP_PENDING_DROP, 16384, 5);
					census_side_entry(&set, 2, CLUSTER_RELMAP_PENDING_DROP, 16385, 5);
					UT_ASSERT(!set.entries[3].occupied);
					UT_ASSERT_EQ(out.map.mappings[0].mapfilenumber, cut == 1 ? 40001 : 40000);
					seen |= 2;
				}
				before = set;
				UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side(root.census,
					&set, &side.source, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
				UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			}
			UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(seen, 3);
			UT_ASSERT(census_pin(&root.origin[0], 5) != NULL);
			for (unsigned i = 0; i < 2; i++)
				UT_ASSERT_EQ(root.origin[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		rf_side_online_plan_destroy_v1(&side.plan);
		census_cleanup(&root);
	}
}

UT_TEST(census_side_missing_is_not_explicit_empty_and_refusal_preserves_pins)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		CensusFixture root;
		CensusSideFixture side;
		ClusterRelmapPendingSet set, before;

		census_fixture(&root, true);
		census_side_plan(&side, 100, false, RF_SIDE_PLAN_RECOVERY);
		census_side_seal(&side);
		cluster_relmap_pending_init(&set);
		before = set;
		if (census_observe_begin(&root) && census_until_committed(&root, 5)) {
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side(root.census,
				&set, empty ? &side.source : NULL, empty), empty
				? CLUSTER_RELMAP_PENDING_OK : CLUSTER_RELMAP_PENDING_NOT_READY);
			UT_ASSERT(!set.entries[0].occupied);
			UT_ASSERT(census_pin(&root.origin[0], 5) != NULL);
			UT_ASSERT_EQ(census_drain(&root), empty
				? CLUSTER_RELMAP_CENSUS_DRAINED : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			if (!empty) {
				UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
				census_refused(&root);
			}
		}
		rf_side_online_plan_destroy_v1(&side.plan);
		census_cleanup(&root);
	}
}

UT_TEST(census_side_rejects_original_source_drift_and_unsealed_plan_atomically)
{
	for (unsigned bad = 0; bad < 8; bad++) {
		CensusFixture root;
		CensusSideFixture side;
		ClusterRelmapPendingSet set, before;

		census_fixture(&root, true);
		census_side_plan(&side, 200, false, RF_SIDE_PLAN_RECOVERY);
		census_side_truncate(&side, 16386, 5, 100);
		if (bad != 0)
			census_side_seal(&side);
		cluster_relmap_pending_init(&set);
		before = set;
		if (census_observe_begin(&root) && census_until_committed(&root, 5)) {
			switch (bad) {
				case 1: side.source.source.claim.identity.system_identifier++; break;
				case 2: side.source.source.claim.database_incarnation++; break;
				case 3: side.source.source.claim.identity.storage_uuid[0]++; break;
				case 4: side.source.source.claim.claim_sha256[0]++; break;
				case 5: side.source.cut.scan_end_exclusive++; break;
				case 6: side.source.plan = NULL; break;
				case 7: side.source.source.timeline++; break;
			}
			UT_ASSERT_NE(cluster_relmap_retention_census_rebuild_pending_side(root.census,
				&set, &side.source, 1), CLUSTER_RELMAP_PENDING_OK);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			UT_ASSERT_EQ(census_drain(&root), CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			UT_ASSERT(census_pin(&root.origin[0], 5) != NULL);
			census_refused(&root);
		}
		rf_side_online_plan_destroy_v1(&side.plan);
		census_cleanup(&root);
	}
}

UT_TEST(census_side_window_expiry_and_borrowed_drift)
{
	for (unsigned change = 0; change < 5; change++) {
		CensusFixture root;
		CensusSideFixture side;
		ClusterRelmapPendingSet set, before;

		census_fixture(&root, true);
		census_side_plan(&side, 100, false, RF_SIDE_PLAN_RECOVERY);
		census_side_seal(&side);
		cluster_relmap_pending_init(&set);
		before = set;
		if (census_observe_begin(&root)) {
			if (change != 0)
				UT_ASSERT(census_until_committed(&root, 5));
			if (change == 1)
				UT_ASSERT_EQ(cluster_relmap_retention_census_next(root.census), CLUSTER_RELMAP_CENSUS_MORE);
			else if (change == 2)
				cluster_recovery_root_set_invalidate(root.roots);
			else if (change == 3)
				root.origin[1].last_issued++;
			else if (change == 4)
				cluster_relmap_retention_invalidate(&root.origin[0]);
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side(root.census,
				&set, &side.source, 1), CLUSTER_RELMAP_PENDING_NOT_READY);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			UT_ASSERT_EQ(census_drain(&root), change < 2
				? CLUSTER_RELMAP_CENSUS_DRAINED : CLUSTER_RELMAP_CENSUS_CHANGED);
			if (change >= 2) {
				UT_ASSERT(census_pin(&root.origin[0], 5) != NULL);
				census_refused(&root);
			}
		}
		rf_side_online_plan_destroy_v1(&side.plan);
		census_cleanup(&root);
	}
}

UT_TEST(census_side_late_root_change_cannot_adopt_previous_result)
{
	CensusFixture root;
	CensusSideFixture side;
	ClusterRelmapPendingSet set;

	census_fixture(&root, true);
	census_side_plan(&side, 200, false, RF_SIDE_PLAN_RECOVERY);
	census_side_truncate(&side, 16386, 5, 100);
	census_side_seal(&side);
	cluster_relmap_pending_init(&set);
	if (census_observe_begin(&root) && census_until_committed(&root, 5)) {
		UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side(root.census,
			&set, &side.source, 1), CLUSTER_RELMAP_PENDING_OK);
		root_file(root.fd, "unrelated", "different", 9);
		UT_ASSERT_EQ(census_drain(&root), CLUSTER_RELMAP_CENSUS_CHANGED);
		census_refused(&root);
		census_side_entry(&set, 0, CLUSTER_RELMAP_PENDING_TRUNCATE, 16386, 5);
		UT_ASSERT(census_pin(&root.origin[0], 5) != NULL);
		/* The original owner discards provisional copies on any later refusal. */
		cluster_relmap_pending_init(&set);
	}
	rf_side_online_plan_destroy_v1(&side.plan);
	census_cleanup(&root);
}

UT_TEST(census_side_invalid_destination_duplicate_and_capacity_keep_responsibility)
{
	for (unsigned bad = 0; bad < 6; bad++) {
		CensusFixture root;
		CensusSideFixture side;
		ClusterRelmapPendingSideSource sources[2];
		ClusterRelmapPendingSet set, before;
		ClusterSpaceIdentityKey key = census_side_key(70000, 5);
		ClusterRelmapPendingResult expected = CLUSTER_RELMAP_PENDING_INVALID_INPUT;
		size_t count = 1;

		census_fixture(&root, true);
		census_side_plan(&side, 100, false, RF_SIDE_PLAN_RECOVERY);
		census_side_seal(&side);
		sources[0] = sources[1] = side.source;
		cluster_relmap_pending_init(&set);
		if (bad == 1) {
			UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP,
				&key, true), CLUSTER_RELMAP_PENDING_OK);
			expected = CLUSTER_RELMAP_PENDING_CONFLICT;
		} else if (bad == 2) {
			count = 2;
			/* A one-participant plan supplied twice fails completeness first. */
			expected = CLUSTER_RELMAP_PENDING_NOT_READY;
		} else if (bad == 3) {
			count = RF_PAGE_STABLE_MAX_PARTICIPANTS + 1;
			expected = CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED;
		}
		before = set;
		if (census_observe_begin(&root) && census_until_committed(&root, 5)) {
			UT_ASSERT_EQ(cluster_relmap_retention_census_rebuild_pending_side(
				bad == 5 ? NULL : root.census, bad == 0 ? NULL : &set,
				bad == 4 ? NULL : sources, count), expected);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
			UT_ASSERT_EQ(census_drain(&root), bad == 5 ? CLUSTER_RELMAP_CENSUS_DRAINED
				: bad == 3 ? CLUSTER_RELMAP_CENSUS_CAPACITY : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			UT_ASSERT(census_pin(&root.origin[0], 5) != NULL);
			if (bad != 5)
				census_refused(&root);
		}
		rf_side_online_plan_destroy_v1(&side.plan);
		census_cleanup(&root);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(census_side_real_pair_history_and_every_commit_drop);
	UT_RUN(census_side_missing_is_not_explicit_empty_and_refusal_preserves_pins);
	UT_RUN(census_side_rejects_original_source_drift_and_unsealed_plan_atomically);
	UT_RUN(census_side_window_expiry_and_borrowed_drift);
	UT_RUN(census_side_late_root_change_cannot_adopt_previous_result);
	UT_RUN(census_side_invalid_destination_duplicate_and_capacity_keep_responsibility);
	UT_DONE();
	return ut_failed_count != 0;
}
