/* Real sealed SIDE inputs and committed relmap/pending core; native I/O and
 * authority remain the existing controlled fixture, never live admission. */
int side_xact_fixture_main(void);
#define main side_xact_fixture_main
#include "test_cluster_side_xact.c"
#undef main
#include "cluster/cluster_relmap_pending_side.h"

typedef struct PendingSideFixture
{
	ClusterRelmapPendingSideSource source;
	RfSideOnlinePlanV1 *plan;
} PendingSideFixture;

static const ClusterRelmapV2Namespace side_pending_namespace = {
	UINT64CONST(0x11223344), 42, 5, false
};

static void
side_pending_plan_origin(PendingSideFixture *f, XLogRecPtr end, bool history,
						 RfSidePlanPurposeV1 purpose, bool bind, uint64 owner_incarnation)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterWalThreadClaimRefV2 *claim;
	XLogRecPtr redo = end;

	memset(f, 0, sizeof(*f));
	claim = &f->source.source.claim;
	claim->identity.system_identifier = side_pending_namespace.system_identifier;
	memset(claim->identity.storage_uuid, 0x44, 16);
	memset(claim->identity.authority_uuid, 0x55, 16);
	claim->identity.origin_thread_id = 3;
	claim->identity.origin_node_id = 2;
	claim->identity.origin_owner_incarnation = owner_incarnation;
	claim->identity.root_lineage_seq = 1;
	claim->identity.thread_claim_created_at = 42;
	claim->database_incarnation = 42;
	claim->max_config_generation = 9;
	memset(claim->claim_sha256, 0x21, 32);
	f->source.source.timeline = 7;
	f->source.cut.failed_thread = 3;
	f->source.cut.timeline_id = 7;
	f->source.cut.origin_owner_incarnation = owner_incarnation;
	f->source.cut.scan_begin_inclusive = 100;
	f->source.cut.scan_end_exclusive = end;
	f->source.cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE |
		(end == 100 ? RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY : 0);
	request.system_identifier = claim->identity.system_identifier;
	memcpy(request.storage_uuid, claim->identity.storage_uuid, 16);
	request.physical_cuts = &f->source.cut;
	request.participant_count = 1;
	request.purpose = purpose;
	request.redo_starts = history ? &redo : NULL;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	if (bind)
		UT_ASSERT(rf_side_online_plan_bind_sources_v1(f->plan, &f->source.source, 1));
	f->source.plan = f->plan;
}

static void
side_pending_plan(PendingSideFixture *f, XLogRecPtr end, bool history,
				  RfSidePlanPurposeV1 purpose, bool bind)
{
	side_pending_plan_origin(f, end, history, purpose, bind, 33);
}

static void
side_pending_seal(PendingSideFixture *f)
{
	UT_ASSERT(rf_side_online_plan_bind_database_v1(f->plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
}

static ClusterRelmapPendingResult
side_pending_collect(const PendingSideFixture *f,
					 ClusterRelmapPendingDescriptor *out, size_t capacity, size_t *count)
{
	return cluster_relmap_pending_collect_side(&side_pending_namespace,
		f->source.source.claim.identity.storage_uuid, &f->source, 1, out, capacity, count);
}

static void
side_pending_truncate(PendingSideFixture *f, uint32 relnumber, XLogRecPtr begin)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange change;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	advance.result.identity.key.locator.relNumber = relnumber;
	change = space_truncate_fixture(&advance.result, 80, 123, 70, 0x29);
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(f->plan, XLOG_SMGR_SPACE_IDENTITY,
		wal, sizeof(wal), begin, false), RF_PAGE_PROOF_DETAIL_OK);
}

static pg_crc32c
side_pending_crc(const void *bytes, size_t length)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, length);
	FIN_CRC32C(crc);
	return crc;
}

static void
side_pending_pair(ClusterRelmapV2View *view)
{
	ClusterRelmapV2Image image = { 0 };
	uint8 primary[CLUSTER_RELMAP_V2_FILE_SIZE], backup[CLUSTER_RELMAP_V2_FILE_SIZE];

	image.identity = side_pending_namespace;
	image.committed_generation = 7;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0].mapoid = 1259;
	image.committed.mappings[0].mapfilenumber = 40000;
	image.committed.crc = side_pending_crc(&image.committed, offsetof(RelMapFile, crc));
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, primary, sizeof(primary)), CLUSTER_RELMAP_V2_VALID);
	memcpy(backup, primary, sizeof(primary));
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
		&side_pending_namespace, view), CLUSTER_RELMAP_V2_PAIR_VALID);
}

UT_TEST(side_pending_retains_history_and_every_commit_locator)
{
	PendingSideFixture f;
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	ClusterRelmapPendingDescriptor out[CLUSTER_RELMAP_PENDING_CAPACITY];
	ClusterRelmapPendingSet pending;
	ClusterRelmapV2View view;
	RfSideOnlineOperationV1 operation;
	FakeXactRecord fake;
	size_t count = 999;

	reset_prepare_apply();
	side_pending_plan(&f, 300, true, RF_SIDE_PLAN_RECOVERY, true);
	side_pending_truncate(&f, 16386, 100);
	make_space_commit(&fake, drops, lengthof(drops));
	UT_ASSERT_EQ(space_commit_feed(f.plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
	/* The adapter must consume the immutable owned payload, not this input. */
	memset(fake.data, 0xee, sizeof(fake.data));
	side_pending_seal(&f);
	UT_ASSERT(rf_side_online_plan_operation_v1(f.plan, 1, &operation));
	UT_ASSERT(operation.history_only);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(f.plan), 0);
	UT_ASSERT_EQ(side_pending_collect(&f, out, lengthof(out), &count), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT_EQ(out[0].kind, CLUSTER_RELMAP_PENDING_TRUNCATE);
	UT_ASSERT_EQ(out[0].identity.locator.relNumber, 16386);
	UT_ASSERT_EQ(out[1].kind, CLUSTER_RELMAP_PENDING_DROP);
	UT_ASSERT_EQ(out[2].identity.locator.relNumber, 16385);
	side_pending_pair(&view);
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(&pending, &side_pending_namespace,
		CLUSTER_RELMAP_V2_PAIR_VALID, &view, out, count), CLUSTER_RELMAP_PENDING_OK);
	for (size_t i = 0; i < count; i++)
	{
		UT_ASSERT(cluster_relmap_pending_contains(&pending, out[i].kind, &out[i].identity));
		UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&pending, &out[i].identity));
		UT_ASSERT(pending.entries[i].main_placeholder);
		UT_ASSERT(!pending.entries[i].copies_cleared_readback && !pending.entries[i].native_terminal &&
			!pending.entries[i].tt_complete && !pending.entries[i].space_complete);
		UT_ASSERT(!cluster_relmap_pending_can_release(&pending, out[i].kind, &out[i].identity));
	}
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	rf_side_online_plan_destroy_v1(&f.plan);
}

UT_TEST(side_pending_namespace_selection_and_redo_independence)
{
	for (int history = 0; history < 2; history++)
	{
		PendingSideFixture f;
		ClusterSpaceStructureChange drops[3] = { space_drop_fixture(16400),
			space_drop_fixture(16401), space_drop_fixture(16402) };
		ClusterRelmapV2Namespace ns = side_pending_namespace;
		ClusterRelmapPendingDescriptor out[3];
		FakeXactRecord fake;
		size_t count;

		/* Original typed DROP arrays are ordered by the physical locator. */
		drops[0].identity.expected.key.locator.dbOid = 6;
		drops[1].identity.expected.key.locator.spcOid = GLOBALTABLESPACE_OID;
		drops[1].identity.expected.key.locator.dbOid = InvalidOid;
		drops[2].identity.expected.key.locator.spcOid = 9000;
		for (size_t i = 0; i < lengthof(drops); i++)
		{
			drops[i].identity.result.key = drops[i].identity.expected.key;
			drops[i].reservation.before.identity.key = drops[i].identity.expected.key;
			drops[i].reservation.result.identity.key = drops[i].identity.expected.key;
		}
		side_pending_plan(&f, 200, history != 0, RF_SIDE_PLAN_RECOVERY, true);
		make_space_commit(&fake, drops, lengthof(drops));
		UT_ASSERT_EQ(space_commit_feed(f.plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
		side_pending_seal(&f);
		for (int selection = 0; selection < 3; selection++)
		{
			ns.dbid = drops[selection].identity.result.key.locator.dbOid;
			ns.shared = ns.dbid == InvalidOid;
			UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&ns,
				f.source.source.claim.identity.storage_uuid, &f.source, 1, out, lengthof(out), &count),
				CLUSTER_RELMAP_PENDING_OK);
			UT_ASSERT_EQ(count, 1);
			UT_ASSERT(RelFileLocatorEquals(out[0].identity.locator, drops[selection].identity.result.key.locator));
		}
		rf_side_online_plan_destroy_v1(&f.plan);
	}
}

UT_TEST(side_pending_refusal_preserves_outputs)
{
	PendingSideFixture f;
	ClusterRelmapPendingDescriptor out[2], before[2];
	ClusterRelmapPendingSideSource original;
	ClusterRelmapV2Namespace ns;
	size_t count;
	uint8 uuid[16];

	side_pending_plan(&f, 200, false, RF_SIDE_PLAN_RECOVERY, true);
	side_pending_truncate(&f, 16384, 100);
	memset(out, 0xa5, sizeof(out));
	memcpy(before, out, sizeof(out));
	count = 999;
	UT_ASSERT_EQ(side_pending_collect(&f, out, lengthof(out), &count), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(count, 999);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	side_pending_seal(&f);
	original = f.source;
	for (int fault = 0; fault < 12; fault++)
	{
		f.source = original;
		ns = side_pending_namespace;
		memcpy(uuid, original.source.claim.identity.storage_uuid, 16);
		switch (fault)
		{
			case 0: f.source.cut.scan_begin_inclusive++; break;
			case 1: f.source.cut.scan_end_exclusive++; break;
			case 2: f.source.cut.flags = 0; break;
			case 3: f.source.cut.origin_owner_incarnation++; break;
			case 4: f.source.source.claim.claim_sha256[0] ^= 1; break;
			case 5: f.source.source.claim.identity.authority_uuid[0] ^= 1; break;
			case 6: f.source.source.timeline++; break;
			case 7: f.source.source.claim.database_incarnation++; break;
			case 8: ns.database_incarnation++; break;
			case 9: ns.system_identifier++; break;
			case 10: uuid[0] ^= 1; break;
			case 11: f.source.plan = NULL; break;
		}
		count = 999;
		UT_ASSERT(cluster_relmap_pending_collect_side(&ns, uuid, &f.source, 1,
			out, lengthof(out), &count) != CLUSTER_RELMAP_PENDING_OK);
		UT_ASSERT_EQ(count, 999);
		UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	}
	f.source = original;
	count = 999;
	UT_ASSERT_EQ(side_pending_collect(&f, out, 0, &count), CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED);
	UT_ASSERT_EQ(count, 999);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	rf_side_online_plan_destroy_v1(&f.plan);
}

UT_TEST(side_pending_requires_original_bound_recovery_source)
{
	for (int fault = 0; fault < 2; fault++)
	{
		PendingSideFixture f;
		ClusterRelmapPendingDescriptor out, before;
		size_t count = 999;

		side_pending_plan(&f, 200, false,
			fault == 0 ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY, fault == 0);
		side_pending_truncate(&f, 16384, 100);
		side_pending_seal(&f);
		memset(&out, 0xa5, sizeof(out));
		memcpy(&before, &out, sizeof(out));
		UT_ASSERT_EQ(side_pending_collect(&f, &out, 1, &count), CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(count, 999);
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
		rf_side_online_plan_destroy_v1(&f.plan);
	}
}

UT_TEST(side_pending_duplicate_sources_and_locator_history_refuse)
{
	for (int duplicate_locator = 0; duplicate_locator < 2; duplicate_locator++)
	{
		PendingSideFixture f;
		ClusterRelmapPendingSideSource sources[2];
		ClusterRelmapPendingDescriptor out[2], before[2];
		size_t count = 999;

		side_pending_plan(&f, duplicate_locator ? 300 : 200, true, RF_SIDE_PLAN_RECOVERY, true);
		side_pending_truncate(&f, 16384, 100);
		if (duplicate_locator)
			side_pending_truncate(&f, 16384, 200);
		side_pending_seal(&f);
		sources[0] = sources[1] = f.source;
		memset(out, 0xa5, sizeof(out));
		memcpy(before, out, sizeof(out));
		UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&side_pending_namespace,
			f.source.source.claim.identity.storage_uuid, sources, duplicate_locator ? 1 : 2,
			out, lengthof(out), &count), duplicate_locator ? CLUSTER_RELMAP_PENDING_CONFLICT :
			CLUSTER_RELMAP_PENDING_NOT_READY);
		UT_ASSERT_EQ(count, 999);
		UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
		rf_side_online_plan_destroy_v1(&f.plan);
	}
}

UT_TEST(side_pending_is_bounded_without_partial_output)
{
	PendingSideFixture f;
	ClusterRelmapPendingDescriptor out[CLUSTER_RELMAP_PENDING_CAPACITY], before[lengthof(out)];
	size_t count = 999;

	side_pending_plan(&f, 100 * (CLUSTER_RELMAP_PENDING_CAPACITY + 2), true, RF_SIDE_PLAN_RECOVERY, true);
	for (uint32 i = 0; i <= CLUSTER_RELMAP_PENDING_CAPACITY; i++)
		side_pending_truncate(&f, 17000 + i, 100 * (i + 1));
	side_pending_seal(&f);
	memset(out, 0xa5, sizeof(out));
	memcpy(before, out, sizeof(out));
	UT_ASSERT_EQ(side_pending_collect(&f, out, lengthof(out), &count), CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED);
	UT_ASSERT_EQ(count, 999);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	rf_side_online_plan_destroy_v1(&f.plan);
}

UT_TEST(side_pending_explicit_empty_is_not_missing_source)
{
	PendingSideFixture f;
	size_t count = 999;

	side_pending_plan(&f, 100, false, RF_SIDE_PLAN_RECOVERY, true);
	side_pending_seal(&f);
	UT_ASSERT_EQ(side_pending_collect(&f, NULL, 0, &count), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(count, 0);
	count = 999;
	UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&side_pending_namespace,
		f.source.source.claim.identity.storage_uuid, NULL, 0, NULL, 0, &count), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(count, 999);
	rf_side_online_plan_destroy_v1(&f.plan);
}

UT_TEST(side_pending_relation_bearing_abort_is_not_silently_empty)
{
	PendingSideFixture f;
	FakeXactRecord fake;
	RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, 16384 };
	ClusterRelmapPendingDescriptor out, before;
	size_t count = 999;

	side_pending_plan(&f, 400, true, RF_SIDE_PLAN_RECOVERY, true);
	/* The original seal requires the exact native/typed CREATE pair. */
	for (uint32 i = 0; i < 2; i++)
	{
		cold_abort_create_record(&fake, locator, i == 0, 802);
		UT_ASSERT_EQ(cold_abort_delete_feed(f.plan, &fake, (i + 1) * 100, 0), RF_PAGE_PROOF_DETAIL_OK);
	}
	cold_abort_deletes(&fake, &locator, 1, false);
	UT_ASSERT_EQ(space_commit_feed(f.plan, &fake, 300), RF_PAGE_PROOF_DETAIL_OK);
	side_pending_seal(&f);
	memset(&out, 0xa5, sizeof(out));
	memcpy(&before, &out, sizeof(out));
	UT_ASSERT_EQ(side_pending_collect(&f, &out, 1, &count), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(count, 999);
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	rf_side_online_plan_destroy_v1(&f.plan);
}

UT_TEST(side_pending_requires_all_participants_and_enumerates_plan_once)
{
	PendingSideFixture f;
	ClusterRelmapPendingSideSource sources[2];
	RfContributorStreamCutV1 cuts[2];
	ClusterWalSourceRef claims[2];
	RfSideOnlinePlanRequestV1 request = { 0 };
	XLogRecPtr redo[2] = { 200, 100 };
	ClusterRelmapPendingDescriptor out[2], before[2];
	size_t count = 999;

	side_pending_plan(&f, 200, true, RF_SIDE_PLAN_RECOVERY, true);
	rf_side_online_plan_destroy_v1(&f.plan);
	claims[0] = claims[1] = f.source.source;
	claims[1].claim.identity.origin_thread_id = 4;
	claims[1].claim.identity.origin_node_id = 3;
	claims[1].claim.identity.origin_owner_incarnation = 34;
	cuts[0] = cuts[1] = f.source.cut;
	cuts[1].failed_thread = 4;
	cuts[1].origin_owner_incarnation = 34;
	cuts[1].scan_end_exclusive = 100;
	cuts[1].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	request.system_identifier = side_pending_namespace.system_identifier;
	memcpy(request.storage_uuid, claims[0].claim.identity.storage_uuid, 16);
	request.physical_cuts = cuts;
	request.participant_count = 2;
	request.purpose = RF_SIDE_PLAN_RECOVERY;
	request.redo_starts = redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &f.plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_sources_v1(f.plan, claims, lengthof(claims)));
	side_pending_truncate(&f, 16384, 100);
	side_pending_seal(&f);
	for (size_t i = 0; i < lengthof(sources); i++)
	{
		sources[i].plan = f.plan;
		sources[i].source = claims[i];
		sources[i].cut = cuts[i];
	}
	memset(out, 0xa5, sizeof(out));
	memcpy(before, out, sizeof(out));
	/* The empty physical participant is still required. */
	UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&side_pending_namespace,
		request.storage_uuid, sources, 1, out, lengthof(out), &count), CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT_EQ(count, 999);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	sources[1] = sources[0];
	UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&side_pending_namespace,
		request.storage_uuid, sources, 2, out, lengthof(out), &count), CLUSTER_RELMAP_PENDING_CONFLICT);
	UT_ASSERT_EQ(count, 999);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	sources[1].source = claims[1];
	sources[1].cut = cuts[1];
	UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&side_pending_namespace,
		request.storage_uuid, sources, 2, out, lengthof(out), &count), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(out[0].identity.locator.relNumber, 16384);
	rf_side_online_plan_destroy_v1(&f.plan);
}

UT_TEST(side_pending_retained_generations_keep_cross_plan_locator_conflicts)
{
	for (int duplicate = 0; duplicate < 2; duplicate++)
	{
		PendingSideFixture f[2];
		ClusterRelmapPendingSideSource sources[2];
		ClusterRelmapPendingDescriptor out[2], before[2];
		size_t count = 999;

		for (size_t i = 0; i < lengthof(f); i++)
		{
			side_pending_plan_origin(&f[i], 200, true, RF_SIDE_PLAN_RECOVERY, true, 33 + i);
			side_pending_truncate(&f[i], 16384 + (duplicate ? 0 : i), 100);
			side_pending_seal(&f[i]);
			sources[i] = f[i].source;
		}
		memset(out, 0xa5, sizeof(out));
		memcpy(before, out, sizeof(out));
		UT_ASSERT_EQ(cluster_relmap_pending_collect_side(&side_pending_namespace,
			f[0].source.source.claim.identity.storage_uuid, sources, 2, out, lengthof(out), &count),
			duplicate ? CLUSTER_RELMAP_PENDING_CONFLICT : CLUSTER_RELMAP_PENDING_OK);
		if (duplicate)
		{
			UT_ASSERT_EQ(count, 999);
			UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
		}
		else
		{
			UT_ASSERT_EQ(count, 2);
			UT_ASSERT_EQ(out[0].identity.locator.relNumber, 16384);
			UT_ASSERT_EQ(out[1].identity.locator.relNumber, 16385);
		}
		for (size_t i = 0; i < lengthof(f); i++)
			rf_side_online_plan_destroy_v1(&f[i].plan);
	}
}

UT_TEST(side_pending_create_and_advance_are_not_deletion_descriptors)
{
	PendingSideFixture f;
	FakeXactRecord fake;
	ClusterSpaceReservationChange advance = space_advance_fixture();
	RelFileLocator locator = advance.before.identity.key.locator;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	size_t count = 999;

	side_pending_plan(&f, 300, true, RF_SIDE_PLAN_RECOVERY, true);
	cold_abort_create_record(&fake, locator, false, 802);
	UT_ASSERT_EQ(cold_abort_delete_feed(f.plan, &fake, 100, 0), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(f.plan, XLOG_SMGR_SPACE_RESERVATION,
		wal, sizeof(wal), 200, false), RF_PAGE_PROOF_DETAIL_OK);
	side_pending_seal(&f);
	UT_ASSERT_EQ(side_pending_collect(&f, NULL, 0, &count), CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(count, 0);
	rf_side_online_plan_destroy_v1(&f.plan);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(side_pending_retains_history_and_every_commit_locator);
	UT_RUN(side_pending_namespace_selection_and_redo_independence);
	UT_RUN(side_pending_refusal_preserves_outputs);
	UT_RUN(side_pending_requires_original_bound_recovery_source);
	UT_RUN(side_pending_duplicate_sources_and_locator_history_refuse);
	UT_RUN(side_pending_is_bounded_without_partial_output);
	UT_RUN(side_pending_explicit_empty_is_not_missing_source);
	UT_RUN(side_pending_relation_bearing_abort_is_not_silently_empty);
	UT_RUN(side_pending_requires_all_participants_and_enumerates_plan_once);
	UT_RUN(side_pending_retained_generations_keep_cross_plan_locator_conflicts);
	UT_RUN(side_pending_create_and_advance_are_not_deletion_descriptors);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
