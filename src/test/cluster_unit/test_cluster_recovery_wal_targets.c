/* Real WAL route/native/SPACE parsing into the two-pass physical root set.
 * No page mutation, admission or SIDE completion is mocked successful.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define RECOVERY_NATIVE_ROOTS_FIXTURE_ONLY
#include "test_cluster_recovery_native_roots.c"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_uba.h"
#include "../../backend/cluster/cluster_recovery_wal_targets.c"

/* A target census must never invoke a page's mutation owner. */
ClusterBlkApplyResult
cluster_block_apply_one(XLogReaderState *reader, uint8 block_id, char *page)
{
	abort();
}

static union {
	DecodedXLogRecord decoded;
	char storage[sizeof(DecodedXLogRecord) + 3 * sizeof(DecodedBkpBlock)];
} wal_record_storage;
static union {
	uint64 align;
	char bytes[2 * BLCKSZ];
} wal_payload;
static char wal_image[BLCKSZ];

static void
wal_fixture(uint8 rmid, uint8 info, uint32 length, int blocks)
{
	native_fixture(rmid, info, length);
	memset(&wal_record_storage, 0, sizeof(wal_record_storage));
	memset(&wal_payload, 0, sizeof(wal_payload));
	wal_record_storage.decoded = native_decoded;
	wal_record_storage.decoded.main_data = wal_payload.bytes;
	wal_record_storage.decoded.max_block_id = blocks - 1;
	native_reader.record = &wal_record_storage.decoded;
}

static ClusterSpaceStructureChange
wal_structure(ClusterSpaceWalAction action)
{
	ClusterSpaceStructureChange c = { 0 };
	ClusterSpaceIdentity *identity = &c.identity.result;

	identity->key.system_identifier = 817;
	identity->key.database_incarnation = 91;
	memcpy(identity->key.storage_uuid, root_uuid, 16);
	identity->key.locator = (RelFileLocator){ 1663, 5, 16385 };
	memset(identity->incarnation, 0x31, 16);
	identity->sequence = 1;
	identity->operation = 7;
	identity->state = CLUSTER_SPACE_IDENTITY_LIVE;
	c.identity.action = action;
	c.identity.nblocks = InvalidBlockNumber;
	c.identity.result_token = c.reservation.result_token = 211;
	c.reservation.result.identity = *identity;
	if (action == CLUSTER_SPACE_WAL_CREATE) {
		c.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
		return c;
	}
	c.identity.expected = *identity;
	c.identity.before_token = 17;
	c.reservation.before.identity = *identity;
	c.reservation.before.next_block = 10;
	c.reservation.before_token = 99;
	identity->sequence++;
	identity->operation++;
	if (action == CLUSTER_SPACE_WAL_TRUNCATE) {
		c.identity.nblocks = 4;
		identity->incarnation[0]++;
		c.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
		c.reservation.first_block = c.reservation.result.next_block = 4;
	} else {
		identity->state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		c.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
		c.reservation.result.next_block = 10;
	}
	c.reservation.result.identity = *identity;
	return c;
}

static bool
collect(ClusterRecoveryRootSet *roots, ClusterRecoveryWalTargetsDiag *diag)
{
	return cluster_recovery_wal_targets_collect(roots, &native_reader, &native_source, &native_cut,
												65536, diag);
}

static uint32
wal_forks(ClusterRecoveryRootSet *roots, RelFileNumber number, uint64 space_blocks)
{
	uint32 mask = 0;

	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		UT_ASSERT(entry != NULL);
		if (entry == NULL)
			break;
		if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELATION
			|| entry->file.locator.relNumber != number)
			continue;
		mask |= 1U << entry->file.forknum;
		UT_ASSERT(entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL);
		UT_ASSERT_EQ(entry->required_blocks,
					 entry->file.forknum == SPACE_FORKNUM ? space_blocks : 0);
	}
	return mask;
}

UT_TEST(test_page_and_rebuildable_fsm_keep_all_file_targets)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryWalTargetsDiag diag;
	const ForkNumber forks[] = { MAIN_FORKNUM, VISIBILITYMAP_FORKNUM, FSM_FORKNUM };
	unsigned count = 0, untouched = 0;

	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	root_file(fd, "base/5/999", "untouched", 9);
	roots = begin_roots(fd, 1024 * 1024);
	wal_fixture(RM_GENERIC_ID, 0, 0, 3);
	wal_record_storage.decoded.has_page_version_edge = true;
	wal_record_storage.decoded.page_version_edge.entry_count = 3;
	wal_record_storage.decoded.page_version_edge.result_token = 211;
	for (unsigned i = 0; i < lengthof(forks); i++) {
		DecodedBkpBlock *block = &wal_record_storage.decoded.blocks[i];
		RfPageVersionEdgeEntryV1 *edge = &wal_record_storage.decoded.page_version_edge.entries[i];

		block->in_use = true;
		block->rlocator = (RelFileLocator){ 1663, 5, 16385 };
		block->forknum = forks[i];
		block->blkno = 10 + i;
		block->component_ordinal = i;
		edge->block_id = edge->component_ordinal = i;
		if (forks[i] == FSM_FORKNUM) {
			edge->page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
			edge->before_kind = edge->result_kind = RF_PAGE_STATE_REBUILDABLE;
		} else {
			edge->page_class = RF_PAGE_CLASS_ORDINARY;
			edge->before_kind = edge->result_kind = RF_PAGE_STATE_PRESENT;
			memset(edge->before.segment_incarnation, 7, 16);
			memset(edge->result_incarnation, 7, 16);
			edge->before.mutation_token = 17;
			edge->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
			block->has_image = block->apply_image = true;
			block->bimg_len = BLCKSZ;
			block->bkp_image = wal_image;
		}
	}
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.page_targets, 3);
	UT_ASSERT_EQ(diag.rebuildable_targets, 1);
	UT_ASSERT_EQ(diag.unowned_records, 0);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		UT_ASSERT(entry != NULL);
		if (entry == NULL)
			break;
		if (entry->file.locator.relNumber == 16385) {
			count++;
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->required_blocks, entry->file.forknum == MAIN_FORKNUM ? 11
												 : entry->file.forknum == VISIBILITYMAP_FORKNUM
													 ? 12
													 : 13);
		}
		if (entry->file.locator.relNumber == 999)
			untouched++;
	}
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT_EQ(untouched, 1);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_typed_structure_keeps_all_forks_and_space_pages)
{
	int fd = new_root();
	const uint32 all_forks = (1U << MAIN_FORKNUM) | (1U << FSM_FORKNUM)
							 | (1U << VISIBILITYMAP_FORKNUM) | (1U << SPACE_FORKNUM);

	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	root_file(fd, "base/5/16385", "", 0);
	for (unsigned action = CLUSTER_SPACE_WAL_CREATE; action <= CLUSTER_SPACE_WAL_TOMBSTONE;
		 action++) {
		ClusterSpaceStructureChange change = wal_structure(action);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		size_t offset = 0;

		if (action == CLUSTER_SPACE_WAL_TOMBSTONE) {
			TimestampTz time = 42;
			uint32 xinfo = XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_SPACE_DROP;
			uint32 one = 1;

			wal_fixture(RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO,
						MinSizeOfXactCommit + 4 + 4 + sizeof(RelFileLocator) + 4
							+ CLUSTER_SPACE_STRUCTURE_WAL_BYTES,
						0);
#define PUT(value_)                                                                                \
	do {                                                                                           \
		memcpy(wal_payload.bytes + offset, &(value_), sizeof(value_));                             \
		offset += sizeof(value_);                                                                  \
	} while (0)
			PUT(time);
			PUT(xinfo);
			PUT(one);
			PUT(change.identity.result.key.locator);
			PUT(one);
#undef PUT
		} else
			wal_fixture(RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, CLUSTER_SPACE_STRUCTURE_WAL_BYTES, 0);
		UT_ASSERT(cluster_space_structure_wal_encode(&change, wal_payload.bytes + offset,
													 CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(diag.structural_effects, action == CLUSTER_SPACE_WAL_TOMBSTONE ? 2 : 1);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(wal_forks(roots, 16385, 2), all_forks);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_native_create_and_truncate_do_not_invent_page_requirements)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	xl_smgr_create create = { { 1664, 0, 1260 }, MAIN_FORKNUM };
	xl_smgr_truncate truncate = { 100, { 1663, 5, 16385 }, SMGR_TRUNCATE_HEAP | SMGR_TRUNCATE_FSM };

	wal_fixture(RM_SMGR_ID, XLOG_SMGR_CREATE, sizeof(create), 0);
	memcpy(wal_payload.bytes, &create, sizeof(create));
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.native_effects, 1);
	wal_fixture(RM_SMGR_ID, XLOG_SMGR_TRUNCATE, sizeof(truncate), 0);
	memcpy(wal_payload.bytes, &truncate, sizeof(truncate));
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.structural_effects, 1);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(wal_forks(roots, 1260, 0), 1U << MAIN_FORKNUM);
	UT_ASSERT_EQ(wal_forks(roots, 16385, 0),
				 (1U << MAIN_FORKNUM) | (1U << FSM_FORKNUM) | (1U << SPACE_FORKNUM));
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_non_file_effects_are_explicit_and_not_completion)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryWalTargetsDiag diag;
	TimestampTz time = 42;
	uint32 xinfo = XACT_COMPLETION_FORCE_SYNC_COMMIT | XACT_COMPLETION_UPDATE_RELCACHE_FILE;

	wal_fixture(RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO,
				MinSizeOfXactCommit + sizeof(xinfo), 0);
	memcpy(wal_payload.bytes, &time, sizeof(time));
	memcpy(wal_payload.bytes + sizeof(time), &xinfo, sizeof(xinfo));
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.native_effects, 1);
	UT_ASSERT_EQ(diag.other_native_effects, 1);
	UT_ASSERT_EQ(diag.page_targets + diag.structural_effects, 0);
	wal_fixture(RM_XLOG_ID, XLOG_NOOP, 0, 0);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.unowned_records, 1);
	UT_ASSERT_EQ(diag.page_targets + diag.structural_effects, 0);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_bad_scope_shape_or_capacity_invalidates_partial_roots)
{
	int fd = new_root();
	uint8 digest[32];

	for (unsigned variant = 0; variant < 12; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		xl_smgr_create create = { { 1663, 5, 16385 }, MAIN_FORKNUM };
		size_t budget = 65536;

		wal_fixture(RM_SMGR_ID, XLOG_SMGR_CREATE, sizeof(create), 0);
		memcpy(wal_payload.bytes, &create, sizeof(create));
		UT_ASSERT(collect(roots, &diag));
		switch (variant) {
		case 0:
			native_cut.scan_end_exclusive--;
			break;
		case 1:
			native_cut.origin_owner_incarnation++;
			break;
		case 2:
			native_source.claim.database_incarnation++;
			break;
		case 3:
			native_source.claim.identity.storage_uuid[0]++;
			break;
		case 4:
			wal_record_storage.decoded.main_data_len--;
			break;
		case 5:
			wal_record_storage.decoded.header.xl_info = 0xf0;
			break;
		case 6:
			create.rlocator.dbOid = 0;
			memcpy(wal_payload.bytes, &create, sizeof(create));
			break;
		case 7:
			budget = 1;
			break;
		case 8:
			wal_record_storage.decoded.header.xl_rmid = RM_XACT_ID;
			wal_record_storage.decoded.header.xl_info = XLOG_XACT_PREPARE;
			break;
		case 9:
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			break;
		case 10:
			native_reader.record->next_lsn++;
			break;
		case 11:
			native_cut.flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
			break;
		}
		UT_ASSERT(!cluster_recovery_wal_targets_collect(roots, &native_reader, &native_source,
														&native_cut, budget, &diag));
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_typed_namespace_and_payload_fail_closed)
{
	int fd = new_root();
	uint8 digest[32];

	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		ClusterSpaceStructureChange change = wal_structure(CLUSTER_SPACE_WAL_CREATE);

		wal_fixture(RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, CLUSTER_SPACE_STRUCTURE_WAL_BYTES, 0);
		if (variant == 0)
			change.identity.result.key.database_incarnation++;
		if (variant == 1)
			change.identity.result.key.storage_uuid[0]++;
		change.reservation.result.identity = change.identity.result;
		UT_ASSERT(cluster_space_structure_wal_encode(&change, wal_payload.bytes,
													 CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
		if (variant == 2)
			wal_record_storage.decoded.main_data_len--;
		if (variant == 3)
			wal_payload.bytes[0] ^= 0x80;
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_advance_is_not_a_promise_that_reserved_main_blocks_exist)
{
	int fd = new_root();
	ClusterSpaceReservationChange change = { 0 };
	const uint32 all_forks = (1U << MAIN_FORKNUM) | (1U << FSM_FORKNUM)
							 | (1U << VISIBILITYMAP_FORKNUM) | (1U << SPACE_FORKNUM);

	change.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	change.before = wal_structure(CLUSTER_SPACE_WAL_CREATE).reservation.result;
	change.before.next_block = 4;
	change.result = change.before;
	change.result.next_block = 9;
	change.first_block = 4;
	change.granted = 5;
	change.before_token = 17;
	change.result_token = 211;
	for (unsigned variant = 0; variant < 2; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		ClusterSpaceReservationChange input = change;

		if (variant == 1)
			input = wal_structure(CLUSTER_SPACE_WAL_CREATE).reservation;
		wal_fixture(RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, CLUSTER_SPACE_RESERVATION_WAL_BYTES,
					0);
		UT_ASSERT(cluster_space_reservation_wal_encode(&input, wal_payload.bytes,
													   CLUSTER_SPACE_RESERVATION_WAL_BYTES));
		UT_ASSERT_EQ(collect(roots, &diag), variant == 0);
		if (variant == 0) {
			UT_ASSERT_EQ(diag.structural_effects, 1);
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			UT_ASSERT_EQ(wal_forks(roots, 16385, 2), all_forks);
		} else
			UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_bad_edge_mixed_side_and_unclassified_opcode_do_not_finish_roots)
{
	int fd = new_root();

	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		DecodedBkpBlock *block;
		RfPageVersionEdgeEntryV1 *edge;

		wal_fixture(RM_GENERIC_ID, 0, 0, 1);
		wal_record_storage.decoded.has_page_version_edge = true;
		wal_record_storage.decoded.page_version_edge.entry_count = 1;
		wal_record_storage.decoded.page_version_edge.result_token = 211;
		block = &wal_record_storage.decoded.blocks[0];
		block->in_use = true;
		block->rlocator = (RelFileLocator){ 1663, 5, 16385 };
		block->forknum = MAIN_FORKNUM;
		block->has_image = block->apply_image = true;
		block->bimg_len = BLCKSZ;
		block->bkp_image = wal_image;
		edge = &wal_record_storage.decoded.page_version_edge.entries[0];
		edge->page_class = RF_PAGE_CLASS_ORDINARY;
		edge->before_kind = edge->result_kind = RF_PAGE_STATE_PRESENT;
		memset(edge->before.segment_incarnation, 7, 16);
		memset(edge->result_incarnation, 7, 16);
		edge->before.mutation_token = 17;
		edge->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
		if (variant == 0)
			wal_record_storage.decoded.has_page_version_edge = false;
		if (variant == 1) {
			edge->page_class = RF_PAGE_CLASS_ROUTED_SPACE;
			block->forknum = SPACE_FORKNUM;
		}
		if (variant == 2)
			wal_record_storage.decoded.header.xl_info = 0xf0;
		if (variant == 3)
			block->blkno = InvalidBlockNumber;
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

#include "test_cluster_recovery_undo_targets.inc"
#include "test_cluster_recovery_multixact_targets.inc"
#include "test_cluster_recovery_multixact_physical.inc"

int
main(void)
{
	UT_PLAN(29);
	UT_RUN(test_page_and_rebuildable_fsm_keep_all_file_targets);
	UT_RUN(test_typed_structure_keeps_all_forks_and_space_pages);
	UT_RUN(test_native_create_and_truncate_do_not_invent_page_requirements);
	UT_RUN(test_non_file_effects_are_explicit_and_not_completion);
	UT_RUN(test_bad_scope_shape_or_capacity_invalidates_partial_roots);
	UT_RUN(test_typed_namespace_and_payload_fail_closed);
	UT_RUN(test_advance_is_not_a_promise_that_reserved_main_blocks_exist);
	UT_RUN(test_bad_edge_mixed_side_and_unclassified_opcode_do_not_finish_roots);
	UT_RUN(test_undo_wal_all_shapes_preserve_segment_targets);
	UT_RUN(test_undo_wal_union_keeps_missing_and_largest_requirement);
	UT_RUN(test_folded_tt_commit_keeps_undo_target_and_terminal_obligation);
	UT_RUN(test_undo_wal_malformed_targets_poison_whole_roots);
	UT_RUN(test_undo_targets_reject_block_and_namespace_boundaries);
	UT_RUN(test_folded_tt_commit_rejects_wrong_identity_and_shape);
	UT_RUN(test_origin_slru_names_do_not_adopt_local_state);
	UT_RUN(test_mx_create_keeps_next_sentinel_and_every_member_segment);
	UT_RUN(test_mx_wrap_preserves_both_ends_without_native_local_substitution);
	UT_RUN(test_mx_root_union_keeps_short_missing_untouched_and_local_files);
	UT_RUN(test_mx_zero_page_and_truncate_preserve_distinct_obligations);
	UT_RUN(test_mx_target_failures_revoke_the_whole_union);
	UT_RUN(test_mx_target_capacity_never_exports_a_partial_union);
	UT_RUN(test_mx_physical_census_retains_all_slots_and_origin_boundaries);
	UT_RUN(test_mx_physical_uint32_tail_does_not_become_member_zero);
	UT_RUN(test_mx_physical_empty_files_remain_explicit_candidates);
	UT_RUN(test_mx_physical_missing_short_and_bad_geometry_do_not_drain);
	UT_RUN(test_mx_physical_reads_and_rereads_refuse_all_io_failures);
	UT_RUN(test_mx_physical_mutation_and_final_recheck_revoke_observations);
	UT_RUN(test_mx_physical_budget_and_invalid_arguments_fail_closed);
	UT_RUN(test_mx_physical_metadata_budget_preserves_untouched_files);
	UT_DONE();
	return ut_failed_count != 0;
}
