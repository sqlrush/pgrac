/*-------------------------------------------------------------------------
 * test_cluster_ctrc_successor_fence.c
 *    Original cleaner successor publication across an object fence.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Real successor construction, PREPARE/APPLY and finite cleaner scope.
 * Descriptor allocation, active TT proof, KO registry and WAL are explicit
 * boundary fixtures. This does not prove a physical page rewrite or fabric.
 *-------------------------------------------------------------------------
 */
int ctrc_cleaner_original_main(void);
#define main ctrc_cleaner_original_main
#include "test_cluster_ctrc_cleaner.c"
#undef main

static ClusterCtrcOriginEntry *survivor_origin;
static unsigned descriptor_creates;
static bool fail_survivor_proof;

MultiXactId
MultiXactIdCreateLocalCurrentMembers(int count, MultiXactMember *members)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(members[0].xid, survivor_origin->key.xid);
	descriptor_creates++;
	return 1200;
}

XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return UINT64_C(0xa100);
}

int
cluster_mxid_origin_slot(MultiXactId mxid)
{
	return mxid == 1200 ? 0 : -1;
}

uint64
cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members, uint16 count)
{
	UT_ASSERT_EQ(key->multixact_id, 1200);
	UT_ASSERT_EQ(key->origin_node_id, 0);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(members[0].xid, survivor_origin->key.xid);
	return 99;
}

bool
cluster_runtime_visibility_active_proof_ctrc_identity_exact(
	const ClusterCurrentMemberProofKey *proof_key, uint32 grant, uint32 capability,
	ClusterCtrcTxnKeyV1 *key, ClusterCtrcParticipantIdentity *identity)
{
	UT_ASSERT_EQ(held_count, 0);
	if (fail_survivor_proof || proof_key->local_xid != survivor_origin->key.xid
		|| grant != survivor_origin->grant_generation
		|| capability != survivor_origin->touched[0].capability_record_generation)
		return false;
	*key = survivor_origin->key;
	*identity = survivor_origin->touched[0];
	return true;
}

static bool
seed_mx_scope(ClusterCtrcReceipt *predecessor, uint64 *serial)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcApplyToken token;
	ClusterCtrcPublicationIdV1 publication = { 0 };
	ClusterCtrcTargetV1 target = { 0 };
	ClusterCtrcParticipantEntry participant;

	reset_fixture();
	descriptor_creates = 0;
	fail_survivor_proof = false;
	origin = seed_origin(0, 1);
	publication.requester_node_id = 0;
	publication.requester_boot_incarnation = origin->touched[0].boot_incarnation;
	publication.capability_record_generation = origin->touched[0].capability_record_generation;
	publication.requester_backend_id = 11;
	publication.wire_request_id = 101;
	publication.operation_id = 201;
	publication.attempt_generation = 1;
	publication.member_role = 1;
	publication.reference_kind = CTRC_REF_CURRENT_MX_LOCKER;
	publication.target_kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	publication.grant_generation = origin->grant_generation;
	publication.descriptor_hash = 88;
	target.kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	target.spc_oid = 1663;
	target.db_oid = 5;
	target.rel_number = 9001;
	target.block_number = 44;
	target.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	target.publication_own_generation = 17;
	target.publication_acquisition_epoch = 19;
	target.relation_persistence = 'p';
	target.needs_wal = true;
	target.intended_descriptor_hash = 88;
	if (cluster_ctrc_receipt_prepare_shared(&origin->key, &origin->touched[0],
											origin->grant_generation, &publication, &target,
											&handle)
		!= CLUSTER_CTRC_PREPARE_READY)
		return false;
	target.kind = CTRC_TARGET_EXACT_TID;
	target.offset_number = 4;
	target.itemid_flags = LP_NORMAL;
	target.itemid_offset = 128;
	target.itemid_length = 40;
	target.tuple_header_sha256[0] = 1;
	target.multixact_id = 1100;
	target.mx_cluster_epoch = test_cluster_epoch;
	target.descriptor_hash = 88;
	target.intended_descriptor_hash = 0;
	if (cluster_ctrc_receipt_apply_shared(&handle, &target, &token) != CLUSTER_CTRC_APPLY_APPLIED
		|| cluster_ctrc_participant_close(handle.participant, &origin->touched[0],
										  origin->grant_generation, origin->seal_generation)
			   != CLUSTER_CTRC_CLOSE_PENDING_DRAIN)
		return false;
	participant = *handle.participant;
	*predecessor = *handle.receipt;
	survivor_origin = seed_origin(1, 1);
	CtrcWorkerId = 0;
	MyBackendType = B_UNDO_CLEANER;
	MyAuxProcType = UndoCleanerProcess;
	cluster_shared_config = true;
	publication_fenced = true;
	publication_revision++;
	return ctrc_cleaner_read_scope_begin(&participant, predecessor, handle.participant_index,
										 handle.receipt_index, serial);
}

static bool
prepare_successor(const ClusterCtrcReceipt *predecessor, ClusterCtrcTransferState *transfer)
{
	CtrcCleanerCurrentMxCapture capture = { 0 };
	ClusterCurrentMxMemberDesc member = { 0 };
	ClusterCurrentMemberProof proof = { 0 };
	ClusterCtrcCurrentMxRewritePlan plan = { 0 };
	ClusterCtrcTargetV1 target;
	uint8 header[SizeofHeapTupleHeader] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	uint32 capability = survivor_origin->touched[0].capability_record_generation;

	capture.pcm.generation = 17;
	capture.page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	ItemIdSetNormal(&capture.line_pointer, 128, 40);
	member.xid = survivor_origin->key.xid;
	member.member_status = MultiXactStatusForKeyShare;
	proof.key.local_xid = member.xid;
	proof.member_xid = member.xid;
	proof.member_status = member.member_status;
	proof.state = CCM_ACTIVE;
	ClusterCurrentMemberProofSetCtrcGrant(&proof, survivor_origin->grant_generation);
	plan.kind = CTRC_CURRENT_MX_REWRITE_SUCCESSOR;
	plan.survivor_count = 1;
	plan.survivors[0].xid = member.xid;
	plan.survivors[0].status = MultiXactStatusForKeyShare;
	return ctrc_cleaner_prepare_current_mx_successor(
		predecessor, &capture, &member, &proof, &capability, 1, &plan, header, &target, transfer);
}

UT_TEST(test_original_finite_cleaner_can_publish_its_exact_successor)
{
	ClusterCtrcReceipt predecessor;
	ClusterCtrcTransferState transfer;
	ClusterCtrcReceiptHandle unrelated;
	uint64 serial = 0, pi;

	UT_ASSERT(seed_mx_scope(&predecessor, &serial));
	UT_ASSERT(prepare_successor(&predecessor, &transfer));
	UT_ASSERT_EQ(descriptor_creates, 1);
	UT_ASSERT_EQ(transfer.active_survivor_count, 1);
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT(ctrc_participant_index(&survivor_origin->key, 0, &pi));
	UT_ASSERT_EQ(ctrc_participant_entries()[pi].receipt_count, 1);
	UT_ASSERT_EQ(ctrc_participant_entries()[pi].applied_count, 1);
	/* The same background role must not grant arbitrary new publication. */
	UT_ASSERT_EQ(prepare_fixture_receipt(survivor_origin, 302, &unrelated),
				 CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT(!unrelated.valid);
	UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_successor_requires_exact_live_predecessor_attempt)
{
	for (unsigned kind = 0; kind < 7; kind++) {
		ClusterCtrcReceipt predecessor;
		ClusterCtrcTransferState transfer;
		uint64 serial = 0, pi;

		UT_ASSERT(seed_mx_scope(&predecessor, &serial));
		switch (kind) {
		case 0:
			UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
			break;
		case 1:
			MyProcPid++;
			break;
		case 2:
			predecessor.publication.journal_slot_generation++;
			break;
		case 3:
			predecessor.target.block_number++;
			break;
		case 4:
			predecessor.key.slot_wrap++;
			break;
		case 5:
			MyBackendType = B_BACKEND;
			break;
		case 6:
			fail_survivor_proof = true;
			break;
		}
		UT_ASSERT(!prepare_successor(&predecessor, &transfer));
		UT_ASSERT(ctrc_participant_index(&survivor_origin->key, 0, &pi));
		UT_ASSERT_EQ(ctrc_participant_entries()[pi].receipt_count, 0);
		UT_ASSERT_EQ(flush_calls, 0);
		if (kind == 1)
			MyProcPid--;
		MyBackendType = B_UNDO_CLEANER;
		if (kind != 0)
			UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
	}
}

UT_TEST(test_successor_rechecks_fence_revision_at_original_receipt_publication)
{
	for (unsigned already_fenced = 0; already_fenced < 2; already_fenced++) {
		ClusterCtrcReceipt predecessor;
		ClusterCtrcTransferState transfer;
		uint64 serial = 0, pi;

		UT_ASSERT(seed_mx_scope(&predecessor, &serial));
		publication_fenced = already_fenced != 0;
		fence_at_receipt_lock = true;
		UT_ASSERT(!prepare_successor(&predecessor, &transfer));
		UT_ASSERT(ctrc_participant_index(&survivor_origin->key, 0, &pi));
		UT_ASSERT_EQ(ctrc_participant_entries()[pi].receipt_count, 0);
		UT_ASSERT_EQ(flush_calls, 0);
		/* The original finite attempt can resample; the stale sample itself
		 * did not open a participant, publish a receipt or retire its duty. */
		UT_ASSERT(CtrcReadScope.active);
		UT_ASSERT(prepare_successor(&predecessor, &transfer));
		UT_ASSERT_EQ(ctrc_participant_entries()[pi].applied_count, 1);
		UT_ASSERT(ctrc_cleaner_read_scope_end(serial));
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_original_finite_cleaner_can_publish_its_exact_successor);
	UT_RUN(test_successor_requires_exact_live_predecessor_attempt);
	UT_RUN(test_successor_rechecks_fence_revision_at_original_receipt_publication);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
