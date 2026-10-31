/*-------------------------------------------------------------------------
 * test_cluster_mx_operation_core.c
 *    Exact DATA-to-member proof with a separate operation disposition.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Real DATA/UBA/payload validation; the already-qualified local top sample
 * is an explicit boundary value, not a native TT40 or transport fixture.
 *-------------------------------------------------------------------------
 */
int operation_unused_reader_main(void);
#define main operation_unused_reader_main
#include "test_cluster_savepoint_reader.c"
#undef main
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_record_api.h"

int cluster_subtrans_max_chain_depth = 32;

/* Legacy top-only resolver leaf, as in its existing standalone test. The
 * operation core uses full 64-bit identities and never this raw comparison. */
bool
TransactionIdPrecedes(TransactionId left, TransactionId right)
{
	return NormalTransactionIdPrecedes(left, right);
}

typedef struct MxOperationFixture {
	ReaderFixture record;
	PGAlignedBlock data;
	ClusterSavepointOperationKey key;
	ClusterTxResolution top;
	ClusterTTStatusKey ttkey;
	ClusterTTStatusResult status;
	ClusterCurrentMxMemberDesc member;
} MxOperationFixture;

static void
operation_store(MxOperationFixture *f)
{
	UndoBlockHeader header = { .magic = PGRAC_UNDO_BLOCK_MAGIC,
							   .block_version = UNDO_BLOCK_VERSION_1,
							   .slot_count = 1,
							   .free_offset = sizeof(header) + sizeof(f->record.record)
											  + f->record.record.payload_length };
	UndoSlotDirEntry entry
		= { .record_offset = sizeof(header),
			.record_length = sizeof(f->record.record) + f->record.record.payload_length,
			.record_type = f->record.record.record_type,
			.flags = f->record.record.flags };
	memset(f->data.data, 0, BLCKSZ);
	memcpy(f->data.data, &header, sizeof(header));
	memcpy(f->data.data + sizeof(header), &f->record.record, sizeof(f->record.record));
	memcpy(f->data.data + sizeof(header) + sizeof(f->record.record), f->record.body,
		   f->record.record.payload_length);
	memcpy(f->data.data + BLCKSZ - sizeof(entry), &entry, sizeof(entry));
}

static void
operation_setup(MxOperationFixture *f, unsigned kind)
{
	memset(f, 0, sizeof(*f));
	init_fixture(&f->record);
	if (kind == 1 || kind == 2)
		make_update(&f->record, kind == 2);
	if (kind == 3)
		make_delete(&f->record);
	if (kind == 4)
		make_lock(&f->record);
	f->key.locator = f->record.locator;
	f->key.target = (ClusterUndoRecordTarget){ .locator = f->record.record.target_locator,
											   .forknum = MAIN_FORKNUM,
											   .blockno = 17,
											   .offnum = 1 };
	f->key.top_fxid = f->record.expected.top_fxid;
	f->key.operation_fxid = f->record.expected.operation_fxid;
	f->top.locator_echo = f->key.locator;
	f->top.top_xid = 100;
	f->top.outcome = CLUSTER_TX_IN_PROGRESS;
	f->top.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	f->top.authority.origin_epoch = 9;
	f->top.authority.authority_scn = scn_encode(0, 1100);
	f->top.local_canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = 1,
															 .xid = 100,
															 .slot_offset = 2,
															 .wrap = 3 };
	f->top.local_canonical_generation = 7;
	f->top.local_canonical_valid = true;
	f->top.local_original_full_xid = f->key.top_fxid;
	f->ttkey = (ClusterTTStatusKey){ .origin_node_id = 0,
									 .undo_segment_id = 1,
									 .tt_slot_id = 3,
									 .cluster_epoch = 9,
									 .local_xid = 100 };
	f->status.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	f->status.status_epoch = 9;
	f->status.authoritative = true;
	f->member
		= (ClusterCurrentMxMemberDesc){ .xid = 101, .member_status = MultiXactStatusForKeyShare };
	operation_store(f);
}

static bool
operation_effect(MxOperationFixture *f, ClusterSavepointEffectProof *effect)
{
	bool ok = cluster_savepoint_reader_mx_effect(&f->key, &f->top, f->data.data, effect);
	UT_ASSERT(ok);
	return ok;
}

static void
operation_unknown(const ClusterCurrentMemberOperationProof *proof)
{
	ClusterCurrentMemberOperationProof unknown
		= { .base = { .state = CCM_UNKNOWN }, .format_version = 2 };
	UT_ASSERT_EQ(memcmp(proof, &unknown, sizeof(*proof)), 0);
}

UT_TEST(all_data_kinds_bind_exact_raw_member_to_canonical_top)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect;
		ClusterCurrentMemberOperationProof proof;
		operation_setup(&f, kind);
		if (!operation_effect(&f, &effect))
			return;
		UT_ASSERT(cluster_multixact_current_resolve_operation_member_proof(
			&f.member, 6, &f.ttkey, &f.status, &effect, &proof));
		UT_ASSERT_EQ(proof.base.state, CCM_ACTIVE);
		UT_ASSERT_EQ(proof.base.member_xid, 101);
		UT_ASSERT_EQ(proof.base.member_ordinal, 6);
		UT_ASSERT_EQ(proof.base.member_status, MultiXactStatusForKeyShare);
		UT_ASSERT_EQ(proof.base.key.local_xid, 100);
		UT_ASSERT_EQ(proof.base.key.undo_segment_id, 1);
		UT_ASSERT_EQ(proof.base.key.tt_slot_id, 3);
		UT_ASSERT_EQ(proof.base.key.segment_generation, 7);
		UT_ASSERT_EQ(proof.base.key.slot_wrap, 3);
		UT_ASSERT_EQ(proof.base.key.binding_version, 1);
		UT_ASSERT_EQ(proof.format_version, 2);
		UT_ASSERT_EQ(proof.flags, 1);
		UT_ASSERT_EQ(proof.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		UT_ASSERT(cluster_undo_record_uba_equal(proof.operation_uba, f.key.locator.uba));
		UT_ASSERT(FullTransactionIdEquals(proof.top_fxid, f.key.top_fxid));
		UT_ASSERT(FullTransactionIdEquals(proof.operation_fxid, f.key.operation_fxid));
		UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proof.base), 0);
	}
}

UT_TEST(top_state_and_operation_disposition_are_independent)
{
	static const ClusterTTStatus tt[]
		= { CLUSTER_TT_STATUS_IN_PROGRESS, CLUSTER_TT_STATUS_COMMITTED,
			CLUSTER_TT_STATUS_CLEANED_OUT, CLUSTER_TT_STATUS_ABORTED };
	static const ClusterTxOutcome top[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED,
											CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	static const unsigned state[] = { CCM_ACTIVE, CCM_COMMITTED, CCM_COMMITTED, CCM_ABORTED };
	for (unsigned i = 0; i < lengthof(tt); i++)
		for (unsigned disposition = 1; disposition <= 2; disposition++) {
			MxOperationFixture f;
			ClusterSavepointEffectProof effect;
			ClusterCurrentMemberOperationProof proof;
			operation_setup(&f, 4);
			f.status.status = tt[i];
			f.top.outcome = top[i];
			if (state[i] == CCM_COMMITTED)
				f.status.commit_scn = f.top.commit_scn = scn_encode(0, 1200);
			f.record.operation.effect_state = disposition;
			seal_fixture(&f.record);
			operation_store(&f);
			if (!operation_effect(&f, &effect))
				return;
			UT_ASSERT(cluster_multixact_current_resolve_operation_member_proof(
				&f.member, 0, &f.ttkey, &f.status, &effect, &proof));
			UT_ASSERT_EQ(proof.base.state, state[i]);
			UT_ASSERT_EQ(proof.effect_state, disposition);
			UT_ASSERT_EQ(proof.base.commit_scn, f.top.commit_scn);
			UT_ASSERT_EQ(proof.base.key.local_xid, 100);
		}
}

UT_TEST(update_old_and_replacement_targets_are_exact)
{
	for (unsigned cross = 0; cross < 2; cross++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect, zero = { 0 };
		operation_setup(&f, cross ? 2 : 1);
		if (!operation_effect(&f, &effect))
			return;
		f.key.target.blockno = cross ? 18 : 17;
		f.key.target.offnum = 2;
		UT_ASSERT(cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		f.key.target.offnum = 3;
		UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	}
}

UT_TEST(same_child_on_another_row_cannot_supply_effect)
{
	for (unsigned field = 0; field < 7; field++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect, zero = { 0 };
		operation_setup(&f, 0);
		if (!operation_effect(&f, &effect))
			return;
		if (field == 0)
			f.key.target.locator.spcOid++;
		if (field == 1)
			f.key.target.locator.dbOid++;
		if (field == 2)
			f.key.target.locator.relNumber++;
		if (field == 3)
			f.key.target.forknum = VISIBILITYMAP_FORKNUM;
		if (field == 4)
			f.key.target.blockno++;
		if (field == 5)
			f.key.target.offnum++;
		if (field == 6)
			f.key.target._pad = 1;
		UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	}
}

UT_TEST(original_full_xid_and_canonical_incarnation_are_required)
{
	for (unsigned fault = 0; fault < 18; fault++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect, zero = { 0 };
		operation_setup(&f, 0);
		if (!operation_effect(&f, &effect))
			return;
		if (fault == 0)
			f.top.local_original_full_xid = InvalidFullTransactionId;
		if (fault == 1)
			f.top.local_original_full_xid = FullTransactionIdFromU64((UINT64CONST(2) << 32) | 100);
		if (fault == 2)
			f.top.local_canonical_valid = false;
		if (fault == 3)
			f.top.local_canonical_generation++;
		if (fault == 4)
			f.top.local_canonical.wrap++;
		if (fault == 5)
			f.top.local_canonical.slot_offset++;
		if (fault == 6)
			f.top.local_canonical.segment_id++;
		if (fault == 7)
			f.top.local_canonical.xid++;
		if (fault == 8)
			f.top.locator_echo.uba.raw[1] ^= 1;
		if (fault == 9)
			f.top.top_xid++;
		if (fault == 10)
			f.top.proof_kind = CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP;
		if (fault == 11)
			f.top.authority.authority_scn = InvalidScn;
		if (fault == 12)
			f.top.authority.origin_epoch = UINT64CONST(1) << 32;
		if (fault == 13)
			f.top.local_canonical.reserved32 = 1;
		if (fault == 14)
			f.top.outcome = (ClusterTxOutcome)257;
		if (fault == 15)
			f.top.outcome = CLUSTER_TX_COMMITTED;
		if (fault == 16) {
			f.top.outcome = CLUSTER_TX_ABORTED;
			f.top.commit_scn = scn_encode(0, 1200);
		}
		if (fault == 17)
			f.top.commit_scn = scn_encode(0, 1200);
		UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	}
}

UT_TEST(current_data_echo_and_tail_mismatches_refuse)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect, zero = { 0 };
		operation_setup(&f, 0);
		if (!operation_effect(&f, &effect))
			return;
		if (fault == 0)
			f.record.record.xid++;
		if (fault == 1)
			f.record.operation.canonical_top_fxid += UINT64CONST(1) << 32;
		if (fault == 2)
			f.record.operation.operation_fxid += UINT64CONST(1) << 32;
		if (fault == 3)
			f.record.operation.canonical_segment_generation++;
		if (fault == 4)
			f.record.record.tt_wrap_plus1++;
		if (fault == 5)
			f.record.record.origin_node_id++;
		if (fault == 6)
			f.record.operation.effect_state = 3;
		if (fault == 7)
			f.record.operation.operation_sequence = 0;
		if (fault == 8)
			f.record.record.prev_uba = f.record.current;
		if (fault == 9)
			f.record.operation.reserved_end = 1;
		seal_fixture(&f.record);
		operation_store(&f);
		UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	}
}

UT_TEST(wrapped_raw_xids_keep_full_identity_order)
{
	MxOperationFixture f;
	ClusterSavepointEffectProof effect;
	ClusterCurrentMemberOperationProof proof;
	operation_setup(&f, 0);
	f.key.top_fxid = FullTransactionIdFromU64(UINT64CONST(0x1fffffff0));
	f.key.operation_fxid = FullTransactionIdFromU64(UINT64CONST(0x200000008));
	f.key.locator.xid = f.top.top_xid = f.top.local_canonical.xid = f.ttkey.local_xid = 0xfffffff0;
	f.top.locator_echo = f.key.locator;
	f.top.local_original_full_xid = f.key.top_fxid;
	f.record.record.xid = f.member.xid = 8;
	f.record.operation.canonical_top_fxid = U64FromFullTransactionId(f.key.top_fxid);
	f.record.operation.operation_fxid = U64FromFullTransactionId(f.key.operation_fxid);
	seal_fixture(&f.record);
	operation_store(&f);
	if (!operation_effect(&f, &effect))
		return;
	UT_ASSERT(cluster_multixact_current_resolve_operation_member_proof(&f.member, 255, &f.ttkey,
																	   &f.status, &effect, &proof));
	UT_ASSERT_EQ(proof.base.member_xid, 8);
	UT_ASSERT_EQ(proof.base.key.local_xid, 0xfffffff0);
	UT_ASSERT_EQ(proof.base.state, CCM_ACTIVE);
}

UT_TEST(unaligned_data_and_malformed_slot_directory_are_checked)
{
	MxOperationFixture f;
	ClusterSavepointEffectProof effect, zero = { 0 };
	char unaligned[BLCKSZ + 1];
	operation_setup(&f, 0);
	memcpy(unaligned + 1, f.data.data, BLCKSZ);
	UT_ASSERT(cluster_savepoint_reader_mx_effect(&f.key, &f.top, unaligned + 1, &effect));
	for (unsigned fault = 0; fault < 3; fault++) {
		UndoBlockHeader header;
		UndoSlotDirEntry entry;
		operation_store(&f);
		memcpy(&header, f.data.data, sizeof(header));
		memcpy(&entry, f.data.data + BLCKSZ - sizeof(entry), sizeof(entry));
		if (fault == 0)
			header.slot_count = UINT16_MAX;
		if (fault == 1)
			header.free_offset = sizeof(header);
		if (fault == 2)
			entry.record_length--;
		memcpy(f.data.data, &header, sizeof(header));
		memcpy(f.data.data + BLCKSZ - sizeof(entry), &entry, sizeof(entry));
		UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	}
}

UT_TEST(member_echo_mode_order_and_top_key_mismatches_refuse)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect;
		ClusterCurrentMemberOperationProof proof;
		uint16 ordinal = 3;
		operation_setup(&f, 0);
		if (!operation_effect(&f, &effect))
			return;
		if (fault == 0)
			f.member.xid++;
		if (fault == 1)
			f.member.member_status = MaxMultiXactStatus + 1;
		if (fault == 2)
			f.member.reserved8[2] = 1;
		if (fault == 3)
			ordinal = 256;
		if (fault == 4)
			f.ttkey.origin_node_id++;
		if (fault == 5)
			f.ttkey.undo_segment_id++;
		if (fault == 6)
			f.ttkey.tt_slot_id++;
		if (fault == 7)
			f.ttkey.cluster_epoch++;
		if (fault == 8)
			f.ttkey.local_xid++;
		if (fault == 9)
			f.ttkey._reserved = 1;
		if (fault == 10)
			f.ttkey._reserved2 = 1;
		memset(&proof, 0xa5, sizeof(proof));
		UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(
			&f.member, ordinal, &f.ttkey, &f.status, &effect, &proof));
		operation_unknown(&proof);
	}
}

UT_TEST(unknown_parent_status_or_mixed_samples_never_grant)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		MxOperationFixture f;
		ClusterSavepointEffectProof effect;
		ClusterCurrentMemberOperationProof proof;
		operation_setup(&f, 0);
		if (!operation_effect(&f, &effect))
			return;
		if (fault == 0)
			f.status.authoritative = false;
		if (fault == 1)
			f.status.status = CLUSTER_TT_STATUS_UNKNOWN;
		if (fault == 2)
			f.status.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
		if (fault == 3)
			f.status.has_parent_key = true;
		if (fault == 4)
			f.status.parent_key.local_xid = 99;
		if (fault == 5)
			f.status.status_epoch++;
		if (fault == 6)
			f.status.commit_scn = scn_encode(0, 1200);
		if (fault == 7)
			f.status.status = CLUSTER_TT_STATUS_ABORTED;
		if (fault == 8)
			f.status.status = CLUSTER_TT_STATUS_COMMITTED;
		if (fault == 9)
			effect.reserved16 = 1;
		memset(&proof, 0xa5, sizeof(proof));
		UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(
			&f.member, 0, &f.ttkey, &f.status, &effect, &proof));
		operation_unknown(&proof);
	}
}

UT_TEST(ctrc_binding_stays_on_exact_top_even_when_discarded)
{
	MxOperationFixture f;
	ClusterSavepointEffectProof effect;
	ClusterCurrentMemberOperationProof proof, saved;
	ClusterCtrcTxnKeyV1 ctrc = { .format_version = CLUSTER_CTRC_FORMAT_VERSION,
								 .owner_instance = 1,
								 .origin_node_id = 0,
								 .segment_id = 1,
								 .segment_generation = 7,
								 .slot_offset = 2,
								 .slot_wrap = 3,
								 .xid = 100,
								 .cluster_epoch = 9 };
	operation_setup(&f, 4);
	if (!operation_effect(&f, &effect))
		return;
	UT_ASSERT(cluster_multixact_current_resolve_operation_member_proof(&f.member, 1, &f.ttkey,
																	   &f.status, &effect, &proof));
	ClusterCurrentMemberProofSetCtrcGrant(&proof.base, 17);
	saved = proof;
	UT_ASSERT(cluster_multixact_current_operation_member_proof_bind_ctrc(&proof, &ctrc));
	UT_ASSERT_EQ(proof.base.key.local_xid, 100);
	UT_ASSERT_EQ(proof.base.member_xid, 101);
	UT_ASSERT_EQ(proof.base.state, CCM_ACTIVE);
	UT_ASSERT_EQ(proof.effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterCtrcTxnKeyV1 bad = ctrc;
		proof = saved;
		if (fault == 0)
			bad.xid = 101;
		if (fault == 1)
			bad.segment_generation++;
		if (fault == 2)
			bad.slot_wrap++;
		if (fault == 3)
			bad.segment_id++;
		if (fault == 4)
			bad.slot_offset++;
		if (fault == 5)
			bad.cluster_epoch++;
		UT_ASSERT(!cluster_multixact_current_operation_member_proof_bind_ctrc(&proof, &bad));
		UT_ASSERT_EQ(memcmp(&proof, &saved, sizeof(proof)), 0);
	}
}

UT_TEST(old_top_resolver_does_not_become_a_child_lookup)
{
	MxOperationFixture f;
	ClusterSavepointEffectProof effect;
	ClusterCurrentMemberOperationProof operation;
	ClusterCurrentMemberProof old;
	operation_setup(&f, 0);
	if (!operation_effect(&f, &effect))
		return;
	UT_ASSERT(cluster_multixact_current_resolve_operation_member_proof(
		&f.member, 0, &f.ttkey, &f.status, &effect, &operation));
	UT_ASSERT(!cluster_multixact_current_resolve_origin_member_proof(
		101, MultiXactStatusForKeyShare, 0, 0, 9, false, &f.ttkey, &f.status, NULL, NULL, &old));
	UT_ASSERT_EQ(old.state, CCM_UNKNOWN);
	UT_ASSERT(cluster_multixact_current_resolve_origin_member_proof(
		100, MultiXactStatusForKeyShare, 0, 0, 9, false, &f.ttkey, &f.status, NULL, NULL, &old));
	UT_ASSERT_EQ(old.state, CCM_ACTIVE);
}

UT_TEST(missing_inputs_and_nonoperation_records_fail_closed)
{
	MxOperationFixture f;
	ClusterSavepointEffectProof effect, zero = { 0 };
	ClusterCurrentMemberOperationProof proof;
	operation_setup(&f, 0);
	if (!operation_effect(&f, &effect))
		return;
	UT_ASSERT(cluster_savepoint_reader_mx_effect_valid(&effect));
	UT_ASSERT(!cluster_savepoint_reader_mx_effect_valid(NULL));
	UT_ASSERT(!cluster_savepoint_reader_mx_effect(NULL, &f.top, f.data.data, &effect));
	UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, NULL, f.data.data, &effect));
	UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, NULL, &effect));
	UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, NULL));
	UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(
		NULL, 0, &f.ttkey, &f.status, &effect, &proof));
	operation_unknown(&proof);
	UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(
		&f.member, 0, NULL, &f.status, &effect, &proof));
	operation_unknown(&proof);
	UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(&f.member, 0, &f.ttkey,
																		NULL, &effect, &proof));
	operation_unknown(&proof);
	UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(&f.member, 0, &f.ttkey,
																		&f.status, NULL, &proof));
	operation_unknown(&proof);
	UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(&f.member, 0, &f.ttkey,
																		&f.status, &effect, NULL));
	for (unsigned fault = 0; fault < 4; fault++) {
		operation_setup(&f, 0);
		if (fault == 0) {
			f.record.record.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
			f.record.record.payload_length -= sizeof(UndoOperationTrailer);
		}
		if (fault == 1)
			f.record.record.record_type = UNDO_RECORD_SAVEPOINT_ROLLBACK;
		if (fault == 2)
			f.key.locator.uba = (UBA){ { 0, 0 } };
		if (fault == 3)
			f.key.operation_fxid = f.key.top_fxid;
		operation_store(&f);
		memset(&effect, 0xa5, sizeof(effect));
		UT_ASSERT(!cluster_savepoint_reader_mx_effect(&f.key, &f.top, f.data.data, &effect));
		UT_ASSERT_EQ(memcmp(&effect, &zero, sizeof(effect)), 0);
	}
}

UT_TEST(incoherent_effect_and_operation_binding_are_rejected)
{
	MxOperationFixture f;
	ClusterSavepointEffectProof effect, bad;
	ClusterCurrentMemberOperationProof proof, saved;
	ClusterCtrcTxnKeyV1 ctrc = { .format_version = CLUSTER_CTRC_FORMAT_VERSION,
								 .owner_instance = 1,
								 .origin_node_id = 0,
								 .segment_id = 1,
								 .segment_generation = 7,
								 .slot_offset = 2,
								 .slot_wrap = 3,
								 .xid = 100,
								 .cluster_epoch = 9 };
	operation_setup(&f, 0);
	if (!operation_effect(&f, &effect))
		return;
	for (unsigned fault = 0; fault < 9; fault++) {
		bad = effect;
		if (fault == 0)
			bad.identity.canonical.reserved32 = 1;
		if (fault == 1)
			bad.key.top_fxid = FullTransactionIdFromU64(UINT64_MAX);
		if (fault == 2)
			bad.identity.operation_fxid = bad.identity.top_fxid;
		if (fault == 3)
			bad.key.locator.uba = uba_encode(2, 7, 3, 0);
		if (fault == 4)
			bad.key.locator.tt_wrap++;
		if (fault == 5)
			bad.effect_state = 0;
		if (fault == 6)
			bad.top_outcome = CLUSTER_TX_PREPARED;
		if (fault == 7)
			bad.top_commit_scn = scn_encode(0, 1200);
		if (fault == 8)
			bad.key.target._pad = 1;
		UT_ASSERT(!cluster_savepoint_reader_mx_effect_valid(&bad));
		UT_ASSERT(!cluster_multixact_current_resolve_operation_member_proof(
			&f.member, 0, &f.ttkey, &f.status, &bad, &proof));
		operation_unknown(&proof);
	}
	UT_ASSERT(cluster_multixact_current_resolve_operation_member_proof(&f.member, 0, &f.ttkey,
																	   &f.status, &effect, &proof));
	ClusterCurrentMemberProofSetCtrcGrant(&proof.base, 17);
	saved = proof;
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterCurrentMemberOperationProof before;
		proof = saved;
		if (fault == 0)
			proof.format_version = 1;
		if (fault == 1)
			proof.flags = 0;
		if (fault == 2)
			proof.reserved = 1;
		if (fault == 3)
			proof.base.key.binding_version = 0;
		if (fault == 4)
			proof.operation_fxid = proof.top_fxid;
		if (fault == 5)
			proof.operation_uba = uba_encode(2, 7, 3, 0);
		if (fault == 6)
			proof.effect_state = 0;
		if (fault == 7)
			proof.base.commit_scn = scn_encode(0, 1200);
		if (fault == 8)
			proof.base.state = CCM_COMMITTED;
		if (fault == 9)
			ClusterCurrentMemberProofSetCtrcGrant(&proof.base, 0);
		before = proof;
		UT_ASSERT(!cluster_multixact_current_operation_member_proof_bind_ctrc(&proof, &ctrc));
		UT_ASSERT_EQ(memcmp(&proof, &before, sizeof(proof)), 0);
	}
	UT_ASSERT(!cluster_multixact_current_operation_member_proof_bind_ctrc(NULL, &ctrc));
	proof = saved;
	UT_ASSERT(!cluster_multixact_current_operation_member_proof_bind_ctrc(&proof, NULL));
	UT_ASSERT_EQ(memcmp(&proof, &saved, sizeof(proof)), 0);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(all_data_kinds_bind_exact_raw_member_to_canonical_top);
	UT_RUN(top_state_and_operation_disposition_are_independent);
	UT_RUN(update_old_and_replacement_targets_are_exact);
	UT_RUN(same_child_on_another_row_cannot_supply_effect);
	UT_RUN(original_full_xid_and_canonical_incarnation_are_required);
	UT_RUN(current_data_echo_and_tail_mismatches_refuse);
	UT_RUN(wrapped_raw_xids_keep_full_identity_order);
	UT_RUN(unaligned_data_and_malformed_slot_directory_are_checked);
	UT_RUN(member_echo_mode_order_and_top_key_mismatches_refuse);
	UT_RUN(unknown_parent_status_or_mixed_samples_never_grant);
	UT_RUN(ctrc_binding_stays_on_exact_top_even_when_discarded);
	UT_RUN(old_top_resolver_does_not_become_a_child_lookup);
	UT_RUN(missing_inputs_and_nonoperation_records_fail_closed);
	UT_RUN(incoherent_effect_and_operation_binding_are_rejected);
	UT_DONE();
	return ut_failed_count != 0;
}
