/* Exact DROP lifecycle + original DATA producer/notice consumer.
 * KO/PCM/input liveness are explicit fixtures; parser, sealed PAGE/SIDE
 * graph and temporary-file read/fsync/read are real. No server/fabric claim.
 * Author: SqlRush <sqlrush@gmail.com> */
int pi_storage_fixture_main(void);
#define main pi_storage_fixture_main
#include "test_cluster_page_data.c"
#undef main

/* 0 LIVE, 1 DROP, 2 TRUNCATE, 3 wrong marker, 4 absent marker,
 * 5 duplicate marker, 6 reservation gap, 7 wrong CREATE transaction,
 * 8 PAGE-only retained input, 9 A->B->A incarnation reuse,
 * 10 ADVANCE-only, 11 ADVANCE gap, 12 ADVANCE with another incarnation. */
static ClusterThreadRecoveryFabricPlanV1 *
drop_storage_plan(unsigned mode, RfSidePlanPurposeV1 purpose, bool seal,
	ClusterWalSourceRef *source, ClusterPcmPiStorageCutV1 *cut)
{
	ClusterThreadRecoveryFabricPlanRequestV1 request = {0};
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	RfContributorStreamCutV1 input = {0};
	ClusterSpaceStructureChange create = {0}, end = {0};
	ClusterSpaceReservationChange reserve = {0};
	unsigned records = mode >= 10 ? 3 : mode == 8 ? 1 : mode == 9 ? 6 : (mode == 0 || mode >= 6 ? 4 : 5);
	bool truncate = mode == 2 || mode == 9;
	reset();
	MyBackendType = B_CHECKPOINTER;
	*source = writer;
	input.failed_thread = source->claim.identity.origin_thread_id;
	input.origin_owner_incarnation = source->claim.identity.origin_owner_incarnation;
	input.timeline_id = source->timeline;
	input.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	input.scan_begin_inclusive = mode >= 10 ? 0x200 : mode == 8 ? 0x400 : 0x100;
	input.scan_end_exclusive = mode >= 10 || mode == 8 ? 0x500 : (records + 1) * 0x100;
	request.system_identifier = identity.key.system_identifier;
	memcpy(request.storage_uuid, identity.key.storage_uuid, 16);
	request.sources = source;
	request.physical_cuts = &input;
	request.participant_count = 1;
	request.retention_binding_cookie = 41;
	request.space_active = true;
	request.purpose = purpose;
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	create.identity.action = CLUSTER_SPACE_WAL_CREATE;
	create.identity.nblocks = InvalidBlockNumber;
	create.identity.result = identity;
	create.identity.result_token = 50;
	create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	create.reservation.result.identity = identity;
	create.reservation.result_token = 50;
	reserve.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	reserve.before = create.reservation.result;
	reserve.before_token = mode == 6 ? 51 : 50;
	reserve.result = reserve.before;
	reserve.result.next_block = reserve.granted = 8;
	reserve.result_token = 70;
	end.identity.action = truncate ? CLUSTER_SPACE_WAL_TRUNCATE : CLUSTER_SPACE_WAL_TOMBSTONE;
	end.identity.nblocks = truncate ? 4 : InvalidBlockNumber;
	end.identity.expected = end.identity.result = identity;
	end.identity.result.sequence++;
	end.identity.result.operation++;
	end.identity.before_token = 50;
	end.identity.result_token = 90;
	if (truncate) end.identity.result.incarnation[0]++;
	else end.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	end.reservation.action = truncate ? CLUSTER_SPACE_RESERVATION_RESET : CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	end.reservation.before = reserve.result;
	end.reservation.before_token = 70;
	end.reservation.result = end.reservation.before;
	end.reservation.result.identity = end.identity.result;
	end.reservation.result_token = 90;
	if (truncate) end.reservation.result.next_block = end.reservation.first_block = 4;
	PageSetLSNPreserveOrigin(pages[1].data, 0x500);
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, 0));
	for (unsigned i = 0; i < records; i++) {
		unsigned stage = mode >= 10 ? i + 1 : mode == 8 ? 3 : i;
		union { DecodedXLogRecord record; char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)]; } data = {0};
		DecodedXLogRecord *r = &data.record;
		XLogReaderState reader = {0};
		PGAlignedBlock payload = {0};
		char error[1024];
		uint32 len = 0;
		reader.record = r;
		reader.ReadRecPtr = r->lsn = (stage + 1) * 0x100;
		reader.EndRecPtr = r->next_lsn = (stage + 2) * 0x100;
		reader.system_identifier = identity.key.system_identifier;
		reader.errormsg_buf = error;
		r->header.xl_crc = 0x9192 + stage;
		r->header.xl_xid = 802;
		r->header.xl_rmid = RM_SMGR_ID;
		r->max_block_id = -1;
		if (mode >= 10 && stage < 3) {
			ClusterSpaceReservationChange advance = reserve;
			if (stage == 1) {
				advance.result.next_block = advance.granted = 4;
				advance.result_token = 60;
			} else {
				advance.before.next_block = advance.first_block = 4;
				advance.before_token = mode == 11 ? 61 : 60;
				advance.granted = 4;
			}
			if (mode == 12) {
				advance.before.identity.incarnation[0]++;
				advance.result.identity.incarnation[0]++;
			}
			len = CLUSTER_SPACE_RESERVATION_WAL_BYTES;
			UT_ASSERT(cluster_space_reservation_wal_encode(&advance, payload.data, len));
			r->header.xl_info = XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE;
		} else if (stage == 0) {
			xl_smgr_create native = {.rlocator = identity.key.locator, .forkNum = MAIN_FORKNUM};
			len = sizeof(native);
			memcpy(payload.data, &native, len);
			r->header.xl_info = XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE;
		} else if (stage == 1) {
			len = CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
			UT_ASSERT(cluster_space_structure_wal_encode(&create, payload.data, len));
			r->header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
			if (mode == 7) r->header.xl_xid++;
		} else if (stage == 2) {
			len = CLUSTER_SPACE_RESERVATION_WAL_BYTES;
			UT_ASSERT(cluster_space_reservation_wal_encode(&reserve, payload.data, len));
			r->header.xl_info = XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE;
		} else if (stage == 3) {
			DecodedBkpBlock *block = &r->blocks[0];
			RfPageVersionEdgeEntryV1 *edge = &r->page_version_edge.entries[0];
			r->header.xl_rmid = RM_XLOG_ID;
			r->header.xl_info = XLOG_FPI;
			r->max_block_id = 0;
			r->has_page_version_edge = true;
			r->page_version_edge.entry_count = 1;
			r->page_version_edge.result_token = 80;
			block->in_use = block->has_image = block->apply_image = true;
			block->rlocator = target.identity.locator;
			block->forknum = target.identity.forknum;
			block->blkno = target.identity.blockno;
			block->bkp_image = pages[1].data;
			block->bimg_len = BLCKSZ;
			block->bimg_info = BKPIMAGE_APPLY;
			edge->page_class = RF_PAGE_CLASS_ORDINARY;
			edge->before_kind = edge->result_kind = RF_PAGE_STATE_PRESENT;
			memcpy(edge->before.segment_incarnation, identity.incarnation, 16);
			memcpy(edge->result_incarnation, identity.incarnation, 16);
			edge->before.mutation_token = 10;
			edge->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
		} else if (truncate) {
			if (stage == 5) {
				end.identity.expected = end.identity.result;
				end.identity.before_token = 90;
				end.identity.result_token = 100;
				end.identity.result.sequence++;
				end.identity.result.operation++;
				memcpy(end.identity.result.incarnation, identity.incarnation, 16);
				end.reservation.before = end.reservation.result;
				end.reservation.before_token = 90;
				end.reservation.result.identity = end.identity.result;
				end.reservation.result_token = 100;
			}
			len = CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
			UT_ASSERT(cluster_space_structure_wal_encode(&end, payload.data, len));
			r->header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
		} else {
			xl_xact_commit commit = {.xact_time = 123456};
			xl_xact_xinfo xinfo = {.xinfo = XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_RELFILELOCATORS
				| XACT_XINFO_HAS_INVALS | XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT | XACT_XINFO_HAS_SPACE_DROP};
			xl_xact_dbinfo db = {.dbId = identity.key.locator.dbOid, .tsId = identity.key.locator.spcOid};
			xl_xact_scn scn = {.scn = 99};
			xl_xact_tt_commit tt = {.instance = 1, .segment_id = 1, .segment_generation = 11,
				.slot_offset = 4, .wrap = 7, .xid = 802, .format_version = CLUSTER_XACT_TT_COMMIT_VERSION, .commit_scn = 99};
			SharedInvalidationMessage msg = {0};
			uint32 one = 1, nmsgs = mode == 4 ? 0 : (mode == 5 ? 2 : 1);
			char *p = payload.data;
			msg.ko.id = SHAREDINVALKO_COMMIT_ID;
			msg.ko.version = SHAREDINVALKO_COMMIT_VERSION;
			msg.ko.batch_lo = mode == 3 ? 772 : 771;
#define APPEND(v) do { memcpy(p, &(v), sizeof(v)); p += sizeof(v); } while (0)
			APPEND(commit); APPEND(xinfo); APPEND(db); APPEND(one); APPEND(identity.key.locator);
			APPEND(nmsgs);
			for (unsigned j = 0; j < nmsgs; j++) APPEND(msg);
			APPEND(scn); APPEND(tt); APPEND(one);
#undef APPEND
			UT_ASSERT(cluster_space_structure_wal_encode(&end, p, CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
			len = p - payload.data + CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
			r->header.xl_rmid = RM_XACT_ID;
			r->header.xl_info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
		}
		r->main_data = payload.data;
		r->main_data_len = len;
		UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_feed_record_v1(plan, &reader, 0), RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(cluster_thread_recovery_fabric_bind_database_v1(plan, identity.key.database_incarnation));
	if (seal) {
		RfPageProofDetailV1 result = cluster_thread_recovery_fabric_plan_seal_v1(plan);
		if (mode == 6 || mode == 7 || mode == 11) UT_ASSERT(result != RF_PAGE_PROOF_DETAIL_OK);
		else UT_ASSERT_EQ(result, RF_PAGE_PROOF_DETAIL_OK);
	}
	memset(cut, 0, sizeof(*cut));
	cut->authority.state = PCM_STATE_N;
	cut->authority.master_holder.node_id = UINT32_MAX;
	cut->authority.x_holder_node = cut->authority.pending_x_requester_node = -1;
	cut->authority.transition_count = 6;
	cut->resource = descriptors[1].bufferdesc.tag;
	cut->pi_holders_bitmap = 1;
	cut->binding_generation = 1;
	cut->resource_formation = 17;
	cut->authority_generation = 3;
	cut->master_generation = 4;
	cut->master_session_incarnation = 31;
	resident[0] = resident[1] = false;
	storage_read = require_data_io_scope = true;
	PageSetChecksumInplace(pages[1].data, target.identity.blockno);
	UT_ASSERT_EQ(pwrite(fileno(file), pages[1].data, BLCKSZ, 0), BLCKSZ);
	pi_drop_request.base.key = identity.key;
	memcpy(pi_drop_request.base.incarnation, identity.incarnation, 16);
	pi_drop_request.base.origin_node = 0;
	pi_drop_request.base.origin_boot = source->claim.identity.origin_owner_incarnation;
	pi_drop_request.base.batch_id = 771;
	pi_drop_request.operation = CLUSTER_KO_SHARED_V3_OPERATION_DROP;
	pi_drop_request.space_mutation_token = 50;
	pi_drop_available = true;
	return plan;
}

static void
complete_drop_identity_without_current_and_notice_without_second_io(void)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut, observed = {0};
		ClusterPageDataReceiptV1 *receipt = NULL, *peer = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(mode, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		const RfPageOnlinePlanV1 *page = cluster_thread_recovery_fabric_page_plan_v1(plan);
		UT_ASSERT(cluster_thread_recovery_fabric_pi_contributions_v1(plan));
		UT_ASSERT(!cluster_thread_recovery_fabric_replay_allowed_v1(plan));
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT(receipt != NULL);
		UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(writes + wal_flushes, 0);
		UT_ASSERT(cluster_page_data_bind_plan_v1(receipt, plan));
		UT_ASSERT(cluster_page_data_pi_storage_proof_v1(receipt, page, &source, 1, &observed));
		UT_ASSERT_EQ(memcmp(&observed, &cut, sizeof(cut)), 0);
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &notice_fact));
		pi_drop_available = pi_drop_current = false;
		notice_ready = true;
		MyBackendType = B_BG_WRITER;
		UT_ASSERT(cluster_page_data_from_notice_v1((void *)1, 0, &peer));
		UT_ASSERT(cluster_page_data_bind_plan_v1(peer, plan));
		UT_ASSERT(cluster_page_data_pi_storage_proof_v1(peer, page, &source, 1, &observed));
		UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(peer, page, &source, 1), CLUSTER_PI_PHYSICAL_ABSENT);
		UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(syncs, 1);
		cluster_page_data_receipt_free_v1(&peer); cluster_page_data_receipt_free_v1(&receipt);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
unqualified_capture_and_lifecycle_refuse_before_data_io(void)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(1, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		switch (fault) {
		case 0: pi_drop_available = false; break;
		case 1: pi_drop_request.space_mutation_token++; break;
		case 2: pi_drop_request.base.incarnation[0]++; break;
		case 3: pi_drop_request.base.key.database_incarnation++; break;
		case 4: pi_drop_request.base.key.storage_uuid[0]++; break;
		case 5: pi_drop_request.base.key.system_identifier++; break;
		case 6: pi_drop_request.base.origin_boot++; break;
		case 7: pi_drop_request.base.origin_node++; break;
		case 8: pi_drop_request.base.batch_id++; break;
		case 9: pi_drop_request.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE; break;
		case 10: directory_io_denied = true; break;
		case 11: source.claim.claim_sha256[1]++; break;
		}
		UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT(receipt == NULL); UT_ASSERT_EQ(reads + syncs + writes, 0);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
unsupported_or_incomplete_typed_lifecycle_cannot_replace_resident_identity(void)
{
	for (unsigned mode = 2; mode <= 7; mode++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(mode, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT(receipt == NULL); UT_ASSERT_EQ(reads + syncs + writes, 0);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
storage_publication_rechecks_operation_source_and_exact_cut(void)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(1, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		if (fault == 0) pi_drop_expire_sync = true;
		if (fault == 1) storage_cut_changed = true;
		if (fault == 2) stale_sync = true;
		if (fault == 3) bad_checksum = true;
		if (fault == 4) bad_bytes = true;
		if (fault == 5) { receipt_allocation_watch = true; receipt_allocation_close = true; }
		UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT(receipt == NULL && !directory_io_entered);
		UT_ASSERT_EQ(writes + wal_flushes, 0);
		if (fault != 3) UT_ASSERT_EQ(syncs, 1);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
notice_requires_same_complete_fact_and_lifecycle(void)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut, observed;
		ClusterPageDataReceiptV1 *receipt = NULL, *peer = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(1, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		const RfPageOnlinePlanV1 *page = cluster_thread_recovery_fabric_page_plan_v1(plan);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &notice_fact));
		if (fault == 0) notice_fact.binding.record_crc++;
		if (fault == 1) notice_fact.binding.source.claim.claim_sha256[1]++;
		if (fault == 2) notice_fact.binding.version.mutation_token++;
		notice_ready = true;
		pi_drop_available = pi_drop_current = false;
		MyBackendType = B_BG_WRITER;
		UT_ASSERT(cluster_page_data_from_notice_v1((void *)1, 0, &peer));
		UT_ASSERT(!cluster_page_data_pi_storage_proof_v1(peer, page, &source, 1, &observed));
		if (fault < 3) {
			UT_ASSERT(!cluster_page_data_bind_plan_v1(peer, plan));
		} else {
			UT_ASSERT(cluster_page_data_bind_plan_v1(peer, plan));
			if (fault == 3) notice_ready = false;
			if (fault == 4) notice_fact.storage_cut.authority_generation++;
			if (fault == 5) notice_fact.binding.source.claim.identity.origin_owner_incarnation++;
			if (fault == 6) notice_fact.binding.record_crc++;
			UT_ASSERT(!cluster_page_data_bind_plan_v1(peer, plan));
			UT_ASSERT(!cluster_page_data_pi_storage_proof_v1(peer, page, &source, 1, &observed));
		}
		UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(syncs, 1);
		cluster_page_data_receipt_free_v1(&peer); cluster_page_data_receipt_free_v1(&receipt);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
unsealed_or_recovery_plan_cannot_authorize_drop_identity(void)
{
	ClusterWalSourceRef source;
	ClusterPcmPiStorageCutV1 cut;
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(0, RF_SIDE_PLAN_PI_CONTRIBUTIONS, false, &source, &cut);
	ClusterThreadRecoveryFabricPlanV1 *recovery = NULL;
	RfContributorStreamCutV1 empty = {.failed_thread = 1, .origin_owner_incarnation = 9,
		.timeline_id = 1, .flags = RF_CONTRIBUTOR_CUT_COMPLETE | RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY,
		.scan_begin_inclusive = 0x100, .scan_end_exclusive = 0x100};
	ClusterThreadRecoveryFabricPlanRequestV1 request = {.system_identifier = 17,
		.sources = &source, .physical_cuts = &empty, .participant_count = 1,
		.retention_binding_cookie = 41, .space_active = true, .purpose = RF_SIDE_PLAN_RECOVERY};
	request.storage_uuid[0] = 1;
	UT_ASSERT(!cluster_thread_recovery_fabric_pi_contributions_v1(plan));
	UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_create_v1(&request, &recovery), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(cluster_thread_recovery_fabric_bind_database_v1(recovery, 3));
	UT_ASSERT_EQ(cluster_thread_recovery_fabric_plan_seal_v1(recovery), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(cluster_thread_recovery_fabric_replay_allowed_v1(recovery));
	UT_ASSERT(!cluster_thread_recovery_fabric_pi_contributions_v1(recovery));
	UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, recovery, &source, 1, &receipt));
	UT_ASSERT(receipt == NULL); UT_ASSERT_EQ(reads + syncs + directory_io_begins, 0);
	cluster_thread_recovery_fabric_plan_destroy_v1(&recovery);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
}

static void
data_error_and_allocation_error_release_only_the_original_call(void)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(1, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		volatile bool caught = false;
		if (fault == 0) throw_at = 4;
		if (fault == 1) throw_at = 3;
		if (fault == 2) throw_at = 5;
		if (fault == 3) { receipt_allocation_watch = true; receipt_allocation_error = true; }
		PG_TRY();
		{
			(void)cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught && receipt == NULL && !directory_io_entered);
		UT_ASSERT_EQ(directory_io_begins, 1); UT_ASSERT_EQ(directory_io_ends, 1);
		UT_ASSERT_EQ(logical_pi_retire_calls + pi_discards + writes + wal_flushes, 0);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
same_uuid_after_truncate_cannot_inherit_old_capture(void)
{
	ClusterWalSourceRef source;
	ClusterPcmPiStorageCutV1 cut;
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(9, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
	UT_ASSERT(cluster_thread_recovery_fabric_pi_contributions_v1(plan));
	UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
	UT_ASSERT(receipt == NULL); UT_ASSERT_EQ(reads + syncs + writes, 0);
	cluster_page_data_receipt_free_v1(&receipt);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
}

static void
authenticated_resident_storage_fact_keeps_page_only_coverage(void)
{
	ClusterWalSourceRef source;
	ClusterPcmPiStorageCutV1 cut, observed;
	ClusterPageDataReceiptV1 *receipt = NULL, *peer = NULL;
	ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(8, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
	const RfPageOnlinePlanV1 *page = cluster_thread_recovery_fabric_page_plan_v1(plan);
	/* New file observations cannot use PAGE-only as a missing identity. */
	UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
	UT_ASSERT_EQ(reads + syncs, 0);
	resident[0] = true;
	UT_ASSERT(cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &notice_fact));
	pi_drop_available = pi_drop_current = false;
	MyBackendType = B_BG_WRITER;
	notice_ready = true;
	UT_ASSERT(cluster_page_data_from_notice_v1((void *)1, 0, &peer));
	UT_ASSERT(cluster_page_data_bind_plan_v1(peer, plan));
	UT_ASSERT(cluster_page_data_pi_storage_proof_v1(peer, page, &source, 1, &observed));
	UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(syncs, 1);
	cluster_page_data_receipt_free_v1(&peer); cluster_page_data_receipt_free_v1(&receipt);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
}

static void
authenticated_resident_fact_covers_exact_reservation_only_history(void)
{
	for (unsigned mode = 10; mode <= 12; mode++) {
		ClusterWalSourceRef source;
		ClusterPcmPiStorageCutV1 cut, observed;
		ClusterPageDataReceiptV1 *receipt = NULL, *peer = NULL;
		ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(mode, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
		const RfPageOnlinePlanV1 *page = cluster_thread_recovery_fabric_page_plan_v1(plan);
		/* No retained SPACE0 capture: this cannot authorize a new observation. */
		UT_ASSERT(!cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT_EQ(reads + syncs, 0);
		if (mode == 11) {
			UT_ASSERT(!cluster_thread_recovery_fabric_pi_contributions_v1(plan));
			cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
			continue;
		}
		resident[0] = true;
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &notice_fact));
		pi_drop_available = pi_drop_current = false;
		MyBackendType = B_BG_WRITER;
		notice_ready = true;
		UT_ASSERT(cluster_page_data_from_notice_v1((void *)1, 0, &peer));
		UT_ASSERT_EQ(cluster_page_data_bind_plan_v1(peer, plan), mode == 10);
		UT_ASSERT_EQ(cluster_page_data_pi_storage_proof_v1(peer, page, &source, 1, &observed), mode == 10);
		UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(syncs, 1);
		cluster_page_data_receipt_free_v1(&peer); cluster_page_data_receipt_free_v1(&receipt);
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
	}
}

static void
original_resident_path_and_missing_plan_remain_separate(void)
{
	ClusterWalSourceRef source;
	ClusterPcmPiStorageCutV1 cut;
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterThreadRecoveryFabricPlanV1 *plan = drop_storage_plan(0, RF_SIDE_PLAN_PI_CONTRIBUTIONS, true, &source, &cut);
	const RfPageOnlinePlanV1 *page = cluster_thread_recovery_fabric_page_plan_v1(plan);
	UT_ASSERT(!cluster_bufmgr_observe_pi_storage_v1(&target, &cut, page, &source, 1, &receipt));
	UT_ASSERT_EQ(reads + syncs, 0);
	resident[0] = true;
	pi_drop_available = false;
	UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, page, &source, 1, &receipt));
	UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(syncs, 1);
	cluster_page_data_receipt_free_v1(&receipt);
	/* A qualified resident proof does not depend on the additional DROP
	 * consumer or require a captured token in the retained SIDE graph. */
	pi_drop_available = true;
	pi_drop_request.space_mutation_token++;
	UT_ASSERT(cluster_bufmgr_observe_pi_storage_plan_v1(&target, &cut, plan, &source, 1, &receipt));
	UT_ASSERT(!pi_drop_sampled);
	UT_ASSERT_EQ(reads, 4); UT_ASSERT_EQ(syncs, 2);
	cluster_page_data_receipt_free_v1(&receipt);
	cluster_thread_recovery_fabric_plan_destroy_v1(&plan); clean();
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(complete_drop_identity_without_current_and_notice_without_second_io);
	UT_RUN(unqualified_capture_and_lifecycle_refuse_before_data_io);
	UT_RUN(unsupported_or_incomplete_typed_lifecycle_cannot_replace_resident_identity);
	UT_RUN(storage_publication_rechecks_operation_source_and_exact_cut);
	UT_RUN(notice_requires_same_complete_fact_and_lifecycle);
	UT_RUN(unsealed_or_recovery_plan_cannot_authorize_drop_identity);
	UT_RUN(data_error_and_allocation_error_release_only_the_original_call);
	UT_RUN(same_uuid_after_truncate_cannot_inherit_old_capture);
	UT_RUN(authenticated_resident_storage_fact_keeps_page_only_coverage);
	UT_RUN(authenticated_resident_fact_covers_exact_reservation_only_history);
	UT_RUN(original_resident_path_and_missing_plan_remain_separate);
	UT_DONE();
	return ut_failed_count != 0;
}
