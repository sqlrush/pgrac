/*-------------------------------------------------------------------------
 *
 * cluster_cr_server_policy.c
 *	  pgrac spec-6.12b — pure split policy for the CR-server (no shmem, no
 *	  locks, no elog) so cluster_unit exercises every branch standalone.
 *
 *	  See cluster_cr_server.h for the FULL / PARTIAL / DENY contract and
 *	  why an interleaved home order must refuse (write_scn-DESC peel
 *	  ordering across chains is a correctness invariant, spec-3.10 Q10).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_server_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_tt_slot.h"

ClusterCrServerSplit
cluster_cr_server_split_classify(const int32 *chain_origins, int nchains, int32 self_node,
								 int *out_prefix_len)
{
	int prefix = 0;

	if (out_prefix_len != NULL)
		*out_prefix_len = 0;
	if (nchains < 0 || (nchains > 0 && chain_origins == NULL))
		return CLUSTER_CR_SPLIT_DENY; /* malformed input: fail closed */

	/* Leading self-home run = the peelable DESC prefix. */
	while (prefix < nchains && chain_origins[prefix] == self_node)
		prefix++;

	/* Any self-home chain AFTER a foreign one = interleave = refuse. */
	for (int i = prefix; i < nchains; i++) {
		if (chain_origins[i] == self_node)
			return CLUSTER_CR_SPLIT_DENY;
	}

	if (out_prefix_len != NULL)
		*out_prefix_len = prefix;
	return (prefix == nchains) ? CLUSTER_CR_SPLIT_FULL : CLUSTER_CR_SPLIT_PARTIAL;
}

/*
 * cluster_cr_server_invalid_scn_verdict — spec-7.1 D1 serve pure decision.
 *
 *	The origin's durable by-xid scan matched our own xid but the slot carries
 *	no stamped commit_scn (the delayed-cleanout window: XID_MATCH_INVALID_SCN).
 *	Per IN-5 the real population is aborted-unstamped -- an abort writes no
 *	durable commit_scn -- so cross-checking CLOG lets us answer a provably
 *	ABORTED xid positively (invisible at the requester) instead of 53R97.
 *
 *	8.A (positive proof only): ONLY an explicit CLOG abort upgrades.  A
 *	committed-but-unstamped xid (we must never fabricate its commit_scn), an
 *	in-flight / 2PC-prepared / crashed-without-abort-record xid -- for all of
 *	which TransactionIdDidAbort is false -- stays REFUSE (fail-closed, the
 *	refuse direction is unchanged).
 */
ClusterCrInvalidScnVerdict
cluster_cr_server_invalid_scn_verdict(bool clog_did_abort)
{
	return clog_did_abort ? CLUSTER_CR_INVALID_SCN_ABORTED : CLUSTER_CR_INVALID_SCN_REFUSE;
}

ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_verdict(bool clog_did_commit, bool clog_did_abort,
									   bool xid_is_in_progress)
{
	if (clog_did_commit)
		return CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	if (clog_did_abort)
		return CLUSTER_UNDO_VERDICT_ABORTED;
	if (xid_is_in_progress)
		return CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
}

bool
cluster_cr_server_live_binding_exact(bool xid_is_mine, uint32 expected_segment_id,
									 uint32 expected_tt_slot_id, uint16 matched_segment,
									 uint16 matched_slot, bool xid_is_in_progress,
									 bool durable_binding_stable)
{
	return xid_is_mine && expected_segment_id > 0 && expected_segment_id <= UINT16_MAX
		   && expected_segment_id == (uint32)matched_segment && expected_tt_slot_id >= 1
		   && expected_tt_slot_id <= TT_SLOTS_PER_SEGMENT
		   && expected_tt_slot_id == (uint32)matched_slot + 1 && xid_is_in_progress
		   && durable_binding_stable;
}

ClusterUndoVerdictKind
cluster_cr_server_c0_zero_match_verdict(bool authoritative, bool xid_is_mine,
										uint32 expected_segment_id, uint32 expected_tt_slot_id,
										bool no_raw_reuse_window, bool clog_is_committed,
										bool clog_is_aborted, bool clog_is_in_progress,
										bool xid_is_in_progress)
{
	int raw_status_count;

	if (!authoritative || !xid_is_mine || expected_segment_id == 0
		|| expected_segment_id > UINT16_MAX || expected_tt_slot_id < 1
		|| expected_tt_slot_id > TT_SLOTS_PER_SEGMENT || !no_raw_reuse_window)
		return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;

	raw_status_count
		= (clog_is_committed ? 1 : 0) + (clog_is_aborted ? 1 : 0) + (clog_is_in_progress ? 1 : 0);
	if (raw_status_count != 1 || clog_is_committed)
		return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	if (clog_is_aborted)
		return xid_is_in_progress ? CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED
								  : CLUSTER_UNDO_VERDICT_ABORTED;
	if (clog_is_in_progress && xid_is_in_progress)
		return CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
}

ClusterUndoVerdictKind
cluster_cr_server_freshref_c1b_pair_verdict(bool pair_request, bool xid_is_mine,
											uint32 expected_segment_id, uint32 expected_tt_slot_id,
											bool no_raw_reuse_window, int raw_clog_status,
											ClusterTTDurableResolve resolve, uint16 matched_segment,
											uint16 matched_slot, SCN resolved_scn, SCN proposed_scn,
											bool retention_ok, SCN horizon_scn)
{
	if (!pair_request || !xid_is_mine || expected_segment_id == 0
		|| expected_segment_id > UINT16_MAX || expected_tt_slot_id < 1
		|| expected_tt_slot_id > TT_SLOTS_PER_SEGMENT || !no_raw_reuse_window
		|| raw_clog_status != TRANSACTION_STATUS_COMMITTED || !SCN_VALID(proposed_scn))
		return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;

	switch (resolve) {
	case CLUSTER_TT_DURABLE_RESOLVED_SCN:
		if ((uint32)matched_segment == expected_segment_id
			&& (uint32)matched_slot + 1 == expected_tt_slot_id && SCN_VALID(resolved_scn)
			&& resolved_scn == proposed_scn)
			return CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		break;
	case CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH:
		/* SCN_CMP_OK: visibility time order is the local-SCN component. */
		if (retention_ok && SCN_VALID(horizon_scn)
			&& scn_local(proposed_scn) <= scn_local(horizon_scn))
			return CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		break;
	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
	default:
		break;
	}

	return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
}

bool
cluster_cr_server_freshref_c1b_pair_request_decode(const GcsBlockForwardPayload *fwd,
												   int32 authenticated_source_node,
												   int32 local_node, uint64 current_epoch,
												   int max_backends, uint32 *segment_id,
												   TransactionId *xid, uint32 *expected_tt_slot_id,
												   SCN *proposed_scn)
{
	uint32 decoded_segment = 0;
	uint32 decoded_slot = 0;
	TransactionId decoded_xid = InvalidTransactionId;
	SCN decoded_scn;
	int i;

	if (fwd == NULL || !GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(fwd)
		|| fwd->request_id == 0 || fwd->epoch != current_epoch || authenticated_source_node < 0
		|| authenticated_source_node >= CLUSTER_MAX_NODES || local_node < 0
		|| local_node >= CLUSTER_MAX_NODES || authenticated_source_node == local_node
		|| fwd->original_requester_node != authenticated_source_node
		|| fwd->master_node != authenticated_source_node || max_backends <= 0
		|| fwd->requester_backend_id <= 0 || fwd->requester_backend_id > max_backends
		|| fwd->transition_id != (uint8)PCM_TRANS_N_TO_S)
		return false;
	for (i = 0; i < 6; i++)
		if (fwd->reserved_0[i] != 0)
			return false;
	if (!GcsBlockUndoFreshRefC1bTagDecode(fwd->tag, &decoded_segment, &decoded_xid, &decoded_slot))
		return false;
	decoded_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
	if (!SCN_VALID(decoded_scn))
		return false;

	if (segment_id != NULL)
		*segment_id = decoded_segment;
	if (xid != NULL)
		*xid = decoded_xid;
	if (expected_tt_slot_id != NULL)
		*expected_tt_slot_id = decoded_slot;
	if (proposed_scn != NULL)
		*proposed_scn = decoded_scn;
	return true;
}

#endif /* USE_PGRAC_CLUSTER */
