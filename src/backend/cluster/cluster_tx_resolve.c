/*-------------------------------------------------------------------------
 *
 * cluster_tx_resolve.c
 *	  Exact transaction identity and outcome entry points for R4.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tx_resolve.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/multixact.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tx_resolve.h"

static bool
cluster_tx_locator_is_well_formed(const ClusterTxLocator *locator,
								  bool allow_partial_wrap,
								  ClusterTxResolveReason *reason_out)
{
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	bool data_kind;

	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_BAD_LOCATOR;
	if (locator == NULL)
		return false;

	data_kind = locator->itl_kind == ITL_FLAG_ACTIVE
				|| locator->itl_kind == ITL_FLAG_COMMITTED
				|| locator->itl_kind == ITL_FLAG_ABORTED
				|| locator->itl_kind == ITL_FLAG_NEEDS_CLEANOUT;
	if (locator->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| (!data_kind && !ITL_FLAG_IS_LOCK_ONLY(locator->itl_kind)))
		return false;
	if (!TransactionIdIsNormal(locator->xid)) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_XID_MISMATCH;
		return false;
	}
	if (locator->tt_wrap == TT_WRAP_INVALID && !allow_partial_wrap) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_WRAP_MISMATCH;
		return false;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset, &row_offset)
		|| block_no == 0) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_BAD_UBA;
		return false;
	}

	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

static bool
cluster_tx_resolve_reason_is_known(ClusterTxResolveReason reason)
{
	return (unsigned int)reason <= (unsigned int)CLUSTER_TX_RESOLVE_PROTOCOL;
}

static bool
cluster_tx_zero_epoch_terminal_census_is_admissible(
	const ClusterTxLocator *locator,
	const ClusterSemanticAdmissionToken *admission,
	bool caller_owned_terminal_census)
{
	NodeId origin = uba_origin_node_id(locator->uba);
	int node_count = cluster_conf_node_count();
	bool generation_admissible;
	bool topology_admissible;

	topology_admissible
		= (node_count == 1 && origin == (NodeId)cluster_node_id)
		  || (node_count == 4 && origin != InvalidNodeId);
	/* The single-node sentinel exists only before the first PGSA record.
	 * The approved homogeneous four-node clean-formation path instead binds
	 * whatever record generation the caller-owned admission currently holds;
	 * its exact-current check below is the freshness proof. */
	generation_admissible
		= (node_count == 1 && admission->record_generation == 0)
		  || node_count == 4;
	return caller_owned_terminal_census
		   && generation_admissible
		   && cluster_storage_mode_enabled()
		   && topology_admissible
		   && !cluster_recmerge_window_active
		   && cluster_epoch_get_current() == 0
		   && cluster_semantic_activation_recheck_r4_terminal_census(admission);
}

/* Freshref §3.1 narrow override: status-22 reaches this resolver only with
 * the caller-selected physical locator whose wrap is intentionally partial.
 * Unlike terminal census it must own an ordinary current TARGET admission,
 * and is admitted only for the homogeneous four-node clean formation. */
static bool
cluster_tx_zero_epoch_partial_visibility_is_admissible(
	const ClusterTxLocator *locator,
	const ClusterSemanticAdmissionToken *admission)
{
	NodeId origin = uba_origin_node_id(locator->uba);

	return cluster_conf_node_count() == 4
		   && origin != InvalidNodeId
		   && admission->record_generation != 0
		   && cluster_storage_mode_enabled()
		   && !cluster_recmerge_window_active
		   && cluster_epoch_get_current() == 0
		   && cluster_semantic_activation_recheck(admission);
}

static bool
cluster_tx_resolution_is_publishable(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
									 uint64 formation_epoch, ClusterTxOutcome returned,
									 const ClusterTxResolution *resolution,
									 ClusterTxResolveReason provider_reason)
{
	if (returned == CLUSTER_TX_UNKNOWN || returned != resolution->outcome
		|| provider_reason != CLUSTER_TX_RESOLVE_NONE
		|| !cluster_tx_locator_reply_matches(locator, &resolution->locator_echo)
		|| !TransactionIdIsNormal(resolution->top_xid)
		|| !cluster_tx_outcome_proof_is_valid(resolution->outcome, resolution->proof_kind)
		|| resolution->authority.origin_epoch != formation_epoch
		|| !SCN_VALID(resolution->authority.authority_scn))
		return false;

	if (resolution->outcome == CLUSTER_TX_COMMITTED) {
		if (!SCN_VALID(resolution->commit_scn))
			return false;
	} else if (SCN_VALID(resolution->commit_scn))
		return false;

	/* Hint cleanout and bounded terminal census may publish only an
	 * irreversible terminal result. */
	if ((mode == CLUSTER_TX_RESOLVE_CLEANOUT_HINT
		 || mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)
		&& resolution->outcome != CLUSTER_TX_COMMITTED
		&& resolution->outcome != CLUSTER_TX_ABORTED)
		return false;

	return true;
}

bool
cluster_tx_locator_from_itl(Page page, uint8 slot_index, ClusterTxLocator *out,
							ClusterTxResolveReason *reason_out)
{
	ClusterItlSlotData *slot;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	bool data_slot;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_BAD_LOCATOR;

	if (page == NULL || out == NULL || !PageHasItl(page)
		|| PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE)
		return false;
	if (slot_index >= CLUSTER_ITL_INITRANS_DEFAULT) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_SLOT_MISMATCH;
		return false;
	}

	slot = &ClusterPageGetItlSlots(page)[slot_index];
	data_slot = slot->flags == ITL_FLAG_ACTIVE || slot->flags == ITL_FLAG_COMMITTED
				|| slot->flags == ITL_FLAG_ABORTED || slot->flags == ITL_FLAG_NEEDS_CLEANOUT;
	if (!data_slot && !ITL_FLAG_IS_LOCK_ONLY(slot->flags))
		return false;
	if (!TransactionIdIsNormal(slot->xid)) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_XID_MISMATCH;
		return false;
	}
	if (slot->wrap == TT_WRAP_INVALID) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_WRAP_MISMATCH;
		return false;
	}
	if (!uba_decode(slot->undo_segment_head, &segment_id, &block_no, &tt_slot_offset,
					&row_offset)) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_BAD_UBA;
		return false;
	}

	out->uba = slot->undo_segment_head;
	out->xid = slot->xid;
	out->tt_wrap = slot->wrap;
	out->itl_kind = slot->flags;
	out->itl_slot_index = slot_index;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

bool
cluster_tx_locator_from_itl_terminal_census(
	Page page, uint8 slot_index, ClusterTxLocator *out,
	ClusterTxResolveReason *reason_out)
{
	if (!cluster_tx_locator_from_itl(page, slot_index, out, reason_out))
		return false;

	/* M4 alone keeps the page wrap in its 48-byte slot witness.  Candidate-2
	 * derives the durable canonical wrap exactly once from the origin record. */
	out->tt_wrap = TT_WRAP_INVALID;
	return true;
}

static ClusterTxOutcome
cluster_tx_resolve_exact_with_admission(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out, bool caller_owned_terminal_census,
	SCN retained_commit_scn)
{
	ClusterTxResolution candidate;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterTxResolveReason provider_reason = CLUSTER_TX_RESOLVE_NONE;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	uint64 formation_epoch;
	bool terminal_census = mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;
	bool partial_visibility
		= mode == CLUSTER_TX_RESOLVE_VISIBILITY
		  && locator != NULL && locator->tt_wrap == TT_WRAP_INVALID;
	bool clean_formation_row_wait = false;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_PROTOCOL;
	memset(&candidate, 0, sizeof(candidate));

	if (admission == NULL || !admission->entered
		|| admission->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| admission->side != CLUSTER_SEMANTIC_TARGET_SIDE || out == NULL
		|| (unsigned int)mode
			   > (unsigned int)CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)
		goto done;
	if (!cluster_tx_locator_is_well_formed(
			locator, terminal_census || partial_visibility, &reason))
		goto done;

	formation_epoch = admission->formation_epoch;
	clean_formation_row_wait = mode == CLUSTER_TX_RESOLVE_ROW_WAIT && formation_epoch == 0;
	if (formation_epoch == 0) {
		bool zero_epoch_admissible
			= terminal_census ? cluster_tx_zero_epoch_terminal_census_is_admissible(
									locator, admission, caller_owned_terminal_census)
							  : (partial_visibility || clean_formation_row_wait)
									&& cluster_tx_zero_epoch_partial_visibility_is_admissible(
										locator, admission);

		if (!zero_epoch_admissible) {
			reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
			goto done;
		}
	} else if (cluster_epoch_get_current() != formation_epoch) {
		reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
		goto done;
	}
	if (clean_formation_row_wait) {
		ClusterTxLocator request = *locator;

		/* The existing origin channel accepts a partial request, not a new
		 * canonical identity. Preserve the complete caller locator and compare
		 * the returned canonical echo against it below, including TT wrap.
		 * The input validator above still rejects partial ROW_WAIT callers. */
		request.tt_wrap = TT_WRAP_INVALID;
		outcome = cluster_runtime_visibility_resolve_exact_origin_admitted(
			&request, CLUSTER_TX_RESOLVE_VISIBILITY, admission, &candidate, &provider_reason);
	} else if (terminal_census || partial_visibility)
		outcome = cluster_runtime_visibility_resolve_exact_origin_admitted(
			locator, mode, admission, &candidate, &provider_reason);
	else
		outcome = cluster_runtime_visibility_resolve_exact_origin(
			locator, mode, formation_epoch, &candidate, &provider_reason);
	if (outcome == CLUSTER_TX_UNKNOWN && terminal_census
		&& locator->itl_kind == ITL_FLAG_NEEDS_CLEANOUT
		&& SCN_VALID(retained_commit_scn))
	{
		memset(&candidate, 0, sizeof(candidate));
		provider_reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		outcome
			= cluster_runtime_visibility_resolve_terminal_census_retained_exact(
				locator, retained_commit_scn, admission, &candidate,
				&provider_reason);
	}
	if (outcome == CLUSTER_TX_UNKNOWN) {
		reason = provider_reason == CLUSTER_TX_RESOLVE_NONE
					 || !cluster_tx_resolve_reason_is_known(provider_reason)
			 ? CLUSTER_TX_RESOLVE_PROTOCOL
			 : provider_reason;
		goto done;
	}
	if (!cluster_tx_resolution_is_publishable(locator, mode, formation_epoch,
										  outcome, &candidate, provider_reason)) {
		outcome = CLUSTER_TX_UNKNOWN;
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		goto done;
	}
	if (cluster_epoch_get_current() != formation_epoch
		|| (terminal_census
				? !cluster_semantic_activation_recheck_r4_terminal_census(admission)
				: !cluster_semantic_activation_recheck(admission))) {
		outcome = CLUSTER_TX_UNKNOWN;
		reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
		goto done;
	}

	*out = candidate;
	reason = CLUSTER_TX_RESOLVE_NONE;

done:
	switch (outcome) {
		case CLUSTER_TX_UNKNOWN:
			cluster_r4_observe(CLUSTER_R4_EVENT_TX_UNKNOWN, reason,
						   CLUSTER_CR_BUILD_NONE);
			break;
		case CLUSTER_TX_IN_PROGRESS:
			cluster_r4_observe(CLUSTER_R4_EVENT_TX_IN_PROGRESS, reason,
						   CLUSTER_CR_BUILD_NONE);
			break;
		case CLUSTER_TX_PREPARED:
			cluster_r4_observe(CLUSTER_R4_EVENT_TX_PREPARED, reason,
						   CLUSTER_CR_BUILD_NONE);
			break;
		case CLUSTER_TX_COMMITTED:
			cluster_r4_observe(CLUSTER_R4_EVENT_TX_COMMITTED, reason,
						   CLUSTER_CR_BUILD_NONE);
			break;
		case CLUSTER_TX_ABORTED:
			cluster_r4_observe(CLUSTER_R4_EVENT_TX_ABORTED, reason,
						   CLUSTER_CR_BUILD_NONE);
			break;
	}
	if (reason_out != NULL)
		*reason_out = reason;
	return outcome;
}

ClusterTxOutcome
cluster_tx_resolve_exact_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	if (mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_PROTOCOL;
		return CLUSTER_TX_UNKNOWN;
	}
	return cluster_tx_resolve_exact_with_admission(
		locator, mode, admission, out, reason_out, true, InvalidScn);
}

ClusterTxOutcome
cluster_tx_resolve_terminal_census_retained_admitted(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	if (!SCN_VALID(retained_commit_scn))
	{
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_PROTOCOL;
		return CLUSTER_TX_UNKNOWN;
	}
	return cluster_tx_resolve_exact_with_admission(
		locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, admission, out,
		reason_out, true, retained_commit_scn);
}

void
cluster_tx_resolve_terminal_census_batch_preflight(void)
{
	cluster_runtime_visibility_ensure_exit_hooks();
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
						 ClusterTxResolution *out,
						 ClusterTxResolveReason *reason_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	bool terminal_census = mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	memset(&admission, 0, sizeof(admission));
	if (terminal_census)
		admission_result
			= cluster_semantic_activation_enter_r4_terminal_census(&admission);
	else
		admission_result = cluster_semantic_activation_enter(
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		reason = admission_result == CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
				 ? CLUSTER_TX_RESOLVE_TARGET_DISABLED
				 : CLUSTER_TX_RESOLVE_RF_DEFERRED;
		goto done;
	}

	PG_TRY();
	{
		outcome = cluster_tx_resolve_exact_with_admission(
			locator, mode, &admission, out, &reason, false, InvalidScn);
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();

done:
	if (reason_out != NULL)
		*reason_out = reason;
	return outcome;
}

ClusterTxOutcome
cluster_tx_resolve_multixact(MultiXactId mxid, ClusterMultiResolution *out,
							 ClusterTxResolveReason *reason_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	int attempt;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	memset(&admission, 0, sizeof(admission));

	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		reason = admission_result == CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
				 ? CLUSTER_TX_RESOLVE_TARGET_DISABLED
				 : CLUSTER_TX_RESOLVE_RF_DEFERRED;
		goto done;
	}

	PG_TRY();
	{
		if (out == NULL) {
			reason = CLUSTER_TX_RESOLVE_PROTOCOL;
			goto admitted_done;
		}
		if (!MultiXactIdIsValid(mxid)) {
			reason = CLUSTER_TX_RESOLVE_BAD_COMPOSITION;
			goto admitted_done;
		}

		for (attempt = 0; attempt < 2; attempt++) {
			MultiXactMember *first = NULL;
			MultiXactMember *second = NULL;
			MultiXactOffset first_start = 0;
			MultiXactOffset second_start = 0;
			int first_count;
			int second_count;
			bool first_valid;
			bool second_valid;
			bool stable;
			int i;

			first_count = GetMultiXactIdMembersWithOffset(mxid, &first, false, false,
												 &first_start);
			second_count = GetMultiXactIdMembersWithOffset(mxid, &second, false, false,
												  &second_start);

			first_valid = first_start != 0 && first_count >= 2
						  && first_count <= CLUSTER_R4_MAX_MULTI_MEMBERS && first != NULL;
			second_valid = second_start != 0 && second_count >= 2
						   && second_count <= CLUSTER_R4_MAX_MULTI_MEMBERS && second != NULL;
			if (first_valid) {
				for (i = 0; i < first_count; i++) {
					if (!TransactionIdIsNormal(first[i].xid)
						|| first[i].status < MultiXactStatusForKeyShare
						|| first[i].status > MaxMultiXactStatus) {
						first_valid = false;
						break;
					}
				}
			}
			if (second_valid) {
				for (i = 0; i < second_count; i++) {
					if (!TransactionIdIsNormal(second[i].xid)
						|| second[i].status < MultiXactStatusForKeyShare
						|| second[i].status > MaxMultiXactStatus) {
						second_valid = false;
						break;
					}
				}
			}

			stable = first_valid && second_valid
					 && cluster_multixact_native_snapshot_equal(
							first_start, first_count, first, second_start, second_count, second);
			if (first != NULL)
				pfree(first);
			if (second != NULL)
				pfree(second);

			if (!first_valid || !second_valid) {
				reason = CLUSTER_TX_RESOLVE_BAD_COMPOSITION;
				break;
			}
			if (stable) {
				reason = CLUSTER_TX_RESOLVE_COVERAGE_GAP;
				break;
			}
			reason = CLUSTER_TX_RESOLVE_COMPOSITION_CHANGED;
		}

admitted_done:
		;
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();

done:
	if (reason_out != NULL)
		*reason_out = reason;
	return CLUSTER_TX_UNKNOWN;
}

const char *
cluster_tx_resolve_reason_name(ClusterTxResolveReason reason)
{
	switch (reason) {
	case CLUSTER_TX_RESOLVE_NONE:
		return "none";
	case CLUSTER_TX_RESOLVE_TARGET_DISABLED:
		return "target_disabled";
	case CLUSTER_TX_RESOLVE_RF_DEFERRED:
		return "rf_deferred";
	case CLUSTER_TX_RESOLVE_BAD_LOCATOR:
		return "bad_locator";
	case CLUSTER_TX_RESOLVE_BAD_UBA:
		return "bad_uba";
	case CLUSTER_TX_RESOLVE_XID_MISMATCH:
		return "xid_mismatch";
	case CLUSTER_TX_RESOLVE_WRAP_MISMATCH:
		return "wrap_mismatch";
	case CLUSTER_TX_RESOLVE_SLOT_MISMATCH:
		return "slot_mismatch";
	case CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE:
		return "authority_unavailable";
	case CLUSTER_TX_RESOLVE_AUTHORITY_STALE:
		return "authority_stale";
	case CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT:
		return "authority_conflict";
	case CLUSTER_TX_RESOLVE_COVERAGE_GAP:
		return "coverage_gap";
	case CLUSTER_TX_RESOLVE_SUBTRANS_CHANGED:
		return "subtrans_changed";
	case CLUSTER_TX_RESOLVE_SUBTRANS_CYCLE:
		return "subtrans_cycle";
	case CLUSTER_TX_RESOLVE_SUBTRANS_DEPTH:
		return "subtrans_depth";
	case CLUSTER_TX_RESOLVE_TWOPHASE_CONFLICT:
		return "twophase_conflict";
	case CLUSTER_TX_RESOLVE_BAD_COMPOSITION:
		return "bad_composition";
	case CLUSTER_TX_RESOLVE_COMPOSITION_CHANGED:
		return "composition_changed";
	case CLUSTER_TX_RESOLVE_HORIZON_RECYCLED:
		return "horizon_recycled";
	case CLUSTER_TX_RESOLVE_HOLDER_MOVED:
		return "holder_moved";
	case CLUSTER_TX_RESOLVE_CAPACITY:
		return "capacity";
	case CLUSTER_TX_RESOLVE_TIMEOUT:
		return "timeout";
	case CLUSTER_TX_RESOLVE_CANCELLED:
		return "cancelled";
	case CLUSTER_TX_RESOLVE_REENTRANT:
		return "reentrant";
	case CLUSTER_TX_RESOLVE_IO_ERROR:
		return "io_error";
	case CLUSTER_TX_RESOLVE_PROTOCOL:
		return "protocol";
	default:
		return "invalid_reason";
	}
}

#endif /* USE_PGRAC_CLUSTER */
