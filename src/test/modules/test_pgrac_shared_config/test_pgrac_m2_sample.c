/* Opt-in, read-only calls into native visibility providers.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"

#include "access/table.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_m2_sample);
PG_FUNCTION_INFO_V1(test_pgrac_m2_guards_clear);
PG_FUNCTION_INFO_V1(test_pgrac_m2_limits);

static void
check_reader(void)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("native visibility diagnostics require superuser")));
#ifdef USE_PGRAC_CLUSTER
	if (!cluster_trace_tx_identity)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native visibility diagnostics require trace_tx_identity")));
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("native visibility diagnostics require --enable-cluster")));
#endif
}

Datum
test_pgrac_m2_limits(PG_FUNCTION_ARGS)
{
	check_reader();
#ifdef USE_PGRAC_CLUSTER
	{
		StringInfoData out;

		initStringInfo(&out);
		appendStringInfo(
			&out,
			"{\"segment_bytes\":%u,\"inline_capacity_bytes\":%d,"
			"\"update_payload_header_bytes\":%u,\"record_header_bytes\":%u,"
			"\"history_trailer_bytes\":%u,\"record_hard_cap_bytes\":%u,\"authoritative\":false}",
			(uint32)UNDO_SEGMENT_SIZE_BYTES, cluster_undo_record_inline_max_bytes,
			(uint32)sizeof(UndoUpdatePayload), (uint32)sizeof(UndoRecordHeader),
			(uint32)sizeof(UndoItlHistoryTrailer), (uint32)UNDO_RECORD_HARD_CAP_BYTES);
		PG_RETURN_TEXT_P(cstring_to_text(out.data));
	}
#else
	PG_RETURN_NULL();
#endif
}

#ifdef USE_PGRAC_CLUSTER
/* Begin physical probe. */
typedef struct M2PhysicalLocateScope {
	FullTransactionId full_xid;
	uint64 formation_epoch;
	uint64 system_identifier;
	uint64 boot_incarnation;
	uint32 current_segment;
	int origin_node_id;
} M2PhysicalLocateScope;

/* The durable locator supplies identity, never an outcome. Keep the same
 * raw-xid interpretation bracket as the runtime's local physical lookup.
 * No native lock spans the scan or a current-guard wait. */
static bool
sample_physical_scope_capture(TransactionId xid, const ClusterSemanticAdmissionToken *admission,
							  M2PhysicalLocateScope *scope)
{
	FullTransactionId next;
	TransactionId oldest_clog;

	if (!TransactionIdIsNormal(xid) || !cluster_semantic_activation_recheck(admission))
		return false;
	next = ReadNextFullTransactionId();
	if (!FullTransactionIdIsValid(next) || EpochFromFullTransactionId(next) != 0
		|| xid >= XidFromFullTransactionId(next))
		return false;
	LWLockAcquire(XactTruncationLock, LW_SHARED);
	oldest_clog = ShmemVariableCache->oldestClogXid;
	LWLockRelease(XactTruncationLock);
	if (!TransactionIdIsNormal(oldest_clog) || xid < oldest_clog
		|| cluster_xid_origin_slot(xid) != cluster_node_id)
		return false;

	memset(scope, 0, sizeof(*scope));
	scope->full_xid = FullTransactionIdFromEpochAndXid(0, xid);
	scope->formation_epoch = cluster_epoch_get_current();
	scope->system_identifier = GetSystemIdentifier();
	scope->boot_incarnation = cluster_qvotec_get_self_incarnation();
	scope->current_segment = cluster_tt_slot_current_segment(cluster_node_id);
	scope->origin_node_id = cluster_node_id;
	return scope->formation_epoch == admission->formation_epoch
		   && scope->formation_epoch <= UINT32_MAX && scope->system_identifier != 0
		   && scope->boot_incarnation != 0 && scope->current_segment != 0
		   && cluster_semantic_activation_recheck(admission);
}

static bool
sample_physical_scope_current(TransactionId xid, const ClusterSemanticAdmissionToken *admission,
							  const M2PhysicalLocateScope *expected)
{
	M2PhysicalLocateScope current;

	return sample_physical_scope_capture(xid, admission, &current)
		   && FullTransactionIdEquals(current.full_xid, expected->full_xid)
		   && current.formation_epoch == expected->formation_epoch
		   && current.system_identifier == expected->system_identifier
		   && current.boot_incarnation == expected->boot_incarnation
		   && current.current_segment == expected->current_segment
		   && current.origin_node_id == expected->origin_node_id;
}

/* The original provider samples physical bytes and native CLOG, and retains
 * all corroboration checks. This wrapper neither creates a CTRC participant
 * nor requests an X guard. All locator state belongs to this one call. */
static bool
sample_physical(TransactionId xid, ClusterTTStatusResult *result)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterTTSlotCurrentOwner owner;
	ClusterTTSlotCurrentOwner final_owner;
	M2PhysicalLocateScope locate_scope;
	ClusterTTSlotPhysicalLocator locator = { 0 };
	ClusterTTStatusKey key;
	ClusterUndoBlock0LogicalKey logical = { 0 };
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result failure;
	volatile bool started = false;
	bool sampled = false;
	bool physical_active = false;
	bool located_physical = false;
	bool current_owner;
	const char *stage = "origin";

	if (cluster_xid_origin_slot(xid) != cluster_node_id)
		goto done;
	cluster_runtime_visibility_ensure_exit_hooks();
	cluster_undo_block0_current_ensure_exit_hooks();
	PG_TRY();
	{
		stage = "admission";
		if (cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											  CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
			== CLUSTER_SEMANTIC_ADMISSION_OK) {
			stage = "owner";
			current_owner = cluster_tt_slot_current_owner_by_xid(cluster_node_id, xid, &owner);
			if (!current_owner) {
				uint16 segment = 0;
				uint16 slot = TT_SLOTS_PER_SEGMENT;
				uint16 wrap = TT_WRAP_INVALID;

				stage = "locate_scope";
				if (sample_physical_scope_capture(xid, &admission, &locate_scope)) {
					stage = "locate";
					if (cluster_tt_slot_durable_locate_any_by_xid_origin(
							cluster_node_id, xid, &segment, &slot, &wrap, NULL)
							== CLUSTER_TT_DURABLE_LOCATE_FOUND
						&& segment != 0 && slot < TT_SLOTS_PER_SEGMENT && wrap != TT_WRAP_INVALID) {
						stage = "locate_recheck";
						located_physical
							= sample_physical_scope_current(xid, &admission, &locate_scope);
						memset(&owner, 0, sizeof(owner));
						owner.segment_id = segment;
						owner.xid = xid;
						owner.slot_offset = slot;
						owner.wrap = wrap;
					}
				}
			}
			if (current_owner || located_physical) {
				logical.owner_instance = (uint8)(cluster_node_id + 1);
				logical.segment_id = owner.segment_id;
				locator.segment_id = owner.segment_id;
				locator.xid = xid;
				locator.slot_offset = owner.slot_offset;
				locator.wrap = owner.wrap;
				stage = "root";
				if (cluster_semantic_activation_resolve_shared_undo_root(
						&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
						logical.segment_id, &root)) {
					started = true;
					stage = "acquire";
					step = cluster_undo_block0_current_acquire_begin_admitted(
						&logical, CLUSTER_UNDO_BLOCK0_SCUR, 0, &admission, &guard, &failure);
					while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
						CHECK_FOR_INTERRUPTS();
						step = cluster_undo_block0_current_acquire_poll(&guard, &failure);
						if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
							&& !cluster_undo_block0_current_wait_reply(
								&guard, CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_ACQUIRE))
							pg_usleep(1000L);
					}
					if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
						stage = "provider";
						sampled = cluster_runtime_visibility_physical_locator_sample_held(
							&locator, &admission, &guard, &root, &key, result, &physical_active,
							NULL);
						if (sampled && located_physical) {
							stage = "sample_scope";
							sampled = sample_physical_scope_current(xid, &admission, &locate_scope);
						}
						if (sampled)
							stage = "release";
						step = cluster_undo_block0_current_release_begin(&guard, &failure);
						while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
							CHECK_FOR_INTERRUPTS();
							step = cluster_undo_block0_current_release_poll(&guard, &failure);
							if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
								&& !cluster_undo_block0_current_wait_reply(
									&guard, CLUSTER_UNDO_BLOCK0_WAIT_RUNTIME_VIS_RELEASE))
								pg_usleep(1000L);
						}
						sampled = sampled && step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
					}
					/* Release can wait. Keep the identity/root/admission fences. */
					if (sampled) {
						if (located_physical) {
							stage = "final_scope";
							sampled = sample_physical_scope_current(xid, &admission, &locate_scope);
						} else {
							stage = "final_owner";
							sampled = cluster_tt_slot_current_owner_by_xid(cluster_node_id, xid,
																		   &final_owner)
									  && final_owner.segment_id == owner.segment_id
									  && final_owner.xid == owner.xid
									  && final_owner.wrap == owner.wrap
									  && final_owner.slot_offset == owner.slot_offset;
						}
					}
					if (sampled) {
						stage = "final_root";
						sampled = cluster_semantic_activation_resolve_shared_undo_root(
									  &admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
									  logical.owner_instance, logical.segment_id, &final_root)
								  && root.intent == final_root.intent
								  && root.root_id == final_root.root_id
								  && root.root_generation == final_root.root_generation;
					}
					if (sampled) {
						stage = "final_admission";
						sampled = cluster_semantic_activation_recheck(&admission);
					}
				}
			}
		}
	}
	PG_FINALLY();
	{
		if (started)
			cluster_undo_block0_current_cancel(&guard);
		if (admission.entered)
			cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();
	if (sampled)
		stage = "complete";
done:
	/* Probe-local diagnostics, never a published status or provider-entry proof. */
	elog(LOG, "M2_PROBE path=physical xid=%u stage=%s sampled=%d authoritative=false", xid, stage,
		 sampled);
	return sampled;
}
/* End physical probe. */

static ClusterTxOutcome
sample_origin(const ClusterTxLocator *locator, ClusterTxResolution *resolution,
			  ClusterTxResolveReason *reason)
{
	ClusterTxLocator request = *locator;

	/* As in the heap's partial visibility callers, page ITL wrap identifies
	 * the page slot, not the canonical TT incarnation. Only the original
	 * exact origin proof supplies TT wrap. The resolver checks the complete
	 * UBA/xid/ITL echo, native proof and current admission before returning. */
	if (locator->tt_wrap == TT_WRAP_INVALID) {
		memset(resolution, 0, sizeof(*resolution));
		*reason = CLUSTER_TX_RESOLVE_WRAP_MISMATCH;
		return CLUSTER_TX_UNKNOWN;
	}
	request.tt_wrap = TT_WRAP_INVALID;
	return cluster_tx_resolve_exact(&request, CLUSTER_TX_RESOLVE_VISIBILITY, resolution, reason);
}

static bool
page_locator(Oid relid, int32 block, TransactionId xid, ClusterTxLocator *locator)
{
	Relation rel;
	Buffer buffer;
	PGAlignedBlock image;
	ClusterTxResolveReason reason;

	if (block < 0)
		return false;
	rel = table_open(relid, AccessShareLock);
	if (rel->rd_rel->relkind != RELKIND_RELATION
		|| rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("native visibility probe requires a permanent heap")));
	buffer = ReadBuffer(rel, (BlockNumber)block);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	memcpy(image.data, BufferGetPage(buffer), BLCKSZ);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);
	/* No buffer content lock or pin spans a provider wait. */
	for (uint8 i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		if (cluster_tx_locator_from_itl(image.data, i, locator, &reason) && locator->xid == xid)
			return true;
	return false;
}
#endif

Datum
test_pgrac_m2_sample(PG_FUNCTION_ARGS)
{
	check_reader();
#ifdef USE_PGRAC_CLUSTER
	{
		char *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
		TransactionId xid = PG_GETARG_TRANSACTIONID(3);
		ClusterTTStatusKey key;
		ClusterTTStatusResult result = { 0 };
		ClusterTxLocator locator = { 0 };
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_BAD_LOCATOR;
		ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
		SCN scn = InvalidScn;
		bool sampled = false;
		bool locator_found = false;
		StringInfoData out;

		if (strcmp(path, "owner") == 0)
			sampled = cluster_runtime_visibility_current_owner_lookup_exact(xid, &key, &result);
		else if (strcmp(path, "physical") == 0)
			sampled = sample_physical(xid, &result);
		else if (strcmp(path, "origin") == 0) {
			locator_found = page_locator(PG_GETARG_OID(1), PG_GETARG_INT32(2), xid, &locator);
			if (locator_found) {
				outcome = sample_origin(&locator, &resolution, &reason);
				if (outcome != CLUSTER_TX_UNKNOWN)
					scn = resolution.commit_scn;
			}
		} else
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("native visibility path must be owner, physical or origin")));
		if (sampled && result.authoritative) {
			if (result.status == CLUSTER_TT_STATUS_IN_PROGRESS)
				outcome = CLUSTER_TX_IN_PROGRESS;
			else if (result.status == CLUSTER_TT_STATUS_COMMITTED)
				outcome = CLUSTER_TX_COMMITTED;
			else if (result.status == CLUSTER_TT_STATUS_ABORTED)
				outcome = CLUSTER_TX_ABORTED;
			scn = result.commit_scn;
		}
		elog(LOG,
			 "M2_PROBE path=%s xid=%u phase=RETURN pid=%d sampled=%d outcome=%d reason=%d "
			 "locator_found=%d locator_uba0=" UINT64_FORMAT " locator_uba1=" UINT64_FORMAT
			 " locator_wrap=%u authoritative=false",
			 path, xid, MyProcPid, sampled, outcome, reason, locator_found, locator.uba.raw[0],
			 locator.uba.raw[1], locator.tt_wrap);
		initStringInfo(&out);
		appendStringInfo(&out,
						 "{\"outcome\":%d,\"scn\":" UINT64_FORMAT
						 ",\"reason\":%d,\"pid\":%d,\"guard_clear\":%s,\"authoritative\":false}",
						 outcome, (uint64)scn, reason, MyProcPid,
						 cluster_undo_block0_current_backend_has_guards() ? "false" : "true");
		PG_RETURN_TEXT_P(cstring_to_text(out.data));
	}
#else
	PG_RETURN_NULL();
#endif
}

Datum
test_pgrac_m2_guards_clear(PG_FUNCTION_ARGS)
{
	check_reader();
#ifdef USE_PGRAC_CLUSTER
	PG_RETURN_BOOL(!cluster_undo_block0_current_backend_has_guards());
#else
	PG_RETURN_NULL();
#endif
}
