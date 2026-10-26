/* Opt-in, read-only calls into native visibility providers.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_m2_sample);
PG_FUNCTION_INFO_V1(test_pgrac_m2_guards_clear);

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

#ifdef USE_PGRAC_CLUSTER
/* Only identity comes from the allocator. The original provider samples
 * physical bytes and native CLOG, and retains all its corroboration checks.
 * This wrapper neither creates a CTRC participant nor requests an X guard. */
static bool
sample_physical(TransactionId xid, ClusterTTStatusResult *result)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterTTSlotCurrentOwner owner;
	ClusterTTSlotCurrentOwner final_owner;
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
			if (cluster_tt_slot_current_owner_by_xid(cluster_node_id, xid, &owner)) {
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
						stage = "final_owner";
						sampled = cluster_tt_slot_current_owner_by_xid(cluster_node_id, xid,
																	   &final_owner)
								  && final_owner.segment_id == owner.segment_id
								  && final_owner.xid == owner.xid && final_owner.wrap == owner.wrap
								  && final_owner.slot_offset == owner.slot_offset;
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
				outcome = cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_VISIBILITY,
												   &resolution, &reason);
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
