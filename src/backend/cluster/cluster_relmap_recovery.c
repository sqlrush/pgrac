/*-------------------------------------------------------------------------
 * cluster_relmap_recovery.c
 *    Original native issuance inputs for relmap recovery.
 *
 * These observations are volatile and belong to the current native owner.
 * A checkpoint/cold source needs its own persisted base and terminal seal;
 * neither a legal future allocation limit nor native_side projection pages
 * supply that qualification. No consumer is activated here.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_relmap_recovery.h"
#include "cluster/cluster_relmap_owner.h"
#include "storage/lwlock.h"
#include "miscadmin.h"

void
cluster_relmap_recovery_issued_publish(FullTransactionId issued)
{
	VariableCache cache = ShmemVariableCache;

	Assert(LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE));
	if (cache == NULL || cache->clusterIssuedState == 2)
		return;
	if (!TransactionIdIsNormal(XidFromFullTransactionId(issued))
		|| (cache->clusterIssuedState != 0
			&& (cache->clusterIssuedState != 1
				|| !FullTransactionIdFollows(issued, cache->clusterLastIssuedXid)))) {
		cache->clusterIssuedState = 2;
		return;
	}
	cache->clusterLastIssuedXid = issued;
	cache->clusterIssuedState = 1;
}

/* Owner/fence queries must remain outside both native locks. */
static bool
relmap_recovery_owner_ready(const ClusterRelmapOwnerSampleV2 *owner)
{
	const ClusterControlRootIdentity *id;

	if (owner == NULL || ShmemVariableCache == NULL || LWLockHeldByMe(XactTruncationLock)
		|| LWLockHeldByMe(XidGenLock) || !cluster_relmap_owner_terminal_matches_v2(owner))
		return false;
	id = &owner->writer.ref.claim.identity;
	return id->system_identifier != 0 && owner->writer.ref.claim.database_incarnation != 0
		   && id->origin_node_id >= 0 && id->origin_node_id == owner->owner.node
		   && owner->owner.admitted_incarnation == id->origin_owner_incarnation
		   && owner->owner.boot_incarnation != 0 && owner->writer.epoch != 0
		   && owner->owner.membership_epoch == owner->writer.epoch;
}

/* Caller holds the original arbitrary-CLOG-lookup truncation lock. Sampling
 * issuance needs XidGenLock only for the native metadata copy, never CLOG I/O. */
static bool
relmap_recovery_bounds_locked(const ClusterRelmapOwnerSampleV2 *owner,
							  ClusterRelmapRecoveryNativeBounds *out)
{
	ClusterRelmapRecoveryNativeBounds result = { 0 };
	const ClusterControlRootIdentity *id = &owner->writer.ref.claim.identity;
	uint64 last, next, distance;
	TransactionId oldest;
	uint8 state;

	Assert(LWLockHeldByMe(XactTruncationLock));
	LWLockAcquire(XidGenLock, LW_SHARED);
	oldest = ShmemVariableCache->oldestClogXid;
	last = U64FromFullTransactionId(ShmemVariableCache->clusterLastIssuedXid);
	next = U64FromFullTransactionId(ShmemVariableCache->nextXid);
	state = ShmemVariableCache->clusterIssuedState;
	LWLockRelease(XidGenLock);

	if (state != 1 || !TransactionIdIsNormal((TransactionId)last)
		|| !TransactionIdIsNormal((TransactionId)next) || last >= next
		|| !TransactionIdIsNormal(oldest))
		return false;
	distance = (uint32)((TransactionId)last - oldest);
	if (distance >= UINT64CONST(0x80000000) || distance > last)
		return false;
	result.native_floor = last - distance;
	result.last_issued = last;
	if (owner->owner.top_full_xid < result.native_floor
		|| owner->owner.top_full_xid > result.last_issued)
		return false;
	result.system_identifier = id->system_identifier;
	result.database_incarnation = owner->writer.ref.claim.database_incarnation;
	result.origin = owner->owner.node;
	result.membership_epoch = owner->owner.membership_epoch;
	result.admitted_incarnation = owner->owner.admitted_incarnation;
	result.boot_incarnation = owner->owner.boot_incarnation;
	*out = result;
	return true;
}

bool
cluster_relmap_recovery_native_bounds(const ClusterRelmapOwnerSampleV2 *owner,
									  ClusterRelmapRecoveryNativeBounds *out)
{
	ClusterRelmapRecoveryNativeBounds result = { 0 };
	bool valid;

	if (out == NULL)
		return false;
	if (owner != NULL) {
		uintptr_t x = (uintptr_t)owner, y = (uintptr_t)out;
		if (x <= y ? y - x < sizeof(*owner) : x - y < sizeof(*out))
			return false;
	}
	memset(out, 0, sizeof(*out));
	if (!relmap_recovery_owner_ready(owner))
		return false;
	LWLockAcquire(XactTruncationLock, LW_SHARED);
	valid = relmap_recovery_bounds_locked(owner, &result);
	LWLockRelease(XactTruncationLock);
	if (!valid || !cluster_relmap_owner_terminal_matches_v2(owner))
		return false;
	*out = result;
	return true;
}

static bool
relmap_recovery_terminal(const ClusterRelmapOwnerSampleV2 *owner,
						 ClusterRelmapRecoveryNativeTerminal *out, bool sync)
{
	ClusterRelmapRecoveryNativeTerminal result = { 0 };
	volatile bool valid = false;

	if (out == NULL)
		return false;
	if (owner != NULL) {
		uintptr_t x = (uintptr_t)owner, y = (uintptr_t)out;
		if (x <= y ? y - x < sizeof(*owner) : x - y < sizeof(*out))
			return false;
	}
	memset(out, 0, sizeof(*out));
	if (LWLockHeldByMe(XactSLRULock) || !relmap_recovery_owner_ready(owner)
		|| !TransactionIdIsNormal((TransactionId)owner->owner.top_full_xid))
		return false;

	/* The floor check and native read share one truncation-lock interval.
	 * XidGenLock is released before the SLRU reader can perform I/O. */
	LWLockAcquire(XactTruncationLock, LW_SHARED);
	PG_TRY();
	{
		if (relmap_recovery_bounds_locked(owner, &result.bounds)) {
			result.top_full_xid = owner->owner.top_full_xid;
			if (sync) {
				ClusterClogNativeTerminalV1 native;
				if (cluster_clog_native_terminal_sync_v1((TransactionId)result.top_full_xid,
														 &native)) {
					result.status = native.status;
					result.status_lsn = native.status_lsn;
				}
			} else
				result.status = TransactionIdGetStatus((TransactionId)result.top_full_xid,
													   &result.status_lsn);
			/* Native allocation can advance while the CLOG reader runs. Recheck
			 * its provenance before publishing, including sticky invalidation. */
			valid = (result.status == TRANSACTION_STATUS_COMMITTED
					 || result.status == TRANSACTION_STATUS_ABORTED)
					&& relmap_recovery_bounds_locked(owner, &result.bounds);
		}
	}
	PG_CATCH();
	{
		/* errfinish(ERROR) resets holdoff; mirror LWLockReleaseAll before
		 * releasing only this acquisition and rethrowing the native error. */
		HOLD_INTERRUPTS();
		LWLockRelease(XactTruncationLock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockRelease(XactTruncationLock);
	if (!valid || !cluster_relmap_owner_terminal_matches_v2(owner))
		return false;
	*out = result;
	return true;
}
bool
cluster_relmap_recovery_native_terminal_capture(const ClusterRelmapOwnerSampleV2 *owner,
												ClusterRelmapRecoveryNativeTerminal *out)
{
	return relmap_recovery_terminal(owner, out, false);
}

bool
cluster_relmap_recovery_native_terminal_sync(const ClusterRelmapOwnerSampleV2 *owner,
											 ClusterRelmapRecoveryNativeTerminal *out)
{
	return relmap_recovery_terminal(owner, out, true);
}
#endif
