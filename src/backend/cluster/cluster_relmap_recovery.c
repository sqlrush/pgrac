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

bool
cluster_relmap_recovery_native_bounds(const ClusterRelmapOwnerSampleV2 *owner,
									  ClusterRelmapRecoveryNativeBounds *out)
{
	ClusterRelmapRecoveryNativeBounds result = { 0 };
	const ClusterControlRootIdentity *id;
	uint64 last, next, distance;
	TransactionId oldest;
	uint8 state;

	if (out == NULL)
		return false;
	if (owner != NULL) {
		uintptr_t x = (uintptr_t)owner, y = (uintptr_t)out;
		if (x <= y ? y - x < sizeof(*owner) : x - y < sizeof(*out))
			return false;
	}
	memset(out, 0, sizeof(*out));
	if (owner == NULL || ShmemVariableCache == NULL || LWLockHeldByMe(XactTruncationLock)
		|| LWLockHeldByMe(XidGenLock) || !cluster_relmap_owner_terminal_matches_v2(owner))
		return false;
	id = &owner->writer.ref.claim.identity;
	if (id->system_identifier == 0 || owner->writer.ref.claim.database_incarnation == 0
		|| id->origin_node_id < 0 || id->origin_node_id != owner->owner.node
		|| owner->owner.admitted_incarnation != id->origin_owner_incarnation
		|| owner->owner.boot_incarnation == 0 || owner->writer.epoch == 0
		|| owner->owner.membership_epoch != owner->writer.epoch)
		return false;

	/* Use the native arbitrary-CLOG-lookup lock before observing its floor.
	 * XidGenLock protects the actual publication receipt, not a guessed era.
	 * No I/O, allocation or owner/fence check occurs while either is held. */
	LWLockAcquire(XactTruncationLock, LW_SHARED);
	LWLockAcquire(XidGenLock, LW_SHARED);
	oldest = ShmemVariableCache->oldestClogXid;
	last = U64FromFullTransactionId(ShmemVariableCache->clusterLastIssuedXid);
	next = U64FromFullTransactionId(ShmemVariableCache->nextXid);
	state = ShmemVariableCache->clusterIssuedState;
	LWLockRelease(XidGenLock);
	LWLockRelease(XactTruncationLock);

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
		|| owner->owner.top_full_xid > result.last_issued
		|| !cluster_relmap_owner_terminal_matches_v2(owner))
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
#endif
