/* PGRAC: original relmap producer identity.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_owner.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"

static bool
relmap_owner_context(void)
{
	return cluster_enabled && cluster_shared_config
		   && cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES
		   && MyBackendType == B_BACKEND && MyProcPid > 0 && CritSectionCount == 0
		   && !ShutdownRequestPending && IsTransactionState()
		   && cluster_relmap_authority_x_held() && !RecoveryInProgress();
}

bool
cluster_relmap_owner_capture_v2(ClusterRelmapOwnerSampleV2 *out)
{
	ClusterRelmapOwnerSampleV2 work = { 0 };
	FullTransactionId top;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!relmap_owner_context() || LWLockHeldByMe(RelationMappingLock))
		return false;

	/* Assignment can wait and write transaction metadata. It must precede the
	 * local mapping lock and WAL window. Never reconstruct from a raw XID. */
	top = GetTopFullTransactionId();
	if (!FullTransactionIdIsValid(top) || !TransactionIdIsNormal(XidFromFullTransactionId(top))
		|| cluster_wal_writer_begin(GetWALInsertionTimeLine(), &work.writer)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	work.owner.node = cluster_node_id;
	work.owner.top_full_xid = U64FromFullTransactionId(top);
	work.owner.membership_epoch = work.writer.epoch;
	/* These are separate observations, even when native writer qualification
	 * currently requires both to match its admitted claim incarnation. */
	work.owner.boot_incarnation = cluster_qvotec_get_self_incarnation();
	work.owner.admitted_incarnation =
		cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	work.producer_pid = MyProcPid;
	if (!cluster_relmap_owner_matches_v2(&work))
		return false;
	*out = work;
	return true;
}

bool
cluster_relmap_owner_matches_v2(const ClusterRelmapOwnerSampleV2 *sample)
{
	FullTransactionId top;

	if (sample == NULL || !relmap_owner_context()
		|| sample->producer_pid != MyProcPid || sample->owner.node != cluster_node_id
		|| sample->owner.wal_end != InvalidXLogRecPtr || sample->writer.startup_first_lsn != 0)
		return false;
	/* IfAny never assigns an XID, including when RelationMappingLock is held.
	 * A later epoch with the same raw XID is a different original owner. */
	top = GetTopFullTransactionIdIfAny();
	if (!FullTransactionIdIsValid(top) || !TransactionIdIsNormal(XidFromFullTransactionId(top))
		|| sample->owner.top_full_xid != U64FromFullTransactionId(top)
		|| sample->owner.membership_epoch != sample->writer.epoch
		|| sample->owner.boot_incarnation == 0
		|| sample->owner.boot_incarnation != cluster_qvotec_get_self_incarnation()
		|| sample->owner.admitted_incarnation == 0
		|| sample->owner.admitted_incarnation
			   != sample->writer.ref.claim.identity.origin_owner_incarnation
		|| sample->owner.admitted_incarnation
			   != cluster_membership_get_last_admitted_incarnation(cluster_node_id)
		|| sample->writer.ref.timeline != GetWALInsertionTimeLine())
		return false;
	return cluster_wal_writer_check(&sample->writer) == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
