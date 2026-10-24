/* Original Checkpointer issuance observation. The ROOT/immutable anchor is
 * the durable owner, not this process-local descriptor or a caller boolean. */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_relmap_checkpoint.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
static bool
checkpoint_role(void)
{
	return MyBackendType == B_CHECKPOINTER && CritSectionCount == 0 && enableFsync
		   && cluster_enabled && cluster_shared_config && ShmemVariableCache
		   && !LWLockHeldByMe(XactTruncationLock) && !LWLockHeldByMe(XidGenLock)
		   && !LWLockHeldByMe(ControlFileLock) && !LWLockHeldByMe(XactSLRULock);
}
static bool
checkpoint_native_window(ClusterRelmapCheckpointWindow *w)
{
	uint64 last, next, distance;
	TransactionId floor;
	uint8 state;
	LWLockAcquire(XactTruncationLock, LW_SHARED);
	LWLockAcquire(XidGenLock, LW_SHARED);
	floor = ShmemVariableCache->oldestClogXid;
	last = U64FromFullTransactionId(ShmemVariableCache->clusterLastIssuedXid);
	next = U64FromFullTransactionId(ShmemVariableCache->nextXid);
	state = ShmemVariableCache->clusterIssuedState;
	LWLockRelease(XidGenLock);
	LWLockRelease(XactTruncationLock);
	if (state != 1 || !TransactionIdIsNormal(floor) || !TransactionIdIsNormal((TransactionId)last)
		|| !TransactionIdIsNormal((TransactionId)next) || last >= next)
		return false;
	distance = (uint32)((TransactionId)last - floor);
	if (distance >= UINT64CONST(0x80000000) || distance > last)
		return false;
	w->native_floor = last - distance;
	w->last_issued = last;
	w->allocation_next = next;
	return true;
}
bool
cluster_relmap_recovery_checkpoint_window_capture(TimeLineID timeline,
												  ClusterRelmapCheckpointSampleV1 *out)
{
	ClusterRelmapCheckpointSampleV1 s = { 0 };
	const ClusterControlRootIdentity *id = &s.writer.ref.claim.identity;
	if (!out)
		return false;
	memset(out, 0, sizeof(*out));
	if (!checkpoint_role()
		|| cluster_wal_writer_begin(timeline, &s.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	s.window.system_identifier = id->system_identifier;
	s.window.database_incarnation = s.writer.ref.claim.database_incarnation;
	s.window.origin = id->origin_node_id;
	s.window.epoch = s.writer.epoch;
	s.window.admitted_incarnation = id->origin_owner_incarnation;
	s.window.boot_incarnation = cluster_qvotec_get_self_incarnation();
	if (id->origin_node_id < 0 || id->origin_node_id >= 64 || !s.window.system_identifier
		|| !s.window.database_incarnation || !s.window.epoch || !s.window.admitted_incarnation
		|| !s.window.boot_incarnation || !checkpoint_native_window(&s.window)
		|| cluster_wal_writer_check(&s.writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_qvotec_get_self_incarnation() != s.window.boot_incarnation)
		return false;
	*out = s;
	return true;
}
static bool
alias(const void *a, size_t na, const void *b, size_t nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a && b && (x <= y ? y - x < na : x - y < nb);
}
bool
cluster_relmap_recovery_checkpoint_window_finish(const ClusterRelmapCheckpointSampleV1 *s,
												 const CheckPoint *cp, XLogRecPtr start,
												 XLogRecPtr end, uint8 out[160])
{
	ClusterRelmapCheckpointWindow now = { 0 };
	const ClusterControlRootIdentity *id;
	if (!out || alias(out, 160, s, sizeof(*s)) || alias(out, 160, cp, sizeof(*cp)))
		return false;
	memset(out, 0, 160);
	if (!s || !cp || !checkpoint_role())
		return false;
	id = &s->writer.ref.claim.identity;
	if (s->window.system_identifier != id->system_identifier
		|| s->window.database_incarnation != s->writer.ref.claim.database_incarnation
		|| s->window.origin != id->origin_node_id
		|| s->window.admitted_incarnation != id->origin_owner_incarnation
		|| s->window.epoch != s->writer.epoch || s->writer.ref.timeline != cp->ThisTimeLineID
		|| cluster_qvotec_get_self_incarnation() != s->window.boot_incarnation
		|| cluster_wal_writer_check(&s->writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !checkpoint_native_window(&now) || now.native_floor != s->window.native_floor
		|| now.last_issued < s->window.last_issued
		|| now.allocation_next < s->window.allocation_next
		|| cluster_wal_writer_check(&s->writer) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_qvotec_get_self_incarnation() != s->window.boot_incarnation)
		return false;
	return cluster_relmap_checkpoint_window_encode_v2(&s->window, cp, start, end, now.last_issued,
													  out);
}
#endif
