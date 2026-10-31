/* Immutable ROOT/origin/anchor/claim I/O are configured boundary inputs.
 * The extracted selection, all-or-nothing sequencing, CF and pin are real.
 * Unsupported terminal/remote leaf entries abort, never synthesize proof. */
#include "common/cryptohash.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_qvotec.h"
#include "postmaster/startup.h"
#include "storage/lwlock.h"
#include "cluster_recovery_anchor_private.h"
#include "test_cluster_wal_inputs_job_begin_types.inc"

/* These cold-only leaves are forbidden in every ONLINE job case below. */
LWLockPadded *MainLWLockArray;
bool
LWLockHeldByMe(LWLock *lock)
{
	(void)lock;
	abort();
}
bool
cluster_recovery_transport_components_current(void)
{
	abort();
}

#ifndef PGRAC_WAL_INPUTS_REAL_CUT
/* The ONLINE-only fixture must never enter cold CF wait capture. */
uint64
cluster_qvotec_get_self_incarnation(void)
{
	abort();
}
#endif

char *cluster_wal_threads_dir = "fixture-immutable-inputs";
static ControlRootImage begin_root;
static ClusterWalOriginInputs begin_origin[2];
static ClusterControlRootFileToken begin_token;
static ClusterWalInputsJobV1 *begin_job;
static unsigned begin_roots, begin_origins, begin_anchors, begin_claims;
static unsigned begin_pending_phase, begin_changed_phase, begin_failed_phase;
static unsigned begin_claim_failure, begin_claim_throw;
static bool begin_origin_pending, begin_guard_output;
static bool begin_foreign_attempt;
static ClusterControlRootResult begin_read_result;
static char begin_order[64];
static unsigned begin_order_count;

static void
begin_provisional(char step)
{
	UT_ASSERT(begin_order_count < sizeof(begin_order));
	if (begin_order_count < sizeof(begin_order))
		begin_order[begin_order_count++] = step;
	if (begin_guard_output && begin_job != NULL && begin_job->inputs != NULL) {
		UT_ASSERT_EQ(begin_job->state, WAL_INPUT_JOB_STATE_BUILDING);
		UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs), 0);
		UT_ASSERT(cluster_wal_inputs_at_v1(begin_job->inputs, 0) == NULL);
	}
}

uint64
GetSystemIdentifier(void)
{
	return 1234;
}

ClusterControlRootResult
cluster_control_root_v3_read_control_locked(const uint8 uuid[16], uint64 system_identifier,
	ControlRootImage *root, ControlFileData *common, ClusterControlRootFileToken *token)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(uuid[0] == 1 && system_identifier == 1234);
	begin_provisional('R');
	++begin_roots;
	owner_ack_on_wait = begin_roots != begin_pending_phase;
	if (begin_foreign_attempt) {
		static const char foreign;
		MyBackendType = B_LMON;
		UT_ASSERT(!cluster_cf_lock_poll_owned(ShareLock, &foreign));
		MyBackendType = B_CHECKPOINTER;
	}
	if (begin_roots == begin_failed_phase)
		return begin_read_result;
	*root = begin_root;
	memset(common, 0, sizeof(*common));
	*token = begin_token;
	if (begin_roots == begin_changed_phase)
		++token->file_txn_seq;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_origin_inputs_read_locked(const ControlRootImage *root, uint32 node,
	ClusterWalOriginInputs *out)
{
	UT_ASSERT(node < lengthof(begin_origin) && root->present[node]);
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(cluster_wal_read_pin_covers_v1(begin_job->pin, node + 1));
	begin_provisional('O');
	++begin_origins;
	*out = begin_origin[node];
	if (begin_origin_pending)
		out->has_pending = true;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_recovery_anchor_v3_read_native_window_locked(const ClusterRecoveryAnchorRefV2 *ref,
	const ControlFileData *common, ControlFileData *out, uint8 window[160])
{
	(void)common;
	UT_ASSERT(cluster_cf_held_is_clusterwide(ShareLock));
	UT_ASSERT(cluster_wal_read_pin_covers_v1(begin_job->pin, ref->identity.origin_thread_id));
	begin_provisional('A');
	++begin_anchors;
	memset(out, 0, sizeof(*out));
	out->checkPoint = 100 + ref->identity.origin_thread_id;
	out->checkPointCopy.redo = 80;
	memset(window, ref->identity.origin_thread_id, 160);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_recovery_anchor_v2_thread_state(const ClusterRecoveryAnchorRefV2 *ref,
	const ClusterControlRootSnapshot *record, ControlFileData *view)
{
	(void)view;
	return memcmp(&ref->identity, &record->identity, sizeof(ref->identity)) == 0
		? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}

ClusterControlRootResult
cluster_wal_claim_v2_read(const char *root, const ClusterWalThreadClaimRefV2 *ref,
	ClusterWalThreadClaimV2 *out)
{
	UT_ASSERT(root == cluster_wal_threads_dir);
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_wal_read_pin_covers_v1(begin_job->pin, ref->identity.origin_thread_id));
	begin_provisional('C');
	++begin_claims;
	if (begin_claims == begin_claim_throw)
		pg_re_throw();
	if (begin_claims == begin_claim_failure)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	memset(out, 0, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_terminal_read_locked(const ControlRootImage *root, uint32 node, uint32 index,
	ClusterWalTerminalImage *out)
{
	(void)root; (void)node; (void)index; (void)out; abort();
}
ClusterControlRootResult
cluster_wal_claim_v2_encode(const ClusterWalThreadClaimV2 *claim,
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES])
{
	(void)claim; (void)bytes; abort();
}
#ifndef PGRAC_WAL_INPUTS_REAL_CUT
ClusterControlRootResult
cluster_wal_cut_begin_v1(const ClusterWalSourceRef *source, ClusterWalCutV1 **out)
{
	(void)source; (void)out; abort();
}
ClusterControlRootResult
cluster_wal_cut_poll_v1(ClusterWalCutV1 *cut, ClusterWalWriterFlushV1 *out)
{
	(void)cut; (void)out; abort();
}
#endif

#include "postmaster/interrupt.h"
#ifndef PGRAC_WAL_INPUTS_REAL_CUT
volatile sig_atomic_t ShutdownRequestPending;
#endif
/* Local native writer I/O is outside this remote collector fixture. */
ClusterControlRootResult
cluster_wal_inputs_prepare_live_local_v1(ClusterWalInputsV1 *inputs, uint32 index, XLogRecPtr *end)
{ (void)inputs; (void)index; (void)end; abort(); }

#include "test_cluster_wal_inputs_job_begin_bodies.inc"

static void
begin_setup(void)
{
	pin_begin();
	cluster_controlfile_shared_authority = true;
	memset(&begin_root, 0, sizeof(begin_root));
	memset(begin_origin, 0, sizeof(begin_origin));
	memset(&begin_token, 0, sizeof(begin_token));
	begin_token.file_txn_seq = 17;
	begin_token.system_identifier = 1234;
	begin_root.header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	begin_root.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	begin_root.header.v2.database_incarnation = 3;
	begin_root.header.v2.config_generation = 5;
	for (unsigned i = 0; i < lengthof(begin_origin); ++i) {
		ClusterWalHistoryRecord *current = &begin_origin[i].current;
		begin_root.present[i] = true;
		current->snapshot.identity.origin_thread_id = i + 1;
		current->snapshot.identity.origin_node_id = i;
		current->snapshot.identity.origin_owner_incarnation = 7;
		current->snapshot.checkpoint_tli = 1;
	}
	begin_roots = begin_origins = begin_anchors = begin_claims = 0;
	begin_pending_phase = begin_changed_phase = begin_failed_phase = 0;
	begin_claim_failure = begin_claim_throw = 0;
	begin_origin_pending = false;
	begin_foreign_attempt = false;
	begin_guard_output = true;
	begin_read_result = CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	memset(begin_order, 0, sizeof(begin_order));
	begin_order_count = 0;
	owner_ack_on_wait = true;
	begin_job = foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
}

static void
begin_cleanup(void)
{
	ClusterWalReadPinV1 *pin;
	begin_guard_output = false;
	if (begin_job->cf.state == CF_JOB_RECEIPT_RETIRING) {
		job_ack(&begin_job->cf);
		cluster_cf_retirement_poll();
	}
	pin_cleanup(&begin_job, &pin);
	cluster_controlfile_shared_authority = false;
}
