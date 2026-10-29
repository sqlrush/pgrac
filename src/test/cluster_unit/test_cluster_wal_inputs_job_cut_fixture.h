/* Original cut collector/cancel/poll and claim validation bodies. Native
 * membership, random, allocator and callback dispatch are explicit leaves. */
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_activation.h"
#include "storage/spin.h"
#include "cluster_wal_inputs_job_payload_private.h"
#include "test_cluster_wal_inputs_job_cut_types.inc"

static WalCutShared lifecycle_mailbox;
static ResourceReleaseCallback lifecycle_cut_callback;
static pg_on_exit_callback lifecycle_cut_exit;
static uint64 lifecycle_nonce;
static bool lifecycle_stop, lifecycle_continue = true;
static uint32 lifecycle_wait_event;
uint32 *my_wait_event_info = &lifecycle_wait_event;
volatile sig_atomic_t ShutdownRequestPending;

MemoryContext
GetMemoryChunkContext(void *pointer)
{
	for (JobFixtureAllocation *entry = job_fixture_allocations; entry != NULL; entry = entry->next)
		if (entry->allocation == pointer)
			return entry->context;
	abort();
}
bool cluster_normal_stop_requested(void) { return lifecycle_stop; }
bool cluster_normal_stop_continuation_allowed(void) { return lifecycle_continue; }
bool cluster_pi_writeback_retained_read_current_v1(const ClusterWalSourceRef *source)
{ (void)source; return false; /* No retained PI continuation in this fixture. */ }
uint64 cluster_qvotec_get_self_incarnation(void) { return 7; }
bool cluster_qvotec_in_quorum(void) { return true; }
bool cluster_reconfig_has_pending_prebump_stage(void) { return false; }
ClusterNormalStartState cluster_semantic_normal_start_state(void)
{ return CLUSTER_NORMAL_START_TARGET_READY; }
bool cluster_semantic_activation_recovered_open_current(void) { return true; }
ClusterMembershipState cluster_membership_get_state(int32 node)
{ return node == 0 || node == 1 ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_ABSENT; }
uint64 cluster_membership_get_last_admitted_incarnation(int32 node)
{ return node == 0 || node == 1 ? 7 : 0; }
bool pg_strong_random(void *out, size_t bytes)
{
	UT_ASSERT_EQ(bytes, sizeof(lifecycle_nonce));
	++lifecycle_nonce;
	memcpy(out, &lifecycle_nonce, bytes);
	return true;
}
static void lifecycle_cut_register(ResourceReleaseCallback callback, void *argument)
{ UT_ASSERT(argument == NULL); lifecycle_cut_callback = callback; }
static void lifecycle_cut_before_exit(pg_on_exit_callback callback, Datum argument)
{ UT_ASSERT_EQ(argument, 0); lifecycle_cut_exit = callback; }

#define RegisterResourceReleaseCallback lifecycle_cut_register
#define before_shmem_exit lifecycle_cut_before_exit
#define getpid() MyProcPid
#include "test_cluster_wal_inputs_job_cut_bodies.inc"
#undef getpid
#undef before_shmem_exit
#undef RegisterResourceReleaseCallback

static void
lifecycle_cut_setup(void)
{
	UT_ASSERT(active_cut == NULL);
	memset(&lifecycle_mailbox, 0, sizeof(lifecycle_mailbox));
	SpinLockInit(&lifecycle_mailbox.lock);
	cut_shared = &lifecycle_mailbox;
	lifecycle_stop = false;
	lifecycle_continue = true;
}
