/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual KO SPSC admission/drain, with explicit storage and ACK-queue boundary
 * fixtures. A queue-empty observation alone does not certify flush completion. */
#include "postgres.h"
#include "access/xact.h"
#include "access/multixact.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_hw_lease.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/storage/cluster_smgr.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/inval.h"
#include "utils/relcache.h"
static bool ko_offer_notice_fixture(const ClusterPiWritebackNoticeV1 *, uint32, uint64 *,
									ClusterPiWritebackFactV2 *);
static bool ko_offer_ack_fixture(const ClusterPiWritebackJobV1 *, uint32, uint64,
								 ClusterWalWriterToken *);
static bool ko_drop_poll_fixture(uint32 *, bool *);
static bool (*ko_drop_poll_boundary)(uint32 *, bool *);
#define cluster_smgr_drop_work_poll ko_drop_poll_fixture
#define cluster_pi_writeback_structure_offer_read_v2 ko_offer_notice_fixture
#define cluster_pi_writeback_structure_offer_ack_v2 ko_offer_ack_fixture
#include "../../backend/cluster/cluster_ko_lock.c"
#undef cluster_smgr_drop_work_poll
#undef cluster_pi_writeback_structure_offer_read_v2
#undef cluster_pi_writeback_structure_offer_ack_v2
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
AuxProcType MyAuxProcType = SinvalBcastProcess;
int cluster_node_id = 0;
int MyProcPid = 199;
BackendType MyBackendType = B_BACKEND;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
volatile uint32 CritSectionCount;
bool cluster_enabled = true;
bool cluster_object_reuse_flush_enabled = false;
bool cluster_shared_config = false;
int cluster_space_affinity = CLUSTER_SPACE_AFFINITY_STATIC;
int cluster_injection_armed_count = 0;
static ClusterKoShared storage;
static ClusterHwLeaseShared leases;
static ClusterNodeInfo peer;
static SMgrRelationData relation;
static bool removal_ready = true;
static bool flushing;
static int flush_count, drop_count, ack_count, wake_count;
static KoFlushAckHeader last_ack;
static uint64 current_epoch;
static uint32 existing_forks, synced_forks;
static int sync_count, sync_fail_fork, epoch_change_sync_fork;
static bool epoch_change_flush, epoch_change_drop;
static bool shared_relation;
static uint64 recorded_batch;
static int record_count;
static bool barrier_active, barrier_complete, epoch_change_complete;
static int barrier_requests, barrier_waits, barrier_removes;
static bool multiple_barriers;
static uint64 allocated_batch, active_batch;
static int barrier_record_base;
static KoFlushHeader last_request;
static bool gate_admit = true;
static ClusterFormationSnapshotV1 formation;
static ClusterWalSourceRef writer;
static ClusterSpaceIdentity space_identity;
static uint64 member_generation;
static bool capture_ok, cap_ok, space_ok, generation_race;
static bool native_commit_active;
static int fixture_error_level;
static unsigned fixture_log_events;

static bool
ko_drop_poll_fixture(uint32 *cursor, bool *completed)
{
	return ko_drop_poll_boundary != NULL && ko_drop_poll_boundary(cursor, completed);
}
static int cap_missing_peer = -1, cap_zero_peer = -1, cap_change_cut_peer = -1;
static int cut_writer_samples, cut_writer_change_at, cut_cap_samples[CLUSTER_KO_SHARED_NODE_LIMIT];
static int cut_cap_change_peer = -1;
static int space_reads, send_calls, lock_calls;
static int boot_change_phase;
static bool cancel_wait;
static bool drive_shared_ack;
static bool expecting_error;
static int reported_sqlstate;
static ClusterKoSharedMessageV2 last_shared_request, last_shared_ack;
static ClusterPiWritebackFactV2 offered_structure;
static bool offer_notice_live;
static const ClusterPiWritebackNoticeV1 *offer_notice = (const ClusterPiWritebackNoticeV1 *)9;
static const ClusterPiWritebackJobV1 *offer_job = (const ClusterPiWritebackJobV1 *)11;
static ClusterWalWriterToken offered_peer;
static bool offer_ack_live, offer_ack_drift;
static uint32 offer_ack_slot;
static uint64 offer_ack_serial;
/* The exact opaque transport ACK is the boundary here; the writeback suite
 * verifies that the real singleton job cannot mint it from an empty reply. */
static bool
ko_offer_ack_fixture(const ClusterPiWritebackJobV1 *job, uint32 slot, uint64 serial,
					 ClusterWalWriterToken *out)
{
	if (!offer_ack_live || job != offer_job || slot != offer_ack_slot || serial != offer_ack_serial)
		return false;
	if (offer_ack_drift)
		storage.contexts[slot].serial++;
	*out = offered_peer;
	return true;
}
/* The authenticated WB notice is this unit's explicit input boundary. Its
 * runtime identity checks execute in the writeback unit; no raw public fact
 * is accepted by the KO owner. */
static bool
ko_offer_notice_fixture(const ClusterPiWritebackNoticeV1 *notice, uint32 index, uint64 *revision,
						ClusterPiWritebackFactV2 *out)
{
	if (!offer_notice_live || notice != offer_notice || index != 0)
		return false;
	*revision = 7;
	*out = offered_structure;
	return true;
}
/* PCM is the explicit read-only module boundary in this owner fixture;
 * the PCM unit exercises its actual registry/protocol table. */
static ClusterPcmPiRelationScanV2 relation_scan_result;
static uint32 relation_scan_calls, relation_scan_advance;
static bool relation_scan_epoch_drift, relation_scan_slot_drift, relation_scan_freeze_drift;
static uint64 relation_gate_freeze;
static bool relation_gate_ok;
bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *out)
{
	if (!relation_gate_ok)
		return false;
	memset(out, 0, sizeof(*out));
	out->phase = RESOURCE_X_GATE_OPEN;
	out->formation = current_epoch;
	out->freeze_generation = relation_gate_freeze;
	return true;
}
ClusterPcmPiRelationScanV2
cluster_pcm_lock_pi_relation_scan_v2(RelFileLocator locator, uint64 epoch, uint32 budget,
									 uint32 *cursor)
{
	UT_ASSERT_EQ(locator.relNumber, 99);
	UT_ASSERT_EQ(epoch, current_epoch);
	UT_ASSERT_EQ(budget, 128);
	relation_scan_calls++;
	*cursor += relation_scan_advance;
	if (relation_scan_epoch_drift)
		current_epoch++;
	if (relation_scan_slot_drift)
		storage.contexts[0].serial++;
	if (relation_scan_freeze_drift)
		relation_gate_freeze++;
	return relation_scan_result;
}

static void (*exit_callback)(int, Datum);
static ResourceReleaseCallback resource_callback;
static XactCallback xact_callback;
ResourceOwner CurrentResourceOwner = (ResourceOwner)1;
ResourceOwner CurTransactionResourceOwner = (ResourceOwner)1;
ResourceOwner TopTransactionResourceOwner = (ResourceOwner)1;
MemoryContext TopTransactionContext = (MemoryContext)1;
MemoryContext TopMemoryContext = (MemoryContext)4;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
/* Explicit standard-PG allocator boundary. Actual KO publication, owners,
 * queue cut and active-slot consumers execute from the product source. */
static void *native_allocations[512];
static unsigned native_allocated, native_allocate_calls, native_detaches, native_releases;
static bool native_alloc_fail;
size_t
dsa_minimum_size(void)
{
	return 65536;
}
Size
add_size(Size a, Size b)
{
	Assert(a <= SIZE_MAX - b);
	return a + b;
}
int
LWLockNewTrancheId(void)
{
	return 71;
}
void
LWLockRegisterTranche(int id, const char *name)
{
	Assert(id == 71);
}
dsa_area *
dsa_create_in_place(void *place, size_t size, int tranche, dsm_segment *seg)
{
	Assert(native_allocated == 0);
	return (dsa_area *)1;
}
dsa_area *
dsa_attach_in_place(void *place, dsm_segment *seg)
{
	Assert(SpinLockFree(&storage.shared_lock));
	return (dsa_area *)1;
}
void
dsa_pin(dsa_area *area)
{
	Assert(area == (dsa_area *)1);
}
void
dsa_pin_mapping(dsa_area *area)
{
	Assert(area == (dsa_area *)1);
}
void
dsa_detach(dsa_area *area)
{
	Assert(SpinLockFree(&storage.shared_lock));
	native_detaches++;
}
void
dsa_release_in_place(void *place)
{
	Assert(SpinLockFree(&storage.shared_lock));
	native_releases++;
}
dsa_pointer
dsa_allocate_extended(dsa_area *area, size_t size, int flags)
{
	Assert(SpinLockFree(&storage.shared_lock));
	Assert(flags == (DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM));
	native_allocate_calls++;
	if (native_alloc_fail)
		return InvalidDsaPointer;
	for (unsigned i = 1; i < lengthof(native_allocations); i++)
		if (native_allocations[i] == NULL) {
			native_allocations[i] = calloc(1, size);
			native_allocated++;
			return i;
		}
	abort();
}
void *
dsa_get_address(dsa_area *area, dsa_pointer dp)
{
	Assert(SpinLockFree(&storage.shared_lock));
	Assert(dp > 0 && dp < lengthof(native_allocations) && native_allocations[dp] != NULL);
	return native_allocations[dp];
}
void
dsa_free(dsa_area *area, dsa_pointer dp)
{
	void *address = dsa_get_address(area, dp);
	free(address);
	native_allocations[dp] = NULL;
	native_allocated--;
}

ResourceOwner
ResourceOwnerGetParent(ResourceOwner owner)
{
	Assert(owner == (ResourceOwner)1 || owner == (ResourceOwner)2 || owner == (ResourceOwner)3);
	return owner == (ResourceOwner)3 ? (ResourceOwner)2
		: owner == (ResourceOwner)2 ? (ResourceOwner)1 : NULL;
}
static int completion_allocations;
static unsigned gate_calls;
static unsigned liveness_calls;
int cluster_ges_request_timeout_ms = 100;
int cluster_sinval_ack_timeout_ms = 100;
Latch *MyLatch;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	Assert(context == TopTransactionContext || context == TopMemoryContext);
	Assert(SpinLockFree(&storage.shared_lock));
	completion_allocations++;
	return calloc(1, size);
}
void pfree(void *p) { completion_allocations--; free(p); }
/* Only the allocation-free writeback codec is reachable in this fixture. */
void *palloc0(Size size) { abort(); }
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	Assert(arg == NULL);
	resource_callback = callback;
}

void
RegisterXactCallback(XactCallback callback, void *arg)
{
	Assert(arg == NULL);
	xact_callback = callback;
}


void ProcessInterrupts(void)
{
	InterruptPending = false;
	pg_re_throw();
}
void before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	Assert(arg == 0);
	exit_callback = function;
}
uint64 cluster_membership_cut_generation(void) { return member_generation; }
uint32
cluster_ic_local_capability_word(void)
{
	return native_commit_active ? PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2 : 0;
}
bool cluster_membership_cut_generation_current(uint64 g)
{ return !generation_race && g != 0 && g == member_generation; }
bool cluster_reconfig_capture_formation_snapshot_v1(uint16 origin, ClusterFormationSnapshotV1 *out)
{
	Assert(SpinLockFree(&storage.shared_lock));
	Assert(origin == cluster_node_id + 1);
	*out = formation;
	out->local_epoch = current_epoch;
	return capture_ok;
}
bool cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	*out = writer;
	if (++cut_writer_samples == cut_writer_change_at)
		out->claim.identity.origin_owner_incarnation++;
	return true;
}
bool cluster_space_relation_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	Assert(SpinLockFree(&storage.shared_lock));
	Assert(RelFileLocatorEquals(locator, space_identity.key.locator));
	space_reads++;
	*out = space_identity;
	return space_ok;
}
bool cluster_sf_peer_capability_word_sample(int32 peer_id, uint32 required,
	uint32 *word, uint32 *generation)
{
	Assert(peer_id >= 0 && peer_id < CLUSTER_KO_SHARED_NODE_LIMIT
		&& peer_id != cluster_node_id && required == PGRAC_IC_HELLO_CAP_KO_SHARED_V2);
	*word = required;
	*generation = peer_id == cap_zero_peer ? 0 : 19;
	if (++cut_cap_samples[peer_id] > 1 && peer_id == cut_cap_change_peer)
		(*generation)++;
	if (peer_id == cap_change_cut_peer)
		member_generation++;
	return cap_ok && peer_id != cap_missing_peer;
}
static void shared_send_tick(void)
{
	AuxProcType saved = MyAuxProcType;
	MyAuxProcType = LmonProcess;
	cluster_ko_lmon_tick_v2();
	MyAuxProcType = saved;
}
ClusterICSendResult cluster_ic_send_envelope(uint8 type, int32 dest, const void *bytes, uint32 length)
{
	ClusterKoSharedMessageV2 m;
	Assert(SpinLockFree(&storage.shared_lock));
	Assert(dest == 1 && length == CLUSTER_KO_SHARED_V2_BYTES);
	Assert(cluster_ko_shared_decode_v2(bytes, length, &m));
	send_calls++;
	if (type == PGRAC_IC_MSG_KO_FLUSH) {
		last_shared_request = m;
		barrier_requests++;
	} else {
		Assert(type == PGRAC_IC_MSG_KO_FLUSH_ACK && m.verb == CLUSTER_KO_SHARED_ACK);
		Assert(flush_count == drop_count && drop_count == ack_count + 1);
		last_shared_ack = m;
		last_ack.batch_id = m.batch_id;
		last_ack.epoch = m.epoch;
		last_ack.status = KO_FLUSH_ACK_DONE;
		ack_count++;
	}
	return CLUSTER_IC_SEND_DONE;
}

bool RecoveryInProgress(void) { return false; }
TimestampTz GetCurrentTimestamp(void) { return 1; }
void
ResetLatch(Latch *latch)
{
	Assert(barrier_active);
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 wait_event)
{
	Assert(barrier_active && timeout > 0);
	barrier_waits++;
	if (cancel_wait)
		pg_re_throw();
	current_epoch++;
	barrier_complete = true;
	return WL_LATCH_SET;
}
void
pg_re_throw(void)
{
	Assert(PG_exception_stack != NULL);
	siglongjmp(*PG_exception_stack, 1);
}
int errhint(const char *format, ...) { return 0; }
bool
errstart(int level, const char *domain)
{
	fixture_error_level = level;
	return level >= ERROR || level == LOG;
}
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int
errmsg(const char *format, ...)
{
	Assert(expecting_error || fixture_error_level == LOG);
	return 0;
}
int
errmsg_internal(const char *format, ...)
{
	Assert(expecting_error || fixture_error_level == LOG);
	return 0;
}
int errcode(int sqlstate) { reported_sqlstate = sqlstate; return 0; }
void
errfinish(const char *file, int line, const char *function)
{
	if (fixture_error_level == LOG) {
		fixture_log_events++;
		return;
	}
	if (expecting_error)
		pg_re_throw();
	fprintf(stderr, "unexpected error %s:%d %s\n", file, line, function);
	abort();
}
ClusterExtendEngage cluster_extend_liveness_engage(bool wait_for_lms)
{
	Assert(!wait_for_lms);
	liveness_calls++;
	return CLUSTER_EXTEND_ENGAGE_NATIVE;
}
ClusterLockAcquireResult cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{
	lock_calls++;
	return CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
}
ClusterLockAcquireResult cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{ abort(); }
ClusterLockAcquireResult cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{ abort(); }
uint32 cluster_sinval_compute_alive_peer_mask(void) { abort(); }
uint64
cluster_sinval_ack_wait_alloc_batch_id(void)
{
	return multiple_barriers ? ++allocated_batch : 912;
}
bool
cluster_sinval_ack_wait_begin(uint64 batch, uint32 mask, TimestampTz deadline)
{
	Assert((multiple_barriers ? batch == allocated_batch : batch == 912) && mask == (1u << 1)
		   && !barrier_active);
	active_batch = batch;
	barrier_record_base = record_count;
	barrier_active = true;
	return true;
}
bool
cluster_sinval_ack_wait_is_complete(uint64 batch)
{
	Assert(barrier_active && batch == active_batch);
	if (drive_shared_ack) {
		ClusterICEnvelope env = {0};
		uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
		ClusterKoSharedMessageV2 ack;
		shared_send_tick();
		ack = last_shared_request;
		ack.verb = CLUSTER_KO_SHARED_ACK;
		ack.status = CLUSTER_KO_SHARED_DONE;
		Assert(cluster_ko_shared_encode_v2(&ack, bytes, sizeof(bytes)));
		env.source_node_id = 1;
		env.payload_length = sizeof(bytes);
		cluster_ko_flush_ack_handler(&env, bytes);
		barrier_complete = record_count == barrier_record_base + 1;
	}
	if (epoch_change_complete)
		current_epoch++;
	return barrier_complete;
}
void
cluster_sinval_ack_wait_remove(uint64 batch)
{
	Assert(barrier_active && batch == active_batch);
	barrier_removes++;
	barrier_active = false;
}
void
cluster_sinval_ack_wait_record(uint64 batch, int32 acker)
{
	Assert(acker == 1);
	recorded_batch = batch;
	record_count++;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	return true;
}
void
LWLockRelease(LWLock *lock)
{}
void LWLockInitialize(LWLock *lock, int tranche) { abort(); }
void cluster_shmem_register_region(const ClusterShmemRegion *region) { abort(); }

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(modifies_data);
	gate_calls++;
	return gate_admit;
}

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	fprintf(stderr, "unexpected recursive spinlock at %s:%d %s\n", file, line, func);
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	Assert(size == cluster_ko_shmem_size());
	*found = false;
	return &storage;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	return node == 1 ? &peer : NULL;
}
uint64
cluster_epoch_get_current(void)
{
	return current_epoch;
}
void
cluster_sinval_set_proc_latch(void)
{
	wake_count++;
}
void
cluster_lmon_wakeup(void)
{
	wake_count++;
}
void
cluster_injection_run(const char *name)
{
	abort();
}
bool
cluster_injection_should_skip(const char *name)
{
	return false;
}
bool
cluster_ctrc_relation_removal_ready_shared(uint32 spc, uint32 db, uint32 rel)
{
	Assert(spc == 1663 && rel == (multiple_barriers ? space_identity.key.locator.relNumber : 99));
	return removal_ready;
}
SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	Assert(locator.spcOid == 1663
		   && locator.relNumber == (multiple_barriers ? space_identity.key.locator.relNumber : 99)
		   && backend == InvalidBackendId);
	return &relation;
}
void
FlushRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
	const char *reason;
	uint32 slot;
	Assert(nrels == 1 && *smgrs == &relation);
	flushing = true;
	/* The last item has already left the ring. Its actual actor must stay
	 * active across this call; the read-only ring poll cannot sign that. */
	if (pg_atomic_read_u32(&storage.inbound_head) == pg_atomic_read_u32(&storage.inbound_tail))
		UT_ASSERT_EQ(cluster_ko_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	flush_count++;
	flushing = false;
	if (epoch_change_flush)
		current_epoch++;
	if (boot_change_phase == 1)
		formation.membership.last_admitted_incarnation[1]++;
}
void
DropRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
	Assert(!flushing && flush_count == drop_count + 1);
	if (cluster_shared_config && shared_relation)
		UT_ASSERT_EQ(synced_forks, existing_forks);
	drop_count++;
	if (epoch_change_drop)
		current_epoch++;
	if (boot_change_phase == 3)
		formation.membership.last_admitted_incarnation[1]++;
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	Assert(locator.spcOid == 1663
		   && locator.relNumber == (multiple_barriers ? space_identity.key.locator.relNumber : 99)
		   && backend == InvalidBackendId);
	return shared_relation ? 1 : 0;
}
bool
smgrexists(SMgrRelation smgr, ForkNumber fork)
{
	Assert(smgr == &relation && fork >= MAIN_FORKNUM && fork <= MAX_FORKNUM);
	return (existing_forks & (1u << fork)) != 0;
}
void
smgrimmedsync(SMgrRelation smgr, ForkNumber fork)
{
	Assert(smgr == &relation && !flushing && flush_count == drop_count + 1);
	Assert((existing_forks & (1u << fork)) != 0);
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(drop_count, 0);
	sync_count++;
	if (fork == sync_fail_fork)
		pg_re_throw();
	synced_forks |= 1u << fork;
	if (fork == epoch_change_sync_fork)
		current_epoch++;
	if (boot_change_phase == 2)
		formation.membership.last_admitted_incarnation[1]++;
}
bool
cluster_grd_outbound_enqueue_backend_msg(uint8 type, uint32 dest, const void *payload, uint16 len)
{
	if (type == PGRAC_IC_MSG_KO_FLUSH) {
		Assert(barrier_active && len == sizeof(last_request));
		UT_ASSERT_EQ(dest, 1);
		memcpy(&last_request, payload, len);
		barrier_requests++;
		return true;
	}
	Assert(type == PGRAC_IC_MSG_KO_FLUSH_ACK && dest == 1 && len == sizeof(last_ack));
	Assert(flush_count == drop_count && drop_count == ack_count + 1);
	/* Even if the file later regrows beyond block 4, the pre-RESET range
	 * cannot be consumed after this peer has acknowledged the barrier. */
	UT_ASSERT_EQ(cluster_hw_lease_next_block((RelFileLocator){1663, 0, 99}, MAIN_FORKNUM),
		InvalidBlockNumber);
	memcpy(&last_ack, payload, len);
	ack_count++;
	return true;
}
static ClusterNormalStopPollResult
poll_queue(void)
{
	uint32 slot;
	const char *reason;
	return cluster_ko_normal_stop_poll(&slot, &reason);
}
static void
reset_test(void)
{
	Assert(completion_allocations == 0);
	Assert(native_allocated == 0);
	native_allocate_calls = 0;
	native_alloc_fail = false;
	native_commit_active = false;
	ko_drop_poll_boundary = NULL;
	fixture_log_events = 0;
	ko_drop_scan = 0;
	ko_native_report_serial = 0;
	memset(ko_drop_report_serial, 0, sizeof(ko_drop_report_serial));
	ko_native_area = NULL;
	cluster_enabled = true;
	MyBackendType = B_BACKEND;
	CurrentResourceOwner = (ResourceOwner)1;
	CurTransactionResourceOwner = (ResourceOwner)1;
	TopTransactionContext = (MemoryContext)1;
	memset(&storage, 0, sizeof(storage));
	relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
	relation_scan_calls = relation_scan_advance = 0;
	relation_scan_epoch_drift = relation_scan_slot_drift = relation_scan_freeze_drift = false;
	relation_gate_freeze = 3;
	relation_gate_ok = true;
	IsUnderPostmaster = false;
	cluster_ko_shmem_init();
	IsUnderPostmaster = true;
	MyAuxProcType = SinvalBcastProcess;
	flush_count = drop_count = ack_count = wake_count = 0;
	removal_ready = true;
	gate_admit = true;
	gate_calls = 0;
	liveness_calls = 0;
	cluster_shared_config = false;
	capture_ok = cap_ok = space_ok = true;
	cap_missing_peer = cap_zero_peer = cap_change_cut_peer = -1;
	cut_writer_samples = cut_writer_change_at = 0;
	memset(cut_cap_samples, 0, sizeof(cut_cap_samples));
	cut_cap_change_peer = -1;
	generation_race = cancel_wait = drive_shared_ack = false;
	expecting_error = false;
	reported_sqlstate = 0;
	InterruptPending = false;
	space_reads = send_calls = lock_calls = boot_change_phase = 0;
	member_generation = 2;
	memset(&formation, 0, sizeof(formation));
	formation.local_epoch = 1;
	formation.startup_formation_generation = 4;
	formation.membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	formation.membership.membership_state[1] = CLUSTER_MEMBER_MEMBER;
	formation.membership.last_admitted_incarnation[0] = 11;
	formation.membership.last_admitted_incarnation[1] = 22;
	memset(&writer, 0, sizeof(writer));
	writer.claim.identity.origin_node_id = 0;
	writer.claim.identity.origin_owner_incarnation = 11;
	writer.claim.identity.system_identifier = 1234;
	writer.claim.identity.storage_uuid[0] = 42;
	writer.claim.database_incarnation = 5;
	memset(&space_identity, 0, sizeof(space_identity));
	space_identity.key.system_identifier = 1234;
	space_identity.key.database_incarnation = 5;
	space_identity.key.storage_uuid[0] = 42;
	space_identity.key.locator = (RelFileLocator){1663, 0, 99};
	space_identity.incarnation[0] = 99;
	space_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	memset(&last_shared_request, 0, sizeof(last_shared_request));
	memset(&last_shared_ack, 0, sizeof(last_shared_ack));
	shared_relation = true;
	current_epoch = 1;
	existing_forks = (1u << MAIN_FORKNUM) | (1u << VISIBILITYMAP_FORKNUM) | (1u << SPACE_FORKNUM);
	synced_forks = 0;
	sync_count = 0;
	sync_fail_fork = epoch_change_sync_fork = -1;
	epoch_change_flush = epoch_change_drop = false;
	recorded_batch = 0;
	record_count = 0;
	barrier_active = false;
	barrier_complete = true;
	multiple_barriers = false;
	allocated_batch = 911;
	active_batch = 0;
	barrier_record_base = 0;
	epoch_change_complete = false;
	barrier_requests = barrier_waits = barrier_removes = 0;
	memset(&last_ack, 0, sizeof(last_ack));
	memset(&last_request, 0, sizeof(last_request));
	memset(&leases, 0, sizeof(leases));
	ClusterHwLeaseCtl = &leases;
	for (int i = 0; i < CLUSTER_HW_LEASE_SLOTS; i++)
		leases.slots[i].fork = -1;
}
static void
enqueue_legacy_epoch(uint64 id, uint64 epoch)
{
	ClusterICEnvelope env = { 0 };
	KoFlushHeader request = { 0 };
	request.batch_id = id;
	request.epoch = epoch;
	request.source_node = 1;
	request.spc_oid = 1663;
	request.db_oid = 0; /* Shared relations have a valid zero database OID. */
	request.rel_number = 99;
	env.source_node_id = 1;
	env.payload_length = sizeof(request);
	cluster_ko_flush_request_handler(&env, &request);
}

static ClusterKoSharedMessageV2 shared_request(uint64 id, uint64 epoch)
{
	ClusterKoSharedMessageV2 m = {0};
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	Assert(ko_shared_members(&m, boots, &generation));
	m.verb = CLUSTER_KO_SHARED_REQUEST;
	m.batch_id = id;
	m.epoch = epoch;
	m.key = space_identity.key;
	memcpy(m.incarnation, space_identity.incarnation, 16);
	m.origin_node = 1;
	m.peer_node = 0;
	m.origin_boot = boots[1];
	m.peer_boot = boots[0];
	return m;
}
static void enqueue_shared(const ClusterKoSharedMessageV2 *m)
{
	ClusterICEnvelope env = {0};
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
	Assert(cluster_ko_shared_encode_v2(m, bytes, sizeof(bytes)));
	env.source_node_id = 1;
	env.payload_length = sizeof(bytes);
	cluster_ko_flush_request_handler(&env, bytes);
}
static void enqueue_epoch(uint64 id, uint64 epoch)
{
	if (cluster_shared_config) {
		ClusterKoSharedMessageV2 m = shared_request(id, epoch);
		enqueue_shared(&m);
	} else
		enqueue_legacy_epoch(id, epoch);
}
static void drain_and_send(void)
{
	cluster_ko_drain_inbound_and_apply();
	if (cluster_shared_config)
		shared_send_tick();
}

static void
enqueue(uint64 id)
{
	enqueue_epoch(id, current_epoch);
}

UT_TEST(test_only_actual_consumer_can_observe)
{
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
	reset_test();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmonProcess;
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
	MyAuxProcType = SinvalBcastProcess;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_real_admission_flush_drop_ack_order)
{
	reset_test();
	cluster_hw_lease_install((RelFileLocator){1663, 0, 99}, MAIN_FORKNUM, 4, 4);
	enqueue(42);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(ack_count, 0);
	drain_and_send();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(sync_count, 0); /* The original nonshared path is unchanged. */
	UT_ASSERT_EQ(drop_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 42);
	UT_ASSERT_EQ(last_ack.status, KO_FLUSH_ACK_DONE);
	UT_ASSERT_EQ(ack_count, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_consumed), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_orphan_zero), 4);
}

UT_TEST(test_origin_barrier_discards_lease_even_without_remote_work)
{
	RelFileLocator locator = {1663, 0, 99};

	for (int shared = 0; shared <= 1; shared++) {
		reset_test();
		cluster_shared_config = shared;
		formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
		cluster_hw_lease_install(locator, MAIN_FORKNUM, 4, 4);
		cluster_ko_flush_and_wait_ack(locator, RELPERSISTENCE_PERMANENT);
		UT_ASSERT_EQ(cluster_hw_lease_next_block(locator, MAIN_FORKNUM), InvalidBlockNumber);
		UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_consumed), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&leases.d_orphan_zero), 4);
		UT_ASSERT_EQ(liveness_calls, 0); /* Shared uses the full member cut, not liveness. */
		UT_ASSERT_EQ(lock_calls, shared);
		UT_ASSERT_EQ(cluster_ko_native_count(), shared);
		if (shared) {
			UT_ASSERT_EQ(completion_allocations, 1);
			resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
			UT_ASSERT_EQ(completion_allocations, 0);
		}
	}
}
UT_TEST(test_full_ring_wrap_and_retained_stale_bytes)
{
	reset_test();
	for (int i = 1; i <= 64; i++)
		enqueue(i);
	UT_ASSERT_EQ(cluster_ko_inbound_full_count(), 1);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	drain_and_send();
	UT_ASSERT_EQ(ack_count, 63);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	enqueue(65);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	drain_and_send();
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(last_ack.batch_id, 65);
}
UT_TEST(test_later_invalid_slot_overrides_pending)
{
	uint32 slot;
	const char *reason;
	reset_test();
	enqueue(1);
	enqueue(2);
	storage.inbound[1].source_node = CLUSTER_MAX_NODES;
	UT_ASSERT_EQ(cluster_ko_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(slot, 1);
	storage.inbound[1].source_node = 1;
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_write_u32(&storage.inbound_tail, CLUSTER_KO_INBOUND_CAPACITY);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_refused_removal_never_forges_apply_ack)
{
	reset_test();
	removal_ready = false;
	enqueue(7);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
	drain_and_send();
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(flush_count, 0);
	/* The original refusal consumes the local request, not its remote ACK
	 * wait. The separate actual SI wait-table poll covers that responsibility. */
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_sealed_new_request_never_enqueues_or_acks)
{
	ClusterICEnvelope env = { 0 };
	KoFlushHeader request = { 0 };
	reset_test();
	gate_admit = false;
	cluster_ko_flush_request_handler(&env, &request);
	UT_ASSERT_EQ(gate_calls, 0); /* Original validation still first. */
	enqueue(80);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ack_count, 0);
	reset_test();
	enqueue(81);
	UT_ASSERT_EQ(gate_calls, 1);
	drain_and_send();
	UT_ASSERT_EQ(ack_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 81);
}

UT_TEST(test_shared_existing_forks_synced_before_drop_and_ack)
{
	for (int all_forks = 0; all_forks <= 1; all_forks++) {
		reset_test();
		cluster_shared_config = true;
		if (all_forks)
			existing_forks = (1u << (MAX_FORKNUM + 1)) - 1;
		enqueue(91);
		drain_and_send();
		UT_ASSERT_EQ(synced_forks, existing_forks);
		UT_ASSERT_EQ(sync_count, all_forks ? MAX_FORKNUM + 1 : 3);
		UT_ASSERT_EQ(drop_count, 1);
		UT_ASSERT_EQ(ack_count, 1);
		UT_ASSERT_EQ(last_ack.epoch, 1);
	}
	reset_test();
	cluster_shared_config = true;
	shared_relation = false;
	enqueue(92);
	drain_and_send();
	UT_ASSERT_EQ(sync_count, 0);
	UT_ASSERT_EQ(ack_count, 1);
}
UT_TEST(test_shared_sync_failure_preserves_buffers_and_never_acks)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		volatile bool caught = false;
		reset_test();
		cluster_shared_config = true;
		existing_forks = (1u << (MAX_FORKNUM + 1)) - 1;
		sync_fail_fork = fork;
		enqueue(93);
		PG_TRY();
		{
			drain_and_send();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(flush_count, 1);
		UT_ASSERT_EQ(sync_count, fork + 1);
		UT_ASSERT_EQ(drop_count, 0);
		UT_ASSERT_EQ(ack_count, 0);
		UT_ASSERT_EQ(cluster_ko_peer_apply_count(), 0);
		UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
		drain_and_send();
		UT_ASSERT_EQ(flush_count, 1); /* Original failed request is not retried. */
	}
}
UT_TEST(test_queued_old_epoch_refused_before_page_work)
{
	reset_test();
	enqueue(94);
	current_epoch++;
	drain_and_send();
	UT_ASSERT_EQ(flush_count, 0);
	UT_ASSERT_EQ(drop_count, 0);
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_epoch_change_during_io_never_acks_new_epoch)
{
	for (int phase = 0; phase < 3; phase++) {
		reset_test();
		cluster_shared_config = true;
		epoch_change_flush = phase == 0;
		epoch_change_sync_fork = phase == 1 ? MAIN_FORKNUM : -1;
		epoch_change_drop = phase == 2;
		enqueue(95);
		drain_and_send();
		UT_ASSERT_EQ(current_epoch, 2);
		UT_ASSERT_EQ(flush_count, 1);
		UT_ASSERT_EQ(drop_count, phase == 2 ? 1 : 0);
		UT_ASSERT_EQ(ack_count, 0);
		UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	}
}
UT_TEST(test_request_requires_exact_nonzero_epoch)
{
	reset_test();
	current_epoch = 3;
	enqueue_epoch(96, 0);
	enqueue_epoch(96, 2);
	enqueue_epoch(96, 4);
	UT_ASSERT_EQ(gate_calls, 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
	enqueue_epoch(97, 3);
	drain_and_send();
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(ack_count, 1);
	UT_ASSERT_EQ(last_ack.batch_id, 97);
	UT_ASSERT_EQ(last_ack.epoch, 3);
	reset_test();
	current_epoch = 0;
	enqueue_epoch(98, 0);
	UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_ack_requires_exact_nonzero_epoch)
{
	ClusterICEnvelope env = { 0 };
	KoFlushAckHeader ack = { 0 };
	reset_test();
	current_epoch = 3;
	env.source_node_id = 1;
	env.payload_length = sizeof(ack);
	ack.batch_id = 99;
	ack.acker_node = 1;
	ack.status = KO_FLUSH_ACK_DONE;
	for (int epoch = 0; epoch <= 4; epoch++) {
		if (epoch == 3)
			continue;
		ack.epoch = epoch;
		cluster_ko_flush_ack_handler(&env, &ack);
	}
	UT_ASSERT_EQ(record_count, 0);
	ack.epoch = 3;
	cluster_ko_flush_ack_handler(&env, &ack);
	UT_ASSERT_EQ(record_count, 1);
	UT_ASSERT_EQ(recorded_batch, 99);
	UT_ASSERT_EQ(cluster_ko_ack_received_count(), 1);
	current_epoch = ack.epoch = 0;
	cluster_ko_flush_ack_handler(&env, &ack);
	UT_ASSERT_EQ(record_count, 1);
}
UT_TEST(test_origin_completion_stays_in_request_epoch)
{
	RelFileLocator locator = { 1663, 0, 99 };
	for (int phase = 0; phase < 3; phase++) {
		reset_test();
		barrier_complete = phase != 1;
		epoch_change_complete = phase == 2;
		UT_ASSERT_EQ(ko_run_barrier(locator, 1u << 1), phase == 0);
		UT_ASSERT_EQ(barrier_requests, 1);
		UT_ASSERT_EQ(last_request.epoch, 1);
		UT_ASSERT_EQ(barrier_waits, phase == 1 ? 1 : 0);
		UT_ASSERT_EQ(barrier_removes, 1);
		UT_ASSERT(!barrier_active);
	}
}

UT_TEST(test_shared_legacy_request_and_ack_refuse_before_work)
{
	ClusterICEnvelope env = {0};
	KoFlushAckHeader ack = {0};
	reset_test();
	cluster_shared_config = true;
	enqueue_legacy_epoch(99, current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
	drain_and_send();
	UT_ASSERT_EQ(flush_count, 0);
	UT_ASSERT_EQ(drop_count, 0);
	UT_ASSERT_EQ(ack_count, 0);
	ack.batch_id = 99;
	ack.epoch = current_epoch;
	ack.acker_node = 1;
	env.source_node_id = 1;
	env.payload_length = sizeof(ack);
	cluster_ko_flush_ack_handler(&env, &ack);
	UT_ASSERT_EQ(record_count, 0);
}

UT_TEST(test_shared_member_digest_is_canonical_and_covers_every_boot)
{
	const uint8 golden[32] = {
		0x50,0xb5,0xdc,0xc7,0xd8,0x37,0xf4,0xeb,0x87,0x2f,0x6d,0xda,0xa1,0xd0,0x7a,0xe1,
		0x47,0x00,0xa8,0x14,0xc1,0xcd,0xb0,0xce,0x18,0x83,0x91,0x27,0x35,0xe9,0xbf,0x87
	};
	ClusterKoSharedMessageV2 before = {0}, after = {0};
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	reset_test();
	UT_ASSERT(ko_shared_members(&before, boots, &generation));
	UT_ASSERT_EQ(memcmp(before.member_digest, golden, sizeof(golden)), 0);
	UT_ASSERT_EQ(before.members[0], 3);
	UT_ASSERT_EQ(boots[0], 11);
	UT_ASSERT_EQ(boots[1], 22);
	UT_ASSERT_EQ(generation, 2);
	/* Observer metadata differs legitimately on the same accepted cut. */
	formation.applied.observer_role = CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
	formation.applied.applied_at = 90;
	formation.applied.event_seq = 99;
	UT_ASSERT(ko_shared_members(&after, boots, &generation));
	UT_ASSERT_EQ(memcmp(before.member_digest, after.member_digest, 32), 0);
	formation.membership.last_admitted_incarnation[127] = 70;
	UT_ASSERT(ko_shared_members(&after, boots, &generation));
	UT_ASSERT_NE(memcmp(before.member_digest, after.member_digest, 32), 0);
}

UT_TEST(test_shared_ingress_rejects_changed_cut_before_any_page_io)
{
	for (int fault = 0; fault < 14; fault++) {
		ClusterKoSharedMessageV2 message;
		reset_test();
		cluster_shared_config = true;
		message = shared_request(123, current_epoch);
		switch (fault) {
		case 0: message.origin_boot++; break;
		case 1: message.peer_boot++; break;
		case 2: message.key.system_identifier++; break;
		case 3: message.key.database_incarnation++; break;
		case 4: message.key.storage_uuid[1]++; break;
		case 5: message.member_digest[0] ^= 1; break;
		case 6: cap_ok = false; break;
		case 7: capture_ok = false; break;
		case 8: generation_race = true; break;
		case 9: formation.membership.last_admitted_incarnation[1]++; break;
		case 10: formation.pending_join_bitmap[1] = 1; break;
		case 11: formation.prebump_sync_active = 1; break;
		case 12: formation.membership.membership_state[1] = CLUSTER_MEMBER_DEAD; break;
		case 13: writer.claim.identity.origin_owner_incarnation++; break;
		}
		enqueue_shared(&message);
		UT_ASSERT_EQ(pg_atomic_read_u32(&storage.inbound_tail), 0);
		UT_ASSERT_EQ(gate_calls, 0);
		drain_and_send();
		UT_ASSERT_EQ(space_reads, 0);
		UT_ASSERT_EQ(flush_count, 0);
		UT_ASSERT_EQ(ack_count, 0);
	}
}

UT_TEST(test_shared_old_segment_and_queued_boot_change_never_flush)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterKoSharedMessageV2 message;
		reset_test();
		cluster_shared_config = true;
		message = shared_request(124, current_epoch);
		enqueue_shared(&message);
		UT_ASSERT_EQ(poll_queue(), CLUSTER_NORMAL_STOP_PENDING);
		if (fault == 0)
			space_identity.incarnation[0]++;
		else if (fault == 1)
			formation.membership.last_admitted_incarnation[1]++;
		else
			space_ok = false;
		drain_and_send();
		UT_ASSERT_EQ(flush_count, 0);
		UT_ASSERT_EQ(drop_count, 0);
		UT_ASSERT_EQ(ack_count, 0);
		UT_ASSERT_EQ(space_reads, fault == 1 ? 0 : 1);
	}
}

UT_TEST(test_shared_same_epoch_boot_change_during_each_io_never_acks)
{
	for (int phase = 1; phase <= 3; phase++) {
		reset_test();
		cluster_shared_config = true;
		enqueue(125);
		boot_change_phase = phase;
		drain_and_send();
		UT_ASSERT_EQ(current_epoch, 1);
		UT_ASSERT_EQ(flush_count, 1);
		UT_ASSERT_EQ(drop_count, phase == 3 ? 1 : 0);
		UT_ASSERT_EQ(ack_count, 0);
		UT_ASSERT_EQ(storage.send_count, 0);
	}
}

UT_TEST(test_shared_ack_send_revalidates_boot_and_normal_stop_owner)
{
	const char *reason;
	reset_test();
	cluster_shared_config = true;
	enqueue(126);
	cluster_ko_drain_inbound_and_apply();
	UT_ASSERT_EQ(flush_count, 1);
	UT_ASSERT_EQ(drop_count, 1);
	UT_ASSERT_EQ(space_reads, 2); /* No read recreates SPACE after invalidation. */
	UT_ASSERT_EQ(ack_count, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	formation.membership.last_admitted_incarnation[0]++;
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_shared_origin_uses_real_ack_handler_and_original_wait_entry)
{
	const char *reason;
	reset_test();
	cluster_shared_config = true;
	drive_shared_ack = true;
	UT_ASSERT(ko_run_shared_barrier(space_identity.key.locator));
	UT_ASSERT_EQ(barrier_requests, 1);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(record_count, 1);
	UT_ASSERT_EQ(last_shared_request.origin_boot, 11);
	UT_ASSERT_EQ(last_shared_request.peer_boot, 22);
	UT_ASSERT_EQ(barrier_removes, 1);
	UT_ASSERT(!barrier_active);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_shared_origin_missing_capability_has_no_barrier_or_send)
{
	reset_test();
	cluster_shared_config = true;
	cap_ok = false;
	UT_ASSERT(!ko_run_shared_barrier(space_identity.key.locator));
	UT_ASSERT_EQ(storage.send_count, 0);
	UT_ASSERT_EQ(barrier_requests, 0);
	UT_ASSERT_EQ(barrier_removes, 0);
	UT_ASSERT_EQ(flush_count, 0);
	UT_ASSERT(!barrier_active);
}

UT_TEST(test_shared_cancel_releases_context_and_suppresses_unsent_request)
{
	volatile bool caught = false;
	const char *reason;
	reset_test();
	cluster_shared_config = true;
	barrier_complete = false;
	cancel_wait = true;
	PG_TRY();
	{
		(void)ko_run_shared_barrier(space_identity.key.locator);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(barrier_removes, 1);
	UT_ASSERT(!barrier_active);
	UT_ASSERT_EQ(storage.send_count, 1);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_shared_ack_needs_exact_registered_original_scope)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterKoSharedMessageV2 request, ack;
		ClusterICEnvelope env = {0};
		uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
		reset_test();
		cluster_shared_config = true;
		request = shared_request(912, current_epoch);
		request.origin_node = 0;
		request.origin_boot = 11;
		request.peer_node = 1;
		request.peer_boot = 22;
		storage.contexts[0].used = true;
		storage.contexts[0].pid = MyProcPid;
		storage.contexts[0].request = request;
		storage.contexts[0].peer_boots[1] = 22;
		ack = request;
		ack.verb = CLUSTER_KO_SHARED_ACK;
		ack.status = CLUSTER_KO_SHARED_DONE;
		switch (fault) {
		case 0: ack.batch_id++; break;
		case 1: ack.incarnation[1]++; break;
		case 2: ack.key.locator.relNumber++; break;
		case 3: ack.peer_boot++; break;
		case 4: ack.status = CLUSTER_KO_SHARED_FAILED; break;
		case 5: storage.contexts[0].used = false; break;
		default: break;
		}
		UT_ASSERT(cluster_ko_shared_encode_v2(&ack, bytes, sizeof(bytes)));
		env.source_node_id = 1;
		env.payload_length = sizeof(bytes);
		cluster_ko_flush_ack_handler(&env, bytes);
		UT_ASSERT_EQ(record_count, fault == 6 ? 1 : 0);
	}
}

UT_TEST(test_shared_capacity_and_backend_exit_preserve_other_owners)
{
	ClusterKoSharedMessageV2 request;
	const char *reason;
	reset_test();
	cluster_shared_config = true;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		storage.contexts[i].used = true;
		storage.contexts[i].pid = MyProcPid + 1;
	}
	UT_ASSERT(!ko_run_shared_barrier(space_identity.key.locator));
	UT_ASSERT(!barrier_active);
	UT_ASSERT_EQ(storage.send_count, 0);
	UT_ASSERT_EQ(barrier_removes, 0);
	request = shared_request(912, current_epoch);
	request.origin_node = 0;
	request.origin_boot = 11;
	request.peer_node = 1;
	request.peer_boot = 22;
	storage.contexts[0].pid = MyProcPid;
	storage.contexts[0].request = request;
	storage.contexts[0].peer_boots[1] = 22;
	UT_ASSERT(cluster_sinval_ack_wait_begin(912, 2, 100));
	UT_ASSERT(ko_shared_enqueue(&request));
	UT_ASSERT(exit_callback != NULL);
	exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(barrier_removes, 1);
	UT_ASSERT(!storage.contexts[0].used);
	UT_ASSERT(storage.contexts[1].used);
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(test_shared_completion_survives_original_wait_and_checks_scope)
{
	ClusterKoCompletionV2 *completion = NULL;
	ClusterKoSharedMessageV2 projection, unchanged;
	ClusterSpaceIdentityKey key;
	uint8 incarnation[16];
	const char *reason;
	reset_test();
	cluster_shared_config = drive_shared_ack = true;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &completion));
	UT_ASSERT_NOT_NULL(completion);
	UT_ASSERT_EQ(barrier_removes, 1);
	UT_ASSERT(!barrier_active);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(strcmp(reason, "KO_SHARED_COMPLETION_OWNED"), 0);
	key = space_identity.key;
	memcpy(incarnation, space_identity.incarnation, 16);
	UT_ASSERT(cluster_ko_shared_covers_v2(completion, &key, incarnation));
	UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &projection));
	UT_ASSERT_EQ(projection.batch_id, last_shared_request.batch_id);
	UT_ASSERT_EQ(projection.origin_boot, 11);
	UT_ASSERT_EQ(projection.peer_boot, 22);
	UT_ASSERT_EQ(projection.verb, CLUSTER_KO_SHARED_REQUEST);
	UT_ASSERT_EQ(projection.status, 0);
	/* A structural owner consumes the old identity after publication; the
	 * receipt does not mistake a newly published identity for its old scope. */
	space_identity.incarnation[0]++;
	UT_ASSERT(cluster_ko_shared_covers_v2(completion, &key, incarnation));
	UT_ASSERT(!cluster_ko_shared_covers_v2(completion, &key, space_identity.incarnation));
	key.locator.relNumber++;
	UT_ASSERT(!cluster_ko_shared_covers_v2(completion, &key, incarnation));
	memset(&projection, 0x93, sizeof(projection));
	unchanged = projection;
	UT_ASSERT(!cluster_ko_shared_read_v2(completion, 0, &projection));
	UT_ASSERT(!cluster_ko_shared_read_v2(completion, 2, &projection));
	UT_ASSERT_EQ(memcmp(&projection, &unchanged, sizeof(projection)), 0);
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT(completion == NULL);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(barrier_removes, 1); /* No second removal of the wait entry. */
}

UT_TEST(test_shared_completion_rejects_other_owner_and_every_changed_cut)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterKoCompletionV2 *completion = NULL;
		ClusterKoSharedMessageV2 projection, before;
		reset_test();
		cluster_shared_config = drive_shared_ack = true;
		UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
			RELPERSISTENCE_PERMANENT, &completion));
		memset(&projection, 0xa2, sizeof(projection));
		before = projection;
		switch (fault) {
		case 0: CurrentResourceOwner = (ResourceOwner)2; break;
		case 1: MyProcPid++; break;
		case 2: current_epoch++; break;
		case 3: formation.membership.last_admitted_incarnation[1]++; break;
		case 4: writer.claim.database_incarnation++; break;
		case 5: generation_race = true; break;
		case 6: cap_ok = false; break;
		}
		UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &projection));
		UT_ASSERT_EQ(memcmp(&projection, &before, sizeof(before)), 0);
		if (fault < 2) {
			cluster_ko_shared_release_v2(&completion);
			UT_ASSERT_NOT_NULL(completion);
		}
		CurrentResourceOwner = (ResourceOwner)1;
		if (fault == 1) MyProcPid--;
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT(completion == NULL);
	}
}

UT_TEST(test_shared_completion_cleanup_is_owner_scoped_and_no_peer_is_not_an_ack)
{
	ClusterKoCompletionV2 *completion = NULL, *other = NULL;
	ClusterKoSharedMessageV2 projection;
	const char *reason;
	reset_test();
	cluster_shared_config = true;
	formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &completion));
	UT_ASSERT_EQ(barrier_requests, 0);
	UT_ASSERT_EQ(barrier_removes, 0);
	UT_ASSERT_EQ(record_count, 0);
	UT_ASSERT(cluster_ko_shared_covers_v2(completion, &space_identity.key, space_identity.incarnation));
	UT_ASSERT(!cluster_ko_shared_read_v2(completion, 0, &projection));
	UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &projection));
	CurrentResourceOwner = (ResourceOwner)2;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &other));
	UT_ASSERT(resource_callback != NULL);
	resource_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, false, NULL);
	UT_ASSERT(cluster_ko_shared_covers_v2(other, &space_identity.key, space_identity.incarnation));
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT(!cluster_ko_shared_covers_v2(other, &space_identity.key, space_identity.incarnation));
	cluster_ko_shared_release_v2(&other);
	CurrentResourceOwner = (ResourceOwner)1;
	UT_ASSERT(cluster_ko_shared_covers_v2(completion, &space_identity.key, space_identity.incarnation));
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	exit_callback(0, (Datum)0);
	UT_ASSERT(!cluster_ko_shared_covers_v2(completion, &space_identity.key, space_identity.incarnation));
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(barrier_removes, 0);
}

UT_TEST(test_shared_completion_has_no_handle_after_cancel_or_unsupported_scope)
{
	ClusterKoCompletionV2 *completion = NULL;
	volatile bool caught = false;
	const char *reason;
	reset_test();
	UT_ASSERT(!cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &completion));
	cluster_shared_config = true;
	UT_ASSERT(!cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_TEMP, &completion));
	CurrentResourceOwner = NULL;
	UT_ASSERT(!cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &completion));
	CurrentResourceOwner = (ResourceOwner)1;
	UT_ASSERT_EQ(barrier_requests, 0);
	barrier_complete = false;
	cancel_wait = true;
	PG_TRY();
	{
		(void)cluster_ko_shared_begin_v2(space_identity.key.locator,
			RELPERSISTENCE_PERMANENT, &completion);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(completion == NULL);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(barrier_removes, 1);
	shared_send_tick();
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_shared_completion_raw_copy_and_slot_reuse_do_not_convey_ownership)
{
	ClusterKoCompletionV2 *completion = NULL;
	ClusterKoCompletionV2 copy;
	ClusterKoSharedMessageV2 projection;
	unsigned slot;
	reset_test();
	cluster_shared_config = drive_shared_ack = true;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &completion));
	copy = *completion;
	UT_ASSERT(!cluster_ko_shared_covers_v2(&copy, &space_identity.key, space_identity.incarnation));
	UT_ASSERT(!cluster_ko_shared_read_v2(&copy, 1, &projection));
	slot = completion->slot;
	/* Reusing a bounded slot, even for the same batch/PID, cannot revive or
	 * release an earlier completion. This models a changed original owner. */
	storage.contexts[slot].serial++;
	UT_ASSERT(!cluster_ko_shared_covers_v2(completion, &space_identity.key, space_identity.incarnation));
	UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &projection));
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT(completion == NULL);
	UT_ASSERT(storage.contexts[slot].used);
	UT_ASSERT(storage.contexts[slot].complete);
	memset(&storage.contexts[slot], 0, sizeof(storage.contexts[slot]));
}

UT_TEST(test_shared_subcommit_transfers_original_completion_to_parent)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		ClusterKoCompletionV2 *completion = NULL;
		ClusterKoSharedMessageV2 original, observed;
		int requests, removes;
		const char *reason;
		reset_test();
		cluster_shared_config = drive_shared_ack = true;
		CurrentResourceOwner = (ResourceOwner)3;
		UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
			RELPERSISTENCE_PERMANENT, &completion));
		UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &original));
		requests = barrier_requests;
		removes = barrier_removes;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
		UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &observed));
		CurrentResourceOwner = (ResourceOwner)2;
		UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &observed));
		UT_ASSERT(memcmp(&original, &observed, sizeof(original)) == 0);
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
		UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &observed));
		CurrentResourceOwner = (ResourceOwner)1;
		UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &observed));
		UT_ASSERT(memcmp(&original, &observed, sizeof(original)) == 0);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, commit, true, NULL);
		UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &observed));
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT(completion == NULL);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(barrier_requests, requests);
		UT_ASSERT_EQ(barrier_removes, removes);
	}
}

UT_TEST(test_shared_subabort_cancels_only_the_child_completion)
{
	ClusterKoCompletionV2 *parent = NULL, *child = NULL;
	const char *reason;
	reset_test();
	cluster_shared_config = true;
	/* This case isolates ResourceOwner cleanup; the preceding test exercises
	 * real remote ACK handling. No fabricated second ACK/batch is needed. */
	formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &parent));
	CurrentResourceOwner = (ResourceOwner)2;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator,
		RELPERSISTENCE_PERMANENT, &child));
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT(!cluster_ko_shared_covers_v2(child, &space_identity.key, space_identity.incarnation));
	cluster_ko_shared_release_v2(&child);
	CurrentResourceOwner = (ResourceOwner)1;
	UT_ASSERT(cluster_ko_shared_covers_v2(parent, &space_identity.key, space_identity.incarnation));
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	cluster_ko_shared_release_v2(&parent);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
}

/* A transaction keeps original proof handles, not completed transport slots.
 * DROP claims them at commit; TRUNCATE takes each immediately. Both use the
 * actual shared codec and ACK consumer for every distinct batch. */
UT_TEST(test_native_seventy_two_relations_do_not_hold_completed_transport_slots)
{
	for (unsigned truncate = 0; truncate < 2; truncate++) {
		ClusterKoCompletionV2 *handles[72] = { 0 };
		ClusterSpaceIdentityKey keys[72];
		volatile unsigned finished = 0;
		volatile bool caught = false;
		reset_test();
		cluster_shared_config = drive_shared_ack = multiple_barriers = true;
		expecting_error = true;
		PG_TRY();
		{
			for (unsigned i = 0; i < lengthof(handles); i++) {
				space_identity.key.locator.relNumber = 1000 + i;
				keys[i] = space_identity.key;
				cluster_ko_flush_and_wait_ack(keys[i].locator, RELPERSISTENCE_PERMANENT);
				if (truncate)
					UT_ASSERT(cluster_ko_shared_claim_v2(&keys[i], space_identity.incarnation,
														 &handles[i]));
				finished++;
			}
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(!caught);
		UT_ASSERT_EQ(finished, lengthof(handles));
		UT_ASSERT_EQ(record_count, finished);
		UT_ASSERT_EQ(barrier_removes, finished);
		for (unsigned i = 0; !caught && i < finished; i++) {
			ClusterKoSharedMessageV2 request;
			if (!truncate)
				UT_ASSERT(
					cluster_ko_shared_claim_v2(&keys[i], space_identity.incarnation, &handles[i]));
			UT_ASSERT(
				cluster_ko_shared_covers_v2(handles[i], &keys[i], space_identity.incarnation));
			UT_ASSERT(cluster_ko_shared_read_v2(handles[i], 1, &request));
			UT_ASSERT_EQ(request.batch_id, 912 + i);
		}
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
			UT_ASSERT(!storage.contexts[i].used);
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_shared_barrier_is_claimed_once_by_original_space_owner)
{
	ClusterKoCompletionV2 *completion = NULL, *second = NULL;
	ClusterKoSharedMessageV2 original, observed;
	const char *reason;
	int reads, requests, sends;
	reset_test();
	cluster_shared_config = drive_shared_ack = true;
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)3;
	cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	reads = space_reads;
	requests = barrier_requests;
	sends = send_calls;
	UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &completion));
	UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &original));
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &second));
	UT_ASSERT(second == NULL);
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &completion));
	UT_ASSERT(completion != NULL);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
	UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &observed));
	UT_ASSERT(memcmp(&observed, &original, sizeof(observed)) == 0);
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &second));
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
	UT_ASSERT(cluster_ko_shared_covers_v2(completion, &space_identity.key, space_identity.incarnation));
	/* Backend drain, not the shared transport poll, owns this live transaction. */
	UT_ASSERT_EQ(completion_allocations, 1);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!cluster_ko_shared_read_v2(completion, 1, &observed));
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(space_reads, reads);
	UT_ASSERT_EQ(barrier_requests, requests);
	UT_ASSERT_EQ(send_calls, sends);
	UT_ASSERT_EQ(completion_allocations, 0);
}

UT_TEST(test_native_completion_claim_refuses_changed_identity_and_owner)
{
	for (int fault = 0; fault < 15; fault++) {
		ClusterKoCompletionV2 *completion = NULL;
		ClusterSpaceIdentityKey key;
		uint8 incarnation[16];
		int reads, requests;
		reset_test();
		cluster_shared_config = drive_shared_ack = true;
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
		reads = space_reads;
		requests = barrier_requests;
		key = space_identity.key;
		memcpy(incarnation, space_identity.incarnation, sizeof(incarnation));
		switch (fault) {
		case 0: key.system_identifier++; break;
		case 1: key.database_incarnation++; break;
		case 2: key.storage_uuid[0]++; break;
		case 3: key.locator.dbOid++; break;
		case 4: key.locator.relNumber++; break;
		case 5: incarnation[0]++; break;
		case 6: CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2; break;
		case 7: current_epoch++; break;
		case 8: formation.membership.last_admitted_incarnation[1]++; break;
		case 9: writer.claim.identity.origin_owner_incarnation++; break;
		case 10: capture_ok = false; break;
		case 11: generation_race = true; break;
		case 12: CritSectionCount++; break;
		case 13: resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL); break;
		case 14: formation.pending_join_bitmap[0] = 2; break;
		}
		UT_ASSERT(!cluster_ko_shared_claim_v2(&key, incarnation, &completion));
		UT_ASSERT(completion == NULL);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(barrier_requests, requests);
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_claim_cannot_borrow_direct_handle_and_full_capacity_refuses_before_send)
{
	ClusterKoCompletionV2 *direct = NULL, *claimed = NULL;
	volatile bool caught = false;
	reset_test();
	cluster_shared_config = drive_shared_ack = true;
	UT_ASSERT(cluster_ko_shared_begin_v2(space_identity.key.locator, RELPERSISTENCE_PERMANENT, &direct));
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &claimed));
	UT_ASSERT(claimed == NULL);
	cluster_ko_shared_release_v2(&direct);
	reset_test();
	cluster_shared_config = true;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		storage.contexts[i].used = true;
		storage.contexts[i].pid = MyProcPid + 1;
	}
	expecting_error = true;
	PG_TRY();
	{
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(send_calls + barrier_requests + flush_count, 0);
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &claimed));
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		UT_ASSERT(storage.contexts[i].used && storage.contexts[i].pid == MyProcPid + 1);
}

UT_TEST(test_native_claim_rejects_ambiguous_repeated_barriers)
{
	ClusterKoCompletionV2 *claimed = NULL;
	int reads, requests;
	reset_test();
	cluster_shared_config = true;
	formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
	for (unsigned i = 0; i < 2; i++)
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	reads = space_reads;
	requests = barrier_requests;
	UT_ASSERT_EQ(completion_allocations, 2);
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &claimed));
	UT_ASSERT(claimed == NULL);
	UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &claimed));
	UT_ASSERT_EQ(space_reads, reads);
	UT_ASSERT_EQ(barrier_requests, requests);
	UT_ASSERT_EQ(completion_allocations, 2);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(completion_allocations, 0);
}

UT_TEST(test_native_portal_barrier_survives_precommit_without_portal_release)
{
	for (unsigned immediate = 0; immediate < 2; immediate++) {
		ClusterKoCompletionV2 *completion = NULL;
		int reads, requests, sends;
		reset_test();
		cluster_shared_config = drive_shared_ack = true;
		/* PortalRun uses the portal child while the original transaction
		 * owner remains current for transaction resources. */
		CurrentResourceOwner = (ResourceOwner)2;
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
		reads = space_reads;
		requests = barrier_requests;
		sends = send_calls;
		if (immediate)
			UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key,
				space_identity.incarnation, &completion));
		/* PreCommit_Portals -> PortalDrop(true) does NOT release a successful
		 * portal's resources before RecordTransactionCommit prepares SPACE. */
		CurrentResourceOwner = (ResourceOwner)1;
		if (!immediate)
			UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key,
				space_identity.incarnation, &completion));
		UT_ASSERT(cluster_ko_shared_covers_v2(completion,
			&space_identity.key, space_identity.incarnation));
		/* Original recursive transaction cleanup, child before parent. */
		CurrentResourceOwner = (ResourceOwner)2;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
		CurrentResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(barrier_requests, requests);
		UT_ASSERT_EQ(send_calls, sends);
	}
}

UT_TEST(test_native_portal_subtransaction_keeps_exact_transaction_scope)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		ClusterKoCompletionV2 *parent = NULL, *child = NULL;
		reset_test();
		cluster_shared_config = true;
		formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
		CurTransactionResourceOwner = (ResourceOwner)2;
		CurrentResourceOwner = (ResourceOwner)3;
		UT_ASSERT(!cluster_ko_shared_claim_v2(&space_identity.key,
			space_identity.incarnation, &child));
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
		UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key,
			space_identity.incarnation, &child));
		/* Portal cleanup cannot retire the subtransaction's responsibility. */
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
		UT_ASSERT(cluster_ko_shared_covers_v2(child,
			&space_identity.key, space_identity.incarnation));
		CurrentResourceOwner = (ResourceOwner)2;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, commit, false, NULL);
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		UT_ASSERT_EQ(cluster_ko_shared_covers_v2(child,
			&space_identity.key, space_identity.incarnation), commit);
		UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key,
			space_identity.incarnation, &parent));
		UT_ASSERT(cluster_ko_shared_covers_v2(parent,
			&space_identity.key, space_identity.incarnation));
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&child);
		cluster_ko_shared_release_v2(&parent);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_barrier_rejects_missing_or_unrelated_transaction_before_io)
{
	for (unsigned unrelated = 0; unrelated < 2; unrelated++) {
		volatile bool caught = false;
		reset_test();
		cluster_shared_config = drive_shared_ack = true;
		CurTransactionResourceOwner = unrelated ? (ResourceOwner)2 : NULL;
		expecting_error = true;
		PG_TRY();
		{
			cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(reported_sqlstate, ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(space_reads + barrier_requests + send_calls, 0);
		/* Also dispose of the old implementation's incorrectly accepted
		 * completion so every RED case can run and be classified. */
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	}
}

static ClusterKoCompletionV2 *
prepare_native_structure_owner(bool drop, ClusterPageWalBindingV1 *binding, uint8 *wal,
	ResourceOwner owner, bool remote)
{
	ClusterKoCompletionV2 *completion = NULL;
	ClusterSpaceStructureChange change = {0};
	reset_test();
	CurrentResourceOwner = CurTransactionResourceOwner = owner;
	cluster_shared_config = drive_shared_ack = true;
	if (!remote) {
		formation.membership.membership_state[1] = CLUSTER_MEMBER_ABSENT;
		drive_shared_ack = false;
	}
	writer.claim.identity.origin_thread_id = 1;
	writer.claim.identity.thread_claim_created_at = 123;
	writer.claim.identity.root_lineage_seq = 1;
	writer.claim.identity.authority_uuid[0] = 17;
	writer.claim.max_config_generation = 1;
	writer.claim.claim_sha256[0] = 19;
	writer.timeline = 1;
	space_identity.key.locator.dbOid = 44;
	space_identity.sequence = 1;
	space_identity.operation = 10;
	cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &completion));
	change.identity.expected = space_identity;
	change.identity.result = space_identity;
	change.identity.result.sequence++;
	change.identity.result.operation = change.identity.result_token = 20;
	change.identity.before_token = 10;
	change.identity.action = drop ? CLUSTER_SPACE_WAL_TOMBSTONE : CLUSTER_SPACE_WAL_TRUNCATE;
	change.identity.nblocks = drop ? InvalidBlockNumber : 4;
	if (drop)
		change.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	else
		change.identity.result.incarnation[0]++;
	change.reservation.action = drop ? CLUSTER_SPACE_RESERVATION_TOMBSTONE : CLUSTER_SPACE_RESERVATION_RESET;
	change.reservation.before.identity = space_identity;
	change.reservation.before.next_block = 8;
	change.reservation.before_token = 10;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result.next_block = drop ? 8 : 4;
	change.reservation.first_block = drop ? 0 : 4;
	change.reservation.result_token = 20;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
	memset(binding, 0, sizeof(*binding));
	binding->source = writer;
	binding->identity.system_identifier = space_identity.key.system_identifier;
	memcpy(binding->identity.storage_uuid, space_identity.key.storage_uuid, 16);
	binding->identity.locator = space_identity.key.locator;
	binding->identity.forknum = SPACE_FORKNUM;
	memcpy(binding->version.segment_incarnation, change.identity.result.incarnation, 16);
	binding->version.mutation_token = 20;
	binding->record_start = 0x100;
	binding->record_end = 0x180;
	binding->record_crc = 7;
	binding->rmid = drop ? RM_XACT_ID : RM_SMGR_ID;
	binding->info = drop ? XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO : XLOG_SMGR_SPACE_IDENTITY;
	binding->flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	UT_ASSERT(cluster_page_wal_binding_shape_v1(binding));
	return completion;
}

/* Observe the one actual shared structural owner created by these cases. */
static unsigned
structure_owned_slot(void)
{
	unsigned slot = CLUSTER_KO_SHARED_CAPACITY;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (storage.contexts[i].used && storage.contexts[i].structure_owned) {
			UT_ASSERT_EQ(slot, CLUSTER_KO_SHARED_CAPACITY);
			slot = i;
		}
	Assert(slot < CLUSTER_KO_SHARED_CAPACITY);
	return slot;
}

static ClusterKoCompletionV2 *
prepare_native_structure(bool drop, ClusterPageWalBindingV1 *binding, uint8 *wal)
{
	return prepare_native_structure_owner(drop, binding, wal, (ResourceOwner)1, true);
}

/* Use the original KO output as-is at the real PPWB codec boundary. A
 * separately corrected fixture must not hide an incompatible native offer. */
static void
assert_structure_offer_codec(const ClusterPiWritebackFactV2 *offer)
{
	const ClusterKoSharedMessageV2 *ko = &offer->proof.structural.ko;
	ClusterPiWritebackMessageV2 message = {0}, decoded;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	Size length = 0;
	message.verb = CLUSTER_PI_WRITEBACK_NOTIFY;
	message.count = 1;
	message.nonce = ko->batch_id;
	message.epoch = ko->epoch;
	message.peer = offer->proof.structural.terminal.binding.source;
	message.peer.claim.identity.origin_node_id = ko->peer_node;
	message.peer.claim.identity.origin_thread_id = ko->peer_node + 1;
	message.peer.claim.identity.origin_owner_incarnation = ko->peer_boot;
	message.facts[0] = *offer;
	UT_ASSERT(cluster_pi_writeback_encode_v2(&message, bytes, sizeof(bytes), &length));
	if (length != 0) {
		UT_ASSERT(cluster_pi_writeback_decode_v2(bytes, length, &decoded));
		UT_ASSERT(memcmp(&decoded.facts[0], offer, sizeof(*offer)) == 0);
	}
	message.facts[0].proof.structural.durability_flags ^= CLUSTER_PI_STRUCTURAL_BASE_DURABLE;
	UT_ASSERT(!cluster_pi_writeback_encode_v2(&message, bytes, sizeof(bytes), &length));
}

UT_TEST(test_native_space_observation_is_original_once_and_transaction_owned)
{
	for (unsigned drop = 0; drop < 2; drop++) {
		ClusterPageWalBindingV1 binding, observed = {0};
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], copied[sizeof(wal)] = {0};
		ClusterKoCompletionV2 *completion = prepare_native_structure(drop, &binding, wal);
		int reads = space_reads, requests = barrier_requests;
		UT_ASSERT(!cluster_ko_shared_space_observation_v2(completion, &observed, copied, sizeof(copied)));
		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		UT_ASSERT(!cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		UT_ASSERT(cluster_ko_shared_space_observation_v2(completion, &observed, copied, sizeof(copied)));
		UT_ASSERT(memcmp(&observed, &binding, sizeof(binding)) == 0);
		UT_ASSERT(memcmp(copied, wal, sizeof(wal)) == 0);
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		UT_ASSERT(!cluster_ko_shared_space_observation_v2(completion, &observed, copied, sizeof(copied)));
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(barrier_requests, requests);
	}
}

UT_TEST(test_native_space_observation_refuses_foreign_or_changed_record)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		ClusterPageWalBindingV1 binding, observed, before;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], copied[sizeof(wal)], previous[sizeof(wal)];
		ClusterKoCompletionV2 *completion = prepare_native_structure(fault % 2, &binding, wal);
		Size len = sizeof(wal);
		switch (fault) {
		case 0: binding.flags = 0; break;
		case 1: binding.source.claim.identity.origin_owner_incarnation++; break;
		case 2: binding.source.claim.claim_sha256[0]++; break;
		case 3: binding.source.timeline++; break;
		case 4: binding.identity.forknum = MAIN_FORKNUM; break;
		case 5: binding.identity.blockno = 1; break;
		case 6: binding.identity.locator.relNumber++; break;
		case 7: binding.version.segment_incarnation[0]++; break;
		case 8: binding.version.mutation_token++; break;
		case 9: binding.rmid = RM_HEAP_ID; break;
		case 10: binding.info = XLOG_SMGR_TRUNCATE; break;
		case 11: binding.record_start = binding.record_end; break;
		case 12: wal[40] ^= 1; break;
		case 13: len--; break;
		case 14: CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2; break;
		case 15: current_epoch++; break;
		case 16: resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL); break;
		}
		memset(&before, 0x5a, sizeof(before));
		memset(previous, 0x5a, sizeof(previous));
		observed = before;
		memcpy(copied, previous, sizeof(copied));
		UT_ASSERT(!cluster_ko_shared_observe_space_v2(completion, &binding, wal, len));
		UT_ASSERT(!cluster_ko_shared_space_observation_v2(completion, &observed, copied, sizeof(copied)));
		UT_ASSERT(memcmp(&observed, &before, sizeof(before)) == 0);
		UT_ASSERT(memcmp(copied, previous, sizeof(previous)) == 0);
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_truncate_effect_needs_original_space_and_is_once_only)
{
	ClusterPageWalBindingV1 binding, observed = {0};
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], copied[sizeof(wal)] = {0};
	ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
	int reads = space_reads, requests = barrier_requests;

	UT_ASSERT(!cluster_ko_shared_observe_truncate_v2(completion));
	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(!cluster_ko_shared_truncate_observation_v2(completion, &observed, copied, sizeof(copied)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	UT_ASSERT(!cluster_ko_shared_observe_truncate_v2(completion));
	UT_ASSERT(cluster_ko_shared_truncate_observation_v2(completion, &observed, copied, sizeof(copied)));
	UT_ASSERT(memcmp(&binding, &observed, sizeof(binding)) == 0);
	UT_ASSERT(memcmp(wal, copied, sizeof(wal)) == 0);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!cluster_ko_shared_truncate_observation_v2(completion, &observed, copied, sizeof(copied)));
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(space_reads, reads);
	UT_ASSERT_EQ(barrier_requests, requests);
}

UT_TEST(test_native_truncate_effect_refuses_drop_late_or_changed_owner)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterPageWalBindingV1 binding, observed, saved;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], copied[sizeof(wal)], before[sizeof(wal)];
		ClusterKoCompletionV2 *completion = prepare_native_structure(fault == 0, &binding, wal);

		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		if (fault == 1)
			CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		else if (fault == 2)
			current_epoch++;
		else if (fault == 3)
			writer.claim.claim_sha256[0]++;
		else if (fault == 4)
			resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		else if (fault == 5)
			xact_callback(XACT_EVENT_COMMIT, NULL);
		memset(&saved, 0x5a, sizeof(saved));
		memset(before, 0x5a, sizeof(before));
		observed = saved;
		memcpy(copied, before, sizeof(copied));
		UT_ASSERT(!cluster_ko_shared_observe_truncate_v2(completion));
		UT_ASSERT(!cluster_ko_shared_truncate_observation_v2(completion, &observed, copied, sizeof(copied)));
		UT_ASSERT(memcmp(&observed, &saved, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(copied, before, sizeof(before)) == 0);
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_truncate_effect_export_rechecks_cut_without_changing_output)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterPageWalBindingV1 binding, observed, saved;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], copied[sizeof(wal)], before[sizeof(wal)];
		ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
		Size len = sizeof(copied);

		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
		if (fault == 0)
			len--;
		else if (fault == 1)
			CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		else if (fault == 2)
			current_epoch++;
		else if (fault == 3)
			writer.claim.claim_sha256[0]++;
		else if (fault == 4)
			formation.membership.last_admitted_incarnation[1]++;
		else
			resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		memset(&saved, 0x5a, sizeof(saved));
		memset(before, 0x5a, sizeof(before));
		observed = saved;
		memcpy(copied, before, sizeof(copied));
		UT_ASSERT(!cluster_ko_shared_truncate_observation_v2(completion, &observed, copied, len));
		UT_ASSERT(memcmp(&observed, &saved, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(copied, before, sizeof(before)) == 0);
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_relation_offer_requires_real_commit_and_original_effect)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterPiWritebackFactV2 offer, before;
	ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
	ClusterKoSharedMessageV2 ko;

	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	memset(&before, 0xa5, sizeof(before));
	offer = before;
	UT_ASSERT(!cluster_ko_shared_structure_offer_v2(completion, 1, &offer));
	UT_ASSERT(memcmp(&offer, &before, sizeof(before)) == 0);
	xact_callback(XACT_EVENT_COMMIT, NULL);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
	UT_ASSERT(cluster_ko_shared_read_v2(completion, 1, &ko));
	UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &offer));
	UT_ASSERT_EQ(offer.kind, CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2);
	UT_ASSERT_EQ(offer.proof.structural.durability_flags, 31);
	assert_structure_offer_codec(&offer);
	UT_ASSERT(memcmp(&offer.proof.structural.terminal.binding, &binding, sizeof(binding)) == 0);
	UT_ASSERT(memcmp(&offer.proof.structural.ko, &ko, sizeof(ko)) == 0);
	UT_ASSERT_EQ(offer.proof.structural.terminal.write_cut.binding_generation, 0);
	UT_ASSERT_EQ(offer.proof.structural.terminal.storage_cut.binding_generation, 0);
	UT_ASSERT_EQ(offer.proof.structural.change.identity.nblocks, 4);
	cluster_ko_shared_postcommit_cleanup_v2();
	offer = before;
	UT_ASSERT(!cluster_ko_shared_structure_offer_v2(completion, 1, &offer));
	UT_ASSERT(memcmp(&offer, &before, sizeof(before)) == 0);
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT_EQ(completion_allocations, 0);
}

UT_TEST(test_native_relation_offer_refuses_drop_missing_effect_and_changed_scope)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterPiWritebackFactV2 offer, before;
		ClusterKoCompletionV2 *completion = prepare_native_structure(fault == 0, &binding, wal);
		int peer = fault == 2 ? 0 : 1;

		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		if (fault >= 2)
			UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
		xact_callback(fault == 6 ? XACT_EVENT_PREPARE : XACT_EVENT_COMMIT, NULL);
		if (fault == 3)
			formation.membership.last_admitted_incarnation[1]++;
		if (fault == 4)
			CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		if (fault == 5)
			writer.claim.claim_sha256[0]++;
		if (fault == 7)
			resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		memset(&before, 0xa5, sizeof(before));
		offer = before;
		UT_ASSERT(!cluster_ko_shared_structure_offer_v2(completion, peer, &offer));
		UT_ASSERT(memcmp(&offer, &before, sizeof(before)) == 0);
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_structure_handoff_consumes_original_handle_without_new_work)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
	ClusterKoCompletionV2 *old;
	ClusterPiWritebackFactV2 expected, actual;
	const char *reason;
	uint32 cursor = 0;
	uint64 serial = 0, original_serial = completion->serial;
	unsigned slot;
	int sends = send_calls, syncs = sync_count, reads = space_reads, pid = MyProcPid;

	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &expected));
	old = completion;
	UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
	slot = structure_owned_slot();
	UT_ASSERT(storage.contexts[slot].serial > original_serial);
	original_serial = storage.contexts[slot].serial;
	UT_ASSERT(completion == NULL);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT(!cluster_ko_shared_structure_handoff_v2(&old));
	UT_ASSERT_EQ(storage.contexts[slot].serial, original_serial);
	UT_ASSERT(storage.contexts[slot].used);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		UT_ASSERT_EQ(storage.contexts[i].used, i == slot);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
	cluster_ko_shared_postcommit_cleanup_v2();
	exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(reason, "KO_SHARED_STRUCTURE_OWNED") == 0);
	MyProcPid = pid + 1;
	MyBackendType = B_BG_WRITER;
	{
		ClusterPageWalBindingV1 observed, unchanged;
		uint8 bytes[sizeof(wal)], saved[sizeof(wal)];
		UT_ASSERT(cluster_ko_shared_structure_observation_v2(slot, original_serial,
			&observed, bytes, sizeof(bytes)));
		UT_ASSERT(memcmp(&observed, &binding, sizeof(binding)) == 0);
		UT_ASSERT(memcmp(bytes, wal, sizeof(wal)) == 0);
		unchanged = observed;
		memcpy(saved, bytes, sizeof(saved));
		UT_ASSERT(!cluster_ko_shared_structure_observation_v2(slot, original_serial + 1,
			&observed, bytes, sizeof(bytes)));
		UT_ASSERT(!cluster_ko_shared_structure_observation_v2(slot, original_serial,
			&observed, bytes, sizeof(bytes) - 1));
		UT_ASSERT(memcmp(&observed, &unchanged, sizeof(observed)) == 0);
		UT_ASSERT(memcmp(bytes, saved, sizeof(bytes)) == 0);
	}
	UT_ASSERT(cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &actual));
	UT_ASSERT_EQ(cursor, slot + 1);
	UT_ASSERT_EQ(serial, original_serial);
	UT_ASSERT(memcmp(&actual, &expected, sizeof(actual)) == 0);
	UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &actual));
	assert_structure_offer_codec(&actual);
	UT_ASSERT_EQ(send_calls, sends);
	UT_ASSERT_EQ(sync_count, syncs);
	UT_ASSERT_EQ(space_reads, reads);
	MyProcPid = pid;
}

UT_TEST(test_structure_handoff_refuses_uncommitted_incomplete_and_wrong_owner)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *completion = prepare_native_structure(fault == 1, &binding, wal);
		ClusterKoCompletionV2 copy, *argument;
		ClusterKoSharedContext before[CLUSTER_KO_SHARED_CAPACITY];
		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		if (fault != 1 && fault != 2)
			UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
		if (fault != 0)
			xact_callback(fault == 3 ? XACT_EVENT_PREPARE : XACT_EVENT_COMMIT, NULL);
		copy = *completion;
		argument = fault == 4 ? &copy : completion;
		if (fault == 5) CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		if (fault == 6) formation.membership.last_admitted_incarnation[1]++;
		if (fault == 7) writer.claim.claim_sha256[0]++;
		if (fault == 8) CritSectionCount = 1;
		memcpy(before, storage.contexts, sizeof(before));
		UT_ASSERT(!cluster_ko_shared_structure_handoff_v2(&argument));
		UT_ASSERT(argument == (fault == 4 ? &copy : completion));
		UT_ASSERT(memcmp(before, storage.contexts, sizeof(before)) == 0);
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
	UT_ASSERT(!cluster_ko_shared_structure_handoff_v2(NULL));
}

UT_TEST(test_structure_offer_ack_only_advances_original_peer_progress)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
	ClusterPiWritebackFactV2 fact;
	const char *reason;
	uint32 cursor = 0;
	uint64 serial;

	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
	offer_ack_slot = structure_owned_slot();
	offer_ack_serial = storage.contexts[offer_ack_slot].serial;
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &fact));
	memset(&offered_peer, 0, sizeof(offered_peer));
	offered_peer.ref = writer;
	offered_peer.ref.claim.identity.origin_node_id = 1;
	offered_peer.ref.claim.identity.origin_thread_id = 2;
	offered_peer.ref.claim.identity.origin_owner_incarnation = fact.proof.structural.ko.peer_boot;
	offered_peer.epoch = current_epoch;
	offer_ack_live = true;
	offer_ack_drift = false;
	UT_ASSERT(cluster_ko_shared_structure_offer_complete_v2(offer_ack_slot, serial, offer_job));
	cursor = 0;
	UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &fact));
	UT_ASSERT(storage.contexts[offer_ack_slot].used
			  && storage.contexts[offer_ack_slot].structure_owned);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(cluster_ko_shared_structure_observation_v2(offer_ack_slot, offer_ack_serial, &binding,
														 wal, sizeof(wal)));
}

UT_TEST(test_structure_offer_ack_refuses_wrong_scope_and_concurrent_slot_reuse)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
		ClusterKoSharedContext before;

		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
		xact_callback(XACT_EVENT_COMMIT, NULL);
		UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
		offer_ack_slot = structure_owned_slot();
		offer_ack_serial = storage.contexts[offer_ack_slot].serial;
		MyBackendType = B_CHECKPOINTER;
		memset(&offered_peer, 0, sizeof(offered_peer));
		offered_peer.ref = writer;
		offered_peer.ref.claim.identity.origin_node_id = 1;
		offered_peer.ref.claim.identity.origin_thread_id = 2;
		offered_peer.ref.claim.identity.origin_owner_incarnation = 22;
		offered_peer.epoch = current_epoch;
		offer_ack_live = fault != 0;
		offer_ack_drift = fault == 6;
		if (fault == 1)
			offered_peer.ref.claim.identity.origin_node_id = 0;
		if (fault == 2)
			offered_peer.ref.claim.identity.origin_node_id = 16;
		if (fault == 3)
			offered_peer.ref.claim.identity.origin_owner_incarnation++;
		if (fault == 4)
			offered_peer.epoch++;
		if (fault == 5)
			offer_ack_serial++;
		if (fault == 7)
			MyBackendType = B_BACKEND;
		before = storage.contexts[offer_ack_slot];
		UT_ASSERT(!cluster_ko_shared_structure_offer_complete_v2(offer_ack_slot, offer_ack_serial,
																 offer_job));
		if (fault == 6)
			before.serial++;
		UT_ASSERT_EQ(memcmp(&before, &storage.contexts[offer_ack_slot], sizeof(before)), 0);
	}
}

UT_TEST(test_structure_background_scan_preserves_stale_responsibility)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
		ClusterPiWritebackFactV2 value, before;
		ClusterKoSharedContext owned;
		uint32 cursor = 0, saved_cursor;
		uint64 serial = UINT64_C(0xaabbccdd);
		int peer = 1;
		const char *reason;
		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
		xact_callback(XACT_EVENT_COMMIT, NULL);
		UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
		owned = storage.contexts[0];
		MyBackendType = B_BG_WRITER;
		switch (fault) {
		case 0: MyBackendType = B_BACKEND; break;
		case 1: MyBackendType = B_LMON; break;
		case 2: CurrentResourceOwner = NULL; break;
		case 3: CritSectionCount = 1; break;
		case 4: formation.membership.last_admitted_incarnation[1]++; break;
		case 5: formation.membership.membership_state[1] = CLUSTER_MEMBER_DEAD; break;
		case 6: current_epoch++; break;
		case 7: writer.claim.claim_sha256[0]++; break;
		case 8: writer.timeline++; break;
		case 9: peer = 0; break;
		case 10: peer = CLUSTER_KO_SHARED_NODE_LIMIT; break;
		case 11: cap_ok = false; break;
		case 12: generation_race = true; break;
		case 13: cursor = CLUSTER_KO_SHARED_CAPACITY; break;
		}
		saved_cursor = cursor;
		memset(&before, 0xa5, sizeof(before));
		value = before;
		UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, peer, &serial, &value));
		UT_ASSERT_EQ(cursor, saved_cursor);
		UT_ASSERT_EQ(serial, UINT64_C(0xaabbccdd));
		UT_ASSERT(memcmp(&value, &before, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&owned, &storage.contexts[0], sizeof(owned)) == 0);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(strcmp(reason, "KO_SHARED_STRUCTURE_OWNED") == 0);
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		exit_callback(0, (Datum)0);
		UT_ASSERT(storage.contexts[0].used);
	}
}

UT_TEST(test_structure_handoff_uses_last_free_background_slot)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion = prepare_native_structure(false, &binding, wal);
	ClusterPiWritebackFactV2 fact;
	uint32 cursor = 0;
	uint64 serial;
	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	for (unsigned i = 1; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		storage.contexts[i].used = true;
		storage.contexts[i].pid = MyProcPid + 10;
	}
	UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
	UT_ASSERT(!ko_run_shared_barrier(space_identity.key.locator));
	UT_ASSERT(!barrier_active);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &fact));
	UT_ASSERT_EQ(cursor, 1);
	UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &fact));
	exit_callback(0, (Datum)0);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		UT_ASSERT(storage.contexts[i].used);
}

UT_TEST(test_native_full_background_region_preserves_proof_until_retry)
{
	for (unsigned drop = 0; drop < 2; drop++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *completion = prepare_native_structure(drop, &binding, wal), *old;
		ClusterKoSharedContext before[CLUSTER_KO_SHARED_CAPACITY];
		ClusterPiWritebackFactV2 offered, retried;
		uint64 barrier_serial = completion->serial;
		int sends = send_calls, reads = space_reads, syncs = sync_count;
		UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		if (!drop)
			UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
		xact_callback(XACT_EVENT_COMMIT, NULL);
		if (drop)
			UT_ASSERT(cluster_ko_shared_observe_drop_v2(completion));
		UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &offered));
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			storage.contexts[i].used = storage.contexts[i].structure_owned = true;
			storage.contexts[i].pid = MyProcPid + 1;
			storage.contexts[i].serial = ++storage.context_serial;
		}
		memcpy(before, storage.contexts, sizeof(before));
		old = completion;
		UT_ASSERT(!cluster_ko_shared_structure_handoff_v2(&completion));
		UT_ASSERT(completion == old);
		UT_ASSERT_EQ(completion_allocations, 1);
		UT_ASSERT_EQ(memcmp(before, storage.contexts, sizeof(before)), 0);
		/* The original transaction owner still holds exactly the same proof. */
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
		UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &retried));
		UT_ASSERT_EQ(memcmp(&offered, &retried, sizeof(offered)), 0);
		memset(&storage.contexts[7], 0, sizeof(storage.contexts[7]));
		UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
		UT_ASSERT(completion == NULL);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT(!cluster_ko_shared_structure_handoff_v2(&old));
		UT_ASSERT(storage.contexts[7].serial > barrier_serial);
		UT_ASSERT(storage.contexts[7].structure_owned);
		UT_ASSERT_EQ(memcmp(&storage.contexts[7].terminal, &binding, sizeof(binding)), 0);
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
			if (i != 7)
				UT_ASSERT_EQ(memcmp(&before[i], &storage.contexts[i], sizeof(before[i])), 0);
		UT_ASSERT_EQ(send_calls, sends);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(sync_count, syncs);
	}
}

UT_TEST(test_native_private_proof_survives_slot_reuse_but_not_cut_or_owner_change)
{
	ClusterKoCompletionV2 *native = NULL, *direct = NULL;
	ClusterKoCompletionV2 copy;
	ClusterKoSharedMessageV2 before, after;
	ClusterSpaceIdentityKey first;
	reset_test();
	cluster_shared_config = drive_shared_ack = multiple_barriers = true;
	first = space_identity.key;
	cluster_ko_flush_and_wait_ack(first.locator, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(cluster_ko_shared_claim_v2(&first, space_identity.incarnation, &native));
	UT_ASSERT(cluster_ko_shared_read_v2(native, 1, &before));
	copy = *native;
	space_identity.key.locator.relNumber++;
	UT_ASSERT(
		cluster_ko_shared_begin_v2(space_identity.key.locator, RELPERSISTENCE_PERMANENT, &direct));
	UT_ASSERT(storage.contexts[0].used);
	UT_ASSERT(storage.contexts[0].serial > native->serial);
	UT_ASSERT(cluster_ko_shared_read_v2(native, 1, &after));
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	UT_ASSERT(!cluster_ko_shared_read_v2(&copy, 1, &after));
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
	UT_ASSERT(!cluster_ko_shared_read_v2(native, 1, &after));
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
	current_epoch++;
	UT_ASSERT(!cluster_ko_shared_read_v2(native, 1, &after));
	current_epoch--;
	formation.membership.last_admitted_incarnation[1]++;
	UT_ASSERT(!cluster_ko_shared_read_v2(native, 1, &after));
	formation.membership.last_admitted_incarnation[1]--;
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!cluster_ko_shared_read_v2(native, 1, &after));
	cluster_ko_shared_release_v2(&native);
	cluster_ko_shared_release_v2(&direct);
	UT_ASSERT_EQ(completion_allocations, 0);
}

UT_TEST(test_structure_one_member_keeps_local_owner_without_fake_peer)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion = prepare_native_structure_owner(false, &binding, wal,
		(ResourceOwner)1, false);
	ClusterPiWritebackFactV2 value;
	uint32 cursor = 0;
	uint64 serial = 0;
	const char *reason;
	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
	MyBackendType = B_BG_WRITER;
	{
		ClusterPageWalBindingV1 observed;
		uint8 copied[sizeof(wal)];
		UT_ASSERT(cluster_ko_shared_structure_observation_v2(0, storage.contexts[0].serial,
			&observed, copied, sizeof(copied)));
		UT_ASSERT(memcmp(&observed, &binding, sizeof(binding)) == 0);
		UT_ASSERT(memcmp(copied, wal, sizeof(wal)) == 0);
	}
	UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, 0, &serial, &value));
	UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &value));
	UT_ASSERT_EQ(cursor, 0);
	UT_ASSERT_EQ(serial, 0);
	exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(reason, "KO_SHARED_STRUCTURE_OWNED") == 0);
	UT_ASSERT_EQ(send_calls, 0);
}

/* Execute the real native sequence from ResourceOwner release through
 * pending deletes and its local KO cleanup. Storage is a boundary here:
 * reaching it is not a durability certificate. */
static ClusterKoCompletionV2 *postcommit_owner;
static ClusterPageWalBindingV1 postcommit_binding;
static uint8 postcommit_wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
static bool postcommit_drop, postcommit_error, postcommit_scope_changed;
static bool postcommit_callback_error;
static unsigned postcommit_deletes, postcommit_phases;
static unsigned postcommit_native_count = 1;
static bool (*postcommit_storage_consumer)(RelFileLocator);

static void
CallXactCallbacks(XactEvent event)
{
	if (native_commit_active && event == XACT_EVENT_COMMIT)
		UT_ASSERT_EQ(storage.native_waiting, postcommit_native_count);
	if (postcommit_callback_error)
		pg_re_throw();
	if (xact_callback != NULL)
		xact_callback(event, NULL);
}
static void
AtEOXact_ClusterRelmapPublish(void)
{
	if (native_commit_active)
		UT_ASSERT_EQ(storage.native_waiting, postcommit_native_count);
}

void
ResourceOwnerRelease(ResourceOwner owner, ResourceReleasePhase phase, bool commit, bool top)
{
	UT_ASSERT(owner == TopTransactionResourceOwner && owner == CurrentResourceOwner);
	UT_ASSERT(commit && top);
	postcommit_phases++;
	resource_callback(phase, commit, top, NULL);
}
void AtEOXact_Buffers(bool commit) { UT_ASSERT(commit); }
void AtEOXact_RelationCache(bool commit) { UT_ASSERT(commit); }
void AtEOXact_Inval(bool commit) { UT_ASSERT(commit); }
void AtEOXact_MultiXact(void) {}
void
smgrDoPendingDeletes(bool commit)
{
	ClusterPageWalBindingV1 observed = {0};
	uint8 copied[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = {0};
	ClusterKoCompletionV2 *borrowed = NULL;
	UT_ASSERT(commit);
	UT_ASSERT_EQ(postcommit_phases, 3);
	UT_ASSERT_EQ(completion_allocations, postcommit_native_count);
	if (postcommit_scope_changed)
		current_epoch++;
	UT_ASSERT_EQ(
		cluster_ko_shared_space_observation_v2(postcommit_owner, &observed, copied, sizeof(copied)),
		!postcommit_scope_changed && !native_commit_active);
	UT_ASSERT_EQ(cluster_ko_shared_pending_drop_v2(postcommit_binding.identity.locator, &borrowed),
				 postcommit_drop && !postcommit_scope_changed && !native_commit_active);
	if (!postcommit_scope_changed && !native_commit_active) {
		UT_ASSERT(memcmp(&observed, &postcommit_binding, sizeof(observed)) == 0);
		UT_ASSERT(memcmp(copied, postcommit_wal, sizeof(copied)) == 0);
	}
	UT_ASSERT(borrowed
			  == (postcommit_drop && !postcommit_scope_changed && !native_commit_active
					  ? postcommit_owner
					  : NULL));
	if (native_commit_active) {
		UT_ASSERT_EQ(cluster_ko_shared_native_drop_deferred_v2(postcommit_binding.identity.locator),
					 postcommit_drop);
		UT_ASSERT(!cluster_ko_shared_observe_drop_v2(postcommit_owner));
	}
	if (postcommit_storage_consumer != NULL)
		UT_ASSERT(postcommit_storage_consumer(postcommit_binding.identity.locator));
	postcommit_deletes++;
	if (postcommit_error)
		pg_re_throw();
}
static void
run_native_postcommit(void)
{
	bool is_parallel_worker = false;
#include "test_cluster_ko_postcommit.inc"
}

static unsigned ko_tick_absorbed, ko_tick_writebacks;
static void
ko_tick_absorb(void)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	ko_tick_absorbed++;
}
static bool
ko_tick_writeback(void)
{
	UT_ASSERT_EQ(ko_tick_absorbed, ko_tick_writebacks + 1);
	ko_tick_writebacks++;
	return false;
}
static bool
run_native_checkpointer(void)
{
	bool pi_pending = false;
#define AbsorbSyncRequests ko_tick_absorb
#define cluster_pi_writeback_checkpointer_tick_v1 ko_tick_writeback
#include "test_cluster_ko_checkpointer.inc"
#undef cluster_pi_writeback_checkpointer_tick_v1
#undef AbsorbSyncRequests
	return pi_pending;
}

static void
prepare_postcommit(bool drop)
{
	postcommit_owner = prepare_native_structure(drop, &postcommit_binding, postcommit_wal);
	postcommit_drop = drop;
	postcommit_error = postcommit_scope_changed = false;
	postcommit_callback_error = false;
	postcommit_native_count = 1;
	postcommit_storage_consumer = NULL;
	postcommit_deletes = postcommit_phases = 0;
	UT_ASSERT(cluster_ko_shared_observe_space_v2(postcommit_owner, &postcommit_binding,
		postcommit_wal, sizeof(postcommit_wal)));
}

UT_TEST(test_native_postcommit_retains_exact_observation_until_pending_deletes_return)
{
	for (unsigned drop = 0; drop < 2; drop++) {
		ClusterKoCompletionV2 *borrowed = NULL;
		const char *reason;
		int reads, requests;
		prepare_postcommit(drop);
		reads = space_reads;
		requests = barrier_requests;
		UT_ASSERT(!cluster_ko_shared_pending_drop_v2(postcommit_binding.identity.locator, &borrowed));
		UT_ASSERT(borrowed == NULL);
		run_native_postcommit();
		UT_ASSERT_EQ(postcommit_deletes, 1);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT(!cluster_ko_shared_pending_drop_v2(postcommit_binding.identity.locator, &borrowed));
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(barrier_requests, requests);
		cluster_ko_shared_release_v2(&postcommit_owner);
		UT_ASSERT(postcommit_owner == NULL);
	}
}

UT_TEST(test_native_postcommit_refuses_drift_but_always_cleans_original_local_owner)
{
	const char *reason;
	prepare_postcommit(true);
	postcommit_scope_changed = true;
	run_native_postcommit();
	UT_ASSERT_EQ(postcommit_deletes, 1);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	cluster_ko_shared_release_v2(&postcommit_owner);
}

UT_TEST(test_native_postcommit_error_and_exit_do_not_leave_a_completion)
{
	for (unsigned exiting = 0; exiting < 2; exiting++) {
		volatile bool caught = false;
		const char *reason;
		prepare_postcommit(true);
		postcommit_error = true;
		PG_TRY();
		{
			run_native_postcommit();
		}
		PG_CATCH();
		{
			caught = true;
			if (exiting)
				exit_callback(0, (Datum)0);
			else
				resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(postcommit_deletes, 1);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
		cluster_ko_shared_release_v2(&postcommit_owner);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_commit_transfers_before_callbacks_and_local_cleanup)
{
	for (unsigned drop = 0; drop < 2; drop++) {
		const char *reason;
		prepare_postcommit(drop);
		if (!drop)
			UT_ASSERT(cluster_ko_shared_observe_truncate_v2(postcommit_owner));
		native_commit_active = true;
		run_native_postcommit();
		UT_ASSERT_EQ(postcommit_deletes, 1);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
		UT_ASSERT_EQ(storage.contexts[0].structure_drop_pending, drop);
		UT_ASSERT(
			memcmp(&storage.contexts[0].terminal, &postcommit_binding, sizeof(postcommit_binding))
			== 0);
		UT_ASSERT_EQ(native_allocated, 0);
	}
}

UT_TEST(test_native_commit_callback_error_cannot_cancel_shared_responsibility)
{
	for (unsigned exiting = 0; exiting < 2; exiting++) {
		volatile bool caught = false;
		prepare_postcommit(true);
		native_commit_active = postcommit_callback_error = true;
		PG_TRY();
		{
			run_native_postcommit();
		}
		PG_CATCH();
		{
			caught = true;
			if (exiting)
				exit_callback(0, (Datum)0);
			else
				resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		}
		PG_END_TRY();
		postcommit_callback_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(postcommit_deletes, 0);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
		UT_ASSERT(storage.contexts[0].structure_drop_pending);
		UT_ASSERT_EQ(native_allocated, 0);
	}
}

UT_TEST(test_native_commit_keeps_old_cut_without_executing_it)
{
	for (unsigned before = 0; before < 2; before++) {
		prepare_postcommit(true);
		native_commit_active = postcommit_scope_changed = true;
		if (before)
			current_epoch++;
		run_native_postcommit();
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		current_epoch = last_shared_request.epoch;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
		UT_ASSERT(memcmp(storage.contexts[0].structure, postcommit_wal, sizeof(postcommit_wal))
				  == 0);
		UT_ASSERT_EQ(native_allocated, 0);
	}
}

UT_TEST(test_native_checkpointer_keeps_owner_gates_and_services_sync_requests)
{
	unsigned syncs = ko_tick_absorbed, writes = ko_tick_writebacks;
	prepare_postcommit(true);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(!run_native_checkpointer());
	native_commit_active = true;
	MyBackendType = B_BACKEND;
	UT_ASSERT(!run_native_checkpointer());
	MyBackendType = B_CHECKPOINTER;
	CritSectionCount = 1;
	UT_ASSERT(!run_native_checkpointer());
	CritSectionCount = 0;
	CurrentResourceOwner = NULL;
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(ko_tick_absorbed, syncs + 4);
	UT_ASSERT_EQ(ko_tick_writebacks, writes + 4);
	UT_ASSERT_EQ(storage.native_waiting, 0);
	UT_ASSERT_EQ(fixture_log_events, 0);
	MyBackendType = B_BACKEND;
	CurrentResourceOwner = (ResourceOwner)1;
	cluster_ko_shared_release_v2(&postcommit_owner);
	UT_ASSERT_EQ(native_allocated, 0);
}

UT_TEST(test_native_subcommit_cannot_qualify_pending_drop_before_top_commit)
{
	ClusterKoCompletionV2 *borrowed = NULL;
	const char *reason;
	postcommit_owner = prepare_native_structure_owner(true, &postcommit_binding,
		postcommit_wal, (ResourceOwner)2, true);
	postcommit_drop = true;
	postcommit_error = postcommit_scope_changed = false;
	postcommit_deletes = postcommit_phases = 0;
	UT_ASSERT(cluster_ko_shared_observe_space_v2(postcommit_owner, &postcommit_binding,
		postcommit_wal, sizeof(postcommit_wal)));
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, NULL);
	CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
	UT_ASSERT(!cluster_ko_shared_pending_drop_v2(postcommit_binding.identity.locator, &borrowed));
	UT_ASSERT(borrowed == NULL);
	UT_ASSERT_EQ(completion_allocations, 1);
	run_native_postcommit();
	UT_ASSERT_EQ(postcommit_deletes, 1);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	cluster_ko_shared_release_v2(&postcommit_owner);
}

UT_TEST(test_pending_drop_refuses_other_owner_and_changed_scope_without_touching_output)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterKoCompletionV2 *borrowed = NULL, *before;
		RelFileLocator locator;
		const char *reason;
		prepare_postcommit(true);
		/* Premature cleanup cannot cancel a still-live transaction. */
		cluster_ko_shared_postcommit_cleanup_v2();
		UT_ASSERT_EQ(completion_allocations, 1);
		CallXactCallbacks(XACT_EVENT_COMMIT);
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
		locator = postcommit_binding.identity.locator;
		switch (fault) {
		case 0: locator.relNumber++; break;
		case 1: CurrentResourceOwner = (ResourceOwner)2; break;
		case 2: CurTransactionResourceOwner = (ResourceOwner)2; break;
		case 3: writer.claim.identity.origin_owner_incarnation++; break;
		case 4: writer.claim.claim_sha256[0]++; break;
		case 5: formation.membership.last_admitted_incarnation[1]++; break;
		case 6: CritSectionCount = 1; break;
		case 7: generation_race = true; break;
		case 8: borrowed = (ClusterKoCompletionV2 *)1; break;
		}
		before = borrowed;
		UT_ASSERT(!cluster_ko_shared_pending_drop_v2(locator, &borrowed));
		UT_ASSERT(borrowed == before);
		UT_ASSERT(!cluster_ko_shared_pending_drop_v2(locator, NULL));
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		CritSectionCount = 0;
		cluster_ko_shared_postcommit_cleanup_v2();
		cluster_ko_shared_postcommit_cleanup_v2();
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
		cluster_ko_shared_release_v2(&postcommit_owner);
	}
}

UT_TEST(test_pending_drop_rejects_ambiguous_original_native_completions)
{
	ClusterKoCompletionV2 *first, *second = NULL, *borrowed = NULL;
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	const char *reason;
	/* One-member barriers exercise the actual bounded completion allocator
	 * without inventing a second remote ACK in the original fixture. */
	first = prepare_native_structure_owner(true, &binding, wal, (ResourceOwner)1, false);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(first, &binding, wal, sizeof(wal)));
	cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &second));
	UT_ASSERT(first != second && first->serial != second->serial);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(second, &binding, wal, sizeof(wal)));
	CallXactCallbacks(XACT_EVENT_COMMIT);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
	UT_ASSERT_EQ(completion_allocations, 2);
	UT_ASSERT(!cluster_ko_shared_pending_drop_v2(binding.identity.locator, &borrowed));
	UT_ASSERT(borrowed == NULL);
	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(first));
	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(second));
	cluster_ko_shared_postcommit_cleanup_v2();
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
	cluster_ko_shared_release_v2(&first);
	cluster_ko_shared_release_v2(&second);
}

UT_TEST(test_prepare_and_noncommit_events_never_retain_native_observation)
{
	const XactEvent events[] = { XACT_EVENT_PREPARE, XACT_EVENT_PRE_PREPARE,
		XACT_EVENT_PRE_COMMIT, XACT_EVENT_ABORT, XACT_EVENT_PARALLEL_COMMIT };
	for (unsigned i = 0; i < lengthof(events); i++) {
		ClusterKoCompletionV2 *borrowed = NULL;
		const char *reason;
		prepare_postcommit(false); /* Native TRUNCATE can already have observed SPACE. */
		native_commit_active = true;
		CallXactCallbacks(events[i]);
		/* PrepareTransaction really uses these same two true arguments. */
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
		UT_ASSERT_EQ(completion_allocations, 0);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT(!cluster_ko_shared_pending_drop_v2(postcommit_binding.identity.locator, &borrowed));
		cluster_ko_shared_release_v2(&postcommit_owner);
	}
}

UT_TEST(test_native_drop_effect_is_once_only_after_original_top_commit)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion = prepare_native_structure(true, &binding, wal);
	ClusterKoCompletionV2 *borrowed = NULL;
	ClusterPiWritebackFactV2 offer, before;
	int reads = space_reads, requests = barrier_requests, syncs = sync_count;

	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(completion));
	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(completion));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
	UT_ASSERT(cluster_ko_shared_pending_drop_v2(binding.identity.locator, &borrowed));
	UT_ASSERT(borrowed == completion);
	memset(&before, 0xa5, sizeof(before));
	offer = before;
	UT_ASSERT(!cluster_ko_shared_structure_offer_v2(completion, 1, &offer));
	UT_ASSERT(memcmp(&offer, &before, sizeof(before)) == 0);
	UT_ASSERT(cluster_ko_shared_observe_drop_v2(borrowed));
	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(borrowed));
	UT_ASSERT(!cluster_ko_shared_observe_truncate_v2(borrowed));
	UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &offer));
	UT_ASSERT_EQ(offer.kind, CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2);
	UT_ASSERT_EQ(offer.proof.structural.durability_flags, 15);
	UT_ASSERT_EQ(offer.proof.structural.change.identity.action, CLUSTER_SPACE_WAL_TOMBSTONE);
	assert_structure_offer_codec(&offer);
	UT_ASSERT(memcmp(&offer.proof.structural.terminal.binding, &binding, sizeof(binding)) == 0);
	UT_ASSERT_EQ(offer.proof.structural.terminal.write_cut.binding_generation, 0);
	UT_ASSERT_EQ(offer.proof.structural.terminal.storage_cut.binding_generation, 0);
	cluster_ko_shared_postcommit_cleanup_v2();
	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(borrowed));
	offer = before;
	UT_ASSERT(!cluster_ko_shared_structure_offer_v2(completion, 1, &offer));
	UT_ASSERT(memcmp(&offer, &before, sizeof(before)) == 0);
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(space_reads, reads);
	UT_ASSERT_EQ(barrier_requests, requests);
	UT_ASSERT_EQ(sync_count, syncs);
}

UT_TEST(test_native_drop_effect_refuses_wrong_lifetime_scope_or_handle)
{
	for (unsigned fault = 0; fault < 18; fault++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *completion = prepare_native_structure(fault != 3, &binding, wal);
		ClusterKoCompletionV2 copy = *completion, *candidate = completion;
		ClusterKoSharedContext before[CLUSTER_KO_SHARED_CAPACITY];
		int pid = MyProcPid;

		if (fault != 4)
			UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
		if (fault != 0)
			xact_callback(fault == 1 ? XACT_EVENT_PREPARE
				: fault == 2 ? XACT_EVENT_ABORT : XACT_EVENT_COMMIT, NULL);
		switch (fault) {
		case 5: CurrentResourceOwner = (ResourceOwner)2; break;
		case 6: CurTransactionResourceOwner = (ResourceOwner)2; break;
		case 7: TopTransactionResourceOwner = (ResourceOwner)2; break;
		case 8: MyProcPid++; break;
		case 9: writer.claim.identity.origin_owner_incarnation++; break;
		case 10: writer.claim.claim_sha256[0]++; break;
		case 11: formation.membership.last_admitted_incarnation[1]++; break;
		case 12: current_epoch++; break;
		case 13: CritSectionCount = 1; break;
		case 14: generation_race = true; break;
		case 15: cluster_ko_shared_postcommit_cleanup_v2(); break;
		case 16: candidate = &copy; break;
		case 17: candidate = NULL; break;
		}
		memcpy(before, storage.contexts, sizeof(before));
		UT_ASSERT(!cluster_ko_shared_observe_drop_v2(candidate));
		UT_ASSERT(memcmp(storage.contexts, before, sizeof(before)) == 0);
		MyProcPid = pid;
		CurrentResourceOwner = CurTransactionResourceOwner = TopTransactionResourceOwner = (ResourceOwner)1;
		CritSectionCount = 0;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		cluster_ko_shared_release_v2(&completion);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_drop_result_handoff_preserves_original_shared_obligation)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion = prepare_native_structure(true, &binding, wal), *old;
	ClusterPiWritebackFactV2 expected, actual;
	unsigned slot;
	uint64 original_serial = completion->serial, serial = 0;
	uint32 cursor = 0;
	const char *reason;
	int pid = MyProcPid, sends = send_calls, reads = space_reads, syncs = sync_count;

	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	UT_ASSERT(!cluster_ko_shared_structure_handoff_v2(&completion));
	UT_ASSERT(cluster_ko_shared_observe_drop_v2(completion));
	UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &expected));
	old = completion;
	UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
	slot = structure_owned_slot();
	UT_ASSERT(storage.contexts[slot].serial > original_serial);
	original_serial = storage.contexts[slot].serial;
	UT_ASSERT(completion == NULL);
	UT_ASSERT(!cluster_ko_shared_observe_drop_v2(old));
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT_EQ(storage.contexts[slot].serial, original_serial);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, true, NULL);
	cluster_ko_shared_postcommit_cleanup_v2();
	exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(reason, "KO_SHARED_STRUCTURE_OWNED") == 0);
	MyProcPid = pid + 1;
	MyBackendType = B_BG_WRITER;
	UT_ASSERT(cluster_ko_shared_structure_offer_next_v2(&cursor, 1, &serial, &actual));
	UT_ASSERT_EQ(cursor, slot + 1);
	UT_ASSERT_EQ(serial, original_serial);
	UT_ASSERT(memcmp(&actual, &expected, sizeof(actual)) == 0);
	assert_structure_offer_codec(&actual);
	UT_ASSERT_EQ(send_calls, sends);
	UT_ASSERT_EQ(space_reads, reads);
	UT_ASSERT_EQ(sync_count, syncs);
	MyProcPid = pid;
}

static ClusterKoSharedMessageV2
structure_projection_setup(void)
{
	cluster_node_id = 0;
	reset_test();
	cluster_shared_config = true;
	MyBackendType = B_BG_WRITER;
	formation.membership.membership_state[2] = CLUSTER_MEMBER_MEMBER;
	formation.membership.last_admitted_incarnation[2] = 33;
	return shared_request(123, current_epoch);
}

static void
remote_structure_setup(bool drop)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *completion;
	cluster_node_id = 0;
	completion = prepare_native_structure(drop, &binding, wal);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
	if (!drop)
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	if (drop)
		UT_ASSERT(cluster_ko_shared_observe_drop_v2(completion));
	UT_ASSERT(cluster_ko_shared_structure_offer_v2(completion, 1, &offered_structure));
	cluster_ko_shared_release_v2(&completion);
	UT_ASSERT_EQ(completion_allocations, 0);
	/* Add the third member to the authenticated-notice fixture's complete
	 * cut. This is not a claim that the preceding two-node KO used it. */
	formation.membership.membership_state[2] = CLUSTER_MEMBER_MEMBER;
	formation.membership.last_admitted_incarnation[2] = 33;
	offered_structure.proof.structural.ko = shared_request(123, current_epoch);
	offered_structure.proof.structural.ko.origin_node = 0;
	offered_structure.proof.structural.ko.origin_boot = 11;
	offered_structure.proof.structural.ko.peer_node = 1;
	offered_structure.proof.structural.ko.peer_boot = 22;
	assert_structure_offer_codec(&offered_structure);
	cluster_node_id = 1;
	writer.claim.identity.origin_node_id = 1;
	writer.claim.identity.origin_thread_id = 2;
	writer.claim.identity.origin_owner_incarnation = 22;
	MyBackendType = B_BG_WRITER;
	offer_notice_live = true;
}

UT_TEST(test_remote_structure_accept_preserves_responsibility_after_notice_and_exit)
{
	for (unsigned drop = 0; drop < 2; drop++) {
		ClusterPageWalBindingV1 observed = { 0 };
		ClusterKoSharedMessageV2 projected = { 0 };
		ClusterPiWritebackFactV2 forbidden;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = { 0 }, expected[sizeof(wal)];
		uint32 cursor = 0;
		uint64 serial;
		const char *reason;
		int sends, syncs, reads;
		remote_structure_setup(drop);
		sends = send_calls;
		syncs = sync_count;
		reads = space_reads;
		UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
		serial = storage.contexts[0].serial;
		UT_ASSERT(serial != 0 && storage.contexts[0].used && storage.contexts[0].structure_owned);
		UT_ASSERT_EQ(storage.contexts[0].request.origin_node, 0);
		offer_notice_live = false;
		ko_shared_backend_exit(0, (Datum)0);
		UT_ASSERT(
			cluster_ko_shared_structure_observation_v2(0, serial, &observed, wal, sizeof(wal)));
		UT_ASSERT_EQ(memcmp(&observed, &offered_structure.proof.structural.terminal.binding,
							sizeof(observed)),
					 0);
		UT_ASSERT(cluster_space_structure_wal_encode(&offered_structure.proof.structural.change,
													 expected, sizeof(expected)));
		UT_ASSERT_EQ(memcmp(wal, expected, sizeof(wal)), 0);
		UT_ASSERT(cluster_ko_shared_structure_peer_v2(0, serial, 0, &projected));
		UT_ASSERT_EQ(projected.origin_node, 0);
		UT_ASSERT_EQ(projected.peer_node, 1); /* Origin is never its own peer. */
		UT_ASSERT(cluster_ko_shared_structure_peer_v2(0, serial, 2, &projected));
		UT_ASSERT_EQ(projected.origin_node, 0);
		UT_ASSERT_EQ(projected.peer_node, 2);
		UT_ASSERT(!cluster_ko_shared_structure_offer_next_v2(&cursor, 2, &serial, &forbidden));
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(send_calls, sends);
		UT_ASSERT_EQ(sync_count, syncs);
		UT_ASSERT_EQ(space_reads, reads);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
	cluster_node_id = 0;
}

UT_TEST(test_structure_page_scan_includes_local_single_member_and_imported_results)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterPageWalBindingV1 binding, observed;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoShared before;
		uint32 cursor = 0;
		uint64 serial = 0;
		const char *reason;
		if (mode == 2) {
			remote_structure_setup(true);
			UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
			binding = offered_structure.proof.structural.terminal.binding;
		} else {
			ClusterKoCompletionV2 *completion
				= prepare_native_structure_owner(false, &binding, wal, (ResourceOwner)1, mode != 0);
			UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &binding, wal, sizeof(wal)));
			UT_ASSERT(cluster_ko_shared_observe_truncate_v2(completion));
			xact_callback(XACT_EVENT_COMMIT, NULL);
			UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&completion));
		}
		MyBackendType = B_CHECKPOINTER;
		/* All peer acceptance bits still leave local page work outstanding. */
		storage.contexts[0].structure_peers_accepted = UINT16_MAX;
		before = storage;
		UT_ASSERT(cluster_ko_shared_structure_next_v2(&cursor, &serial, &observed));
		UT_ASSERT_EQ(cursor, 1);
		UT_ASSERT_EQ(serial, before.contexts[0].serial);
		UT_ASSERT_EQ(memcmp(&observed, &binding, sizeof(binding)), 0);
		UT_ASSERT(!cluster_ko_shared_structure_next_v2(&cursor, &serial, &observed));
		UT_ASSERT_EQ(cursor, 1);
		UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		cluster_node_id = 0;
	}
}

UT_TEST(test_structure_page_scan_reaches_last_slot_and_skips_unowned_entries)
{
	ClusterPageWalBindingV1 observed, unchanged;
	ClusterKoShared before;
	uint32 cursor = 0;
	uint64 serial = 0;
	remote_structure_setup(false);
	UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
	for (uint32 i = 1; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		storage.contexts[i] = storage.contexts[0];
	for (uint32 i = 0; i < CLUSTER_KO_SHARED_CAPACITY - 1; i++) {
		switch (i % 4) {
		case 0:
			storage.contexts[i].used = false;
			break;
		case 1:
			storage.contexts[i].complete = false;
			break;
		case 2:
			storage.contexts[i].structure_owned = false;
			break;
		case 3:
			storage.contexts[i].serial = 0;
			break;
		}
	}
	before = storage;
	UT_ASSERT(cluster_ko_shared_structure_next_v2(&cursor, &serial, &observed));
	UT_ASSERT_EQ(cursor, CLUSTER_KO_SHARED_CAPACITY);
	UT_ASSERT_EQ(serial, before.contexts[CLUSTER_KO_SHARED_CAPACITY - 1].serial);
	unchanged = observed;
	UT_ASSERT(!cluster_ko_shared_structure_next_v2(&cursor, &serial, &observed));
	UT_ASSERT_EQ(cursor, CLUSTER_KO_SHARED_CAPACITY);
	UT_ASSERT_EQ(memcmp(&observed, &unchanged, sizeof(observed)), 0);
	UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
	cluster_node_id = 0;
}

UT_TEST(test_structure_page_scan_refuses_stale_cut_without_modifying_outputs)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterPageWalBindingV1 observed, before;
		ClusterKoSharedContext owned;
		uint32 cursor = 0, saved;
		uint64 serial = 987;
		remote_structure_setup(false);
		UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
		owned = storage.contexts[0];
		switch (fault) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			MyBackendType = B_LMON;
			break;
		case 2:
			CurrentResourceOwner = NULL;
			break;
		case 3:
			CritSectionCount = 1;
			break;
		case 4:
			formation.membership.last_admitted_incarnation[0]++;
			break;
		case 5:
			formation.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
			break;
		case 6:
			current_epoch++;
			break;
		case 7:
			writer.claim.identity.origin_owner_incarnation++;
			break;
		case 8:
			writer.claim.identity.storage_uuid[0]++;
			break;
		case 9:
			cap_ok = false;
			break;
		case 10:
			generation_race = true;
			break;
		case 11:
			cursor = CLUSTER_KO_SHARED_CAPACITY;
			break;
		case 12:
			cursor = UINT32_MAX;
			break;
		}
		saved = cursor;
		memset(&before, 0xa5, sizeof(before));
		observed = before;
		UT_ASSERT(!cluster_ko_shared_structure_next_v2(&cursor, &serial, &observed));
		UT_ASSERT(!cluster_ko_shared_structure_next_v2(NULL, &serial, &observed));
		UT_ASSERT(!cluster_ko_shared_structure_next_v2(&cursor, NULL, &observed));
		UT_ASSERT(!cluster_ko_shared_structure_next_v2(&cursor, &serial, NULL));
		UT_ASSERT_EQ(cursor, saved);
		UT_ASSERT_EQ(serial, 987);
		UT_ASSERT_EQ(memcmp(&observed, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&storage.contexts[0], &owned, sizeof(owned)), 0);
		CritSectionCount = 0;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		cluster_node_id = 0;
	}
}


static void
structure_finish_setup(bool imported, bool single)
{
	cluster_node_id = 0;
	MyProcPid = 199;
	CritSectionCount = 0;
	TopTransactionResourceOwner = (ResourceOwner)1;
	if (imported) {
		remote_structure_setup(false);
		UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
	} else {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *owner
			= prepare_native_structure_owner(false, &binding, wal, (ResourceOwner)1, !single);
		UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &binding, wal, sizeof(wal)));
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(owner));
		xact_callback(XACT_EVENT_COMMIT, NULL);
		UT_ASSERT(cluster_ko_shared_structure_handoff_v2(&owner));
	}
	MyBackendType = B_CHECKPOINTER;
}

UT_TEST(test_structure_local_work_end_requires_complete_scan_and_peer_owner)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		uint64 serial;
		ClusterKoSharedContext before;
		structure_finish_setup(mode == 2, mode == 1);
		serial = storage.contexts[0].serial;
		before = storage.contexts[0];
		if (mode == 0) {
			UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
						 CLUSTER_KO_STRUCTURE_PENDING);
			UT_ASSERT_EQ(relation_scan_calls, 0);
			UT_ASSERT_EQ(memcmp(&storage.contexts[0], &before, sizeof(before)), 0);
			/* This unit's completed-job boundary is checked by offer tests. */
			storage.contexts[0].structure_peers_accepted = 2;
		}
		relation_scan_result = CLUSTER_PCM_PI_RELATION_MORE;
		relation_scan_advance = 128;
		UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
					 CLUSTER_KO_STRUCTURE_PROGRESS);
		UT_ASSERT_EQ(storage.contexts[0].structure_scan, 128);
		UT_ASSERT(storage.contexts[0].used);
		relation_scan_result = CLUSTER_PCM_PI_RELATION_PENDING;
		relation_scan_advance = 0;
		before = storage.contexts[0];
		UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
					 CLUSTER_KO_STRUCTURE_PENDING);
		UT_ASSERT_EQ(memcmp(&storage.contexts[0], &before, sizeof(before)), 0);
		relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
		relation_scan_advance = 64;
		UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
					 CLUSTER_KO_STRUCTURE_RELEASED);
		UT_ASSERT(!storage.contexts[0].used);
		UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
					 CLUSTER_KO_STRUCTURE_INVALID);
		cluster_node_id = 0;
	}
}

UT_TEST(test_structure_local_work_end_rejects_drift_and_invalid_scan)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterKoSharedContext before;
		uint64 serial;
		structure_finish_setup(false, true);
		serial = storage.contexts[0].serial;
		relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
		relation_scan_advance = 64;
		switch (fault) {
		case 0:
			relation_scan_result = CLUSTER_PCM_PI_RELATION_INVALID;
			break;
		case 1:
			relation_scan_epoch_drift = true;
			break;
		case 2:
			relation_scan_slot_drift = true;
			break;
		case 3:
			serial++;
			break;
		case 4:
			CurrentResourceOwner = NULL;
			break;
		case 5:
			relation_gate_ok = false;
			break;
		case 6:
			relation_scan_freeze_drift = true;
			break;
		}
		before = storage.contexts[0];
		UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
					 CLUSTER_KO_STRUCTURE_INVALID);
		if (fault == 2)
			before.serial++;
		UT_ASSERT_EQ(memcmp(&storage.contexts[0], &before, sizeof(before)), 0);
		UT_ASSERT_EQ(storage.contexts[0].structure_scan, 0);
		cluster_node_id = 0;
	}
}

UT_TEST(test_structure_local_work_end_restarts_prefix_after_gate_generation_change)
{
	uint64 serial;
	structure_finish_setup(false, true);
	serial = storage.contexts[0].serial;
	relation_scan_result = CLUSTER_PCM_PI_RELATION_MORE;
	relation_scan_advance = 128;
	UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
				 CLUSTER_KO_STRUCTURE_PROGRESS);
	relation_gate_freeze++;
	relation_scan_result = CLUSTER_PCM_PI_RELATION_EMPTY;
	UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
				 CLUSTER_KO_STRUCTURE_PROGRESS);
	UT_ASSERT(storage.contexts[0].used);
	UT_ASSERT_EQ(storage.contexts[0].structure_scan, 0);
	UT_ASSERT_EQ(relation_scan_calls, 1);
	UT_ASSERT_EQ(cluster_ko_shared_structure_finish_local_v2(0, serial),
				 CLUSTER_KO_STRUCTURE_RELEASED);
	cluster_node_id = 0;
}

UT_TEST(test_remote_structure_accept_is_idempotent_and_never_overwrites_conflict)
{
	ClusterKoShared before;
	remote_structure_setup(false);
	UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
	before = storage;
	UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
	UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
	offered_structure.proof.structural.terminal.binding.record_crc++;
	UT_ASSERT(!cluster_ko_shared_structure_accept_v2(offer_notice, 0));
	UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
	cluster_node_id = 0;
}

UT_TEST(test_remote_structure_accept_full_and_stale_cut_preserve_every_slot)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterKoShared before;
		remote_structure_setup(false);
		switch (fault) {
		case 0:
			offer_notice_live = false;
			break;
		case 1:
			CurrentResourceOwner = NULL;
			break;
		case 2:
			MyBackendType = B_LMON;
			break;
		case 3:
			CritSectionCount++;
			break;
		case 4:
			current_epoch++;
			break;
		case 5:
			formation.membership.last_admitted_incarnation[0]++;
			break;
		case 6:
			writer.claim.identity.origin_owner_incarnation++;
			break;
		case 7:
			cap_ok = false;
			break;
		case 8:
			generation_race = true;
			break;
		case 9:
			for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
				storage.contexts[i].used = true;
				storage.contexts[i].serial = i + 1;
			}
			break;
		case 10:
			storage.context_serial = UINT64_MAX;
			break;
		case 11:
			offered_structure.kind = CLUSTER_PI_WRITEBACK_STRUCTURAL_V2;
			break;
		}
		before = storage;
		UT_ASSERT(!cluster_ko_shared_structure_accept_v2(offer_notice, 0));
		UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
		CritSectionCount = 0;
	}
	cluster_node_id = 0;
}

UT_TEST(test_remote_structure_observation_rechecks_cut_and_exact_lifetime)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterKoShared before;
		ClusterPageWalBindingV1 observed, saved;
		ClusterKoSharedMessageV2 projected, original;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], original_wal[sizeof(wal)];
		uint64 serial;
		remote_structure_setup(false);
		UT_ASSERT(cluster_ko_shared_structure_accept_v2(offer_notice, 0));
		serial = storage.contexts[0].serial;
		if (fault == 0)
			current_epoch++;
		if (fault == 1)
			formation.membership.last_admitted_incarnation[2]++;
		if (fault == 2)
			serial++;
		if (fault == 3)
			writer.claim.identity.origin_owner_incarnation++;
		if (fault == 4)
			cap_ok = false;
		before = storage;
		memset(&saved, 0x5a, sizeof(saved));
		observed = saved;
		memset(original_wal, 0x5a, sizeof(original_wal));
		memcpy(wal, original_wal, sizeof(wal));
		memset(&original, 0x5a, sizeof(original));
		projected = original;
		UT_ASSERT(
			!cluster_ko_shared_structure_observation_v2(0, serial, &observed, wal, sizeof(wal)));
		UT_ASSERT(!cluster_ko_shared_structure_peer_v2(0, serial, 2, &projected));
		UT_ASSERT_EQ(memcmp(&storage, &before, sizeof(storage)), 0);
		UT_ASSERT_EQ(memcmp(&observed, &saved, sizeof(saved)), 0);
		UT_ASSERT_EQ(memcmp(wal, original_wal, sizeof(wal)), 0);
		UT_ASSERT_EQ(memcmp(&projected, &original, sizeof(original)), 0);
	}
	cluster_node_id = 0;
}

UT_TEST(test_structure_projection_keeps_original_cut_and_origin_route)
{
	ClusterKoSharedMessageV2 request = structure_projection_setup(), result, expected;
	ClusterKoShared before = storage;
	uint8 original_bytes[CLUSTER_KO_SHARED_V2_BYTES], projected_bytes[sizeof(original_bytes)];

	UT_ASSERT(cluster_ko_shared_peer_projection_v2(&request, 2, &result));
	expected = request;
	expected.peer_node = 2;
	expected.peer_boot = 33;
	UT_ASSERT(memcmp(&expected, &result, sizeof(result)) == 0);
	UT_ASSERT(cluster_ko_shared_encode_v2(&request, original_bytes, sizeof(original_bytes)));
	UT_ASSERT(cluster_ko_shared_encode_v2(&result, projected_bytes, sizeof(projected_bytes)));
	/* Only the actual peer coordinates change; the complete original KO
	 * batch, old segment and canonical membership digest stay immutable. */
	for (unsigned i = 0; i < sizeof(original_bytes); i++)
		if ((i < 40 || i >= 48) && (i < 110 || i >= 112))
			UT_ASSERT_EQ(original_bytes[i], projected_bytes[i]);
	UT_ASSERT(cluster_ko_shared_peer_projection_v2(&request, 1, &result));
	UT_ASSERT(memcmp(&request, &result, sizeof(result)) == 0);
	UT_ASSERT_EQ(space_reads + flush_count + sync_count + drop_count + send_calls, 0);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT(memcmp(&before, &storage, sizeof(storage)) == 0);
}

UT_TEST(test_structure_projection_preserves_origin_endpoint_and_local_role)
{
	ClusterKoSharedMessageV2 request = structure_projection_setup(), result;
	cluster_node_id = 1;
	writer.claim.identity.origin_node_id = 1;
	writer.claim.identity.origin_owner_incarnation = 22;
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(cluster_ko_shared_peer_projection_v2(&request, 2, &result));
	UT_ASSERT_EQ(result.origin_node, 1);
	UT_ASSERT_EQ(result.origin_boot, 22);
	UT_ASSERT_EQ(result.peer_node, 2);
	UT_ASSERT_EQ(result.peer_boot, 33);
	UT_ASSERT(cluster_ko_shared_peer_projection_v2(&request, 0, &result));
	UT_ASSERT(memcmp(&request, &result, sizeof(result)) == 0);
	cluster_node_id = 0;
}

UT_TEST(test_structure_projection_refuses_changed_cut_capability_and_wrong_actor)
{
	for (unsigned fault = 0; fault < 27; fault++) {
		ClusterKoSharedMessageV2 request = structure_projection_setup(), output, before;
		int32 target = 2;
		switch (fault) {
		case 0: request.origin_boot++; break;
		case 1: request.peer_boot++; break;
		case 2: request.epoch++; break;
		case 3: request.key.system_identifier++; break;
		case 4: request.key.database_incarnation++; break;
		case 5: request.key.storage_uuid[1]++; break;
		case 6: request.member_digest[0] ^= 1; break;
		case 7: cap_missing_peer = 1; break;
		case 8: cap_missing_peer = 2; break;
		case 9: cap_zero_peer = 1; break;
		case 10: cap_zero_peer = 2; break;
		case 11: cap_change_cut_peer = 2; break;
		case 12: generation_race = true; break;
		case 13: formation.membership.last_admitted_incarnation[2]++; break;
		case 14: formation.membership.membership_state[2] = CLUSTER_MEMBER_DEAD; break;
		case 15: capture_ok = false; break;
		case 16: target = 0; break;
		case 17: target = -1; break;
		case 18: target = CLUSTER_KO_SHARED_NODE_LIMIT; break;
		case 19: MyBackendType = B_BACKEND; break;
		case 20: MyBackendType = B_LMON; break;
		case 21: CurrentResourceOwner = NULL; break;
		case 22: CritSectionCount = 1; break;
		case 23: cluster_shared_config = false; break;
		case 24: request.verb = CLUSTER_KO_SHARED_ACK; request.status = CLUSTER_KO_SHARED_DONE; break;
		case 25: writer.claim.identity.origin_owner_incarnation++; break;
		case 26:
			cluster_node_id = 2;
			writer.claim.identity.origin_node_id = 2;
			writer.claim.identity.origin_owner_incarnation = 33;
			target = 0;
			break;
		}
		memset(&before, 0xa5, sizeof(before));
		output = before;
		UT_ASSERT(!cluster_ko_shared_peer_projection_v2(&request, target, &output));
		UT_ASSERT(memcmp(&output, &before, sizeof(output)) == 0);
		UT_ASSERT_EQ(space_reads + flush_count + sync_count + drop_count + send_calls, 0);
		UT_ASSERT_EQ(completion_allocations, 0);
		CritSectionCount = 0;
		cluster_node_id = 0;
	}
	UT_ASSERT(!cluster_ko_shared_peer_projection_v2(NULL, 1, NULL));
}

UT_TEST(test_structural_cut_query_accepts_each_current_member_without_authority)
{
	for (int local = 0; local < 3; local++) {
		ClusterKoSharedMessageV2 request = structure_projection_setup();
		ClusterKoShared before = storage;
		cluster_node_id = local;
		writer.claim.identity.origin_node_id = local;
		writer.claim.identity.origin_owner_incarnation = (local + 1) * 11;
		UT_ASSERT(cluster_ko_shared_cut_current_v2(&request));
		UT_ASSERT(memcmp(&before, &storage, sizeof(storage)) == 0);
		UT_ASSERT_EQ(space_reads + flush_count + sync_count + drop_count + send_calls, 0);
		UT_ASSERT_EQ(completion_allocations, 0);
		/* A third member may inspect the cut, but not consume the KO request. */
		if (local == 2)
			UT_ASSERT(!ko_shared_control_current(&request));
	}
	cluster_node_id = 0;
}

UT_TEST(test_structural_cut_query_refuses_wire_only_nodes_before_sampling)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterKoSharedMessageV2 request = structure_projection_setup();
		uint8 wire[CLUSTER_KO_SHARED_V2_BYTES];
		int node = fault % 2 == 0 ? CLUSTER_KO_SHARED_NODE_LIMIT : 127;
		if (fault < 2)
			request.origin_node = node;
		else
			request.peer_node = node;
		request.members[node / 8] |= 1u << (node % 8);
		UT_ASSERT(cluster_ko_shared_encode_v2(&request, wire, sizeof(wire)));
		cut_writer_samples = 0;
		UT_ASSERT(!cluster_ko_shared_cut_current_v2(&request));
		UT_ASSERT_EQ(cut_writer_samples, 0);
	}
}

UT_TEST(test_structural_cut_query_rejects_changed_identity_or_sample)
{
	for (unsigned fault = 0; fault < 24; fault++) {
		ClusterKoSharedMessageV2 request = structure_projection_setup(), before;
		cluster_node_id = 2;
		writer.claim.identity.origin_node_id = 2;
		writer.claim.identity.origin_owner_incarnation = 33;
		cut_writer_samples = 0;
		switch (fault) {
		case 0: request.origin_boot++; break;
		case 1: request.peer_boot++; break;
		case 2: request.epoch++; break;
		case 3: request.key.system_identifier++; break;
		case 4: request.key.database_incarnation++; break;
		case 5: request.key.storage_uuid[0]++; break;
		case 6: request.member_digest[0] ^= 1; break;
		case 7: formation.membership.membership_state[2] = CLUSTER_MEMBER_DEAD; break;
		case 8: formation.membership.last_admitted_incarnation[2]++; break;
		case 9: cap_missing_peer = 0; break;
		case 10: cap_missing_peer = 1; break;
		case 11: cap_zero_peer = 0; break;
		case 12: cap_zero_peer = 1; break;
		case 13: cap_change_cut_peer = 0; break;
		case 14: generation_race = true; break;
		case 15: capture_ok = false; break;
		case 16: cluster_shared_config = false; break;
		case 17: cluster_enabled = false; break;
		case 18: request.verb = CLUSTER_KO_SHARED_ACK; request.status = CLUSTER_KO_SHARED_DONE; break;
		case 19: cut_writer_change_at = 2; break;
		case 20: cut_cap_change_peer = 0; break;
		case 21: cut_cap_change_peer = 1; break;
		case 22: request.members[0] ^= 4; break;
		case 23: writer.claim.identity.origin_node_id = 1; break;
		}
		before = request;
		UT_ASSERT(!cluster_ko_shared_cut_current_v2(&request));
		UT_ASSERT(memcmp(&request, &before, sizeof(request)) == 0);
		UT_ASSERT_EQ(space_reads + flush_count + sync_count + drop_count + send_calls, 0);
		cluster_node_id = 0;
	}
	UT_ASSERT(!cluster_ko_shared_cut_current_v2(NULL));
}

/* Allocation-backed continuation is an original KO owner, not an effect
 * certificate. The active 64 slots remain the execution boundary. */

UT_TEST(test_native_committed_seventy_two_owners_survive_full_background_region)
{
	for (unsigned automatic = 0; automatic < 2; automatic++)
		for (unsigned drop = 0; drop < 2; drop++) {
			ClusterKoCompletionV2 *owners[72] = { 0 }, *template_owner;
			bool seen[72] = { false };
			ClusterPageWalBindingV1 binding, observed, expected[72];
			ClusterSpaceStructureChange change;
			uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES],
				records[72][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
			const char *reason;
			template_owner = prepare_native_structure(drop, &binding, wal);
			UT_ASSERT(cluster_space_structure_wal_decode(wal, sizeof(wal), &change));
			cluster_ko_shared_release_v2(&template_owner);
			multiple_barriers = true;
			allocated_batch = last_shared_request.batch_id;
			for (unsigned i = 0; i < lengthof(owners); i++) {
				RelFileLocator locator = space_identity.key.locator;
				locator.relNumber = 1000 + i;
				space_identity.key.locator = locator;
				cluster_ko_flush_and_wait_ack(locator, RELPERSISTENCE_PERMANENT);
				UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key,
													 space_identity.incarnation, &owners[i]));
				change.identity.expected.key.locator = change.identity.result.key.locator = locator;
				change.reservation.before.identity.key.locator
					= change.reservation.result.identity.key.locator = locator;
				UT_ASSERT(
					cluster_space_structure_wal_encode(&change, records[i], sizeof(records[i])));
				expected[i] = binding;
				expected[i].identity.locator = locator;
				UT_ASSERT(cluster_ko_shared_observe_space_v2(owners[i], &expected[i], records[i],
															 sizeof(records[i])));
				if (!drop)
					UT_ASSERT(cluster_ko_shared_observe_truncate_v2(owners[i]));
			}
			for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
				storage.contexts[i].used = true;
				storage.contexts[i].pid = MyProcPid + 1;
			}
			if (automatic) {
				postcommit_owner = owners[0];
				postcommit_binding = expected[0];
				memcpy(postcommit_wal, records[0], sizeof(postcommit_wal));
				postcommit_drop = drop;
				postcommit_error = postcommit_scope_changed = postcommit_callback_error = false;
				postcommit_native_count = lengthof(owners);
				postcommit_deletes = postcommit_phases = 0;
				postcommit_storage_consumer = NULL;
				native_commit_active = true;
				run_native_postcommit();
				UT_ASSERT_EQ(storage.native_waiting, lengthof(owners));
				UT_ASSERT_EQ(completion_allocations, 0);
				postcommit_native_count = 1;
			} else {
				xact_callback(XACT_EVENT_COMMIT, NULL);
				for (unsigned i = 0; i < lengthof(owners); i++) {
					UT_ASSERT(cluster_ko_shared_native_handoff_v2(&owners[i]));
					UT_ASSERT(owners[i] == NULL);
				}
			}
			cluster_ko_shared_postcommit_cleanup_v2();
			exit_callback(0, (Datum)0);
			UT_ASSERT_EQ(completion_allocations, 0);
			MyBackendType = B_CHECKPOINTER;
			ko_exit_registered = false;
			exit_callback = NULL; /* A fresh checkpointer has no prior native barrier. */
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PENDING);
			memset(storage.contexts, 0,
				   sizeof(storage.contexts)); /* Release the fixture's unrelated owners. */
			UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason),
						 CLUSTER_NORMAL_STOP_PENDING);
			for (unsigned n = 0; n < lengthof(owners); n++) {
				uint32 i;
				if (n == 1) {
					ClusterKoCompletionV2 *arrival = NULL;
					ClusterPageWalBindingV1 next = binding;
					/* An independent committed arrival cannot preempt this batch. */
					MyBackendType = B_BACKEND;
					space_identity.key.locator.relNumber = 2000;
					cluster_ko_flush_and_wait_ack(space_identity.key.locator,
												  RELPERSISTENCE_PERMANENT);
					UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key,
														 space_identity.incarnation, &arrival));
					next.identity.locator = space_identity.key.locator;
					change.identity.expected.key.locator = change.identity.result.key.locator
						= space_identity.key.locator;
					change.reservation.before.identity.key.locator
						= change.reservation.result.identity.key.locator
						= space_identity.key.locator;
					UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
					UT_ASSERT(cluster_ko_shared_observe_space_v2(arrival, &next, wal, sizeof(wal)));
					if (!drop)
						UT_ASSERT(cluster_ko_shared_observe_truncate_v2(arrival));
					xact_callback(XACT_EVENT_COMMIT, NULL);
					UT_ASSERT(cluster_ko_shared_native_handoff_v2(&arrival));
					MyBackendType = B_CHECKPOINTER;
				}
				UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
				i = storage.contexts[0].terminal.identity.locator.relNumber - 1000;
				UT_ASSERT(i < lengthof(owners));
				if (i < lengthof(owners)) {
					UT_ASSERT(!seen[i]);
					seen[i] = true;
					UT_ASSERT(memcmp(&storage.contexts[0].terminal, &expected[i], sizeof(binding))
							  == 0);
					UT_ASSERT(memcmp(storage.contexts[0].structure, records[i], sizeof(wal)) == 0);
				}
				/* An undurable DROP must not be visible to any receipt consumer. */
				UT_ASSERT_EQ(cluster_ko_shared_structure_observation_v2(
								 0, storage.contexts[0].serial, &observed, wal, sizeof(wal)),
							 !drop);
				memset(
					storage.contexts, 0,
					sizeof(storage.contexts)); /* Physical/page consumer is a separate boundary. */
			}
			for (unsigned i = 0; i < lengthof(owners); i++)
				UT_ASSERT(seen[i]);
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
			UT_ASSERT_EQ(storage.contexts[0].terminal.identity.locator.relNumber, 2000);
			memset(storage.contexts, 0, sizeof(storage.contexts));
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_RELEASED);
			UT_ASSERT_EQ(native_allocated, 0);
			UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_READY);
			UT_ASSERT(exit_callback != NULL);
			{
				unsigned detached = native_detaches, released = native_releases;
				exit_callback(0, (Datum)0);
				UT_ASSERT_EQ(native_detaches, detached + 1);
				UT_ASSERT_EQ(native_releases, released + 1);
				UT_ASSERT(ko_native_area == NULL);
			}
		}
}

UT_TEST(test_native_continuation_refuses_uncommitted_or_foreign_owner)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterPageWalBindingV1 binding;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterKoCompletionV2 *owner = prepare_native_structure(true, &binding, wal), *before;
		ClusterKoCompletionV2 copied;
		if (fault != 0)
			UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &binding, wal, sizeof(wal)));
		if (fault != 1)
			xact_callback(fault == 2 ? XACT_EVENT_PREPARE : XACT_EVENT_COMMIT, NULL);
		if (fault == 3)
			CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)2;
		if (fault == 4)
			CritSectionCount = 1;
		if (fault == 5) {
			copied = *owner;
			owner = &copied;
		}
		if (fault == 6)
			MyBackendType = B_CHECKPOINTER;
		before = owner;
		UT_ASSERT(!cluster_ko_shared_native_handoff_v2(&owner));
		UT_ASSERT(owner == before);
		CritSectionCount = 0;
		MyBackendType = B_BACKEND;
		CurrentResourceOwner = CurTransactionResourceOwner = (ResourceOwner)1;
		resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_native_autovacuum_truncate_keeps_its_committed_continuation)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *owner = prepare_native_structure(false, &binding, wal);
	bool moved;
	UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &binding, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_truncate_v2(owner));
	MyBackendType = B_AUTOVAC_WORKER;
	xact_callback(XACT_EVENT_COMMIT, NULL);
	moved = cluster_ko_shared_native_handoff_v2(&owner);
	UT_ASSERT(moved);
	if (moved) {
		UT_ASSERT(owner == NULL);
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
		UT_ASSERT(!storage.contexts[0].structure_drop_pending);
	} else
		cluster_ko_shared_release_v2(&owner);
	UT_ASSERT_EQ(native_allocated, 0);
	UT_ASSERT_EQ(completion_allocations, 0);
}

UT_TEST(test_native_continuation_allocation_failure_precedes_space_and_cleans_owner)
{
	volatile bool caught = false;
	reset_test();
	cluster_shared_config = drive_shared_ack = true;
	native_alloc_fail = expecting_error = true;
	PG_TRY();
	{
		cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_OUT_OF_MEMORY);
	UT_ASSERT_EQ(native_allocate_calls, 1);
	UT_ASSERT_EQ(native_allocated, 0);
	UT_ASSERT(ko_completions != NULL && !ko_completions->space_observed);
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(completion_allocations, 0);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		UT_ASSERT(!storage.contexts[i].used);
}

UT_TEST(test_native_continuation_retains_stale_commit_but_cannot_serve_it)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterKoCompletionV2 *owner = prepare_native_structure(true, &binding, wal), *old;
	const char *reason;
	UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &binding, wal, sizeof(wal)));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	current_epoch++;
	old = owner;
	UT_ASSERT(cluster_ko_shared_native_handoff_v2(&owner));
	UT_ASSERT(owner == NULL);
	UT_ASSERT(!cluster_ko_shared_native_handoff_v2(&old));
	MyBackendType = B_CHECKPOINTER;
	for (unsigned attempt = 0; attempt < 3; attempt++) {
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(!storage.contexts[0].used);
	}
	current_epoch--;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
}


/* The storage consumer is an explicit boundary here. Its real fd/fsync
 * failure matrix remains in the storage owner's tests before activation. */
static uint32
prepare_promoted_drop(ClusterPageWalBindingV1 *binding, uint8 *wal)
{
	ClusterKoCompletionV2 *owner = prepare_native_structure(true, binding, wal);
	if (!cluster_ko_shared_observe_space_v2(owner, binding, wal, CLUSTER_SPACE_STRUCTURE_WAL_BYTES))
		abort();
	xact_callback(XACT_EVENT_COMMIT, NULL);
	if (!cluster_ko_shared_native_handoff_v2(&owner))
		abort();
	MyBackendType = B_CHECKPOINTER;
	if (cluster_ko_shared_native_promote_v2() != CLUSTER_KO_STRUCTURE_PROGRESS)
		abort();
	for (uint32 i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (storage.contexts[i].used && storage.contexts[i].structure_drop_pending)
			return i;
	abort();
}

UT_TEST(test_drop_work_preserves_same_storage_state_until_exact_finish)
{
	ClusterPageWalBindingV1 terminal, observed;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], readback[sizeof(wal)];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoDropWorkV2 *work = NULL, *again = NULL;
	uint64 *state;
	const char *reason;
	uint64 serial = storage.contexts[slot].serial;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 4 * sizeof(uint64), &work));
	UT_ASSERT_EQ(cursor, slot + 1);
	state = cluster_ko_shared_drop_work_state_v2(work, 4 * sizeof(uint64));
	UT_ASSERT(state != NULL);
	UT_ASSERT_EQ(state[0], 0);
	state[0] = 7;
	state[1] = 810; /* Original fd/stage carrier, not an effect. */
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(cluster_ko_shared_drop_work_revalidate_v2(work));
	UT_ASSERT(cluster_ko_shared_drop_work_read_v2(work, &observed, readback, sizeof(readback)));
	UT_ASSERT(memcmp(&observed, &terminal, sizeof(terminal)) == 0);
	UT_ASSERT(memcmp(wal, readback, sizeof(wal)) == 0);
	UT_ASSERT(!cluster_ko_shared_structure_observation_v2(slot, serial, &observed, readback,
														  sizeof(readback)));
	cursor = slot;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 4 * sizeof(uint64), &again));
	UT_ASSERT(again == work);
	UT_ASSERT(cluster_ko_shared_drop_work_state_v2(again, 4 * sizeof(uint64)) == state);
	UT_ASSERT_EQ(state[1], 810);
	UT_ASSERT(cluster_ko_shared_drop_work_state_v2(work, sizeof(uint64)) == NULL);
	/* Only the original durable storage-success branch may call this API. */
	UT_ASSERT(cluster_ko_shared_drop_work_finish_v2(&work));
	UT_ASSERT(work == NULL);
	UT_ASSERT(!cluster_ko_shared_drop_work_finish_v2(&again));
	UT_ASSERT(cluster_ko_shared_structure_observation_v2(slot, serial, &observed, readback,
														 sizeof(readback)));
	UT_ASSERT(storage.contexts[slot].used && storage.contexts[slot].structure_owned);
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(completion_allocations, 0);
}

UT_TEST(test_drop_work_rejects_owner_and_full_cut_drift_without_losing_progress)
{
	for (unsigned bad = 0; bad < 12; bad++) {
		ClusterPageWalBindingV1 terminal, observed, sentinel;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES], readback[sizeof(wal)];
		uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
		ClusterKoDropWorkV2 *work = NULL, *saved;
		ClusterKoSharedContext before;
		ClusterWalSourceRef source = writer;
		uint64 epoch = current_epoch;
		int pid = MyProcPid;
		ResourceOwner owner = CurrentResourceOwner;
		UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
		before = storage.contexts[slot];
		saved = work;
		switch (bad) {
		case 0:
			CurrentResourceOwner = (ResourceOwner)2;
			break;
		case 1:
			MyProcPid++;
			break;
		case 2:
			MyBackendType = B_BG_WRITER;
			break;
		case 3:
			CritSectionCount = 1;
			break;
		case 4:
			current_epoch++;
			break;
		case 5:
			writer.claim.identity.origin_owner_incarnation++;
			break;
		case 6:
			writer.claim.database_incarnation++;
			break;
		case 7:
			storage.contexts[slot].serial++;
			break;
		case 8:
			storage.contexts[slot].structure_drop_pending = false;
			break;
		case 9:
			storage.contexts[slot].drop_executor_pid++;
			break;
		case 10:
			storage.contexts[slot].terminal.record_start++;
			break;
		case 11:
			storage.contexts[slot].request.members[0] ^= 4;
			break;
		}
		memset(&observed, 0xa5, sizeof(observed));
		sentinel = observed;
		memset(readback, 0xa5, sizeof(readback));
		UT_ASSERT(
			!cluster_ko_shared_drop_work_read_v2(work, &observed, readback, sizeof(readback)));
		UT_ASSERT(memcmp(&observed, &sentinel, sizeof(observed)) == 0);
		for (Size i = 0; i < sizeof(readback); i++)
			UT_ASSERT_EQ(readback[i], 0xa5);
		UT_ASSERT(!cluster_ko_shared_drop_work_finish_v2(&work));
		UT_ASSERT(work == saved);
		MyProcPid = pid;
		MyBackendType = B_CHECKPOINTER;
		CurrentResourceOwner = owner;
		CritSectionCount = 0;
		current_epoch = epoch;
		writer = source;
		storage.contexts[slot] = before;
		UT_ASSERT(cluster_ko_shared_drop_work_finish_v2(&work));
		UT_ASSERT_EQ(completion_allocations, 0);
	}
}

UT_TEST(test_drop_work_exit_and_slot_reuse_never_adopt_unknown_physical_progress)
{
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 slot = prepare_promoted_drop(&terminal, wal), cursor = slot;
	ClusterKoDropWorkV2 *work = NULL, *other = NULL, copy, *fake;
	ClusterKoSharedContext before;
	int pid = MyProcPid;
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &work));
	before = storage.contexts[slot];
	copy = *work;
	fake = &copy;
	UT_ASSERT(!cluster_ko_shared_drop_work_finish_v2(&fake));
	MyProcPid++;
	cursor = slot;
	UT_ASSERT(!cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &other));
	UT_ASSERT_EQ(cursor, slot);
	UT_ASSERT(other == NULL);
	MyProcPid = pid;
	exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(completion_allocations, 0);
	UT_ASSERT(memcmp(&before, &storage.contexts[slot], sizeof(before)) == 0);
	MyProcPid++;
	cursor = slot;
	UT_ASSERT(!cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &other));
	UT_ASSERT(!cluster_ko_shared_drop_work_finish_v2(&work));
	UT_ASSERT(storage.contexts[slot].structure_drop_pending);
	MyProcPid = pid;
}


UT_TEST(test_drop_work_bounded_scan_continues_after_a_retained_storage_failure)
{
	ClusterPageWalBindingV1 terminal, next;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 first = prepare_promoted_drop(&terminal, wal), cursor = first;
	ClusterKoCompletionV2 *completion = NULL;
	ClusterKoDropWorkV2 *one = NULL, *two = NULL;
	ClusterSpaceStructureChange change;
	uint64 *state;
	uint32 second;
	UT_ASSERT(cluster_space_structure_wal_decode(wal, sizeof(wal), &change));
	MyBackendType = B_BACKEND;
	multiple_barriers = true;
	allocated_batch = last_shared_request.batch_id;
	space_identity.key.locator.relNumber++;
	cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(
		cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &completion));
	change.identity.expected.key.locator = change.identity.result.key.locator
		= space_identity.key.locator;
	change.reservation.before.identity.key.locator = change.reservation.result.identity.key.locator
		= space_identity.key.locator;
	next = terminal;
	next.identity.locator = space_identity.key.locator;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_space_v2(completion, &next, wal, sizeof(wal)));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	UT_ASSERT(cluster_ko_shared_native_handoff_v2(&completion));
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
	UT_ASSERT(!cluster_ko_shared_drop_work_begin_v2(&cursor, 0, &one));
	UT_ASSERT(!cluster_ko_shared_drop_work_begin_v2(&cursor, MaxAllocSize, &one));
	UT_ASSERT_EQ(cursor, first);
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &one));
	state = cluster_ko_shared_drop_work_state_v2(one, 16);
	UT_ASSERT(state != NULL);
	state[0] = 314; /* Retained first I/O failure: do not call finish. */
	UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(&cursor, 16, &two));
	second = cursor - 1;
	UT_ASSERT(second != first && two != one);
	UT_ASSERT(cluster_ko_shared_drop_work_finish_v2(&two));
	UT_ASSERT(storage.contexts[first].structure_drop_pending);
	UT_ASSERT(!storage.contexts[second].structure_drop_pending);
	UT_ASSERT_EQ(state[0], 314);
	UT_ASSERT(cluster_ko_shared_drop_work_finish_v2(&one));
	UT_ASSERT_EQ(completion_allocations, 0);
}

#include "test_cluster_ko_abandon.h"

int
main(void)
{
	printf("# sizeof_ClusterKoShared=%zu\n", sizeof(ClusterKoShared));
	UT_PLAN(100);
	UT_RUN(test_only_actual_consumer_can_observe);
	UT_RUN(test_real_admission_flush_drop_ack_order);
	UT_RUN(test_origin_barrier_discards_lease_even_without_remote_work);
	UT_RUN(test_full_ring_wrap_and_retained_stale_bytes);
	UT_RUN(test_later_invalid_slot_overrides_pending);
	UT_RUN(test_refused_removal_never_forges_apply_ack);
	UT_RUN(test_sealed_new_request_never_enqueues_or_acks);
	UT_RUN(test_shared_existing_forks_synced_before_drop_and_ack);
	UT_RUN(test_shared_sync_failure_preserves_buffers_and_never_acks);
	UT_RUN(test_queued_old_epoch_refused_before_page_work);
	UT_RUN(test_epoch_change_during_io_never_acks_new_epoch);
	UT_RUN(test_request_requires_exact_nonzero_epoch);
	UT_RUN(test_ack_requires_exact_nonzero_epoch);
	UT_RUN(test_origin_completion_stays_in_request_epoch);
	UT_RUN(test_shared_legacy_request_and_ack_refuse_before_work);
	UT_RUN(test_shared_member_digest_is_canonical_and_covers_every_boot);
	UT_RUN(test_shared_ingress_rejects_changed_cut_before_any_page_io);
	UT_RUN(test_shared_old_segment_and_queued_boot_change_never_flush);
	UT_RUN(test_shared_same_epoch_boot_change_during_each_io_never_acks);
	UT_RUN(test_shared_ack_send_revalidates_boot_and_normal_stop_owner);
	UT_RUN(test_shared_origin_uses_real_ack_handler_and_original_wait_entry);
	UT_RUN(test_shared_origin_missing_capability_has_no_barrier_or_send);
	UT_RUN(test_shared_cancel_releases_context_and_suppresses_unsent_request);
	UT_RUN(test_shared_ack_needs_exact_registered_original_scope);
	UT_RUN(test_shared_capacity_and_backend_exit_preserve_other_owners);
	UT_RUN(test_shared_completion_survives_original_wait_and_checks_scope);
	UT_RUN(test_shared_completion_rejects_other_owner_and_every_changed_cut);
	UT_RUN(test_shared_completion_cleanup_is_owner_scoped_and_no_peer_is_not_an_ack);
	UT_RUN(test_shared_completion_has_no_handle_after_cancel_or_unsupported_scope);
	UT_RUN(test_shared_completion_raw_copy_and_slot_reuse_do_not_convey_ownership);
	UT_RUN(test_shared_subcommit_transfers_original_completion_to_parent);
	UT_RUN(test_shared_subabort_cancels_only_the_child_completion);
	UT_RUN(test_native_shared_barrier_is_claimed_once_by_original_space_owner);
	UT_RUN(test_native_seventy_two_relations_do_not_hold_completed_transport_slots);
	UT_RUN(test_native_completion_claim_refuses_changed_identity_and_owner);
	UT_RUN(test_native_claim_cannot_borrow_direct_handle_and_full_capacity_refuses_before_send);
	UT_RUN(test_native_claim_rejects_ambiguous_repeated_barriers);
	UT_RUN(test_native_portal_barrier_survives_precommit_without_portal_release);
	UT_RUN(test_native_portal_subtransaction_keeps_exact_transaction_scope);
	UT_RUN(test_native_barrier_rejects_missing_or_unrelated_transaction_before_io);
	UT_RUN(test_native_space_observation_is_original_once_and_transaction_owned);
	UT_RUN(test_native_space_observation_refuses_foreign_or_changed_record);
	UT_RUN(test_native_truncate_effect_needs_original_space_and_is_once_only);
	UT_RUN(test_native_truncate_effect_refuses_drop_late_or_changed_owner);
	UT_RUN(test_native_truncate_effect_export_rechecks_cut_without_changing_output);
	UT_RUN(test_native_relation_offer_requires_real_commit_and_original_effect);
	UT_RUN(test_native_relation_offer_refuses_drop_missing_effect_and_changed_scope);
	UT_RUN(test_structure_handoff_consumes_original_handle_without_new_work);
	UT_RUN(test_structure_page_scan_includes_local_single_member_and_imported_results);
	UT_RUN(test_structure_page_scan_reaches_last_slot_and_skips_unowned_entries);
	UT_RUN(test_structure_page_scan_refuses_stale_cut_without_modifying_outputs);
	UT_RUN(test_structure_offer_ack_only_advances_original_peer_progress);
	UT_RUN(test_structure_offer_ack_refuses_wrong_scope_and_concurrent_slot_reuse);
	UT_RUN(test_structure_handoff_refuses_uncommitted_incomplete_and_wrong_owner);
	UT_RUN(test_structure_background_scan_preserves_stale_responsibility);
	UT_RUN(test_structure_handoff_uses_last_free_background_slot);
	UT_RUN(test_native_full_background_region_preserves_proof_until_retry);
	UT_RUN(test_native_private_proof_survives_slot_reuse_but_not_cut_or_owner_change);
	UT_RUN(test_structure_one_member_keeps_local_owner_without_fake_peer);
	UT_RUN(test_native_postcommit_retains_exact_observation_until_pending_deletes_return);
	UT_RUN(test_native_postcommit_refuses_drift_but_always_cleans_original_local_owner);
	UT_RUN(test_native_postcommit_error_and_exit_do_not_leave_a_completion);
	UT_RUN(test_native_commit_transfers_before_callbacks_and_local_cleanup);
	UT_RUN(test_native_commit_callback_error_cannot_cancel_shared_responsibility);
	UT_RUN(test_native_commit_keeps_old_cut_without_executing_it);
	UT_RUN(test_native_checkpointer_keeps_owner_gates_and_services_sync_requests);
	UT_RUN(test_native_subcommit_cannot_qualify_pending_drop_before_top_commit);
	UT_RUN(test_pending_drop_refuses_other_owner_and_changed_scope_without_touching_output);
	UT_RUN(test_pending_drop_rejects_ambiguous_original_native_completions);
	UT_RUN(test_prepare_and_noncommit_events_never_retain_native_observation);
	UT_RUN(test_native_drop_effect_is_once_only_after_original_top_commit);
	UT_RUN(test_native_drop_effect_refuses_wrong_lifetime_scope_or_handle);
	UT_RUN(test_native_drop_result_handoff_preserves_original_shared_obligation);
	UT_RUN(test_remote_structure_accept_preserves_responsibility_after_notice_and_exit);
	UT_RUN(test_remote_structure_accept_is_idempotent_and_never_overwrites_conflict);
	UT_RUN(test_remote_structure_accept_full_and_stale_cut_preserve_every_slot);
	UT_RUN(test_remote_structure_observation_rechecks_cut_and_exact_lifetime);
	UT_RUN(test_structure_projection_keeps_original_cut_and_origin_route);
	UT_RUN(test_structure_projection_preserves_origin_endpoint_and_local_role);
	UT_RUN(test_structure_projection_refuses_changed_cut_capability_and_wrong_actor);
	UT_RUN(test_structural_cut_query_accepts_each_current_member_without_authority);
	UT_RUN(test_structural_cut_query_refuses_wire_only_nodes_before_sampling);
	UT_RUN(test_structural_cut_query_rejects_changed_identity_or_sample);
	UT_RUN(test_structure_local_work_end_requires_complete_scan_and_peer_owner);
	UT_RUN(test_structure_local_work_end_rejects_drift_and_invalid_scan);
	UT_RUN(test_structure_local_work_end_restarts_prefix_after_gate_generation_change);
	UT_RUN(test_native_committed_seventy_two_owners_survive_full_background_region);
	UT_RUN(test_native_continuation_refuses_uncommitted_or_foreign_owner);
	UT_RUN(test_native_continuation_retains_stale_commit_but_cannot_serve_it);
	UT_RUN(test_native_continuation_allocation_failure_precedes_space_and_cleans_owner);
	UT_RUN(test_native_autovacuum_truncate_keeps_its_committed_continuation);
	UT_RUN(test_drop_work_preserves_same_storage_state_until_exact_finish);
	UT_RUN(test_drop_work_rejects_owner_and_full_cut_drift_without_losing_progress);
	UT_RUN(test_drop_work_exit_and_slot_reuse_never_adopt_unknown_physical_progress);
	UT_RUN(test_drop_work_bounded_scan_continues_after_a_retained_storage_failure);
	UT_RUN(test_drop_abandon_preserves_debt_and_permanently_refuses_execution);
	UT_RUN(test_drop_abandon_authenticates_original_pointer_actor_and_size);
	UT_RUN(test_drop_abandon_scan_keeps_transient_wait_retryable);
	UT_RUN(test_drop_abandon_scan_requires_original_executor_of_replaced_context);
	UT_RUN(test_drop_abandon_requires_recovery_at_normal_stop_even_with_pending_work);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
