/* Actual cut codec/mailbox/CONTROL handlers. Native sampling, membership and
 * transport are explicit fixtures; native producer tests remain independent.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>
#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_cut.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
BackendType MyBackendType = B_BG_WRITER;
int cluster_node_id, MyProcPid = 100;
bool cluster_enabled = true, cluster_shared_config = true;
ResourceOwner CurrentResourceOwner = (void *)1;
MemoryContext TopMemoryContext = (void *)1;
MemoryContext CurrentMemoryContext = (void *)1;
static struct { void *pointer; MemoryContext context; } cut_allocations[256];
static uint64 epoch = 17, incarnations[2] = { 31, 41 }, random_counter;
static bool quorum = true, pending, native_ready = true;
static bool flush_ready = true, stop_requested, flush_error;
static bool continuation_window, retained_read, send_error;
static ClusterICSendResult send_result=CLUSTER_IC_SEND_DONE;
static unsigned force_flushes;
PGPROC *MyProc;
PROC_HDR *ProcGlobal;
static PGPROC test_procs[2];
static PROC_HDR test_proc_header;
static unsigned latch_sets[2];
volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static TimestampTz now_us = 1000000;
static ClusterWalSourceRef sources[2];
static XLogRecPtr reserved = 0x1200, flushed = 0x1100;
static unsigned samples, confirms, sends, wakeups;
static uint8 sent[CLUSTER_WAL_CUT_BYTES];
static int32 destination;
static ClusterICMsgTypeInfo registration;
static const ClusterShmemRegion *region;
static char shared_bytes[2][2048] pg_attribute_aligned(MAXIMUM_ALIGNOF);
static bool shared_found[2];
static ResourceReleaseCallback release_callback;
static pg_on_exit_callback exit_callback;
static ClusterNormalStartState test_normal_start_state = CLUSTER_NORMAL_START_TARGET_READY;
static bool test_recovered_open = true;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
SetLatch(Latch *latch)
{
	wakeups++;
	for (unsigned i = 0; i < lengthof(test_procs); i++)
		if (latch == &test_procs[i].procLatch) {
			latch_sets[i]++;
			latch->is_set = true;
		}
}

bool
cluster_normal_stop_requested(void)
{
	return stop_requested;
}
bool
cluster_normal_stop_continuation_allowed(void)
{
 return !stop_requested || continuation_window;
}
bool
cluster_pi_writeback_retained_read_current_v1(const ClusterWalSourceRef *source)
{
 return source!=NULL && retained_read && cluster_normal_stop_continuation_allowed();
}

void
ExceptionalCondition(const char *a, const char *b, int c)
{
	abort();
}
void *
palloc(Size n)
{
	void *p = malloc(n);
	UT_ASSERT(p != NULL);
	return p;
}
void *
palloc0(Size n)
{
	return memset(palloc(n), 0, n);
}
void
pfree(void *p)
{
	for (unsigned i = 0; i < lengthof(cut_allocations); ++i)
		if (cut_allocations[i].pointer == p)
			cut_allocations[i].pointer = NULL;
	free(p);
}
void *
MemoryContextAllocZero(MemoryContext context, Size bytes)
{
	void *pointer = palloc0(bytes);
	for (unsigned i = 0; i < lengthof(cut_allocations); ++i)
		if (cut_allocations[i].pointer == NULL) {
			cut_allocations[i].pointer = pointer;
			cut_allocations[i].context = context;
			return pointer;
		}
	abort();
}
MemoryContext
GetMemoryChunkContext(void *pointer)
{
	for (unsigned i = 0; i < lengthof(cut_allocations); ++i)
		if (cut_allocations[i].pointer == pointer)
			return cut_allocations[i].context;
	abort();
}
uint64
GetSystemIdentifier(void)
{
	return 27;
}
uint64
cluster_epoch_get_current(void)
{
	return epoch;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return incarnations[cluster_node_id];
}
bool
cluster_qvotec_in_quorum(void)
{
	return quorum;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return pending;
}

ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return test_normal_start_state;
}

bool
cluster_semantic_activation_recovered_open_current(void)
{
	return test_recovered_open;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return node >= 0 && node < 2 ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_ABSENT;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return node >= 0 && node < 2 ? incarnations[node] : 0;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return now_us;
}
bool
pg_strong_random(void *p, size_t n)
{
	UT_ASSERT_EQ(n, sizeof(uint64));
	++random_counter;
	memcpy(p, &random_counter, n);
	return true;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *r)
{
	region = r;
}
void *
ShmemInitStruct(const char *name, Size n, bool *found)
{
	UT_ASSERT(n <= sizeof(shared_bytes[0]));
	*found = shared_found[cluster_node_id];
	shared_found[cluster_node_id] = true;
	return shared_bytes[cluster_node_id];
}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback c, void *arg)
{
	release_callback = c;
}
void
before_shmem_exit(pg_on_exit_callback c, Datum arg)
{
	exit_callback = c;
}
void
cluster_lmon_wakeup(void)
{
	wakeups++;
}
void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *i)
{
	registration = *i;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 kind, int32 dest, const void *p, uint32 n)
{
	UT_ASSERT_EQ(kind, PGRAC_IC_MSG_WAL_CUT);
	UT_ASSERT_EQ(n, sizeof(sent));
	UT_ASSERT_EQ(MyBackendType, B_LMON);
	memcpy(sent, p, n);
	destination = dest;
	sends++;
	if(send_error) siglongjmp(*PG_exception_stack,1);
	return send_result;
}
ClusterControlRootResult
cluster_wal_writer_sample_v1(ClusterWalWriterSampleV1 *out)
{
	UT_ASSERT_EQ(MyBackendType, B_LMON);
	memset(out, 0, sizeof(*out));
	if (!native_ready)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	out->writer.ref = sources[cluster_node_id];
	out->writer.epoch = epoch;
	out->reserved_end = reserved;
	samples++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_wal_writer_confirm_v1(const ClusterWalWriterSampleV1 *sample, ClusterWalWriterFlushV1 *out)
{
	UT_ASSERT(MyBackendType == B_LMON || MyBackendType == B_BG_WRITER);
	memset(out, 0, sizeof(*out));
	confirms++;
	if (!native_ready || sample->writer.epoch != epoch
		|| memcmp(&sample->writer.ref, &sources[cluster_node_id], sizeof(ClusterWalSourceRef)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (sample->reserved_end > reserved)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (flushed < sample->reserved_end)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	out->writer = sample->writer;
	out->complete_end = sample->reserved_end;
	out->flushed_end = flushed;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_writer_flush_sample_v1(const ClusterWalWriterSampleV1 *sample,
								   ClusterWalWriterFlushV1 *out)
{
	ClusterControlRootResult result;
	UT_ASSERT_EQ(MyBackendType, B_BG_WRITER);
	result = cluster_wal_writer_confirm_v1(sample, out);
	if (result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT || !flush_ready)
		return result;
	force_flushes++;
	if (flush_error)
		pg_re_throw();
	flushed = sample->reserved_end;
	return cluster_wal_writer_confirm_v1(sample, out);
}

#define getpid() MyProcPid
#include "../../backend/cluster/cluster_wal_cut.c"
#undef getpid

static void
select_node(int node)
{
	cluster_node_id = node;
	cut_shared = (WalCutShared *)shared_bytes[node];
	MyProc = &test_procs[node];
	MyProcPid = 100 + node;
}

static void
reset(void)
{
	cluster_node_id = 0;
	MyBackendType = B_BG_WRITER;
	CurrentResourceOwner = (void *)1;
	epoch = 17;
	incarnations[0] = 31;
	incarnations[1] = 41;
	quorum = native_ready = true;
	flush_ready = true;
	stop_requested = flush_error = false;
	test_normal_start_state = CLUSTER_NORMAL_START_TARGET_READY;
	test_recovered_open = true;
 continuation_window=retained_read=send_error=false;
 send_result=CLUSTER_IC_SEND_DONE;
	force_flushes = 0;
	CritSectionCount = 0;
	memset(shared_found, 0, sizeof(shared_found));
	pending = false;
	reserved = 0x1200;
	flushed = 0x1100;
	samples = confirms = sends = wakeups = 0;
	memset(test_procs, 0, sizeof(test_procs));
	memset(latch_sets, 0, sizeof(latch_sets));
	memset(&test_proc_header, 0, sizeof(test_proc_header));
	test_proc_header.allProcs = test_procs;
	test_proc_header.allProcCount = lengthof(test_procs);
	ProcGlobal = &test_proc_header;
	for (unsigned i = 0; i < lengthof(test_procs); i++) {
		test_procs[i].pgprocno = i;
		test_procs[i].pid = 100 + i;
	}
	select_node(0);
	now_us += 1000000;
	for (int i = 0; i < 2; i++) {
		ClusterControlRootIdentity *id;
		memset(&sources[i], 0, sizeof(sources[i]));
		id = &sources[i].claim.identity;
		id->system_identifier = 27;
		id->storage_uuid[0] = 2;
		id->authority_uuid[0] = 4;
		id->origin_thread_id = i + 1;
		id->origin_node_id = i;
		id->thread_claim_created_at = 100;
		id->origin_owner_incarnation = incarnations[i];
		id->root_lineage_seq = 1;
		sources[i].claim.database_incarnation = 3;
		sources[i].claim.max_config_generation = 4;
		sources[i].claim.claim_sha256[0] = i + 1;
		sources[i].timeline = 1;
	}
	cluster_wal_cut_shmem_register_v1();
	cluster_node_id = 1;
	region->init_fn();
	cluster_node_id = 0;
	region->init_fn();
	cluster_wal_cut_register_v1();
	UT_ASSERT_EQ(registration.plane, CLUSTER_IC_PLANE_CONTROL);
	UT_ASSERT_EQ(registration.allowed_producer_mask, CLUSTER_IC_PRODUCER_LMON);
}

static void
deliver(int source, int dest, const uint8 *wire)
{
	ClusterICEnvelope env = { 0 };
	select_node(dest);
	MyBackendType = B_LMON;
	env.msg_type = PGRAC_IC_MSG_WAL_CUT;
	env.source_node_id = source;
	env.dest_node_id = dest;
	env.epoch = epoch;
	env.payload_length = CLUSTER_WAL_CUT_BYTES;
	cluster_wal_cut_ingress_v1(&env, wire);
}

static void
roundtrip(void)
{
	uint8 wire[CLUSTER_WAL_CUT_BYTES];
	unsigned before = sends;
	select_node(0);
	MyBackendType = B_LMON;
	now_us += 100001;
	cluster_wal_cut_lmon_tick_v1();
	UT_ASSERT_EQ(sends, before + 1);
	UT_ASSERT_EQ(destination, 1);
	memcpy(wire, sent, sizeof(wire));
	deliver(0, 1, wire);
	UT_ASSERT_EQ(sends, before + 1); /* CONTROL cannot flush or reply early. */
	MyBackendType = B_BG_WRITER;
	(void)cluster_wal_cut_bgwriter_tick_v1();
	MyBackendType = B_LMON;
	cluster_wal_cut_lmon_tick_v1();
	if (sends == before + 2) {
		UT_ASSERT_EQ(destination, 0);
		memcpy(wire, sent, sizeof(wire));
		deliver(1, 0, wire);
	}
	select_node(0);
	MyBackendType = B_BG_WRITER;
}

UT_TEST(fixed_remote_cut_actively_flushes_idle_original_writer)
{
	ClusterWalCutV1 *cut = NULL;
	ClusterWalWriterFlushV1 out;
	static const ClusterWalWriterFlushV1 zero;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	flush_ready = false;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	reserved += 0x1000;
	roundtrip();
	UT_ASSERT_EQ(samples, 1);
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	flush_ready = true;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, 0x1200);
	UT_ASSERT_EQ(out.flushed_end, 0x1200);
	UT_ASSERT_EQ(force_flushes, 1);
	flushed = reserved;
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, 0x1200);
	UT_ASSERT_EQ(samples, 1);
	cluster_wal_cut_release_v1(&cut);
	UT_ASSERT(cut == NULL);
}

UT_TEST(recovered_remote_cut_requires_semantic_open)
{
	ClusterWalCutV1 *cut = NULL;

	reset();
	test_normal_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	test_recovered_open = false;
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut),
					 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(cut == NULL);

	test_recovered_open = true;
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_cut_release_v1(&cut);
}

UT_TEST(old_reply_and_other_owner_cannot_complete_new_job)
{
	ClusterWalCutV1 *first = NULL, *second = NULL;
	ClusterWalWriterFlushV1 out;
	uint8 old[CLUSTER_WAL_CUT_BYTES];
	reset();
	flushed = reserved;
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &first), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	roundtrip();
	memcpy(old, sent, sizeof(old));
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(first, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &second), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	deliver(1, 0, old);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(second, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	CurrentResourceOwner = (void *)2;
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT_NE(cluster_wal_cut_poll_v1(second, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	CurrentResourceOwner = (void *)1;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(second, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_cut_release_v1(&first);
	cluster_wal_cut_release_v1(&second);
}

UT_TEST(cancellation_releases_mailbox_without_inventing_completion)
{
	ClusterWalCutV1 *first = NULL, *other = NULL;
	ClusterWalWriterFlushV1 out;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &first), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &other), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(other == NULL);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT_NE(cluster_wal_cut_poll_v1(first, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &other), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	exit_callback(0, (Datum)0);
	UT_ASSERT_NE(cluster_wal_cut_poll_v1(other, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_cut_release_v1(&first);
	cluster_wal_cut_release_v1(&other);
}

UT_TEST(completed_cut_refuses_new_epoch_boot_or_quorum)
{
	for (int change = 0; change < 4; change++) {
		ClusterWalCutV1 *cut = NULL;
		ClusterWalWriterFlushV1 out;
		static const ClusterWalWriterFlushV1 zero;
		reset();
		flushed = reserved;
		UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		roundtrip();
		UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		if (change == 0)
			epoch++;
		if (change == 1)
			incarnations[1]++;
		if (change == 2)
			incarnations[0]++;
		if (change == 3)
			quorum = false;
		UT_ASSERT_NE(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		cluster_wal_cut_release_v1(&cut);
	}
}

UT_TEST(codec_preserves_full_identity_and_rejects_unconfirmed_success)
{
	ClusterWalCutMessageV1 m = { 0 }, decoded, zero = { 0 };
	uint8 wire[CLUSTER_WAL_CUT_BYTES], bad[CLUSTER_WAL_CUT_BYTES];
	static const int reserved_offsets[] = { 82, 83, 100, 101, 102, 103, 172, 173, 174, 175 };
	reset();
	m.verb = CLUSTER_WAL_CUT_SAMPLE;
	m.collector = 0;
	m.nonce = 1;
	m.collector_incarnation = incarnations[0];
	m.epoch = epoch;
	m.source = sources[1];
	for (uint32 verb = 1; verb <= 4; verb++) {
		m.verb = verb;
		m.reserved_end = verb == 1 ? 0 : reserved;
		m.native_config_generation = verb == 1 ? 0 : 3;
		m.flushed_end = verb == 4 ? reserved : 0;
		UT_ASSERT(cluster_wal_cut_encode_v1(&m, wire));
		UT_ASSERT(cluster_wal_cut_decode_v1(wire, sizeof(wire), &decoded));
		UT_ASSERT_EQ(memcmp(&m, &decoded, sizeof(m)), 0);
	}
	for (unsigned i = 0; i < lengthof(reserved_offsets); i++) {
		memcpy(bad, wire, sizeof(bad));
		bad[reserved_offsets[i]] = 1;
		UT_ASSERT(!cluster_wal_cut_decode_v1(bad, sizeof(bad), &decoded));
		UT_ASSERT_EQ(memcmp(&decoded, &zero, sizeof(zero)), 0);
	}
	m.flushed_end = m.reserved_end - 1;
	UT_ASSERT(!cluster_wal_cut_encode_v1(&m, wire));
	m.flushed_end = m.reserved_end;
	m.native_config_generation = 5;
	UT_ASSERT(!cluster_wal_cut_encode_v1(&m, wire));
	m.native_config_generation = 4;
	m.source.claim.identity.origin_owner_incarnation = 0;
	UT_ASSERT(!cluster_wal_cut_encode_v1(&m, wire));
	UT_ASSERT(!cluster_wal_cut_decode_v1(wire, sizeof(wire) - 1, &decoded));
}

UT_TEST(reordered_samples_and_wrong_sender_do_not_replace_fixed_minimum)
{
	ClusterWalCutV1 *cut = NULL;
	ClusterWalWriterFlushV1 out;
	ClusterWalCutMessageV1 sampled;
	uint8 old[CLUSTER_WAL_CUT_BYTES];
	reset();
	flush_ready = false;
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	roundtrip();
	UT_ASSERT(cluster_wal_cut_decode_v1(sent, sizeof(sent), &sampled));
	sampled.verb = CLUSTER_WAL_CUT_SAMPLED;
	sampled.native_config_generation = 4;
	sampled.reserved_end = 0x1200;
	sampled.flushed_end = 0;
	UT_ASSERT(cluster_wal_cut_encode_v1(&sampled, old));
	deliver(1, 0, old);
	/* A late initial reply may have sampled a later end. The first proposal
	 * already fixed this job's end; only its matching confirmation can finish. */
	sampled.reserved_end += 0x1000;
	sampled.flushed_end = sampled.reserved_end;
	UT_ASSERT(cluster_wal_cut_encode_v1(&sampled, old));
	deliver(1, 0, old);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	sampled.reserved_end = 0x1200;
	sampled.flushed_end = 0x1200;
	sampled.verb = CLUSTER_WAL_CUT_CONFIRMED;
	UT_ASSERT(cluster_wal_cut_encode_v1(&sampled, old));
	deliver(0, 0, old);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	flushed = reserved;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, 0x1200);
	cluster_wal_cut_release_v1(&cut);
}

UT_TEST(native_writer_refusal_and_later_selected_ceiling)
{
	ClusterWalCutV1 *cut = NULL;
	ClusterWalSourceRef selected;
	ClusterWalWriterFlushV1 out;
	reset();
	selected = sources[1];
	selected.claim.max_config_generation++;
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&selected, &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	native_ready = false;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	native_ready = true;
	flushed = reserved;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.writer.ref.claim.max_config_generation, 4);
	UT_ASSERT_EQ(memcmp(&out.writer.ref, &sources[1], sizeof(sources[1])), 0);
	cluster_wal_cut_release_v1(&cut);
}

UT_TEST(source_error_and_stop_preserve_pending_without_success)
{
	ClusterWalCutV1 *cut = NULL;
	ClusterWalWriterFlushV1 out;
	uint8 wire[CLUSTER_WAL_CUT_BYTES];
	volatile bool failed = false;
	const char *reason;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	MyBackendType = B_LMON;
	cluster_wal_cut_lmon_tick_v1();
	memcpy(wire, sent, sizeof(wire));
	deliver(0, 1, wire);
	UT_ASSERT_EQ(cluster_wal_cut_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_PENDING);
	MyBackendType = B_BG_WRITER;
	flush_error = true;
	PG_TRY();
	{
		(void)cluster_wal_cut_bgwriter_tick_v1();
	}
	PG_CATCH();
	{
		failed = true;
	}
	PG_END_TRY();
	UT_ASSERT(failed);
	select_node(0);
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	flush_error = false;
	stop_requested = true;
	MyBackendType = B_LMON;
	cluster_wal_cut_lmon_tick_v1();
	UT_ASSERT_EQ(cluster_wal_cut_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_READY);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_NE(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_cut_release_v1(&cut);
}

static void
deliver_reply(ClusterWalCutMessageV1 *reply)
{
	uint8 wire[CLUSTER_WAL_CUT_BYTES];
	UT_ASSERT(cluster_wal_cut_encode_v1(reply, wire));
	deliver(1, 0, wire);
	MyBackendType = B_BG_WRITER;
}

UT_TEST(completed_sample_and_confirmation_wake_exact_collector_once)
{
	for (int confirm = 0; confirm < 2; confirm++) {
		ClusterWalCutV1 *cut = NULL;
		ClusterWalWriterFlushV1 out;
		ClusterWalCutMessageV1 reply;
		reset();
		UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		reply = cut->requested;
		reply.verb = CLUSTER_WAL_CUT_SAMPLED;
		reply.native_config_generation = 4;
		reply.reserved_end = reserved;
		if (confirm) {
			deliver_reply(&reply);
			UT_ASSERT_EQ(latch_sets[0], 0);
			UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
			reply.verb = CLUSTER_WAL_CUT_CONFIRMED;
		}
		reply.flushed_end = reserved;
		deliver_reply(&reply);
		UT_ASSERT_EQ(latch_sets[0], 1);
		UT_ASSERT(MyProc->procLatch.is_set);
		UT_ASSERT_EQ(latch_sets[1], 0);
		deliver_reply(&reply);
		UT_ASSERT_EQ(latch_sets[0], 1);
		UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(out.complete_end, reserved);
		cluster_wal_cut_release_v1(&cut);
		deliver_reply(&reply);
		UT_ASSERT_EQ(latch_sets[0], 1);
	}
}

UT_TEST(refused_or_cancelled_reply_cannot_wake_replacement_collector)
{
	for (int change = 0; change < 12; change++) {
		ClusterWalCutV1 *cut = NULL, *replacement = NULL;
		ClusterWalWriterFlushV1 out;
		ClusterWalCutMessageV1 reply;
		reset();
		UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		reply = cut->requested;
		reply.verb = CLUSTER_WAL_CUT_SAMPLED;
		reply.native_config_generation = 4;
		reply.reserved_end = reply.flushed_end = reserved;
		switch (change) {
		case 0:
			reply.nonce++;
			break;
		case 1:
			reply.source.timeline++;
			break;
		case 2:
			reply.source.claim.identity.origin_owner_incarnation++;
			break;
		case 3:
			epoch++;
			break;
		case 4:
			quorum = false;
			break;
		case 5:
			stop_requested = true;
			break;
		case 6:
			test_procs[0].pid++;
			break;
		case 7:
			release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
			break;
		case 8:
			exit_callback(0, (Datum)0);
			break;
		case 9:
			ProcGlobal = NULL;
			break;
		case 10:
			test_proc_header.allProcs = NULL;
			break;
		case 11:
			test_procs[0].pgprocno = test_proc_header.allProcCount;
			break;
		}
		if (change == 7 || change == 8)
			UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &replacement),
						 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		deliver_reply(&reply);
		UT_ASSERT_EQ(latch_sets[0], 0);
		UT_ASSERT_EQ(latch_sets[1], 0);
		if (change == 7 || change == 8)
			UT_ASSERT_EQ(cluster_wal_cut_poll_v1(replacement, &out),
						 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		else if (change < 6)
			UT_ASSERT_NE(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		cluster_wal_cut_release_v1(&cut);
		cluster_wal_cut_release_v1(&replacement);
	}
}

UT_TEST(completed_cut_survives_internal_latch_reset_for_one_poll_retry)
{
	ClusterWalCutV1 *cut = NULL;
	ClusterWalWriterFlushV1 out;
	reset();
	UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
	roundtrip();
	UT_ASSERT(MyProc->procLatch.is_set);
	/* Model the intervening WALR confirmed-release wait consuming its latch.
	 * The actual mailbox consumer must preserve one bounded retry afterward. */
	MyProc->procLatch.is_set = false;
	UT_ASSERT(cluster_wal_cut_consume_completion_v1());
	for (unsigned i = 0; i < 3; i++)
		UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
	/* The hint neither consumes nor replaces the original full poll checks. */
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.flushed_end, reserved);
	UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
	cluster_wal_cut_release_v1(&cut);
}

UT_TEST(completion_retry_respects_owner_gates_and_cancellation)
{
	for (int change = 0; change < 9; change++) {
		ClusterWalCutV1 *cut = NULL, *replacement = NULL;
		ClusterWalWriterFlushV1 out;
		reset();
		UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		roundtrip();
		MyProc->procLatch.is_set = false;
		switch (change) {
		case 0:
			quorum = false;
			break;
		case 1:
			stop_requested = true;
			break;
		case 2:
			epoch++;
			break;
		case 3:
			incarnations[1]++;
			break;
		case 4:
			pending = true;
			break;
		case 5:
			CurrentResourceOwner = (void *)2;
			break;
		case 6:
			MyBackendType = B_LMON;
			break;
		case 7:
			release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
			break;
		case 8:
			exit_callback(0, (Datum)0);
			break;
		}
		if (change >= 7)
			UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &replacement),
						 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
		UT_ASSERT(!MyProc->procLatch.is_set);
		if (change < 5) {
			/* Restoring eligibility cannot revive an already consumed old hint. */
			quorum = true;
			stop_requested = pending = false;
			epoch = 17;
			incarnations[1] = 41;
			UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
		}
		if (change >= 7)
			UT_ASSERT_EQ(cluster_wal_cut_poll_v1(replacement, &out),
						 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		CurrentResourceOwner = (void *)1;
		MyBackendType = B_BG_WRITER;
		cluster_wal_cut_release_v1(&cut);
		cluster_wal_cut_release_v1(&replacement);
		UT_ASSERT(!cluster_wal_cut_consume_completion_v1());
	}
}

UT_TEST(stop_accepts_only_derived_old_wb_read_and_finishes_original_cut)
{
 ClusterWalCutV1 *cut=NULL;
 ClusterWalWriterFlushV1 out;
 reset();stop_requested=true;continuation_window=true;
 UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1],&cut),CLUSTER_CONTROL_ROOT_STALE_TOKEN);
 UT_ASSERT(cut==NULL);
 retained_read=true;
 UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1],&cut),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
 if(cut==NULL) return;
 for(unsigned i=0;i<4 && cluster_wal_cut_poll_v1(cut,&out)!=CLUSTER_CONTROL_ROOT_OK_PRIMARY;i++) roundtrip();
 UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut,&out),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
 UT_ASSERT_EQ(out.complete_end,0x1200);
 cluster_wal_cut_release_v1(&cut);
 continuation_window=false;
 UT_ASSERT_NE(cluster_wal_cut_begin_v1(&sources[1],&cut),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
 UT_ASSERT(cut==NULL);
}
UT_TEST(stop_source_reply_is_owned_until_transport_accepts)
{
 ClusterWalCutV1 *cut=NULL;
 uint8 wire[CLUSTER_WAL_CUT_BYTES];
 const char *reason;
 volatile bool caught=false;
 reset();
 UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1],&cut),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
 MyBackendType=B_LMON;cluster_wal_cut_lmon_tick_v1();
 memcpy(wire,sent,sizeof(wire));deliver(0,1,wire);
 stop_requested=true;continuation_window=true;
 MyBackendType=B_BG_WRITER;
 UT_ASSERT(cluster_wal_cut_bgwriter_tick_v1());
 UT_ASSERT(cut_shared->source_reply_pending);
 MyBackendType=B_LMON;send_error=true;
 PG_TRY(); { cluster_wal_cut_lmon_tick_v1(); }
 PG_CATCH(); { caught=true; } PG_END_TRY();
 UT_ASSERT(caught);
 UT_ASSERT(cut_shared->source_reply_pending);
 UT_ASSERT_EQ(cluster_wal_cut_normal_stop_poll_v1(&reason),CLUSTER_NORMAL_STOP_PENDING);
 send_error=false;send_result=CLUSTER_IC_SEND_HARD_ERROR;
 cluster_wal_cut_lmon_tick_v1();UT_ASSERT(cut_shared->source_reply_pending);
 send_result=CLUSTER_IC_SEND_DONE;cluster_wal_cut_lmon_tick_v1();
 UT_ASSERT(!cut_shared->source_reply_pending);
 UT_ASSERT_EQ(cluster_wal_cut_normal_stop_poll_v1(&reason),CLUSTER_NORMAL_STOP_READY);
 select_node(0);MyBackendType=B_BG_WRITER;cluster_wal_cut_release_v1(&cut);
}
UT_TEST(stop_unknown_source_state_is_never_empty)
{
 const char *reason;
 reset();stop_requested=true;continuation_window=true;
 cut_shared->source_state=(WalCutSourceState)(WAL_CUT_SOURCE_REPLIED+1);
 MyBackendType=B_LMON;cluster_wal_cut_lmon_tick_v1();
 UT_ASSERT_EQ(cluster_wal_cut_normal_stop_poll_v1(&reason),CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(stable_descriptor_uses_requested_context_and_exact_original_binding)
{
	ClusterWalCutV1 *cut = NULL;
	MemoryContext context = (MemoryContext)2;
	ClusterWalSourceRef wrong;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_in_context_v1(&sources[1], context, &cut),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_wal_cut_context_binding_v1(cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT(!cluster_wal_cut_context_binding_v1(cut, (MemoryContext)3, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT(!cluster_wal_cut_context_binding_v1(cut, context, (ResourceOwner)2, MyProcPid, &sources[1]));
	UT_ASSERT(!cluster_wal_cut_context_binding_v1(cut, context, CurrentResourceOwner, MyProcPid + 1, &sources[1]));
	wrong = sources[1];
	++wrong.claim.identity.root_lineage_seq;
	UT_ASSERT(!cluster_wal_cut_context_binding_v1(cut, context, CurrentResourceOwner, MyProcPid, &wrong));
	UT_ASSERT(cluster_wal_cut_release_in_context_v1(&cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT(cut == NULL && active_cut == NULL && cut_shared->owner_pid == 0);
}

UT_TEST(stable_cancel_keeps_allocation_and_clears_original_completion_hint)
{
	ClusterWalCutV1 *cut = NULL;
	MemoryContext context = (MemoryContext)2;
	ClusterWalWriterFlushV1 sample;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_in_context_v1(&sources[1], context, &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cut_shared->completion_pending = true;
	UT_ASSERT(cluster_wal_cut_cancel_in_context_v1(cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT(cut->stale && active_cut == NULL);
	UT_ASSERT(!cut_shared->completion_pending && cut_shared->owner_pid == 0);
	UT_ASSERT(cluster_wal_cut_context_binding_v1(cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &sample), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(cluster_wal_cut_release_in_context_v1(&cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
}

UT_TEST(stable_owner_callback_cancels_without_dropping_descriptor)
{
	ClusterWalCutV1 *cut = NULL;
	MemoryContext context = (MemoryContext)2;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_in_context_v1(&sources[1], context, &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(cut->stale && cut_shared->owner_pid == 0 && active_cut == NULL);
	UT_ASSERT(cluster_wal_cut_context_binding_v1(cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT(cluster_wal_cut_release_in_context_v1(&cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
}

UT_TEST(stable_foreign_cleanup_does_not_cancel_or_free_original_request)
{
	ClusterWalCutV1 *cut = NULL;
	MemoryContext context = (MemoryContext)2;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_in_context_v1(&sources[1], context, &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(!cluster_wal_cut_cancel_in_context_v1(cut, (MemoryContext)3, CurrentResourceOwner, MyProcPid, &sources[1]));
	UT_ASSERT(!cluster_wal_cut_release_in_context_v1(&cut, context, (ResourceOwner)2, MyProcPid, &sources[1]));
	UT_ASSERT(cut == active_cut && !cut->stale && cut_shared->owner_pid == MyProcPid);
	UT_ASSERT(cluster_wal_cut_release_in_context_v1(&cut, context, CurrentResourceOwner, MyProcPid, &sources[1]));
}

int
main(void)
{
 UT_RUN(stop_accepts_only_derived_old_wb_read_and_finishes_original_cut);
 UT_RUN(stop_source_reply_is_owned_until_transport_accepts);
 UT_RUN(stop_unknown_source_state_is_never_empty);
	UT_PLAN(20);
	UT_RUN(stable_descriptor_uses_requested_context_and_exact_original_binding);
	UT_RUN(stable_cancel_keeps_allocation_and_clears_original_completion_hint);
	UT_RUN(stable_owner_callback_cancels_without_dropping_descriptor);
	UT_RUN(stable_foreign_cleanup_does_not_cancel_or_free_original_request);
	UT_RUN(fixed_remote_cut_actively_flushes_idle_original_writer);
	UT_RUN(recovered_remote_cut_requires_semantic_open);
	UT_RUN(old_reply_and_other_owner_cannot_complete_new_job);
	UT_RUN(cancellation_releases_mailbox_without_inventing_completion);
	UT_RUN(completed_cut_refuses_new_epoch_boot_or_quorum);
	UT_RUN(codec_preserves_full_identity_and_rejects_unconfirmed_success);
	UT_RUN(reordered_samples_and_wrong_sender_do_not_replace_fixed_minimum);
	UT_RUN(native_writer_refusal_and_later_selected_ceiling);
	UT_RUN(source_error_and_stop_preserve_pending_without_success);
	UT_RUN(completed_sample_and_confirmation_wake_exact_collector_once);
	UT_RUN(refused_or_cancelled_reply_cannot_wake_replacement_collector);
	UT_RUN(completed_cut_survives_internal_latch_reset_for_one_poll_retry);
	UT_RUN(completion_retry_respects_owner_gates_and_cancellation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
