/* Actual DATA CONTROL codec/mailboxes. Transport/native/physical endpoints
 * are explicit fixtures; bufmgr's real-file producer/import is tested apart.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>
#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_pi_data.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_writer.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
BackendType MyBackendType = B_BG_WRITER;
int cluster_node_id, MyProcPid = 100;
bool cluster_enabled = true, cluster_shared_config = true;
volatile uint32 CritSectionCount;
ResourceOwner CurrentResourceOwner = (void *)1;
PGPROC *MyProc;
PROC_HDR *ProcGlobal;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static uint64 epoch = 17, boots[2] = { 31, 41 }, serial;
static bool quorum = true, prebump, new_work = true, native_ready = true;
static bool data_ready = true, cut_ready = true, native_changes, stop_during_io;
static bool io_error;
static ClusterPiDataMessageV1 fixture;
static ClusterWalWriterToken writers[2];
static unsigned sends, writes, imports, freed;
static ClusterICSendResult send_outcome = CLUSTER_IC_SEND_DONE;
static uint8 sent[CLUSTER_PI_DATA_BYTES];
static uint32 destination;
static TimestampTz clock_us = 1000000;
static const ClusterShmemRegion *region;
static char memory[4096] pg_attribute_aligned(MAXIMUM_ALIGNOF);
static bool found;
static ResourceReleaseCallback release_cb;
static pg_on_exit_callback exit_cb;
static ClusterICMsgTypeInfo registered;
struct ClusterPageDataReceiptV1 {
	int unused;
};
static ClusterPageDataReceiptV1 physical;

void
ExceptionalCondition(const char *a, const char *b, int c)
{
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
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
	free(p);
}
bool
RecoveryInProgress(void)
{
	return false;
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
	return boots[cluster_node_id];
}
bool
cluster_qvotec_in_quorum(void)
{
	return quorum;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return prebump;
}
ClusterMembershipState
cluster_membership_get_state(int32 n)
{
	return n >= 0 && n < 2 ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_ABSENT;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 n)
{
	return n >= 0 && n < 2 ? boots[n] : 0;
}
bool
cluster_normal_stop_requested(void)
{
	return !new_work;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return clock_us;
}
bool
pg_strong_random(void *p, size_t n)
{
	UT_ASSERT_EQ(n, sizeof(serial));
	++serial;
	memcpy(p, &serial, n);
	return true;
}
void
cluster_lmon_wakeup(void)
{}
void
SetLatch(Latch *latch)
{}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback f, void *arg)
{
	release_cb = f;
}
void
before_shmem_exit(pg_on_exit_callback f, Datum arg)
{
	exit_cb = f;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *r)
{
	region = r;
}
void *
ShmemInitStruct(const char *name, Size n, bool *old)
{
	UT_ASSERT(n <= sizeof(memory));
	*old = found;
	found = true;
	return memory;
}
void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *r)
{
	registered = *r;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 to, const void *data, uint32 n)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_PI_DATA);
	UT_ASSERT_EQ(n, sizeof(sent));
	memcpy(sent, data, n);
	destination = to;
	sends++;
	return send_outcome;
}
int32
cluster_gcs_lookup_master(BufferTag tag)
{
	return fixture.cut.master_node;
}
bool
cluster_pcm_lock_pi_write_snapshot_v1(BufferTag tag, ClusterPcmPiWriteCutV1 *out)
{
	if (!cut_ready || !BufferTagsEqual(&tag, &fixture.cut.holder.assertion.resource))
		return false;
	*out = fixture.cut;
	return true;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	if (!native_ready)
		return false;
	*out = writers[cluster_node_id].ref;
	return true;
}
ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID tli, ClusterWalWriterToken *out)
{
	if (!native_ready)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	*out = writers[cluster_node_id];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_bufmgr_write_tag_data_at_cut_v1(const ClusterSpaceIdentityKey *key,
										const ClusterPcmPiWriteCutV1 *cut,
										ClusterPageDataReceiptV1 **out)
{
	UT_ASSERT_EQ(MyBackendType, B_BG_WRITER);
	UT_ASSERT_EQ(cluster_node_id, 1);
	UT_ASSERT_EQ(memcmp(key, &fixture.key, sizeof(*key)), 0);
	UT_ASSERT_EQ(memcmp(cut, &fixture.cut, sizeof(*cut)), 0);
	writes++;
	if (io_error)
		pg_re_throw();
	if (native_changes)
		writers[1].epoch++;
	if (stop_during_io) {
		const char *reason = NULL;
		new_work = false;
		UT_ASSERT_EQ(cluster_pi_data_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT(reason != NULL);
	}
	if (!data_ready)
		return false;
	*out = &physical;
	return true;
}
bool
cluster_page_data_pi_export_v1(const ClusterPageDataReceiptV1 *r, ClusterPageWalBindingV1 *binding,
							   ClusterPcmPiWriteCutV1 *cut)
{
	UT_ASSERT(r == &physical);
	*binding = fixture.binding;
	*cut = fixture.cut;
	return true;
}
void
cluster_page_data_receipt_free_v1(ClusterPageDataReceiptV1 **r)
{
	if (*r)
		freed++;
	*r = NULL;
}
bool
cluster_page_data_from_remote_v1(const ClusterPiDataV1 *job, ClusterPageDataReceiptV1 **out)
{
	ClusterPageWalBindingV1 binding;
	ClusterPcmPiWriteCutV1 cut;
	if (!cluster_pi_data_read_v1(job, &binding, &cut))
		return false;
	UT_ASSERT_EQ(memcmp(&binding, &fixture.binding, sizeof(binding)), 0);
	UT_ASSERT_EQ(memcmp(&cut, &fixture.cut, sizeof(cut)), 0);
	*out = &physical;
	imports++;
	return true;
}

#include "../../backend/cluster/cluster_pi_data.c"

static void
reset(void)
{
	cluster_node_id = 0;
	MyBackendType = B_BG_WRITER;
	CurrentResourceOwner = (void *)1;
	epoch = 17;
	boots[0] = 31;
	boots[1] = 41;
	quorum = new_work = native_ready = data_ready = cut_ready = true;
	prebump = native_changes = stop_during_io = io_error = false;
	sends = writes = imports = freed = 0;
	clock_us += 1000000;
	send_outcome = CLUSTER_IC_SEND_DONE;
	memset(&fixture, 0, sizeof(fixture));
	memset(writers, 0, sizeof(writers));
	fixture.verb = CLUSTER_PI_DATA_WRITE;
	fixture.nonce = 10;
	fixture.epoch = epoch;
	fixture.holder_incarnation = boots[1];
	fixture.key.system_identifier = 27;
	fixture.key.database_incarnation = 4;
	fixture.key.storage_uuid[0] = 9;
	fixture.key.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 9, 18000 };
	InitBufferTag(&fixture.cut.holder.assertion.resource, &fixture.key.locator, MAIN_FORKNUM, 7);
	fixture.cut.holder.assertion.requester_node = 1;
	fixture.cut.holder.base_authority_generation = 1;
	fixture.cut.holder.final_authority_generation = 2;
	fixture.cut.holder.resource_formation = 17;
	fixture.cut.holder.master_session_incarnation = boots[0];
	fixture.cut.holder.assertion_sequence = 41;
	fixture.cut.holder.requester_target_generation = 2;
	fixture.cut.holder.phase = RESOURCE_X_MASTER_SETTLED;
	fixture.cut.binding_generation = 51;
	fixture.cut.transition_count = 4;
	fixture.cut.master_generation = 71;
	fixture.cut.pi_holders_bitmap = 3;
	for (int i = 0; i < 2; i++) {
		ClusterWalSourceRef *s = &writers[i].ref;
		s->claim.identity.system_identifier = 27;
		s->claim.identity.storage_uuid[0] = 9;
		s->claim.identity.authority_uuid[0] = 8;
		s->claim.identity.origin_thread_id = i + 1;
		s->claim.identity.origin_node_id = i;
		s->claim.identity.thread_claim_created_at = 100;
		s->claim.identity.origin_owner_incarnation = boots[i];
		s->claim.identity.root_lineage_seq = 1;
		s->claim.database_incarnation = 4;
		s->claim.max_config_generation = 1;
		s->claim.claim_sha256[0] = 6;
		s->timeline = 1;
		writers[i].epoch = epoch;
	}
	fixture.binding.source = writers[1].ref;
	fixture.binding.identity.system_identifier = 27;
	fixture.binding.identity.storage_uuid[0] = 9;
	fixture.binding.identity.locator = fixture.key.locator;
	fixture.binding.identity.forknum = MAIN_FORKNUM;
	fixture.binding.identity.blockno = 7;
	fixture.binding.version.segment_incarnation[0] = 12;
	fixture.binding.version.mutation_token = 80;
	fixture.binding.record_start = 0x100;
	fixture.binding.record_end = 0x200;
	fixture.binding.record_crc = 19;
	fixture.binding.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	found = false;
	region->init_fn();
}
static ClusterPiDataV1 *
begin(void)
{
	ClusterPiDataV1 *job = NULL;
	UT_ASSERT_EQ(cluster_pi_data_begin_v1(&fixture.key, &fixture.cut, &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(job != NULL);
	return job;
}
static void
deliver(int from, int to, uint8 *bytes)
{
	ClusterICEnvelope env = { 0 };
	env.msg_type = PGRAC_IC_MSG_PI_DATA;
	env.source_node_id = from;
	env.dest_node_id = to;
	env.epoch = epoch;
	env.payload_length = CLUSTER_PI_DATA_BYTES;
	cluster_node_id = to;
	MyBackendType = B_LMON;
	cluster_pi_data_ingress_v1(&env, bytes);
}
static void
exchange(void)
{
	MyBackendType = B_LMON;
	cluster_pi_data_lmon_tick_v1();
	UT_ASSERT_EQ(destination, 1);
	deliver(0, 1, sent);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!cluster_pi_data_bgwriter_tick_v1());
	UT_ASSERT_EQ(writes, 0);
	MyBackendType = B_BG_WRITER;
	(void)cluster_pi_data_bgwriter_tick_v1();
	MyBackendType = B_LMON;
	cluster_pi_data_lmon_tick_v1();
	if (data_ready && !native_changes && !stop_during_io) {
		UT_ASSERT_EQ(destination, 0);
		deliver(1, 0, sent);
	}
	cluster_node_id = 0;
	MyBackendType = B_BG_WRITER;
}
static void
actual_job_requires_holder_completion(void)
{
	ClusterPiDataV1 *job;
	ClusterPageDataReceiptV1 *out = NULL;
	reset();
	job = begin();
	UT_ASSERT_EQ(cluster_pi_data_poll_v1(job, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(out == NULL);
	exchange();
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(freed, 1);
	UT_ASSERT_EQ(cluster_pi_data_poll_v1(job, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(out != NULL);
	UT_ASSERT_EQ(imports, 1);
	cluster_pi_data_release_v1(&job);
	UT_ASSERT(job == NULL);
}
static void
codec_rejects_unqualified_binding(void)
{
	ClusterPiDataMessageV1 m, decoded;
	uint8 bytes[CLUSTER_PI_DATA_BYTES];
	reset();
	m = fixture;
	memset(&m.binding, 0, sizeof(m.binding));
	UT_ASSERT(cluster_pi_data_encode_v1(&m, bytes));
	UT_ASSERT(cluster_pi_data_decode_v1(bytes, sizeof(bytes), &decoded));
	UT_ASSERT_EQ(memcmp(&m, &decoded, sizeof(m)), 0);
	m = fixture;
	m.verb = CLUSTER_PI_DATA_WRITTEN;
	UT_ASSERT(cluster_pi_data_encode_v1(&m, bytes));
	UT_ASSERT(cluster_pi_data_decode_v1(bytes, sizeof(bytes), &decoded));
	UT_ASSERT_EQ(memcmp(&m, &decoded, sizeof(m)), 0);
	for (int c = 0; c < 7; c++) {
		m = fixture;
		m.verb = CLUSTER_PI_DATA_WRITTEN;
		switch (c) {
		case 0:
			m.binding.flags = 0;
			break;
		case 1:
			m.binding.identity.blockno++;
			break;
		case 2:
			m.binding.source.claim.database_incarnation++;
			break;
		case 3:
			m.binding.source.claim.identity.origin_owner_incarnation = 0;
			break;
		case 4:
			m.key.storage_uuid[1]++;
			break;
		case 5:
			m.holder_incarnation = 0;
			break;
		case 6:
			m.cut.holder.phase = RESOURCE_X_MASTER_WAIT_PROOF;
			break;
		}
		UT_ASSERT(!cluster_pi_data_encode_v1(&m, bytes));
	}
	UT_ASSERT(!cluster_pi_data_decode_v1(bytes, sizeof(bytes) - 1, &decoded));
}
static void
stale_reply_and_cut_never_import(void)
{
	for (int c = 0; c < 4; c++) {
		ClusterPiDataV1 *job;
		ClusterPageDataReceiptV1 *out = NULL;
		reset();
		job = begin();
		exchange();
		if (c == 0)
			epoch++;
		if (c == 1)
			boots[1]++;
		if (c == 2)
			cut_ready = false;
		if (c == 3)
			CurrentResourceOwner = (void *)2;
		UT_ASSERT(cluster_pi_data_poll_v1(job, &out) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(out == NULL);
		UT_ASSERT_EQ(imports, 0);
		CurrentResourceOwner = (void *)1;
		cluster_pi_data_release_v1(&job);
	}
}
static void
busy_changed_native_and_stop_keep_obligations(void)
{
	for (int c = 0; c < 3; c++) {
		ClusterPiDataV1 *job;
		ClusterPageDataReceiptV1 *out = NULL;
		reset();
		job = begin();
		if (c == 0)
			data_ready = false;
		if (c == 1)
			native_changes = true;
		if (c == 2)
			stop_during_io = true;
		exchange();
		UT_ASSERT(cluster_pi_data_poll_v1(job, &out) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(out == NULL);
		UT_ASSERT_EQ(imports, 0);
		cluster_pi_data_release_v1(&job);
	}
}
static void
original_owner_cleanup_releases_capacity(void)
{
	ClusterPiDataV1 *a, *b = NULL;
	const char *reason = NULL;
	reset();
	a = begin();
	UT_ASSERT_EQ(cluster_pi_data_begin_v1(&fixture.key, &fixture.cut, &b),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(b == NULL);
	UT_ASSERT_EQ(cluster_pi_data_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_PENDING);
	release_cb(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, NULL);
	UT_ASSERT_EQ(cluster_pi_data_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_READY);
	b = begin();
	cluster_pi_data_release_v1(&a);
	UT_ASSERT_EQ(cluster_pi_data_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_PENDING);
	exit_cb(0, (Datum)0);
	cluster_pi_data_release_v1(&b);
	UT_ASSERT_EQ(cluster_pi_data_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_READY);
}
static void
wrong_sender_nonce_and_duplicate_cannot_replace_completion(void)
{
	ClusterPiDataV1 *job;
	ClusterPageDataReceiptV1 *out = NULL;
	ClusterPiDataMessageV1 request, reply;
	uint8 bytes[CLUSTER_PI_DATA_BYTES];
	reset();
	job = begin();
	MyBackendType = B_LMON;
	cluster_pi_data_lmon_tick_v1();
	UT_ASSERT(cluster_pi_data_decode_v1(sent, sizeof(sent), &request));
	for (int c = 0; c < 5; c++) {
		reply = request;
		reply.verb = CLUSTER_PI_DATA_WRITTEN;
		reply.binding = fixture.binding;
		if (c == 1)
			reply.nonce++;
		if (c == 2)
			reply.cut.transition_count++;
		if (c == 3)
			reply.holder_incarnation++;
		if (c == 4)
			reply.epoch++;
		UT_ASSERT(cluster_pi_data_encode_v1(&reply, bytes));
		deliver(c == 0 ? 0 : 1, 0, bytes);
		MyBackendType = B_BG_WRITER;
		UT_ASSERT_EQ(cluster_pi_data_poll_v1(job, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT(out == NULL);
	}
	reply = request;
	reply.verb = CLUSTER_PI_DATA_WRITTEN;
	reply.binding = fixture.binding;
	UT_ASSERT(cluster_pi_data_encode_v1(&reply, bytes));
	deliver(1, 0, bytes);
	/* Another actual completion of the same job must not move an accepted
	 * result. This is deliberately a smaller opaque token, not an LSN order. */
	reply.binding.version.mutation_token = 7;
	UT_ASSERT(cluster_pi_data_encode_v1(&reply, bytes));
	deliver(1, 0, bytes);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_EQ(cluster_pi_data_poll_v1(job, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(imports, 1);
	out = NULL;
	deliver(1, 0, bytes);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_EQ(cluster_pi_data_poll_v1(job, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(imports, 2);
	cluster_pi_data_release_v1(&job);
}
static void
server_error_and_producer_cut_release_pending_work(void)
{
	for (int c = 0; c < 2; c++) {
		ClusterPiDataV1 *job;
		const char *reason = NULL;
		volatile bool caught = false;
		reset();
		job = begin();
		MyBackendType = B_LMON;
		cluster_pi_data_lmon_tick_v1();
		deliver(0, 1, sent);
		if (c == 0) {
			io_error = true;
			MyBackendType = B_BG_WRITER;
			PG_TRY();
			{
				(void)cluster_pi_data_bgwriter_tick_v1();
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT(caught);
			UT_ASSERT_EQ(writes, 1);
		} else {
			new_work = false;
			cluster_pi_data_lmon_tick_v1();
			MyBackendType = B_BG_WRITER;
			UT_ASSERT(!cluster_pi_data_bgwriter_tick_v1());
			UT_ASSERT_EQ(writes, 0);
		}
		cluster_node_id = 0;
		cluster_pi_data_release_v1(&job);
		UT_ASSERT_EQ(cluster_pi_data_normal_stop_poll_v1(&reason), CLUSTER_NORMAL_STOP_READY);
	}
}
static void
completed_reply_sends_only_when_requested(void)
{
	ClusterPiDataV1 *job;
	ClusterPageDataReceiptV1 *out = NULL;
	uint8 request[CLUSTER_PI_DATA_BYTES];
	unsigned sent_once;
	reset();
	job = begin();
	MyBackendType = B_LMON;
	cluster_pi_data_lmon_tick_v1();
	memcpy(request, sent, sizeof(request));
	deliver(0, 1, request);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT(cluster_pi_data_bgwriter_tick_v1());
	MyBackendType = B_LMON;
	cluster_pi_data_lmon_tick_v1();
	deliver(1, 0, sent);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT_EQ(cluster_pi_data_poll_v1(job, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_pi_data_release_v1(&job);
	sent_once = sends;
	cluster_node_id = 1;
	MyBackendType = B_LMON;
	clock_us += 1000000;
	cluster_pi_data_lmon_tick_v1();
	UT_ASSERT_EQ(sends, sent_once);
	/* A lost completion is replayed from the original cache, without a new
	 * DATA write. Refused transport retains exactly this pending reply. */
	deliver(0, 1, request);
	MyBackendType = B_BG_WRITER;
	UT_ASSERT(!cluster_pi_data_bgwriter_tick_v1());
	UT_ASSERT_EQ(writes, 1);
	MyBackendType = B_LMON;
	send_outcome = CLUSTER_IC_SEND_NOT_ADMITTED;
	clock_us += 1000000;
	cluster_pi_data_lmon_tick_v1();
	UT_ASSERT_EQ(sends, sent_once + 1);
	send_outcome = CLUSTER_IC_SEND_WOULD_BLOCK;
	clock_us += 1000000;
	cluster_pi_data_lmon_tick_v1();
	UT_ASSERT_EQ(sends, sent_once + 2);
	clock_us += 1000000;
	cluster_pi_data_lmon_tick_v1();
	UT_ASSERT_EQ(sends, sent_once + 2);
	UT_ASSERT_EQ(writes, 1);
}

int
main(void)
{
	cluster_pi_data_shmem_register_v1();
	cluster_pi_data_register_v1();
	UT_PLAN(8);
	UT_RUN(completed_reply_sends_only_when_requested);
	UT_RUN(wrong_sender_nonce_and_duplicate_cannot_replace_completion);
	UT_RUN(server_error_and_producer_cut_release_pending_work);
	UT_RUN(actual_job_requires_holder_completion);
	UT_RUN(codec_rejects_unqualified_binding);
	UT_RUN(stale_reply_and_cut_never_import);
	UT_RUN(busy_changed_native_and_stop_keep_obligations);
	UT_RUN(original_owner_cleanup_releases_capacity);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
