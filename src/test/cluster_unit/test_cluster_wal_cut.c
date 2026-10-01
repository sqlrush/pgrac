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
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_cut.h"
#include "miscadmin.h"
#include "storage/ipc.h"
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
static uint64 epoch = 17, incarnations[2] = { 31, 41 }, random_counter;
static bool quorum = true, pending, native_ready = true;
static TimestampTz now_us = 1000000;
static ClusterWalSourceRef sources[2];
static XLogRecPtr reserved = 0x1200, flushed = 0x1100;
static unsigned samples, confirms, sends, wakeups;
static uint8 sent[CLUSTER_WAL_CUT_BYTES];
static int32 destination;
static ClusterICMsgTypeInfo registration;
static const ClusterShmemRegion *region;
static char shared_bytes[1024] pg_attribute_aligned(MAXIMUM_ALIGNOF);
static bool shared_found;
static ResourceReleaseCallback release_callback;
static pg_on_exit_callback exit_callback;

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
	free(p);
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
	UT_ASSERT(n <= sizeof(shared_bytes));
	*found = shared_found;
	shared_found = true;
	return shared_bytes;
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
	return CLUSTER_IC_SEND_DONE;
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
	UT_ASSERT_EQ(MyBackendType, B_LMON);
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

#include "../../backend/cluster/cluster_wal_cut.c"

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
	pending = false;
	reserved = 0x1200;
	flushed = 0x1100;
	samples = confirms = sends = wakeups = 0;
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
	region->init_fn();
	cluster_wal_cut_register_v1();
	UT_ASSERT_EQ(registration.plane, CLUSTER_IC_PLANE_CONTROL);
	UT_ASSERT_EQ(registration.allowed_producer_mask, CLUSTER_IC_PRODUCER_LMON);
}

static void
deliver(int source, int dest, const uint8 *wire)
{
	ClusterICEnvelope env = { 0 };
	cluster_node_id = dest;
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
	cluster_node_id = 0;
	MyBackendType = B_LMON;
	now_us += 100001;
	cluster_wal_cut_lmon_tick_v1();
	UT_ASSERT_EQ(sends, before + 1);
	UT_ASSERT_EQ(destination, 1);
	memcpy(wire, sent, sizeof(wire));
	deliver(0, 1, wire);
	if (sends == before + 2) {
		UT_ASSERT_EQ(destination, 0);
		memcpy(wire, sent, sizeof(wire));
		deliver(1, 0, wire);
	}
	cluster_node_id = 0;
	MyBackendType = B_BG_WRITER;
}

UT_TEST(fixed_remote_cut_waits_for_original_flush)
{
	ClusterWalCutV1 *cut = NULL;
	ClusterWalWriterFlushV1 out;
	static const ClusterWalWriterFlushV1 zero;
	reset();
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	reserved += 0x1000;
	roundtrip();
	UT_ASSERT_EQ(samples, 1);
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	flushed = 0x1200;
	roundtrip();
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, 0x1200);
	UT_ASSERT_EQ(out.flushed_end, 0x1200);
	flushed = reserved;
	UT_ASSERT_EQ(cluster_wal_cut_poll_v1(cut, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, 0x1200);
	UT_ASSERT_EQ(samples, 1);
	cluster_wal_cut_release_v1(&cut);
	UT_ASSERT(cut == NULL);
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
	UT_ASSERT_EQ(cluster_wal_cut_begin_v1(&sources[1], &cut), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	roundtrip();
	UT_ASSERT(cluster_wal_cut_decode_v1(sent, sizeof(sent), &sampled));
	UT_ASSERT_EQ(sampled.verb, CLUSTER_WAL_CUT_SAMPLED);
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

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(fixed_remote_cut_waits_for_original_flush);
	UT_RUN(old_reply_and_other_owner_cannot_complete_new_job);
	UT_RUN(cancellation_releases_mailbox_without_inventing_completion);
	UT_RUN(completed_cut_refuses_new_epoch_boot_or_quorum);
	UT_RUN(codec_preserves_full_identity_and_rejects_unconfirmed_success);
	UT_RUN(reordered_samples_and_wrong_sender_do_not_replace_fixed_minimum);
	UT_RUN(native_writer_refusal_and_later_selected_ceiling);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
