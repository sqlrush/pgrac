/*-------------------------------------------------------------------------
 *
 * test_cluster_config_channels.c
 *    Native channel adapter with explicit process and transport boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_channels.c
 * NOTES
 *    The adapter and prefix exchange are real production C. Native PGPROC,
 *    membership, family mapping and socket boundaries are fixtures. Saving
 *    process-private contexts simulates distinct owners; it is not native
 *    multi-process or distributed application evidence.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_channels.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "../../backend/cluster/cluster_config_channels.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true, cluster_lms_enabled = true;
int cluster_lms_workers = 2, cluster_node_id;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
bool IsUnderPostmaster = true;
int MyProcPid;
BackendType MyBackendType;
AuxProcType MyAuxProcType;
const ClusterICOps ClusterICOps_Tier1 = { 0 };
const ClusterICOps *ClusterICOps_Active = &ClusterICOps_Tier1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterConfigChannelsBoard board;
static PGPROC procs[12];
static PROC_HDR proc_hdr;
PROC_HDR *ProcGlobal = &proc_hdr;
PGPROC *MyProc;
static ClusterR4MembershipSnapshot members;
static ClusterConfigMembersKey expected_key;
static uint8 episode[16];
static ChannelLocal saved_local[CLUSTER_CONFIG_CHANNEL_OWNERS];
static int selected_owner = -1;
static bool family_available, membership_available, stopping, send_throws, stream_throws;
static bool change_members_during_census;
static bool change_caps_after_census;
static unsigned pid_snapshots;
static int32 lmon_pid;
static uint64 streams[CLUSTER_CONFIG_CHANNEL_OWNERS][CLUSTER_MAX_NODES], random_id;
static uint32 caps_generation;
static unsigned sends, handlers;
static ClusterConfigPrefixMessage sent[128];
static ClusterICSendResult send_result;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

ClusterConfigChannelsBoard *
cluster_shared_config_delivery_channels(int32 *pid)
{
	*pid = lmon_pid;
	return family_available ? &board : NULL;
}
bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	*out = members;
	return membership_available;
}
/* The canonical builder is tested through the real member collector suite.
 * This fixture supplies its deterministic selected/member boundary. */
bool
cluster_config_members_make_key(const ClusterSharedConfigRef *ref,
								const ClusterR4MembershipSnapshot *cut,
								ClusterConfigMembersKey *out)
{
	*out = expected_key;
	out->ref = *ref;
	out->epoch = cut->formation_epoch;
	out->required[0] = cut->admitted_members_lo;
	out->required[1] = cut->admitted_members_hi;
	return cut->local_self_boot_incarnation == 31 && cut->admitted_incarnation[0] == 31;
}
bool
cluster_shared_config_registration_read(ClusterSharedConfigSlot *slot,
										ClusterSharedConfigRegistration *out)
{
	*out = slot->value;
	return (pg_atomic_read_u64(&slot->sequence) & 1) == 0;
}
bool
ProcConfigSnapshotPids(int32 *out, uint32 count)
{
	if (++pid_snapshots % 2 == 0 && change_caps_after_census) {
		caps_generation++;
		change_caps_after_census = false;
	}
	if (change_members_during_census) {
		members.formation_epoch++;
		change_members_during_census = false;
	}
	if (count != lengthof(procs))
		return false;
	for (uint32 i = 0; i < count; ++i)
		out[i] = procs[i].pid;
	return true;
}
bool
cluster_normal_stop_requested(void)
{
	return stopping;
}
void *
palloc(Size size)
{
	void *ptr = malloc(size);
	if (!ptr)
		abort();
	return ptr;
}
void
pfree(void *ptr)
{
	free(ptr);
}
void
pg_re_throw(void)
{
	if (!PG_exception_stack)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}
bool
pg_strong_random(void *out, size_t len)
{
	memset(out, 0, len);
	++random_id;
	memcpy(out, &random_id, sizeof(random_id));
	return true;
}
bool
cluster_sf_peer_capability_word_sample(int32 peer, uint32 mask, uint32 *word, uint32 *gen)
{
	UT_ASSERT(peer == 1 || peer == 127);
	*word = mask;
	*gen = caps_generation;
	return true;
}
bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 mask, uint32 gen)
{
	UT_ASSERT(peer == 1 || peer == 127);
	UT_ASSERT_EQ(mask, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1);
	return gen == caps_generation;
}
bool
cluster_ic_tier1_stream_capture(int32 peer, ClusterICTier1Stream *out)
{
	if (stream_throws)
		pg_re_throw();
	memset(out, 0, sizeof(*out));
	if (peer < 0 || peer >= CLUSTER_MAX_NODES || selected_owner < 0
		|| streams[selected_owner][peer] == 0)
		return false;
	out->serial = streams[selected_owner][peer];
	out->epoch = members.formation_epoch;
	out->owner_pid = MyProcPid;
	out->peer = peer;
	out->plane = selected_owner == 0 ? CLUSTER_IC_PLANE_CONTROL : CLUSTER_IC_PLANE_DATA;
	out->channel = selected_owner - 1;
	return true;
}
bool
cluster_ic_tier1_stream_current(const ClusterICTier1Stream *stream)
{
	ClusterICTier1Stream current;
	return cluster_ic_tier1_stream_capture(stream->peer, &current)
		   && memcmp(&current, stream, sizeof(current)) == 0;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 peer, const void *bytes, uint32 len)
{
	UT_ASSERT(sends < lengthof(sent));
	UT_ASSERT(cluster_config_prefix_decode(bytes, len, &sent[sends]));
	UT_ASSERT_EQ(sent[sends].receiver, peer);
	UT_ASSERT_EQ(type, selected_owner == 0 ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
										   : PGRAC_IC_MSG_CONFIG_PREFIX_DATA);
	sends++;
	if (send_throws)
		pg_re_throw();
	return send_result;
}
void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	UT_ASSERT(info->handler == cluster_config_channels_ingress);
	UT_ASSERT(!info->broadcast_ok);
	UT_ASSERT_EQ(info->msg_type, info->plane == CLUSTER_IC_PLANE_CONTROL
									 ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
									 : PGRAC_IC_MSG_CONFIG_PREFIX_DATA);
	handlers++;
}

static void
owner(int idx)
{
	if (selected_owner >= 0)
		saved_local[selected_owner] = channel_local;
	selected_owner = idx;
	channel_local = saved_local[idx];
	MyProc = &procs[idx];
	MyProcPid = MyProc->pid;
	MyBackendType = MyProc->cluster_config.value.role;
	MyAuxProcType = MyProc->cluster_config.value.aux_type;
}
static void
setup(void)
{
	memset(&board, 0, sizeof(board));
	cluster_config_channels_init(&board);
	memset(procs, 0, sizeof(procs));
	memset(&members, 0, sizeof(members));
	memset(&expected_key, 0, sizeof(expected_key));
	memset(saved_local, 0, sizeof(saved_local));
	memset(&channel_local, 0, sizeof(channel_local));
	memset(episode, 1, sizeof(episode));
	proc_hdr.allProcs = procs;
	proc_hdr.allProcCount = lengthof(procs);
	expected_key.ref.identity.system_identifier = 11;
	expected_key.ref.identity.database_incarnation = 12;
	expected_key.ref.identity.generation = 13;
	expected_key.ref.identity.storage_uuid[0] = 14;
	expected_key.ref.identity.authority_uuid[0] = 15;
	expected_key.ref.identity.configured[0] = 3;
	expected_key.ref.identity.configured[1] = UINT64CONST(1) << 63;
	expected_key.ref.sha256[0] = 16;
	expected_key.epoch = members.formation_epoch = 17;
	expected_key.members_sha256[0] = 18;
	expected_key.required[0] = members.admitted_members_lo = 3;
	expected_key.required[1] = members.admitted_members_hi = UINT64CONST(1) << 63;
	members.local_self_boot_incarnation = members.admitted_incarnation[0] = 31;
	members.admitted_incarnation[1] = 32;
	members.admitted_incarnation[127] = 158;
	family_available = membership_available = true;
	cluster_enabled = cluster_shared_config = cluster_lms_enabled = IsUnderPostmaster = true;
	cluster_lms_workers = 2;
	cluster_node_id = 0;
	stopping = send_throws = stream_throws = change_members_during_census = false;
	change_caps_after_census = false;
	pid_snapshots = 0;
	caps_generation = 1;
	sends = handlers = 0;
	send_result = CLUSTER_IC_SEND_DONE;
	for (int i = 0; i < CLUSTER_CONFIG_CHANNEL_OWNERS; ++i) {
		ClusterSharedConfigRegistration *r = &procs[i].cluster_config.value;
		procs[i].pgprocno = i;
		procs[i].pid = r->pid = 100 + i;
		r->registration = 50 + i;
		r->observed = true;
		r->role = i == 0 ? B_LMON : (i == 1 ? B_LMS : B_LMS_WORKER);
		r->aux_type = i == 0 ? LmonProcess : (i == 1 ? LmsProcess : LmsWorker1Process + i - 2);
		pg_atomic_init_u64(&procs[i].cluster_config.sequence, 2);
		for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; ++peer)
			streams[i][peer] = 10 + peer;
	}
	/* Only declared owners are allocated; enable the rest in the eight-worker case. */
	for (int i = 3; i < CLUSTER_CONFIG_CHANNEL_OWNERS; ++i)
		procs[i].pid = 0;
	selected_owner = -1;
	lmon_pid = 100;
	owner(0);
}
static void
arm_owners(void)
{
	owner(0);
	UT_ASSERT(cluster_config_channels_arm(&expected_key.ref, episode));
	for (int i = 0; i <= cluster_lms_workers; ++i) {
		owner(i);
		cluster_config_channels_tick();
	}
	owner(0);
}
static void
reply(const ClusterConfigPrefixMessage *outgoing, bool peer_mark)
{
	ClusterConfigPrefixMessage m = *outgoing;
	ClusterICEnvelope env = { 0 };
	uint8 bytes[CLUSTER_CONFIG_PREFIX_BYTES];
	m.sender = outgoing->receiver;
	m.receiver = outgoing->sender;
	m.sender_incarnation = outgoing->receiver_incarnation;
	m.receiver_incarnation = outgoing->sender_incarnation;
	m.verb = peer_mark ? CLUSTER_CONFIG_PREFIX_MARK : CLUSTER_CONFIG_PREFIX_ACK;
	if (peer_mark)
		m.challenge[15] = 93;
	UT_ASSERT(cluster_config_prefix_encode(&m, bytes));
	env.msg_type = selected_owner == 0 ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
									   : PGRAC_IC_MSG_CONFIG_PREFIX_DATA;
	env.source_node_id = m.sender;
	env.dest_node_id = m.receiver;
	env.epoch = m.key.epoch;
	env.payload_length = sizeof(bytes);
	cluster_config_channels_ingress(&env, bytes);
}

UT_TEST(arming_needs_every_owner_without_sending)
{
	ClusterConfigChannelsCensus census;
	setup();
	UT_ASSERT(cluster_config_channels_arm(&expected_key.ref, episode));
	cluster_config_channels_tick();
	UT_ASSERT(!cluster_config_channels_exchange());
	owner(1);
	cluster_config_channels_tick();
	owner(0);
	UT_ASSERT(!cluster_config_channels_exchange());
	owner(2);
	cluster_config_channels_tick();
	owner(0);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.required, 7);
	UT_ASSERT_EQ(census.armed, 7);
	UT_ASSERT_EQ(census.complete, 0);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(cluster_config_channels_exchange());
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 2); /* Both peers, including node127. */
}
UT_TEST(all_peers_and_directions_precede_completion)
{
	ClusterConfigChannelsCensus census;
	setup();
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	for (int i = 0; i < 3; ++i) {
		unsigned start = sends;
		owner(i);
		cluster_config_channels_tick();
		UT_ASSERT_EQ(sends, start + 2);
		reply(&sent[start], false);
		reply(&sent[start + 1], false);
		reply(&sent[start], true);
		reply(&sent[start + 1], true);
		cluster_config_channels_tick();
	}
	owner(0);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.complete, 7);
	UT_ASSERT_EQ(census.invalid, 0);
	UT_ASSERT_EQ(sends, 12);
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 12);
}
UT_TEST(missing_stream_is_pending_and_rebind_invalidates)
{
	ClusterConfigChannelsCensus census;
	setup();
	streams[2][127] = 0;
	arm_owners();
	UT_ASSERT(!cluster_config_channels_exchange());
	streams[2][127] = 55;
	owner(2);
	cluster_config_channels_tick();
	owner(0);
	UT_ASSERT(cluster_config_channels_exchange());
	owner(2);
	streams[2][1]++;
	cluster_config_channels_tick();
	owner(0);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.invalid, 4);
	UT_ASSERT_EQ(sends, 0);
}
UT_TEST(reused_or_missing_native_owner_cannot_supply_readiness)
{
	ClusterConfigChannelsCensus census;
	setup();
	arm_owners();
	procs[2].cluster_config.value.registration++;
	UT_ASSERT(!cluster_config_channels_observe(&census));
	UT_ASSERT(!cluster_config_channels_exchange());
	procs[2].cluster_config.value.registration--;
	procs[2].pid = 0;
	UT_ASSERT(!cluster_config_channels_observe(&census));
}
UT_TEST(old_owner_and_changed_membership_do_not_continue)
{
	setup();
	arm_owners();
	lmon_pid++;
	UT_ASSERT(!cluster_config_channels_exchange());
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 0);
	setup();
	arm_owners();
	members.formation_epoch++;
	UT_ASSERT(!cluster_config_channels_exchange());
}
UT_TEST(cancel_and_new_episode_do_not_accept_old_ack)
{
	ClusterConfigPrefixMessage old;
	ClusterConfigChannelsCensus census;
	setup();
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	cluster_config_channels_tick();
	old = sent[0];
	cluster_config_channels_cancel();
	episode[0]++;
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	cluster_config_channels_tick();
	reply(&old, false);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.complete, 0);
}
UT_TEST(send_error_publishes_invalid_before_unwind)
{
	ClusterConfigChannelsCensus census;
	volatile bool caught = false;
	setup();
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	send_throws = true;
	PG_TRY();
	{
		cluster_config_channels_tick();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.invalid, 1);
}
UT_TEST(queue_refusal_and_admitted_tail_keep_exact_ownership)
{
	ClusterConfigChannelsCensus census;
	setup();
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 2);
	send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 4);
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 4);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.complete, 0);
}
UT_TEST(all_eight_data_workers_are_required)
{
	ClusterConfigChannelsCensus census;
	setup();
	cluster_lms_workers = 8;
	for (int i = 3; i < CLUSTER_CONFIG_CHANNEL_OWNERS; ++i)
		procs[i].pid = 100 + i;
	arm_owners();
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.required, 511);
	UT_ASSERT_EQ(census.armed, 511);
	UT_ASSERT(cluster_config_channels_exchange());
	owner(8);
	cluster_config_channels_tick();
	UT_ASSERT_EQ(sends, 2);
	UT_ASSERT_EQ(sent[0].channel, 7);
}
UT_TEST(profile_stop_and_torn_publication_never_complete)
{
	ClusterConfigChannelsCensus census;
	setup();
	family_available = false;
	UT_ASSERT(!cluster_config_channels_arm(&expected_key.ref, episode));
	setup();
	arm_owners();
	pg_atomic_fetch_add_u64(&board.command_sequence, 1);
	UT_ASSERT(!cluster_config_channels_observe(&census));
	UT_ASSERT(!cluster_config_channels_exchange());
	setup();
	arm_owners();
	stopping = true;
	cluster_config_channels_tick();
	UT_ASSERT(!cluster_config_channels_exchange());
	UT_ASSERT_EQ(sends, 0);
}

UT_TEST(member_change_during_census_is_not_armed)
{
	ClusterConfigChannelsCensus census;
	setup();
	arm_owners();
	change_members_during_census = true;
	UT_ASSERT(!cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.armed, 0);
	UT_ASSERT(!cluster_config_channels_exchange());
}
UT_TEST(ingress_error_cannot_leave_a_completed_report)
{
	ClusterConfigChannelsCensus census;
	volatile bool caught = false;
	setup();
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	cluster_config_channels_tick();
	reply(&sent[0], false);
	reply(&sent[1], false);
	reply(&sent[0], true);
	reply(&sent[1], true);
	cluster_config_channels_tick();
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.complete, 1);
	stream_throws = true;
	PG_TRY();
	{
		reply(&sent[0], false);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	stream_throws = false;
	UT_ASSERT(caught);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.complete, 0);
	UT_ASSERT_EQ(census.invalid, 1);
}
UT_TEST(registration_retains_exact_native_planes)
{
	setup();
	cluster_config_channels_register();
	UT_ASSERT_EQ(handlers, 2);
}

UT_TEST(retired_stream_and_capability_cannot_leave_completion)
{
	ClusterConfigChannelsCensus census;
	setup();
	arm_owners();
	UT_ASSERT(cluster_config_channels_exchange());
	for (int i = 0; i < 3; ++i) {
		unsigned start = sends;
		owner(i);
		cluster_config_channels_tick();
		reply(&sent[start], false);
		reply(&sent[start + 1], false);
		reply(&sent[start], true);
		reply(&sent[start + 1], true);
		cluster_config_channels_tick();
	}
	owner(0);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.complete, 7);
	/* The native CONTROL capability is shared, independently of worker ticks. */
	caps_generation++;
	UT_ASSERT(!cluster_config_channels_observe(&census) || census.invalid != 0);
	caps_generation--;
	owner(2);
	/* Explicit native-close boundary; the transport suite checks the actual
	 * close/rebind placement before changing its real private stream. */
	cluster_config_channels_stream_retiring(1);
	streams[2][1]++;
	/* No worker tick between transport retirement and LMON observation. */
	owner(0);
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.invalid, 4);
	UT_ASSERT_EQ(census.complete, 3);
}

UT_TEST(zero_capability_generation_is_still_rechecked)
{
	ClusterConfigChannelsCensus census;
	setup();
	/* The shared capability API permits zero. Native TCP currently normalizes
	 * its first generation to one; this is a boundary/sentinel negative, not
	 * evidence of a zero-generation TCP connection or RDMA support. */
	caps_generation = 0;
	arm_owners();
	UT_ASSERT(cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.armed, 7);
	change_caps_after_census = true;
	UT_ASSERT(!cluster_config_channels_observe(&census));
	UT_ASSERT_EQ(census.armed, 0);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(arming_needs_every_owner_without_sending);
	UT_RUN(all_peers_and_directions_precede_completion);
	UT_RUN(missing_stream_is_pending_and_rebind_invalidates);
	UT_RUN(reused_or_missing_native_owner_cannot_supply_readiness);
	UT_RUN(old_owner_and_changed_membership_do_not_continue);
	UT_RUN(cancel_and_new_episode_do_not_accept_old_ack);
	UT_RUN(send_error_publishes_invalid_before_unwind);
	UT_RUN(queue_refusal_and_admitted_tail_keep_exact_ownership);
	UT_RUN(all_eight_data_workers_are_required);
	UT_RUN(profile_stop_and_torn_publication_never_complete);
	UT_RUN(member_change_during_census_is_not_armed);
	UT_RUN(ingress_error_cannot_leave_a_completed_report);
	UT_RUN(registration_retains_exact_native_planes);
	UT_RUN(retired_stream_and_capability_cannot_leave_completion);
	UT_RUN(zero_capability_generation_is_still_rechecked);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
