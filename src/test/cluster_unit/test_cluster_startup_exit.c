/*-------------------------------------------------------------------------
 *
 * test_cluster_startup_exit.c
 *    Actual prior-exit codec/collector with transport and producer fixtures.
 *    QVOTEC's disk capture is tested by test_cluster_qvotec; this program does
 *    not claim to certify distributed startup, CF ownership or network auth.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_startup_exit.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_startup_exit.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

BackendType MyBackendType = B_LMON;
bool cluster_shared_config = true;
bool cluster_enabled = true;
int cluster_node_id;
static bool quorum = true, evidence_available = true;
static uint64 epoch = 7, incarnations[CLUSTER_MAX_NODES];
static bool members[CLUSTER_MAX_NODES];
static TimestampTz now_us = 1000000;
static uint64 nonce_source;
static unsigned sends, observations;
static ClusterICSendResult send_result = CLUSTER_IC_SEND_DONE;
static ClusterStartupExitMessage last;
static int32 last_dest;
static ClusterICMsgTypeInfo registration;
static bool drift_on_observe;
int MyProcPid = 100;
static unsigned wakeups;
static const ClusterShmemRegion *registered_region;
static char shared_bytes[8192] pg_attribute_aligned(MAXIMUM_ALIGNOF);
static bool shared_found;
static pg_on_exit_callback exit_callback;
static Datum exit_callback_arg;

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	registered_region = region;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT(strcmp(name, "pgrac startup exit mailbox") == 0);
	UT_ASSERT(size <= sizeof(shared_bytes));
	*found = shared_found;
	shared_found = true;
	return shared_bytes;
}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	exit_callback = function;
	exit_callback_arg = arg;
}

void
cluster_lmon_wakeup(void)
{
	++wakeups;
}

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	(void)lock;
	(void)file;
	(void)line;
	(void)func;
	abort();
}

bool
pg_strong_random(void *buf, size_t len)
{
	UT_ASSERT_EQ(len, sizeof(uint64));
	++nonce_source;
	memcpy(buf, &nonce_source, len);
	return true;
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
cluster_membership_is_member(int32 node)
{
	return node >= 0 && node < CLUSTER_MAX_NODES && members[node];
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return node >= 0 && node < CLUSTER_MAX_NODES ? incarnations[node] : 0;
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	static ClusterNodeInfo info;
	return node == 0 || node == 3 ? &info : NULL;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return now_us;
}

bool
cluster_qvotec_prior_exit_observe(uint32 node, uint64 old_inc, uint64 observer,
								  ClusterQvotecPriorExitObservation *out)
{
	memset(out, 0, sizeof(*out));
	++observations;
	if (!evidence_available || node != (uint32)cluster_node_id || old_inc != 11 + node
		|| observer != incarnations[node])
		return false;
	out->node_id = node;
	out->observing_incarnation = observer;
	out->n_disks = 3;
	for (int i = 0; i < 3; ++i) {
		out->slots[i].node_id = node;
		out->slots[i].incarnation = old_inc;
		out->slots[i].generation = 91 + i;
	}
	if (drift_on_observe)
		++epoch;
	return true;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 length)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_STARTUP_EXIT);
	UT_ASSERT(cluster_startup_exit_decode(payload, length, &last));
	last_dest = dest;
	++sends;
	return send_result;
}

void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	registration = *info;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected Assert %s at %s:%d\n", condition, file, line);
	abort();
}

static void
put64(uint8 *b, uint64 v)
{
	for (int i = 0; i < 8; i++)
		b[i] = (uint8)(v >> (i * 8));
}

static void
literal(uint8 *b)
{
	memset(b, 0, 208);
	memcpy(b, "PWEX", 4);
	b[4] = 1;
	b[6] = 208;
	b[8] = 1;
	b[12] = 3;
	put64(b + 16, 14);
	put64(b + 24, 24);
	put64(b + 40, 21);
	put64(b + 48, 7);
	put64(b + 56, 9);
	memset(b + 64, 0x11, 32);
	put64(b + 96, 123456789);
	put64(b + 104, 4);
	put64(b + 112, 5);
	memset(b + 120, 0x22, 16);
	memset(b + 136, 0x33, 16);
	put64(b + 152, 6);
}

static ClusterStartupExitCut
reset(void)
{
	ClusterStartupExitCut cut;
	memset(&cut, 0, sizeof(cut));
	cut.key.system_identifier = 123456789;
	cut.key.database_incarnation = 4;
	cut.key.config_generation = 5;
	cut.key.epoch = 7;
	cut.key.root_sequence = 9;
	cut.key.coordinator_incarnation = 21;
	memset(cut.key.root_sha256, 0x11, 32);
	memset(cut.key.storage_uuid, 0x22, 16);
	memset(cut.key.authority_uuid, 0x33, 16);
	cut.required[0] = 9;
	cut.predecessor[0] = 11;
	cut.predecessor[3] = 14;
	cut.observer[0] = 21;
	cut.observer[3] = 24;
	memset(members, 0, sizeof(members));
	memset(incarnations, 0, sizeof(incarnations));
	members[0] = members[3] = true;
	incarnations[0] = 21;
	incarnations[3] = 24;
	cluster_node_id = 0;
	MyBackendType = B_LMON;
	cluster_shared_config = quorum = evidence_available = true;
	drift_on_observe = false;
	epoch = 7;
	now_us += 1000000;
	sends = observations = 0;
	send_result = CLUSTER_IC_SEND_DONE;
	memset(&last, 0, sizeof(last));
	cluster_startup_exit_cancel();
	return cut;
}

static void
ingress(ClusterStartupExitMessage message, uint32 source, uint32 dest)
{
	uint8 wire[208];
	ClusterICEnvelope env;
	memset(&env, 0, sizeof(env));
	env.msg_type = PGRAC_IC_MSG_STARTUP_EXIT;
	env.source_node_id = source;
	env.dest_node_id = dest;
	env.epoch = message.key.epoch;
	env.payload_length = sizeof(wire);
	UT_ASSERT(cluster_startup_exit_encode(&message, wire));
	cluster_startup_exit_ingress(&env, wire);
}

static ClusterStartupExitMessage
peer_reply(ClusterStartupExitMessage request)
{
	cluster_node_id = 3;
	ingress(request, 0, 3);
	UT_ASSERT_EQ(last.verb, CLUSTER_STARTUP_EXIT_REPLY);
	UT_ASSERT_EQ(last_dest, 0);
	cluster_node_id = 0;
	return last;
}

UT_TEST(independent_wire_and_encoder)
{
	uint8 bytes[208], encoded[208];
	ClusterStartupExitMessage message;
	literal(bytes);
	UT_ASSERT(cluster_startup_exit_decode(bytes, sizeof(bytes), &message));
	UT_ASSERT_EQ(message.key.system_identifier, 123456789);
	UT_ASSERT_EQ(message.key.coordinator_incarnation, 21);
	UT_ASSERT_EQ(message.node, 3);
	UT_ASSERT_EQ(message.old_incarnation, 14);
	UT_ASSERT_EQ(message.observing_incarnation, 24);
	UT_ASSERT_EQ(message.nonce, 6);
	UT_ASSERT(cluster_startup_exit_encode(&message, encoded));
	UT_ASSERT_EQ(memcmp(bytes, encoded, 208), 0);
}

UT_TEST(malformed_wire_never_observation)
{
	uint8 bytes[208];
	ClusterStartupExitMessage message, zero = { 0 };
	const int offsets[] = { 0, 4, 6, 9, 13, 36, 160, 191, 192, 207 };
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		literal(bytes);
		bytes[offsets[i]] ^= 0x80;
		memset(&message, 0xff, sizeof(message));
		UT_ASSERT(!cluster_startup_exit_decode(bytes, sizeof(bytes), &message));
		UT_ASSERT_EQ(memcmp(&message, &zero, sizeof(zero)), 0);
	}
	literal(bytes);
	UT_ASSERT(!cluster_startup_exit_decode(bytes, 207, &message));
	bytes[8] = 2;
	UT_ASSERT(!cluster_startup_exit_decode(bytes, 208, &message));
	memset(bytes + 160, 0x44, 32);
	UT_ASSERT(cluster_startup_exit_decode(bytes, 208, &message));
}

UT_TEST(sparse_pair_needs_actual_local_and_peer)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage reply;
	uint8 digest[32], previous[32], zero[32] = { 0 };
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	UT_ASSERT_EQ(memcmp(digest, zero, 32), 0);
	UT_ASSERT_EQ(sends, 1);
	UT_ASSERT_EQ(observations, 1);
	UT_ASSERT_EQ(last_dest, 3);
	reply = peer_reply(last);
	ingress(reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	UT_ASSERT_NE(memcmp(digest, zero, 32), 0);
	memcpy(previous, digest, 32);
	ingress(reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	UT_ASSERT_EQ(memcmp(digest, previous, 32), 0);
}

UT_TEST(loss_retries_same_identity_without_false_ready)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage original;
	uint8 digest[32];
	send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	original = last;
	now_us += 1000000;
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	UT_ASSERT_EQ(memcmp(&last, &original, sizeof(last)), 0);
	UT_ASSERT_EQ(sends, 2);
	evidence_available = false;
	cluster_node_id = 3;
	ingress(original, 0, 3);
	UT_ASSERT_EQ(sends, 2);
}

UT_TEST(old_round_and_wrong_envelope_cannot_complete)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage reply;
	uint8 digest[32];
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	reply = peer_reply(last);
	cluster_startup_exit_cancel();
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	UT_ASSERT_NE(last.nonce, reply.nonce);
	ingress(reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	reply = peer_reply(last);
	ingress(reply, 0, 0);
	ingress(reply, 3, 1);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	ingress(reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
}

UT_TEST(replies_bind_every_cut_identity)
{
	for (int variant = 0; variant < 12; variant++) {
		ClusterStartupExitCut cut = reset();
		ClusterStartupExitMessage reply;
		uint8 digest[32];
		UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
		reply = peer_reply(last);
		switch (variant) {
		case 0:
			++reply.key.system_identifier;
			break;
		case 1:
			++reply.key.database_incarnation;
			break;
		case 2:
			++reply.key.config_generation;
			break;
		case 3:
			++reply.key.epoch;
			break;
		case 4:
			++reply.key.root_sequence;
			break;
		case 5:
			++reply.key.coordinator_incarnation;
			break;
		case 6:
			++reply.key.root_sha256[0];
			break;
		case 7:
			++reply.key.storage_uuid[0];
			break;
		case 8:
			++reply.key.authority_uuid[0];
			break;
		case 9:
			++reply.old_incarnation;
			break;
		case 10:
			++reply.observing_incarnation;
			break;
		case 11:
			++reply.nonce;
			break;
		}
		ingress(reply, 3, 0);
		UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	}
}

UT_TEST(runtime_guards_and_membership_drift)
{
	for (int variant = 0; variant < 7; variant++) {
		ClusterStartupExitCut cut = reset();
		uint8 digest[32], zero[32] = { 0 };
		switch (variant) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			cluster_shared_config = false;
			break;
		case 2:
			quorum = false;
			break;
		case 3:
			++epoch;
			break;
		case 4:
			++incarnations[3];
			break;
		case 5:
			members[3] = false;
			break;
		case 6:
			drift_on_observe = true;
			break;
		}
		memset(digest, 0xff, 32);
		UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(digest, zero, 32), 0);
	}
}

UT_TEST(conflicting_same_round_proof_poisoned)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage reply;
	uint8 digest[32];
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	reply = peer_reply(last);
	ingress(reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	reply.evidence_sha256[0] ^= 1;
	ingress(reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_UNAVAILABLE);
}

UT_TEST(registration_is_point_to_point_control_lmon_only)
{
	memset(&registration, 0, sizeof(registration));
	cluster_startup_exit_register();
	UT_ASSERT_EQ(registration.msg_type, PGRAC_IC_MSG_STARTUP_EXIT);
	UT_ASSERT_EQ(registration.allowed_producer_mask, CLUSTER_IC_PRODUCER_LMON);
	UT_ASSERT(!registration.broadcast_ok);
	UT_ASSERT_EQ(registration.plane, CLUSTER_IC_PLANE_CONTROL);
	UT_ASSERT(registration.handler == cluster_startup_exit_ingress);
}

UT_TEST(changed_root_recollects_and_cached_proof_rechecks_members)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage old_reply;
	uint8 digest[32], zero[32] = { 0 };
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	old_reply = peer_reply(last);
	ingress(old_reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	members[3] = false;
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(digest, zero, 32), 0);
	members[3] = true;
	++cut.key.root_sequence;
	++cut.key.root_sha256[0];
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	ingress(old_reply, 3, 0);
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
}

UT_TEST(missing_local_evidence_cannot_be_replaced_by_peer)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage reply;
	uint8 digest[32];
	evidence_available = false;
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	evidence_available = true;
	reply = peer_reply(last);
	ingress(reply, 3, 0);
	evidence_available = false;
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
}

UT_TEST(request_to_wrong_owner_does_not_attest)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage request;
	uint8 digest[32];
	unsigned count;
	UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	request = last;
	count = sends;
	cluster_node_id = 3;
	ingress(request, 3, 3);
	UT_ASSERT_EQ(sends, count);
	++request.observing_incarnation;
	ingress(request, 0, 3);
	UT_ASSERT_EQ(sends, count);
}

UT_TEST(invalid_or_empty_roster_has_no_messages)
{
	for (int fault = 0; fault < 5; ++fault) {
		ClusterStartupExitCut cut = reset();
		uint8 digest[32];
		switch (fault) {
		case 0:
			memset(cut.required, 0, sizeof(cut.required));
			break;
		case 1:
			cut.predecessor[1] = 42;
			break;
		case 2:
			cut.required[0] |= 2;
			break;
		case 3:
			cut.key.reserved = 1;
			break;
		case 4:
			cut.key.coordinator_incarnation++;
			break;
		}
		UT_ASSERT_EQ(cluster_startup_exit_collect(&cut, digest), CLUSTER_STARTUP_EXIT_UNAVAILABLE);
		UT_ASSERT_EQ(sends, 0);
	}
}

UT_TEST(codec_aliases_refuse_without_partial_observation)
{
	union {
		ClusterStartupExitMessage message;
		uint8 bytes[208];
	} alias;
	ClusterStartupExitMessage message;
	uint8 bytes[208];
	literal(bytes);
	UT_ASSERT(cluster_startup_exit_decode(bytes, sizeof(bytes), &message));
	alias.message = message;
	UT_ASSERT(!cluster_startup_exit_encode(&alias.message, alias.bytes));
	memcpy(alias.bytes, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_startup_exit_decode(alias.bytes, sizeof(alias.bytes), &alias.message));
	UT_ASSERT(!cluster_startup_exit_decode(NULL, 208, &message));
	UT_ASSERT(!cluster_startup_exit_encode(NULL, bytes));
	for (unsigned i = 0; i < sizeof(bytes); ++i)
		UT_ASSERT_EQ(bytes[i], 0);
}

UT_TEST(native_executor_uses_actual_lmon_round_without_transporting)
{
	ClusterStartupExitCut cut = reset();
	ClusterStartupExitMessage reply;
	uint8 digest[32], zero[32] = { 0 };
	cluster_startup_exit_shmem_register();
	UT_ASSERT(registered_region != NULL);
	if (registered_region == NULL)
		return;
	shared_found = false;
	registered_region->init_fn();
	MyBackendType = B_STARTUP;
	wakeups = 0;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT_EQ(wakeups, 1);
	MyBackendType = B_LMON;
	cluster_startup_exit_lmon_tick();
	UT_ASSERT_EQ(sends, 1);
	reply = peer_reply(last);
	ingress(reply, 3, 0);
	cluster_startup_exit_lmon_tick();
	MyBackendType = B_STARTUP;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	UT_ASSERT_NE(memcmp(digest, zero, 32), 0);
	cluster_startup_exit_request_cancel();
}

static ClusterStartupExitCut
mailbox_ready(ClusterStartupExitMessage *reply)
{
	ClusterStartupExitCut cut = reset();
	uint8 digest[32];
	MyProcPid = 100;
	shared_found = false;
	registered_region->init_fn();
	MyBackendType = B_STARTUP;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	MyBackendType = B_LMON;
	cluster_startup_exit_lmon_tick();
	*reply = peer_reply(last);
	ingress(*reply, 3, 0);
	cluster_startup_exit_lmon_tick();
	MyBackendType = B_STARTUP;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	return cut;
}

UT_TEST(mailbox_cannot_steal_replay_or_outlive_its_executor)
{
	ClusterStartupExitMessage old;
	ClusterStartupExitCut cut = mailbox_ready(&old);
	uint8 digest[32], zero[32] = { 0 };
	MyProcPid = 101;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_UNAVAILABLE);
	cluster_startup_exit_request_cancel();
	MyProcPid = 100;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_READY);
	UT_ASSERT(exit_callback != NULL);
	if (exit_callback == NULL)
		return;
	exit_callback(0, exit_callback_arg);
	/* No LMON tick between cancellation and resubmission: revision must
	 * still force a fresh wire nonce instead of reusing the cached proof. */
	MyProcPid = 101;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	MyBackendType = B_LMON;
	cluster_startup_exit_lmon_tick();
	UT_ASSERT_NE(last.nonce, old.nonce);
	ingress(old, 3, 0);
	cluster_startup_exit_lmon_tick();
	MyBackendType = B_STARTUP;
	UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest), CLUSTER_STARTUP_EXIT_WAITING);
	UT_ASSERT_EQ(memcmp(digest, zero, 32), 0);
	cluster_startup_exit_request_cancel();
	MyProcPid = 100;
}

UT_TEST(mailbox_never_reuses_wrong_cut_or_poisoned_evidence)
{
	for (int fault = 0; fault < 5; ++fault) {
		ClusterStartupExitMessage old;
		ClusterStartupExitCut cut = mailbox_ready(&old);
		uint8 digest[32], zero[32] = { 0 };
		if (fault == 0)
			++cut.key.root_sequence;
		if (fault == 1)
			++incarnations[3];
		if (fault == 2)
			quorum = false;
		if (fault == 3)
			MyBackendType = B_BACKEND;
		if (fault == 4) {
			MyBackendType = B_LMON;
			old.evidence_sha256[0] ^= 1;
			ingress(old, 3, 0);
			MyBackendType = B_STARTUP;
		}
		UT_ASSERT_EQ(cluster_startup_exit_request(&cut, digest),
					 fault == 0 ? CLUSTER_STARTUP_EXIT_WAITING : CLUSTER_STARTUP_EXIT_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(digest, zero, 32), 0);
		MyBackendType = B_STARTUP;
		cluster_startup_exit_request_cancel();
	}
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(native_executor_uses_actual_lmon_round_without_transporting);
	UT_RUN(mailbox_cannot_steal_replay_or_outlive_its_executor);
	UT_RUN(mailbox_never_reuses_wrong_cut_or_poisoned_evidence);
	UT_RUN(independent_wire_and_encoder);
	UT_RUN(malformed_wire_never_observation);
	UT_RUN(sparse_pair_needs_actual_local_and_peer);
	UT_RUN(loss_retries_same_identity_without_false_ready);
	UT_RUN(old_round_and_wrong_envelope_cannot_complete);
	UT_RUN(replies_bind_every_cut_identity);
	UT_RUN(runtime_guards_and_membership_drift);
	UT_RUN(conflicting_same_round_proof_poisoned);
	UT_RUN(registration_is_point_to_point_control_lmon_only);
	UT_RUN(changed_root_recollects_and_cached_proof_rechecks_members);
	UT_RUN(missing_local_evidence_cannot_be_replaced_by_peer);
	UT_RUN(request_to_wrong_owner_does_not_attest);
	UT_RUN(invalid_or_empty_roster_has_no_messages);
	UT_RUN(codec_aliases_refuse_without_partial_observation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
