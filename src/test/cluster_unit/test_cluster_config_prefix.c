/*-------------------------------------------------------------------------
 *
 * test_cluster_config_prefix.c
 *    Execute the actual prefix codec and native-owner exchange.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_prefix.c
 * NOTES
 *    Transport/capability/randomness boundaries are explicit. Native TCP
 *    partial/FIFO/stream tests cover that substrate, not global admission.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_prefix.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

BackendType MyBackendType;
AuxProcType MyAuxProcType;
bool IsUnderPostmaster = true, cluster_enabled = true, cluster_shared_config = true;
int MyProcPid, cluster_node_id;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
const ClusterICOps ClusterICOps_Tier1 = { 0 };
static const ClusterICOps other_transport = { 0 };
const ClusterICOps *ClusterICOps_Active = &ClusterICOps_Tier1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterConfigMembersKey key;
static uint8 episode[16];
static ClusterConfigPrefixExchange exchange[2];
static ClusterConfigPrefixMessage sent[2];
static unsigned sends[2];
static bool stream_available, caps_available, random_available, send_error, rebind_during_send;
static uint64 serial[2], random_counter;
static uint32 cap_generation, plane;
static ClusterICSendResult send_result;

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
	UT_ASSERT_EQ(len, 16);
	memset(out, 0, len);
	++random_counter;
	memcpy(out, &random_counter, sizeof(random_counter));
	return random_available;
}
bool
cluster_sf_peer_capability_word_sample(int32 peer, uint32 bits, uint32 *word, uint32 *generation)
{
	UT_ASSERT(peer == 1 - cluster_node_id);
	UT_ASSERT_EQ(bits, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1);
	*word = caps_available ? bits : 0;
	*generation = cap_generation;
	return caps_available && cap_generation != 0;
}
bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 bits, uint32 generation)
{
	UT_ASSERT(peer == 1 - cluster_node_id);
	UT_ASSERT_EQ(bits, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1);
	return caps_available && cap_generation != 0 && generation == cap_generation;
}
bool
cluster_ic_tier1_stream_capture(int32 peer, ClusterICTier1Stream *out)
{
	memset(out, 0, sizeof(*out));
	if (!stream_available || peer != 1 - cluster_node_id)
		return false;
	out->serial = serial[cluster_node_id];
	out->epoch = key.epoch;
	out->owner_pid = MyProcPid;
	out->peer = peer;
	out->plane = plane;
	out->channel = plane == CLUSTER_IC_PLANE_CONTROL ? -1 : 1;
	return true;
}
bool
cluster_ic_tier1_stream_current(const ClusterICTier1Stream *s)
{
	ClusterICTier1Stream actual;
	return cluster_ic_tier1_stream_capture(s->peer, &actual)
		   && memcmp(s, &actual, sizeof(actual)) == 0;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 peer, const void *payload, uint32 len)
{
	UT_ASSERT(peer == 1 - cluster_node_id);
	UT_ASSERT_EQ(type, plane == CLUSTER_IC_PLANE_CONTROL ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
														 : PGRAC_IC_MSG_CONFIG_PREFIX_DATA);
	UT_ASSERT(cluster_config_prefix_decode(payload, len, &sent[cluster_node_id]));
	sends[cluster_node_id]++;
	if (rebind_during_send)
		serial[cluster_node_id]++;
	if (send_error)
		pg_re_throw();
	return send_result;
}
static void
node(unsigned n)
{
	cluster_node_id = n;
	MyProcPid = 100 + n;
	MyBackendType = plane == CLUSTER_IC_PLANE_CONTROL ? B_LMON : B_LMS_WORKER;
	MyAuxProcType = plane == CLUSTER_IC_PLANE_CONTROL ? LmonProcess : LmsWorker1Process;
}
static void
reset(unsigned selected_plane)
{
	memset(&key, 0, sizeof(key));
	memset(exchange, 0, sizeof(exchange));
	memset(sent, 0, sizeof(sent));
	memset(sends, 0, sizeof(sends));
	memset(episode, 1, sizeof(episode));
	key.ref.identity.system_identifier = 11;
	key.ref.identity.database_incarnation = 12;
	key.ref.identity.generation = 13;
	key.ref.identity.storage_uuid[0] = 14;
	key.ref.identity.authority_uuid[0] = 15;
	key.ref.identity.configured[0] = 3;
	key.ref.sha256[0] = 16;
	key.required[0] = 3;
	key.epoch = 17;
	key.members_sha256[0] = 18;
	stream_available = caps_available = random_available = true;
	cluster_enabled = cluster_shared_config = IsUnderPostmaster = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	ClusterICOps_Active = &ClusterICOps_Tier1;
	send_error = false;
	rebind_during_send = false;
	serial[0] = 21;
	serial[1] = 22;
	cap_generation = 23;
	random_counter = 0;
	send_result = CLUSTER_IC_SEND_DONE;
	plane = selected_plane;
	node(0);
}
static void
begin_pair(void)
{
	for (unsigned n = 0; n < 2; ++n) {
		node(n);
		UT_ASSERT(cluster_config_prefix_begin(&exchange[n], &key, episode, 1 - n, 31 + n, 32 - n));
		UT_ASSERT(!cluster_config_prefix_complete(&exchange[n]));
	}
}
static bool
deliver(const ClusterConfigPrefixMessage *m)
{
	uint8 bytes[CLUSTER_CONFIG_PREFIX_BYTES];
	ClusterICEnvelope env = { 0 };
	UT_ASSERT(cluster_config_prefix_encode(m, bytes));
	env.msg_type = plane == CLUSTER_IC_PLANE_CONTROL ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
													 : PGRAC_IC_MSG_CONFIG_PREFIX_DATA;
	env.source_node_id = m->sender;
	env.dest_node_id = m->receiver;
	env.epoch = m->key.epoch;
	env.payload_length = sizeof(bytes);
	node(m->receiver);
	return cluster_config_prefix_ingress(&exchange[cluster_node_id], &env, bytes);
}
static void
complete_pair(void)
{
	ClusterConfigPrefixMessage marks[2];
	for (unsigned n = 0; n < 2; ++n) {
		node(n);
		cluster_config_prefix_poll(&exchange[n]);
		marks[n] = sent[n];
		UT_ASSERT_EQ(marks[n].verb, CLUSTER_CONFIG_PREFIX_MARK);
	}
	UT_ASSERT(deliver(&marks[0]));
	UT_ASSERT(deliver(&marks[1]));
	for (unsigned n = 0; n < 2; ++n) {
		node(n);
		UT_ASSERT(!cluster_config_prefix_complete(&exchange[n]));
		cluster_config_prefix_poll(&exchange[n]);
		UT_ASSERT_EQ(sent[n].verb, CLUSTER_CONFIG_PREFIX_ACK);
	}
	UT_ASSERT(deliver(&sent[0]));
	UT_ASSERT(deliver(&sent[1]));
	for (unsigned n = 0; n < 2; ++n) {
		node(n);
		UT_ASSERT(cluster_config_prefix_complete(&exchange[n]));
	}
}
UT_TEST(literal_wire_roundtrip_and_reserved_bytes)
{
	for (unsigned p = 0; p < 2; ++p) {
		ClusterConfigPrefixMessage decoded;
		uint8 bytes[256];
		reset(p);
		begin_pair();
		UT_ASSERT(cluster_config_prefix_encode(&exchange[0].mark, bytes));
		UT_ASSERT(memcmp(bytes, "PCPX\1\0\0\1", 8) == 0);
		UT_ASSERT_EQ(bytes[8], CLUSTER_CONFIG_PREFIX_MARK);
		UT_ASSERT_EQ(bytes[12], 0);
		UT_ASSERT_EQ(bytes[16], 1);
		UT_ASSERT_EQ(bytes[20], p);
		UT_ASSERT_EQ(bytes[24], p == 0 ? 255 : 1);
		UT_ASSERT_EQ(bytes[32], 17);
		UT_ASSERT_EQ(bytes[40], 31);
		UT_ASSERT_EQ(bytes[48], 32);
		UT_ASSERT_EQ(bytes[88], 11);
		UT_ASSERT_EQ(bytes[192], 3);
		UT_ASSERT(cluster_config_prefix_decode(bytes, sizeof(bytes), &decoded));
		UT_ASSERT(memcmp(&decoded, &exchange[0].mark, sizeof(decoded)) == 0);
		for (unsigned offset = 240; offset < 256; offset++) {
			bytes[offset] = 1;
			UT_ASSERT(!cluster_config_prefix_decode(bytes, sizeof(bytes), &decoded));
			UT_ASSERT_EQ(decoded.verb, 0);
			bytes[offset] = 0;
		}
	}
}
UT_TEST(malformed_identity_is_not_a_marker)
{
	reset(0);
	begin_pair();
	for (unsigned bad = 0; bad < 15; bad++) {
		ClusterConfigPrefixMessage m = exchange[0].mark;
		uint8 bytes[256];
		switch (bad) {
		case 0:
			m.verb = 0;
			break;
		case 1:
			m.sender = m.receiver;
			break;
		case 2:
			m.receiver = 128;
			break;
		case 3:
			m.key.required[0] = 1;
			break;
		case 4:
			m.key.required[1] = 1;
			break;
		case 5:
			m.key.ref.identity.system_identifier = 0;
			break;
		case 6:
			m.key.epoch = 0;
			break;
		case 7:
			m.sender_incarnation = 0;
			break;
		case 8:
			m.receiver_incarnation = 0;
			break;
		case 9:
			memset(m.episode, 0, sizeof(m.episode));
			break;
		case 10:
			memset(m.challenge, 0, sizeof(m.challenge));
			break;
		case 11:
			memset(m.key.ref.sha256, 0, 32);
			break;
		case 12:
			memset(m.key.members_sha256, 0, 32);
			break;
		case 13:
			m.plane = 2;
			break;
		case 14:
			m.channel = 0;
			break;
		}
		memset(bytes, 255, sizeof(bytes));
		UT_ASSERT(!cluster_config_prefix_encode(&m, bytes));
		UT_ASSERT_EQ(bytes[0], 0);
	}
}
UT_TEST(alias_does_not_clobber_input)
{
	union {
		ClusterConfigPrefixMessage m;
		uint8 bytes[512];
	} carrier, before;
	reset(0);
	begin_pair();
	memset(&carrier, 0, sizeof(carrier));
	carrier.m = exchange[0].mark;
	before = carrier;
	UT_ASSERT(!cluster_config_prefix_encode(&carrier.m, carrier.bytes));
	UT_ASSERT(memcmp(&carrier, &before, sizeof(carrier)) == 0);
	UT_ASSERT(cluster_config_prefix_encode(&carrier.m, before.bytes));
	carrier = before;
	UT_ASSERT(!cluster_config_prefix_decode(carrier.bytes, 256, &carrier.m));
	UT_ASSERT(memcmp(&carrier, &before, sizeof(carrier)) == 0);
}
UT_TEST(malformed_carrier_and_null_outputs_are_refused)
{
	static const unsigned offsets[] = { 0, 4, 6, 8, 12, 16, 20, 24, 28, 29, 30, 31 };
	uint8 bytes[256], good[256];
	ClusterConfigPrefixMessage out;
	reset(0);
	begin_pair();
	UT_ASSERT(cluster_config_prefix_encode(&exchange[0].mark, good));
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		memcpy(bytes, good, sizeof(bytes));
		bytes[offsets[i]] ^= 128;
		memset(&out, 255, sizeof(out));
		UT_ASSERT(!cluster_config_prefix_decode(bytes, sizeof(bytes), &out));
		UT_ASSERT_EQ(out.verb, 0);
	}
	for (unsigned len = 0; len < sizeof(good); len++)
		UT_ASSERT(!cluster_config_prefix_decode(good, len, &out));
	UT_ASSERT(!cluster_config_prefix_decode(good, sizeof(good) + 1, &out));
	UT_ASSERT(!cluster_config_prefix_decode(NULL, sizeof(good), &out));
	UT_ASSERT(!cluster_config_prefix_decode(good, sizeof(good), NULL));
	UT_ASSERT(!cluster_config_prefix_encode(NULL, bytes));
	UT_ASSERT_EQ(bytes[0], 0);
	UT_ASSERT(!cluster_config_prefix_encode(&exchange[0].mark, NULL));
	reset(1);
	begin_pair();
	exchange[0].mark.channel = CLUSTER_IC_TIER1_DATA_CHANNELS;
	UT_ASSERT(!cluster_config_prefix_encode(&exchange[0].mark, bytes));
}
UT_TEST(nonzero_but_different_identity_or_envelope_is_not_a_receipt)
{
	for (unsigned bad = 0; bad < 17; bad++) {
		ClusterConfigPrefixMessage m;
		ClusterICEnvelope env = { 0 };
		uint8 bytes[256];
		reset(1);
		begin_pair();
		m = exchange[1].mark;
		env.msg_type = PGRAC_IC_MSG_CONFIG_PREFIX_DATA;
		env.source_node_id = 1;
		env.dest_node_id = 0;
		env.epoch = key.epoch;
		env.payload_length = sizeof(bytes);
		switch (bad) {
		case 0:
			env.msg_type = PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL;
			break;
		case 1:
			env.source_node_id = 2;
			break;
		case 2:
			env.dest_node_id = 2;
			break;
		case 3:
			env.epoch++;
			break;
		case 4:
			m.sender_incarnation++;
			break;
		case 5:
			m.receiver_incarnation++;
			break;
		case 6:
			m.channel++;
			break;
		case 7:
			m.episode[0]++;
			break;
		case 8:
			m.key.ref.identity.system_identifier++;
			break;
		case 9:
			m.key.ref.identity.database_incarnation++;
			break;
		case 10:
			m.key.ref.identity.generation++;
			break;
		case 11:
			m.key.ref.identity.storage_uuid[0]++;
			break;
		case 12:
			m.key.ref.identity.authority_uuid[0]++;
			break;
		case 13:
			m.key.ref.sha256[0]++;
			break;
		case 14:
			m.key.members_sha256[0]++;
			break;
		case 15:
			m.key.ref.identity.configured[1] = 1;
			break;
		case 16:
			m.key.ref.identity.configured[1] = m.key.required[1] = 1;
			break;
		}
		UT_ASSERT(cluster_config_prefix_encode(&m, bytes));
		node(0);
		UT_ASSERT(!cluster_config_prefix_ingress(&exchange[0], &env, bytes));
		UT_ASSERT(!exchange[0].peer_mark_seen && !exchange[0].mark_acknowledged);
		UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
	}
}
UT_TEST(both_directions_and_both_planes_are_required)
{
	for (unsigned p = 0; p < 2; ++p) {
		reset(p);
		begin_pair();
		complete_pair();
		for (unsigned n = 0; n < 2; ++n) {
			node(n);
			cluster_config_prefix_poll(&exchange[n]);
			UT_ASSERT_EQ(sends[n], 2);
		}
	}
}
UT_TEST(queue_refusal_retains_but_admitted_tail_is_not_resent)
{
	reset(1);
	begin_pair();
	node(0);
	send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(!exchange[0].mark_admitted && !exchange[0].failed);
	send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(exchange[0].mark_admitted);
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT_EQ(sends[0], 2);
	UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
}
UT_TEST(duplicate_mark_and_ack_are_idempotent)
{
	reset(0);
	begin_pair();
	complete_pair();
	UT_ASSERT(deliver(&exchange[0].mark));
	UT_ASSERT(deliver(&sent[1]));
	node(0);
	UT_ASSERT(cluster_config_prefix_complete(&exchange[0]));
	node(1);
	cluster_config_prefix_poll(&exchange[1]);
	UT_ASSERT_EQ(sends[1], 2);
}
UT_TEST(ack_before_admission_and_wrong_challenge_cannot_complete)
{
	ClusterConfigPrefixMessage ack;
	reset(0);
	begin_pair();
	ack = exchange[0].mark;
	ack.verb = CLUSTER_CONFIG_PREFIX_ACK;
	ack.sender = 1;
	ack.receiver = 0;
	ack.sender_incarnation = 32;
	ack.receiver_incarnation = 31;
	UT_ASSERT(!deliver(&ack));
	node(0);
	cluster_config_prefix_poll(&exchange[0]);
	ack.challenge[0]++;
	UT_ASSERT(!deliver(&ack));
	UT_ASSERT(!exchange[0].mark_acknowledged);
}
UT_TEST(changed_peer_exchange_is_not_combined_with_old_proof)
{
	ClusterConfigPrefixMessage changed;
	reset(1);
	begin_pair();
	complete_pair();
	changed = exchange[0].mark;
	changed.challenge[0]++;
	UT_ASSERT(!deliver(&changed));
	node(1);
	UT_ASSERT(exchange[1].failed && !cluster_config_prefix_complete(&exchange[1]));
}
UT_TEST(native_stream_and_capability_change_invalidate_completion)
{
	for (unsigned bad = 0; bad < 8; bad++) {
		reset(1);
		begin_pair();
		complete_pair();
		node(0);
		switch (bad) {
		case 0:
			serial[0]++;
			break;
		case 1:
			key.epoch++;
			break;
		case 2:
			MyProcPid++;
			break;
		case 3:
			cap_generation++;
			break;
		case 4:
			caps_available = false;
			break;
		case 5:
			MyBackendType = B_BACKEND;
			break;
		case 6:
			ClusterICOps_Active = &other_transport;
			break;
		case 7:
			cluster_interconnect_tier = CLUSTER_IC_TIER_2;
			break;
		}
		UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
		UT_ASSERT(exchange[0].failed);
	}
}
UT_TEST(missing_native_endpoint_or_randomness_refuses_begin)
{
	for (unsigned bad = 0; bad < 7; bad++) {
		reset(0);
		switch (bad) {
		case 0:
			stream_available = false;
			break;
		case 1:
			caps_available = false;
			break;
		case 2:
			random_available = false;
			break;
		case 3:
			IsUnderPostmaster = false;
			break;
		case 4:
			cluster_shared_config = false;
			break;
		case 5:
			ClusterICOps_Active = NULL;
			break;
		case 6:
			ClusterICOps_Active = &other_transport;
			break;
		}
		UT_ASSERT(!cluster_config_prefix_begin(&exchange[0], &key, episode, 1, 31, 32));
		UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
	}
}
UT_TEST(error_and_explicit_dirtying_never_certify_prefix)
{
	sigjmp_buf outer;
	reset(0);
	begin_pair();
	node(0);
	send_error = true;
	PG_exception_stack = &outer;
	if (sigsetjmp(outer, 0) == 0) {
		cluster_config_prefix_poll(&exchange[0]);
		UT_ASSERT(false);
	}
	UT_ASSERT(exchange[0].failed && !cluster_config_prefix_complete(&exchange[0]));
	PG_exception_stack = NULL;
	reset(0);
	begin_pair();
	complete_pair();
	node(0);
	cluster_config_prefix_invalidate(&exchange[0]);
	UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
}
UT_TEST(clear_restarts_with_a_distinct_local_challenge)
{
	uint8 old[16];
	reset(0);
	begin_pair();
	node(0);
	memcpy(old, exchange[0].mark.challenge, sizeof(old));
	UT_ASSERT(!cluster_config_prefix_begin(&exchange[0], &key, episode, 1, 31, 32));
	cluster_config_prefix_clear(&exchange[0]);
	UT_ASSERT(cluster_config_prefix_begin(&exchange[0], &key, episode, 1, 31, 32));
	UT_ASSERT(memcmp(old, exchange[0].mark.challenge, sizeof(old)) != 0);
	UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
}
UT_TEST(ack_queue_refusal_and_hard_error_preserve_ownership)
{
	reset(0);
	begin_pair();
	node(0);
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(deliver(&exchange[1].mark));
	UT_ASSERT_EQ(sends[0], 1); /* ingress cannot send */
	send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(exchange[0].peer_mark_seen && !exchange[0].peer_ack_admitted);
	UT_ASSERT(!exchange[0].failed);
	send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(exchange[0].peer_ack_admitted);
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT_EQ(sends[0], 3);
	UT_ASSERT(!cluster_config_prefix_complete(&exchange[0])); /* no peer ACK */
	reset(0);
	begin_pair();
	node(0);
	send_result = CLUSTER_IC_SEND_HARD_ERROR;
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(exchange[0].failed && !exchange[0].mark_admitted);
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT_EQ(sends[0], 1);
}
UT_TEST(rebind_during_admission_cannot_complete_old_stream)
{
	reset(1);
	begin_pair();
	node(0);
	rebind_during_send = true;
	cluster_config_prefix_poll(&exchange[0]);
	UT_ASSERT(exchange[0].failed && !exchange[0].mark_admitted);
	UT_ASSERT(!cluster_config_prefix_complete(&exchange[0]));
}
UT_TEST(begin_rejects_aliases_and_cannot_mutate_a_live_exchange)
{
	ClusterConfigPrefixExchange before;
	reset(0);
	before = exchange[0];
	UT_ASSERT(
		!cluster_config_prefix_begin(&exchange[0], &exchange[0].mark.key, episode, 1, 31, 32));
	UT_ASSERT(
		!cluster_config_prefix_begin(&exchange[0], &key, exchange[0].mark.episode, 1, 31, 32));
	UT_ASSERT(memcmp(&before, &exchange[0], sizeof(before)) == 0);
	begin_pair();
	node(0);
	before = exchange[0];
	UT_ASSERT(!cluster_config_prefix_begin(&exchange[0], &key, episode, 1, 31, 32));
	UT_ASSERT(memcmp(&before, &exchange[0], sizeof(before)) == 0);
}
int
main(void)
{
	UT_PLAN(17);
	UT_RUN(literal_wire_roundtrip_and_reserved_bytes);
	UT_RUN(malformed_identity_is_not_a_marker);
	UT_RUN(alias_does_not_clobber_input);
	UT_RUN(malformed_carrier_and_null_outputs_are_refused);
	UT_RUN(nonzero_but_different_identity_or_envelope_is_not_a_receipt);
	UT_RUN(both_directions_and_both_planes_are_required);
	UT_RUN(queue_refusal_retains_but_admitted_tail_is_not_resent);
	UT_RUN(duplicate_mark_and_ack_are_idempotent);
	UT_RUN(ack_before_admission_and_wrong_challenge_cannot_complete);
	UT_RUN(changed_peer_exchange_is_not_combined_with_old_proof);
	UT_RUN(native_stream_and_capability_change_invalidate_completion);
	UT_RUN(missing_native_endpoint_or_randomness_refuses_begin);
	UT_RUN(error_and_explicit_dirtying_never_certify_prefix);
	UT_RUN(clear_restarts_with_a_distinct_local_challenge);
	UT_RUN(ack_queue_refusal_and_hard_error_preserve_ownership);
	UT_RUN(rebind_during_admission_cannot_complete_old_stream);
	UT_RUN(begin_rejects_aliases_and_cannot_mutate_a_live_exchange);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
