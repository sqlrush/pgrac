/*-------------------------------------------------------------------------
 *
 * test_cluster_config_members.c
 *    Actual member observation codec/collector with transport/startup-profile boundaries.
 *    Native parent/process behavior has separate real-C and native TAP tests.
 *    This program does not certify distributed authentication or admission.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_members.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
BackendType MyBackendType = B_LMON;
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id;
static ClusterSharedConfigRef target;
static ClusterR4MembershipSnapshot live;
static uint32 connections[CLUSTER_MAX_NODES];
static uint64 streams[CLUSTER_MAX_NODES];
static bool stream_available, stream_change_during_profile;
static bool member_available, profile_available, change_during_profile, caps_available, random_ok;
static unsigned sends, captures;
static uint64 nonce_source;
static ClusterICSendResult send_result;
static ClusterConfigMembersMessage sent[CLUSTER_MAX_NODES];
static ClusterConfigMembersObservation observation;
static ClusterICMsgTypeInfo registered;
static bool profile_error, send_error;
static ClusterConfigMountProof mount_proof;
static bool local_profile_available = true, local_profile_changed;

void
cluster_shared_config_mount_publish(const ClusterConfigMountProof *proof)
{
	mount_proof = *proof;
}
bool
cluster_shared_config_mount_observe(ClusterConfigMountProof *proof)
{
	*proof = mount_proof;
	return mount_proof.result != CLUSTER_CONFIG_MOUNT_UNPROVEN;
}
bool
cluster_shared_config_common_profile(ClusterSharedConfigActive *out)
{
	memset(out, 0, sizeof(*out));
	out->version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	out->static_entries = 100;
	out->dynamic_entries = 149;
	memset(out->static_sha256, 0x33, 32);
	memset(out->dynamic_sha256, 0x44, 32);
	if (local_profile_changed)
		out->static_sha256[0] ^= 1;
	return local_profile_available;
}
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	*out = live;
	out->local_self_boot_incarnation = live.admitted_incarnation[cluster_node_id];
	return member_available;
}
uint32
cluster_ic_local_capability_word(void)
{
	return UINT32_C(0x00800000);
}
bool
cluster_sf_peer_capability_word_sample(int32 node, uint32 bits, uint32 *word, uint32 *gen)
{
	*word = caps_available ? UINT32_C(0x00800000) : 0;
	*gen = connections[node];
	return (*word & bits) == bits && *gen != 0;
}
bool
cluster_sf_peer_capability_generation_matches(int32 node, uint32 bits, uint32 gen)
{
	return caps_available && bits == UINT32_C(0x00800000) && connections[node] == gen;
}
/* Explicit transport boundary; actual stream binding/retirement has real
 * TCP tests, not a fabricated capability counter in this collector test. */
bool
cluster_ic_tier1_stream_capture(int32 node, ClusterICTier1Stream *out)
{
	memset(out, 0, sizeof(*out));
	if (!stream_available)
		return false;
	out->peer = node;
	out->serial = streams[node];
	return true;
}
bool
cluster_ic_tier1_stream_current(const ClusterICTier1Stream *stream)
{
	return stream_available && stream->peer >= 0 && stream->peer < CLUSTER_MAX_NODES
		   && stream->serial != 0 && stream->serial == streams[stream->peer];
}
bool
pg_strong_random(void *out, size_t len)
{
	UT_ASSERT_EQ(len, sizeof(nonce_source));
	++nonce_source;
	memcpy(out, &nonce_source, len);
	return random_ok;
}
static ClusterSharedConfigActive
profile_for(unsigned node)
{
	ClusterSharedConfigActive c = { 0 };
	(void)node;
	c.version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	c.static_entries = 100;
	c.dynamic_entries = 149;
	memset(c.static_sha256, 0x33, 32);
	memset(c.dynamic_sha256, 0x44, 32);
	return c;
}
bool
cluster_shared_config_parent_profile(const ClusterSharedConfigRef *ref, int node,
									 ClusterSharedConfigActive *out)
{
	++captures;
	if (profile_error)
		pg_re_throw();
	*out = profile_for(node);
	UT_ASSERT_EQ(ref->identity.system_identifier, target.identity.system_identifier);
	if (change_during_profile)
		++live.formation_epoch;
	if (stream_change_during_profile)
		++streams[3];
	return profile_available;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 len)
{
	UT_ASSERT_EQ(type, 67);
	UT_ASSERT(cluster_config_members_decode(payload, len, &sent[dest]));
	++sends;
	if (send_error)
		pg_re_throw();
	return send_result;
}
void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	registered = *info;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected Assert %s at %s:%d\n", condition, file, line);
	abort();
}
static bool
zero(const void *p, size_t n)
{
	const uint8 *b = p;
	for (size_t i = 0; i < n; i++)
		if (b[i])
			return false;
	return true;
}
static void
put64(uint8 *p, uint64 v)
{
	for (unsigned i = 0; i < 8; i++)
		p[i] = (uint8)(v >> (8 * i));
}
static void
reset(void)
{
	memset(&mount_proof, 0, sizeof(mount_proof));
	local_profile_available = true;
	local_profile_changed = false;
	MyBackendType = B_LMON;
	cluster_config_members_cancel();
	cluster_node_id = 0;
	cluster_enabled = cluster_shared_config = true;
	member_available = profile_available = caps_available = random_ok = true;
	change_during_profile = false;
	stream_available = true;
	stream_change_during_profile = false;
	profile_error = send_error = false;
	send_result = CLUSTER_IC_SEND_DONE;
	sends = captures = 0;
	memset(&live, 0, sizeof(live));
	memset(&target, 0, sizeof(target));
	memset(sent, 0, sizeof(sent));
	memset(&observation, 0, sizeof(observation));
	live.formation_epoch = 7;
	live.admitted_members_lo = 9;
	live.admitted_incarnation[0] = 31;
	live.admitted_incarnation[3] = 34;
	live.local_self_boot_incarnation = 31;
	target.identity.system_identifier = 987654321;
	target.identity.database_incarnation = 2;
	target.identity.generation = 5;
	target.identity.configured[0] = 9;
	memset(target.identity.storage_uuid, 0x11, 16);
	memset(target.identity.authority_uuid, 0x22, 16);
	memset(target.sha256, 0x55, 32);
	for (unsigned i = 0; i < CLUSTER_MAX_NODES; ++i) {
		connections[i] = 1;
		streams[i] = 1;
	}
}
static void
deliver(const ClusterConfigMembersMessage *m, int source, uint64 epoch)
{
	uint8 bytes[CLUSTER_CONFIG_MEMBERS_BYTES];
	ClusterICEnvelope env = { 0 };
	if (!cluster_config_members_encode(m, bytes)) {
		UT_ASSERT(false);
		return;
	}
	env.msg_type = 67;
	env.source_node_id = source;
	env.dest_node_id = cluster_node_id;
	env.epoch = epoch;
	env.payload_length = sizeof(bytes);
	cluster_config_members_ingress(&env, bytes);
}
static ClusterConfigMembersMessage
reply(unsigned node)
{
	ClusterConfigMembersMessage m = sent[node];
	m.verb = CLUSTER_CONFIG_MEMBERS_REPLY;
	m.outcome = CLUSTER_CONFIG_MEMBERS_OBSERVED;
	m.common = profile_for(node);
	return m;
}

UT_TEST(literal_request)
{
	uint8 b[288] = { 0 }, encoded[288];
	ClusterConfigMembersMessage m = { 0 };
	reset();
	memcpy(b, "PCSO", 4);
	b[4] = 3;
	b[6] = 32;
	b[7] = 1;
	b[8] = 1;
	b[16] = 3;
	put64(b + 24, 7);
	put64(b + 32, 31);
	put64(b + 40, 34);
	put64(b + 48, 11);
	put64(b + 56, 987654321);
	put64(b + 64, 2);
	put64(b + 72, 5);
	memset(b + 80, 0x11, 16);
	memset(b + 96, 0x22, 16);
	put64(b + 112, 9);
	memset(b + 128, 0x55, 32);
	put64(b + 160, 9);
	memset(b + 176, 0x66, 32);
	UT_ASSERT(cluster_config_members_decode(b, sizeof(b), &m));
	UT_ASSERT_EQ(m.key.epoch, 7);
	UT_ASSERT_EQ(m.responder, 3);
	UT_ASSERT(memcmp(&m.key.ref, &target, sizeof(target)) == 0);
	UT_ASSERT(cluster_config_members_encode(&m, encoded));
	UT_ASSERT(memcmp(b, encoded, sizeof(b)) == 0);
	for (unsigned i = 0; i < 7; ++i) {
		static const unsigned bad[] = { 0, 4, 6, 20, 208, 284, 287 };
		uint8 before = b[bad[i]];
		b[bad[i]] ^= 4;
		memset(&m, 0xa5, sizeof(m));
		UT_ASSERT(!cluster_config_members_decode(b, sizeof(b), &m));
		UT_ASSERT(zero(&m, sizeof(m)));
		b[bad[i]] = before;
	}
	UT_ASSERT(!cluster_config_members_decode(b, sizeof(b) - 1, &m));
}
UT_TEST(sparse_members_and_retry)
{
	uint64 nonce;
	ClusterConfigMembersMessage m;
	reset();
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 1);
	UT_ASSERT_EQ(sent[3].responder_incarnation, 34);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
	nonce = observation.nonce;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 2);
	UT_ASSERT_EQ(sent[3].nonce, nonce);
	m = reply(3);
	deliver(&m, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 9);
	UT_ASSERT_EQ(observation.node[3].static_entries, 100);
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(sent[3].nonce != nonce);
}
UT_TEST(reply_counts_and_profiles)
{
	uint8 b[288];
	ClusterConfigMembersMessage m, decoded;
	reset();
	cluster_config_members_poll(&target, &live);
	m = reply(3);
	UT_ASSERT(cluster_config_members_encode(&m, b));
	UT_ASSERT_EQ(b[208], CLUSTER_SHARED_CONFIG_COMMON_VERSION);
	UT_ASSERT_EQ(b[212], 100);
	UT_ASSERT_EQ(b[216], 149);
	UT_ASSERT(cluster_config_members_decode(b, sizeof(b), &decoded));
	UT_ASSERT(memcmp(&decoded.common, &m.common, sizeof(m.common)) == 0);
	m.common.version = 1;
	UT_ASSERT(!cluster_config_members_encode(&m, b));
	UT_ASSERT(zero(b, sizeof(b)));
	m = reply(3);
	m.common.static_entries = 0;
	UT_ASSERT(!cluster_config_members_encode(&m, b));
	m = reply(3);
	memset(m.common.dynamic_sha256, 0, 32);
	UT_ASSERT(!cluster_config_members_encode(&m, b));
	m = reply(3);
	m.common.dynamic_entries = CLUSTER_SHARED_CONFIG_MAX_ENTRIES;
	UT_ASSERT(!cluster_config_members_encode(&m, b));
}
UT_TEST(aliases_do_not_clobber)
{
	ClusterConfigMembersMessage m, before;
	reset();
	cluster_config_members_poll(&target, &live);
	m = before = sent[3];
	UT_ASSERT(!cluster_config_members_encode(&m, (uint8 *)&m));
	UT_ASSERT(memcmp(&m, &before, sizeof(m)) == 0);
	UT_ASSERT(!cluster_config_members_decode(&m, 288, &m));
	UT_ASSERT(memcmp(&m, &before, sizeof(m)) == 0);
}
UT_TEST(wrong_identity_cannot_fill_slot)
{
	ClusterConfigMembersMessage final_reply;
	reset();
	cluster_config_members_poll(&target, &live);
	for (unsigned i = 0; i < 10; ++i) {
		ClusterConfigMembersMessage m = reply(3);
		switch (i) {
		case 0:
			++m.nonce;
			break;
		case 1:
			++m.collector_incarnation;
			break;
		case 2:
			++m.responder_incarnation;
			break;
		case 3:
			++m.key.ref.identity.generation;
			break;
		case 4:
			m.key.ref.sha256[0]++;
			break;
		case 5:
			m.key.members_sha256[0]++;
			break;
		case 6:
			++m.key.ref.identity.database_incarnation;
			break;
		case 7:
			++m.key.epoch;
			break;
		case 8:
			m.key.ref.identity.authority_uuid[0]++;
			break;
		case 9:
			m.collector = 3;
			m.responder = 0;
			break;
		}
		deliver(&m, 3, 7);
		UT_ASSERT(cluster_config_members_observe(&observation));
		UT_ASSERT_EQ(observation.observed[0], 1);
	}
	final_reply = reply(3);
	deliver(&final_reply, 0, 7);
	deliver(&final_reply, 3, 8);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
}
UT_TEST(duplicate_is_not_another_member_or_new_observation)
{
	ClusterConfigMembersMessage m;
	reset();
	cluster_config_members_poll(&target, &live);
	m = reply(3);
	deliver(&m, 3, 7);
	m.common.dynamic_sha256[0]++;
	deliver(&m, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 9);
	UT_ASSERT_EQ(observation.node[3].dynamic_sha256[0], 0x44);
}
UT_TEST(unavailable_is_not_observed)
{
	ClusterConfigMembersMessage m;
	reset();
	cluster_config_members_poll(&target, &live);
	m = reply(3);
	m.outcome = CLUSTER_CONFIG_MEMBERS_UNAVAILABLE;
	memset(&m.common, 0, sizeof(m.common));
	deliver(&m, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
	UT_ASSERT_EQ(observation.unavailable[0], 8);
	UT_ASSERT(zero(&observation.node[3], sizeof(observation.node[3])));
}
UT_TEST(send_refusal_never_completes)
{
	reset();
	send_result = CLUSTER_IC_SEND_HARD_ERROR;
	for (int i = 0; i < 10; ++i)
		cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 10);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
}
UT_TEST(member_change_retires_round)
{
	ClusterConfigMembersMessage old;
	reset();
	cluster_config_members_poll(&target, &live);
	old = reply(3);
	++live.admitted_incarnation[3];
	UT_ASSERT(!cluster_config_members_observe(&observation));
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(sent[3].nonce != old.nonce);
	deliver(&old, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
}
UT_TEST(connection_change_retires_round)
{
	ClusterConfigMembersMessage old;
	reset();
	cluster_config_members_poll(&target, &live);
	old = reply(3);
	++connections[3];
	deliver(&old, 3, 7);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(sent[3].nonce != old.nonce);
	deliver(&old, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
}
UT_TEST(ordinary_refresh_does_not_churn_identity)
{
	uint64 nonce;
	reset();
	cluster_config_members_poll(&target, &live);
	nonce = sent[3].nonce;
	++live.observed_generation[3];
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sent[3].nonce, nonce);
}
UT_TEST(native_stream_replacement_retires_same_capability_generation)
{
	ClusterConfigMembersMessage old;
	reset();
	cluster_config_members_poll(&target, &live);
	old = reply(3);
	++streams[3]; /* Same epoch, member and legacy uint32 diagnostic. */
	deliver(&old, 3, 7);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	UT_ASSERT(zero(&observation, sizeof(observation)));
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(sent[3].nonce != old.nonce);
	deliver(&old, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 1);
}
UT_TEST(no_native_stream_is_not_capability_proof)
{
	reset();
	stream_available = false;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(!cluster_config_members_observe(&observation));
}
UT_TEST(native_stream_changed_during_profile_cannot_send)
{
	reset();
	stream_change_during_profile = true;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(!cluster_config_members_observe(&observation));
}
UT_TEST(changed_selection_cancels_old_reply)
{
	ClusterConfigMembersMessage old;
	reset();
	cluster_config_members_poll(&target, &live);
	old = reply(3);
	++target.identity.generation;
	target.sha256[0]++;
	cluster_config_members_poll(&target, &live);
	deliver(&old, 3, 7);
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.key.ref.identity.generation, 6);
	UT_ASSERT_EQ(observation.observed[0], 1);
}
UT_TEST(no_partial_membership_or_missing_capability)
{
	reset();
	target.identity.configured[0] = 1;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	reset();
	caps_available = false;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	reset();
	random_ok = false;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(!cluster_config_members_observe(&observation));
}
UT_TEST(sparse_four_members_include_high_bitmap)
{
	reset();
	live.admitted_members_hi = target.identity.configured[1] = 3;
	live.admitted_incarnation[64] = 164;
	live.admitted_incarnation[65] = 165;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(sends, 3);
	for (unsigned i = 0; i < 3; ++i) {
		unsigned node = i == 0 ? 3 : 63 + i;
		ClusterConfigMembersMessage m = reply(node);
		deliver(&m, node, 7);
	}
	UT_ASSERT(cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(observation.observed[0], 9);
	UT_ASSERT_EQ(observation.observed[1], 3);
}
UT_TEST(profile_identity_change_has_no_reply)
{
	reset();
	change_during_profile = true;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	UT_ASSERT_EQ(sends, 0);
}
UT_TEST(responder_uses_own_selected_target)
{
	ClusterConfigMembersMessage request;
	unsigned before;
	reset();
	cluster_config_members_poll(&target, &live);
	request = sent[3];
	cluster_config_members_cancel();
	cluster_node_id = 3;
	live.local_self_boot_incarnation = 34;
	deliver(&request, 0, 7); /* no local CF selection yet */
	UT_ASSERT_EQ(sends, 1);
	cluster_config_members_poll(&target, &live);
	before = sends;
	deliver(&request, 0, 7);
	UT_ASSERT_EQ(sends, before + 1);
	UT_ASSERT_EQ(sent[0].verb, CLUSTER_CONFIG_MEMBERS_REPLY);
	UT_ASSERT_EQ(sent[0].nonce, request.nonce);
	UT_ASSERT_EQ(sent[0].responder, 3);
	UT_ASSERT_EQ(sent[0].outcome, CLUSTER_CONFIG_MEMBERS_OBSERVED);
	profile_available = false;
	deliver(&request, 0, 7);
	UT_ASSERT_EQ(sent[0].outcome, CLUSTER_CONFIG_MEMBERS_UNAVAILABLE);
	UT_ASSERT(zero(&sent[0].common, sizeof(sent[0].common)));
}
UT_TEST(cancel_and_role_guard)
{
	ClusterConfigMembersMessage old;
	reset();
	cluster_config_members_poll(&target, &live);
	old = reply(3);
	cluster_config_members_cancel();
	deliver(&old, 3, 7);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	MyBackendType = B_BACKEND;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(!cluster_config_members_observe(&observation));
	MyBackendType = B_LMON;
	cluster_shared_config = false;
	cluster_config_members_poll(&target, &live);
	UT_ASSERT(!cluster_config_members_observe(&observation));
}
UT_TEST(register_control_owner)
{
	reset();
	cluster_config_members_register();
	UT_ASSERT_EQ(registered.msg_type, 67);
	UT_ASSERT_EQ(registered.plane, CLUSTER_IC_PLANE_CONTROL);
	UT_ASSERT_EQ(registered.allowed_producer_mask, CLUSTER_IC_PRODUCER_LMON);
	UT_ASSERT(!registered.broadcast_ok);
	UT_ASSERT(registered.handler == cluster_config_members_ingress);
}

UT_TEST(ingress_error_retires_observation)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterConfigMembersMessage request;
		volatile bool caught = false;
		reset();
		cluster_config_members_poll(&target, &live);
		request = sent[3];
		request.collector = 3;
		request.responder = 0;
		request.collector_incarnation = 34;
		request.responder_incarnation = 31;
		profile_error = fault == 0;
		send_error = fault == 1;
		PG_TRY();
		{
			deliver(&request, 3, 7);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(!cluster_config_members_observe(&observation));
		UT_ASSERT(zero(&observation, sizeof(observation)));
		profile_error = send_error = false;
		cluster_config_members_poll(&target, &live);
		UT_ASSERT(cluster_config_members_observe(&observation));
		UT_ASSERT(observation.nonce != request.nonce);
	}
}
UT_TEST(mount_requires_complete_common_values)
{
	ClusterConfigMembersMessage m;
	reset();
	cluster_config_members_poll(&target, &live);
	UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_UNPROVEN);
	m = reply(3);
	m.common.version = CLUSTER_SHARED_CONFIG_COMMON_VERSION;
	deliver(&m, 3, 7);
	UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_MATCH);
}

UT_TEST(mount_pending_is_not_active_mismatch)
{
	ClusterConfigMembersMessage m;
	reset();
	cluster_config_members_poll(&target, &live);
	m = reply(3);
	/* Pending restart counts deliberately do not exist on this wire. */
	deliver(&m, 3, 7);
	UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_MATCH);
}
UT_TEST(mount_mismatch_is_not_missing_evidence)
{
	for (unsigned kind = 0; kind < 2; ++kind) {
		ClusterConfigMembersMessage m;
		reset();
		cluster_config_members_poll(&target, &live);
		m = reply(3);
		if (kind == 0)
			m.common.static_sha256[0] ^= 1;
		else
			m.common.dynamic_sha256[0] ^= 1;
		deliver(&m, 3, 7);
		UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_MISMATCH);
	}
}
UT_TEST(mount_stale_identity_and_missing_profile_are_not_match)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		ClusterConfigMembersMessage m;
		reset();
		cluster_config_members_poll(&target, &live);
		m = reply(3);
		deliver(&m, 3, 7);
		UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_MATCH);
		switch (fault) {
		case 0:
			++live.formation_epoch;
			break;
		case 1:
			++live.admitted_incarnation[3];
			break;
		case 2:
			++live.admitted_incarnation[0];
			break;
		case 3:
			member_available = false;
			break;
		case 4:
			local_profile_available = false;
			break;
		case 5:
			cluster_config_members_cancel();
			break;
		}
		UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_UNPROVEN);
	}
}
UT_TEST(mount_own_value_contradiction_is_mismatch)
{
	ClusterConfigMembersMessage m;
	reset();
	cluster_config_members_poll(&target, &live);
	m = reply(3);
	deliver(&m, 3, 7);
	local_profile_changed = true;
	UT_ASSERT_EQ(cluster_config_members_mount_status(), CLUSTER_CONFIG_MOUNT_MISMATCH);
}

int
main(void)
{
	UT_PLAN(27);
	UT_RUN(mount_pending_is_not_active_mismatch);
	UT_RUN(mount_mismatch_is_not_missing_evidence);
	UT_RUN(mount_stale_identity_and_missing_profile_are_not_match);
	UT_RUN(mount_own_value_contradiction_is_mismatch);
	UT_RUN(mount_requires_complete_common_values);
	UT_RUN(literal_request);
	UT_RUN(sparse_members_and_retry);
	UT_RUN(reply_counts_and_profiles);
	UT_RUN(aliases_do_not_clobber);
	UT_RUN(wrong_identity_cannot_fill_slot);
	UT_RUN(duplicate_is_not_another_member_or_new_observation);
	UT_RUN(unavailable_is_not_observed);
	UT_RUN(send_refusal_never_completes);
	UT_RUN(member_change_retires_round);
	UT_RUN(connection_change_retires_round);
	UT_RUN(native_stream_replacement_retires_same_capability_generation);
	UT_RUN(no_native_stream_is_not_capability_proof);
	UT_RUN(native_stream_changed_during_profile_cannot_send);
	UT_RUN(ordinary_refresh_does_not_churn_identity);
	UT_RUN(changed_selection_cancels_old_reply);
	UT_RUN(no_partial_membership_or_missing_capability);
	UT_RUN(sparse_four_members_include_high_bitmap);
	UT_RUN(profile_identity_change_has_no_reply);
	UT_RUN(responder_uses_own_selected_target);
	UT_RUN(cancel_and_role_guard);
	UT_RUN(register_control_owner);
	UT_RUN(ingress_error_retires_observation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
