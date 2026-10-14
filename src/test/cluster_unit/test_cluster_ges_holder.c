/*-------------------------------------------------------------------------
 * test_cluster_ges_holder.c -- read-only master query, route and boot fences.
 * The codec and query owner run product C; transport and cluster state are
 * deterministic fixtures. Actual GRD ownership is tested by grd_capacity.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "../../backend/cluster/cluster_ges_holder.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
bool cluster_shared_config = true;
int cluster_node_id = 1;
BackendType MyBackendType = B_LMON;
static uint64 epoch = 7, generation = 47, boots[4] = { 100, 101, 102, 103 };
static int32 master = 2;
static bool service = true, member = true, queue_room = true, capability = true;
static bool change_after_read;
static ClusterGrdHolderObservation observed = CLUSTER_GRD_HOLDER_ABSENT;
static int reads, enqueued, sent, delivered;
static ClusterGrdWorkItem queued;
static uint8 wire_sent[80];
static ClusterGesHolderProbe accepted;
static ClusterResId expected_resid = { .field1 = 5,
									   .field2 = 16385,
									   .type = LOCKTAG_RELATION,
									   .lockmethodid = DEFAULT_LOCKMETHOD };
static ClusterGrdHolderId expected_holder = { 3, 21, 7, 901 };

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s:%d %s\n", f, l, c);
	abort();
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
	return service;
}
bool
cluster_authority_readiness_managed(void)
{
	return true;
}
bool
cluster_serving_ready_is_current(void)
{
	return service;
}
int32
cluster_grd_lookup_master_gen(const ClusterResId *r, uint64 *g)
{
	UT_ASSERT(memcmp(r, &expected_resid, sizeof(*r)) == 0);
	*g = generation;
	return master;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return member && node >= 0 && node < 4 ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_DEAD;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return node >= 0 && node < 4 ? boots[node] : 0;
}
bool
cluster_sf_peer_capability_word_sample(int peer, uint32 required, uint32 *word, uint32 *gen)
{
	UT_ASSERT(peer >= 0 && peer < 4);
	*word = capability ? required : 0;
	*gen = 1;
	return capability;
}
bool
cluster_grd_work_queue_enqueue(uint32 source, const void *p, uint16 n)
{
	UT_ASSERT_EQ(n, 80);
	if (!queue_room)
		return false;
	memset(&queued, 0, sizeof(queued));
	queued.source_node_id = source;
	queued.routing_generation = generation;
	queued.payload_len = n;
	memcpy(queued.payload, p, n);
	enqueued++;
	return true;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *p, uint32 n)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GES_REQUEST);
	UT_ASSERT_EQ(n, 80);
	UT_ASSERT(dest != cluster_node_id);
	memcpy(wire_sent, p, n);
	sent++;
	return CLUSTER_IC_SEND_DONE;
}
ClusterGrdHolderObservation
cluster_grd_holder_observe(const ClusterResId *r, const ClusterGrdHolderId *h, uint64 gen,
						   LOCKMODE *mode)
{
	UT_ASSERT_EQ(cluster_node_id, master);
	UT_ASSERT_EQ(gen, generation);
	UT_ASSERT(memcmp(r, &expected_resid, sizeof(*r)) == 0);
	UT_ASSERT(memcmp(h, &expected_holder, sizeof(*h)) == 0);
	reads++;
	*mode = observed == CLUSTER_GRD_HOLDER_PRESENT ? AccessExclusiveLock : NoLock;
	if (change_after_read)
		boots[1]++;
	return observed;
}
void
cluster_ko_peer_holder_observed(int32 source, const ClusterGesHolderProbe *p)
{
	UT_ASSERT_EQ(source, master);
	accepted = *p;
	delivered++;
}

static ClusterGesHolderProbe
setup(void)
{
	ClusterGesHolderProbe q = { 0 };
	epoch = 7;
	generation = 47;
	master = 2;
	cluster_node_id = 1;
	for (int i = 0; i < 4; i++)
		boots[i] = 100 + i;
	service = member = queue_room = capability = true;
	change_after_read = false;
	reads = enqueued = sent = delivered = 0;
	MyBackendType = B_LMON;
	observed = CLUSTER_GRD_HOLDER_ABSENT;
	q.resid = expected_resid;
	q.holder = expected_holder;
	q.nonce = 1001;
	q.master_generation = 47;
	q.collector_boot = 101;
	q.master_boot = 102;
	return q;
}
static ClusterICEnvelope
env_for(int source)
{
	ClusterICEnvelope e = { 0 };
	e.source_node_id = source;
	e.epoch = epoch;
	e.payload_length = 80;
	return e;
}

UT_TEST(query_round_trip_is_read_only_and_correlated)
{
	ClusterGesHolderProbe q = setup(), decoded;
	ClusterICEnvelope env;
	uint8 request[80];
	UT_ASSERT(cluster_ges_holder_probe_send(master, &q));
	UT_ASSERT_EQ(sent, 1);
	memcpy(request, wire_sent, 80);
	UT_ASSERT(cluster_ges_holder_probe_decode(request, 80, &decoded));
	UT_ASSERT_EQ(decoded.nonce, 1001);
	UT_ASSERT_EQ(decoded.holder.request_id, 901);
	cluster_node_id = 2;
	env = env_for(1);
	cluster_ges_holder_probe_ingress(&env, request);
	UT_ASSERT_EQ(enqueued, 1);
	UT_ASSERT_EQ(reads, 0);
	cluster_ges_holder_probe_drain(&queued);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(sent, 2);
	cluster_node_id = 1;
	env = env_for(2);
	cluster_ges_holder_probe_ingress(&env, wire_sent);
	UT_ASSERT_EQ(delivered, 1);
	UT_ASSERT_EQ(accepted.observation, CLUSTER_GRD_HOLDER_ABSENT);
	UT_ASSERT_EQ(accepted.nonce, 1001);
	UT_ASSERT_EQ(accepted.mode, NoLock);
}
UT_TEST(query_local_master_uses_same_work_owner)
{
	ClusterGesHolderProbe q = setup();
	master = cluster_node_id;
	q.master_boot = boots[master];
	UT_ASSERT(cluster_ges_holder_probe_send(master, &q));
	UT_ASSERT_EQ(enqueued, 1);
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(sent, 0);
	observed = CLUSTER_GRD_HOLDER_PRESENT;
	cluster_ges_holder_probe_drain(&queued);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(delivered, 1);
	UT_ASSERT_EQ(accepted.mode, AccessExclusiveLock);
}
UT_TEST(query_never_turns_failed_lookup_or_refusal_into_absence)
{
	ClusterGesHolderProbe q = setup();
	ClusterICEnvelope env;
	UT_ASSERT(cluster_ges_holder_probe_send(master, &q));
	cluster_node_id = 2;
	env = env_for(1);
	cluster_ges_holder_probe_ingress(&env, wire_sent);
	observed = CLUSTER_GRD_HOLDER_UNKNOWN;
	cluster_ges_holder_probe_drain(&queued);
	cluster_node_id = 1;
	env = env_for(2);
	cluster_ges_holder_probe_ingress(&env, wire_sent);
	UT_ASSERT_EQ(delivered, 1);
	UT_ASSERT_EQ(accepted.observation, CLUSTER_GRD_HOLDER_UNKNOWN);
	q = setup();
	queue_room = false;
	master = 1;
	q.master_boot = 101;
	UT_ASSERT(!cluster_ges_holder_probe_send(master, &q));
	UT_ASSERT_EQ(reads, 0);
	service = false;
	UT_ASSERT(!cluster_ges_holder_probe_send(master, &q));
	UT_ASSERT_EQ(delivered, 0);
	q = setup();
	capability = false;
	UT_ASSERT(!cluster_ges_holder_probe_send(master, &q));
	UT_ASSERT_EQ(sent, 0);
	capability = true;
	MyBackendType = B_BACKEND;
	UT_ASSERT(!cluster_ges_holder_probe_send(master, &q));
	UT_ASSERT_EQ(sent, 0);
}
UT_TEST(query_rejects_boot_epoch_master_and_generation_drift)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterGesHolderProbe q = setup();
		ClusterICEnvelope env;
		UT_ASSERT(cluster_ges_holder_probe_send(master, &q));
		cluster_node_id = 2;
		env = env_for(1);
		cluster_ges_holder_probe_ingress(&env, wire_sent);
		UT_ASSERT_EQ(enqueued, 1);
		if (fault == 0)
			boots[1]++;
		if (fault == 1)
			boots[2]++;
		if (fault == 2)
			epoch++;
		if (fault == 3)
			generation++;
		if (fault == 4)
			master = 0;
		if (fault == 5)
			member = false;
		if (fault == 6)
			change_after_read = true;
		cluster_ges_holder_probe_drain(&queued);
		UT_ASSERT_EQ(reads, fault == 6 ? 1 : 0);
		UT_ASSERT_EQ(sent, 1);
		UT_ASSERT_EQ(delivered, 0);
	}
}
UT_TEST(query_reply_rechecks_consumer_cut)
{
	ClusterGesHolderProbe q = setup();
	ClusterICEnvelope env = env_for(2);
	uint8 bytes[80];
	q.reply = true;
	q.observation = CLUSTER_GRD_HOLDER_ABSENT;
	UT_ASSERT(cluster_ges_holder_probe_encode(&q, bytes, sizeof(bytes)));
	boots[1]++;
	cluster_ges_holder_probe_ingress(&env, bytes);
	UT_ASSERT_EQ(delivered, 0);
	boots[1]--;
	env.source_node_id = 3;
	cluster_ges_holder_probe_ingress(&env, bytes);
	UT_ASSERT_EQ(delivered, 0);
	env.source_node_id = 2;
	env.epoch++;
	cluster_ges_holder_probe_ingress(&env, bytes);
	UT_ASSERT_EQ(delivered, 0);
}
UT_TEST(query_codec_rejects_bad_shape_and_preserves_full_identity)
{
	ClusterGesHolderProbe q = setup(), out;
	uint8 bytes[80], copy[80];
	UT_ASSERT(cluster_ges_holder_probe_encode(&q, bytes, sizeof(bytes)));
	UT_ASSERT_EQ(bytes[0], 20);
	UT_ASSERT_EQ(bytes[4], 1);
	UT_ASSERT_EQ(bytes[8], 5);
	UT_ASSERT_EQ(bytes[24], 3);
	UT_ASSERT_EQ(bytes[28], 21);
	UT_ASSERT_EQ(bytes[32], 7);
	UT_ASSERT_EQ(bytes[40], 133);
	UT_ASSERT_EQ(bytes[41], 3);
	UT_ASSERT_EQ(bytes[48], 233);
	UT_ASSERT_EQ(bytes[49], 3);
	UT_ASSERT_EQ(bytes[56], 47);
	UT_ASSERT_EQ(bytes[64], 101);
	UT_ASSERT_EQ(bytes[72], 102);
	for (int fault = 0; fault < 8; fault++) {
		memcpy(copy, bytes, 80);
		if (fault == 0)
			copy[4] = 2;
		if (fault == 1)
			copy[7] = 1;
		if (fault == 2)
			copy[5] = 2; /* request claiming a result */
		if (fault == 3)
			copy[6] = AccessExclusiveLock;
		if (fault == 4)
			memset(copy + 48, 0, 8);
		if (fault == 5)
			copy[22] = LOCKTAG_PAGE;
		if (fault == 6)
			memset(copy + 64, 0, 8);
		if (fault == 7)
			memset(copy + 40, 255, 8);
		UT_ASSERT(!cluster_ges_holder_probe_decode(copy, 80, &out));
	}
	UT_ASSERT(!cluster_ges_holder_probe_decode(bytes, 79, &out));
	q.reply = true;
	q.observation = CLUSTER_GRD_HOLDER_PRESENT;
	UT_ASSERT(!cluster_ges_holder_probe_encode(&q, bytes, sizeof(bytes)));
	q.mode = AccessExclusiveLock;
	UT_ASSERT(cluster_ges_holder_probe_encode(&q, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_ges_holder_probe_decode(bytes, 80, &out));
	UT_ASSERT_EQ(out.observation, CLUSTER_GRD_HOLDER_PRESENT);
	UT_ASSERT_EQ(out.mode, AccessExclusiveLock);
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(query_round_trip_is_read_only_and_correlated);
	UT_RUN(query_local_master_uses_same_work_owner);
	UT_RUN(query_never_turns_failed_lookup_or_refusal_into_absence);
	UT_RUN(query_rejects_boot_epoch_master_and_generation_drift);
	UT_RUN(query_reply_rechecks_consumer_cut);
	UT_RUN(query_codec_rejects_bad_shape_and_preserves_full_identity);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
