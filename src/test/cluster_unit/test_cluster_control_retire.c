/*-------------------------------------------------------------------------
 * test_cluster_control_retire.c -- production retirement service and registry.
 * Formation, transport admission and the GRD callback are boundary fixtures.
 * The GRD atomic mutation is separately exercised by test_cluster_grd.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int control_registry_main(void);
#define main control_registry_main
#include "test_cluster_control_request.c"
#undef main

#include "cluster/cluster_control_retire.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_startup_phase.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/timestamp.h"

BackendType MyBackendType = B_LMON;
int cluster_node_id = 1;
static uint64 now_epoch;
static uint64 now_generation;
static int32 now_master;
static bool current_transport, quorum, accept;
static TimestampTz now_us;
static int sent, enqueued, retired;
static ClusterICSendResult send_result;
static ClusterControlRetireMessage last;
static ClusterGrdWorkItem queued;
static PGPROC processes[16];
static PROC_HDR process_header;
PROC_HDR *ProcGlobal = &process_header;
static unsigned owner_wakes;

void
SetLatch(Latch *latch)
{
	UT_ASSERT(latch == &processes[12].procLatch);
	owner_wakes++;
}

uint64
cluster_epoch_get_current(void)
{
	return now_epoch;
}
int32
cluster_grd_lookup_master_gen(const ClusterResId *resid pg_attribute_unused(), uint64 *generation)
{
	*generation = now_generation;
	return now_master;
}
bool
cluster_authority_readiness_managed(void)
{
	return true;
}
bool
cluster_serving_ready_is_current(void)
{
	return current_transport;
}
bool
cluster_recovery_transport_components_current(void)
{
	return current_transport;
}
bool
cluster_recovery_transport_is_current(void)
{
	return current_transport;
}
bool
cluster_qvotec_in_quorum(void)
{
	return quorum;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return now_us;
}
void
cluster_lmon_wakeup(void)
{}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	static ClusterNodeInfo info;
	return node >= 0 && node < 4 ? &info : NULL;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GES_REQUEST);
	UT_ASSERT(destination >= 0 && destination < 4);
	UT_ASSERT(cluster_control_retire_decode(payload, length, &last));
	sent++;
	return send_result;
}

bool
cluster_grd_work_queue_enqueue(uint32 source, const void *payload, uint16 length)
{
	if (!accept)
		return false;
	UT_ASSERT(length <= sizeof(queued.payload));
	queued.source_node_id = source;
	queued.routing_generation = now_generation;
	queued.payload_len = length;
	memcpy(queued.payload, payload, length);
	enqueued++;
	return true;
}

ClusterControlRetireVerb
cluster_ges_control_retire_at_master(const ClusterControlRetireMessage *message,
									 const ClusterControlRequestCut *observed)
{
	UT_ASSERT_EQ(message->cleanup_epoch, now_epoch);
	UT_ASSERT_EQ(observed->master, cluster_node_id);
	UT_ASSERT_EQ(observed->generation, now_generation);
	retired++;
	return CLUSTER_CONTROL_RETIRED;
}

static ClusterControlRequestHandle
service_reset(void)
{
	ClusterControlRequestHandle handle;

	reset();
	now_epoch = cut.epoch;
	now_generation = cut.generation;
	now_master = cut.master;
	cluster_node_id = 1;
	MyBackendType = B_LMON;
	current_transport = quorum = accept = true;
	now_us = 1000000;
	sent = enqueued = retired = 0;
	owner_wakes = 0;
	process_header.allProcs = processes;
	process_header.allProcCount = lengthof(processes);
	processes[12].pid = owner.pid;
	send_result = CLUSTER_IC_SEND_DONE;
	memset(&last, 0, sizeof(last));
	cluster_control_retire_lmon_start();
	handle = registration(0);
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	return handle;
}

static void
ingress(const ClusterControlRetireMessage *message, uint32 source)
{
	uint8 wire[CLUSTER_CONTROL_RETIRE_BYTES];
	ClusterICEnvelope envelope;

	memset(&envelope, 0, sizeof(envelope));
	envelope.source_node_id = source;
	envelope.epoch = now_epoch;
	envelope.payload_length = sizeof(wire);
	UT_ASSERT(cluster_control_retire_encode(message, wire));
	cluster_control_retire_ingress(&envelope, wire);
}

UT_TEST(send_refusal_and_missing_ack_keep_owned_retry)
{
	ClusterControlRequestHandle handle = service_reset();
	ClusterControlRetireMessage first;
	ClusterControlRequestView view;

	send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_control_retire_lmon_tick();
	UT_ASSERT_EQ(sent, 1);
	first = last;
	UT_ASSERT(cluster_control_request_snapshot(&handle, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	now_us += 1000000;
	send_result = CLUSTER_IC_SEND_DONE;
	cluster_control_retire_lmon_tick();
	UT_ASSERT_EQ(sent, 2);
	UT_ASSERT_EQ(last.exchange_id, first.exchange_id);
	now_us += 1000000;
	cluster_control_retire_lmon_tick();
	UT_ASSERT_EQ(sent, 3);
	last.verb = CLUSTER_CONTROL_RETIRED;
	ingress(&last, 2);
	UT_ASSERT(cluster_control_request_forget(&handle, &owner, &cut));
}

UT_TEST(ingress_cannot_bypass_fifo_or_authentication)
{
	ClusterControlRetireMessage request;

	(void)service_reset();
	cluster_control_retire_lmon_tick();
	request = last;
	cluster_node_id = 2;
	accept = false;
	ingress(&request, 1);
	UT_ASSERT_EQ(enqueued, 0);
	UT_ASSERT_EQ(retired, 0);
	accept = true;
	ingress(&request, 3);
	UT_ASSERT_EQ(enqueued, 0);
	ingress(&request, 1);
	UT_ASSERT_EQ(enqueued, 1);
	UT_ASSERT_EQ(retired, 0);
	cluster_control_retire_drain(&queued);
	UT_ASSERT_EQ(retired, 1);
	UT_ASSERT_EQ(last.verb, CLUSTER_CONTROL_RETIRED);
}

UT_TEST(drain_revalidates_cut_and_does_not_accept_old_ack)
{
	ClusterControlRequestHandle handle = service_reset();
	ClusterControlRequestView view;
	ClusterControlRetireMessage request;

	cluster_control_retire_lmon_tick();
	request = last;
	cluster_node_id = 2;
	ingress(&request, 1);
	now_generation++;
	cluster_control_retire_drain(&queued);
	UT_ASSERT_EQ(retired, 0);
	cluster_node_id = 1;
	request.verb = CLUSTER_CONTROL_RETIRED;
	ingress(&request, 2);
	UT_ASSERT(cluster_control_request_snapshot(&handle, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	current_transport = false;
	now_us += 1000000;
	cluster_control_retire_lmon_tick();
	UT_ASSERT_EQ(retired, 0);
}

UT_TEST(local_master_uses_queue_and_exact_ack_too)
{
	ClusterControlRequestHandle handle = service_reset();

	now_master = cluster_node_id;
	cut.master = now_master;
	cluster_control_retire_lmon_tick();
	UT_ASSERT_EQ(sent, 0);
	UT_ASSERT_EQ(enqueued, 1);
	UT_ASSERT_EQ(retired, 0);
	cluster_control_retire_drain(&queued);
	UT_ASSERT_EQ(retired, 1);
	UT_ASSERT(cluster_control_request_forget(&handle, &owner, &cut));
}

UT_TEST(final_send_gate_rejects_queued_abandoned_and_recycled_identity)
{
	ClusterControlRequestHandle handle;
	GesRequestPayload frame;

	reset();
	handle = registration(0);
	memset(&frame, 0, sizeof(frame));
	frame.opcode = GES_REQ_OPCODE_REQUEST;
	frame.lockmode = ExclusiveLock;
	frame.holder_node_id = key.holder.node_id;
	frame.holder_procno = key.holder.procno;
	frame.holder_cluster_epoch_lo = key.holder.cluster_epoch;
	frame.holder_request_id_lo = key.holder.request_id;
	memcpy(frame.resid, &key.resid, sizeof(key.resid));
	UT_ASSERT(
		cluster_control_retire_outbound_allowed(PGRAC_IC_MSG_GES_REQUEST, &frame, sizeof(frame)));
	frame.lockmode = ShareLock;
	UT_ASSERT(
		!cluster_control_retire_outbound_allowed(PGRAC_IC_MSG_GES_REQUEST, &frame, sizeof(frame)));
	frame.lockmode = ExclusiveLock;
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	UT_ASSERT(
		!cluster_control_retire_outbound_allowed(PGRAC_IC_MSG_GES_REQUEST, &frame, sizeof(frame)));
	frame.holder_request_id_lo++;
	UT_ASSERT(
		!cluster_control_retire_outbound_allowed(PGRAC_IC_MSG_GES_REQUEST, &frame, sizeof(frame)));
	frame.opcode = GES_REQ_OPCODE_RELEASE;
	UT_ASSERT(
		cluster_control_retire_outbound_allowed(PGRAC_IC_MSG_GES_REQUEST, &frame, sizeof(frame)));
}

UT_TEST(only_exact_ack_wakes_the_original_owner)
{
	ClusterControlRequestHandle handle = service_reset();
	ClusterControlRetireMessage reply;

	cluster_control_retire_lmon_tick();
	reply = last;
	reply.verb = CLUSTER_CONTROL_RETIRED;
	reply.exchange_id++;
	ingress(&reply, cut.master);
	UT_ASSERT_EQ(owner_wakes, 0);
	reply.exchange_id--;
	ingress(&reply, cut.master);
	UT_ASSERT_EQ(owner_wakes, 1);
	UT_ASSERT(cluster_control_request_forget(&handle, &owner, &cut));
	ingress(&reply, cut.master);
	UT_ASSERT_EQ(owner_wakes, 1);
	(void)service_reset();
	cluster_control_retire_lmon_tick();
	last.verb = CLUSTER_CONTROL_RETIRED;
	processes[12].pid++; /* No signal to a new process's recycled slot. */
	ingress(&last, cut.master);
	UT_ASSERT_EQ(owner_wakes, 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(send_refusal_and_missing_ack_keep_owned_retry);
	UT_RUN(ingress_cannot_bypass_fifo_or_authentication);
	UT_RUN(drain_revalidates_cut_and_does_not_accept_old_ack);
	UT_RUN(local_master_uses_queue_and_exact_ack_too);
	UT_RUN(final_send_gate_rejects_queued_abandoned_and_recycled_identity);
	UT_RUN(only_exact_ack_wakes_the_original_owner);
	UT_DONE();
	free(memory);
	return ut_failed_count ? 1 : 0;
}
