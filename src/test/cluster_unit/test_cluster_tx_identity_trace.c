/* Opt-in exact identity diagnostics: exercise the real bounded logger. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_tx_identity_trace.h"
#include "cluster/cluster_wait_trace.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include <stdarg.h>
#undef snprintf
#undef vsnprintf
#include <assert.h>
#include <stdio.h>
#include <string.h>

bool cluster_trace_tx_identity = false;
int cluster_node_id = 2;
int MyProcPid = 1234;
PGPROC *MyProc = NULL;
static char last[8192];
static unsigned logs;
static bool forbid_ereport;

bool errstart(int level, const char *domain) { (void)domain; assert(!forbid_ereport); assert(level == LOG); return true; }
void write_stderr(const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(last, sizeof(last), fmt, ap);
	va_end(ap);
	logs++;
}
int errmsg_internal(const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(last, sizeof(last), fmt, ap);
	va_end(ap);
	return 0;
}
void errfinish(const char *file, int line, const char *func) {
	(void)file; (void)line; (void)func; logs++;
}

int main(void) {
	ClusterTxIdentityTrace row = {0};
	ClusterTTStatusKey key = {0};
	ClusterTxLocator locator = {0};
	ClusterWaitTrace resource = {0};
	ClusterWaitSendGate send = {0};
	ClusterWaitRequesterGate requester = {0};
	unsigned before;

	/* Disabled instrumentation must not inspect an identity at all. */
	cluster_tx_identity_trace_emit(CLTXID_ACTIVE, NULL);
	cluster_wait_trace_send_gate_emit(NULL, NULL);
	cluster_wait_trace_requester_gate_emit(NULL);
	requester.known = true;
	cluster_wait_trace_requester_gate_emit(&requester);
	requester.known = false;
	assert(logs == 0);
	cluster_trace_tx_identity = true;
	cluster_tx_identity_trace_canonical(&row, 2, 4196258, 513, 24, 0, 0, true, 77);
	cluster_tx_identity_trace_emit(CLTXID_ACTIVE, &row);
	assert(logs == 1 && strstr(last, "event=ACTIVE") != NULL);
	assert(strstr(last, "tt_known=1 tt_segment=513 tt_slot=24 tt_wrap=0 generation_known=1 tt_generation=0") != NULL);

	key.origin_node_id = 2; key.local_xid = 4196258; key.undo_segment_id = 514;
	key.tt_slot_id = 25; key.cluster_epoch = 77;
	cluster_tx_identity_trace_source(&row, &key);
	cluster_tx_identity_trace_emit(CLTXID_TERMINAL, &row);
	assert(strstr(last, "tt_known=0") != NULL && strstr(last, "generation_known=0") != NULL);
	assert(strstr(last, "source_known=1 source_segment=514 source_slot_id=25") != NULL);

	locator.xid = key.local_xid; locator.uba.raw[0] = UINT64CONST(0x2010000000a);
	locator.uba.raw[1] = UINT64CONST(0x180009); locator.tt_wrap = 23;
	locator.itl_kind = 1; locator.itl_slot_index = 4;
	cluster_tx_identity_trace_target(&row, &locator, 2, 77);
	row.wait_seq = 42;
	cluster_tx_identity_trace_emit(CLTXID_WAIT_TARGET, &row);
	assert(strstr(last, "tt_known=0") != NULL && strstr(last, "target_known=1") != NULL);
	assert(strstr(last, "itl_wrap=23") != NULL && strstr(last, "wait_seq=42") != NULL);
	assert(strstr(last, "uba0=000002010000000a uba1=0000000000180009") != NULL);

	/* Origin maps are separate from requester ITL wrap and are deduplicated. */
	row.tt_known = true; row.tt_segment = 513; row.tt_slot = 24; row.tt_wrap = 1;
	row.generation_known = true; row.tt_generation = 0;
	cluster_tx_identity_trace_emit(CLTXID_ORIGIN_MAP, &row);
	before = logs;
	cluster_tx_identity_trace_emit(CLTXID_ORIGIN_MAP, &row);
	assert(logs == before);
	row.tt_wrap = 2;
	cluster_tx_identity_trace_emit(CLTXID_ORIGIN_MAP, &row);
	assert(logs == before + 1 && strstr(last, "tt_wrap=2") != NULL);

	/* A family cannot overwrite/consume the other families' budgets. */
	for (unsigned i = 0; i < CLUSTER_TX_IDENTITY_TRACE_LIMIT + 2; i++)
		cluster_tx_identity_trace_emit(CLTXID_RESERVE, &row);
	assert(strstr(last, "event=LIMIT family=RESERVE") != NULL);
	before = logs;
	cluster_tx_identity_trace_emit(CLTXID_WAIT_TARGET, &row);
	assert(logs == before + 1);
	cluster_trace_tx_identity = false;
	cluster_tx_identity_trace_emit(CLTXID_TERMINAL, NULL);
	assert(logs == before + 1);

	/* The new resource families are passive, independently bounded and OFF. */
	before = logs;
	assert(cluster_wait_trace_begin(CLWAIT_PCM_WAIT) == 0);
	assert(cluster_wait_trace_begin(CLWAIT_PCM_GUARD) == 0);
	cluster_wait_trace_emit(CLWAIT_TX_BUFFER, NULL);
	cluster_wait_trace_emit(CLWAIT_PCM_GUARD, NULL);
	assert(logs == before);
	cluster_trace_tx_identity = true;
	resource.snapshot = cluster_wait_trace_begin(CLWAIT_PCM_WAIT);
	resource.tag_known = true;
	resource.tag.spcOid = 11; resource.tag.dbOid = 22;
	resource.tag.relNumber = 33; resource.tag.forkNum = VISIBILITYMAP_FORKNUM;
	resource.tag.blockNum = 44;
	resource.site = CLWAIT_SITE_PCM_EXECUTOR;
	resource.wait_seq = 77; resource.request_seq = 99;
	resource.request_known = true; resource.requester = 2;
	resource.mode = 2;
	resource.expected_master = 1; resource.x_holder = 3;
	resource.entry_known = true; resource.s_holders = 5; resource.pi_holders = 8;
	resource.pending_requester = 2; resource.binding = 101;
	resource.formation = 102; resource.master_session = 103;
	resource.private_pins = 2; resource.shared_pins = 4;
	resource.content_held = true; resource.ledger_phase = 3;
	resource.omitted = 7;
	resource.wire_sender_connection = 104;
	resource.entry_formation = 105; resource.entry_acquisition = 106; resource.entry_base = 107;
	resource.round_request_known = true;
	resource.round_formation = 108; resource.round_session = 109; resource.round_r4 = 110;
	resource.round_base = 111; resource.round_requester_connection = 112;
	resource.round_ingress_connection = 113; resource.round_master = 1;
	resource.head_request_known = true;
	resource.head_formation = 114; resource.head_session = 115; resource.head_base = 116;
	resource.head_requester_connection = 117; resource.head_ingress_connection = 118;
	resource.wait_head_change = 119; resource.head_change = 120;
	resource.caller_matches_round = true; resource.caller_matches_head = false;
	resource.continuation_known = true;
	resource.continuation_pending = 121; resource.continuation_expected_x = 122;
	resource.continuation_reservation = 123; resource.continuation_claim_pending = 124;
	resource.continuation_claim_reservation = 125;
	resource.guard = CLWAIT_GUARD_ACTIVATION; resource.guard_result = 2;
	resource.refusal_reason = 3; resource.refusal_token = 126;
	resource.refusal_flags = 4; resource.refusal_pins = 5; resource.refusal_io = true;
	resource.transport_known = true;
	resource.blocking_instance = 4; resource.blocking_session = 9; resource.blocking_pid = 5555;
	resource.blocking_known = true;
	resource.final_blocking_instance = 5; resource.final_blocking_session = 10; resource.final_blocking_pid = 6666;
	resource.final_blocking_known = true;
	resource.master_holder_instance = 4; resource.master_holder_session = 9; resource.master_holder_pid = 5555;
	resource.master_holder_known = true;
	resource.master_request = 130; resource.master_head_request = 131;
	resource.master_head_known = true; resource.master_request_head_known = true;
	cluster_wait_trace_emit(CLWAIT_PCM_WAIT, &resource);
	assert(strstr(last, "event=PCM_WAIT") && strstr(last, "actor_node=2 pid=1234"));
	assert(strstr(last, "tag_known=1 spc=11 db=22 rel=33 fork=2 block=44"));
	assert(strstr(last, "wait_seq=77 tx_xid=0 request_known=1 request_seq=99"));
	assert(strstr(last, "mode=2 master_view=0 expected_master=1 requester=2"));
	assert(strstr(last, "x_holder=3 s_holders=00000005 pi_holders=00000008"));
	assert(strstr(last, "private_pins=2 shared_pins=4 content_held=1 ledger_phase=3"));
	assert(strstr(last, "omitted=7 atomic_graph=0"));
	assert(strstr(last, "transport_known=1 wire_sender_conn=104"));
	assert(strstr(last, "entry_formation=105 entry_acquisition=106 entry_base=107"));
	assert(strstr(last, "round_known=1 round_formation=108 round_session=109 round_r4=110 round_base=111 round_master=1"));
	assert(strstr(last, "round_requester_conn=112 round_ingress_conn=113 caller_matches_round=1"));
	assert(strstr(last, "head_known=1 head_formation=114 head_session=115 head_base=116"));
	assert(strstr(last, "head_requester_conn=117 head_ingress_conn=118 caller_matches_head=0"));
	assert(strstr(last, "wait_head_change=119 head_change=120"));
	assert(strstr(last, "continuation_known=1 continuation_pending=121 continuation_expected_x=122"));
	assert(strstr(last, "continuation_reservation=123 continuation_claim_pending=124 continuation_claim_reservation=125"));
	assert(strstr(last, "guard=3 guard_result=2 refusal_reason=3 refusal_token=126"));
	assert(strstr(last, "refusal_flags=00000004 refusal_pins=5 refusal_io=1"));
	assert(strstr(last, "blocking_instance=4 blocking_session=9 blocking_pid=5555 blocking_known=1"));
	assert(strstr(last, "final_blocking_instance=5 final_blocking_session=10 final_blocking_pid=6666 final_blocking_known=1"));
	assert(strstr(last, "master_holder_instance=4 master_holder_session=9 master_holder_pid=5555 master_holder_known=1"));
	assert(strstr(last, "master_request=130 master_head_request=131 master_head_known=1 master_request_head_known=1"));
	/* Unknown projections must remain explicit sentinels on every emitter. */
	resource.blocking_known = resource.final_blocking_known = resource.master_holder_known = false;
	resource.master_head_known = resource.master_request_head_known = false;
	before = logs;
	cluster_wait_trace_emit(CLWAIT_TX_SUMMARY, &resource);
	assert(logs == before + 1);
	assert(strstr(last, "blocking_instance=-1 blocking_session=-1 blocking_pid=-1 blocking_known=0"));
	assert(strstr(last, "final_blocking_instance=-1 final_blocking_session=-1 final_blocking_pid=-1 final_blocking_known=0"));
	assert(strstr(last, "master_holder_instance=-1 master_holder_session=-1 master_holder_pid=-1 master_holder_known=0"));
	assert(strstr(last, "master_request=0 master_head_request=0 master_head_known=0 master_request_head_known=0"));
	resource.master_view = true;
	cluster_wait_trace_emit(CLWAIT_PCM_ADMISSION, &resource);
	assert(strstr(last, "event=PCM_ADMISSION") && strstr(last, "master_view=1"));
	/* A later queue projection alone is not an admission-to-head receipt. */
	assert(strstr(last, "admission_bridge_known=0 admission_r4=0 admitted_base=0 admitted_head=0 admitted_request=0"));
	resource.admission = (ClusterWaitAdmissionBridge) {
		.known = true, .r4 = 132, .base = 133, .head = 134,
		.request_seq = 135, .enqueue_order = 136,
		.requester_connection = 0, .ingress_connection = 137,
		.phase = 2, .source = 3
	};
	cluster_wait_trace_emit(CLWAIT_PCM_ADMISSION, &resource);
	assert(strstr(last, "admission_bridge_known=1 admission_r4=132 admitted_base=133 admitted_head=134 admitted_request=135"));
	assert(strstr(last, "admitted_order=136 admitted_requester_conn=0 admitted_ingress_conn=137 admitted_phase=2 admitted_source=3"));
	/* Unknown must suppress stale payload, rather than fill a missing bridge. */
	resource.admission.known = false;
	cluster_wait_trace_emit(CLWAIT_PCM_ADMISSION, &resource);
	assert(strstr(last, "admission_bridge_known=0 admission_r4=0 admitted_base=0 admitted_head=0 admitted_request=0"));
	for (unsigned i = 0; i < CLUSTER_WAIT_TRACE_LIMIT + 2; i++)
		cluster_wait_trace_emit(CLWAIT_PCM_WAIT, &resource);
	assert(strstr(last, "event=LIMIT family=PCM_WAIT") && strstr(last, "evidence_complete=0"));
	assert(cluster_wait_trace_begin(CLWAIT_PCM_WAIT) == 0);
	before = logs;
	cluster_wait_trace_emit(CLWAIT_TX_SUMMARY, &resource);
	assert(logs == before + 1 && strstr(last, "event=TX_SUMMARY"));
	before = logs;
	resource.snapshot = cluster_wait_trace_begin(CLWAIT_PCM_GUARD);
	assert(resource.snapshot != 0);
	cluster_wait_trace_emit(CLWAIT_PCM_GUARD, &resource);
	assert(logs == before + 1 && strstr(last, "event=PCM_GUARD"));
	for (unsigned i = 0; i < CLUSTER_WAIT_TRACE_LIMIT + 2; i++)
		cluster_wait_trace_emit(CLWAIT_PCM_GUARD, &resource);
	assert(strstr(last, "event=LIMIT family=PCM_GUARD") && strstr(last, "evidence_complete=0"));
	assert(cluster_wait_trace_begin(CLWAIT_PCM_GUARD) == 0);
	before = logs;
	cluster_wait_trace_emit(CLWAIT_TX_SUMMARY, &resource);
	assert(logs == before + 1 && strstr(last, "event=TX_SUMMARY"));
	cluster_wait_trace_emit(CLWAIT_NKINDS, &resource);
	cluster_wait_trace_emit(CLWAIT_TX_BUFFER, NULL);
	assert(logs == before + 1);
	before = logs;
	cluster_wait_trace_send_gate_emit(&resource.tag, &send);
	assert(logs == before); /* Unknown is not a known send decision. */
	send.known = true; send.owner = 6; send.state = 1;
	send.reason = CLWAIT_SEND_RETRY_AGE; send.observed_us = 500001;
	send.last_attempt_us = 1; send.logical_generation = 41;
	forbid_ereport = true;
	cluster_wait_trace_emit(CLWAIT_PCM_SEND_GATE, &resource);
	assert(logs == before); /* This family has only the non-throwing emitter. */
	cluster_wait_trace_send_gate_emit(&resource.tag, &send);
	assert(logs == before + 1 && strstr(last, "event=PCM_SEND_GATE"));
	assert(strstr(last, "send_gate_known=1 send_owner=6 send_state=1 send_reason=5"));
	assert(strstr(last, "send_observed_us=500001 send_last_attempt_us=1"));
	for (unsigned i = 0; i < CLUSTER_WAIT_TRACE_LIMIT + 2; i++)
		cluster_wait_trace_send_gate_emit(&resource.tag, &send);
	assert(strstr(last, "event=LIMIT family=PCM_SEND_GATE") && strstr(last, "evidence_complete=0"));
	before = logs;
	cluster_wait_trace_requester_gate_emit(&requester);
	assert(logs == before);
	cluster_wait_trace_emit(CLWAIT_PCM_REQUESTER_GATE, &resource);
	assert(logs == before); /* This family also forbids ereport. */
	requester.known = true; requester.tag = resource.tag;
	requester.site = CLWAIT_REQUESTER_ROUND;
	requester.reason = CLWAIT_SEND_PHYSICAL_OWNER;
	requester.pending = 3; requester.episode = 17; requester.r4 = 77;
	requester.request_seq = 31; requester.requester_connection = 51;
	requester.master_ingress = 61; requester.observed_us = 1000000;
	requester.completed_us = 9; requester.response_us = 10;
	cluster_wait_trace_requester_gate_emit(&requester);
	assert(logs == before + 1 && strstr(last, "event=PCM_REQUESTER_GATE"));
	assert(strstr(last, "requester_gate_known=1 requester_site=3 requester_reason=3"));
	assert(strstr(last, "r4=77 request_seq=31") && strstr(last, "pending=3 episode=17"));
	assert(strstr(last, "requester_conn=51 ingress_conn=61"));
	assert(strstr(last, "observed_us=1000000 completed_us=9 response_us=10"));
	for (unsigned i = 0; i < CLUSTER_WAIT_TRACE_LIMIT + 2; i++)
		cluster_wait_trace_requester_gate_emit(&requester);
	assert(strstr(last, "event=LIMIT family=PCM_REQUESTER_GATE") && strstr(last, "evidence_complete=0"));
	forbid_ereport = false;
	puts("tx identity / wait resources trace: OFF, identities and independent budgets PASS");
	return 0;
}
