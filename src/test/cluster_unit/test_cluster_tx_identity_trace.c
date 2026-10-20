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

bool errstart(int level, const char *domain) { (void)domain; assert(level == LOG); return true; }
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
	unsigned before;

	/* Disabled instrumentation must not inspect an identity at all. */
	cluster_tx_identity_trace_emit(CLTXID_ACTIVE, NULL);
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
	resource.master_view = true;
	cluster_wait_trace_emit(CLWAIT_PCM_ADMISSION, &resource);
	assert(strstr(last, "event=PCM_ADMISSION") && strstr(last, "master_view=1"));
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
	puts("tx identity / wait resources trace: OFF, identities and independent budgets PASS");
	return 0;
}
