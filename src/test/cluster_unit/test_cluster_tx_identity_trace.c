/* Opt-in exact identity diagnostics: exercise the real bounded logger. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_tx_identity_trace.h"
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
static char last[2048];
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
	puts("tx identity trace: 14 checks PASS");
	return 0;
}
