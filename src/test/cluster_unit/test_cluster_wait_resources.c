/* Exercise the extracted production TX resource collector, not a second model. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_wait_trace.h"
#include "utils/hsearch.h"
#include <assert.h>
#include <stdio.h>
#undef fprintf

typedef struct PrivateRefCountEntry { Buffer buffer; int32 refcount; } PrivateRefCountEntry;
typedef struct ClusterPcmXWriterLedgerEntry {
	int phase; int32 buffer_id; LWLock *content_lock;
	ResourceXWriterUseContext authority; ClusterPcmOwnSnapshot granted;
} ClusterPcmXWriterLedgerEntry;
#define PCM_X_WRITER_LEDGER_UNUSED 0
static PrivateRefCountEntry PrivateRefCountArray[8], overflow[48];
static HTAB *PrivateRefCountHash;
static int32 PrivateRefCountOverflowed;
static ClusterPcmXWriterLedgerEntry cluster_bufmgr_pcm_x_writer_ledger[8];
static BufferDesc buffers[64];
static int32 local_refs[2];
int NLocBuffer = 2;
int32 *LocalRefCount = local_refs;
bool cluster_trace_tx_identity;
void ExceptionalCondition(const char *condition, const char *file, int line) {
	fprintf(stderr, "%s:%d: %s\n", file, line, condition); abort();
}
static unsigned reads, locks, emits, hash_cursor, hash_terminated;
static ClusterWaitTrace observed[CLUSTER_WAIT_TRACE_RESOURCES + 1];
static ClusterWaitTraceKind kinds[CLUSTER_WAIT_TRACE_RESOURCES + 1];
static BufferDesc *test_descriptor(int id) { assert(id >= 0 && id < 64); reads++; return &buffers[id]; }
static uint32 test_lock(BufferDesc *buf) { locks++; return pg_atomic_read_u32(&buf->state); }
static void test_unlock(BufferDesc *buf, uint32 state) { (void)buf; (void)state; assert(locks == 1); locks--; }
static bool test_held(LWLock *lock) { return lock == &buffers[0].content_lock; }
static void cluster_pcm_own_snapshot_post_state_locked(BufferDesc *buf, uint32 state, ClusterPcmOwnSnapshot *out) {
	assert(locks == 1); memset(out, 0, sizeof(*out)); out->tag = buf->tag;
	out->semantic_buf_state = state; out->pcm_state = buf->pcm_state;
	out->generation = 71; out->reservation_token = 72; out->flags = 73;
	out->writer_activation_token = 74; out->resource_x_activation_generation = 75;
}
static void test_hash_init(HASH_SEQ_STATUS *status, HTAB *tab) { (void)status; assert(tab); hash_cursor = 0; }
static void *test_hash_search(HASH_SEQ_STATUS *status) { (void)status; return hash_cursor < (unsigned)PrivateRefCountOverflowed ? &overflow[hash_cursor++] : NULL; }
static void test_hash_term(HASH_SEQ_STATUS *status) { (void)status; hash_terminated++; }
uint64 cluster_wait_trace_begin(ClusterWaitTraceKind kind) { assert(kind == CLWAIT_TX_SUMMARY); return cluster_trace_tx_identity ? 19 : 0; }
void cluster_wait_trace_emit(ClusterWaitTraceKind kind, const ClusterWaitTrace *row) {
	assert(locks == 0 && emits < lengthof(observed)); kinds[emits] = kind; observed[emits++] = *row;
}
#define GetBufferDescriptor test_descriptor
#define LockBufHdr test_lock
#define UnlockBufHdr test_unlock
#define LWLockHeldByMe test_held
#define hash_seq_init test_hash_init
#define hash_seq_search test_hash_search
#define hash_seq_term test_hash_term
#include "test_cluster_wait_resources.inc"

int main(void) {
	ClusterWaitTrace *row;
	/* OFF must not touch even deliberately invalid backend references. */
	PrivateRefCountArray[0].buffer = INT_MAX; PrivateRefCountArray[0].refcount = 1;
	cluster_buffer_trace_tx_wait(CLWAIT_SITE_TX_TARGET, 31, 4195393, 0);
	assert(reads == 0 && emits == 0);
	cluster_trace_tx_identity = true;
	PrivateRefCountArray[0].buffer = 1; PrivateRefCountArray[0].refcount = 3;
	buffers[0].tag.spcOid = 1; buffers[0].tag.dbOid = 2; buffers[0].tag.relNumber = 3;
	buffers[0].tag.forkNum = VISIBILITYMAP_FORKNUM; buffers[0].tag.blockNum = 17;
	buffers[0].pcm_state = 2; pg_atomic_init_u32(&buffers[0].state, BM_TAG_VALID | 5);
	local_refs[1] = 2;
	cluster_bufmgr_pcm_x_writer_ledger[0].phase = 3;
	cluster_bufmgr_pcm_x_writer_ledger[0].buffer_id = 0;
	cluster_bufmgr_pcm_x_writer_ledger[0].content_lock = &buffers[0].content_lock;
	cluster_bufmgr_pcm_x_writer_ledger[0].granted.tag = buffers[0].tag;
	cluster_bufmgr_pcm_x_writer_ledger[0].granted.generation = 71;
	cluster_buffer_trace_tx_wait(CLWAIT_SITE_TX_TARGET, 31, 4195393, 0);
	assert(emits == 3 && kinds[0] == CLWAIT_TX_BUFFER && kinds[1] == CLWAIT_TX_LEDGER);
	row = &observed[0]; assert(row->tag_known && row->tag.blockNum == 17 && row->tag.forkNum == VISIBILITYMAP_FORKNUM);
	assert(row->private_pins == 3 && row->shared_pins == 5 && row->content_held);
	assert(row->own_generation == 71 && row->reservation == 72 && row->own_flags == 73);
	assert(row->writer_activation == 74 && row->rx_activation == 75 && row->wait_seq == 31 && row->tx_xid == 4195393);
	assert(observed[1].ledger_phase == 3 && observed[1].content_held);
	assert(observed[2].pins_seen == 1 && observed[2].ledger_seen == 1 && observed[2].local_pins == 2 && observed[2].omitted == 0);
	/* Overflow hash is copied before logging; a bounded cut reports omitted rows. */
	memset(PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	PrivateRefCountHash = (HTAB *)1; PrivateRefCountOverflowed = 40;
	for (int i = 0; i < 40; i++) { overflow[i].buffer = i + 1; overflow[i].refcount = 1; }
	emits = 0; cluster_buffer_trace_tx_wait(CLWAIT_SITE_GES_TX, 32, 42, 99);
	assert(emits == CLUSTER_WAIT_TRACE_RESOURCES + 1 && hash_terminated == 1);
	row = &observed[emits - 1]; assert(row->pins_seen == 40 && row->omitted == 8 && row->rows == 32);
	assert(row->request_known && row->request_seq == 99 && row->tx_xid == 42);
	puts("TX resource collector: OFF, pin/hash/ledger identity, lock release and bounded cut PASS");
	return 0;
}
