/* PGRAC: real native checkpoint owner with explicit I/O/scheduler boundaries.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "access/xlog.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/buf_internals.h"
#include "storage/procsignal.h"
#include "utils/resowner_private.h"
#include "utils/wait_event.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#define BUF_WRITTEN 0x01
#define BUF_REUSABLE 0x02
#define NB 4

int NBuffers = NB, checkpoint_flush_after;
bool cluster_shared_config = true;
static BufferDescPadded descriptors[NB];
BufferDescPadded *BufferDescriptors = descriptors;
static ConditionVariableMinimallyPadded cvs[NB];
ConditionVariableMinimallyPadded *BufferIOCVArray = cvs;
static CkptSortItem items[NB];
CkptSortItem *CkptBufferIds = items;
ResourceOwner CurrentResourceOwner;
CheckpointStatsData CheckpointStats;
PgStat_CheckpointerStats PendingCheckpointerStats;
volatile sig_atomic_t ProcSignalBarrierPending, InterruptPending;
volatile uint32 InterruptHoldoffCount, QueryCancelHoldoffCount, CritSectionCount;
static Latch latch;
Latch *MyLatch = &latch;
static sigjmp_buf error_boundary;
static bool allow_pin[NB], retained[NB], content[NB];
static unsigned pins[NB], flushes[NB], probe[NB], waits, absorbs, writebacks;
static unsigned release_at;
static bool cancel_wait, peer_write, recycle_peer, error_write, retain_on_lock, fresh_dirty;

static int SyncOneBuffer(int buf_id, bool skip, WritebackContext *wb);
static void TerminateBufferIO(BufferDesc *buf, bool clear, uint32 flags);
#include "test_cluster_checkpoint_buffers_types.inc"
static int ts_ckpt_progress_comparator(Datum a, Datum b, void *arg);

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s %s:%d\n", c, f, l);
	abort();
}
void *
palloc(Size n)
{
	void *p = malloc(n);
	Assert(p != NULL);
	return p;
}
void *
repalloc(void *p, Size n)
{
	p = realloc(p, n);
	Assert(p != NULL);
	return p;
}
void
pfree(void *p)
{
	free(p);
}

static void
assert_unheld(void)
{
	int i;
	for (i = 0; i < NB; i++) {
		UT_ASSERT_EQ(pins[i], 0);
		UT_ASSERT(!content[i]);
		UT_ASSERT((pg_atomic_read_u32(&descriptors[i].bufferdesc.state) & BM_LOCKED) == 0);
	}
}

uint32
LockBufHdr(BufferDesc *buf)
{
	uint32 s = pg_atomic_read_u32(&buf->state);
	Assert((s & BM_LOCKED) == 0);
	pg_atomic_write_u32(&buf->state, s | BM_LOCKED);
	return s | BM_LOCKED;
}
static void
ReservePrivateRefCountEntry(void)
{}
void
ResourceOwnerEnlargeBuffers(ResourceOwner o pg_attribute_unused())
{}
static void
PinBuffer_Locked(BufferDesc *buf)
{
	uint32 s = pg_atomic_read_u32(&buf->state);
	UT_ASSERT(s & BM_LOCKED);
	pins[buf->buf_id]++;
	UnlockBufHdr(buf, s);
}
static void
UnpinBuffer(BufferDesc *buf)
{
	UT_ASSERT_EQ(pins[buf->buf_id], 1);
	pins[buf->buf_id]--;
}
bool
LWLockAcquire(LWLock *l, LWLockMode mode)
{
	int i;
	for (i = 0; i < NB; i++)
		if (l == BufferDescriptorGetContentLock(GetBufferDescriptor(i))) {
			UT_ASSERT(mode == LW_SHARED && pins[i] == 1 && !content[i]);
			content[i] = true;
			if (retain_on_lock && i == 0)
				retained[i] = true;
			return true;
		}
	abort();
}
void
LWLockRelease(LWLock *l)
{
	int i;
	for (i = 0; i < NB; i++)
		if (l == BufferDescriptorGetContentLock(GetBufferDescriptor(i))) {
			UT_ASSERT(content[i]);
			content[i] = false;
			return;
		}
	abort();
}
static bool
cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked(BufferDesc *b, uint32 s)
{
	UT_ASSERT(s & BM_LOCKED);
	return retained[b->buf_id];
}
static bool
cluster_bufmgr_pcm_aux_pin_admission_locked(BufferDesc *b)
{
	UT_ASSERT(pg_atomic_read_u32(&b->state) & BM_LOCKED);
	probe[b->buf_id]++;
	return allow_pin[b->buf_id];
}
void
ResourceOwnerForgetBufferIO(ResourceOwner o pg_attribute_unused(), Buffer b)
{
	UT_ASSERT(pins[b - 1] == 1);
}
void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{}
static void
FlushBuffer(BufferDesc *b, SMgrRelation r pg_attribute_unused(), IOObject object, IOContext context)
{
	uint32 s = LockBufHdr(b);
	UT_ASSERT(pins[b->buf_id] == 1 && content[b->buf_id]);
	UT_ASSERT(object == IOOBJECT_RELATION && context == IOCONTEXT_NORMAL);
	if (error_write)
		siglongjmp(error_boundary, 2);
	flushes[b->buf_id]++;
	UnlockBufHdr(b, (s | BM_IO_IN_PROGRESS) & ~BM_JUST_DIRTIED);
	TerminateBufferIO(b, true, 0);
}
void
WritebackContextInit(WritebackContext *w, int *max)
{
	w->max_pending = max;
	w->nr_pending = 0;
}
void
ScheduleBufferTagForWriteback(WritebackContext *w pg_attribute_unused(), IOContext c,
							  BufferTag *t pg_attribute_unused())
{
	UT_ASSERT_EQ(c, IOCONTEXT_NORMAL);
	assert_unheld();
	writebacks++;
}
void
IssuePendingWritebacks(WritebackContext *w pg_attribute_unused(), IOContext c)
{
	UT_ASSERT_EQ(c, IOCONTEXT_NORMAL);
	assert_unheld();
}
static int
fixture_cmp(const void *aa, const void *bb)
{
	const CkptSortItem *a = aa, *b = bb;
	return (a->tsId > b->tsId) - (a->tsId < b->tsId);
}
static void
sort_checkpoint_bufferids(CkptSortItem *it, size_t n)
{
	qsort(it, n, sizeof(*it), fixture_cmp);
}
void
CheckpointWriteDelay(int flags pg_attribute_unused(), double progress)
{
	UT_ASSERT(progress >= 0 && progress <= 1);
	assert_unheld();
	if (fresh_dirty)
		pg_atomic_fetch_or_u32(&GetBufferDescriptor(3)->state, BM_DIRTY | BM_JUST_DIRTIED);
}
void
AbsorbSyncRequests(void)
{
	assert_unheld();
	absorbs++;
}
void
ProcessProcSignalBarrier(void)
{
	assert_unheld();
	ProcSignalBarrierPending = false;
}
void
ProcessInterrupts(void)
{
	assert_unheld();
	siglongjmp(error_boundary, 1);
}
void
ResetLatch(Latch *l)
{
	UT_ASSERT(l == MyLatch);
	assert_unheld();
}
int
WaitLatch(Latch *l, int e, long timeout, uint32 event)
{
	int i;
	UT_ASSERT(l == MyLatch && (e & WL_EXIT_ON_PM_DEATH) && timeout > 0);
	UT_ASSERT_EQ(event, WAIT_EVENT_CHECKPOINT_WRITE_DELAY);
	assert_unheld();
	waits++;
	Assert(waits < 10);
	/* An unrelated eligible page must already progress before any repoll. */
	UT_ASSERT_EQ(flushes[1], 1);
	if (cancel_wait) {
		InterruptPending = true;
		return WL_LATCH_SET;
	}
	if (waits >= release_at) {
		for (i = 0; i < NB; i++) {
			allow_pin[i] = true;
			retained[i] = false;
		}
		retain_on_lock = false;
		if (peer_write) {
			BufferDesc *b = GetBufferDescriptor(0);
			uint32 s = LockBufHdr(b);
			pins[0]++;
			UnlockBufHdr(b, (s | BM_IO_IN_PROGRESS) & ~BM_JUST_DIRTIED);
			TerminateBufferIO(b, true, 0);
			pins[0]--;
			if (recycle_peer) {
				b->tag.blockNum += 100;
				pg_atomic_fetch_or_u32(&b->state, BM_DIRTY | BM_JUST_DIRTIED);
			}
		}
	}
	return WL_TIMEOUT;
}

#include "test_cluster_checkpoint_buffers.inc"

static void
fixture(void)
{
	int i;
	memset(descriptors, 0, sizeof(descriptors));
	memset(pins, 0, sizeof(pins));
	memset(content, 0, sizeof(content));
	memset(retained, 0, sizeof(retained));
	memset(probe, 0, sizeof(probe));
	memset(flushes, 0, sizeof(flushes));
	memset(&CheckpointStats, 0, sizeof(CheckpointStats));
	memset(&PendingCheckpointerStats, 0, sizeof(PendingCheckpointerStats));
	memset(items, 0, sizeof(items));
	for (i = 0; i < NB; i++) {
		BufferDesc *b = GetBufferDescriptor(i);
		b->buf_id = i;
		b->tag.spcOid = 1663 + i % 2;
		b->tag.dbOid = 5;
		b->tag.relNumber = 17000;
		b->tag.blockNum = i;
		pg_atomic_init_u32(&b->state, BM_VALID | BM_TAG_VALID | BM_PERMANENT
										  | (i < 2 ? BM_DIRTY | BM_JUST_DIRTIED : 0));
		allow_pin[i] = true;
	}
	cluster_shared_config = true;
	cancel_wait = peer_write = recycle_peer = error_write = retain_on_lock = fresh_dirty = false;
	waits = absorbs = writebacks = 0;
	release_at = 2;
	InterruptPending = ProcSignalBarrierPending = false;
	InterruptHoldoffCount = QueryCancelHoldoffCount = CritSectionCount = 0;
}
static bool
needed(int i)
{
	return (pg_atomic_read_u32(&GetBufferDescriptor(i)->state) & BM_CHECKPOINT_NEEDED) != 0;
}
UT_TEST(test_aux_refusal_owned_until_write)
{
	fixture();
	allow_pin[0] = false;
	BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT(!needed(0) && !needed(1));
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT(absorbs >= waits);
	UT_ASSERT_EQ(flushes[0], 1);
	UT_ASSERT_EQ(flushes[1], 1);
	UT_ASSERT_EQ(CheckpointStats.ckpt_bufs_written, 2);
	assert_unheld();
}
UT_TEST(test_retain_after_pin_releases_before_wait)
{
	fixture();
	retain_on_lock = true;
	BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT(!needed(0));
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(flushes[0], 1);
	assert_unheld();
}
UT_TEST(test_peer_write_and_retag_not_new_obligation)
{
	fixture();
	allow_pin[0] = false;
	peer_write = recycle_peer = fresh_dirty = true;
	BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT(!needed(0));
	UT_ASSERT(pg_atomic_read_u32(&GetBufferDescriptor(0)->state) & BM_DIRTY);
	UT_ASSERT(pg_atomic_read_u32(&GetBufferDescriptor(3)->state) & BM_DIRTY);
	UT_ASSERT_EQ(flushes[0], 0);
	UT_ASSERT_EQ(flushes[3], 0);
	UT_ASSERT_EQ(CheckpointStats.ckpt_bufs_written, 1);
}
UT_TEST(test_cancel_never_completes_obligation)
{
	int escaped;
	fixture();
	allow_pin[0] = false;
	cancel_wait = true;
	escaped = sigsetjmp(error_boundary, 0);
	if (!escaped)
		BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT_EQ(escaped, 1);
	UT_ASSERT(needed(0));
	UT_ASSERT_EQ(flushes[0], 0);
	assert_unheld();
}
UT_TEST(test_legacy_pass_and_complete_profile)
{
	fixture();
	cluster_shared_config = false;
	allow_pin[0] = false;
	BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT(needed(0));
	UT_ASSERT_EQ(waits, 0);
	fixture();
	BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT(!needed(0) && !needed(1));
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT_EQ(CheckpointStats.ckpt_bufs_written, 2);
}
UT_TEST(test_io_error_cannot_complete)
{
	int escaped;
	fixture();
	error_write = true;
	escaped = sigsetjmp(error_boundary, 0);
	if (!escaped)
		BufferSync(CHECKPOINT_IMMEDIATE);
	UT_ASSERT_EQ(escaped, 2);
	UT_ASSERT(needed(0) && needed(1));
	UT_ASSERT_EQ(CheckpointStats.ckpt_bufs_written, 0);
	/* Real PG abort owns pin/lock cleanup after I/O ERROR; not mocked as success. */
}
int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_aux_refusal_owned_until_write);
	UT_RUN(test_retain_after_pin_releases_before_wait);
	UT_RUN(test_peer_write_and_retag_not_new_obligation);
	UT_RUN(test_cancel_never_completes_obligation);
	UT_RUN(test_legacy_pass_and_complete_profile);
	UT_RUN(test_io_error_cannot_complete);
	UT_DONE();
	return ut_failed_count != 0;
}
