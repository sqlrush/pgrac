/* Actual diagnostic publisher/reader; fixture supplies only backend state. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/wait_event.h"
#include <assert.h>
#include <stdarg.h>
#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_trace_tx_identity;
int cluster_node_id = 0, MaxBackends = 8, MyProcPid, NBuffers = 48;
TimestampTz MyStartTimestamp;
ProcessingMode Mode = NormalProcessing;
PGPROC *MyProc;
PROC_HDR *ProcGlobal;
BufferDescPadded *BufferDescriptors;
static PGPROC procs[64];
static PROC_HDR proc_header;
static BufferDescPadded descriptors[48];
static LWLock other_lock;
static struct { LWLock *lock; LWLockMode mode; } held[48];
static unsigned nheld, logs;
static char last[8192];
static char pair_request_log[8192];
static unsigned pair_cycle_logs, pair_request_logs;
static const ClusterShmemRegion *registered;
static uint32 native_wait_event;
uint32 *my_wait_event_info = &native_wait_event;

void ExceptionalCondition(const char *condition, const char *file, int line)
{ fprintf(stderr, "%s:%d: %s\n", file, line, condition); abort(); }
Size add_size(Size a, Size b) { assert(a <= SIZE_MAX - b); return a + b; }
Size mul_size(Size a, Size b) { assert(!b || a <= SIZE_MAX / b); return a * b; }
void *ShmemInitStruct(const char *name, Size size, bool *found)
{ (void)name; *found = false; return calloc(1, size); }
void cluster_shmem_register_region(const ClusterShmemRegion *region) { registered = region; }
void ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{ for (unsigned i = 0; i < nheld; i++) callback(held[i].lock, held[i].mode, arg); }
bool errstart(int level, const char *domain) { (void)domain; assert(level == LOG); return true; }
int errmsg_internal(const char *fmt, ...)
{ va_list ap; va_start(ap, fmt); vsnprintf(last, sizeof(last), fmt, ap); va_end(ap); return 0; }
void errfinish(const char *file, int line, const char *func)
{
	(void)file; (void)line; (void)func; logs++;
	if (strstr(last, "event=PAIR_CYCLE ")) pair_cycle_logs++;
	if (strstr(last, "event=PAIR_REQUEST ")) {
		pair_request_logs++; strcpy(pair_request_log, last);
	}
}

#include "../../backend/cluster/cluster_content_trace.c"
#include "test_cluster_content_trace_lw_wait.inc"

static BufferDesc *buf(unsigned i) { return &descriptors[i].bufferdesc; }
static void actor(unsigned i)
{
	memset(&local_owner, 0, sizeof(local_owner));
	pair_depth = 0; pair_recording = false; pair_slot = -1;
	MyProc = &procs[i]; MyProc->pgprocno = i; MyProc->pid = MyProcPid = 100 + i; MyProc->backendId = 1 + i;
	MyStartTimestamp = 1000 + i;
}
static void reset(void)
{
	free(content_trace); content_trace = NULL;
	memset(descriptors, 0, sizeof(descriptors)); memset(procs, 0, sizeof(procs));
	proc_header.allProcs = procs; proc_header.allProcCount = MaxBackends + NUM_AUXILIARY_PROCS;
	ProcGlobal = &proc_header; BufferDescriptors = descriptors; nheld = logs = 0;
	emitted_rows = pair_cycle_logs = pair_request_logs = 0;
	native_wait_event = 0;
	pair_request_log[0] = '\0';
	cluster_trace_tx_identity = true;
	for (unsigned i = 0; i < lengthof(descriptors); i++) {
		buf(i)->buf_id = i; buf(i)->tag.spcOid = 1; buf(i)->tag.dbOid = 2;
		buf(i)->tag.relNumber = 3; buf(i)->tag.forkNum = MAIN_FORKNUM; buf(i)->tag.blockNum = i;
	}
	actor(0); content_trace_init();
	for (unsigned i = 0; i < lengthof(descriptors); i++)
		pg_atomic_init_u32(&buf(i)->state, BM_TAG_VALID);
}
static void hold(unsigned i, LWLockMode mode)
{ held[nheld].lock = &buf(i)->content_lock; held[nheld++].mode = mode; }
static ClusterContentTraceCut cut;

UT_TEST(actual_content_owner)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.matches, 1);
	UT_ASSERT_EQ(cut.owners[0].pid, 100); UT_ASSERT_EQ(cut.owners[0].procno, 0);
	UT_ASSERT_EQ(cut.owners[0].backend_start, 1000); UT_ASSERT_EQ(cut.owners[0].nlocks, 1);
	UT_ASSERT_EQ(cut.owners[0].locks[0].mode, LW_EXCLUSIVE);
}
UT_TEST(all_owner_cut_includes_waiters_without_content_holds)
{
	ClusterWaitTrace wait = {0};
	reset(); hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	actor(1); nheld = 0; wait.tag = buf(2)->tag; wait.tag_known = true;
	cluster_content_trace_wait_set(CLCONTENT_PCM, &wait);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 2); UT_ASSERT_EQ(cut.matches, 2);
	UT_ASSERT_EQ(cut.owners[1].pid, 101); UT_ASSERT_EQ(cut.owners[1].nlocks, 0);
	UT_ASSERT_EQ(cut.owners[1].waits[CLCONTENT_PCM].tag.blockNum, 2);
}
UT_TEST(exact_tag_and_lock_kind)
{
	BufferTag target;
	reset(); hold(0, LW_SHARED);
	held[nheld].lock = &buf(1)->pcm_lock; held[nheld++].mode = LW_EXCLUSIVE;
	held[nheld].lock = &other_lock; held[nheld++].mode = LW_EXCLUSIVE;
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	target = buf(0)->tag; target.forkNum = VISIBILITYMAP_FORKNUM;
	cluster_content_trace_capture(&target, &cut); UT_ASSERT_EQ(cut.matches, 0);
	cluster_content_trace_capture(&buf(0)->tag, &cut); UT_ASSERT_EQ(cut.nowners, 1);
	UT_ASSERT_EQ(cut.owners[0].nlocks, 1); UT_ASSERT_EQ(cut.owners[0].locks[0].mode, LW_SHARED);
	cluster_content_trace_capture(&buf(1)->tag, &cut); UT_ASSERT_EQ(cut.matches, 0);
}
UT_TEST(native_reporter_keeps_exact_space_target_and_mode)
{
	reset(); hold(0, LW_SHARED);
	buf(2)->tag.forkNum = SPACE_FORKNUM;
	MyProc->lwWaitMode = LW_EXCLUSIVE;
	buf(2)->content_lock.tranche = LWTRANCHE_BUFFER_CONTENT;
	LWLockReportWaitStart(&buf(2)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].nlocks, 1);
	UT_ASSERT(cut.owners[0].native_waiting);
	UT_ASSERT_EQ(cut.owners[0].native_wait.buffer, 3);
	UT_ASSERT_EQ(cut.owners[0].native_wait.mode, LW_EXCLUSIVE);
	UT_ASSERT(cut.owners[0].native_wait.tag_known);
	UT_ASSERT_EQ(cut.owners[0].native_wait.tag.forkNum, SPACE_FORKNUM);
	UT_ASSERT_EQ(cut.owners[0].native_wait.tag.blockNum, 2);
	UT_ASSERT_EQ(native_wait_event, PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);
	LWLockReportWaitEnd(); cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(!cut.owners[0].native_waiting); UT_ASSERT_EQ(native_wait_event, 0);
}
UT_TEST(native_free_wait_unknown_tag_and_noncontent)
{
	reset(); MyProc->lwWaitMode = LW_WAIT_UNTIL_FREE;
	pg_atomic_write_u32(&buf(1)->state, 0);
	LWLockReportWaitStart(&buf(1)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].nlocks, 0);
	UT_ASSERT(cut.owners[0].native_waiting);
	UT_ASSERT_EQ(cut.owners[0].native_wait.mode, LW_WAIT_UNTIL_FREE);
	UT_ASSERT(!cut.owners[0].native_wait.tag_known);
	LWLockReportWaitEnd(); LWLockReportWaitStart(&other_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(!cut.owners[0].native_waiting);
	LWLockReportWaitEnd();
}
UT_TEST(enqueue_keys_survive_nested_native_wait)
{
	ClusterResId res = {UINT32_MAX, 22, 33, UINT16_MAX, 0xF2, 1};
	ClusterGrdHolderId holder = {3, 17, 91, 92};
	const ClusterContentTraceEnqueue *wait;
	reset(); cluster_content_trace_enqueue_wait_set(&res, &holder,
		ExclusiveLock, ShareLock, true, 101, 102, 103);
	MyProc->lwWaitMode = LW_SHARED;
	LWLockReportWaitStart(&buf(1)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT(cut.owners[0].enqueue_waiting);
	UT_ASSERT(cut.owners[0].native_waiting);
	wait = &cut.owners[0].enqueue;
	UT_ASSERT_EQ(wait->field1, UINT32_MAX); UT_ASSERT_EQ(wait->field2, 22);
	UT_ASSERT_EQ(wait->field3, 33); UT_ASSERT_EQ(wait->field4, UINT16_MAX);
	UT_ASSERT_EQ(wait->type, 0xF2); UT_ASSERT_EQ(wait->lockmethodid, 1);
	UT_ASSERT_EQ(wait->mode, ExclusiveLock); UT_ASSERT_EQ(wait->current_mode, ShareLock);
	UT_ASSERT(wait->convert); UT_ASSERT_EQ(wait->holder_node, 3);
	UT_ASSERT_EQ(wait->holder_procno, 17); UT_ASSERT_EQ(wait->holder_epoch, 91);
	UT_ASSERT_EQ(wait->holder_request, 92); UT_ASSERT_EQ(wait->request, 101);
	UT_ASSERT_EQ(wait->epoch, 102); UT_ASSERT_EQ(wait->wait_seq, 103);
	LWLockReportWaitEnd(); cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(cut.owners[0].enqueue_waiting); UT_ASSERT(!cut.owners[0].native_waiting);
	cluster_content_trace_enqueue_wait_clear(); cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(!cut.owners[0].enqueue_waiting); UT_ASSERT_EQ(cut.owners[0].enqueue.request, 0);
}
UT_TEST(error_clears_each_active_wait)
{
	ClusterResId res = {0}; ClusterGrdHolderId holder = {0};
	ClusterWaitTrace pcm = {0};
	reset(); cluster_content_trace_enqueue_wait_set(&res, &holder, 8, 0, false, 1, 2, 3);
	cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_EXCLUSIVE);
	cluster_content_trace_wait_set(CLCONTENT_PCM, &pcm);
	cluster_content_trace_error_cleanup(); cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].wait_mask, 0);
	UT_ASSERT(!cut.owners[0].native_waiting); UT_ASSERT(!cut.owners[0].enqueue_waiting);
	UT_ASSERT_EQ(cut.owners[0].enqueue.request, 0); UT_ASSERT_EQ(logs, 0);
}
UT_TEST(release_gap_is_unknown_until_republication)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_lock_invalidating(&other_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut); UT_ASSERT_EQ(cut.matches, 1);
	cluster_content_trace_lock_invalidating(&buf(0)->content_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.changed, 1); UT_ASSERT_EQ(cut.matches, 0);
	nheld = 0; cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.changed, 0); UT_ASSERT_EQ(cut.matches, 0);
}

static void walk_owner(const ClusterContentTraceOwner *owner, void *context)
{
	unsigned *seen = context;
	ContentTraceSlot *slot = &content_trace->slots[owner->procno];
	int32 pid = owner->pid;
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->busy), 0);
	*seen |= 1U << owner->procno;
	/* Callback can alter the source; its owned argument remains intact. */
	memset(&slot->owner, 0, sizeof(slot->owner));
	UT_ASSERT_EQ(owner->pid, pid);
}
UT_TEST(walk_visits_beyond_legacy_cap_without_holding_slot)
{
	ClusterContentTraceWalkStats stats; unsigned seen = 0;
	reset(); hold(0, LW_SHARED);
	for (unsigned i = 0; i < 10; i++) { actor(i); cluster_content_trace_lock_changed(&buf(0)->content_lock); }
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 8); UT_ASSERT_EQ(cut.omitted_owners, 2);
	cluster_content_trace_walk(walk_owner, &seen, &stats);
	UT_ASSERT(stats.available); UT_ASSERT_EQ(stats.owners, 10);
	UT_ASSERT_EQ(stats.scanned, content_trace->nslots);
	UT_ASSERT_EQ(stats.busy + stats.changed + stats.dead, 0);
	UT_ASSERT_EQ(seen, (1U << 10) - 1);
}
UT_TEST(walk_distinguishes_unknown_and_unavailable)
{
	ClusterContentTraceWalkStats stats; unsigned seen = 0;
	reset();
	for (unsigned i = 0; i < 4; i++) { actor(i); cluster_content_trace_native_wait_set(&buf(0)->content_lock, LW_SHARED); }
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	(void)pg_atomic_fetch_add_u64(&content_trace->slots[1].revision, 1);
	procs[2].pid = 0;
	cluster_content_trace_walk(walk_owner, &seen, &stats);
	UT_ASSERT(stats.available); UT_ASSERT_EQ(stats.owners, 1);
	UT_ASSERT_EQ(stats.busy, 1); UT_ASSERT_EQ(stats.changed, 1); UT_ASSERT_EQ(stats.dead, 1);
	UT_ASSERT_EQ(seen, 1U << 3);
	cluster_trace_tx_identity = false;
	cluster_content_trace_walk(walk_owner, &seen, &stats);
	UT_ASSERT(!stats.available); UT_ASSERT_EQ(stats.scanned, 0);
	cluster_trace_tx_identity = true; ProcGlobal = NULL;
	cluster_content_trace_walk(walk_owner, &seen, &stats); UT_ASSERT(!stats.available);
}
UT_TEST(release_removes_owner)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	nheld = 0; cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut); UT_ASSERT_EQ(cut.matches, 0);
}
UT_TEST(nested_wait_keys)
{
	ClusterWaitTrace tx = {0}, pcm = {0};
	reset(); hold(0, LW_EXCLUSIVE); tx.wait_seq = 71; tx.tx_xid = 4195571;
	pcm.tag_known = true; pcm.tag = buf(1)->tag; pcm.request_known = true;
	pcm.request_seq = 99; pcm.formation = 123; pcm.master_session = 456; pcm.site = CLWAIT_SITE_PCM_INSTALL;
	cluster_content_trace_wait_set(CLCONTENT_TX, &tx);
	cluster_content_trace_wait_set(CLCONTENT_PCM, &pcm);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].wait_mask, (1U << CLCONTENT_TX) | (1U << CLCONTENT_PCM));
	UT_ASSERT_EQ(cut.owners[0].waits[CLCONTENT_TX].wait_seq, 71);
	UT_ASSERT_EQ(cut.owners[0].waits[CLCONTENT_PCM].request_seq, 99);
	UT_ASSERT_EQ(cut.owners[0].waits[CLCONTENT_PCM].formation, 123);
	cluster_content_trace_wait_clear(CLCONTENT_PCM);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.owners[0].wait_mask, 1U << CLCONTENT_TX);
	UT_ASSERT_EQ(cut.owners[0].waits_with_locks, 2);
}
UT_TEST(pair_attempt_and_miss)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_pair(2, false);
	cluster_content_trace_pair(2, true); cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].pair_attempts, 1);
	UT_ASSERT_EQ(cut.owners[0].pair_misses, 1); UT_ASSERT_EQ(cut.owners[0].waits[CLCONTENT_PAIR].tag.blockNum, 1);
	cluster_content_trace_wait_clear(CLCONTENT_PAIR);
	cluster_content_trace_capture(&buf(0)->tag, &cut); UT_ASSERT_EQ(cut.owners[0].wait_mask, 0);
}
UT_TEST(busy_cut_never_waits)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.busy, 1); UT_ASSERT_EQ(cut.nowners, 0);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 0);
}
UT_TEST(missed_publish_marks_stale_unknown)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	nheld = 0; cluster_content_trace_lock_changed(&buf(0)->content_lock);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 0);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.changed, 1); UT_ASSERT_EQ(cut.nowners, 0);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut); UT_ASSERT_EQ(cut.changed, 0);
}
UT_TEST(dead_process_not_current_owner)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	procs[0].pid = 0; cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.dead, 1); UT_ASSERT_EQ(cut.nowners, 0);
}
UT_TEST(reused_pid_with_new_backend_is_unknown)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	procs[0].backendId = 2; /* same pid, different backend incarnation */
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.dead, 1); UT_ASSERT_EQ(cut.nowners, 0);
}
UT_TEST(zero_backend_identity_is_unknown)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	procs[0].backendId = 0; /* startup/recycle identity is not stable */
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.dead, 1); UT_ASSERT_EQ(cut.nowners, 0);
}
UT_TEST(bounded_locks_and_owners)
{
	reset();
	for (unsigned i = 0; i < 40; i++) hold(i, LW_SHARED);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].nlocks, CLUSTER_CONTENT_TRACE_LOCKS);
	UT_ASSERT_EQ(cut.owners[0].omitted_locks, 8);
	nheld = 1;
	for (unsigned i = 1; i < 10; i++) { actor(i); cluster_content_trace_lock_changed(&buf(0)->content_lock); }
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.matches, 10); UT_ASSERT_EQ(cut.nowners, CLUSTER_CONTENT_TRACE_OWNERS);
	UT_ASSERT_EQ(cut.omitted_owners, 2);
}
UT_TEST(owned_cut_and_cleanup)
{
	ClusterWaitTrace tx = {0};
	reset(); hold(0, LW_EXCLUSIVE); tx.wait_seq = 51;
	cluster_content_trace_wait_set(CLCONTENT_TX, &tx);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	nheld = 0; cluster_content_trace_error_cleanup();
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].wait_mask, 1U << CLCONTENT_TX);
	cluster_content_trace_capture(&buf(0)->tag, &cut); UT_ASSERT_EQ(cut.matches, 0);
	UT_ASSERT_EQ(local_owner.wait_mask, 0); UT_ASSERT_EQ(logs, 0);
}
UT_TEST(disabled_and_disable_transition)
{
	reset(); cluster_trace_tx_identity = false;
	cluster_content_trace_capture(NULL, &cut); cluster_content_trace_wait_set(CLCONTENT_TX, NULL);
	cluster_content_trace_native_wait_set(&buf(0)->content_lock, LW_EXCLUSIVE);
	cluster_content_trace_enqueue_wait_set(NULL, NULL, 0, 0, false, 0, 0, 0);
	UT_ASSERT(!local_owner.native_waiting); UT_ASSERT(!local_owner.enqueue_waiting);
	UT_ASSERT_EQ(cut.scanned, 0); UT_ASSERT_EQ(logs, 0);
	cluster_trace_tx_identity = true; hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_trace_tx_identity = false; cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_trace_tx_identity = true; cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.matches, 0);
}
UT_TEST(pair_cycle_history_is_bounded)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_pair_begin(1, 2);
	for (unsigned i = 0; i < 6; i++) {
		cluster_content_trace_pair_cycle();
		cluster_content_trace_pair_phase(CLPAIR_CONDITIONAL_SECOND);
		cluster_content_trace_pair(2, true);
		cluster_content_trace_pair_event(2, CLPAIR_CONTENT_BUSY, 71);
	}
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].pair_cycles, 6);
	UT_ASSERT_EQ(cut.owners[0].pair_omitted, 2); UT_ASSERT_EQ(cut.owners[0].npair_cycles, 4);
	UT_ASSERT_EQ(cut.owners[0].pair[0].cycle, 5); UT_ASSERT(cut.owners[0].pair[0].finished);
	UT_ASSERT_EQ(cut.owners[0].pair[1].cycle, 6); UT_ASSERT(!cut.owners[0].pair[1].finished);
	UT_ASSERT(cut.owners[0].pair[1].conditional_miss);
	UT_ASSERT_EQ(cut.owners[0].pair[1].events[1][CLPAIR_CONTENT_BUSY], 1);
	UT_ASSERT_EQ(cut.owners[0].pair[1].last_results[1][CLPAIR_CONTENT_BUSY], 71);
	UT_ASSERT_EQ(cut.owners[0].pair[1].targets[1].tag.blockNum, 1);
	cluster_content_trace_pair_end();
	UT_ASSERT(local_owner.pair[1].finished); UT_ASSERT_EQ(local_owner.pair[1].phase, CLPAIR_DONE);
}
UT_TEST(pair_dispatch_keeps_actual_first_and_latest)
{
	ClusterContentTracePairRequest request = {0};
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_pair_begin(1, 2);
	cluster_content_trace_pair_cycle(); cluster_content_trace_pair_phase(CLPAIR_RESOLVE_SECOND);
	request.tag = buf(1)->tag; request.master = 3; request.requester = 0;
	request.physical_generation = 12; request.r4 = 21; request.formation = 22;
	request.master_session = 23; request.base = 24; request.authority = 25;
	request.requester_connection = 31; request.ingress_connection = 32;
	request.wire_sender_connection = 33; request.action = 2; request.action_node = 3;
	for (unsigned i = 0; i < 4; i++) {
		request.request = 100 + i; request.stage_ok = i % 2 == 0;
		cluster_content_trace_pair_request(2, &request);
	}
	request.tag.blockNum++; cluster_content_trace_pair_request(2, &request);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.owners[0].pair[0].nrequests, 2);
	UT_ASSERT_EQ(cut.owners[0].pair[0].omitted_requests, 2);
	UT_ASSERT_EQ(cut.owners[0].pair[0].requests[0].request, 100);
	UT_ASSERT_EQ(cut.owners[0].pair[0].requests[1].request, 103);
	UT_ASSERT_EQ(cut.owners[0].pair[0].requests[1].physical_generation, 12);
	UT_ASSERT_EQ(cut.owners[0].pair[0].requests[1].ingress_connection, 32);
	UT_ASSERT_EQ(cut.owners[0].pair[0].requests[1].leg, 1);
	UT_ASSERT_EQ(cut.owners[0].pair[0].requests[1].phase, CLPAIR_RESOLVE_SECOND);
	UT_ASSERT_EQ(cut.owners[0].pair[0].events[1][CLPAIR_DISPATCH], 4);
	UT_ASSERT_EQ(cut.owners[0].pair[0].events[1][CLPAIR_STAGE_REJECT], 2);
	UT_ASSERT_EQ(cut.owners[0].pair[0].events[1][CLPAIR_REMOTE_MASTER_DISPATCH], 4);
}
UT_TEST(pair_nested_and_disabled_are_not_attributed)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_trace_tx_identity = false;
	cluster_content_trace_pair_begin(1, 2); cluster_trace_tx_identity = true;
	cluster_content_trace_pair_cycle(); UT_ASSERT(!cluster_content_trace_pair_active(1));
	cluster_content_trace_pair_end(); UT_ASSERT_EQ(local_owner.pair_cycles, 0);
	cluster_content_trace_pair_begin(1, 2); cluster_content_trace_pair_cycle();
	cluster_content_trace_pair_begin(2, 3); cluster_content_trace_pair_cycle();
	cluster_content_trace_pair_event(2, CLPAIR_TARGET_BEGIN, 99);
	UT_ASSERT(!cluster_content_trace_pair_active(2)); cluster_content_trace_pair_end();
	UT_ASSERT(cluster_content_trace_pair_active(1));
	UT_ASSERT_EQ(local_owner.pair_nested_omitted, 1);
	UT_ASSERT_EQ(local_owner.pair[0].events[1][CLPAIR_TARGET_BEGIN], 0);
	cluster_trace_tx_identity = false; cluster_content_trace_pair_event(1, CLPAIR_TARGET_BEGIN, 0);
	cluster_trace_tx_identity = true; UT_ASSERT(!cluster_content_trace_pair_active(1));
	cluster_content_trace_pair_end();
}
UT_TEST(pair_error_cleanup_preserves_owned_reason)
{
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_pair_begin(1, 2);
	cluster_content_trace_pair_cycle(); cluster_content_trace_pair_phase(CLPAIR_RESOLVE_SECOND);
	cluster_content_trace_pair_event(2, CLPAIR_ACTIVATE_WAIT, 7);
	cluster_content_trace_error_cleanup(); cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT(cut.owners[0].pair[0].error); UT_ASSERT(cut.owners[0].pair[0].finished);
	UT_ASSERT_EQ(cut.owners[0].pair[0].phase, CLPAIR_RESOLVE_SECOND);
	UT_ASSERT_EQ(cut.owners[0].pair[0].last_results[1][CLPAIR_ACTIVATE_WAIT], 7);
	UT_ASSERT(!cluster_content_trace_pair_active(2));
	cluster_content_trace_pair_begin(1, 2); cluster_content_trace_pair_cycle();
	UT_ASSERT_EQ(local_owner.pair_invocations, 2); UT_ASSERT_EQ(local_owner.pair_cycles, 1);
	UT_ASSERT(cut.owners[0].pair[0].error); /* owned cut did not change */
}
UT_TEST(pair_post_cleanup_emit_uses_owned_copy)
{
	ClusterWaitTrace guard = {0};
	ClusterContentTracePairRequest request = {0};
	reset(); hold(0, LW_EXCLUSIVE); cluster_content_trace_pair_begin(1, 2);
	cluster_content_trace_pair_cycle(); request.tag = buf(1)->tag;
	request.request = 901; request.physical_generation = 77;
	cluster_content_trace_pair_request(2, &request);
	cluster_content_trace_capture(&buf(0)->tag, &cut);
	nheld = 0; cluster_content_trace_error_cleanup();
	guard.snapshot = 123; guard.tag = buf(0)->tag;
	UT_ASSERT_EQ(logs, 0); cluster_content_trace_emit(&guard, &cut);
	UT_ASSERT_EQ(pair_cycle_logs, 1); UT_ASSERT_EQ(pair_request_logs, 1);
	UT_ASSERT_STR_CONTAINS(pair_request_log, "request=901");
	UT_ASSERT_STR_CONTAINS(pair_request_log, "physical_generation=77");
	UT_ASSERT_STR_CONTAINS(pair_request_log, "current_transfer_known=0");
	emitted_rows = CLUSTER_WAIT_TRACE_LIMIT; cluster_content_trace_emit(&guard, &cut);
	UT_ASSERT_STR_CONTAINS(last, "event=LIMIT");
}
int main(void)
{
	cluster_content_trace_shmem_register(); assert(registered && registered->lwlock_count == 0);
	UT_PLAN(26);
	UT_RUN(actual_content_owner); UT_RUN(exact_tag_and_lock_kind); UT_RUN(release_removes_owner);
	UT_RUN(all_owner_cut_includes_waiters_without_content_holds);
	UT_RUN(native_reporter_keeps_exact_space_target_and_mode);
	UT_RUN(native_free_wait_unknown_tag_and_noncontent);
	UT_RUN(enqueue_keys_survive_nested_native_wait); UT_RUN(error_clears_each_active_wait);
	UT_RUN(release_gap_is_unknown_until_republication);
	UT_RUN(walk_visits_beyond_legacy_cap_without_holding_slot);
	UT_RUN(walk_distinguishes_unknown_and_unavailable);
	UT_RUN(nested_wait_keys); UT_RUN(pair_attempt_and_miss); UT_RUN(busy_cut_never_waits);
	UT_RUN(missed_publish_marks_stale_unknown); UT_RUN(dead_process_not_current_owner);
	UT_RUN(reused_pid_with_new_backend_is_unknown);
	UT_RUN(zero_backend_identity_is_unknown);
	UT_RUN(bounded_locks_and_owners); UT_RUN(owned_cut_and_cleanup); UT_RUN(disabled_and_disable_transition);
	UT_RUN(pair_cycle_history_is_bounded); UT_RUN(pair_dispatch_keeps_actual_first_and_latest);
	UT_RUN(pair_nested_and_disabled_are_not_attributed); UT_RUN(pair_error_cleanup_preserves_owned_reason);
	UT_RUN(pair_post_cleanup_emit_uses_owned_copy);
	UT_DONE(); free(content_trace); return ut_failed_count ? 1 : 0;
}
