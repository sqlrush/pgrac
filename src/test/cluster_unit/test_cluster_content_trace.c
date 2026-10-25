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

static unsigned deadline_procs, deadline_targets, deadline_ends;
int PostPortNumber;

#include "../../backend/cluster/cluster_content_trace.c"
#include "test_cluster_content_trace_lw_wait.inc"

static BufferDesc *buf(unsigned i) { return &descriptors[i].bufferdesc; }
static void actor(unsigned i)
{
	memset(&local_owner, 0, sizeof(local_owner));
	pair_depth = 0; pair_recording = false; pair_slot = -1;
	service_serial = service_current = 0;
	lifetime_serial = 0; memset(held_lifetimes, 0, sizeof(held_lifetimes));
	held_lifetime_count = 0;
	MyProc = &procs[i]; MyProc->pgprocno = i; MyProc->pid = MyProcPid = 100 + i; MyProc->backendId = 1 + i;
	MyStartTimestamp = 1000 + i;
	MyProc->cluster_grd_generation = 10000 + i;
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

static ClusterContentTraceRejection rejected[8];
static unsigned nrejected;

static void
remember_rejection(const ClusterContentTraceRejection *row, void *context)
{
	unsigned *count = context;
	UT_ASSERT(*count < lengthof(rejected));
	/* BUSY belongs to someone else; a copied payload was already released. */
	UT_ASSERT_EQ(pg_atomic_read_u32(&content_trace->slots[row->slot].busy),
		row->reason == CLCONTENT_REJECT_BUSY ? 1 : 0);
	rejected[(*count)++] = *row;
}

static void
count_owner(const ClusterContentTraceOwner *owner, void *context)
{
	unsigned *count = context;
	UT_ASSERT_EQ(pg_atomic_read_u32(&content_trace->slots[owner->procno].busy), 0);
	(*count)++;
}

UT_TEST(process_start_initializes_empty_before_pid_without_local_process)
{
	ClusterContentTraceOwner empty = {0};
	ClusterContentTraceWalkStats stats; unsigned owners = 0;
	reset(); cluster_trace_tx_identity = false; MyProc = NULL;
	UT_ASSERT_EQ(procs[1].pid, 0);
	cluster_content_trace_process_start(1);
	empty.revision = 1;
	UT_ASSERT_EQ(memcmp(&content_trace->slots[1].owner, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&content_trace->slots[1].revision), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&content_trace->slots[1].busy), 0);
	cluster_trace_tx_identity = true;
	cluster_content_trace_walk(count_owner, &owners, &stats);
	UT_ASSERT_EQ(owners, 0); UT_ASSERT_EQ(stats.changed, 0);
	/* The first real hold/wait must replace the baseline and remain visible. */
	actor(1); hold(0, LW_SHARED);
	cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_EXCLUSIVE);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].pid, 101);
	UT_ASSERT_EQ(cut.owners[0].nlocks, 1); UT_ASSERT(cut.owners[0].native_waiting);
}

UT_TEST(process_start_retires_recycled_payload_at_native_allocation)
{
	ClusterContentTraceOwner empty = {0};
	ClusterContentTraceWalkStats stats; unsigned owners = 0;
	uint64 revision;
	reset(); actor(1); hold(0, LW_EXCLUSIVE);
	cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_SHARED);
	revision = pg_atomic_read_u64(&content_trace->slots[1].revision);
	UT_ASSERT(content_trace->slots[1].owner.native_waiting);
	/* Native cleanup releases the old holds before the slot is reallocated.
	 * Old PGPROC PID/backendId may still be present at this pre-PID hook. */
	nheld = 0; cluster_content_trace_process_start(1);
	empty.revision = revision + 1;
	UT_ASSERT_EQ(memcmp(&content_trace->slots[1].owner, &empty, sizeof(empty)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&content_trace->slots[1].revision), revision + 1);
	cluster_content_trace_walk(count_owner, &owners, &stats);
	UT_ASSERT_EQ(owners, 0); UT_ASSERT_EQ(stats.changed, 0);
}

UT_TEST(rejection_keeps_busy_allocation_without_baseline_unknown)
{
	ClusterContentTraceWalkStats stats; unsigned owners = 0;
	reset(); nrejected = 0;
	pg_atomic_write_u32(&content_trace->slots[1].busy, 1);
	cluster_content_trace_process_start(1);
	pg_atomic_write_u32(&content_trace->slots[1].busy, 0);
	cluster_content_trace_walk_rejections(count_owner, &owners,
		remember_rejection, &nrejected, &stats);
	UT_ASSERT_EQ(stats.changed, 1); UT_ASSERT_EQ(stats.owners, 0);
	UT_ASSERT_EQ(nrejected, 1); UT_ASSERT_EQ(rejected[0].slot, 1);
	UT_ASSERT_EQ(rejected[0].reason, CLCONTENT_REJECT_COPY_REVISION);
	UT_ASSERT(rejected[0].payload_copied);
	UT_ASSERT_EQ(rejected[0].payload_pid, 0);
	UT_ASSERT_EQ(rejected[0].payload_revision, 0);
	UT_ASSERT_EQ(rejected[0].slot_revision, 1);
	UT_ASSERT(rejected[0].native_before.available);
	UT_ASSERT_EQ(rejected[0].native_before.pid, 0);
	UT_ASSERT_EQ(rejected[0].native_after.pid, 0);
}

UT_TEST(rejection_retains_stale_payload_and_native_free_identity)
{
	ClusterContentTraceWalkStats stats; unsigned owners = 0;
	reset(); nrejected = 0; actor(1); hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	pg_atomic_write_u32(&content_trace->slots[1].busy, 1);
	cluster_content_trace_process_start(1); procs[1].pid = 0;
	pg_atomic_write_u32(&content_trace->slots[1].busy, 0);
	cluster_content_trace_walk_rejections(count_owner, &owners,
		remember_rejection, &nrejected, &stats);
	UT_ASSERT_EQ(stats.changed, 1); UT_ASSERT_EQ(owners, 0);
	UT_ASSERT_EQ(nrejected, 1); UT_ASSERT_EQ(rejected[0].payload_pid, 101);
	UT_ASSERT_EQ(rejected[0].payload_generation, 10001);
	UT_ASSERT_EQ(rejected[0].payload_nlocks, 1);
	UT_ASSERT_EQ(rejected[0].native_before.pid, 0);
	UT_ASSERT_EQ(rejected[0].native_after.generation, 10001);
	/* The callback's owned copy cannot be changed by later slot reuse. */
	memset(&content_trace->slots[1].owner, 0, sizeof(local_owner));
	UT_ASSERT_EQ(rejected[0].payload_pid, 101);
}

UT_TEST(rejection_busy_never_copies_payload_and_dead_identity_stays_unknown)
{
	ClusterContentTraceWalkStats stats; unsigned owners = 0;
	reset(); nrejected = 0;
	actor(1); cluster_content_trace_native_wait_set(&buf(0)->content_lock, LW_SHARED);
	procs[1].pid = 999;
	actor(2); cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_SHARED);
	pg_atomic_write_u32(&content_trace->slots[2].busy, 1);
	cluster_content_trace_walk_rejections(count_owner, &owners,
		remember_rejection, &nrejected, &stats);
	UT_ASSERT_EQ(stats.busy, 1); UT_ASSERT_EQ(stats.dead, 1);
	UT_ASSERT_EQ(nrejected, 2); UT_ASSERT_EQ(rejected[0].reason, CLCONTENT_REJECT_IDENTITY);
	UT_ASSERT_EQ(rejected[0].payload_pid, 101);
	UT_ASSERT_EQ(rejected[0].native_before.pid, 999);
	UT_ASSERT_EQ(rejected[1].reason, CLCONTENT_REJECT_BUSY);
	UT_ASSERT(!rejected[1].payload_copied); UT_ASSERT_EQ(rejected[1].payload_pid, 0);
	UT_ASSERT_EQ(rejected[1].payload_revision, 0);
	UT_ASSERT_EQ(rejected[1].native_after.pid, 102);
	UT_ASSERT_EQ(pg_atomic_read_u32(&content_trace->slots[2].busy), 1);
}

UT_TEST(rejection_details_preserve_legacy_counters_and_source_bytes)
{
	ClusterContentTraceWalkStats legacy, detailed;
	unsigned old_owners = 0, new_owners = 0;
	void *before; Size size;
	reset(); nrejected = 0;
	for (unsigned i = 0; i < 4; i++) {
		actor(i); cluster_content_trace_native_wait_set(&buf(0)->content_lock, LW_SHARED);
	}
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	pg_atomic_write_u32(&content_trace->slots[1].busy, 1);
	cluster_content_trace_process_start(1); procs[2].pid = 0;
	pg_atomic_write_u32(&content_trace->slots[1].busy, 0);
	size = content_trace_size(); before = malloc(size); memcpy(before, content_trace, size);
	cluster_content_trace_walk(count_owner, &old_owners, &legacy);
	cluster_content_trace_walk_rejections(count_owner, &new_owners,
		remember_rejection, &nrejected, &detailed);
	UT_ASSERT_EQ(memcmp(&legacy, &detailed, sizeof(legacy)), 0);
	UT_ASSERT_EQ(old_owners, 1); UT_ASSERT_EQ(new_owners, 1);
	UT_ASSERT_EQ(nrejected, 3);
	UT_ASSERT_EQ(memcmp(before, content_trace, size), 0); free(before);
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
UT_TEST(hw_ko_grants_keep_native_identity_and_retire_exactly)
{
	ClusterResId hw = {1, 2, 3, 0, 0xf2, 1}, ko = {1, 2, 3, 0, 0xf6, 1};
	ClusterGrdHolderId holder = {0, 0, 17, 19};
	reset();
	cluster_content_trace_enqueue_granted(&hw, &holder, ExclusiveLock);
	cluster_content_trace_enqueue_granted(&hw, &holder, ExclusiveLock);
	cluster_content_trace_enqueue_granted(&ko, &holder, ExclusiveLock);
	cluster_content_trace_error_cleanup();
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].ngrants, 2);
	UT_ASSERT_EQ(cut.owners[0].grants[0].holder_request, 19);
	UT_ASSERT_EQ(cut.owners[0].grants[0].holder_epoch, 17);
	UT_ASSERT_EQ(cut.owners[0].grants[0].type, 0xf2);
	UT_ASSERT_EQ(cut.owners[0].grants[0].mode, ExclusiveLock);
	holder.request_id++;
	cluster_content_trace_enqueue_releasing(&hw, &holder);
	UT_ASSERT_EQ(local_owner.ngrants, 2);
	holder.request_id--;
	cluster_content_trace_enqueue_releasing(&hw, &holder);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.owners[0].ngrants, 1);
	UT_ASSERT_EQ(cut.owners[0].grants[0].type, 0xf6);
	holder.procno++;
	cluster_content_trace_enqueue_granted(&hw, &holder, ExclusiveLock);
	UT_ASSERT_EQ(local_owner.ngrants, 1); /* Never publish another process's hold. */
}
UT_TEST(hw_ko_grant_cap_and_missed_release_are_unknown)
{
	ClusterResId key = {1, 2, 3, 0, 0xf2, 1};
	ClusterGrdHolderId holder = {0, 0, 17, 1};
	reset();
	for (unsigned i = 0; i <= CLUSTER_CONTENT_TRACE_GRANTS; i++) {
		holder.request_id = i + 1;
		cluster_content_trace_enqueue_granted(&key, &holder, ExclusiveLock);
	}
	UT_ASSERT_EQ(local_owner.ngrants, CLUSTER_CONTENT_TRACE_GRANTS);
	UT_ASSERT_EQ(local_owner.omitted_grants, 1);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	holder.request_id = 1;
	cluster_content_trace_enqueue_releasing(&key, &holder);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 0);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.changed, 1); UT_ASSERT_EQ(cut.nowners, 0);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.owners[0].ngrants, CLUSTER_CONTENT_TRACE_GRANTS - 1);
	UT_ASSERT_EQ(cut.owners[0].omitted_grants, 1); /* Sticky until process exit/disable. */
}

UT_TEST(process_start_invalidates_even_identical_reused_native_identity)
{
	ClusterContentTraceOwner before;
	uint64 revision;
	reset(); hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1);
	before = content_trace->slots[0].owner;
	revision = pg_atomic_read_u64(&content_trace->slots[0].revision);
	/* Native pid and backendId may both repeat in a recycled PGPROC. */
	cluster_trace_tx_identity = false;
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	cluster_content_trace_process_start(0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&content_trace->slots[0].busy), 1);
	UT_ASSERT_EQ(memcmp(&before, &content_trace->slots[0].owner, sizeof(before)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&content_trace->slots[0].revision), revision + 1);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 0);
	cluster_trace_tx_identity = true;
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 0); UT_ASSERT_EQ(cut.changed, 1);
	MyStartTimestamp++;
	MyProc->cluster_grd_generation++;
	memset(&local_owner, 0, sizeof(local_owner));
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].backend_start, 1001);
	cluster_content_trace_process_start(content_trace->nslots); /* Range check is not assert-only. */
}

UT_TEST(auxiliary_native_generation_authenticates_timestamp_zero)
{
	reset(); MyProc->backendId = InvalidBackendId; MyStartTimestamp = 0;
	MyProc->cluster_grd_generation = 77; hold(0, LW_EXCLUSIVE);
	cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_EXCLUSIVE);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].backend_id, InvalidBackendId);
	UT_ASSERT_EQ(cut.owners[0].backend_start, 0); UT_ASSERT_EQ(cut.owners[0].proc_generation, 77);
	UT_ASSERT_EQ(cut.owners[0].native_wait.tag.blockNum, 1);
	UT_ASSERT_EQ(cut.owners[0].nlocks, 1);
}
UT_TEST(reused_auxiliary_generation_rejects_same_pid_and_backend)
{
	reset(); MyProc->backendId = InvalidBackendId; hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	MyProc->cluster_grd_generation++;
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 0); UT_ASSERT_EQ(cut.dead, 1);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT_EQ(cut.owners[0].proc_generation, 10001);
}
UT_TEST(zero_native_generation_never_authenticates_a_process)
{
	reset(); hold(0, LW_EXCLUSIVE); MyProc->cluster_grd_generation = 0;
	for (unsigned i = 0; i < 2; i++) {
		if (i == 1) MyProc->backendId = InvalidBackendId;
		cluster_content_trace_lock_changed(&buf(0)->content_lock);
		cluster_content_trace_capture(NULL, &cut);
		UT_ASSERT_EQ(cut.nowners, 0); UT_ASSERT_EQ(cut.dead, 1);
	}
}

UT_TEST(pcm_service_is_active_without_a_content_hold)
{
	ClusterContentTraceServiceScope scope;
	ClusterWaitTrace service = {0};
	reset(); service.tag = buf(2)->tag; service.tag_known = true;
	service.requester = 3; service.request_seq = 91; service.formation = 92;
	service.master_session = 93; service.wire_sender_connection = 94;
	cluster_content_trace_service_begin(&scope, &service);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 1); UT_ASSERT(cut.owners[0].pcm_serving);
	UT_ASSERT_EQ(cut.owners[0].nlocks, 0);
	UT_ASSERT_EQ(cut.owners[0].pcm_service.request_seq, 91);
	UT_ASSERT_EQ(cut.owners[0].pcm_service.wire_sender_connection, 94);
	UT_ASSERT_EQ(cut.owners[0].pcm_service.requester_connection, 0);
	cluster_content_trace_service_end(&scope);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(!cut.owners[0].pcm_serving);
}

UT_TEST(pcm_service_nested_scope_restores_only_live_outer)
{
	ClusterContentTraceServiceScope outer, inner;
	ClusterWaitTrace service = {0};
	reset(); service.request_seq = 81;
	cluster_content_trace_service_begin(&outer, &service);
	service.request_seq = 82; cluster_content_trace_service_begin(&inner, &service);
	UT_ASSERT_EQ(local_owner.pcm_service.request_seq, 82);
	cluster_content_trace_service_end(&inner);
	UT_ASSERT(local_owner.pcm_serving); UT_ASSERT_EQ(local_owner.pcm_service.request_seq, 81);
	cluster_content_trace_service_end(&inner); /* Idempotent retirement. */
	UT_ASSERT_EQ(local_owner.pcm_service.request_seq, 81);
	cluster_content_trace_service_begin(&inner, &service);
	cluster_content_trace_error_cleanup();
	cluster_content_trace_service_end(&inner); cluster_content_trace_service_end(&outer);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(!cut.owners[0].pcm_serving);
	UT_ASSERT_EQ(cut.owners[0].pcm_service.request_seq, 0);
}

UT_TEST(pcm_service_missed_retirement_is_unknown)
{
	ClusterContentTraceServiceScope scope;
	ClusterWaitTrace service = {0};
	reset(); service.request_seq = 81;
	cluster_content_trace_service_begin(&scope, &service);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	cluster_content_trace_service_end(&scope);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 0);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.changed, 1); UT_ASSERT_EQ(cut.nowners, 0);
	UT_ASSERT(!local_owner.pcm_serving);
}

UT_TEST(pcm_service_disabled_and_cleanup_do_not_resurrect)
{
	ClusterContentTraceServiceScope outer, inner;
	ClusterWaitTrace service = {0};
	reset(); cluster_trace_tx_identity = false;
	cluster_content_trace_service_begin(&outer, &service);
	UT_ASSERT_EQ(outer.serial, 0); cluster_content_trace_service_end(&outer);
	cluster_trace_tx_identity = true;
	cluster_content_trace_service_begin(&outer, &service);
	cluster_content_trace_error_cleanup();
	cluster_content_trace_service_begin(&inner, &service);
	cluster_content_trace_service_end(&inner); cluster_content_trace_service_end(&outer);
	UT_ASSERT(!local_owner.pcm_serving);
	cluster_content_trace_service_begin(&outer, &service);
	cluster_trace_tx_identity = false; cluster_content_trace_service_end(&outer);
	cluster_trace_tx_identity = true; cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT_EQ(cut.nowners, 0); UT_ASSERT_EQ(logs, 0);
	cluster_content_trace_service_begin(&outer, &service);
	service_serial = PG_UINT64_MAX;
	cluster_content_trace_service_begin(&inner, &service);
	cluster_content_trace_service_end(&outer);
	UT_ASSERT(!local_owner.pcm_serving); UT_ASSERT_EQ(inner.serial, 0);
}

UT_TEST(pcm_nested_finish_inherits_only_same_native_request_admission)
{
	ClusterContentTraceServiceScope outer, inner;
	ClusterWaitTrace service = {0}, nested;
	reset(); service.tag = buf(2)->tag; service.tag_known = true;
	service.request_known = true; service.request_seq = 81;
	service.formation = 82; service.master_session = 83; service.r4 = 84;
	service.requester = 3; service.source = 0; service.expected_master = 2;
	service.service_admission.supplied = service.service_admission.known = true;
	service.service_admission.origin = CLWAIT_PCM_SERVICE_INGRESS;
	service.service_admission.peer_capability_generation = 61;
	cluster_content_trace_service_begin(&outer, &service);
	nested = service; memset(&nested.service_admission, 0, sizeof(nested.service_admission));
	nested.site = CLWAIT_SITE_PCM_SOURCE_FINISH;
	cluster_content_trace_service_begin(&inner, &nested);
	cluster_content_trace_capture(NULL, &cut);
	UT_ASSERT(cut.owners[0].pcm_service.service_admission.known);
	UT_ASSERT_EQ(cut.owners[0].pcm_service.service_admission.peer_capability_generation, 61);
	cluster_content_trace_service_end(&inner);
	for (int drift = 0; drift < 21; drift++) {
		ClusterWaitTrace changed = nested;
		switch (drift) {
		case 0: changed.tag.forkNum++; break;
		case 1: changed.tag_known = false; break;
		case 2: changed.request_known = false; break;
		case 3: changed.request_seq++; break;
		case 4: changed.requester++; break;
		case 5: changed.formation++; break;
		case 6: changed.master_session++; break;
		case 7: changed.r4++; break;
		case 8: changed.base++; break;
		case 9: changed.authority++; break;
		case 10: changed.expected_master++; break;
		case 11: changed.source++; break;
		case 12: changed.wire_sender_connection++; break;
		case 13: changed.ordered_lane++; break;
		case 14: changed.semantic_crc32c++; break;
		case 15: changed.source_candidate++; break;
		case 16: changed.retain_pi_if_dirty++; break;
		case 17: changed.common_flags++; break;
		case 18: changed.common_outcome++; break;
		case 19: changed.state++; break;
		case 20: changed.mode++; break;
		}
		cluster_content_trace_service_begin(&inner, &changed);
		cluster_content_trace_capture(NULL, &cut);
		UT_ASSERT(!cut.owners[0].pcm_service.service_admission.known);
		cluster_content_trace_service_end(&inner);
	}
	/* An explicitly supplied unknown must not borrow the enclosing proof. */
	nested.service_admission.supplied = true;
	cluster_content_trace_service_begin(&inner, &nested);
	UT_ASSERT(!local_owner.pcm_service.service_admission.known);
	cluster_content_trace_service_end(&inner);
	cluster_content_trace_error_cleanup(); cluster_content_trace_service_end(&outer);
	UT_ASSERT(!local_owner.pcm_serving);
}

UT_TEST(resource_lifetimes_survive_unrelated_samples_but_not_release)
{
	ClusterWaitTrace wait = {0};
	uint64 held_life, wait_life;
	reset(); hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	held_life = local_owner.locks[0].lifetime;
	UT_ASSERT(held_life != 0);
	wait.tag = buf(1)->tag; wait.tag_known = true; wait.binding = 91;
	cluster_content_trace_wait_set(CLCONTENT_PCM, &wait);
	wait_life = local_owner.wait_lifetimes[CLCONTENT_PCM];
	UT_ASSERT(wait_life != 0);
	wait.snapshot++; wait.progress++;
	cluster_content_trace_wait_set(CLCONTENT_PCM, &wait);
	UT_ASSERT_EQ(local_owner.locks[0].lifetime, held_life);
	UT_ASSERT_EQ(local_owner.wait_lifetimes[CLCONTENT_PCM], wait_life);
	cluster_content_trace_lock_invalidating(&buf(0)->content_lock);
	nheld = 0; cluster_content_trace_lock_changed(&buf(0)->content_lock);
	hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	UT_ASSERT(local_owner.locks[0].lifetime != held_life);
	wait.binding++; cluster_content_trace_wait_set(CLCONTENT_PCM, &wait);
	UT_ASSERT(local_owner.wait_lifetimes[CLCONTENT_PCM] != wait_life);
	wait_life = local_owner.wait_lifetimes[CLCONTENT_PCM];
	cluster_content_trace_wait_clear(CLCONTENT_PCM);
	cluster_content_trace_wait_set(CLCONTENT_PCM, &wait);
	UT_ASSERT(local_owner.wait_lifetimes[CLCONTENT_PCM] != wait_life);
}

UT_TEST(grant_and_native_wait_lifetimes_retire_at_native_boundary)
{
	ClusterResId hw = {1, 2, 3, 0, 0xf2, 1};
	ClusterGrdHolderId holder = {0, 0, 17, 19};
	uint64 grant, wait;
	reset(); cluster_content_trace_enqueue_granted(&hw, &holder, ExclusiveLock);
	grant = local_owner.grants[0].lifetime; UT_ASSERT(grant != 0);
	cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_SHARED);
	wait = local_owner.native_wait.lifetime; UT_ASSERT(wait != 0);
	cluster_content_trace_native_wait_clear();
	cluster_content_trace_native_wait_set(&buf(1)->content_lock, LW_SHARED);
	UT_ASSERT(local_owner.native_wait.lifetime != wait);
	UT_ASSERT_EQ(local_owner.grants[0].lifetime, grant);
	cluster_content_trace_enqueue_releasing(&hw, &holder);
	cluster_content_trace_enqueue_granted(&hw, &holder, ExclusiveLock);
	UT_ASSERT(local_owner.grants[0].lifetime != grant);
}

UT_TEST(retry_and_service_lifetimes_cleanup_without_resurrection)
{
	ClusterWaitTrace wait = {0};
	ClusterContentTraceServiceScope scope;
	uint64 life;
	reset(); wait.buffer = 3;
	cluster_content_trace_wait_set(CLCONTENT_RETRY, &wait);
	life = local_owner.wait_lifetimes[CLCONTENT_RETRY]; UT_ASSERT(life != 0);
	cluster_content_trace_service_begin(&scope, &wait);
	UT_ASSERT(local_owner.service_lifetime != 0);
	cluster_content_trace_error_cleanup();
	UT_ASSERT_EQ(local_owner.wait_lifetimes[CLCONTENT_RETRY], 0);
	UT_ASSERT_EQ(local_owner.service_lifetime, 0);
	cluster_content_trace_service_end(&scope);
	UT_ASSERT_EQ(local_owner.service_lifetime, 0);
	cluster_content_trace_wait_set(CLCONTENT_RETRY, &wait);
	UT_ASSERT(local_owner.wait_lifetimes[CLCONTENT_RETRY] != life);
}

UT_TEST(lifetime_exhaustion_and_missed_disable_cannot_resurrect_resources)
{
	ClusterResId hw = {1, 2, 3, 0, 0xf2, 1};
	ClusterGrdHolderId holder = {0, 0, 17, 19};
	ClusterWaitTrace wait = {0};
	ClusterContentTraceServiceScope outer, inner;
	reset();
	cluster_content_trace_enqueue_granted(&hw, &holder, ExclusiveLock);
	cluster_content_trace_service_begin(&outer, &wait);
	cluster_content_trace_service_begin(&inner, &wait);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 1);
	cluster_trace_tx_identity = false;
	cluster_content_trace_enqueue_releasing(&hw, &holder);
	pg_atomic_write_u32(&content_trace->slots[0].busy, 0);
	cluster_trace_tx_identity = true;
	cluster_content_trace_service_end(&inner);
	UT_ASSERT_EQ(local_owner.ngrants, 0); UT_ASSERT(!local_owner.pcm_serving);
	cluster_content_trace_service_end(&outer);
	lifetime_serial = PG_UINT64_MAX;
	cluster_content_trace_wait_set(CLCONTENT_PCM, &wait);
	UT_ASSERT_EQ(local_owner.wait_lifetimes[CLCONTENT_PCM], 0);
	hold(0, LW_EXCLUSIVE); cluster_content_trace_lock_changed(&buf(0)->content_lock);
	UT_ASSERT_EQ(local_owner.locks[0].lifetime, 0);
}

UT_TEST(deadline_census_is_default_off_bounded_and_once)
{
	ClusterWaitTrace wait = {0};
	struct sockaddr_in address = {.sin_family=AF_INET, .sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
	socklen_t len = sizeof(address);
	char packet[2048];
	int receiver = socket(AF_INET, SOCK_DGRAM, 0), n;
	UT_ASSERT(receiver >= 0);
	UT_ASSERT_EQ(bind(receiver, (struct sockaddr *)&address, sizeof(address)), 0);
	UT_ASSERT_EQ(getsockname(receiver, (struct sockaddr *)&address, &len), 0);
	PostPortNumber = ntohs(address.sin_port);
	reset(); cluster_content_trace_wait_set(CLCONTENT_RETRY, &wait);
	actor(1); /* A live, unpublished process is still in the native census. */
	cluster_trace_tx_identity = false;
	cluster_content_trace_deadline_dump(3);
	UT_ASSERT_EQ(deadline_procs, 0);
	cluster_trace_tx_identity = true;
	cluster_content_trace_deadline_dump(3);
	while ((n = recv(receiver, packet, sizeof(packet)-1, MSG_DONTWAIT)) > 0) {
		packet[n] = '\0';
		if (strstr(packet, "DEADLINE_PROC ")) deadline_procs++;
		if (strstr(packet, "DEADLINE_TARGET ")) deadline_targets++;
		if (strstr(packet, "DEADLINE_END ")) {
			deadline_ends++;
			UT_ASSERT(strstr(packet, "dropped=0") != NULL);
		}
	}
	UT_ASSERT_EQ(deadline_procs, 2); UT_ASSERT_EQ(deadline_targets, 1);
	UT_ASSERT_EQ(deadline_ends, 1);
	cluster_content_trace_deadline_dump(3);
	UT_ASSERT_EQ(deadline_procs, 2); UT_ASSERT_EQ(deadline_ends, 1);
	UT_ASSERT_EQ(recv(receiver, packet, sizeof(packet), MSG_DONTWAIT), -1);
	close(receiver);
}

int main(void)
{
	cluster_content_trace_shmem_register(); assert(registered && registered->lwlock_count == 0);
	UT_PLAN(48);
	UT_RUN(lifetime_exhaustion_and_missed_disable_cannot_resurrect_resources);
	UT_RUN(deadline_census_is_default_off_bounded_and_once);
	UT_RUN(resource_lifetimes_survive_unrelated_samples_but_not_release);
	UT_RUN(grant_and_native_wait_lifetimes_retire_at_native_boundary);
	UT_RUN(retry_and_service_lifetimes_cleanup_without_resurrection);
	UT_RUN(process_start_initializes_empty_before_pid_without_local_process);
	UT_RUN(process_start_retires_recycled_payload_at_native_allocation);
	UT_RUN(rejection_keeps_busy_allocation_without_baseline_unknown);
	UT_RUN(rejection_retains_stale_payload_and_native_free_identity);
	UT_RUN(rejection_busy_never_copies_payload_and_dead_identity_stays_unknown);
	UT_RUN(rejection_details_preserve_legacy_counters_and_source_bytes);
	UT_RUN(pcm_nested_finish_inherits_only_same_native_request_admission);
	UT_RUN(pcm_service_is_active_without_a_content_hold);
	UT_RUN(pcm_service_nested_scope_restores_only_live_outer);
	UT_RUN(pcm_service_missed_retirement_is_unknown);
	UT_RUN(pcm_service_disabled_and_cleanup_do_not_resurrect);
	UT_RUN(auxiliary_native_generation_authenticates_timestamp_zero);
	UT_RUN(reused_auxiliary_generation_rejects_same_pid_and_backend);
	UT_RUN(zero_native_generation_never_authenticates_a_process);
	UT_RUN(process_start_invalidates_even_identical_reused_native_identity);
	UT_RUN(hw_ko_grants_keep_native_identity_and_retire_exactly);
	UT_RUN(hw_ko_grant_cap_and_missed_release_are_unknown);
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
