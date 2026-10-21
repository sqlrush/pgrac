/* Actual diagnostic publisher/reader; fixture supplies only backend state. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"
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
static const ClusterShmemRegion *registered;

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
{ (void)file; (void)line; (void)func; logs++; }

#include "../../backend/cluster/cluster_content_trace.c"

static BufferDesc *buf(unsigned i) { return &descriptors[i].bufferdesc; }
static void actor(unsigned i)
{
	memset(&local_owner, 0, sizeof(local_owner));
	MyProc = &procs[i]; MyProc->pgprocno = i; MyProc->pid = MyProcPid = 100 + i; MyProc->backendId = 1 + i;
	MyStartTimestamp = 1000 + i;
}
static void reset(void)
{
	free(content_trace); content_trace = NULL;
	memset(descriptors, 0, sizeof(descriptors)); memset(procs, 0, sizeof(procs));
	proc_header.allProcs = procs; proc_header.allProcCount = MaxBackends + NUM_AUXILIARY_PROCS;
	ProcGlobal = &proc_header; BufferDescriptors = descriptors; nheld = logs = 0;
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
	UT_ASSERT_EQ(cut.scanned, 0); UT_ASSERT_EQ(logs, 0);
	cluster_trace_tx_identity = true; hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_trace_tx_identity = false; cluster_content_trace_lock_changed(&buf(0)->content_lock);
	cluster_trace_tx_identity = true; cluster_content_trace_capture(&buf(0)->tag, &cut);
	UT_ASSERT_EQ(cut.matches, 0);
}
int main(void)
{
	cluster_content_trace_shmem_register(); assert(registered && registered->lwlock_count == 0);
	UT_PLAN(13);
	UT_RUN(actual_content_owner); UT_RUN(exact_tag_and_lock_kind); UT_RUN(release_removes_owner);
	UT_RUN(nested_wait_keys); UT_RUN(pair_attempt_and_miss); UT_RUN(busy_cut_never_waits);
	UT_RUN(missed_publish_marks_stale_unknown); UT_RUN(dead_process_not_current_owner);
	UT_RUN(reused_pid_with_new_backend_is_unknown);
	UT_RUN(zero_backend_identity_is_unknown);
	UT_RUN(bounded_locks_and_owners); UT_RUN(owned_cut_and_cleanup); UT_RUN(disabled_and_disable_transition);
	UT_DONE(); free(content_trace); return ut_failed_count ? 1 : 0;
}
