/* PGRAC: native WAL writer boundaries and caller reconfiguration waits.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
typedef struct TestWalRecord {
	uint64 sequence;
	XLogRecPtr exclusive_end, record_start;
	pg_crc32c record_crc;
} TestWalRecord;
int wal_segment_size = 1024 * 1024;
bool enableFsync = true;
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0;
char *cluster_wal_threads_dir, *DataDir;
int MyProcPid = 79;
BackendType MyBackendType = B_BACKEND;
volatile sig_atomic_t ShutdownRequestPending;
volatile uint32 CritSectionCount = 1;
MemoryContext CurrentMemoryContext, TopMemoryContext;
ResourceOwner CurrentResourceOwner;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	abort();
}
static LWLockPadded test_lwlocks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = test_lwlocks;
static bool held = true, mapping_held, active = true, member = true, fence = true, prebump,
			have_ref = true;
static bool fence_epoch_lag, fence_lease_zero, fence_expired, self_fenced;
static uint64 epoch = 7, incarnation = 99;
static ClusterWalSourceRef ref;
static ClusterWalSourceRef restart_ref;
static ClusterWalStartupImage startup_op;
static ClusterControlRootResult startup_selection;
static char scratch[MAXPGPATH], generation[MAXPGPATH], prefix_path[MAXPGPATH];
static const char *scratch_parent = "/tmp";
static int sync_calls, rename_calls, change_epoch;
static bool expect_panic, device_failure;
static bool startup_changed;
static unsigned startup_reads;
static ClusterControlRootResult route_result;
static sigjmp_buf native_error;
static char segment_path[MAXPGPATH];

bool
LWLockHeldByMeInMode(LWLock *l, LWLockMode m)
{
	return m == LW_EXCLUSIVE
		   && ((l == WALWriteLock && held) || (l == WALBufMappingLock && mapping_held));
}
uint64
GetSystemIdentifier(void)
{
	return ref.claim.identity.system_identifier;
}
uint64
cluster_epoch_get_current(void)
{
	return epoch;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return incarnation;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 n pg_attribute_unused())
{
	return incarnation;
}
ClusterMembershipState
cluster_membership_get_state(int32 n pg_attribute_unused())
{
	return member ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_ABSENT;
}
bool
cluster_external_fence_runtime_active(void)
{
	return active;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return prebump;
}
void
cluster_write_fence_observe(ClusterWriteFenceObservation *o)
{
	memset(o, 0, sizeof(*o));
	o->enforcing = o->attached = o->engaged = o->allowed = fence;
	o->epoch_current = epoch;
	o->authorized_epoch = epoch - (fence_epoch_lag ? 1 : 0);
	o->self_fenced = self_fenced;
	o->now_us = 100;
	o->expiry_us = fence_lease_zero ? 0 : fence_expired ? 99 : 10000;
	if (fence_epoch_lag || fence_lease_zero || fence_expired || self_fenced)
		o->allowed = false;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *o)
{
	*o = ref;
	return have_ref;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *o)
{
	*o = restart_ref;
	return true;
}
ClusterControlRootResult
cluster_control_root_v3_startup_read_writer(const ClusterControlRootIdentity *self,
											const uint8 operation_uuid[16],
											ClusterWalStartupImage *out)
{
	memset(out, 0, sizeof(*out));
	if (memcmp(self, &startup_op.claim.identity, sizeof(*self)) != 0
		|| memcmp(operation_uuid, startup_op.operation_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (startup_selection == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out = startup_op;
		if (++startup_reads == 2 && startup_changed)
			out->formation_epoch++;
	}
	return startup_selection;
}
void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s %s:%d\n", c, f, l);
	abort();
}
int
errcode(int code pg_attribute_unused())
{
	if (!expect_panic)
		abort();
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	if (!expect_panic)
		abort();
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	if (!expect_panic)
		abort();
	return 0;
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level == DEBUG1)
		return false;
	if (!expect_panic || (level != PANIC && level != ERROR))
		abort();
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (!expect_panic)
		abort();
	siglongjmp(native_error, 1);
}

ClusterControlRootResult
cluster_control_bootstrap_wal_route(const char *data, const char *root,
									const ClusterWalSourceRef *source)
{
	UT_ASSERT(strcmp(data, DataDir) == 0 && strcmp(root, cluster_wal_threads_dir) == 0);
	UT_ASSERT(memcmp(source, &ref, sizeof(ref)) == 0);
	return route_result;
}
#include "../../backend/cluster/cluster_wal_writer.c"
/* The extracted native entry and tail share the token that is local to one
 * production XLogWrite invocation. Device completion and root/route admission
 * are boundaries here; actual files/census are covered by control_root tests. */
static ClusterWalWriterToken writer;
typedef struct NativeResult {
	XLogRecPtr Write, Flush;
} NativeResult;
typedef NativeResult XLogwrtRqst;
typedef struct {
	int runningBackups;
} XLogCtlInsert;
static NativeResult LogwrtResult;
static struct {
	NativeResult LogwrtResult, LogwrtRqst;
	int info_lck;
	TimeLineID InsertTimeLineID;
	XLogRecPtr asyncXactLSN;
	XLogCtlInsert Insert;
	XLogRecPtr InitializedUpTo, xlblocks[2];
	int XLogCacheBlck;
	char pages[2 * XLOG_BLCKSZ];
} native_ctl;
static ControlFileData native_control;
static unsigned flush_probe_calls, flush_probe_drift;
static bool flush_probe_recovery;
static XLogRecPtr reserved_end;
XLogRecPtr
GetXLogInsertEndRecPtr(void)
{
	return reserved_end;
}
XLogRecPtr
GetFlushRecPtr(TimeLineID *timeline)
{
	UT_ASSERT(!flush_probe_recovery);
	flush_probe_calls++;
	if (flush_probe_calls == 2) {
		if (flush_probe_drift == 1)
			epoch++;
		else if (flush_probe_drift == 2)
			ref.claim.claim_sha256[0]++;
		else if (flush_probe_drift == 3)
			self_fenced = true;
		else if (flush_probe_drift == 4)
			native_ctl.InsertTimeLineID++;
		else if (flush_probe_drift == 5)
			reserved_end += XLOG_BLCKSZ; /* a later concurrent reservation */
	}
	*timeline = native_ctl.InsertTimeLineID;
	return native_ctl.LogwrtResult.Flush;
}
#define ControlFile (&native_control)
#define XLogRecPtrToBufIdx(p) (((p) / XLOG_BLCKSZ) % (native_ctl.XLogCacheBlck + 1))
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define TRACE_POSTGRESQL_WAL_BUFFER_WRITE_DIRTY_START() ((void)0)
#define TRACE_POSTGRESQL_WAL_BUFFER_WRITE_DIRTY_DONE() ((void)0)
static struct {
	uint64 wal_buffers_full;
} PendingWalStats;
uint16
cluster_wal_thread_stamp(void)
{
	return 1;
}
#define XLogCtl (&native_ctl)
#define SpinLockAcquire(lock) ((void)(lock))
#define SpinLockRelease(lock) ((void)(lock))
static void
WalSndWakeupRequest(void)
{}
#include "test_cluster_wal_publish_tail.inc"
#include "test_cluster_wal_publish_entry.inc"

/* Native caller control flow and the final write slice are real. WAL buffer
 * availability and the scheduler clock/authority update are external inputs. */
static TimestampTz fake_now;
static int native_waits, native_writes;
static bool refresh_on_wait = true;
static bool lag_on_lock, lag_every_lock;
Latch *MyLatch;
int CommitDelay, CommitSiblings;
int WalWriterDelay, WalWriterFlushAfter;
static int openLogFile = -1;
static XLogSegNo openLogSegNo;
bool
XLogInsertAllowed(void)
{
	return true;
}
bool
RecoveryInProgress(void)
{
	return flush_probe_recovery;
}
static void
UpdateMinRecoveryPoint(XLogRecPtr r pg_attribute_unused(), bool f pg_attribute_unused())
{
	abort();
}
static XLogRecPtr
WaitXLogInsertionsToFinish(XLogRecPtr r)
{
	return r;
}
static bool
MinimumActiveBackends(int n pg_attribute_unused())
{
	return false;
}
static void
WalSndWakeupProcessRequests(bool a pg_attribute_unused(), bool b pg_attribute_unused())
{}
static void
XLogFileClose(void)
{
	openLogFile = -1;
}
static void ClusterWALWaitForWriter(TimeLineID, TimestampTz *);
TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}
bool
TimestampDifferenceExceeds(TimestampTz a, TimestampTz b, int ms)
{
	return b - a >= (int64)ms * 1000;
}
bool
LWLockAcquire(LWLock *l, LWLockMode mode)
{
	bool *state = l == WALWriteLock ? &held : &mapping_held;
	Assert((l == WALWriteLock || l == WALBufMappingLock) && mode == LW_EXCLUSIVE && !*state);
	*state = true;
	if (l == WALWriteLock && lag_on_lock) {
		++epoch;
		fence_epoch_lag = true;
		lag_on_lock = lag_every_lock;
	}
	return true;
}
bool
LWLockAcquireOrWait(LWLock *l, LWLockMode mode)
{
	return LWLockAcquire(l, mode);
}
void
LWLockRelease(LWLock *l)
{
	bool *state = l == WALWriteLock ? &held : &mapping_held;
	Assert((l == WALWriteLock || l == WALBufMappingLock) && *state);
	*state = false;
}
int
WaitLatch(Latch *l pg_attribute_unused(), int events pg_attribute_unused(), long ms, uint32 event)
{
	Assert(!held && !mapping_held && event == WAIT_EVENT_RECONFIG_FENCE_WAIT);
	++native_waits;
	fake_now += ms * 1000;
	if (refresh_on_wait) {
		fence_epoch_lag = fence_lease_zero = prebump = false;
	}
	return WL_TIMEOUT;
}
void
ResetLatch(Latch *l pg_attribute_unused())
{}
static bool
XLogWrite(NativeResult request, TimeLineID tli, bool flexible pg_attribute_unused())
{
	Assert(held && CritSectionCount > 0);
	if (!native_write_entry(tli))
		return false;
	++native_writes;
	if (device_failure)
		ereport(PANIC, (errmsg("native device sync failed")));
	if (change_epoch) {
		++epoch;
		fence_epoch_lag = true;
		change_epoch = 0;
	}
	LogwrtResult.Write = Max(request.Write, native_ctl.LogwrtResult.Write);
	return native_flush_tail(tli);
}
#include "test_cluster_wal_publish_callers.inc"
#undef XLogCtl
#undef SpinLockAcquire
#undef SpinLockRelease

static void
bytes_write(const char *path, const void *p, size_t n)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, p, n) != (ssize_t)n || close(fd) != 0)
		abort();
}

static int
fd_count(void)
{
	int count = 0;
	for (int fd = 0; fd < 512; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++count;
	return count;
}
static void
fixture(void)
{
	char path[MAXPGPATH], walroot[MAXPGPATH], data[MAXPGPATH];
	uint8 bytes[112];
	ClusterWalThreadClaimV2 claim;
	pg_cryptohash_ctx *hash;
	if (scratch[0] != '\0' && !rmtree(scratch, true))
		abort();
	if (cluster_wal_threads_dir)
		pfree(cluster_wal_threads_dir);
	if (DataDir)
		pfree(DataDir);
	if (snprintf(scratch, sizeof(scratch), "%s/pgrac-prefix-publish-XXXXXX", scratch_parent)
		>= sizeof(scratch))
		abort();
	if (!mkdtemp(scratch))
		abort();
	snprintf(walroot, sizeof(walroot), "%s/wal", scratch);
	snprintf(data, sizeof(data), "%s/data", scratch);
	cluster_wal_threads_dir = pstrdup(walroot);
	DataDir = pstrdup(data);
	if (mkdir(walroot, 0700) != 0 || mkdir(data, 0700) != 0)
		abort();
	snprintf(path, sizeof(path), "%s/thread_1", walroot);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_99", walroot);
	if (mkdir(generation, 0700) != 0)
		abort();
	snprintf(path, sizeof(path), "%s/durable_prefix", generation);
	snprintf(prefix_path, sizeof(prefix_path), "%s/current", path);
	snprintf(path, sizeof(path), "%s/pg_wal", data);
	if (symlink(generation, path) != 0)
		abort();
	memset(&claim, 0, sizeof(claim));
	claim.identity.system_identifier = UINT64CONST(0x1122334455667788);
	memset(claim.identity.storage_uuid, 3, 16);
	memset(claim.identity.authority_uuid, 5, 16);
	claim.identity.origin_thread_id = 1;
	claim.identity.origin_owner_incarnation = 99;
	claim.identity.root_lineage_seq = 9;
	claim.identity.thread_claim_created_at = 23;
	claim.database_incarnation = 7;
	claim.config_generation = claim.claim_generation = 1;
	UT_ASSERT_EQ(cluster_wal_claim_v2_encode(&claim, bytes), 0);
	memset(&ref, 0, sizeof(ref));
	ref.claim.identity = claim.identity;
	memcpy(&ref.claim.identity.thread_claim_crc32c, bytes + 104, 4);
	ref.claim.database_incarnation = 7;
	ref.claim.max_config_generation = ref.timeline = 1;
	hash = pg_cryptohash_create(PG_SHA256);
	if (!hash || pg_cryptohash_init(hash) || pg_cryptohash_update(hash, bytes, 112)
		|| pg_cryptohash_final(hash, ref.claim.claim_sha256, 32))
		abort();
	pg_cryptohash_free(hash);
	snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
	bytes_write(path, bytes, sizeof(bytes));
	sync_calls = rename_calls = change_epoch = 0;
	expect_panic = device_failure = startup_changed = false;
	startup_reads = 0;
	route_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	held = active = member = fence = have_ref = true;
	mapping_held = false;
	fence_epoch_lag = fence_lease_zero = fence_expired = self_fenced = false;
	enableFsync = cluster_enabled = cluster_shared_config = true;
	prebump = false;
	MyBackendType = B_BACKEND;
	ShutdownRequestPending = false;
	memset(&writer_startup, 0, sizeof(writer_startup));
	epoch = 7;
	incarnation = 99;
	CritSectionCount = 1;
	CurrentMemoryContext = (MemoryContext)2;
	CurrentResourceOwner = (ResourceOwner)2;
	UT_ASSERT_EQ(cluster_wal_writer_begin(1, &writer), 0);
}

/* Real page/record bytes, valid pg CRC, explicit continuation and prev links. */
static TestWalRecord
record_write(XLogRecPtr start, XLogRecPtr previous, size_t payload)
{
	XLogRecord record;
	size_t head = payload < 256 ? 2 : 5, total = SizeOfXLogRecord + head + payload, used = 0;
	uint8 *b = palloc0(total);
	XLogRecPtr pos = start;
	TestWalRecord result;
	memset(&record, 0, sizeof(record));
	record.xl_tot_len = total;
	record.xl_prev = previous;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_info = XLOG_NOOP;
	b[SizeOfXLogRecord] = payload < 256 ? XLR_BLOCK_ID_DATA_SHORT : XLR_BLOCK_ID_DATA_LONG;
	if (head == 2)
		b[SizeOfXLogRecord + 1] = payload;
	else {
		uint32 n = payload;
		memcpy(b + SizeOfXLogRecord + 1, &n, 4);
	}
	memset(b + SizeOfXLogRecord + head, 0x45, payload);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, b + SizeOfXLogRecord, total - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(b, &record, SizeOfXLogRecord);
	while (used < total) {
		char filename[MAXFNAMELEN], path[MAXPGPATH];
		XLogSegNo seg;
		XLogLongPageHeaderData h;
		XLogPageHeaderData existing;
		XLogRecPtr page = pos - pos % XLOG_BLCKSZ;
		size_t offset = pos % XLOG_BLCKSZ, count;
		int fd;
		XLByteToSeg(pos, seg, wal_segment_size);
		XLogFileName(filename, ref.timeline, seg, wal_segment_size);
		snprintf(path, sizeof(path), "%s/%s", generation, filename);
		strlcpy(segment_path, path, sizeof(segment_path));
		fd = open(path, O_RDWR | O_CREAT, 0600);
		if (fd < 0 || ftruncate(fd, wal_segment_size) != 0)
			abort();
		memset(&h, 0, sizeof(h));
		h.std.xlp_magic = XLOG_PAGE_MAGIC;
		h.std.xlp_info = XLP_LONG_HEADER;
		h.std.xlp_tli = ref.timeline;
		h.std.xlp_thread_id = 1;
		h.std.xlp_pageaddr = seg * wal_segment_size;
		h.xlp_sysid = ref.claim.identity.system_identifier;
		h.xlp_seg_size = wal_segment_size;
		h.xlp_xlog_blcksz = XLOG_BLCKSZ;
		if (pread(fd, &existing, sizeof(existing), 0) != sizeof(existing))
			abort();
		if (existing.xlp_magic == 0 && pwrite(fd, &h, SizeOfXLogLongPHD, 0) != SizeOfXLogLongPHD)
			abort();
		if (offset == 0)
			offset = page % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD;
		if (pread(fd, &existing, sizeof(existing), page % wal_segment_size) != sizeof(existing))
			abort();
		if (existing.xlp_magic == 0
			|| (used
				&& offset == (page % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD))) {
			h.std.xlp_pageaddr = page;
			h.std.xlp_info = page % wal_segment_size ? 0 : XLP_LONG_HEADER;
			if (used) {
				h.std.xlp_info |= XLP_FIRST_IS_CONTRECORD;
				h.std.xlp_rem_len = total - used;
			}
			if (pwrite(fd, &h, page % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD,
					   page % wal_segment_size)
				< 0)
				abort();
		}
		count = Min(total - used, XLOG_BLCKSZ - offset);
		if (pwrite(fd, b + used, count, page % wal_segment_size + offset) != (ssize_t)count
			|| close(fd) != 0)
			abort();
		used += count;
		pos = page + offset + count;
	}
	pfree(b);
	result = (TestWalRecord){ 5, MAXALIGN(pos), start, record.xl_crc };
	return result;
}

static TestWalRecord
base_record(void)
{
	TestWalRecord p = record_write(wal_segment_size + SizeOfXLogLongPHD, 0, 24);
	return p;
}
static void
startup_fixture(void)
{
	uint8 bytes[112];
	char path[MAXPGPATH];
	int fd;
	fixture();
	memset(&startup_op, 0, sizeof(startup_op));
	snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
	fd = open(path, O_RDONLY);
	if (fd < 0 || read(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd) != 0)
		abort();
	UT_ASSERT_EQ(cluster_wal_claim_v2_decode(bytes, sizeof(bytes), &ref.claim, &startup_op.claim),
				 0);
	startup_op.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
	startup_op.input_kind = CLUSTER_WAL_STARTUP_CLEAN;
	memset(startup_op.operation_uuid, 0x43, 16);
	startup_op.database_incarnation = ref.claim.database_incarnation;
	startup_op.config_generation = ref.claim.max_config_generation;
	startup_op.formation_epoch = epoch;
	startup_op.first_segment_lsn = wal_segment_size;
	startup_op.segment_size = wal_segment_size;
	startup_op.timeline = startup_op.input_timeline = ref.timeline;
	restart_ref = ref;
	--restart_ref.claim.identity.origin_owner_incarnation;
	startup_op.predecessor.snapshot.identity = restart_ref.claim.identity;
	memcpy(startup_op.predecessor.refs.claim_sha256, restart_ref.claim.claim_sha256, 32);
	startup_selection = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	MyBackendType = B_STARTUP;
	CritSectionCount = 0;
	held = false;
}
UT_TEST(test_native_flush_has_no_sidefile_io_or_record_rounddown)
{
	TestWalRecord a, b;
	volatile bool panicked = false;
	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start, 2 * XLOG_BLCKSZ);
	memset(&native_ctl, 0, sizeof(native_ctl));
	native_ctl.LogwrtResult.Flush = a.exclusive_end;
	LogwrtResult.Write = b.record_start + XLOG_BLCKSZ;
	expect_panic = true;
	if (sigsetjmp(native_error, 1) == 0)
		UT_ASSERT(native_flush_tail(1));
	else
		panicked = true;
	expect_panic = false;
	UT_ASSERT(!panicked);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.record_start + XLOG_BLCKSZ);
	UT_ASSERT_EQ(sync_calls, 0);
	UT_ASSERT_EQ(rename_calls, 0);
	UT_ASSERT(access(prefix_path, F_OK) != 0 && errno == ENOENT);
}
/* Healthy epoch/token races retain the old ACK frontier. A fresh native
 * attempt binds the new epoch only after releasing the WAL locks. */
static void
native_reconfig_case(int mutation)
{
	TestWalRecord a, b;
	int before;
	volatile bool panicked = false;

	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start, 24);
	memset(&native_ctl, 0, sizeof(native_ctl));
	native_ctl.LogwrtResult.Write = native_ctl.LogwrtResult.Flush = a.exclusive_end;
	LogwrtResult.Write = b.exclusive_end;
	fence_epoch_lag = mutation == 0;
	fence_lease_zero = mutation == -1;
	prebump = mutation == -2;
	if (mutation > 0) {
		++epoch;
		fence_epoch_lag = mutation == 1;
	}
	before = fd_count();
	expect_panic = true;
	if (sigsetjmp(native_error, 1) == 0)
		native_flush_tail(1);
	else
		panicked = true;
	expect_panic = false;
	UT_ASSERT(!panicked);
	UT_ASSERT_EQ(fd_count(), before);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, a.exclusive_end);
	UT_ASSERT_EQ(LogwrtResult.Flush, a.exclusive_end);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Write, b.exclusive_end);
	fence_epoch_lag = fence_lease_zero = false;
	prebump = false;
	change_epoch = 0;
	UT_ASSERT_EQ(cluster_wal_writer_begin(1, &writer), 0);
	native_flush_tail(1);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.exclusive_end);
	UT_ASSERT_EQ(rename_calls, 0);
}

UT_TEST(test_native_survivor_waits_for_epoch_token)
{
	native_reconfig_case(0);
}
UT_TEST(test_native_survivor_waits_for_token_publication)
{
	native_reconfig_case(-1);
}
UT_TEST(test_native_survivor_defers_changed_epoch)
{
	native_reconfig_case(1);
}
UT_TEST(test_native_survivor_defers_fully_refreshed_new_epoch)
{
	native_reconfig_case(4);
}
UT_TEST(test_native_survivor_waits_for_prebump)
{
	native_reconfig_case(-2);
}

static void
native_caller_case(bool background, bool lag, bool hang, bool lock_race)
{
	TestWalRecord a, b;
	volatile bool failed = false;
	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start, 24);
	memset(&native_ctl, 0, sizeof(native_ctl));
	native_ctl.LogwrtResult.Write = native_ctl.LogwrtResult.Flush = a.exclusive_end;
	LogwrtResult = native_ctl.LogwrtResult;
	native_ctl.LogwrtRqst.Write = native_ctl.LogwrtRqst.Flush = b.exclusive_end;
	native_ctl.asyncXactLSN = b.exclusive_end;
	native_ctl.InsertTimeLineID = 1;
	native_ctl.XLogCacheBlck = 1;
	fake_now = 1000000;
	native_waits = native_writes = 0;
	held = false;
	CritSectionCount = 0;
	fence_epoch_lag = lag;
	change_epoch = (lag || lock_race) ? 0 : 4;
	refresh_on_wait = !hang || lock_race;
	lag_on_lock = lock_race;
	lag_every_lock = hang;
	expect_panic = true;
	if (sigsetjmp(native_error, 1) == 0) {
		if (background)
			(void)XLogBackgroundFlush();
		else
			XLogFlush(b.exclusive_end);
	} else
		failed = true;
	expect_panic = false;
	UT_ASSERT_EQ(failed, hang);
	UT_ASSERT(!held);
	if (hang) {
		UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, a.exclusive_end);
		UT_ASSERT_EQ(native_writes, 0);
		UT_ASSERT(fake_now >= 11000000 && fake_now <= 11020000);
	} else {
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.exclusive_end);
		UT_ASSERT((lag || lock_race) ? native_waits > 0 : native_writes > 1);
		}
	refresh_on_wait = true;
	lag_on_lock = lag_every_lock = false;
	CritSectionCount = 1;
}

UT_TEST(test_foreground_flush_reconfiguration_wait)
{
	native_caller_case(false, true, false, false);
}
UT_TEST(test_foreground_flush_reconfiguration_race)
{
	native_caller_case(false, false, false, false);
}
UT_TEST(test_background_flush_reconfiguration_wait)
{
	native_caller_case(true, true, false, false);
}
UT_TEST(test_background_flush_reconfiguration_race)
{
	native_caller_case(true, false, false, false);
}
UT_TEST(test_flush_hang_never_acknowledges)
{
	native_caller_case(false, true, true, false);
}
UT_TEST(test_foreground_entry_rechecks_before_physical_write)
{
	native_caller_case(false, false, false, true);
}
UT_TEST(test_background_entry_rechecks_before_physical_write)
{
	native_caller_case(true, false, false, true);
}
UT_TEST(test_repeated_entry_races_do_not_reset_hang_budget)
{
	native_caller_case(false, false, true, true);
}

UT_TEST(test_wal_buffer_reuse_releases_mapping_lock_before_wait)
{
	TestWalRecord a, b;
	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start,
					 wal_segment_size + XLOG_BLCKSZ - a.exclusive_end - SizeOfXLogRecord - 5);
	UT_ASSERT_EQ(b.exclusive_end, wal_segment_size + XLOG_BLCKSZ);
	memset(&native_ctl, 0, sizeof(native_ctl));
	native_ctl.LogwrtResult.Write = native_ctl.LogwrtResult.Flush = a.exclusive_end;
	LogwrtResult = native_ctl.LogwrtResult;
	native_ctl.InsertTimeLineID = 1;
	native_ctl.XLogCacheBlck = 1;
	native_ctl.InitializedUpTo = wal_segment_size + 2 * XLOG_BLCKSZ;
	native_ctl.xlblocks[0] = b.exclusive_end;
	fake_now = 1000000;
	native_waits = native_writes = 0;
	refresh_on_wait = true;
	held = false;
	fence_epoch_lag = true;
	AdvanceXLInsertBuffer(wal_segment_size + 2 * XLOG_BLCKSZ, 1, false);
	UT_ASSERT(!held && !mapping_held);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(native_ctl.InitializedUpTo, wal_segment_size + 3 * XLOG_BLCKSZ);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.exclusive_end);
	UT_ASSERT_EQ(native_waits, 1);
	UT_ASSERT_EQ(native_writes, 1);
}

UT_TEST(test_runtime_refuses_lost_writer_before_and_after_device_io)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterControlRootResult result;
		fixture();
		fence_epoch_lag = prebump = true;
		switch (fault) {
		case 0: self_fenced = true; break;
		case 1: fence_expired = true; break;
		case 2: incarnation++; break;
		case 3: active = false; break;
		case 4: member = false; break;
		case 5: fence = false; break;
		case 6: have_ref = false; break;
		case 7: enableFsync = false; break;
		case 8: cluster_enabled = false; break;
		case 9: cluster_shared_config = false; break;
		case 10: ref.claim.identity.root_lineage_seq++; break;
		case 11: ref.timeline++; break;
		case 12: cluster_node_id++; break;
		}
		result = cluster_wal_writer_check(&writer);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(sync_calls | rename_calls, 0);
		cluster_node_id = 0;
	}
}

UT_TEST(test_native_device_or_postflush_authority_failure_never_acknowledges)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		volatile bool panicked = false;
		fixture();
		memset(&native_ctl, 0, sizeof(native_ctl));
		native_ctl.LogwrtResult.Flush = 128;
		LogwrtResult.Write = 256;
		expect_panic = true;
		if (sigsetjmp(native_error, 1) == 0) {
			if (fault == 0) {
				device_failure = true;
				(void)XLogWrite((NativeResult){256, 256}, 1, false);
			} else {
				self_fenced = true;
				(void)native_flush_tail(1);
			}
		} else
			panicked = true;
		expect_panic = false;
		UT_ASSERT(panicked);
		UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, 128);
	}
}

UT_TEST(test_startup_binds_only_revalidated_actual_empty_owner)
{
	XLogRecPtr start;
	startup_fixture();
	have_ref = false;
	UT_ASSERT_EQ(cluster_wal_writer_startup_prepare(&ref.claim.identity,
		startup_op.operation_uuid, &start), 0);
	UT_ASSERT_EQ(start, wal_segment_size);
	UT_ASSERT_EQ(startup_reads, 2);
	UT_ASSERT(cluster_wal_writer_startup_matches(&ref.claim.identity,
		startup_op.operation_uuid, start));
	UT_ASSERT_EQ(cluster_wal_writer_begin(1, &writer), 0);
	UT_ASSERT_EQ(cluster_wal_writer_check(&writer), 0);
	MyBackendType = B_BACKEND;
	UT_ASSERT(cluster_wal_writer_ready(1) != 0);
	MyBackendType = B_STARTUP;
	MyProcPid++;
	UT_ASSERT(cluster_wal_writer_ready(1) != 0);
	MyProcPid--;
}

UT_TEST(test_startup_rejects_changed_selection_route_or_owner)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		XLogRecPtr start = 123;
		startup_fixture();
		switch (fault) {
		case 0: CritSectionCount = 1; break;
		case 1: MyBackendType = B_BACKEND; break;
		case 2: ShutdownRequestPending = true; break;
		case 3: held = true; break;
		case 4: startup_op.phase = CLUSTER_WAL_STARTUP_RESERVED; break;
		case 5: startup_op.first_segment_lsn++; break;
		case 6: startup_op.segment_size++; break;
		case 7: restart_ref.claim.identity.origin_owner_incarnation++; break;
		case 8: restart_ref.claim.claim_sha256[0]++; break;
		case 9: restart_ref.timeline++; break;
		case 10: startup_selection = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID; break;
		case 11: route_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN; break;
		case 12: startup_changed = true; break;
		case 13: active = false; break;
		case 14: startup_op.formation_epoch++; break;
		case 15: self_fenced = true; break;
		}
		UT_ASSERT(cluster_wal_writer_startup_prepare(&ref.claim.identity,
			startup_op.operation_uuid, &start) != 0);
		UT_ASSERT_EQ(start, 0);
		UT_ASSERT(!writer_startup.valid);
	}
}

UT_TEST(test_background_flush_snapshot_preserves_native_byte_boundary)
{
	ClusterWalWriterFlushV1 snapshot;
	fixture();
	CritSectionCount = 0;
	held = false;
	MyBackendType = B_BG_WRITER;
	flush_probe_calls = flush_probe_drift = 0;
	native_ctl.InsertTimeLineID = 1;
	native_ctl.LogwrtResult.Flush = wal_segment_size + 89;
	reserved_end = wal_segment_size + 80;
	UT_ASSERT_EQ(cluster_wal_writer_flushed_v1(&snapshot), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(snapshot.flushed_end, wal_segment_size + 89);
	UT_ASSERT_EQ(snapshot.complete_end, reserved_end);
	UT_ASSERT_EQ(snapshot.writer.epoch, epoch);
	UT_ASSERT_EQ(memcmp(&snapshot.writer.ref, &ref, sizeof(ref)), 0);
	UT_ASSERT_EQ(flush_probe_calls, 2);
	UT_ASSERT_EQ(sync_calls, 0);
	UT_ASSERT_EQ(rename_calls, 0);
}

UT_TEST(test_background_flush_snapshot_rejects_writer_change_and_startup)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterWalWriterFlushV1 snapshot;
		static const ClusterWalWriterFlushV1 zero;
		fixture();
		CritSectionCount = 0;
		held = false;
		MyBackendType = B_BG_WRITER;
		native_ctl.InsertTimeLineID = 1;
		native_ctl.LogwrtResult.Flush = wal_segment_size + 89;
		reserved_end = wal_segment_size + 80;
		flush_probe_calls = 0;
		flush_probe_recovery = fault == 9;
		flush_probe_drift = fault < 4 ? fault + 1 : 0;
		if (fault == 4)
			native_ctl.LogwrtResult.Flush = 0;
		else if (fault == 5)
			MyBackendType = B_STARTUP;
		else if (fault == 6)
			active = false;
		else if (fault == 7)
			have_ref = false;
		else if (fault == 8)
			fence_lease_zero = true;
		memset(&snapshot, 0x55, sizeof(snapshot));
		UT_ASSERT(cluster_wal_writer_flushed_v1(&snapshot) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(&snapshot, &zero, sizeof(snapshot)), 0);
		if (flush_probe_recovery)
			UT_ASSERT_EQ(flush_probe_calls, 0);
	}
	UT_ASSERT_EQ(cluster_wal_writer_flushed_v1(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	flush_probe_drift = 0;
	flush_probe_recovery = false;
}

UT_TEST(test_background_complete_prefix_waits_for_reserved_record_end)
{
	ClusterWalWriterFlushV1 snapshot;
	static const ClusterWalWriterFlushV1 zero;
	fixture();
	CritSectionCount = 0;
	held = false;
	MyBackendType = B_BG_WRITER;
	flush_probe_calls = flush_probe_drift = 0;
	native_ctl.InsertTimeLineID = 1;
	/* Native flush can finish one page while this reserved record still
	 * extends into the next. The byte bound alone is not a complete cut. */
	native_ctl.LogwrtResult.Flush = wal_segment_size + XLOG_BLCKSZ;
	reserved_end = wal_segment_size + 2 * XLOG_BLCKSZ;
	memset(&snapshot, 0x55, sizeof(snapshot));
	UT_ASSERT_NE(cluster_wal_writer_flushed_v1(&snapshot), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&snapshot, &zero, sizeof(snapshot)), 0);
	UT_ASSERT_EQ(sync_calls, 0);
	UT_ASSERT_EQ(rename_calls, 0);
	/* Retrying after native flush covers the captured reservation succeeds.
	 * Later reservations must not move the already captured complete cut. */
	flush_probe_calls = 0;
	flush_probe_drift = 5;
	native_ctl.LogwrtResult.Flush = reserved_end;
	reserved_end += XLOG_BLCKSZ;
	UT_ASSERT_EQ(cluster_wal_writer_flushed_v1(&snapshot), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(snapshot.complete_end, wal_segment_size + 2 * XLOG_BLCKSZ);
	UT_ASSERT_EQ(snapshot.flushed_end, snapshot.complete_end);
	UT_ASSERT_EQ(reserved_end, snapshot.complete_end + 2 * XLOG_BLCKSZ);
	flush_probe_drift = 0;
}

UT_TEST(test_background_pending_cut_cannot_cross_writer_token)
{
	for (unsigned changed = 0; changed < 3; changed++) {
		ClusterWalWriterFlushV1 snapshot;
		static const ClusterWalWriterFlushV1 zero;
		fixture();
		CritSectionCount = 0;
		held = false;
		MyBackendType = B_BG_WRITER;
		flush_probe_calls = flush_probe_drift = 0;
		native_ctl.InsertTimeLineID = 1;
		reserved_end = wal_segment_size + 2 * XLOG_BLCKSZ;
		native_ctl.LogwrtResult.Flush = reserved_end - XLOG_BLCKSZ;
		UT_ASSERT_EQ(cluster_wal_writer_flushed_v1(&snapshot), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		native_ctl.LogwrtResult.Flush = reserved_end;
		reserved_end += XLOG_BLCKSZ;
		if (changed == 0)
			epoch++;
		else if (changed == 1)
			ref.claim.claim_sha256[0]++;
		else {
			incarnation++;
			ref.claim.identity.origin_owner_incarnation = incarnation;
		}
		UT_ASSERT_EQ(cluster_wal_writer_flushed_v1(&snapshot), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(memcmp(&snapshot, &zero, sizeof(snapshot)), 0);
		native_ctl.LogwrtResult.Flush = reserved_end;
		UT_ASSERT_EQ(cluster_wal_writer_flushed_v1(&snapshot), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(snapshot.complete_end, reserved_end);
		UT_ASSERT_EQ(memcmp(&snapshot.writer.ref, &ref, sizeof(ref)), 0);
	}
}

int
main(void)
{
	UT_PLAN(23);
	UT_RUN(test_background_pending_cut_cannot_cross_writer_token);
	UT_RUN(test_background_complete_prefix_waits_for_reserved_record_end);
	UT_RUN(test_background_flush_snapshot_preserves_native_byte_boundary);
	UT_RUN(test_background_flush_snapshot_rejects_writer_change_and_startup);
	UT_RUN(test_native_flush_has_no_sidefile_io_or_record_rounddown);
	UT_RUN(test_native_survivor_waits_for_epoch_token);
	UT_RUN(test_native_survivor_waits_for_token_publication);
	UT_RUN(test_native_survivor_defers_changed_epoch);
	UT_RUN(test_native_survivor_defers_fully_refreshed_new_epoch);
	UT_RUN(test_native_survivor_waits_for_prebump);
	UT_RUN(test_foreground_flush_reconfiguration_wait);
	UT_RUN(test_foreground_flush_reconfiguration_race);
	UT_RUN(test_background_flush_reconfiguration_wait);
	UT_RUN(test_background_flush_reconfiguration_race);
	UT_RUN(test_flush_hang_never_acknowledges);
	UT_RUN(test_foreground_entry_rechecks_before_physical_write);
	UT_RUN(test_background_entry_rechecks_before_physical_write);
	UT_RUN(test_repeated_entry_races_do_not_reset_hang_budget);
	UT_RUN(test_wal_buffer_reuse_releases_mapping_lock_before_wait);
	UT_RUN(test_runtime_refuses_lost_writer_before_and_after_device_io);
	UT_RUN(test_native_device_or_postflush_authority_failure_never_acknowledges);
	UT_RUN(test_startup_binds_only_revalidated_actual_empty_owner);
	UT_RUN(test_startup_rejects_changed_selection_route_or_owner);
	if (scratch[0] && !rmtree(scratch, true))
		abort();
	pfree(cluster_wal_threads_dir);
	pfree(DataDir);
	UT_DONE();
	return ut_failed_count != 0;
}
