/* PGRAC: actual WAL + filesystem publication, runtime facts are boundary inputs.
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
static ClusterWalDurablePrefixRef ref;
static ClusterWalDurablePrefixRef restart_ref;
static ClusterWalStartupImage startup_op;
static ClusterControlRootResult startup_selection;
static char scratch[MAXPGPATH], generation[MAXPGPATH], prefix_path[MAXPGPATH];
static int sync_calls, rename_calls, fail_sync, fail_rename, change_epoch;
static bool fail_write, fail_readback, expect_panic;
static sigjmp_buf native_error;
static int mutation_at_sync, mutation_kind;
static char segment_path[MAXPGPATH];
static void mutate_namespace(void);

/* Only allocation/runtime boundaries are substituted, not the decoder,
 * namespace validator, promise selection, IO order or publication logic. */
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent pg_attribute_unused(),
							  const char *name pg_attribute_unused(),
							  Size min pg_attribute_unused(), Size init pg_attribute_unused(),
							  Size max pg_attribute_unused())
{
	return (MemoryContext)1;
}
void
MemoryContextAllowInCriticalSection(MemoryContext c pg_attribute_unused(),
									bool a pg_attribute_unused())
{}
void
MemoryContextReset(MemoryContext c pg_attribute_unused())
{}
void
MemoryContextDelete(MemoryContext c pg_attribute_unused())
{
	abort();
}
void
ResourceOwnerDelete(ResourceOwner o pg_attribute_unused())
{
	abort();
}
ResourceOwner
ResourceOwnerCreate(ResourceOwner p pg_attribute_unused(), const char *n pg_attribute_unused())
{
	return (ResourceOwner)1;
}
void
ResourceOwnerRelease(ResourceOwner o pg_attribute_unused(),
					 ResourceReleasePhase p pg_attribute_unused(), bool c pg_attribute_unused(),
					 bool t pg_attribute_unused())
{}
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
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *o)
{
	*o = ref;
	return have_ref;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *o)
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
	if (startup_selection == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = startup_op;
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

int
pg_fsync(int fd)
{
	++sync_calls;
	if (change_epoch && sync_calls == change_epoch)
		++epoch;
	if (fail_sync && sync_calls == fail_sync) {
		errno = EIO;
		return -1;
	}
	if (mutation_at_sync == sync_calls)
		mutate_namespace();
	return fsync(fd);
}
static ssize_t
publish_test_write(int fd, const void *p, size_t n)
{
	if (fail_write) {
		errno = ENOSPC;
		return -1;
	}
	return write(fd, p, n);
}
static ssize_t
publish_test_pread(int fd, void *p, size_t n, off_t off)
{
	if (fail_readback && rename_calls) {
		errno = EIO;
		return -1;
	}
	return pread(fd, p, n, off);
}
static int
publish_test_renameat(int a, const char *b, int c, const char *d)
{
	++rename_calls;
	if (fail_rename) {
		errno = EIO;
		return -1;
	}
	return renameat(a, b, c, d);
}
#define renameat publish_test_renameat
#define write publish_test_write
#define pread publish_test_pread
#include "../../backend/cluster/cluster_wal_durable_publish.c"
#undef renameat
#undef write
#undef pread

/* Execute the real final XLogWrite slice: preceding device IO is the external
 * boundary. Promise publication is real, not a fabricated successful return. */
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
	return false;
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
static void ClusterWALWaitForPublish(TimeLineID, TimestampTz *);
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
mutate_namespace(void)
{
	char saved[MAXPGPATH], path[MAXPGPATH];
	if (mutation_kind == 1) {
		char *bytes = palloc(wal_segment_size);
		int fd = open(segment_path, O_RDONLY);
		if (fd < 0 || read(fd, bytes, wal_segment_size) != wal_segment_size || close(fd))
			abort();
		snprintf(saved, sizeof(saved), "%s.saved", segment_path);
		if (rename(segment_path, saved))
			abort();
		bytes_write(segment_path, bytes, wal_segment_size);
		pfree(bytes);
	} else if (mutation_kind == 2) {
		snprintf(saved, sizeof(saved), "%s.saved", generation);
		if (rename(generation, saved) || mkdir(generation, 0700))
			abort();
	} else if (mutation_kind == 3) {
		snprintf(path, sizeof(path), "%s/pg_wal", DataDir);
		if (unlink(path) || symlink(DataDir, path))
			abort();
	} else if (mutation_kind == 4) {
		uint8 bytes[256];
		int fd = open(prefix_path, O_RDONLY);
		if (fd < 0 || read(fd, bytes, 256) != 256 || close(fd) || unlink(prefix_path))
			abort();
		bytes_write(prefix_path, bytes, 256);
	} else if (mutation_kind == 5) {
		snprintf(path, sizeof(path), "%s/durable_prefix/.current.79.6", generation);
		snprintf(saved, sizeof(saved), "%s.saved", path);
		if (rename(path, saved))
			abort();
		bytes_write(path, "foreign", 7);
	} else
		abort();
}

static void
prefix_write(ClusterWalDurablePrefix p)
{
	uint8 b[256];
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&ref, &p, b), 0);
	bytes_write(prefix_path, b, sizeof(b));
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
	strlcpy(scratch, "/tmp/pgrac-prefix-publish-XXXXXX", sizeof(scratch));
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
	if (mkdir(path, 0700) != 0)
		abort();
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
	sync_calls = rename_calls = fail_sync = fail_rename = change_epoch = 0;
	fail_write = fail_readback = expect_panic = false;
	mutation_at_sync = mutation_kind = 0;
	held = active = member = fence = have_ref = true;
	mapping_held = false;
	fence_epoch_lag = fence_lease_zero = fence_expired = self_fenced = false;
	enableFsync = cluster_enabled = cluster_shared_config = true;
	prebump = false;
	MyBackendType = B_BACKEND;
	ShutdownRequestPending = false;
	memset(&publish_startup, 0, sizeof(publish_startup));
	epoch = 7;
	incarnation = 99;
	CritSectionCount = 1;
	CurrentMemoryContext = (MemoryContext)2;
	CurrentResourceOwner = (ResourceOwner)2;
}

/* Real page/record bytes, valid pg CRC, explicit continuation and prev links. */
static ClusterWalDurablePrefix
record_write(XLogRecPtr start, XLogRecPtr previous, size_t payload)
{
	XLogRecord record;
	size_t head = payload < 256 ? 2 : 5, total = SizeOfXLogRecord + head + payload, used = 0;
	uint8 *b = palloc0(total);
	XLogRecPtr pos = start;
	ClusterWalDurablePrefix result;
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
	result = (ClusterWalDurablePrefix){ 5, MAXALIGN(pos), start, record.xl_crc };
	return result;
}

static ClusterWalDurablePrefix
base_record(void)
{
	ClusterWalDurablePrefix p = record_write(wal_segment_size + SizeOfXLogLongPHD, 0, 24);
	prefix_write(p);
	return p;
}

static void
check_result(XLogRecPtr expected, ClusterWalDurablePrefix last, uint64 sequence)
{
	ClusterWalDurablePrefix out;
	UT_ASSERT_EQ(cluster_wal_durable_prefix_read(cluster_wal_threads_dir, &ref, &out), 0);
	UT_ASSERT_EQ(out.exclusive_end, expected);
	UT_ASSERT_EQ(out.record_start, last.record_start);
	UT_ASSERT_EQ(out.record_crc, last.record_crc);
	UT_ASSERT_EQ(out.sequence, sequence);
	UT_ASSERT(CurrentMemoryContext == (MemoryContext)2);
	UT_ASSERT(CurrentResourceOwner == (ResourceOwner)2);
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
	prefix_write((ClusterWalDurablePrefix){ 1, 0, 0, 0 });
	MyBackendType = B_STARTUP;
	CritSectionCount = 0;
	held = false;
}

UT_TEST(test_startup_first_prefix_requires_real_independent_records)
{
	XLogRecPtr start = 0, covered = 123;
	ClusterWalDurablePrefix a, b;
	startup_fixture();
	UT_ASSERT_EQ(
		cluster_wal_durable_startup_prepare(&ref.claim.identity, startup_op.operation_uuid, &start),
		0);
	UT_ASSERT_EQ(start, wal_segment_size);
	if (ut_current_failed)
		return;
	a = record_write(start + SizeOfXLogLongPHD, 0, 24);
	b = record_write(a.exclusive_end, a.record_start, 40);
	CritSectionCount = 1;
	held = true;
	UT_ASSERT_EQ(cluster_wal_durable_publish(1, b.exclusive_end, start, &covered), 0);
	UT_ASSERT_EQ(covered, b.exclusive_end);
	check_result(b.exclusive_end, b, 2);
	UT_ASSERT_EQ(cluster_wal_durable_publish(1, b.exclusive_end, b.exclusive_end, &covered), 0);
	check_result(b.exclusive_end, b, 2);
}

UT_TEST(test_startup_binding_refuses_nonowner_and_nonempty_inputs)
{
	for (int fault = 0; fault < 15; ++fault) {
		XLogRecPtr start = 123;
		char path[MAXPGPATH];
		int before;
		startup_fixture();
		switch (fault) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			CritSectionCount = 1;
			break;
		case 2:
			held = true;
			break;
		case 3:
			startup_selection = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
			break;
		case 4:
			startup_op.phase = CLUSTER_WAL_STARTUP_RESERVED;
			break;
		case 5:
			++restart_ref.claim.identity.origin_owner_incarnation;
			break;
		case 6:
			restart_ref.claim.claim_sha256[0] ^= 1;
			break;
		case 7:
			++restart_ref.timeline;
			break;
		case 8:
			active = false;
			break;
		case 9:
			++incarnation;
			break;
		case 10:
			ShutdownRequestPending = true;
			break;
		case 11:
			(void)base_record();
			break;
		case 12:
			snprintf(path, sizeof(path), "%s/pg_wal", DataDir);
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(symlink(DataDir, path), 0);
			break;
		case 13:
			++startup_op.formation_epoch;
			break;
		case 14:
			enableFsync = false;
			break;
		}
		before = fd_count();
		UT_ASSERT(cluster_wal_durable_startup_prepare(&ref.claim.identity,
													  startup_op.operation_uuid, &start)
				  != 0);
		UT_ASSERT_EQ(start, 0);
		UT_ASSERT(!publish_startup.valid);
		UT_ASSERT_EQ(fd_count(), before);
		UT_ASSERT_EQ(sync_calls, 0);
		UT_ASSERT_EQ(rename_calls, 0);
	}
}

UT_TEST(test_startup_empty_does_not_authorize_ordinary_flush)
{
	for (int fault = 0; fault < 8; ++fault) {
		XLogRecPtr start, covered = 123;
		ClusterWalDurablePrefix a;
		startup_fixture();
		UT_ASSERT_EQ(cluster_wal_durable_startup_prepare(&ref.claim.identity,
														 startup_op.operation_uuid, &start),
					 0);
		if (ut_current_failed)
			return;
		a = record_write(start + SizeOfXLogLongPHD, 0, 24);
		CritSectionCount = 1;
		held = true;
		switch (fault) {
		case 0:
			MyBackendType = B_BACKEND;
			break;
		case 1:
			MyBackendType = B_WAL_WRITER;
			break;
		case 2:
			++MyProcPid;
			break;
		case 3:
			++epoch;
			break;
		case 4:
			++incarnation;
			break;
		case 5:
			publish_startup.valid = false;
			break;
		case 6:
			ShutdownRequestPending = true;
			break;
		case 7:
			++start;
			break;
		}
		UT_ASSERT(cluster_wal_durable_publish(1, a.exclusive_end, start, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(sync_calls, 0);
		UT_ASSERT_EQ(rename_calls, 0);
		check_result(0, (ClusterWalDurablePrefix){ 1, 0, 0, 0 }, 1);
		MyProcPid = 79;
	}
}

UT_TEST(test_startup_rejects_old_link_partial_header_and_corrupt_first_record)
{
	for (int fault = 0; fault < 6; ++fault) {
		XLogRecPtr start, covered = 123, upper;
		ClusterWalDurablePrefix a;
		startup_fixture();
		UT_ASSERT_EQ(cluster_wal_durable_startup_prepare(&ref.claim.identity,
														 startup_op.operation_uuid, &start),
					 0);
		if (ut_current_failed)
			return;
		a = record_write(start + SizeOfXLogLongPHD, fault == 0 ? start - 128 : 0, 24);
		upper = a.exclusive_end;
		if (fault == 1)
			upper = start + SizeOfXLogLongPHD;
		if (fault == 2)
			upper = a.record_start + SizeOfXLogRecord;
		if (fault == 3 || fault == 4) {
			uint32 bad = 999;
			int fd = open(segment_path, O_RDWR);
			off_t offset = fault == 3 ? SizeOfXLogLongPHD + offsetof(XLogRecord, xl_crc)
									  : offsetof(XLogPageHeaderData, xlp_tli);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(pwrite(fd, &bad, sizeof(bad), offset), sizeof(bad));
			UT_ASSERT_EQ(close(fd), 0);
		}
		if (fault == 5)
			UT_ASSERT_EQ(unlink(segment_path), 0);
		CritSectionCount = 1;
		held = true;
		UT_ASSERT(cluster_wal_durable_publish(1, upper, start, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(sync_calls, 0);
		UT_ASSERT_EQ(rename_calls, 0);
		check_result(0, (ClusterWalDurablePrefix){ 1, 0, 0, 0 }, 1);
	}
}

UT_TEST(test_startup_first_publication_persistence_failures_reobserve_exact_prefix)
{
	for (int fault = 1; fault <= 5; ++fault) {
		XLogRecPtr start, covered = 123;
		ClusterWalDurablePrefix a;
		int before;
		startup_fixture();
		UT_ASSERT_EQ(cluster_wal_durable_startup_prepare(&ref.claim.identity,
														 startup_op.operation_uuid, &start),
					 0);
		if (ut_current_failed)
			return;
		a = record_write(start + SizeOfXLogLongPHD, 0, 24);
		CritSectionCount = 1;
		held = true;
		if (fault <= 4)
			fail_sync = fault;
		else
			fail_rename = 1;
		before = fd_count();
		UT_ASSERT(cluster_wal_durable_publish(1, a.exclusive_end, start, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(fd_count(), before);
		fail_sync = fail_rename = 0;
		UT_ASSERT_EQ(cluster_wal_durable_publish(1, a.exclusive_end, start, &covered), 0);
		UT_ASSERT_EQ(covered, a.exclusive_end);
		check_result(a.exclusive_end, a, 2);
	}
}

UT_TEST(test_native_startup_flush_acknowledges_only_published_first_record)
{
	XLogRecPtr start;
	ClusterWalDurablePrefix a;
	startup_fixture();
	UT_ASSERT_EQ(
		cluster_wal_durable_startup_prepare(&ref.claim.identity, startup_op.operation_uuid, &start),
		0);
	if (ut_current_failed)
		return;
	a = record_write(start + SizeOfXLogLongPHD, 0, 24);
	memset(&native_ctl, 0, sizeof(native_ctl));
	native_ctl.LogwrtResult.Write = native_ctl.LogwrtResult.Flush = start;
	CritSectionCount = 1;
	held = true;
	UT_ASSERT(native_write_entry(1));
	/* The native entry reloads the shared result before the physical device
	 * write. Supply that completed device boundary before executing its tail. */
	LogwrtResult = (NativeResult){ a.exclusive_end, a.exclusive_end };
	UT_ASSERT(native_flush_tail(1));
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, a.exclusive_end);
	check_result(a.exclusive_end, a, 2);
}

UT_TEST(test_group_flush_complete_and_reuse)
{
	ClusterWalDurablePrefix a, b, c;
	XLogRecPtr covered = 123;
	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start, 32);
	c = record_write(b.exclusive_end, b.record_start, 48);
	UT_ASSERT_EQ(cluster_wal_durable_publish(1, c.exclusive_end, a.exclusive_end, &covered), 0);
	UT_ASSERT_EQ(covered, c.exclusive_end);
	UT_ASSERT_EQ(rename_calls, 1);
	check_result(c.exclusive_end, c, 6);
	rename_calls = 0;
	UT_ASSERT_EQ(cluster_wal_durable_publish(1, c.exclusive_end, a.exclusive_end, &covered), 0);
	UT_ASSERT_EQ(covered, c.exclusive_end);
	UT_ASSERT_EQ(rename_calls, 0);
	check_result(c.exclusive_end, c, 6);
}

UT_TEST(test_partial_record_does_not_become_flush)
{
	ClusterWalDurablePrefix a, b, c;
	XLogRecPtr covered;
	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start, 32);
	c = record_write(b.exclusive_end, b.record_start, 2 * XLOG_BLCKSZ);
	UT_ASSERT_EQ(
		cluster_wal_durable_publish(1, c.record_start + XLOG_BLCKSZ, a.exclusive_end, &covered), 0);
	UT_ASSERT_EQ(covered, b.exclusive_end);
	check_result(b.exclusive_end, b, 6);
}

UT_TEST(test_cross_page_and_segment)
{
	for (int cross = 0; cross < 2; ++cross) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered,
			start = cross ? 2 * wal_segment_size - 48 : wal_segment_size + XLOG_BLCKSZ - 48;
		fixture();
		a = record_write(start, 0, 2 * XLOG_BLCKSZ);
		prefix_write(a);
		b = record_write(a.exclusive_end, a.record_start, 2 * XLOG_BLCKSZ);
		UT_ASSERT_EQ(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered), 0);
		UT_ASSERT_EQ(covered, b.exclusive_end);
		check_result(b.exclusive_end, b, 6);
	}
}

UT_TEST(test_refuses_runtime_and_prefix_inputs)
{
	for (int fault = 0; fault < 13; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		switch (fault) {
		case 0:
			held = false;
			break;
		case 1:
			active = false;
			break;
		case 2:
			member = false;
			break;
		case 3:
			fence = false;
			break;
		case 4:
			prebump = true;
			break;
		case 5:
			have_ref = false;
			break;
		case 6:
			incarnation++;
			break;
		case 7:
			CritSectionCount = 0;
			break;
		case 8:
			UT_ASSERT_EQ(unlink(prefix_path), 0);
			break;
		case 9:
			prefix_write((ClusterWalDurablePrefix){ 1, 0, 0, 0 });
			break;
		case 10:
			a.sequence = UINT64_MAX;
			prefix_write(a);
			break;
		case 11:
			a.record_crc ^= 1;
			prefix_write(a);
			break;
		case 12:
			ref.timeline++;
			break;
		}
		UT_ASSERT(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(rename_calls, 0);
	}
}

UT_TEST(test_sync_rename_and_epoch_failures)
{
	for (int fault = 1; fault <= 7; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		if (fault <= 4)
			fail_sync = fault;
		else if (fault == 5)
			fail_rename = 1;
		else
			change_epoch = fault == 6 ? 1 : 4;
		UT_ASSERT(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		/* Any durable side effect must remain an old or new complete promise;
		 * no fake success and no rollback/delete of an already-published one. */
		{
			ClusterWalDurablePrefix p;
			UT_ASSERT_EQ(cluster_wal_durable_prefix_read(cluster_wal_threads_dir, &ref, &p), 0);
			UT_ASSERT(p.exclusive_end == a.exclusive_end || p.exclusive_end == b.exclusive_end);
		}
	}
}

UT_TEST(test_corrupt_wal_and_missing_segment)
{
	for (int fault = 0; fault < 6; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		int fd, before;
		XLogLongPageHeaderData page;
		XLogRecord rec;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		fd = open(segment_path, O_RDWR);
		if (fd < 0)
			abort();
		if (fault < 3) {
			if (pread(fd, &page, sizeof(page), 0) != sizeof(page))
				abort();
			if (fault == 0)
				page.std.xlp_thread_id++;
			if (fault == 1)
				page.std.xlp_tli++;
			if (fault == 2)
				page.xlp_sysid++;
			if (pwrite(fd, &page, sizeof(page), 0) != sizeof(page))
				abort();
		} else if (fault == 3) {
			if (pread(fd, &rec, SizeOfXLogRecord, b.record_start % wal_segment_size)
				!= SizeOfXLogRecord)
				abort();
			rec.xl_crc ^= 1;
			if (pwrite(fd, &rec, SizeOfXLogRecord, b.record_start % wal_segment_size)
				!= SizeOfXLogRecord)
				abort();
		} else if (fault == 4) {
			(void)record_write(b.record_start, a.record_start - 8, 24);
		} else if (unlink(segment_path))
			abort();
		if (close(fd))
			abort();
		before = fd_count();
		UT_ASSERT(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(rename_calls, 0);
		UT_ASSERT_EQ(fd_count(), before);
	}
}

UT_TEST(test_path_and_current_inputs)
{
	for (int fault = 0; fault < 7; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		char path[MAXPGPATH];
		int before;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		if (fault == 0) {
			if (chmod(generation, 0770))
				abort();
		}
		if (fault == 1) {
			if (chmod(prefix_path, 0660))
				abort();
		}
		if (fault == 2) {
			if (truncate(prefix_path, 255))
				abort();
		}
		if (fault == 3) {
			snprintf(path, sizeof(path), "%s.saved", prefix_path);
			if (rename(prefix_path, path) || symlink(path, prefix_path))
				abort();
		}
		if (fault == 4) {
			mutation_kind = 3;
			mutate_namespace();
		}
		if (fault == 5) {
			if (chmod(segment_path, 0660))
				abort();
		}
		if (fault == 6) {
			snprintf(path, sizeof(path), "%s.saved", segment_path);
			if (rename(segment_path, path) || symlink(path, segment_path))
				abort();
		}
		before = fd_count();
		UT_ASSERT(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(rename_calls, 0);
		UT_ASSERT_EQ(fd_count(), before);
	}
}

UT_TEST(test_namespace_changes_and_owned_cleanup)
{
	for (int fault = 1; fault <= 5; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		char path[MAXPGPATH];
		struct stat st;
		int before;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		mutation_kind = fault;
		mutation_at_sync = fault == 5 ? 3 : 1;
		before = fd_count();
		UT_ASSERT(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(rename_calls, 0);
		UT_ASSERT_EQ(fd_count(), before);
		if (fault == 5) {
			snprintf(path, sizeof(path), "%s/durable_prefix/.current.79.6", generation);
			UT_ASSERT_EQ(stat(path, &st), 0);
			UT_ASSERT_EQ(st.st_size, 7);
		}
	}
}

UT_TEST(test_write_readback_collision_and_reuse_refusal)
{
	for (int fault = 0; fault < 6; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		char path[MAXPGPATH];
		struct stat st;
		int before;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		snprintf(path, sizeof(path), "%s/durable_prefix/.current.79.6", generation);
		if (fault == 0)
			fail_write = true;
		if (fault == 1)
			fail_readback = true;
		if (fault == 2)
			bytes_write(path, "foreign", 7);
		if (fault == 3)
			enableFsync = false;
		if (fault == 4) {
			b = a;
			fail_sync = 3;
		}
		if (fault == 5) {
			b = a;
			change_epoch = 4;
		}
		before = fd_count();
		UT_ASSERT(cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered) != 0);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(fd_count(), before);
		if (fault == 2) {
			UT_ASSERT_EQ(stat(path, &st), 0);
			UT_ASSERT_EQ(st.st_size, 7);
		} else
			UT_ASSERT(lstat(path, &st) != 0 && errno == ENOENT);
	}
}

UT_TEST(test_native_flush_publication_order)
{
	ClusterWalDurablePrefix a, b, c;
	fixture();
	a = base_record();
	b = record_write(a.exclusive_end, a.record_start, 24);
	c = record_write(b.exclusive_end, b.record_start, 2 * XLOG_BLCKSZ);
	memset(&native_ctl, 0, sizeof(native_ctl));
	native_ctl.LogwrtResult.Flush = a.exclusive_end;
	LogwrtResult.Write = c.record_start + XLOG_BLCKSZ;
	native_flush_tail(1);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.exclusive_end);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Write, c.record_start + XLOG_BLCKSZ);
	check_result(b.exclusive_end, b, 6);
	/* A failed promise must not publish Flush or return as a successful commit. */
	expect_panic = true;
	fail_sync = sync_calls + 1;
	LogwrtResult.Write = c.exclusive_end;
	if (sigsetjmp(native_error, 1) == 0) {
		native_flush_tail(1);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.exclusive_end);
	expect_panic = false;
	/* Legacy/noncluster native completion has no PGWP requirement. */
	cluster_shared_config = false;
	fail_sync = 0;
	rename_calls = 0;
	native_flush_tail(1);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, c.exclusive_end);
	UT_ASSERT_EQ(rename_calls, 0);
}

/* Real final-write hook: healthy epoch/token publication races must retain
 * the old ACK frontier, not crash the survivor. A later successful call must
 * validate the actual current file (including a prefix already renamed). */
static void
native_reconfig_case(int mutation)
{
	ClusterWalDurablePrefix a, b;
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
	change_epoch = mutation > 0 ? mutation : 0;
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
	native_flush_tail(1);
	UT_ASSERT_EQ(native_ctl.LogwrtResult.Flush, b.exclusive_end);
	check_result(b.exclusive_end, b, 6);
	UT_ASSERT_EQ(rename_calls, 1);
}

UT_TEST(test_native_survivor_waits_for_epoch_token)
{
	native_reconfig_case(0);
}
UT_TEST(test_native_survivor_waits_for_token_publication)
{
	native_reconfig_case(-1);
}
UT_TEST(test_native_survivor_retries_before_prefix_rename)
{
	native_reconfig_case(1);
}
UT_TEST(test_native_survivor_rechecks_published_prefix)
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
	ClusterWalDurablePrefix a, b;
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
		check_result(b.exclusive_end, b, 6);
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
	ClusterWalDurablePrefix a, b;
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

UT_TEST(test_waitable_state_never_masks_lost_permission)
{
	for (int fault = 0; fault < 4; ++fault) {
		ClusterWalDurablePrefix a, b;
		XLogRecPtr covered = 123;
		ClusterControlRootResult result;
		fixture();
		a = base_record();
		b = record_write(a.exclusive_end, a.record_start, 24);
		fence_epoch_lag = prebump = true;
		switch (fault) {
		case 0:
			self_fenced = true;
			break;
		case 1:
			fence_expired = true;
			break;
		case 2:
			++incarnation;
			break;
		case 3:
			active = false;
			break;
		}
		result = cluster_wal_durable_publish(1, b.exclusive_end, a.exclusive_end, &covered);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(result != CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
		UT_ASSERT_EQ(covered, 0);
		UT_ASSERT_EQ(sync_calls, 0);
		UT_ASSERT_EQ(rename_calls, 0);
		check_result(a.exclusive_end, a, 5);
	}
}

int
main(void)
{
	UT_PLAN(31);
	CritSectionCount = 0;
	cluster_wal_durable_publish_init();
	UT_RUN(test_group_flush_complete_and_reuse);
	UT_RUN(test_partial_record_does_not_become_flush);
	UT_RUN(test_cross_page_and_segment);
	UT_RUN(test_refuses_runtime_and_prefix_inputs);
	UT_RUN(test_sync_rename_and_epoch_failures);
	UT_RUN(test_corrupt_wal_and_missing_segment);
	UT_RUN(test_path_and_current_inputs);
	UT_RUN(test_namespace_changes_and_owned_cleanup);
	UT_RUN(test_write_readback_collision_and_reuse_refusal);
	UT_RUN(test_native_flush_publication_order);
	UT_RUN(test_native_survivor_waits_for_epoch_token);
	UT_RUN(test_native_survivor_waits_for_token_publication);
	UT_RUN(test_native_survivor_retries_before_prefix_rename);
	UT_RUN(test_native_survivor_rechecks_published_prefix);
	UT_RUN(test_foreground_flush_reconfiguration_wait);
	UT_RUN(test_foreground_flush_reconfiguration_race);
	UT_RUN(test_background_flush_reconfiguration_wait);
	UT_RUN(test_background_flush_reconfiguration_race);
	UT_RUN(test_flush_hang_never_acknowledges);
	UT_RUN(test_native_survivor_waits_for_prebump);
	UT_RUN(test_wal_buffer_reuse_releases_mapping_lock_before_wait);
	UT_RUN(test_waitable_state_never_masks_lost_permission);
	UT_RUN(test_foreground_entry_rechecks_before_physical_write);
	UT_RUN(test_background_entry_rechecks_before_physical_write);
	UT_RUN(test_repeated_entry_races_do_not_reset_hang_budget);
	UT_RUN(test_startup_first_prefix_requires_real_independent_records);
	UT_RUN(test_startup_binding_refuses_nonowner_and_nonempty_inputs);
	UT_RUN(test_startup_empty_does_not_authorize_ordinary_flush);
	UT_RUN(test_startup_rejects_old_link_partial_header_and_corrupt_first_record);
	UT_RUN(test_startup_first_publication_persistence_failures_reobserve_exact_prefix);
	UT_RUN(test_native_startup_flush_acknowledges_only_published_first_record);
	if (scratch[0] && !rmtree(scratch, true))
		abort();
	pfree(cluster_wal_threads_dir);
	pfree(DataDir);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
