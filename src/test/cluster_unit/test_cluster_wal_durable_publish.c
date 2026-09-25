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
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int wal_segment_size = 1024 * 1024;
bool enableFsync = true;
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0;
char *cluster_wal_threads_dir, *DataDir;
int MyProcPid = 79;
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
static bool held = true, active = true, member = true, fence = true, prebump, have_ref = true;
static uint64 epoch = 7, incarnation = 99;
static ClusterWalDurablePrefixRef ref;
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
LWLockHeldByMeInMode(LWLock *l pg_attribute_unused(), LWLockMode m)
{
	return held && m == LW_EXCLUSIVE;
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
	o->epoch_current = o->authorized_epoch = epoch;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *o)
{
	*o = ref;
	return have_ref;
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
	if (!expect_panic || level != PANIC)
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
static NativeResult LogwrtResult;
static struct {
	NativeResult LogwrtResult, LogwrtRqst;
	int info_lck;
} native_ctl;
#define XLogCtl (&native_ctl)
#define SpinLockAcquire(lock) ((void)(lock))
#define SpinLockRelease(lock) ((void)(lock))
static void
WalSndWakeupRequest(void)
{}
#include "test_cluster_wal_publish_tail.inc"
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
	enableFsync = cluster_enabled = cluster_shared_config = true;
	prebump = false;
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

int
main(void)
{
	UT_PLAN(10);
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
	if (scratch[0] && !rmtree(scratch, true))
		abort();
	pfree(cluster_wal_threads_dir);
	pfree(DataDir);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
