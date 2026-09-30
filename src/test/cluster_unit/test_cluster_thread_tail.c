/* PGRAC: actual thread WAL tail reader, real files and native WAL decoder.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_wal_state.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/wait_event.h"
#include "unit_test.h"
#include "wal_test_fixture.h"

UT_DEFINE_GLOBALS();
int wal_segment_size = 1024 * 1024;
bool cluster_shared_config;
bool enableFsync = true;
static unsigned tail_sync_count, tail_fail_sync;

int
pg_fsync(int fd)
{
	if (++tail_sync_count == tail_fail_sync) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
char *cluster_wal_threads_dir;
static char scratch[MAXPGPATH], generation[MAXPGPATH], prefix_path[MAXPGPATH];
static ClusterWalSourceRef ref;
static TimeLineID local_tli = 7;

TimeLineID
GetWALInsertionTimeLine(void)
{
	return local_tli;
}
int
BasicOpenFile(const char *path, int flags)
{
	return open(path, flags, 0600);
}
void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s %s:%d\n", c, f, l);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	abort();
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code pg_attribute_unused())
{
	abort();
}
int
errcode_for_file_access(void)
{
	abort();
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	abort();
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	abort();
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	abort();
}
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount, CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int read_action, close_action, suffix_read_action;
static XLogRecPtr mutation_position;
static WalTestRecord changed_prefix;
static ssize_t tail_test_pread(int fd, void *bytes, size_t len, off_t offset);
static int tail_test_close(int fd);
static void overwrite(XLogRecPtr position, const void *bytes, size_t len);

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}
void
ProcessInterrupts(void)
{
	InterruptPending = false;
	pg_re_throw();
}

#include "test_cluster_thread_tail_legacy.inc"
#define pread tail_test_pread
#define close tail_test_close
#include "../../backend/cluster/cluster_wal_tail.c"
#undef pread
#undef close

static void
bytes_write(const char *path, const void *bytes, size_t n)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, n) != (ssize_t)n || close(fd))
		abort();
}

static void
prefix_write(WalTestRecord p)
{
	uint8 bytes[WAL_TEST_OBSOLETE_BYTES];
	wal_test_obsolete_bytes(&ref, &p, bytes);
	bytes_write(prefix_path, bytes, sizeof(bytes));
}

static void
fixture(void)
{
	char path[MAXPGPATH];
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	ClusterWalThreadClaimV2 claim;
	pg_cryptohash_ctx *hash;
	if (scratch[0] && !rmtree(scratch, true))
		abort();
	strlcpy(scratch, "/tmp/pgrac-thread-tail-XXXXXX", sizeof(scratch));
	if (mkdtemp(scratch) == NULL)
		abort();
	cluster_wal_threads_dir = scratch;
	snprintf(path, sizeof(path), "%s/thread_2", scratch);
	if (mkdir(path, 0700))
		abort();
	snprintf(generation, sizeof(generation), "%s/thread_2/generation_99", scratch);
	if (mkdir(generation, 0700))
		abort();
	snprintf(path, sizeof(path), "%s/durable_prefix", generation);
	if (mkdir(path, 0700))
		abort();
	snprintf(prefix_path, sizeof(prefix_path), "%s/current", path);
	memset(&claim, 0, sizeof(claim));
	claim.identity.system_identifier = UINT64CONST(0x1122334455667788);
	memset(claim.identity.storage_uuid, 3, 16);
	memset(claim.identity.authority_uuid, 5, 16);
	claim.identity.origin_node_id = 1;
	claim.identity.origin_thread_id = 2;
	claim.identity.origin_owner_incarnation = 99;
	claim.identity.root_lineage_seq = 9;
	claim.identity.thread_claim_created_at = 23;
	claim.database_incarnation = 7;
	claim.config_generation = claim.claim_generation = 1;
	if (cluster_wal_claim_v2_encode(&claim, bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		abort();
	memset(&ref, 0, sizeof(ref));
	ref.claim.identity = claim.identity;
	memcpy(&ref.claim.identity.thread_claim_crc32c, bytes + 104, 4);
	ref.claim.database_incarnation = 7;
	ref.claim.max_config_generation = 1;
	ref.timeline = local_tli = 7;
	hash = pg_cryptohash_create(PG_SHA256);
	if (!hash || pg_cryptohash_init(hash) || pg_cryptohash_update(hash, bytes, sizeof(bytes))
		|| pg_cryptohash_final(hash, ref.claim.claim_sha256, 32))
		abort();
	pg_cryptohash_free(hash);
	snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
	bytes_write(path, bytes, sizeof(bytes));
	cluster_shared_config = false;
	read_action = close_action = suffix_read_action = 0;
	InterruptPending = false;
}

/* Construct native WAL, including continuation and previous-record links. */
static WalTestRecord
record_write(const char *directory, XLogRecPtr start, XLogRecPtr previous, size_t payload)
{
	XLogRecord record;
	size_t head = payload < 256 ? 2 : 5, total = SizeOfXLogRecord + head + payload, used = 0;
	uint8 *bytes = palloc0(total);
	XLogRecPtr pos = start;
	memset(&record, 0, sizeof(record));
	record.xl_tot_len = total;
	record.xl_prev = previous;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_info = XLOG_NOOP;
	bytes[SizeOfXLogRecord] = payload < 256 ? XLR_BLOCK_ID_DATA_SHORT : XLR_BLOCK_ID_DATA_LONG;
	if (head == 2)
		bytes[SizeOfXLogRecord + 1] = payload;
	else {
		uint32 n = payload;
		memcpy(bytes + SizeOfXLogRecord + 1, &n, 4);
	}
	memset(bytes + SizeOfXLogRecord + head, 0x45, payload);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, total - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	while (used < total) {
		char filename[MAXFNAMELEN], path[MAXPGPATH];
		XLogSegNo seg;
		XLogLongPageHeaderData header;
		XLogPageHeaderData existing;
		XLogRecPtr page = pos - pos % XLOG_BLCKSZ;
		size_t offset = pos % XLOG_BLCKSZ, count;
		int fd;
		XLByteToSeg(pos, seg, wal_segment_size);
		XLogFileName(filename, ref.timeline, seg, wal_segment_size);
		snprintf(path, sizeof(path), "%s/%s", directory, filename);
		fd = open(path, O_RDWR | O_CREAT, 0600);
		if (fd < 0 || ftruncate(fd, wal_segment_size))
			abort();
		memset(&header, 0, sizeof(header));
		header.std.xlp_magic = XLOG_PAGE_MAGIC;
		header.std.xlp_info = XLP_LONG_HEADER;
		header.std.xlp_tli = ref.timeline;
		header.std.xlp_thread_id = ref.claim.identity.origin_thread_id;
		header.std.xlp_pageaddr = seg * wal_segment_size;
		header.xlp_sysid = ref.claim.identity.system_identifier;
		header.xlp_seg_size = wal_segment_size;
		header.xlp_xlog_blcksz = XLOG_BLCKSZ;
		if (pread(fd, &existing, sizeof(existing), 0) != sizeof(existing))
			abort();
		if (!existing.xlp_magic && pwrite(fd, &header, SizeOfXLogLongPHD, 0) != SizeOfXLogLongPHD)
			abort();
		if (!offset)
			offset = page % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD;
		if (pread(fd, &existing, sizeof(existing), page % wal_segment_size) != sizeof(existing))
			abort();
		if (!existing.xlp_magic
			|| (used
				&& offset == (page % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD))) {
			header.std.xlp_pageaddr = page;
			header.std.xlp_info = page % wal_segment_size ? 0 : XLP_LONG_HEADER;
			if (used) {
				header.std.xlp_info |= XLP_FIRST_IS_CONTRECORD;
				header.std.xlp_rem_len = total - used;
			}
			if (pwrite(fd, &header,
					   page % wal_segment_size ? SizeOfXLogShortPHD : SizeOfXLogLongPHD,
					   page % wal_segment_size)
				< 0)
				abort();
		}
		count = Min(total - used, XLOG_BLCKSZ - offset);
		if (pwrite(fd, bytes + used, count, page % wal_segment_size + offset) != (ssize_t)count
			|| close(fd))
			abort();
		used += count;
		pos = page + offset + count;
	}
	pfree(bytes);
	return (WalTestRecord){ 5, MAXALIGN(pos), start, record.xl_crc };
}

static void
segment_path(XLogRecPtr position, char *path)
{
	XLogSegNo seg;
	char filename[MAXFNAMELEN];
	XLByteToSeg(position, seg, wal_segment_size);
	XLogFileName(filename, ref.timeline, seg, wal_segment_size);
	snprintf(path, MAXPGPATH, "%s/%s", generation, filename);
}

static ssize_t
tail_test_pread(int fd, void *bytes, size_t len, off_t offset)
{
	ssize_t n;
	int action = read_action;
	read_action = 0;
	/* Exercise the later-header scan, not the preceding native WAL reads. */
	if (suffix_read_action != 0 && len == SizeOfXLogLongPHD && offset == 0) {
		action = suffix_read_action;
		suffix_read_action = 0;
	}
	if (action == 1) {
		errno = EIO;
		return -1;
	}
	n = pread(fd, bytes, len, offset);
	if (action == 2) {
		char path[MAXPGPATH];
		snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
		bytes_write(path, "bad", 3);
	} else if (action == 3) {
		char path[MAXPGPATH];
		snprintf(path, sizeof(path), "%s.saved", generation);
		if (rename(generation, path) || mkdir(generation, 0700))
			abort();
	} else if (action == 4) {
		char path[MAXPGPATH], saved[MAXPGPATH];
		uint8 *copy = palloc(wal_segment_size);
		segment_path(mutation_position, path);
		snprintf(saved, sizeof(saved), "%s.saved", path);
		if (pread(fd, copy, wal_segment_size, 0) != wal_segment_size || rename(path, saved))
			abort();
		bytes_write(path, copy, wal_segment_size);
		pfree(copy);
	} else if (action == 5)
		InterruptPending = true;
	return n;
}

static int
tail_test_close(int fd)
{
	int result = close(fd);
	if (close_action) {
		close_action = 0;
		errno = EIO;
		return -1;
	}
	return result;
}

static int
fd_count(void)
{
	int count = 0;
	for (int fd = 0; fd < 1024; ++fd)
		if (fcntl(fd, F_GETFD) >= 0)
			++count;
	return count;
}

static WalTestRecord
base_record(void)
{
	WalTestRecord p
		= record_write(generation, wal_segment_size + SizeOfXLogLongPHD, 0, 24);
	prefix_write(p);
	return p;
}

static ClusterControlRootResult
observe(XLogRecPtr minimum, ClusterWalTailObservation *out)
{
	int before = fd_count();
	ClusterControlRootResult result = cluster_wal_tail_observe(
		scratch, &ref, wal_segment_size, wal_segment_size + SizeOfXLogLongPHD, minimum, out);
	if (before != fd_count())
		abort();
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		ClusterWalTailObservation zero = { 0 };
		if (memcmp(out, &zero, sizeof(zero)) != 0)
			abort();
	}
	return result;
}

UT_TEST(exact_source_uses_foreign_timeline_and_generation)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	local_tli = 88;
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
	UT_ASSERT_EQ(out.last_record_crc, p.record_crc);
	UT_ASSERT_EQ(out.records, 1);
}

UT_TEST(complete_unacknowledged_tail_is_observed)
{
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start, 40);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, next.exclusive_end);
	UT_ASSERT_EQ(out.records, 2);
}

UT_TEST(native_tail_needs_claim_and_wal_without_a_flush_sidefile)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	UT_ASSERT_EQ(unlink(prefix_path), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
	UT_ASSERT_EQ(out.last_record_crc, p.record_crc);
}

UT_TEST(loss_beyond_every_known_bound_is_a_media_failure_boundary)
{
	char path[MAXPGPATH];
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start, 40);
	/* Only p is root-selected. There is no separate persistent commit bound. */
	UT_ASSERT_EQ(unlink(prefix_path), 0);
	segment_path(p.exclusive_end, path);
	UT_ASSERT_EQ(truncate(path, p.exclusive_end % wal_segment_size), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
	/* A selected root bound would make the same loss a known missing input. */
	UT_ASSERT_EQ(observe(next.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(unacknowledged_torn_tail_is_not_lost_commit)
{
	char path[MAXPGPATH];
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	(void)record_write(generation, p.exclusive_end, p.record_start, 200);
	segment_path(p.exclusive_end, path);
	UT_ASSERT_EQ(truncate(path, p.exclusive_end % wal_segment_size + SizeOfXLogRecord + 5), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
}

/* A complete corrupt record is positive evidence of broken input, even when
 * a prior record has already satisfied the selected lower bound. */
UT_TEST(complete_corrupt_suffix_is_not_a_normal_tail)
{
	for (int fault = 0; fault < 4; ++fault) {
		WalTestRecord p, next;
		ClusterWalTailObservation out;
		fixture();
		p = base_record();
		next = record_write(generation, p.exclusive_end,
							fault == 1 ? p.record_start + 8 : p.record_start, 40);
		if (fault == 0) {
			uint8 damaged = 0x72;
			overwrite(next.record_start + SizeOfXLogRecord + 2, &damaged, sizeof(damaged));
		} else if (fault == 2) {
			uint32 short_length = 7;
			overwrite(next.record_start, &short_length, sizeof(short_length));
		} else if (fault == 3) {
			uint32 zero_length = 0;
			overwrite(next.record_start, &zero_length, sizeof(zero_length));
		}
		UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	}
}

UT_TEST(unwritten_gap_before_initialized_page_is_not_a_tail)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	(void)record_write(generation, wal_segment_size + 3 * XLOG_BLCKSZ + SizeOfXLogShortPHD,
					   p.record_start, 40);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(whole_segment_loss_refuses_a_known_root_bound)
{
	char path[MAXPGPATH];
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start, 2 * wal_segment_size);
	prefix_write(next);
	segment_path(next.exclusive_end - 1, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(observe(next.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(complete_suffix_loss_refuses_a_known_root_bound)
{
	char path[MAXPGPATH];
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start, 40);
	prefix_write(next);
	segment_path(p.exclusive_end, path);
	UT_ASSERT_EQ(truncate(path, p.exclusive_end % wal_segment_size), 0);
	UT_ASSERT_EQ(observe(next.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(missing_or_bad_claim_never_uses_backup)
{
	char backup[MAXPGPATH], claim_path[MAXPGPATH];
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	snprintf(claim_path, sizeof(claim_path), "%s/pgrac_thread.claim", generation);
	snprintf(backup, sizeof(backup), "%s.bak", claim_path);
	UT_ASSERT_EQ(rename(claim_path, backup), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_ABSENT);
	bytes_write(claim_path, "bad", 3);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_SIZE);
}

UT_TEST(selected_record_crc_and_start_must_match)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	UT_ASSERT_EQ(cluster_wal_tail_observe_checkpoint(scratch, &ref, wal_segment_size,
		p.record_start, p.exclusive_end, p.record_start, p.record_crc ^ 1, &out),
		CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	UT_ASSERT_EQ(cluster_wal_tail_observe_checkpoint(scratch, &ref, wal_segment_size,
		p.record_start, p.exclusive_end, p.record_start + 8, p.record_crc, &out),
		CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(previous_record_link_is_verified)
{
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start - 8, 40);
	prefix_write(next);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

static void
overwrite(XLogRecPtr position, const void *bytes, size_t len)
{
	char path[MAXPGPATH];
	int fd;
	segment_path(position, path);
	fd = open(path, O_WRONLY);
	if (fd < 0 || pwrite(fd, bytes, len, position % wal_segment_size) != (ssize_t)len || close(fd))
		abort();
}

UT_TEST(corrupt_first_record_is_not_skipped)
{
	uint32 bad = 0;
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start, 40);
	prefix_write(next);
	overwrite(p.record_start, &bad, sizeof(bad));
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(many_segment_record_uses_bounded_fds)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = record_write(generation, wal_segment_size + SizeOfXLogLongPHD, 0, 4 * wal_segment_size);
	prefix_write(p);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
	UT_ASSERT_EQ(out.records, 1);
}

UT_TEST(foreign_page_identity_refuses)
{
	uint16 other = 4;
	TimeLineID timeline = 91;
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	overwrite(wal_segment_size + offsetof(XLogPageHeaderData, xlp_thread_id), &other,
			  sizeof(other));
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	fixture();
	p = base_record();
	overwrite(wal_segment_size + offsetof(XLogPageHeaderData, xlp_tli), &timeline,
			  sizeof(timeline));
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(replaced_generation_or_segment_refuses)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	read_action = 3;
	UT_ASSERT_NE(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	fixture();
	p = base_record();
	mutation_position = p.record_start;
	read_action = 4;
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
}

UT_TEST(moving_claim_refuses_old_observation)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	changed_prefix = record_write(generation, p.exclusive_end, p.record_start, 40);
	changed_prefix.sequence = p.sequence + 1;
	read_action = 2;
	UT_ASSERT_NE(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(real_io_or_close_failure_is_not_torn_tail)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	read_action = 1;
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	close_action = 1;
	UT_ASSERT_NE(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(cancellation_closes_owned_descriptors)
{
	WalTestRecord p;
	static ClusterWalTailObservation out;
	sigjmp_buf caller;
	int before;
	fixture();
	p = base_record();
	before = fd_count();
	PG_exception_stack = &caller;
	if (sigsetjmp(caller, 0) == 0) {
		read_action = 5;
		(void)observe(p.exclusive_end, &out);
		UT_ASSERT(false);
	} else {
		ClusterWalTailObservation zero = { 0 };
		UT_ASSERT_EQ(fd_count(), before);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	}
	PG_exception_stack = NULL;
}

UT_TEST(exact_segment_end_allows_missing_unacknowledged_next_segment)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	size_t usable;
	fixture();
	usable = wal_segment_size - SizeOfXLogLongPHD
			 - (wal_segment_size / XLOG_BLCKSZ - 1) * SizeOfXLogShortPHD;
	p = record_write(generation, wal_segment_size + SizeOfXLogLongPHD, 0,
					 usable - SizeOfXLogRecord - 5);
	prefix_write(p);
	UT_ASSERT_EQ(p.exclusive_end, 2 * wal_segment_size);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
}

UT_TEST(missing_unpromised_middle_segment_is_not_a_tail)
{
	char path[MAXPGPATH];
	WalTestRecord p;
	ClusterWalTailObservation out;
	size_t usable;
	fixture();
	usable = wal_segment_size - SizeOfXLogLongPHD
			 - (wal_segment_size / XLOG_BLCKSZ - 1) * SizeOfXLogShortPHD;
	p = record_write(generation, wal_segment_size + SizeOfXLogLongPHD, 0,
					 usable - SizeOfXLogRecord - 5);
	prefix_write(p);
	UT_ASSERT_EQ(p.exclusive_end, 2 * wal_segment_size);
	(void)record_write(generation, p.exclusive_end + SizeOfXLogLongPHD, p.record_start,
					   2 * wal_segment_size);
	segment_path(p.exclusive_end, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(unwritten_suffix_does_not_hide_initialized_later_segment)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	/* No record continues the first segment, but the later exact-address
	 * header proves the stream did not actually end at that zero padding. */
	(void)record_write(generation, 3 * wal_segment_size + SizeOfXLogLongPHD, p.record_start, 40);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(preallocated_and_recycled_later_segments_are_not_redo_evidence)
{
	for (int recycled = 0; recycled < 2; recycled++) {
		char path[MAXPGPATH], old_path[MAXPGPATH];
		WalTestRecord p;
		ClusterWalTailObservation out;
		int fd;
		fixture();
		p = base_record();
		segment_path(4 * wal_segment_size, path);
		if (recycled) {
			/* Native recycling renames a segment without rewriting its pages. */
			(void)record_write(generation, 2 * wal_segment_size + SizeOfXLogLongPHD, p.record_start,
							   40);
			segment_path(2 * wal_segment_size, old_path);
			UT_ASSERT_EQ(rename(old_path, path), 0);
		} else {
			fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(ftruncate(fd, wal_segment_size), 0);
			UT_ASSERT_EQ(close(fd), 0);
		}
		UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
		UT_ASSERT_EQ(out.records, 1);
	}
}

UT_TEST(incomplete_final_record_can_span_initialized_segments)
{
	char path[MAXPGPATH];
	WalTestRecord p, next;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	next = record_write(generation, p.exclusive_end, p.record_start, 2 * wal_segment_size);
	segment_path(next.exclusive_end - 1, path);
	UT_ASSERT_EQ(truncate(path, (next.exclusive_end - 1) % wal_segment_size - 16), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.complete_end, p.exclusive_end);
}

UT_TEST(later_segment_must_match_exact_source_identity)
{
	uint64 foreign = 12345678;
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	(void)record_write(generation, 3 * wal_segment_size + SizeOfXLogLongPHD, p.record_start, 40);
	overwrite(3 * wal_segment_size + offsetof(XLogLongPageHeaderData, xlp_sysid), &foreign,
			  sizeof(foreign));
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(later_segment_scan_preserves_failure_cleanup_and_namespace)
{
	for (int fault = 0; fault < 4; fault++) {
		char path[MAXPGPATH];
		WalTestRecord p;
		ClusterWalTailObservation out, zero = { 0 };
		volatile bool caught = false;
		int fd, before;
		fixture();
		p = base_record();
		mutation_position = 4 * wal_segment_size;
		segment_path(mutation_position, path);
		fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(ftruncate(fd, wal_segment_size), 0);
		UT_ASSERT_EQ(close(fd), 0);
		suffix_read_action = fault == 0 ? 1 : fault == 1 ? 3 : fault == 2 ? 4 : 5;
		before = fd_count();
		memset(&out, 0xa5, sizeof(out));
		PG_TRY();
		{
			ClusterControlRootResult result = observe(p.exclusive_end, &out);
			UT_ASSERT(fault != 3);
			UT_ASSERT_EQ(result, fault == 0 ? CLUSTER_CONTROL_ROOT_IO_ERROR
											: CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3);
		UT_ASSERT_EQ(suffix_read_action, 0);
		UT_ASSERT_EQ(fd_count(), before);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	}
}

UT_TEST(checkpoint_floor_and_claim_cannot_be_substituted)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	UT_ASSERT_EQ(observe(p.exclusive_end + 8, &out), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
	ref.claim.claim_sha256[0] ^= 1;
	UT_ASSERT_NE(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(checkpoint_identity_is_not_implied_by_a_valid_tail)
{
	WalTestRecord p;
	ClusterWalTailObservation out, zero = { 0 };
	fixture();
	p = base_record();
	UT_ASSERT_EQ(cluster_wal_tail_observe_checkpoint(scratch, &ref, wal_segment_size,
													 p.record_start, p.exclusive_end,
													 p.record_start, p.record_crc, &out),
				 0);
	UT_ASSERT_NE(cluster_wal_tail_observe_checkpoint(scratch, &ref, wal_segment_size,
													 p.record_start, p.exclusive_end,
													 p.record_start, p.record_crc ^ 1, &out),
				 0);
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
}

UT_TEST(foreign_database_after_promise_is_not_a_torn_tail)
{
	uint64 other_sysid = 12345678;
	WalTestRecord p;
	ClusterWalTailObservation out;
	size_t usable;
	fixture();
	usable = wal_segment_size - SizeOfXLogLongPHD
			 - (wal_segment_size / XLOG_BLCKSZ - 1) * SizeOfXLogShortPHD;
	p = record_write(generation, wal_segment_size + SizeOfXLogLongPHD, 0,
					 usable - SizeOfXLogRecord - 5);
	prefix_write(p);
	(void)record_write(generation, p.exclusive_end + SizeOfXLogLongPHD, p.record_start, 40);
	overwrite(p.exclusive_end + offsetof(XLogLongPageHeaderData, xlp_sysid), &other_sysid,
			  sizeof(other_sysid));
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(invalid_lower_does_not_search_forward)
{
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	UT_ASSERT_NE(cluster_wal_tail_observe(scratch, &ref, wal_segment_size, wal_segment_size,
										  p.exclusive_end, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(foreign_identity_after_promise_is_not_a_torn_tail)
{
	uint16 other = 4;
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = record_write(generation, wal_segment_size + SizeOfXLogLongPHD, 0,
					 XLOG_BLCKSZ - SizeOfXLogLongPHD - SizeOfXLogRecord - 5);
	prefix_write(p);
	UT_ASSERT_EQ(p.exclusive_end, wal_segment_size + XLOG_BLCKSZ);
	(void)record_write(generation, p.exclusive_end + SizeOfXLogShortPHD, p.record_start, 40);
	overwrite(p.exclusive_end + offsetof(XLogPageHeaderData, xlp_thread_id), &other, sizeof(other));
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
}

UT_TEST(untrusted_segment_types_and_permissions_refuse)
{
	char path[MAXPGPATH], target[MAXPGPATH];
	WalTestRecord p;
	ClusterWalTailObservation out;
	fixture();
	p = base_record();
	segment_path(p.record_start, path);
	UT_ASSERT_EQ(chmod(path, 0660), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(chmod(path, 0600), 0);
	snprintf(target, sizeof(target), "%s.target", path);
	UT_ASSERT_EQ(rename(path, target), 0);
	UT_ASSERT_EQ(symlink(target, path), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(mkfifo(path, 0600), 0);
	UT_ASSERT_EQ(observe(p.exclusive_end, &out), CLUSTER_CONTROL_ROOT_IO_ERROR);
}

UT_TEST(legacy_reader_still_works_without_shared_config)
{
	char flat[MAXPGPATH];
	XLogRecPtr end = 0, lower = wal_segment_size + SizeOfXLogLongPHD;
	WalTestRecord record;
	fixture();
	snprintf(flat, sizeof(flat), "%s/thread_2", scratch);
	record = record_write(flat, lower, 0, 24);
	UT_ASSERT_EQ(validated_end_inner(2, lower, record.exclusive_end, &end), CLUSTER_THREADREC_DONE);
	UT_ASSERT_EQ(end, record.exclusive_end);
}

UT_TEST(pre2_cannot_accept_lost_promised_suffix_from_legacy_reader)
{
	char flat[MAXPGPATH];
	XLogRecPtr end = 0, lower = wal_segment_size + SizeOfXLogLongPHD;
	WalTestRecord old, promised;
	fixture();
	snprintf(flat, sizeof(flat), "%s/thread_2", scratch);
	old = record_write(flat, lower, 0, 24);
	promised = (WalTestRecord){ 6, 3 * wal_segment_size, 2 * wal_segment_size + 48, 456 };
	prefix_write(promised);
	cluster_shared_config = true;
	UT_ASSERT_EQ(validated_end_inner(2, lower, old.exclusive_end, &end), CLUSTER_THREADREC_BLOCKED);
	UT_ASSERT_EQ(end, 0);
}

UT_TEST(pre2_cannot_use_flat_decoy_when_exact_generation_missing)
{
	char flat[MAXPGPATH];
	XLogRecPtr end = 0, lower = wal_segment_size + SizeOfXLogLongPHD;
	WalTestRecord old;
	fixture();
	snprintf(flat, sizeof(flat), "%s/thread_2", scratch);
	old = record_write(flat, lower, 0, 24);
	cluster_shared_config = true;
	UT_ASSERT_EQ(validated_end_inner(2, lower, old.exclusive_end, &end), CLUSTER_THREADREC_BLOCKED);
	UT_ASSERT_EQ(end, 0);
}

static ClusterControlRootResult
startup_observe(ClusterWalStartupObservation *out)
{
	int before = fd_count();
	ClusterControlRootResult result
		= cluster_wal_startup_observe(scratch, &ref, wal_segment_size, wal_segment_size, out);
	UT_ASSERT_EQ(fd_count(), before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		ClusterWalStartupObservation zero = { 0 };
		UT_ASSERT(memcmp(out, &zero, sizeof(zero)) == 0);
	}
	return result;
}

/* Independent native payload/CRC fixture; the production scanner and claim
 * codec do not manufacture the expected parameter maxima. */
static WalTestRecord
startup_record_with_block(XLogRecPtr start, XLogRecPtr previous, uint8 info, TransactionId xid,
						  const void *data, size_t len, bool block)
{
	uint8 bytes[SizeOfXLogRecord + 32 + 255] = { 0 };
	XLogRecord record = { 0 };
	RelFileLocator locator = { 1663, 5, 42 };
	XLogRecordBlockHeader block_header = { 0, MAIN_FORKNUM, 0 };
	BlockNumber block_number = 1;
	size_t offset = SizeOfXLogRecord;
	size_t extra = block ? sizeof(block_header) + sizeof(locator) + sizeof(block_number) : 0;
	WalTestRecord prefix = record_write(generation, start, previous, len + extra);
	if (len + extra > 255 || start % XLOG_BLCKSZ + SizeOfXLogRecord + 2 + len + extra > XLOG_BLCKSZ)
		abort();
	record.xl_tot_len = SizeOfXLogRecord + extra + 2 + len;
	record.xl_prev = previous;
	record.xl_xid = xid;
	record.xl_rmid = RM_XLOG_ID;
	record.xl_info = info;
	if (block) {
		memcpy(bytes + offset, &block_header, sizeof(block_header));
		offset += sizeof(block_header);
		memcpy(bytes + offset, &locator, sizeof(locator));
		offset += sizeof(locator);
		memcpy(bytes + offset, &block_number, sizeof(block_number));
		offset += sizeof(block_number);
	}
	bytes[offset++] = XLR_BLOCK_ID_DATA_SHORT;
	bytes[offset++] = len;
	memcpy(bytes + offset, data, len);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, extra + 2 + len);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	overwrite(start, bytes, record.xl_tot_len);
	prefix.record_crc = record.xl_crc;
	return prefix;
}

static WalTestRecord
startup_record(XLogRecPtr start, XLogRecPtr previous, uint8 info, TransactionId xid,
			   const void *data, size_t len)
{
	return startup_record_with_block(start, previous, info, xid, data, len, false);
}

static WalTestRecord
parameter_record(XLogRecPtr start, XLogRecPtr previous, xl_parameter_change parameters,
				 bool short_payload)
{
	return startup_record(start, previous, XLOG_PARAMETER_CHANGE, InvalidTransactionId, &parameters,
						  sizeof(parameters) - (short_payload ? 1 : 0));
}

UT_TEST(startup_rejects_invalid_fpw_payload)
{
	for (int fault = 0; fault < 2; fault++) {
		uint8 raw[2] = { 2, 0 };
		ClusterWalStartupObservation out;
		fixture();
		prefix_write(startup_record(wal_segment_size + SizeOfXLogLongPHD, 0, XLOG_FPW_CHANGE,
									InvalidTransactionId, raw, fault == 0 ? 1 : 2));
		UT_ASSERT_EQ(startup_observe(&out), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	}
}

UT_TEST(startup_rejects_short_checkpoint_payload)
{
	CheckPoint checkpoint = { 0 };
	ClusterWalStartupObservation out;
	fixture();
	prefix_write(startup_record(wal_segment_size + SizeOfXLogLongPHD, 0, XLOG_CHECKPOINT_SHUTDOWN,
								InvalidTransactionId, &checkpoint, sizeof(checkpoint) - 1));
	UT_ASSERT_EQ(startup_observe(&out), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(startup_captures_first_real_checkpoint_beyond_empty_promise)
{
	ClusterWalStartupObservation out;
	WalTestRecord first, second, third;
	CheckPoint checkpoint = { 0 };
	bool enabled = true;
	fixture();
	first = startup_record(wal_segment_size + SizeOfXLogLongPHD, 0, XLOG_FPW_CHANGE,
						   InvalidTransactionId, &enabled, sizeof(enabled));
	checkpoint.redo = first.record_start;
	checkpoint.ThisTimeLineID = checkpoint.PrevTimeLineID = ref.timeline;
	checkpoint.nextXid = FullTransactionIdFromU64(456);
	checkpoint.fullPageWrites = true;
	second = startup_record(first.exclusive_end, first.record_start, XLOG_CHECKPOINT_ONLINE,
							InvalidTransactionId, &checkpoint, sizeof(checkpoint));
	checkpoint.redo = second.exclusive_end;
	third = startup_record(second.exclusive_end, second.record_start, XLOG_CHECKPOINT_SHUTDOWN,
						   InvalidTransactionId, &checkpoint, sizeof(checkpoint));
	prefix_write((WalTestRecord){ 1, 0, 0, 0 });
	UT_ASSERT_EQ(startup_observe(&out), 0);
	UT_ASSERT_EQ(out.tail.records, 3);
	UT_ASSERT_EQ(out.tail.complete_end, third.exclusive_end);
	UT_ASSERT_EQ(out.checkpoint_records, 2);
	UT_ASSERT_EQ(out.checkpoint_start, second.record_start);
	UT_ASSERT_EQ(out.checkpoint_end, second.exclusive_end);
	UT_ASSERT_EQ(out.checkpoint_crc, second.record_crc);
	UT_ASSERT_EQ(out.checkpoint_info, XLOG_CHECKPOINT_ONLINE);
	UT_ASSERT_EQ(out.checkpoint.redo, first.record_start);
	UT_ASSERT_EQ(out.checkpoint.ThisTimeLineID, ref.timeline);
	UT_ASSERT_EQ(U64FromFullTransactionId(out.checkpoint.nextXid), 456);
	UT_ASSERT_EQ(out.fpw_records, 1);
	UT_ASSERT_EQ(out.unsupported_records, 0);
}

UT_TEST(startup_rejects_checkpoint_identity_and_native_inconsistency)
{
	for (int fault = 0; fault < 8; fault++) {
		ClusterWalStartupObservation out;
		CheckPoint checkpoint = { 0 };
		uint8 raw[sizeof(checkpoint)];
		fixture();
		checkpoint.redo = wal_segment_size + SizeOfXLogLongPHD;
		checkpoint.ThisTimeLineID = checkpoint.PrevTimeLineID = ref.timeline;
		checkpoint.nextXid = FullTransactionIdFromU64(456);
		if (fault == 0)
			checkpoint.ThisTimeLineID++;
		else if (fault == 1)
			checkpoint.PrevTimeLineID = 0;
		else if (fault == 2)
			checkpoint.redo = 0;
		else if (fault == 3)
			checkpoint.redo++;
		else if (fault == 4)
			checkpoint.nextXid = FullTransactionIdFromU64(1);
		else if (fault == 5)
			checkpoint.redo -= 8;
		else if (fault == 7)
			checkpoint.nextXid = FullTransactionIdFromU64(UINT64CONST(0x100000001));
		memcpy(raw, &checkpoint, sizeof(raw));
		if (fault == 6)
			raw[offsetof(CheckPoint, fullPageWrites)] = 2;
		prefix_write(startup_record(wal_segment_size + SizeOfXLogLongPHD, 0,
									XLOG_CHECKPOINT_SHUTDOWN, InvalidTransactionId, raw,
									sizeof(raw)));
		UT_ASSERT_EQ(startup_observe(&out), fault == 0 ? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
													   : CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	}
}

UT_TEST(startup_retains_fpw_off_and_unsupported_effects)
{
	ClusterWalStartupObservation out;
	WalTestRecord p;
	bool enabled = false;
	fixture();
	p = startup_record(wal_segment_size + SizeOfXLogLongPHD, 0, XLOG_FPW_CHANGE,
					   InvalidTransactionId, &enabled, sizeof(enabled));
	enabled = true;
	p = startup_record(p.exclusive_end, p.record_start, XLOG_FPW_CHANGE, InvalidTransactionId,
					   &enabled, sizeof(enabled));
	/* Valid physical records cannot silently disappear from the closure census. */
	p = startup_record(p.exclusive_end, p.record_start, XLOG_FPW_CHANGE, 123, &enabled,
					   sizeof(enabled));
	p = startup_record(p.exclusive_end, p.record_start, XLOG_NOOP, InvalidTransactionId, &enabled,
					   sizeof(enabled));
	p = startup_record_with_block(p.exclusive_end, p.record_start, XLOG_FPI, InvalidTransactionId,
								  &enabled, sizeof(enabled), true);
	prefix_write(p);
	UT_ASSERT_EQ(startup_observe(&out), 0);
	UT_ASSERT_EQ(out.tail.records, 5);
	UT_ASSERT_EQ(out.fpw_records, 3);
	UT_ASSERT(out.fpw_disabled);
	UT_ASSERT_EQ(out.unsupported_records, 3);
	UT_ASSERT_EQ(out.checkpoint_records, 0);
	UT_ASSERT_EQ(out.checkpoint_start, 0);
	UT_ASSERT_EQ(out.checkpoint_end, 0);
	UT_ASSERT_EQ(out.checkpoint_crc, 0);
	{
		CheckPoint zero = { 0 };
		UT_ASSERT_EQ(memcmp(&zero, &out.checkpoint, sizeof(zero)), 0);
	}
}

UT_TEST(startup_empty_is_actual_input_not_ordinary_tail_authority)
{
	ClusterWalStartupObservation out;
	ClusterWalTailObservation ordinary;
	fixture();
	prefix_write((WalTestRecord){ 1, 0, 0, 0 });
	UT_ASSERT_EQ(startup_observe(&out), 0);
	UT_ASSERT_EQ(out.tail.records, 0);
	UT_ASSERT_EQ(out.tail.complete_end, 0);
	UT_ASSERT_EQ(out.max_connections, 0);
	UT_ASSERT_EQ(observe(wal_segment_size + 200, &ordinary), CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC);
}

UT_TEST(startup_scans_complete_records_even_after_empty_promise)
{
	ClusterWalStartupObservation out;
	xl_parameter_change parameters = { 900, 31, 17, 9, 81, WAL_LEVEL_REPLICA, false, false };
	WalTestRecord first, next;
	fixture();
	first = parameter_record(wal_segment_size + SizeOfXLogLongPHD, 0, parameters, false);
	parameters.MaxConnections = 300;
	parameters.max_worker_processes = 61;
	parameters.max_wal_senders = 4;
	parameters.max_prepared_xacts = 2;
	parameters.max_locks_per_xact = 101;
	next = parameter_record(first.exclusive_end, first.record_start, parameters, false);
	prefix_write((WalTestRecord){ 1, 0, 0, 0 });
	UT_ASSERT_EQ(startup_observe(&out), 0);
	UT_ASSERT_EQ(out.tail.records, 2);
	UT_ASSERT_EQ(out.parameter_records, 2);
	UT_ASSERT_EQ(out.tail.complete_end, next.exclusive_end);
	UT_ASSERT_EQ(out.max_connections, 900);
	UT_ASSERT_EQ(out.max_worker_processes, 61);
	UT_ASSERT_EQ(out.max_wal_senders, 17);
	UT_ASSERT_EQ(out.max_prepared_xacts, 9);
	UT_ASSERT_EQ(out.max_locks_per_xact, 101);
	prefix_write(first);
	UT_ASSERT_EQ(startup_observe(&out), 0);
	UT_ASSERT_EQ(out.max_worker_processes, 61);
}

UT_TEST(startup_empty_does_not_hide_foreign_wal_or_bad_geometry)
{
	ClusterWalStartupObservation out, zero = { 0 };
	uint64 other_sysid = 7654321;
	fixture();
	(void)base_record();
	prefix_write((WalTestRecord){ 1, 0, 0, 0 });
	overwrite(wal_segment_size + offsetof(XLogLongPageHeaderData, xlp_sysid), &other_sysid,
			  sizeof(other_sysid));
	UT_ASSERT_EQ(startup_observe(&out), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	for (int fault = 0; fault < 4; fault++) {
		int size = fault == 0 ? 0 : wal_segment_size;
		XLogRecPtr start = fault == 1 ? 0 : fault == 2 ? wal_segment_size + 1 : UINT64_MAX;
		memset(&out, 0xff, sizeof(out));
		UT_ASSERT_EQ(cluster_wal_startup_observe(scratch, &ref, size, start, &out),
					 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	}
}

UT_TEST(startup_rejects_nonindependent_stream_and_malformed_parameters)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterWalStartupObservation out;
		xl_parameter_change parameters = { 900, 31, 17, 9, 81, WAL_LEVEL_REPLICA, false, false };
		WalTestRecord first;
		fixture();
		if (fault == 1)
			parameters.MaxConnections = -1;
		else if (fault == 2)
			parameters.max_locks_per_xact = 0;
		else if (fault == 3)
			parameters.wal_level = WAL_LEVEL_LOGICAL + 1;
		first = parameter_record(wal_segment_size + SizeOfXLogLongPHD,
								 fault == 0 ? wal_segment_size - 128 : 0, parameters, fault == 4);
		prefix_write(first);
		UT_ASSERT(startup_observe(&out) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	}
}

UT_TEST(startup_commit_ts_history_still_refuses)
{
	ClusterWalStartupObservation out;
	xl_parameter_change parameters = { 900, 31, 17, 9, 81, WAL_LEVEL_REPLICA, false, true };
	fixture();
	prefix_write(parameter_record(wal_segment_size + SizeOfXLogLongPHD, 0, parameters, false));
	UT_ASSERT_EQ(startup_observe(&out), CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED);
}

UT_TEST(startup_preserves_physical_failure_and_cancellation_boundaries)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterWalStartupObservation out;
		WalTestRecord first;
		char path[MAXPGPATH];
		fixture();
		first = base_record();
		if (fault == 0) {
			segment_path(first.record_start, path);
			UT_ASSERT_EQ(unlink(path), 0);
			(void)record_write(generation, 3 * wal_segment_size + SizeOfXLogLongPHD,
				first.record_start, 24);
		} else if (fault == 1) {
			uint8 damaged = 0x73;
			overwrite(first.record_start + SizeOfXLogRecord + 2, &damaged, 1);
		}
		else if (fault == 2) {
			changed_prefix = record_write(generation, first.exclusive_end, first.record_start, 40);
			changed_prefix.sequence = first.sequence + 1;
			read_action = 2;
		} else if (fault == 3)
			read_action = 1;
		else if (fault == 4) {
			mutation_position = first.record_start;
			read_action = 4;
		} else {
			snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		UT_ASSERT(startup_observe(&out) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	}
	{
		static ClusterWalStartupObservation out;
		ClusterWalStartupObservation zero = { 0 };
		sigjmp_buf caller;
		int before;
		fixture();
		(void)base_record();
		before = fd_count();
		PG_exception_stack = &caller;
		if (sigsetjmp(caller, 0) == 0) {
			read_action = 5;
			(void)startup_observe(&out);
			UT_ASSERT(false);
		} else {
			UT_ASSERT_EQ(fd_count(), before);
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		}
		PG_exception_stack = NULL;
	}
}

UT_TEST(startup_sync_preserves_native_observation_and_closes_all_fds)
{
	for (unsigned cut = 0; cut <= 5; ++cut) {
		ClusterWalStartupObservation before, after, zero = { 0 };
		int fds;
		fixture();
		(void)base_record();
		/* The physical scan must retain an unpromised complete suffix, without
		 * manufacturing a newer writer promise from the recoverer's fsync. */
		prefix_write((WalTestRecord){ 1, 0, 0, 0 });
		UT_ASSERT_EQ(startup_observe(&before), 0);
		UT_ASSERT_EQ(unlink(prefix_path), 0);
		fds = fd_count();
		tail_sync_count = 0;
		tail_fail_sync = cut;
		memset(&after, 0xa5, sizeof(after));
		if (cut == 0) {
			UT_ASSERT_EQ(
				cluster_wal_startup_sync(scratch, &ref, wal_segment_size, wal_segment_size, &after),
				0);
			UT_ASSERT_EQ(tail_sync_count, 5);
			UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		} else {
			UT_ASSERT(
				cluster_wal_startup_sync(scratch, &ref, wal_segment_size, wal_segment_size, &after)
				!= 0);
			UT_ASSERT_EQ(memcmp(&after, &zero, sizeof(after)), 0);
		}
		tail_fail_sync = 0;
		UT_ASSERT_EQ(fd_count(), fds);
		UT_ASSERT(access(prefix_path, F_OK) != 0 && errno == ENOENT);
	}
}

typedef struct SealedVisitTest {
	unsigned calls;
	int fault;
} SealedVisitTest;

static bool
sealed_visit(XLogReaderState *reader, void *arg)
{
	SealedVisitTest *test = arg;
	UT_ASSERT_EQ(reader->seg.ws_tli, ref.timeline);
	UT_ASSERT_EQ(XLogRecGetRmid(reader), RM_XLOG_ID);
	test->calls++;
	if (test->fault == 1)
		return false;
	if (test->fault == 2)
		pg_re_throw();
	if (test->fault == 3) {
		char path[MAXPGPATH];
		snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
		bytes_write(path, "bad", 3);
	}
	return true;
}

static ClusterControlRootSnapshot
sealed_fixture(bool empty)
{
	ClusterControlRootSnapshot root = { 0 };
	WalTestRecord p;
	CheckPoint cp = { 0 };
	fixture();
	cp.redo = wal_segment_size + SizeOfXLogLongPHD;
	cp.ThisTimeLineID = cp.PrevTimeLineID = ref.timeline;
	cp.nextXid = FullTransactionIdFromU64(100);
	p = startup_record(cp.redo, 0, XLOG_CHECKPOINT_SHUTDOWN, InvalidTransactionId, &cp, sizeof(cp));
	prefix_write(empty ? (WalTestRecord){ 1, 0, 0, 0 } : p);
	root.identity = ref.claim.identity;
	root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	root.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	root.checkpoint_tli = root.tail_tli = ref.timeline;
	root.checkpoint_lower_lsn = root.tail_last_record_lsn = p.record_start;
	root.validated_tail_lsn_exclusive = p.exclusive_end;
	root.checkpoint_record_crc32c = root.tail_last_record_crc32c = p.record_crc;
	root.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	return root;
}

UT_TEST(sealed_reader_needs_native_cut_and_claim)
{
	for (unsigned scenario = 0; scenario < 3; scenario++) {
		ClusterControlRootSnapshot root = sealed_fixture(scenario != 0);
		ClusterWalTailObservation out, zero = { 0 };
		SealedVisitTest visit = { 0 };
		int before = fd_count();
		local_tli = 88;
		UT_ASSERT_EQ(unlink(prefix_path), 0);
		if (scenario == 2) {
			char path[MAXPGPATH];
			snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		memset(&out, 0xa5, sizeof(out));
		if (scenario != 2) {
			UT_ASSERT_EQ(cluster_wal_tail_visit_sealed(scratch, &ref, wal_segment_size, &root,
													   root.checkpoint_lower_lsn, sealed_visit,
													   &visit, &out),
						 0);
			UT_ASSERT_EQ(visit.calls, 1);
			UT_ASSERT_EQ(out.records, 1);
			UT_ASSERT_EQ(out.complete_end, root.validated_tail_lsn_exclusive);
		} else {
			UT_ASSERT(cluster_wal_tail_visit_sealed(scratch, &ref, wal_segment_size, &root,
													root.checkpoint_lower_lsn, sealed_visit, &visit,
													&out)
					  != 0);
			UT_ASSERT_EQ(visit.calls, 0);
			UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		}
		UT_ASSERT_EQ(fd_count(), before);
	}
}

UT_TEST(sealed_reader_refuses_changed_cut_and_discards_provisional_output)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterControlRootSnapshot root = sealed_fixture(false);
		ClusterWalTailObservation out = { 0 }, zero = { 0 };
		SealedVisitTest visit = { 0 };
		volatile bool caught = false;
		int before = fd_count();
		if (fault < 3)
			visit.fault = fault + 1;
		else if (fault == 3)
			(void)record_write(generation, root.validated_tail_lsn_exclusive,
							   root.tail_last_record_lsn, 24);
		else if (fault == 4)
			root.tail_last_record_crc32c ^= 1;
		else if (fault == 5)
			root.checkpoint_record_crc32c ^= 1;
		else if (fault == 6)
			root.identity.origin_owner_incarnation++;
		else if (fault == 7)
			read_action = 3;
		else
			close_action = 1;
		PG_TRY();
		{
			UT_ASSERT(cluster_wal_tail_visit_sealed(scratch, &ref, wal_segment_size, &root,
													root.checkpoint_lower_lsn, sealed_visit, &visit,
													&out)
					  != 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(fd_count(), before);
	}
}

int
main(void)
{
	UT_PLAN(49);
	UT_RUN(later_segment_must_match_exact_source_identity);
	UT_RUN(later_segment_scan_preserves_failure_cleanup_and_namespace);
	UT_RUN(missing_unpromised_middle_segment_is_not_a_tail);
	UT_RUN(unwritten_suffix_does_not_hide_initialized_later_segment);
	UT_RUN(preallocated_and_recycled_later_segments_are_not_redo_evidence);
	UT_RUN(incomplete_final_record_can_span_initialized_segments);
	UT_RUN(startup_commit_ts_history_still_refuses);
	UT_RUN(sealed_reader_needs_native_cut_and_claim);
	UT_RUN(sealed_reader_refuses_changed_cut_and_discards_provisional_output);
	UT_RUN(startup_sync_preserves_native_observation_and_closes_all_fds);
	UT_RUN(startup_rejects_invalid_fpw_payload);
	UT_RUN(startup_rejects_short_checkpoint_payload);
	UT_RUN(startup_captures_first_real_checkpoint_beyond_empty_promise);
	UT_RUN(startup_rejects_checkpoint_identity_and_native_inconsistency);
	UT_RUN(startup_retains_fpw_off_and_unsupported_effects);
	UT_RUN(startup_empty_is_actual_input_not_ordinary_tail_authority);
	UT_RUN(startup_scans_complete_records_even_after_empty_promise);
	UT_RUN(startup_empty_does_not_hide_foreign_wal_or_bad_geometry);
	UT_RUN(startup_rejects_nonindependent_stream_and_malformed_parameters);
	UT_RUN(startup_preserves_physical_failure_and_cancellation_boundaries);
	UT_RUN(legacy_reader_still_works_without_shared_config);
	UT_RUN(pre2_cannot_accept_lost_promised_suffix_from_legacy_reader);
	UT_RUN(pre2_cannot_use_flat_decoy_when_exact_generation_missing);
	UT_RUN(exact_source_uses_foreign_timeline_and_generation);
	UT_RUN(complete_unacknowledged_tail_is_observed);
	UT_RUN(native_tail_needs_claim_and_wal_without_a_flush_sidefile);
	UT_RUN(loss_beyond_every_known_bound_is_a_media_failure_boundary);
	UT_RUN(unacknowledged_torn_tail_is_not_lost_commit);
	UT_RUN(complete_corrupt_suffix_is_not_a_normal_tail);
	UT_RUN(unwritten_gap_before_initialized_page_is_not_a_tail);
	UT_RUN(whole_segment_loss_refuses_a_known_root_bound);
	UT_RUN(complete_suffix_loss_refuses_a_known_root_bound);
	UT_RUN(missing_or_bad_claim_never_uses_backup);
	UT_RUN(selected_record_crc_and_start_must_match);
	UT_RUN(previous_record_link_is_verified);
	UT_RUN(corrupt_first_record_is_not_skipped);
	UT_RUN(many_segment_record_uses_bounded_fds);
	UT_RUN(foreign_page_identity_refuses);
	UT_RUN(replaced_generation_or_segment_refuses);
	UT_RUN(moving_claim_refuses_old_observation);
	UT_RUN(real_io_or_close_failure_is_not_torn_tail);
	UT_RUN(cancellation_closes_owned_descriptors);
	UT_RUN(exact_segment_end_allows_missing_unacknowledged_next_segment);
	UT_RUN(checkpoint_floor_and_claim_cannot_be_substituted);
	UT_RUN(checkpoint_identity_is_not_implied_by_a_valid_tail);
	UT_RUN(foreign_database_after_promise_is_not_a_torn_tail);
	UT_RUN(invalid_lower_does_not_search_forward);
	UT_RUN(foreign_identity_after_promise_is_not_a_torn_tail);
	UT_RUN(untrusted_segment_types_and_permissions_refuse);
	if (scratch[0] && !rmtree(scratch, true))
		return 2;
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
