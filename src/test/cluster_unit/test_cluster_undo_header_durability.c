/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_header_durability.c
 *	  Crash model for the undo segment header (block zero) byte writes.
 *
 *	  Includes the real cluster_undo_smgr.c with its file primitives routed to
 *	  a page-cache model: pwrite lands in a volatile cache, fsync copies the
 *	  cache to the durable image, and a crash discards the cache.  Links the
 *	  real undo buffer pool and block-zero resident objects so the product
 *	  checkpoint step (cluster_undo_buf_flush_all) runs unmodified.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_header_durability.c
 *
 * NOTES
 *	  pgrac-original file.
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_buf.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ----- backend globals ----- */
int cluster_node_id = 0;
bool IsUnderPostmaster = false;
BackendType MyBackendType = B_INVALID;
AuxProcType MyAuxProcType = NotAnAuxProcess;
int MyProcPid = 4321;
pg_time_t MyStartTime = 0;
TimestampTz MyStartTimestamp = INT64CONST(0x102030405060);
int pg_file_create_mode = 0600;
int pg_dir_create_mode = 0700;
bool data_sync_retry = false;
ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
ResourceOwner CurTransactionResourceOwner = NULL;
ResourceOwner TopTransactionResourceOwner = NULL;
ResourceOwner AuxProcessResourceOwner = NULL;
MemoryContext TopMemoryContext = (MemoryContext)(uintptr_t)1;
ClusterConf *ClusterConfShmem = NULL;
int cluster_undo_buffers = 4;
bool cluster_undo_buffer_writeback = false;
int cluster_undo_writeback_boundary_check = CLUSTER_UNDO_WB_CHECK_ON;
int cluster_injection_armed_count = 0;
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;
sigjmp_buf *PG_exception_stack = NULL;
struct ErrorContextCallback *error_context_stack = NULL;

static int error_reports = 0;

/* ----- error reporting: count, never accept an unexpected ERROR silently ----- */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	error_reports++;
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "assertion failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}
int
data_sync_elevel(int elevel)
{
	return data_sync_retry ? elevel : PANIC;
}

/* ----- memory, shmem, locks (single-threaded fixture) ----- */
void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	return malloc(size);
}
void
pfree(void *pointer)
{
	free(pointer);
}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback pg_attribute_unused(),
								void *arg pg_attribute_unused())
{}

static void *shmem_buf = NULL;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (shmem_buf == NULL) {
		shmem_buf = malloc(size);
		memset(shmem_buf, 0, size);
		*foundPtr = false;
	} else
		*foundPtr = true;
	return shmem_buf;
}
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}
bool
LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
bool
LWLockHeldByMe(LWLock *lock pg_attribute_unused())
{
	return false;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}
Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}
Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}
void
cluster_injection_run(const char *name pg_attribute_unused())
{}
bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}
void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{}

/* ----- seams of the linked pool and block-zero objects (not reached) ----- */
bool
cluster_semantic_normal_start_closed(void)
{
	return false;
}
uint64
GetSystemIdentifier(void)
{
	abort();
}
ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	ClusterR4PrerequisiteSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.status = CLUSTER_R4_PREREQUISITE_RF_DEFERRED;
	snapshot.target_node_id = -1;
	return snapshot;
}
bool
cluster_reconfig_r4_publish_ready(
	const ClusterR4PrerequisiteSnapshot *expected pg_attribute_unused())
{
	return false;
}
bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	return false;
}
void
XLogFlush(XLogRecPtr record pg_attribute_unused())
{}
XLogRecPtr
GetFlushRecPtr(TimeLineID *insertTLI)
{
	if (insertTLI)
		*insertTLI = 0;
	return (XLogRecPtr)UINT64CONST(0x7FFFFFFFFFFFFFFF);
}
XLogRecPtr
GetRedoRecPtr(void)
{
	return InvalidXLogRecPtr;
}

/* ----- seams of cluster_undo_smgr.c ----- */
void
cluster_undo_record_note_smgr_open(void)
{}
void
cluster_undo_record_note_smgr_close(void)
{}
void
cluster_undo_record_note_smgr_pread(void)
{}
void
cluster_undo_record_note_smgr_pwrite(void)
{}
ClusterUndoPathIntent
cluster_undo_recovery_intent_for_owner(uint8 owner pg_attribute_unused())
{
	return CLUSTER_UNDO_PATH_RUNTIME_SHARED;
}
int
cluster_undo_recovery_path_resolve_v1(uint8 owner pg_attribute_unused(), uint32 segment, char *buf,
									  size_t buf_size)
{
	int ret = snprintf(buf, buf_size, "model/seg_%u", (unsigned)segment);

	return ret < 0 || (size_t)ret >= buf_size ? -1 : 0;
}
int
cluster_undo_path_resolve(ClusterUndoPathIntent intent pg_attribute_unused(),
						  uint8 owner_instance pg_attribute_unused(), uint32 segment_id, char *buf,
						  size_t buf_size)
{
	int ret = snprintf(buf, buf_size, "model/seg_%u", (unsigned)segment_id);

	return ret < 0 || (size_t)ret >= buf_size ? -1 : 0;
}
bool
cluster_undo_segment_header_identity_ok(const char *blockbuf pg_attribute_unused(),
										uint32 segment_id pg_attribute_unused(),
										uint8 owner_instance pg_attribute_unused())
{
	return false;
}
void
cluster_undo_segment_allocate(uint32 segment_id pg_attribute_unused(),
							  uint8 owner_instance pg_attribute_unused())
{
	abort();
}
DIR *
AllocateDir(const char *dirname pg_attribute_unused())
{
	abort();
}
int
FreeDir(DIR *dir pg_attribute_unused())
{
	abort();
}

/*
 * Page-cache model of one segment file's block zero.  The descriptor of a
 * segment is MODEL_FD_BASE + segment id.
 */
#define MODEL_FD_BASE 1000
#define MODEL_SEGMENTS 8

typedef struct ModelFile {
	char cache[BLCKSZ];
	char durable[BLCKSZ];
	int fsyncs;
} ModelFile;

static ModelFile model_files[MODEL_SEGMENTS];
static bool model_fsync_fails = false;

static ModelFile *
model_file(int fd)
{
	int segment = fd - MODEL_FD_BASE;

	if (segment < 1 || segment >= MODEL_SEGMENTS)
		abort();
	return &model_files[segment];
}

static void
model_reset(void)
{
	memset(model_files, 0, sizeof(model_files));
	model_fsync_fails = false;
}

/* A crash discards every page that was not fsynced. */
static void
model_crash(void)
{
	for (int i = 0; i < MODEL_SEGMENTS; i++)
		memcpy(model_files[i].cache, model_files[i].durable, BLCKSZ);
}

int
BasicOpenFile(const char *fileName, int fileFlags pg_attribute_unused())
{
	unsigned segment;

	if (sscanf(fileName, "model/seg_%u", &segment) != 1 || segment < 1 || segment >= MODEL_SEGMENTS)
		return -1;
	return MODEL_FD_BASE + (int)segment;
}
int
pg_fsync(int fd)
{
	ModelFile *file = model_file(fd);

	if (model_fsync_fails) {
		errno = EIO;
		return -1;
	}
	memcpy(file->durable, file->cache, BLCKSZ);
	file->fsyncs++;
	return 0;
}
static ssize_t
model_pwrite(int fd, const void *buf, size_t nbytes, off_t offset)
{
	ModelFile *file = model_file(fd);

	if (offset < 0 || (size_t)offset + nbytes > BLCKSZ)
		abort();
	memcpy(file->cache + offset, buf, nbytes);
	return (ssize_t)nbytes;
}
static ssize_t
model_pread(int fd, void *buf, size_t nbytes, off_t offset)
{
	ModelFile *file = model_file(fd);

	if (offset < 0 || (size_t)offset + nbytes > BLCKSZ)
		abort();
	memcpy(buf, file->cache + offset, nbytes);
	return (ssize_t)nbytes;
}
static int
model_close(int fd pg_attribute_unused())
{
	return 0;
}
/* A second descriptor of the same model file shares its page cache. */
static int
model_dup(int fd)
{
	(void)model_file(fd);
	return fd;
}

#undef pg_pwrite
#define pg_pwrite model_pwrite
#undef pg_pread
#define pg_pread model_pread
#define close model_close
#define dup model_dup
#include "../../backend/cluster/storage/cluster_undo_smgr.c"
#undef dup
#undef close
#undef pg_pwrite
#undef pg_pread

#define STAMP_OFFSET (112 + 7 * 32)

static void
fresh_instance(void)
{
	if (shmem_buf != NULL) {
		free(shmem_buf);
		shmem_buf = NULL;
	}
	model_reset();
	cluster_undo_smgr_fd_cache_reset();
	cluster_undo_buf_shmem_init();
}

static void
stamp_bytes(char stamp[32], unsigned char marker)
{
	memset(stamp, marker, 32);
}

/*
 * A header write whose WAL is older than a completed checkpoint's redo point
 * is never replayed: the checkpoint itself must have made it durable.
 */
UT_TEST(test_checkpoint_makes_prior_header_writes_durable)
{
	char stamp[32];

	fresh_instance();
	stamp_bytes(stamp, 0xc3);
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
												   STAMP_OFFSET, stamp, sizeof(stamp)));

	cluster_undo_buf_flush_all(true);
	model_crash();

	UT_ASSERT_EQ(memcmp(model_files[1].durable + STAMP_OFFSET, stamp, sizeof(stamp)), 0);
}

/* A write after the checkpoint stays volatile; redo, not the checkpoint, owns it. */
UT_TEST(test_write_after_checkpoint_is_left_to_the_next_checkpoint)
{
	char first[32];
	char second[32];

	fresh_instance();
	stamp_bytes(first, 0x11);
	stamp_bytes(second, 0x22);
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 1,
												   STAMP_OFFSET, first, sizeof(first)));
	cluster_undo_buf_flush_all(true);
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 1,
												   STAMP_OFFSET, second, sizeof(second)));
	model_crash();
	UT_ASSERT_EQ(memcmp(model_files[2].durable + STAMP_OFFSET, first, sizeof(first)), 0);

	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 1,
												   STAMP_OFFSET, second, sizeof(second)));
	cluster_undo_buf_flush_all(true);
	model_crash();
	UT_ASSERT_EQ(memcmp(model_files[2].durable + STAMP_OFFSET, second, sizeof(second)), 0);
}

/* Several writes to one segment cost one fsync; untouched segments none. */
UT_TEST(test_checkpoint_fsyncs_each_written_segment_once)
{
	char stamp[32];

	fresh_instance();
	stamp_bytes(stamp, 0x33);
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 3, 1,
												   STAMP_OFFSET, stamp, sizeof(stamp)));
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 3, 1,
												   STAMP_OFFSET + 32, stamp, sizeof(stamp)));
	UT_ASSERT_EQ(model_files[3].fsyncs, 0);
	cluster_undo_buf_flush_all(true);
	UT_ASSERT_EQ(model_files[3].fsyncs, 1);
	UT_ASSERT_EQ(model_files[4].fsyncs, 0);
	cluster_undo_buf_flush_all(true);
	UT_ASSERT_EQ(model_files[3].fsyncs, 1);
}

/* The critical-section writer records its write exactly like the plain one. */
UT_TEST(test_precommit_opened_writer_is_covered_by_checkpoint)
{
	char stamp[32];
	int fd;

	fresh_instance();
	stamp_bytes(stamp, 0x44);
	fd = cluster_undo_smgr_header_writer_open(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 5, 1);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(cluster_undo_smgr_header_writer_write(fd, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 5, 1,
													STAMP_OFFSET, stamp, sizeof(stamp)));
	cluster_undo_smgr_header_writer_close(fd);
	UT_ASSERT_EQ(model_files[5].fsyncs, 0);
	cluster_undo_buf_flush_all(true);
	model_crash();
	UT_ASSERT_EQ(memcmp(model_files[5].durable + STAMP_OFFSET, stamp, sizeof(stamp)), 0);
}

/* A failed checkpoint fsync keeps the write pending for the next checkpoint. */
UT_TEST(test_checkpoint_fsync_failure_errors_and_keeps_the_write_pending)
{
	char stamp[32];
	volatile bool caught = false;
	sigjmp_buf local_sigjmp_buf;
	sigjmp_buf *save_exception_stack = PG_exception_stack;

	fresh_instance();
	stamp_bytes(stamp, 0x55);
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 6, 1,
												   STAMP_OFFSET, stamp, sizeof(stamp)));
	data_sync_retry = true;
	model_fsync_fails = true;
	error_reports = 0;
	if (sigsetjmp(local_sigjmp_buf, 0) == 0) {
		PG_exception_stack = &local_sigjmp_buf;
		cluster_undo_buf_flush_all(true);
	} else
		caught = true;
	PG_exception_stack = save_exception_stack;
	data_sync_retry = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(error_reports, 1);

	model_fsync_fails = false;
	cluster_undo_buf_flush_all(true);
	model_crash();
	UT_ASSERT_EQ(memcmp(model_files[6].durable + STAMP_OFFSET, stamp, sizeof(stamp)), 0);
}

/* Without the shared block-zero region there is nothing to record into. */
UT_TEST(test_header_write_without_shared_region_fsyncs_itself)
{
	char stamp[32];
	int saved_buffers = cluster_undo_buffers;

	cluster_undo_buffers = 0;
	fresh_instance();
	stamp_bytes(stamp, 0x66);
	UT_ASSERT(cluster_undo_smgr_write_header_bytes(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 7, 1,
												   STAMP_OFFSET, stamp, sizeof(stamp)));
	model_crash();
	UT_ASSERT_EQ(memcmp(model_files[7].durable + STAMP_OFFSET, stamp, sizeof(stamp)), 0);
	cluster_undo_buffers = saved_buffers;
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_checkpoint_makes_prior_header_writes_durable);
	UT_RUN(test_write_after_checkpoint_is_left_to_the_next_checkpoint);
	UT_RUN(test_checkpoint_fsyncs_each_written_segment_once);
	UT_RUN(test_precommit_opened_writer_is_covered_by_checkpoint);
	UT_RUN(test_checkpoint_fsync_failure_errors_and_keeps_the_write_pending);
	UT_RUN(test_header_write_without_shared_region_fsyncs_itself);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
