/* Original checkpoint/SLRU with complete framed files. Table, WAL, lock
 * scheduling and ERROR reset are finite providers; file bytes and SHA are real. */
static int checkpoint_error_level;
#define SLRU_FIXTURE_COUNT_HOLDOFF() true
#define SLRU_FIXTURE_ERROR_RESET() (InterruptHoldoffCount = 0)
#define SLRU_FIXTURE_ERROR_LOCKS() true
#define SLRU_FIXTURE_ERROR_LEVEL(level) (checkpoint_error_level = (level))
#define NATIVE_CLOG_IO_MAIN checkpoint_old_main
int checkpoint_old_main(void);
#include "test_cluster_clog_native_io.c"
#undef NATIVE_CLOG_IO_MAIN
#include <sys/wait.h>

static ssize_t checkpoint_pread(int fd, void *bytes, size_t length, off_t offset);
static int checkpoint_fsync(int fd);
#undef pg_pread
#define pg_pread checkpoint_pread
#define pg_fsync checkpoint_fsync
#include "../../backend/access/transam/clog_sync.c"
#undef pg_fsync

static ClusterRecoveryFileIdentityV2 checkpoint_files[80];
static ClusterRecoveryFileReservationV1 checkpoint_reservations[2];
static unsigned checkpoint_count, checkpoint_captures, checkpoint_destroys;
static unsigned checkpoint_reads, checkpoint_syncs, checkpoint_dir_syncs;
static unsigned checkpoint_current_calls;
static bool checkpoint_capture_fail, checkpoint_drift, checkpoint_bad_borrow;
static bool checkpoint_read_error, checkpoint_sync_fail;
static bool checkpoint_release_error;
static unsigned checkpoint_drift_sync;
static void *checkpoint_allocated_work;

void *
palloc0(Size size)
{
	void *value = calloc(1, size);

	if (value == NULL) abort();
	if (checkpoint_allocated_work != NULL) abort();
	checkpoint_allocated_work = value;
	return value;
}

void
pfree(void *value)
{
	if (value != checkpoint_allocated_work) abort();
	checkpoint_allocated_work = NULL;
	free(value);
}

void pgstat_count_slru_flush(int index pg_attribute_unused()) {}
void
fsync_fname(const char *path, bool isdir)
{
	int fd;

	if (strcmp(path, directory) || !isdir || control_held || io_held) abort();
	fd = open(path, O_RDONLY | O_DIRECTORY);
	if (fd < 0 || fsync(fd) || close(fd)) abort();
	checkpoint_dir_syncs++;
}

ClusterControlNativeResultV2
cluster_control_native_files_inventory_v2(Size budget, ClusterControlNativeInventoryV2 **out)
{
	ClusterControlNativeInventoryV2 *v;
	Size bytes = sizeof(*v) + checkpoint_count * sizeof(*checkpoint_files) + sizeof(checkpoint_reservations);

	if (control_held || io_held || *out || budget != CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2) abort();
	checkpoint_captures++;
	if (checkpoint_capture_fail || native_mode != CLUSTER_NATIVE_FILES_MANAGED_V2)
		return CLUSTER_NATIVE_FILES_REFUSED_V2;
	v = calloc(1, bytes);
	if (!v) abort();
	v->info.mode = native_mode;
	v->info.table_generation = native_generation;
	v->info.startup_generation = 1;
	v->info.count = checkpoint_count;
	v->info.reservation_count = 2;
	v->info.borrowed = native_borrows;
	v->files = (ClusterRecoveryFileIdentityV2 *) (v + 1);
	v->reservations = (ClusterRecoveryFileReservationV1 *) (v->files + checkpoint_count);
	memcpy((void *) v->files, checkpoint_files, checkpoint_count * sizeof(*checkpoint_files));
	memcpy((void *) v->reservations, checkpoint_reservations, sizeof(checkpoint_reservations));
	v->private_self = (uintptr_t) v;
	v->private_pid = 1;
	v->private_bytes = bytes;
	*out = v;
	return CLUSTER_NATIVE_FILES_OK_V2;
}

bool
cluster_control_native_files_inventory_current_v2(const ClusterControlNativeInventoryV2 *v)
{
	checkpoint_current_calls++;
	if (control_held || io_held || v->private_self != (uintptr_t) v) abort();
	return !checkpoint_drift && v->info.table_generation == native_generation &&
		v->info.borrowed == native_borrows &&
		!memcmp(v->files, checkpoint_files, checkpoint_count * sizeof(*checkpoint_files)) &&
		!memcmp(v->reservations, checkpoint_reservations, sizeof(checkpoint_reservations));
}

void
cluster_control_native_inventory_destroy_v2(ClusterControlNativeInventoryV2 **value)
{
	if (*value) {
		if ((*value)->private_self != (uintptr_t) *value) abort();
		free(*value);
		*value = NULL;
		checkpoint_destroys++;
	}
}

static bool
checkpoint_acquire(int pageno, ClusterControlNativeFileBorrowV2 *out)
{
	unsigned segment = (unsigned) pageno / 32;

	if (!control_held || io_held || segment >= checkpoint_count || out->private_self) abort();
	out->expected = checkpoint_files[segment];
	if (checkpoint_bad_borrow) out->expected.creation_generation++;
	out->table_generation = native_generation;
	out->startup_generation = 1;
	out->private_self = (uintptr_t) out;
	out->private_pid = 1;
	native_borrows++; native_acquires++;
	return true;
}

static bool
checkpoint_release(ClusterControlNativeFileBorrowV2 *value)
{
	if (checkpoint_release_error) {
		InterruptHoldoffCount = 0;
		siglongjmp(*PG_exception_stack, 1);
	}
	return cluster_control_native_file_release_v2(value);
}

static ssize_t
checkpoint_pread(int fd, void *bytes, size_t length, off_t offset)
{
	if (!control_held || !native_borrows) abort();
	checkpoint_reads++;
	if (checkpoint_read_error) {
		InterruptHoldoffCount = 0;
		siglongjmp(*PG_exception_stack, 1);
	}
	return pread(fd, bytes, length, offset);
}

static int
checkpoint_fsync(int fd)
{
	if (!control_held || !native_borrows) abort();
	checkpoint_syncs++;
	if (checkpoint_drift_sync == checkpoint_syncs) checkpoint_drift = true;
	if (checkpoint_sync_fail) { errno = EIO; return -1; }
	return fsync(fd);
}

static void
checkpoint_reset(unsigned count)
{
	char path[MAXPGPATH];

	native_reset();
	checkpoint_count = count;
	for (unsigned i = 0; i < count; i++) {
		ClusterClogFileBindingV2 b = binding;
		ClusterClogPageIdentityV2 p = identities[0];
		uint8 header[8192], frame[8256];
		int fd;

		b.identity.object_id = b.segment = i;
		checkpoint_files[i] = b.identity;
		if (!i) continue;
		p.identity.logical_page = i * 32;
		p.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + (uint64) i * 32 * 32768);
		if (!cluster_clog_file_header_encode_v2(&b, header, sizeof(header)) ||
			!cluster_clog_file_page_encode_v2(&b, &p, page.data, BLCKSZ, frame, sizeof(frame))) abort();
		snprintf(path, sizeof(path), "%s/%04X", directory, i);
		fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
		if (fd < 0 || pwrite(fd, header, sizeof(header), 0) != sizeof(header) ||
			pwrite(fd, frame, sizeof(frame), 8192) != sizeof(frame) || close(fd)) abort();
	}
	memset(checkpoint_reservations, 0, sizeof(checkpoint_reservations));
	checkpoint_reservations[0].expected = checkpoint_files[0];
	checkpoint_captures = checkpoint_destroys = checkpoint_reads = checkpoint_syncs = 0;
	checkpoint_dir_syncs = checkpoint_current_calls = checkpoint_drift_sync = 0;
	checkpoint_capture_fail = checkpoint_drift = checkpoint_bad_borrow = false;
	checkpoint_read_error = checkpoint_sync_fail = false;
	checkpoint_release_error = false;
	checkpoint_error_level = 0;
	ctl.clog_native_acquire = checkpoint_acquire;
	ctl.clog_native_release = checkpoint_release;
	*XactCtl = ctl;
	control_held = false;
	InterruptHoldoffCount = 0;
}

static bool
checkpoint_raises(void)
{
	volatile bool caught = false;

	PG_TRY(); { CheckPointCLOG(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

static void
checkpoint_close(void)
{
	char path[MAXPGPATH];

	UT_ASSERT(!control_held && !io_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(native_acquires, native_releases);
	UT_ASSERT(checkpoint_allocated_work == NULL);
	UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
	close(borrowed);
	for (unsigned i = 0; i < checkpoint_count; i++) {
		snprintf(path, sizeof(path), "%s/%04X", directory, i);
		unlink(path);
	}
}

UT_TEST(checkpoint_syncs_entire_inventory_beyond_resident_and_64)
{
	checkpoint_reset(80);
	UT_ASSERT(!checkpoint_raises());
	UT_ASSERT_EQ(checkpoint_captures, 1);
	UT_ASSERT_EQ(checkpoint_destroys, 1);
	UT_ASSERT_EQ(checkpoint_syncs, 160);
	UT_ASSERT_EQ(checkpoint_dir_syncs, 1);
	UT_ASSERT(checkpoint_reads >= 4 * 80);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT(!dirty[0]);
	checkpoint_close();
}

UT_TEST(checkpoint_missing_nonresident_file_cannot_complete)
{
	char path[MAXPGPATH];

	checkpoint_reset(80);
	snprintf(path, sizeof(path), "%s/004F", directory);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT(checkpoint_raises());
	UT_ASSERT_EQ(checkpoint_destroys, 1);
	UT_ASSERT_EQ(checkpoint_syncs, 158);
	UT_ASSERT_EQ(inserts, 1);
	checkpoint_close();
}

UT_TEST(checkpoint_unknown_or_failed_inventory_refuses_before_dirty_write)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		checkpoint_reset(1);
		if (!fault) native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		else checkpoint_capture_fail = true;
		UT_ASSERT(checkpoint_raises());
		UT_ASSERT_EQ(inserts + writes + native_opens + checkpoint_syncs, 0);
		UT_ASSERT(dirty[0]);
		checkpoint_close();
	}
}

UT_TEST(checkpoint_late_inventory_drift_preserves_written_effects)
{
	checkpoint_reset(2);
	checkpoint_drift_sync = 4;
	UT_ASSERT(checkpoint_raises());
	UT_ASSERT_EQ(checkpoint_syncs, 4);
	UT_ASSERT_EQ(checkpoint_destroys, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(!dirty[0]);
	checkpoint_close();
}

UT_TEST(checkpoint_sync_refusal_and_real_error_reset_retire_only_own_locks)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		checkpoint_reset(1);
		dirty[0] = false;
		checkpoint_sync_fail = fault == 0;
		checkpoint_read_error = fault == 1;
		native_open_throw = fault == 2;
		UT_ASSERT(checkpoint_raises());
		UT_ASSERT_EQ(checkpoint_destroys, 1);
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
		checkpoint_close();
	}
}

UT_TEST(checkpoint_exact_selected_identity_cannot_change_at_borrow)
{
	checkpoint_reset(1);
	dirty[0] = false;
	checkpoint_bad_borrow = true;
	UT_ASSERT(checkpoint_raises());
	UT_ASSERT_EQ(native_opens + inserts + writes + checkpoint_syncs, 0);
	UT_ASSERT_EQ(checkpoint_destroys, 1);
	checkpoint_close();
}

UT_TEST(checkpoint_waits_existing_io_before_whole_file_readback)
{
	checkpoint_reset(1);
	dirty[0] = false;
	native_open_busy = true;
	fixture_io_wait = native_other_writer_completes;
	UT_ASSERT(!checkpoint_raises());
	UT_ASSERT_EQ(native_waits, 1);
	UT_ASSERT_EQ(checkpoint_syncs, 2);
	checkpoint_close();
}

UT_TEST(checkpoint_creation_and_disabled_legacy_keep_original_flush)
{
	for (unsigned legacy = 0; legacy < 2; legacy++) {
		checkpoint_reset(1);
		native_mode = legacy ? CLUSTER_NATIVE_FILES_UNKNOWN_V2 : CLUSTER_NATIVE_FILES_CREATION_V2;
		cluster_enabled = !legacy;
		if (legacy) dirty[0] = false;
		UT_ASSERT(!checkpoint_raises());
		UT_ASSERT_EQ(checkpoint_captures + checkpoint_destroys + checkpoint_syncs, 0);
		UT_ASSERT_EQ(checkpoint_dir_syncs, 1);
		UT_ASSERT_EQ(inserts, legacy ? 0 : 1);
		checkpoint_close();
	}
}

UT_TEST(checkpoint_failed_cleanup_cannot_forget_address_bound_borrow)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		pid_t child;
		int status;

		checkpoint_reset(1);
		dirty[0] = false;
		child = fork();
		if (child < 0) abort();
		if (child == 0) {
			/* Each finite PANIC provider runs in its own process: the real
			 * backend must crash, rather than discard live custody and resume. */
			checkpoint_read_error = fault >= 2;
			checkpoint_release_error = (fault & 1) != 0;
			native_release_ok = checkpoint_release_error;
			if (!checkpoint_raises() || checkpoint_error_level != PANIC ||
				checkpoint_allocated_work == NULL || native_borrows != 1 ||
				!control_held || io_held || InterruptHoldoffCount != 0)
				_exit(1);
			_exit(0);
		}
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		checkpoint_close();
	}
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-checkpoint-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(9);
	UT_RUN(checkpoint_syncs_entire_inventory_beyond_resident_and_64);
	UT_RUN(checkpoint_missing_nonresident_file_cannot_complete);
	UT_RUN(checkpoint_unknown_or_failed_inventory_refuses_before_dirty_write);
	UT_RUN(checkpoint_late_inventory_drift_preserves_written_effects);
	UT_RUN(checkpoint_sync_refusal_and_real_error_reset_retire_only_own_locks);
	UT_RUN(checkpoint_exact_selected_identity_cannot_change_at_borrow);
	UT_RUN(checkpoint_waits_existing_io_before_whole_file_readback);
	UT_RUN(checkpoint_creation_and_disabled_legacy_keep_original_flush);
	UT_RUN(checkpoint_failed_cleanup_cannot_forget_address_bound_borrow);
	rmdir(directory);
	UT_DONE();
	return ut_failed_count != 0;
}
