/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual producer/main sync tail, syscall-only faults and real files.
 * Control bytes model finished bootstrap, not proof of new creation; native
 * initdb TAP separately proves the real bootstrap/control boundary. */
#include "postgres_fe.h"
#include <dirent.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
static int hw_test_open(const char *, int, ...);
static int hw_test_openat(int, const char *, int, ...);
static DIR *hw_test_opendir(const char *);
static DIR *hw_test_fdopendir(int);
static struct dirent *hw_test_readdir(DIR *);
static ssize_t hw_test_write(int, const void *, size_t);
static int hw_test_fsync(int);
static int hw_test_linkat(int, const char *, int, const char *, int);
static int hw_test_close(int);
int initdb_fixture_program(int, char **);
#define open hw_test_open
#define openat hw_test_openat
#define opendir hw_test_opendir
#define fdopendir hw_test_fdopendir
#define readdir hw_test_readdir
#define write hw_test_write
#define fsync hw_test_fsync
#define linkat hw_test_linkat
#define close hw_test_close
#define main initdb_fixture_program
#include "../../common/file_utils.c"
#include "../../bin/initdb/initdb.c"
#undef main
#undef open
#undef openat
#undef opendir
#undef fdopendir
#undef readdir
#undef write
#undef fsync
#undef linkat
#undef close
#include "unit_test.h"
UT_DEFINE_GLOBALS();

enum Fault {
	NONE,
	PARTIAL_WRITE,
	WRITE_EIO,
	FILE_SYNC,
	GLOBAL_SYNC,
	ROOT_SYNC,
	COLLISION,
	REPLACE_TEMP,
	REPLACE_FINAL,
	FILE_CLOSE,
	DATA_OPEN,
	DATA_ENUMERATE,
	DATA_READDIR,
	ALIAS_SWITCH
};
static enum Fault fault;
static bool syncing, once;
static char *case_dir;
static bool
same_dir(int a, int b)
{
	struct stat x, y;
	return a >= 0 && b >= 0 && fstat(a, &x) == 0 && fstat(b, &y) == 0 && x.st_dev == y.st_dev
		   && x.st_ino == y.st_ino;
}
static int
hw_test_open(const char *path, int flags, ...)
{
	int mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, int);
		va_end(ap);
	}
	if (syncing && fault == DATA_OPEN && strstr(path, "/sync_target")) {
		errno = EIO;
		return -1;
	}
	return open(path, flags, mode);
}
static int
hw_test_openat(int dir, const char *path, int flags, ...)
{
	int mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, int);
		va_end(ap);
	}
	if (syncing && fault == DATA_OPEN && strcmp(path, "sync_target") == 0) {
		errno = EIO;
		return -1;
	}
	return openat(dir, path, flags, mode);
}
static DIR *
hw_test_opendir(const char *path)
{
	if (syncing && fault == DATA_ENUMERATE && strcmp(path, pgrac_hw_pgdata_path) == 0) {
		errno = EIO;
		return NULL;
	}
	return opendir(path);
}
static DIR *
hw_test_fdopendir(int fd)
{
	if (syncing && fault == DATA_ENUMERATE && same_dir(fd, pgrac_hw_pgdata_fd)) {
		errno = EIO;
		return NULL;
	}
	return fdopendir(fd);
}
static struct dirent *
hw_test_readdir(DIR *dir)
{
	if (syncing && fault == DATA_READDIR && same_dir(dirfd(dir), pgrac_hw_pgdata_fd)) {
		errno = EIO;
		return NULL;
	}
	return readdir(dir);
}
static ssize_t
hw_test_write(int fd, const void *buf, size_t n)
{
	if (fault == WRITE_EIO) {
		errno = EIO;
		return -1;
	}
	if (fault == PARTIAL_WRITE) {
		if (!once) {
			once = true;
			errno = EINTR;
			return -1;
		}
		n = Min(n, 3);
	}
	return write(fd, buf, n);
}
static void
foreign_file(int dir, const char *name)
{
	int fd = openat(dir, name, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, "retained-foreign", 16) != 16 || close(fd) != 0)
		_exit(90);
}
static int
hw_test_fsync(int fd)
{
	bool file = fd != pgrac_hw_global_fd && fd != pgrac_hw_root_fd && fd != pgrac_hw_pgdata_fd;
	if (!syncing
		&& ((fault == FILE_SYNC && file) || (fault == GLOBAL_SYNC && fd == pgrac_hw_global_fd)
			|| (fault == ROOT_SYNC && fd == pgrac_hw_root_fd))) {
		errno = EIO;
		return -1;
	}
	if (!syncing && fault == REPLACE_FINAL && fd == pgrac_hw_global_fd && !once) {
		once = true;
		if (renameat(fd, "pg_hw_snapshot.7", AT_FDCWD, psprintf("%s/saved-final", case_dir)) != 0)
			_exit(91);
		foreign_file(fd, "pg_hw_snapshot.7");
	}
	return fsync(fd);
}
static int
hw_test_linkat(int fromfd, const char *from, int tofd, const char *to, int flags)
{
	int rc;
	if (fault == COLLISION)
		foreign_file(tofd, to);
	rc = linkat(fromfd, from, tofd, to, flags);
	if (rc == 0 && fault == REPLACE_TEMP) {
		if (renameat(fromfd, from, AT_FDCWD, psprintf("%s/saved-temp", case_dir)) != 0)
			_exit(92);
		foreign_file(fromfd, from);
	}
	return rc;
}
static int
hw_test_close(int fd)
{
	int rc = close(fd);
	if (!syncing && fault == FILE_CLOSE && fd != pgrac_hw_global_fd && fd != pgrac_hw_root_fd
		&& fd != pgrac_hw_pgdata_fd) {
		errno = EIO;
		return -1;
	}
	return rc;
}
static void
control_fixture(const char *dir, uint64 sysid)
{
	char bytes[PG_CONTROL_FILE_SIZE] = { 0 };
	ControlFileData control = { 0 };
	char *global = psprintf("%s/global", dir);
	int fd;
	if (mkdir(dir, 0700) != 0 || mkdir(global, 0700) != 0)
		_exit(93);
	control.pg_control_version = PG_CONTROL_VERSION;
	control.catalog_version_no = CATALOG_VERSION_NO;
	control.system_identifier = sysid;
	control.state = DB_SHUTDOWNED;
	control.checkPoint = control.checkPointCopy.redo = 4096;
	control.checkPointCopy.ThisTimeLineID = 1;
	INIT_CRC32C(control.crc);
	COMP_CRC32C(control.crc, &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(control.crc);
	memcpy(bytes, &control, sizeof(control));
	fd = open(psprintf("%s/pg_control", global), O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd) != 0)
		_exit(94);
	if (mkdir(psprintf("%s/pg_wal", dir), 0700) != 0
		|| mkdir(psprintf("%s/pg_tblspc", dir), 0700) != 0)
		_exit(95);
	fd = open(psprintf("%s/sync_target", dir), O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, "must-sync", 9) != 9 || close(fd) != 0)
		_exit(96);
}
static void
actual_sync_tail(void)
{
#include "test_cluster_hw_initdb_sync_tail.inc"
}
static void
run_case(enum Fault injected, bool want_success, const char *reason)
{
	char template[] = "/tmp/pgrac-hw-create-XXXXXX";
	char *base = mkdtemp(template);
	char *root, *data, *snapshot, *log;
	pid_t pid;
	int status, fd;
	char bytes[4096] = { 0 };
	ssize_t n;
	UT_ASSERT_NOT_NULL(base);
	if (!base)
		return;
	case_dir = realpath(base, NULL);
	root = psprintf("%s/shared", case_dir);
	data = psprintf("%s/data", case_dir);
	snapshot = psprintf("%s/global/pg_hw_snapshot.7", root);
	log = psprintf("%s/child.log", case_dir);
	UT_ASSERT_EQ(mkdir(root, 0700), 0);
	fflush(NULL);
	pid = fork();
	UT_ASSERT(pid >= 0);
	if (pid == 0) {
		char *alias = psprintf("%s/alias", case_dir);
		pg_logging_init("hw-initdb-test");
		fd = open(log, O_WRONLY | O_CREAT | O_EXCL, 0600);
		if (fd < 0 || dup2(fd, STDERR_FILENO) < 0 || dup2(fd, STDOUT_FILENO) < 0)
			_exit(97);
		close(fd);
		pg_data = pg_strdup(data);
		if (injected == ALIAS_SWITCH) {
			if (symlink(case_dir, alias) != 0)
				_exit(98);
			pg_data = psprintf("%s/data", alias);
		}
		setup_pgdata();
		pgrac_hw_snapshot_root = root;
		pgrac_hw_snapshot_owner = 7;
		pgrac_hw_prepare_paths();
		control_fixture(data, 987654321);
		made_new_pgdata = true;
		pgrac_hw_bind_creation();
		if (injected == ALIAS_SWITCH) {
			char *other = psprintf("%s/other", case_dir);
			if (mkdir(other, 0700) != 0)
				_exit(99);
			control_fixture(psprintf("%s/data", other), 111222333);
			if (unlink(alias) != 0 || symlink(other, alias) != 0)
				_exit(100);
		}
		fault = injected;
		syncing = injected == DATA_OPEN || injected == DATA_ENUMERATE || injected == DATA_READDIR;
		if (syncing)
			actual_sync_tail();
		else
			finalize_pgrac_hw_snapshot();
		_exit(0);
	}
	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), want_success ? 0 : 1);
	fd = open(log, O_RDONLY);
	UT_ASSERT(fd >= 0);
	n = read(fd, bytes, sizeof(bytes) - 1);
	close(fd);
	UT_ASSERT(n >= 0);
	if (reason != NULL)
		UT_ASSERT(strstr(bytes, reason) != NULL);
	if (want_success) {
		ClusterHwSnapshotHeader hdr;
		fd = open(snapshot, O_RDONLY);
		UT_ASSERT(fd >= 0);
		n = fd >= 0 ? read(fd, bytes, sizeof(bytes)) : -1;
		if (fd >= 0)
			close(fd);
		UT_ASSERT_EQ(n, 52);
		if (n == 52) {
			UT_ASSERT_EQ(cluster_hw_snapshot_codec_deserialize(bytes, n, &hdr, NULL, 0),
						 CLUSTER_HW_SNAPSHOT_VALID);
			UT_ASSERT_EQ(hdr.system_id, 987654321);
			UT_ASSERT_EQ(hdr.snapshot_lsn, 4096);
		}
	} else if (injected == DATA_OPEN || injected == DATA_ENUMERATE || injected == DATA_READDIR
			   || injected == WRITE_EIO || injected == FILE_SYNC) {
		UT_ASSERT_EQ(access(snapshot, F_OK), -1);
	}
	if (injected == REPLACE_TEMP) {
		fd = open(psprintf("%s/global/pg_hw_snapshot.7.initdb.%ld", root, (long)pid), O_RDONLY);
		UT_ASSERT(fd >= 0);
		if (fd >= 0) {
			n = read(fd, bytes, sizeof(bytes));
			close(fd);
			UT_ASSERT_EQ(n, 16);
		}
	}
}
UT_TEST(partial_write_and_eintr_produce_complete_v1)
{
	run_case(PARTIAL_WRITE, true, NULL);
}
UT_TEST(write_failure_preserves_incomplete_not_authority)
{
	run_case(WRITE_EIO, false, "HW_SNAPSHOT_WRITE");
}
UT_TEST(file_sync_failure_never_publishes)
{
	run_case(FILE_SYNC, false, "HW_SNAPSHOT_SYNC");
}
UT_TEST(global_sync_failure_is_not_success)
{
	run_case(GLOBAL_SYNC, false, "HW_SNAPSHOT_SYNC");
}
UT_TEST(root_sync_failure_is_not_success)
{
	run_case(ROOT_SYNC, false, "HW_SNAPSHOT_SYNC");
}
UT_TEST(no_overwrite_publication)
{
	run_case(COLLISION, false, "HW_SNAPSHOT_PUBLISH");
}
UT_TEST(replaced_temp_must_not_be_deleted)
{
	run_case(REPLACE_TEMP, false, "HW_SNAPSHOT_IDENTITY");
}
UT_TEST(replaced_final_must_not_report_success)
{
	run_case(REPLACE_FINAL, false, "HW_SNAPSHOT_IDENTITY");
}
UT_TEST(close_failure_is_not_success)
{
	run_case(FILE_CLOSE, false, "HW_SNAPSHOT_CLOSE");
}
UT_TEST(actual_sync_must_not_ignore_file_open_failure)
{
	run_case(DATA_OPEN, false, "HW_DATA_SYNC");
}
UT_TEST(actual_sync_must_not_ignore_enumeration_failure)
{
	run_case(DATA_ENUMERATE, false, "HW_DATA_SYNC");
}
UT_TEST(actual_sync_must_not_ignore_readdir_failure)
{
	run_case(DATA_READDIR, false, "HW_DATA_SYNC");
}
UT_TEST(control_consumer_uses_created_pgdata_not_movable_alias)
{
	run_case(ALIAS_SWITCH, true, NULL);
}
int
main(void)
{
	UT_PLAN(13);
	UT_RUN(partial_write_and_eintr_produce_complete_v1);
	UT_RUN(write_failure_preserves_incomplete_not_authority);
	UT_RUN(file_sync_failure_never_publishes);
	UT_RUN(global_sync_failure_is_not_success);
	UT_RUN(root_sync_failure_is_not_success);
	UT_RUN(no_overwrite_publication);
	UT_RUN(replaced_temp_must_not_be_deleted);
	UT_RUN(replaced_final_must_not_report_success);
	UT_RUN(close_failure_is_not_success);
	UT_RUN(actual_sync_must_not_ignore_file_open_failure);
	UT_RUN(actual_sync_must_not_ignore_enumeration_failure);
	UT_RUN(actual_sync_must_not_ignore_readdir_failure);
	UT_RUN(control_consumer_uses_created_pgdata_not_movable_alias);
	UT_DONE();
	return ut_failed_count != 0;
}
