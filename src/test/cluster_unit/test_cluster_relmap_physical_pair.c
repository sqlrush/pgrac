/* Complete original authority storage with isolated entry-point names.
 * Successful I/O uses real POSIX operations; selected fsync refusal is typed.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#define cluster_relmap_authority_classify test_relmap_physical_classify
#define cluster_relmap_authority_read_committed test_relmap_physical_read_committed
#define cluster_relmap_authority_read_header test_relmap_physical_read_header
#define cluster_relmap_authority_write_pending test_relmap_physical_write_pending
#define cluster_relmap_authority_publish test_relmap_physical_publish
#define cluster_relmap_authority_discard_pending test_relmap_physical_discard_pending
#define cluster_relmap_authority_v2_read test_relmap_physical_v2_read
#define cluster_relmap_authority_v2_check_staged test_relmap_physical_v2_check_staged
#define cluster_relmap_authority_v2_read_committed test_relmap_physical_v2_read_committed
#define cluster_relmap_authority_v2_read_rebuild_pending test_relmap_physical_v2_read_rebuild_pending
#define cluster_relmap_authority_v2_convert_clean test_relmap_physical_v2_convert_clean
#define cluster_relmap_authority_v2_write_pending test_relmap_physical_v2_write_pending
#define cluster_relmap_authority_v2_publish test_relmap_physical_v2_publish
#define cluster_relmap_authority_v2_discard_pending test_relmap_physical_v2_discard_pending
#define cluster_shared_data_dir test_relmap_physical_root
#define pg_fsync test_relmap_physical_fsync

ssize_t test_relmap_physical_write(int fd, const void *bytes, size_t length);
ssize_t test_relmap_physical_read_bytes(int fd, void *bytes, size_t length);
int test_relmap_physical_renameat(int olddir, const char *old, int newdir, const char *new);
int test_relmap_physical_close(int fd);
#define write test_relmap_physical_write
#define read test_relmap_physical_read_bytes
#define renameat test_relmap_physical_renameat
#define close test_relmap_physical_close
#include "../../backend/cluster/cluster_relmap_authority.c"
#undef write
#undef read
#undef renameat
#undef close

#include "test_cluster_relmap_physical_pair.h"

char *test_relmap_physical_root;
static TestRelmapPhysicalStats physical_stats;
static void (*physical_observer)(unsigned);
static bool fail_directory;
static unsigned fail_sync;

static void
physical_event(unsigned event)
{
	if (physical_observer)
		physical_observer(event);
}

void
test_relmap_physical_reset(const char *root, void (*observe)(unsigned))
{
	test_relmap_physical_root = (char *) root;
	physical_observer = observe;
	memset(&physical_stats, 0, sizeof(physical_stats));
	fail_directory = false;
	fail_sync = 0;
}

void
test_relmap_physical_fail_sync(bool directory, unsigned call)
{
	fail_directory = directory;
	fail_sync = call;
}

TestRelmapPhysicalStats
test_relmap_physical_stats(void)
{
	return physical_stats;
}

int
test_relmap_physical_fsync(int fd)
{
	struct stat st;
	bool directory;
	unsigned call;

	if (fstat(fd, &st) != 0)
		return -1;
	directory = S_ISDIR(st.st_mode);
	call = directory ? ++physical_stats.directory_syncs : ++physical_stats.file_syncs;
	physical_event(directory ? RELMAP_PHYSICAL_DIRECTORY_SYNC : RELMAP_PHYSICAL_FILE_SYNC);
	if (fail_sync != 0 && directory == fail_directory && call == fail_sync)
	{
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}

ssize_t
test_relmap_physical_write(int fd, const void *bytes, size_t length)
{
	physical_stats.writes++;
	physical_event(RELMAP_PHYSICAL_WRITE);
	return write(fd, bytes, length);
}

ssize_t
test_relmap_physical_read_bytes(int fd, void *bytes, size_t length)
{
	physical_stats.reads++;
	physical_event(RELMAP_PHYSICAL_READ);
	return read(fd, bytes, length);
}

int
test_relmap_physical_renameat(int olddir, const char *old, int newdir, const char *new)
{
	physical_stats.renames++;
	physical_event(RELMAP_PHYSICAL_RENAME);
	return renameat(olddir, old, newdir, new);
}

int
test_relmap_physical_close(int fd)
{
	physical_event(RELMAP_PHYSICAL_CLOSE);
	return close(fd);
}
