/* Standalone PG/I/O boundary for the real activation + resident objects.
 * Directory enumeration and header bytes use private real sparse files. The
 * smgr probe's typed result is a fixture boundary; its own file validation has
 * separate production-smgr tests. No resident/admission/cleanup decision is
 * reimplemented here. Included before the production activation source. */
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "cluster/cluster_uba.h"
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include "test_cluster_normal_cold_scn.inc"

static char cold_root_path[MAXPGPATH];
static void *cold_region;
static unsigned cold_probe_count[256];
static unsigned cold_read_count[256];
static int cold_refuse_read = -1;
static int cold_throw_read = -1;
static int cold_change_image = -1;
static int cold_root_drift = -1;
static int cold_boot_drift = -1;
static unsigned cold_reset_count;
static unsigned cold_observe_count;
static SCN cold_observed_scn;
static ResourceReleaseCallback cold_release_callback;
static void *cold_release_arg;

ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
MemoryContext TopMemoryContext = (MemoryContext)(uintptr_t)1;
BackendType MyBackendType = B_INVALID;

Size
add_size(Size a, Size b)
{
	return a + b;
}
Size
mul_size(Size a, Size b)
{
	return a * b;
}
void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	return malloc(size);
}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	cold_release_callback = callback;
	cold_release_arg = arg;
}
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	HOLD_INTERRUPTS();
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return LWLockAcquire(lock, mode);
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	RESUME_INTERRUPTS();
}
DIR *
AllocateDir(const char *path)
{
	return opendir(path);
}
int
FreeDir(DIR *dir)
{
	return closedir(dir);
}

bool
cluster_undo_block0_current_startup_fenced_begin(ClusterUndoBlock0CurrentGuard *guard)
{
	if (test_normal_guard_owned || guard == NULL)
		return false;
	test_normal_guard_owned = true;
	return true;
}
bool
cluster_undo_block0_current_startup_fenced_end(ClusterUndoBlock0CurrentGuard *guard)
{
	if (!test_normal_guard_owned || guard == NULL)
		return false;
	test_normal_guard_owned = false;
	return true;
}

int
cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 owner, uint32 segment, char *path,
						  size_t size)
{
	int n;
	if (intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED || owner != cluster_node_id + 1)
		return -1;
	n = snprintf(path, size, "%s/pg_undo/instance_%d/seg_%u.dat", cold_root_path, cluster_node_id,
				 segment);
	return n < 0 || n >= (int)size ? -1 : 0;
}

static bool
cold_read_header(uint32 segment, uint8 owner, char *out)
{
	char path[MAXPGPATH];
	int fd;
	ssize_t n;
	if (cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner, segment, path,
								  sizeof(path))
		!= 0)
		return false;
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return false;
	n = pread(fd, out, BLCKSZ, 0);
	return close(fd) == 0 && n == BLCKSZ;
}

ClusterUndoSmgrFinalState
cluster_undo_smgr_probe_segment(ClusterUndoPathIntent intent, uint32 segment, uint8 owner,
								char block0[BLCKSZ])
{
	char path[MAXPGPATH];
	struct stat st;
	uint32 index = segment - (uint32)cluster_node_id * 256 - 1;
	if (index >= 256 || cluster_undo_path_resolve(intent, owner, segment, path, sizeof(path)) != 0)
		return CLUSTER_UNDO_SMGR_FINAL_INVALID;
	cold_probe_count[index]++;
	if ((int)index == cold_root_drift)
		test_normal_pgrd_bytes[64] ^= 1;
	if ((int)index == cold_boot_drift)
		test_qvotec_self_incarnation++;
	if (stat(path, &st) != 0)
		return errno == ENOENT ? CLUSTER_UNDO_SMGR_FINAL_ABSENT : CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	if (st.st_size != UNDO_SEGMENT_SIZE_BYTES)
		return CLUSTER_UNDO_SMGR_FINAL_INVALID;
	return cold_read_header(segment, owner, block0) ? CLUSTER_UNDO_SMGR_FINAL_EXACT
													: CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment, uint8 owner,
							 uint32 block, char *out)
{
	uint32 index = segment - (uint32)cluster_node_id * 256 - 1;
	if (intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED || block != 0 || index >= 256)
		return false;
	cold_read_count[index]++;
	if ((int)index == cold_throw_read)
		pg_re_throw();
	if ((int)index == cold_refuse_read || !cold_read_header(segment, owner, out))
		return false;
	if ((int)index == cold_change_image)
		out[BLCKSZ - 1] ^= 1;
	return true;
}

void
cluster_undo_smgr_fd_cache_reset(void)
{
	cold_reset_count++;
}
void
cluster_scn_recovery_replay_observe(SCN scn)
{
	cold_observe_count++;
	if (scn_time_cmp(scn, cold_observed_scn) > 0)
		cold_observed_scn = scn;
}

/* None of these writing/provisioning edges is allowed in a normal census. */
void
XLogFlush(XLogRecPtr lsn pg_attribute_unused())
{
	abort();
}
bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent pg_attribute_unused(),
							  uint32 segment pg_attribute_unused(),
							  uint8 owner pg_attribute_unused(), uint32 block pg_attribute_unused(),
							  const char *buf pg_attribute_unused(),
							  bool fsync pg_attribute_unused())
{
	abort();
}
bool
cluster_undo_smgr_provision_temp_create(ClusterUndoPathIntent intent pg_attribute_unused(),
										uint32 segment pg_attribute_unused(),
										uint8 owner pg_attribute_unused(),
										char temp[MAXPGPATH] pg_attribute_unused())
{
	abort();
}
ClusterUndoSmgrPublishResult
cluster_undo_smgr_provision_temp_publish(ClusterUndoPathIntent intent pg_attribute_unused(),
										 uint32 segment pg_attribute_unused(),
										 uint8 owner pg_attribute_unused(),
										 const char *temp pg_attribute_unused(),
										 const char block0[BLCKSZ] pg_attribute_unused())
{
	abort();
}
bool
cluster_undo_smgr_provision_temp_cleanup(ClusterUndoPathIntent intent pg_attribute_unused(),
										 uint32 segment pg_attribute_unused(),
										 uint8 owner pg_attribute_unused(),
										 const char *temp pg_attribute_unused())
{
	abort();
}
