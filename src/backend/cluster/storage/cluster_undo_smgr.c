/*-------------------------------------------------------------------------
 *
 * cluster_undo_smgr.c
 *	  pgrac undo segment file I/O abstraction layer (spec-3.7 D7 carryover,
 *	  spec-3.8 真 ship implementation).
 *
 *	  Provides block-level read/write/create/fsync of per-instance undo
 *	  segment files.  Wraps the same BasicOpenFile + pg_pread/pwrite +
 *	  pg_fsync I/O pattern used inline by cluster_undo_record.c +
 *	  cluster_undo_alloc.c.
 *
 *	  The spec-3.8 hardening path wires cluster_undo_record.c and the
 *	  lifecycle helpers through this layer so undo I/O has one block-level
 *	  abstraction.  spec-3.9 CR construction + spec-3.10 CR cache will hook
 *	  above this layer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.8-undo-segment-lifecycle-autoextend.md (FROZEN v0.3 +
 *       Hardening v1.0.1;  D7 carryover from spec-3.7 Hardening v1.0.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_smgr.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h" /* before_shmem_exit (fd cache cleanup) */
#include "utils/elog.h"

#include "cluster/cluster_undo_gcs.h"		 /* cluster_undo_intent_for_owner (D2-2) */
#include "cluster/cluster_undo_record_api.h" /* smgr syscall counter bumps */
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"


/*
 * P0 perf hardening (2026-05-31): per-backend undo segment fd cache.
 *
 *	The hot undo write path called open()+pread/pwrite()+close() per record
 *	(8.5 open + 8.5 close per TPC-B txn).  Cache ONE O_RDWR fd for the
 *	most-recently-used (segment, owner) — O_RDWR serves both read_block and
 *	write_block.  Self-heals on (segment, owner) mismatch (close old + open
 *	new).  Normal COMMIT preserves the cache; PREPARE/ABORT full teardown,
 *	cache-key mismatch, and before_shmem_exit close it.  Per-backend fds to a
 *	shared segment file are independent; each pwrites its own offset range, so
 *	there is no cross-backend hazard.
 */
static uint32 cached_fd_segment = 0; /* 0 = empty */
static uint8 cached_fd_owner = 0;
/*
 * spec-5.22b D2-2: the intent is part of the cache key.  A given (segment,
 * owner) resolves to a DIFFERENT physical path for RUNTIME_SHARED (shared
 * root) vs MATERIALIZED_LOCAL (local DataDir), so a cached fd opened under
 * one intent must never be reused for the other.
 */
static ClusterUndoPathIntent cached_fd_intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
static int cached_fd = -1;
static bool cached_fd_exit_registered = false;
static uint64 provision_temp_counter = 0;

static bool provision_fsync_parent(const char *final_path);

#define PGRD_MIRROR_NAME "pgrac_undo_root.control"
#define PGRD_MIRROR_TEMP_MARKER ".pgrac-rdtmp."

typedef enum ProvisionTempNameClass {
	PROVISION_TEMP_NAME_ORDINARY,
	PROVISION_TEMP_NAME_CURRENT_BOOT,
	PROVISION_TEMP_NAME_BOOT_FOREIGN,
	PROVISION_TEMP_NAME_MALFORMED
} ProvisionTempNameClass;

static bool
provision_decimal_token(const char **cursor, uint64 *value)
{
	const char *p = *cursor;
	uint64 result = 0;

	if (*p < '0' || *p > '9')
		return false;
	if (*p == '0' && p[1] >= '0' && p[1] <= '9')
		return false;
	while (*p >= '0' && *p <= '9') {
		uint64 digit = (uint64)(*p - '0');

		if (result > (UINT64_MAX - digit) / 10)
			return false;
		result = result * 10 + digit;
		p++;
	}
	*cursor = p;
	*value = result;
	return true;
}

static ProvisionTempNameClass
provision_temp_name_class(const char *name, uint8 owner_instance)
{
	static const char marker[] = ".dat.pgrac-b0tmp.";
	const char *p;
	const char *start_begin;
	const char *pid_begin;
	const char *pid_end;
	char current_start[32];
	char current_pid[32];
	uint64 segment_id;
	uint64 ignored;
	int start_len;
	int pid_len;

	if (strstr(name, ".pgrac-b0tmp") == NULL)
		return PROVISION_TEMP_NAME_ORDINARY;
	if (strncmp(name, "seg_", 4) != 0)
		return PROVISION_TEMP_NAME_MALFORMED;
	p = name + 4;
	if (!provision_decimal_token(&p, &segment_id) || segment_id == 0 || segment_id > UINT16_MAX
		|| strncmp(p, marker, sizeof(marker) - 1) != 0
		|| ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1 != (uint64)owner_instance)
		return PROVISION_TEMP_NAME_MALFORMED;
	p += sizeof(marker) - 1;
	start_begin = p;
	if (!provision_decimal_token(&p, &ignored) || *p++ != '.')
		return PROVISION_TEMP_NAME_MALFORMED;
	pid_begin = p;
	if (!provision_decimal_token(&p, &ignored))
		return PROVISION_TEMP_NAME_MALFORMED;
	pid_end = p;
	if (*p++ != '.' || !provision_decimal_token(&p, &ignored) || ignored == 0 || *p != '\0')
		return PROVISION_TEMP_NAME_MALFORMED;

	start_len = snprintf(current_start, sizeof(current_start), "%lld", (long long)MyStartTimestamp);
	pid_len = snprintf(current_pid, sizeof(current_pid), "%d", MyProcPid);
	if (start_len < 0 || start_len >= (int)sizeof(current_start) || pid_len < 0
		|| pid_len >= (int)sizeof(current_pid))
		return PROVISION_TEMP_NAME_MALFORMED;
	if ((size_t)start_len == (size_t)((pid_begin - 1) - start_begin)
		&& memcmp(start_begin, current_start, (size_t)start_len) == 0
		&& (size_t)pid_len == (size_t)(pid_end - pid_begin)
		&& memcmp(pid_begin, current_pid, (size_t)pid_len) == 0)
		return PROVISION_TEMP_NAME_CURRENT_BOOT;
	return PROVISION_TEMP_NAME_BOOT_FOREIGN;
}

static void
fd_cache_close(void)
{
	if (cached_fd >= 0) {
		close(cached_fd);
		cluster_undo_record_note_smgr_close();
		cached_fd = -1;
		cached_fd_segment = 0;
		cached_fd_owner = 0;
	}
}

static void
fd_cache_on_exit(int code, Datum arg)
{
	(void)code;
	(void)arg;
	fd_cache_close();
}

void
cluster_undo_smgr_ensure_exit_hook(void)
{
	if (!cached_fd_exit_registered) {
		before_shmem_exit(fd_cache_on_exit, (Datum)0);
		cached_fd_exit_registered = true;
	}
}

/*
 * get_segment_fd -- return a cached O_RDWR fd for (segment, owner), opening
 *	(and caching) one on a miss.  Returns -1 on open failure.  The caller MUST
 *	NOT close the returned fd (the cache owns it).
 */
static int
get_segment_fd(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance)
{
	char path[MAXPGPATH];
	int fd;

	if (cached_fd >= 0 && cached_fd_segment == segment_id && cached_fd_owner == owner_instance
		&& cached_fd_intent == intent)
		return cached_fd; /* hit */

	fd_cache_close(); /* miss: drop the stale fd first */

	if (cluster_undo_path_resolve(intent, owner_instance, segment_id, path, sizeof(path)) != 0)
		return -1;
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		return -1;
	cluster_undo_record_note_smgr_open();

	cached_fd = fd;
	cached_fd_segment = segment_id;
	cached_fd_owner = owner_instance;
	cached_fd_intent = intent;
	cluster_undo_smgr_ensure_exit_hook();
	return fd;
}

/*
 * cluster_undo_smgr_fd_cache_reset -- close the cached fd during full local
 *	teardown or explicit cache invalidation.  Normal COMMIT does not call it.
 */
void
cluster_undo_smgr_fd_cache_reset(void)
{
	fd_cache_close();
}


bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
							 uint32 block_no, char *buf)
{
	int fd;
	off_t offset;
	ssize_t nread;
	bool ok;

	if (buf == NULL || block_no >= UNDO_BLOCKS_PER_SEGMENT)
		return false;

	fd = get_segment_fd(intent, segment_id, owner_instance);
	if (fd < 0)
		return false;

	offset = (off_t)block_no * BLCKSZ;
	nread = pg_pread(fd, buf, BLCKSZ, offset);
	cluster_undo_record_note_smgr_pread();
	ok = (nread == BLCKSZ);
	return ok;
}


bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
							  uint32 block_no, const char *buf, bool do_fsync)
{
	int fd;
	off_t offset;
	ssize_t nwritten;
	bool ok = true;

	if (buf == NULL || block_no >= UNDO_BLOCKS_PER_SEGMENT)
		return false;

	fd = get_segment_fd(intent, segment_id, owner_instance);
	if (fd < 0)
		return false;

	offset = (off_t)block_no * BLCKSZ;
	nwritten = pg_pwrite(fd, buf, BLCKSZ, offset);
	cluster_undo_record_note_smgr_pwrite();
	if (nwritten != BLCKSZ)
		ok = false;

	if (ok && do_fsync) {
		if (pg_fsync(fd) != 0)
			ok = false;
	}
	return ok;
}


/*
 * cluster_undo_smgr_read_header_bytes / _write_header_bytes (spec-3.11 D2)
 *
 *   Targeted read/write of a byte range within segment header block 0 (e.g.
 *   one 32-byte TTSlot at offset 112 + slot*32).  Used by the durable TT slot
 *   commit/lookup path: each committing xact owns a DISTINCT slot, so per-slot
 *   writes hit non-overlapping byte ranges and need NO lock (POSIX concurrent
 *   pwrite to disjoint ranges is safe; lifecycle writes the header prefix at
 *   offset 32-111, also disjoint from the slot array).  The write does NOT
 *   fsync: the durable TT commit is WAL-protected (XLOG_UNDO_TT_SLOT_COMMIT),
 *   so a torn data-file write is recovered by redo (spec-3.11 C10).  offset+len
 *   must stay inside block 0 (BLCKSZ).
 */
bool
cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
									uint8 owner_instance, uint32 offset, char *buf, uint32 len)
{
	int fd;
	ssize_t nread;

	if (buf == NULL || len == 0 || (uint64)offset + (uint64)len > (uint64)BLCKSZ)
		return false;

	fd = get_segment_fd(intent, segment_id, owner_instance);
	if (fd < 0)
		return false;

	nread = pg_pread(fd, buf, len, (off_t)offset);
	cluster_undo_record_note_smgr_pread();
	return (nread == (ssize_t)len);
}

bool
cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
									 uint8 owner_instance, uint32 offset, const char *buf,
									 uint32 len)
{
	int fd;
	ssize_t nwritten;

	if (buf == NULL || len == 0 || (uint64)offset + (uint64)len > (uint64)BLCKSZ)
		return false;

	fd = get_segment_fd(intent, segment_id, owner_instance);
	if (fd < 0)
		return false;

	nwritten = pg_pwrite(fd, buf, len, (off_t)offset);
	cluster_undo_record_note_smgr_pwrite();
	/* No fsync: WAL-protected (spec-3.11 C10). */
	return (nwritten == (ssize_t)len);
}


ClusterUndoSmgrFinalState
cluster_undo_smgr_probe_segment(ClusterUndoPathIntent intent, uint32 segment_id,
								uint8 owner_instance, char block0[BLCKSZ])
{
	PGAlignedBlock private_block;
	char path[MAXPGPATH];
	struct stat st;
	ssize_t nread;
	int fd;
	int save_errno;

	if (block0 == NULL
		|| cluster_undo_path_resolve(intent, owner_instance, segment_id, path, sizeof(path)) != 0)
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return errno == ENOENT ? CLUSTER_UNDO_SMGR_FINAL_ABSENT : CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	if (fstat(fd, &st) != 0) {
		save_errno = errno;
		(void)close(fd);
		errno = save_errno;
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	}
	if (st.st_size != UNDO_SEGMENT_SIZE_BYTES) {
		if (close(fd) != 0)
			return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
		return CLUSTER_UNDO_SMGR_FINAL_INVALID;
	}
	nread = pg_pread(fd, private_block.data, BLCKSZ, 0);
	cluster_undo_record_note_smgr_pread();
	if (nread < 0) {
		save_errno = errno;
		(void)close(fd);
		errno = save_errno;
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	}
	if (close(fd) != 0)
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	if (nread != BLCKSZ
		|| !cluster_undo_segment_header_identity_ok(private_block.data, segment_id, owner_instance))
		return CLUSTER_UNDO_SMGR_FINAL_INVALID;
	if (!provision_fsync_parent(path))
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	memcpy(block0, private_block.data, BLCKSZ);
	return CLUSTER_UNDO_SMGR_FINAL_EXACT;
}


static bool
provision_temp_prefix(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
					  char final_path[MAXPGPATH], char prefix[MAXPGPATH])
{
	int ret;

	if (cluster_undo_path_resolve(intent, owner_instance, segment_id, final_path, MAXPGPATH) != 0)
		return false;
	ret = snprintf(prefix, MAXPGPATH, "%s.pgrac-b0tmp.%lld.%d.", final_path,
				   (long long)MyStartTimestamp, MyProcPid);
	return ret >= 0 && ret < MAXPGPATH;
}


static bool
provision_temp_owned(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
					 const char *temp_path, char final_path[MAXPGPATH])
{
	char prefix[MAXPGPATH];
	const char *suffix;

	if (temp_path == NULL
		|| !provision_temp_prefix(intent, segment_id, owner_instance, final_path, prefix)
		|| strncmp(temp_path, prefix, strlen(prefix)) != 0)
		return false;
	suffix = temp_path + strlen(prefix);
	if (*suffix == '\0')
		return false;
	while (*suffix != '\0') {
		if (*suffix < '0' || *suffix > '9')
			return false;
		suffix++;
	}
	return true;
}


bool
cluster_undo_smgr_provision_temp_create(ClusterUndoPathIntent intent, uint32 segment_id,
										uint8 owner_instance, char temp_path[MAXPGPATH])
{
	char final_path[MAXPGPATH];
	char prefix[MAXPGPATH];
	uint64 counter;
	int ret;
	int fd;

	if (temp_path == NULL
		|| !provision_temp_prefix(intent, segment_id, owner_instance, final_path, prefix))
		return false;
	counter = ++provision_temp_counter;
	ret = snprintf(temp_path, MAXPGPATH, "%s" UINT64_FORMAT, prefix, counter);
	if (ret < 0 || ret >= MAXPGPATH) {
		temp_path[0] = '\0';
		return false;
	}
	fd = BasicOpenFile(temp_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0) {
		temp_path[0] = '\0';
		return false;
	}
	if (close(fd) != 0) {
		int save_errno = errno;

		(void)unlink(temp_path);
		temp_path[0] = '\0';
		errno = save_errno;
		return false;
	}
	return true;
}


bool
cluster_undo_smgr_provision_temp_cleanup(ClusterUndoPathIntent intent, uint32 segment_id,
										 uint8 owner_instance, const char *temp_path)
{
	char final_path[MAXPGPATH];

	if (!provision_temp_owned(intent, segment_id, owner_instance, temp_path, final_path))
		return false;
	return unlink(temp_path) == 0 || errno == ENOENT;
}


bool
cluster_undo_smgr_cleanup_boot_foreign_temps(ClusterUndoPathIntent intent, uint8 owner_instance)
{
	char final_path[MAXPGPATH];
	char directory[MAXPGPATH];
	char candidate[MAXPGPATH];
	uint32 first_segment;
	DIR *dir;
	struct dirent *entry;
	bool removed = false;
	bool ok = true;

	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return false;
	first_segment = ((uint32)owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (cluster_undo_path_resolve(intent, owner_instance, first_segment, final_path,
								  sizeof(final_path))
		!= 0)
		return false;
	strlcpy(directory, final_path, sizeof(directory));
	get_parent_directory(directory);
	dir = AllocateDir(directory);
	if (dir == NULL)
		return false;

	errno = 0;
	while ((entry = readdir(dir)) != NULL) {
		ProvisionTempNameClass name_class;
		struct stat st;
		int ret;

		name_class = provision_temp_name_class(entry->d_name, owner_instance);
		if (name_class == PROVISION_TEMP_NAME_ORDINARY)
			continue;
		if (name_class == PROVISION_TEMP_NAME_CURRENT_BOOT
			|| name_class == PROVISION_TEMP_NAME_MALFORMED) {
			ok = false;
			break;
		}
		ret = snprintf(candidate, sizeof(candidate), "%s/%s", directory, entry->d_name);
		if (ret < 0 || ret >= (int)sizeof(candidate) || lstat(candidate, &st) != 0
			|| !S_ISREG(st.st_mode) || unlink(candidate) != 0) {
			ok = false;
			break;
		}
		removed = true;
		errno = 0;
	}
	if (entry == NULL && errno != 0)
		ok = false;
	if (FreeDir(dir) != 0)
		ok = false;
	if (removed && !provision_fsync_parent(final_path))
		ok = false;
	return ok;
}


static bool
root_descriptor_path(const char *root_directory, char final_path[MAXPGPATH])
{
	int ret;

	if (root_directory == NULL || root_directory[0] == '\0')
		return false;
	ret = snprintf(final_path, MAXPGPATH, "%s/%s", root_directory, PGRD_MIRROR_NAME);
	return ret >= 0 && ret < MAXPGPATH;
}


static ClusterUndoSmgrRootMirrorState
root_descriptor_read(const char *root_directory,
					 const uint8 expected[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
					 uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	char final_path[MAXPGPATH];
	uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	struct stat path_st;
	struct stat fd_st;
	ssize_t nread;
	int save_errno;
	int open_flags = O_RDONLY | PG_BINARY;
	int fd;

	if (observed == NULL || !root_descriptor_path(root_directory, final_path))
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	if (lstat(final_path, &path_st) != 0)
		return errno == ENOENT ? CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT
							   : CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	if (!S_ISREG(path_st.st_mode) || path_st.st_size != CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD;

#ifdef O_NOFOLLOW
	open_flags |= O_NOFOLLOW;
#endif
	fd = BasicOpenFile(final_path, open_flags);
	if (fd < 0) {
#ifdef ELOOP
		if (errno == ELOOP)
			return CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD;
#endif
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	}
	if (fstat(fd, &fd_st) != 0) {
		save_errno = errno;
		(void)close(fd);
		errno = save_errno;
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	}
	if (!S_ISREG(fd_st.st_mode) || fd_st.st_size != CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES
		|| fd_st.st_dev != path_st.st_dev || fd_st.st_ino != path_st.st_ino) {
		if (close(fd) != 0)
			return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD;
	}
	nread = pg_pread(fd, image, sizeof(image), 0);
	if (nread < 0) {
		save_errno = errno;
		(void)close(fd);
		errno = save_errno;
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	}
	if (close(fd) != 0)
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	if (nread != sizeof(image) || (expected != NULL && memcmp(image, expected, sizeof(image)) != 0))
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD;
	if (!provision_fsync_parent(final_path))
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	memcpy(observed, image, sizeof(image));
	return CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT;
}


ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_probe(const char *root_directory,
										const uint8 expected[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
										uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	if (expected == NULL)
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	return root_descriptor_read(root_directory, expected, observed);
}


ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_read_candidate(const char *root_directory,
												 uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	return root_descriptor_read(root_directory, NULL, observed);
}


ClusterUndoSmgrRootMirrorState
cluster_undo_smgr_root_descriptor_publish(const char *root_directory,
										  const uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	char final_path[MAXPGPATH];
	char temp_path[MAXPGPATH];
	uint8 observed[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	ClusterUndoSmgrRootMirrorState state;
	uint64 counter;
	ssize_t nwritten;
	bool temp_ok = true;
	int ret;
	int fd;

	if (image == NULL || !root_descriptor_path(root_directory, final_path))
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	counter = ++provision_temp_counter;
	if (counter == 0)
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	ret = snprintf(temp_path, sizeof(temp_path), "%s%s%lld.%d." UINT64_FORMAT, final_path,
				   PGRD_MIRROR_TEMP_MARKER, (long long)MyStartTimestamp, MyProcPid, counter);
	if (ret < 0 || ret >= (int)sizeof(temp_path))
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	fd = BasicOpenFile(temp_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	nwritten = pg_pwrite(fd, image, CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES, 0);
	if (nwritten != CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
		temp_ok = false;
	if (temp_ok && pg_fsync(fd) != 0)
		temp_ok = false;
	if (close(fd) != 0)
		temp_ok = false;
	if (!temp_ok) {
		(void)unlink(temp_path);
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	}

	if (link(temp_path, final_path) == 0) {
		bool parent_ok = provision_fsync_parent(final_path);
		bool cleanup_ok = unlink(temp_path) == 0;

		return parent_ok && cleanup_ok ? CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED
									   : CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	}
	if (errno != EEXIST) {
		(void)unlink(temp_path);
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	}
	if (unlink(temp_path) != 0)
		return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
	state = cluster_undo_smgr_root_descriptor_probe(root_directory, image, observed);
	if (state == CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT)
		return state;
	if (state == CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD)
		return state;
	return CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR;
}


static bool
provision_fsync_parent(const char *final_path)
{
	char parent[MAXPGPATH];
	int fd;
	bool ok;

	strlcpy(parent, final_path, sizeof(parent));
	get_parent_directory(parent);
	fd = BasicOpenFile(parent, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	ok = pg_fsync(fd) == 0;
	if (close(fd) != 0)
		ok = false;
	return ok;
}


ClusterUndoSmgrPublishResult
cluster_undo_smgr_provision_temp_publish(ClusterUndoPathIntent intent, uint32 segment_id,
										 uint8 owner_instance, const char *temp_path,
										 const char block0[BLCKSZ])
{
	char final_path[MAXPGPATH];
	PGAlignedBlock observed;
	ClusterUndoSmgrFinalState final_state;
	ssize_t nwritten;
	bool temp_ok = true;
	int fd;
	int save_errno;

	if (block0 == NULL
		|| !cluster_undo_segment_header_identity_ok(block0, segment_id, owner_instance)
		|| !provision_temp_owned(intent, segment_id, owner_instance, temp_path, final_path))
		return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	fd = BasicOpenFile(temp_path, O_RDWR | PG_BINARY);
	if (fd < 0)
		return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	if (ftruncate(fd, (off_t)UNDO_SEGMENT_SIZE_BYTES) != 0) {
		save_errno = errno;
		(void)close(fd);
		errno = save_errno;
		(void)cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance,
													   temp_path);
		return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	}
	nwritten = pg_pwrite(fd, block0, BLCKSZ, 0);
	cluster_undo_record_note_smgr_pwrite();
	if (nwritten != BLCKSZ)
		temp_ok = false;
	if (temp_ok && pg_fsync(fd) != 0)
		temp_ok = false;
	if (close(fd) != 0)
		temp_ok = false;
	if (!temp_ok) {
		(void)cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance,
													   temp_path);
		return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	}

	if (link(temp_path, final_path) == 0) {
		if (!provision_fsync_parent(final_path)) {
			/*
			 * The two names now reference the same inode.  Never leave the
			 * private name available for a retry: rewriting it would rewrite
			 * the already-visible final file and defeat no-replace publication.
			 */
			(void)cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance,
														   temp_path);
			return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
		}
		if (!cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance,
													  temp_path))
			return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
		return CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED;
	}
	if (errno != EEXIST) {
		(void)cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance,
													   temp_path);
		return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	}

	final_state
		= cluster_undo_smgr_probe_segment(intent, segment_id, owner_instance, observed.data);
	if (final_state == CLUSTER_UNDO_SMGR_FINAL_EXACT) {
		if (!cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance,
													  temp_path))
			return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
		return CLUSTER_UNDO_SMGR_PUBLISH_EXISTS;
	}
	(void)cluster_undo_smgr_provision_temp_cleanup(intent, segment_id, owner_instance, temp_path);
	if (final_state == CLUSTER_UNDO_SMGR_FINAL_INVALID)
		return CLUSTER_UNDO_SMGR_PUBLISH_INVALID;
	return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
}


int
cluster_undo_smgr_create_segment_file(uint32 segment_id, uint8 owner_instance)
{
	if (owner_instance < 1 || owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return -2;

	/*
	 * cluster_undo_segment_allocate is idempotent — if file already
	 * exists with valid header, no rewrite;  if it doesn't, create +
	 * init + WAL-protect.  May ereport(ERROR) on FS error.
	 */
	cluster_undo_segment_allocate(segment_id, owner_instance);

	/* Verify file exists after the call. */
	{
		char path[MAXPGPATH];
		int ret;
		int fd;

		ret = cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner_instance),
										owner_instance, segment_id, path, sizeof(path));
		if (ret != 0)
			return -2;

		fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0)
			return -1;
		close(fd);
	}

	return 0;
}


bool
cluster_undo_smgr_fsync_segment_file(uint32 segment_id, uint8 owner_instance)
{
	int fd;

	fd = get_segment_fd(cluster_undo_intent_for_owner(owner_instance), segment_id, owner_instance);
	if (fd < 0)
		return false;

	return (pg_fsync(fd) == 0);
}
