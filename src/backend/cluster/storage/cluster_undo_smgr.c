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
#include "cluster/cluster_undo_recovery.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "common/file_perm.h"
#include "storage/fd.h"
#include "storage/ipc.h" /* before_shmem_exit (fd cache cleanup) */
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "cluster/cluster_undo_gcs.h"		 /* cluster_undo_intent_for_owner (D2-2) */
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_undo_record_api.h" /* smgr syscall counter bumps */
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_block0.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/clog_file.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cluster_recovery_bootstrap.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#endif


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
 *	DATA writes remain disjoint; block-zero writes use its content lock.
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
static bool header_write_recorded(int fd, ClusterUndoPathIntent intent, uint32 segment_id,
								  uint8 owner_instance);

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
	bool resolved = false;

	/* A cached descriptor carries no recovery authority. */
	if (intent == CLUSTER_UNDO_PATH_RECOVERY_SHARED) {
		if (cluster_undo_path_resolve(intent, owner_instance, segment_id, path, sizeof(path))
			!= 0) {
			fd_cache_close();
			return -1;
		}
		resolved = true;
	}

	if (cached_fd >= 0 && cached_fd_segment == segment_id && cached_fd_owner == owner_instance
		&& cached_fd_intent == intent)
		return cached_fd; /* hit */

	fd_cache_close(); /* miss: drop the stale fd first */

	if (!resolved
		&& cluster_undo_path_resolve(intent, owner_instance, segment_id, path, sizeof(path)) != 0)
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

static bool
undo_recovery_file_stat(int fd, ClusterUndoSmgrRecoveryFileV1 *file)
{
	struct stat st;

	if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0)
		return false;
	memset(file, 0, sizeof(*file));
	file->exists = true;
	file->device = st.st_dev;
	file->inode = st.st_ino;
	file->size = st.st_size;
	return true;
}

static bool
undo_recovery_file_same(const ClusterUndoSmgrRecoveryFileV1 *a,
						const ClusterUndoSmgrRecoveryFileV1 *b)
{
	return a->exists == b->exists && a->device == b->device && a->inode == b->inode
		   && a->size == b->size;
}

static bool
undo_recovery_read(int fd, uint64 size, uint32 block, char out[BLCKSZ])
{
	uint64 offset = (uint64)block * BLCKSZ;
	Size length = size <= offset ? 0 : Min(size - offset, BLCKSZ);

	memset(out, 0, BLCKSZ);
	if (length == 0)
		return true;
	/* Known EOF is zero extension. An unexpected short/error read is not. */
	return pg_pread(fd, out, length, (off_t)offset) == (ssize_t)length;
}

#ifdef USE_PGRAC_CLUSTER
/* An unpublished creation child has no crash-left runtime TT to resolve.
 * Validate the COMPLETE original BOOT batch instead. Only an explicitly
 * registered initial TT is read, through the original smgr path and one
 * address-bound native borrow; the cohort's unregistered local seg_0 seed is
 * never opened. This grants neither a runtime/redo owner nor a TT writer. */
bool
cluster_undo_smgr_creation_startup_v2(void)
{
	const PgracInitdbRecoveryContext *creator = cluster_wal_thread_initdb_recovery_context();
	ClusterControlNativeTableInfoV2 selected, current;
	ClusterRecoveryFileIdentityV2 boot[PGRAC_INITDB_BOOT_FILES_V2];
	ClusterControlNativeModeV2 mode = cluster_control_native_files_mode_v2();
	uint64 relmaps;

	if (creator == NULL && mode != CLUSTER_NATIVE_FILES_CREATION_V2)
		return false;
	if (creator == NULL || mode != CLUSTER_NATIVE_FILES_CREATION_V2
		|| MyBackendType != B_STANDALONE_BACKEND || IsUnderPostmaster
		|| IsPostmasterEnvironment || cluster_node_id != -1
		|| !(IsBootstrapProcessingMode() || IsInitProcessingMode())
		|| RecoveryInProgress() || HotStandbyActive()
		|| !cluster_control_native_files_info_v2(&selected)
		|| selected.mode != CLUSTER_NATIVE_FILES_CREATION_V2
		|| selected.table_generation == 0 || creator->creator_pid == 0
		|| selected.startup_generation != creator->creator_pid
		|| selected.reservation_count != 0
		|| memcmp(&selected.creation, creator, sizeof(*creator)) != 0
		|| !pgrac_initdb_recovery_boot_v2(&selected.creation, boot))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native creation startup requires its original complete BOOT batch")));
	relmaps = (selected.creation.cohort[0] || selected.creation.cohort[1])
		&& selected.creation.cohort_origin != 0 ? 0 : 4;
	if (selected.count != relmaps + selected.creation.boot_file_count)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native creation startup has an incomplete file inventory")));
	for (unsigned i = 0; i < selected.creation.boot_file_count; i++) {
		ClusterControlNativeFileBorrowV2 borrow = {0};
		ClusterControlFileKeyV2 key = {0};
		uint8 reference[CLUSTER_CLOG_FILE_REFERENCE_BYTES];
		volatile int fd = -1;
		volatile bool ok = false;

		key.kind = boot[i].kind;
		key.origin = boot[i].origin;
		key.object_id = boot[i].object_id;
		if (!cluster_control_native_file_acquire_v2(&key, &borrow))
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native creation BOOT file is not registered")));
		PG_TRY();
		{
			ok = borrow.table_generation == selected.table_generation
				&& borrow.startup_generation == selected.startup_generation
				&& cluster_recovery_file_identity_encode_v2(&boot[i], reference, sizeof(reference))
				&& cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), &borrow.expected)
				&& cluster_control_native_file_current_v2(&borrow);
			if (ok && boot[i].kind == CLUSTER_RECOVERY_FILE_UNDO_V2) {
				ClusterRecoveryBootFileV2 observed;
				char path[MAXPGPATH];
				struct stat before, after, named;

				/* The creator parser admits only its explicit initial TT0 here.
				 * The BOOT reader compares its full pristine TT40 image and
				 * zero tail, not a terminal bit or a fabricated local owner. */
				ok = cluster_undo_path_resolve(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL,
					boot[i].origin + 1, boot[i].object_id, path, sizeof(path)) == 0
					&& (fd = BasicOpenFile(path, O_RDONLY | PG_BINARY | O_NOFOLLOW)) >= 0
					&& fstat(fd, &before) == 0
					&& cluster_control_native_file_current_v2(&borrow)
					&& cluster_undo_bootstrap_read_v2(fd, &boot[i], &observed)
					&& fstat(fd, &after) == 0
					&& fstatat(AT_FDCWD, path, &named, AT_SYMLINK_NOFOLLOW) == 0
					&& before.st_dev == after.st_dev && before.st_ino == after.st_ino
					&& before.st_size == after.st_size && after.st_dev == named.st_dev
					&& after.st_ino == named.st_ino && after.st_size == named.st_size
					&& cluster_control_native_file_current_v2(&borrow);
			}
		}
		PG_FINALLY();
		{
			if (fd >= 0 && close(fd) != 0)
				ok = false;
			if (!cluster_control_native_file_release_v2(&borrow))
				ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("could not return native creation BOOT file borrow")));
		}
		PG_END_TRY();
		if (!ok)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native creation BOOT file does not match its initial identity")));
	}
	if (creator != cluster_wal_thread_initdb_recovery_context()
		|| !cluster_control_native_files_info_v2(&current)
		|| current.mode != selected.mode || current.table_generation != selected.table_generation
		|| current.startup_generation != selected.startup_generation
		|| current.count != selected.count || current.reservation_count != 0
		|| memcmp(&current.creation, &selected.creation, sizeof(current.creation)) != 0)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native creation startup batch changed during validation")));
	return true;
}

/* The original recovery scope still owns source/cut/path authority. Bind each
 * read to its independently selected file, and bind subsequent reads to the
 * first by-value observation as well as the original inode/size. Absence is
 * only observed; no writer or full-image authority is introduced here. */
static bool
undo_native_recovery_read(uint32 segment, uint8 instance, uint32 block,
	const ClusterUndoSmgrRecoveryFileV1 *expected, ClusterUndoSmgrRecoveryFileV1 *file,
	char out[BLCKSZ])
{
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	ClusterUndoSmgrRecoveryFileV1 observed = { 0 };
	PGAlignedBlock header = { 0 }, page = { 0 };
	char path[MAXPGPATH];
	volatile int fd = -1;
	volatile bool ok = false;

	StaticAssertStmt(sizeof(observed.native_file_reference) == CLUSTER_CLOG_FILE_REFERENCE_BYTES,
		"recovery observations must keep the complete selected file identity");
	if (out == NULL || (expected == NULL && file == NULL)
		|| instance == 0 || instance > UNDO_OWNER_INSTANCE_MAX || segment == 0
		|| segment > UINT16_MAX
		|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != instance
		|| block >= UNDO_BLOCKS_PER_SEGMENT || (expected != NULL && !expected->native_selected))
		return false;
	key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	key.origin = instance - 1;
	key.object_id = segment;
	if (!cluster_control_native_file_acquire_v2(&key, &borrow))
		return false;
	PG_TRY();
	{
		do {
			int owned_fd;

			if (!cluster_control_native_file_current_v2(&borrow)
				|| borrow.table_generation == 0 || borrow.startup_generation == 0
				|| (expected != NULL
					&& (expected->native_table_generation != borrow.table_generation
						|| expected->native_startup_generation != borrow.startup_generation
						|| !cluster_recovery_file_identity_matches_v2(expected->native_file_reference,
							sizeof(expected->native_file_reference), &borrow.expected)))
				|| cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment,
					path, sizeof(path)) != 0)
				break;
			fd = BasicOpenFile(path, O_RDONLY | PG_BINARY | O_NOFOLLOW);
			if (fd < 0 && errno != ENOENT)
				break;
			if (!cluster_control_native_file_current_v2(&borrow))
				break;
			if (fd >= 0) {
				if (!undo_recovery_file_stat(fd, &observed)
					|| observed.size < BLCKSZ || observed.size > UNDO_SEGMENT_SIZE_BYTES
					|| (expected != NULL && !undo_recovery_file_same(expected, &observed))
					|| !undo_recovery_read(fd, observed.size, 0, header.data)
					|| !cluster_undo_segment_header_identity_ok(header.data, segment, instance)
					|| !cluster_tt_file_reference_matches_v2(header.data, BLCKSZ, &borrow.expected)
					|| cluster_undo_segment_header_integrity(header.data) != CLUSTER_UNDO_HEADER_VALID)
					break;
			} else if (expected != NULL && expected->exists)
				break;
			if (!cluster_control_native_file_current_v2(&borrow))
				break;
			if (block == 0)
				page = header;
			else if (observed.exists && !undo_recovery_read(fd, observed.size, block, page.data))
				break;
			if (!cluster_control_native_file_current_v2(&borrow)
				|| !cluster_recovery_file_identity_encode_v2(&borrow.expected,
					observed.native_file_reference, sizeof(observed.native_file_reference)))
				break;
			observed.native_selected = true;
			observed.native_table_generation = borrow.table_generation;
			observed.native_startup_generation = borrow.startup_generation;
			if (fd >= 0) {
				/* A failed close must not be retried on a possibly retired FD. */
				owned_fd = fd; fd = -1;
				if (close(owned_fd) != 0)
					break;
			}
			if (!cluster_control_native_file_current_v2(&borrow))
				break;
			ok = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (fd >= 0 && close(fd) != 0)
			ok = false;
		if (!cluster_control_native_file_release_v2(&borrow))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not release recovery TT file identity borrow")));
	}
	PG_END_TRY();
	if (ok) {
		if (file != NULL)
			*file = observed;
		memcpy(out, page.data, BLCKSZ);
	}
	return ok;
}

/* Size preparation of an existing recovery file does not publish block zero.
 * The full-image owner retains source/cut/path and writer authority. Keep its
 * original observation, both TT40 images and all I/O on one selected identity;
 * absence requires a separate qualified creation path, never O_CREAT here. */
static bool
undo_native_recovery_materialize(uint32 segment, uint8 instance,
	const ClusterUndoSmgrRecoveryFileV1 *expected, const char block0[BLCKSZ],
	const char final_header[BLCKSZ])
{
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	ClusterUndoSmgrRecoveryFileV1 observed;
	PGAlignedBlock page;
	char path[MAXPGPATH], checked[MAXPGPATH];
	volatile int fd = -1;
	volatile bool ok = false;

	if (expected == NULL || !expected->native_selected || !expected->exists
		|| expected->native_table_generation == 0 || expected->native_startup_generation == 0
		|| block0 == NULL || final_header == NULL || instance == 0
		|| instance > UNDO_OWNER_INSTANCE_MAX || segment == 0 || segment > UINT16_MAX
		|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != instance)
		return false;
	key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	key.origin = instance - 1;
	key.object_id = segment;
	if (!cluster_control_native_file_acquire_v2(&key, &borrow))
		return false;
	PG_TRY();
	{
		do {
			int owned_fd;
			bool directories_synced = true;

			if (!cluster_control_native_file_current_v2(&borrow)
				|| expected->native_table_generation != borrow.table_generation
				|| expected->native_startup_generation != borrow.startup_generation
				|| !cluster_recovery_file_identity_matches_v2(expected->native_file_reference,
					sizeof(expected->native_file_reference), &borrow.expected)
				|| !cluster_undo_segment_header_identity_ok(block0, segment, instance)
				|| !cluster_tt_file_reference_matches_v2(block0, BLCKSZ, &borrow.expected)
				|| cluster_undo_segment_header_integrity(block0) != CLUSTER_UNDO_HEADER_VALID
				|| !cluster_undo_segment_header_identity_ok(final_header, segment, instance)
				|| !cluster_tt_file_reference_matches_v2(final_header, BLCKSZ, &borrow.expected)
				|| cluster_undo_segment_header_integrity(final_header) != CLUSTER_UNDO_HEADER_VALID
				|| cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment,
					path, sizeof(path)) != 0)
				break;
			fd_cache_close();
			fd = BasicOpenFile(path, O_RDWR | PG_BINARY | O_NOFOLLOW);
			if (fd < 0 || !cluster_control_native_file_current_v2(&borrow)
				|| !undo_recovery_file_stat(fd, &observed)
				|| observed.size < BLCKSZ || observed.size > UNDO_SEGMENT_SIZE_BYTES
				|| !undo_recovery_file_same(expected, &observed)
				|| !undo_recovery_read(fd, observed.size, 0, page.data)
				|| memcmp(page.data, block0, BLCKSZ) != 0
				|| cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment,
					checked, sizeof(checked)) != 0 || strcmp(path, checked) != 0
				|| !cluster_control_native_file_current_v2(&borrow))
				break;
			if (ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES) != 0
				|| !cluster_control_native_file_current_v2(&borrow) || pg_fsync(fd) != 0
				|| !cluster_control_native_file_current_v2(&borrow))
				break;
			owned_fd = fd; fd = -1;
			if (close(owned_fd) != 0 || !cluster_control_native_file_current_v2(&borrow))
				break;
			/* Preserve both original directory syncs, but keep any owned
			 * directory FD in the same ERROR cleanup as the file FD. */
			for (unsigned depth = 0; depth < 2; depth++) {
				get_parent_directory(checked);
				fd = BasicOpenFile(checked, O_RDONLY | PG_BINARY);
				if (fd < 0 || pg_fsync(fd) != 0) {
					directories_synced = false;
					break;
				}
				owned_fd = fd; fd = -1;
				if (close(owned_fd) != 0 || !cluster_control_native_file_current_v2(&borrow)) {
					directories_synced = false;
					break;
				}
			}
			if (!directories_synced)
				break;
			ok = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (fd >= 0 && close(fd) != 0)
			ok = false;
		if (!cluster_control_native_file_release_v2(&borrow))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not release recovery TT size-preparation borrow")));
	}
	PG_END_TRY();
	return ok;
}
#endif

bool
cluster_undo_smgr_recovery_probe_v1(uint32 segment, uint8 instance,
									ClusterUndoSmgrRecoveryFileV1 *file, char block0[BLCKSZ])
{
	ClusterUndoSmgrRecoveryFileV1 observed = { 0 };
	PGAlignedBlock page = { 0 };
	char path[MAXPGPATH];
	int flags = O_RDONLY | PG_BINARY;
	int fd;
	bool ok;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return undo_native_recovery_read(segment, instance, 0, NULL, file, block0);
#endif

	if (file == NULL || block0 == NULL
		|| cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment, path,
									 sizeof(path))
			   != 0)
		return false;
#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	fd = BasicOpenFile(path, flags);
	if (fd < 0) {
		if (errno != ENOENT)
			return false;
	} else {
		ok = undo_recovery_file_stat(fd, &observed)
			 && undo_recovery_read(fd, observed.size, 0, page.data);
		if (close(fd) != 0)
			ok = false;
		if (!ok)
			return false;
	}
	*file = observed;
	memcpy(block0, page.data, BLCKSZ);
	return true;
}

bool
cluster_undo_smgr_recovery_read_block_v1(uint32 segment, uint8 instance, uint32 block,
										 const ClusterUndoSmgrRecoveryFileV1 *expected,
										 char out[BLCKSZ])
{
	ClusterUndoSmgrRecoveryFileV1 observed;
	PGAlignedBlock page;
	char path[MAXPGPATH];
	int flags = O_RDONLY | PG_BINARY;
	int fd;
	bool ok;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2
		|| (expected != NULL && expected->native_selected)) {
		if (expected == NULL)
			return false;
		return undo_native_recovery_read(segment, instance, block, expected, NULL, out);
	}
#endif

	if (expected == NULL || out == NULL || block >= UNDO_BLOCKS_PER_SEGMENT
		|| cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment, path,
									 sizeof(path))
			   != 0)
		return false;
#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	fd = BasicOpenFile(path, flags);
	if (fd < 0) {
		if (errno != ENOENT || expected->exists)
			return false;
		memset(out, 0, BLCKSZ);
		return true;
	}
	ok = undo_recovery_file_stat(fd, &observed) && undo_recovery_file_same(expected, &observed)
		 && undo_recovery_read(fd, observed.size, block, page.data);
	if (close(fd) != 0)
		ok = false;
	if (ok)
		memcpy(out, page.data, BLCKSZ);
	return ok;
}

bool
cluster_undo_smgr_recovery_materialize_v1(uint32 segment, uint8 instance,
										  const ClusterUndoSmgrRecoveryFileV1 *expected,
										  const char block0[BLCKSZ],
										  const char final_header[BLCKSZ])
{
	ClusterUndoSmgrRecoveryFileV1 observed;
	PGAlignedBlock page;
	char path[MAXPGPATH], checked[MAXPGPATH], parent[MAXPGPATH];
	int flags = O_RDWR | PG_BINARY;
	int fd;
	bool ok;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2
		|| (expected != NULL && expected->native_selected))
		return undo_native_recovery_materialize(segment, instance, expected, block0, final_header);
#endif

	if (expected == NULL || block0 == NULL || final_header == NULL
		|| !((uint8)final_header[57] == CLUSTER_UNDO_TT_LAYOUT_V2
				 ? UndoSegmentHeaderV2_identity_matches(final_header, BLCKSZ, segment, instance)
				 : cluster_undo_segment_header_identity_ok(final_header, segment, instance))
		|| cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment, path,
									 sizeof(path))
			   != 0)
		return false;
	fd_cache_close();
	strlcpy(parent, path, sizeof(parent));
	get_parent_directory(parent);
	if (!expected->exists) {
		/* The canonical undo root already exists; create only this instance's
		 * native directory, never an alternative local root. */
		if (mkdir(parent, pg_dir_create_mode) != 0 && errno != EEXIST)
			return false;
		flags |= O_CREAT | O_EXCL;
	}
#ifdef O_NOFOLLOW
	flags |= O_NOFOLLOW;
#endif
	if (cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment, checked,
								  sizeof(checked))
			!= 0
		|| strcmp(path, checked) != 0)
		return false;
	fd = BasicOpenFile(path, flags);
	if (fd < 0)
		return false;
	ok = undo_recovery_file_stat(fd, &observed);
	if (ok && expected->exists)
		ok = undo_recovery_file_same(expected, &observed)
			 && undo_recovery_read(fd, observed.size, 0, page.data)
			 && memcmp(page.data, block0, BLCKSZ) == 0;
	if (ok)
		ok = cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, instance, segment,
									   checked, sizeof(checked))
				 == 0
			 && strcmp(path, checked) == 0;
	if (ok)
		ok = ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES) == 0 && pg_fsync(fd) == 0;
	if (close(fd) != 0)
		ok = false;
	/* Sync both the file name and the possibly newly created instance dir.
	 * Do this on retries too: a previous directory sync may have failed. */
	return ok && provision_fsync_parent(path) && provision_fsync_parent(parent);
}


#ifdef USE_PGRAC_CLUSTER
/* A final-file observation borrows one AVAILABLE identity through the original
 * read and directory sync. It neither creates a file nor supplies path/recovery
 * authority. Keep the caller's output private until all owned cleanup succeeds. */
static ClusterUndoSmgrFinalState
undo_native_tt_probe(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
					 char block0[BLCKSZ])
{
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	PGAlignedBlock page;
	char path[MAXPGPATH];
	volatile int fd = -1;
	volatile int save_errno = 0;
	volatile ClusterUndoSmgrFinalState result = CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;

	if (block0 == NULL || owner_instance == 0 || owner_instance > UNDO_OWNER_INSTANCE_MAX
		|| segment_id == 0 || segment_id > UINT16_MAX
		|| (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != owner_instance)
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	key.origin = owner_instance - 1;
	key.object_id = segment_id;
	if (!cluster_control_native_file_acquire_v2(&key, &borrow))
		return CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
	PG_TRY();
	{
		do {
			struct stat st;
			ssize_t nread;
			int owned_fd;

			if (!cluster_control_native_file_current_v2(&borrow)
				|| cluster_undo_path_resolve(intent, owner_instance, segment_id, path, sizeof(path))
					   != 0)
				break;
			fd = BasicOpenFile(path, O_RDONLY | PG_BINARY | O_NOFOLLOW);
			if (fd < 0) {
				save_errno = errno;
				if (cluster_control_native_file_current_v2(&borrow) && save_errno == ENOENT)
					result = CLUSTER_UNDO_SMGR_FINAL_ABSENT;
				break;
			}
			if (!cluster_control_native_file_current_v2(&borrow))
				break;
			if (fstat(fd, &st) != 0) {
				save_errno = errno;
				break;
			}
			if (st.st_size != UNDO_SEGMENT_SIZE_BYTES) {
				result = CLUSTER_UNDO_SMGR_FINAL_INVALID;
				break;
			}
			nread = pg_pread(fd, page.data, BLCKSZ, 0);
			cluster_undo_record_note_smgr_pread();
			if (nread < 0) {
				save_errno = errno;
				break;
			}
			if (nread != BLCKSZ
				|| !cluster_undo_segment_header_identity_ok(page.data, segment_id, owner_instance)
				|| !cluster_tt_file_reference_matches_v2(page.data, BLCKSZ, &borrow.expected)
				|| cluster_undo_segment_header_integrity(page.data) != CLUSTER_UNDO_HEADER_VALID) {
				result = CLUSTER_UNDO_SMGR_FINAL_INVALID;
				break;
			}
			if (!cluster_control_native_file_current_v2(&borrow))
				break;
			/* Preserve the original close-before-directory-sync order. Do not
			 * retry close on a descriptor the kernel may already have retired. */
			owned_fd = fd;
			fd = -1;
			if (close(owned_fd) != 0) {
				save_errno = errno;
				break;
			}
			if (!cluster_control_native_file_current_v2(&borrow)
				|| !provision_fsync_parent(path)
				|| !cluster_control_native_file_current_v2(&borrow))
				break;
			result = CLUSTER_UNDO_SMGR_FINAL_EXACT;
		} while (false);
	}
	PG_FINALLY();
	{
		if (fd >= 0 && close(fd) != 0) {
			if (save_errno == 0)
				save_errno = errno;
			result = CLUSTER_UNDO_SMGR_FINAL_IO_ERROR;
		}
		if (!cluster_control_native_file_release_v2(&borrow))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not release native TT probe identity borrow")));
	}
	PG_END_TRY();
	if (save_errno != 0)
		errno = save_errno;
	if (result == CLUSTER_UNDO_SMGR_FINAL_EXACT)
		memcpy(block0, page.data, BLCKSZ);
	return result;
}

/* This protects only this physical read/sync. The original caller still owns its
 * path/current/recovery authority and must bind any longer resident lifetime.
 * An idle cached FD has no table borrow: each operation reselects and checks
 * the complete on-disk identity, including on an FD-cache hit. */
static bool
undo_native_tt_read(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
					char *buf, bool sync_header, bool record_header)
{
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	PGAlignedBlock page;
	volatile bool ok = false;

	if (owner_instance == 0 || owner_instance > UNDO_OWNER_INSTANCE_MAX || segment_id == 0
		|| segment_id > UINT16_MAX
		|| (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != owner_instance)
		return false;
	key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	key.origin = owner_instance - 1;
	key.object_id = segment_id;
	if (!cluster_control_native_file_acquire_v2(&key, &borrow))
		return false;
	PG_TRY();
	{
		int fd;

		if (cluster_control_native_file_current_v2(&borrow)
			&& (fd = get_segment_fd(intent, segment_id, owner_instance)) >= 0
			&& cluster_control_native_file_current_v2(&borrow)) {
			ssize_t count = pg_pread(fd, page.data, BLCKSZ, 0);

			cluster_undo_record_note_smgr_pread();
			ok = count == BLCKSZ
				 && cluster_tt_file_reference_matches_v2(page.data, BLCKSZ, &borrow.expected)
				 && cluster_undo_segment_header_integrity(page.data) == CLUSTER_UNDO_HEADER_VALID
				 && cluster_control_native_file_current_v2(&borrow);
			/* Preserve the original checkpoint recorder or explicit same-FD
			 * sync, with the identity still borrowed through completion. */
			if (ok && sync_header) {
				ok = record_header ? header_write_recorded(fd, intent, segment_id, owner_instance)
								   : pg_fsync(fd) == 0;
				if (ok)
					ok = cluster_control_native_file_current_v2(&borrow);
			}
		}
	}
	PG_FINALLY();
	{
		/* A failed release retains the original duty; never return success or
		 * erase the address-bound handle in that case. */
		if (!cluster_control_native_file_release_v2(&borrow))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not release native TT file identity borrow")));
	}
	PG_END_TRY();
	if (ok && buf != NULL)
		memcpy(buf, page.data, BLCKSZ);
	return ok;
}

/* The original caller retains content-X, WAL and file-lifetime authority.
 * Bind this complete write and optional fsync to one selected identity. The
 * existing disk must already be that TT40 file: this never creates/repairs it.
 * Failure after pwrite preserves its effects for the original owner to handle. */
static bool
undo_native_tt_write(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
					 const char *buf, bool do_fsync, bool record_header)
{
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	PGAlignedBlock disk;
	volatile bool ok = false;

	if (owner_instance == 0 || owner_instance > UNDO_OWNER_INSTANCE_MAX || segment_id == 0
		|| segment_id > UINT16_MAX
		|| (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != owner_instance)
		return false;
	key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	key.origin = owner_instance - 1;
	key.object_id = segment_id;
	if (!cluster_control_native_file_acquire_v2(&key, &borrow))
		return false;
	PG_TRY();
	{
		int fd;

		if (cluster_control_native_file_current_v2(&borrow)
			&& cluster_tt_file_reference_matches_v2(buf, BLCKSZ, &borrow.expected)
			&& cluster_undo_segment_header_integrity(buf) == CLUSTER_UNDO_HEADER_VALID
			&& (fd = get_segment_fd(intent, segment_id, owner_instance)) >= 0
			&& cluster_control_native_file_current_v2(&borrow)) {
			ssize_t count = pg_pread(fd, disk.data, BLCKSZ, 0);

			cluster_undo_record_note_smgr_pread();
			if (count == BLCKSZ
				&& cluster_tt_file_reference_matches_v2(disk.data, BLCKSZ, &borrow.expected)
				&& cluster_undo_segment_header_integrity(disk.data) == CLUSTER_UNDO_HEADER_VALID
				&& cluster_control_native_file_current_v2(&borrow)) {
				count = pg_pwrite(fd, buf, BLCKSZ, 0);
				cluster_undo_record_note_smgr_pwrite();
				ok = count == BLCKSZ;
				if (ok && record_header)
					ok = header_write_recorded(fd, intent, segment_id, owner_instance);
				else if (ok && do_fsync)
					ok = pg_fsync(fd) == 0;
				if (ok)
					ok = cluster_control_native_file_current_v2(&borrow);
			}
		}
	}
	PG_FINALLY();
	{
		if (!cluster_control_native_file_release_v2(&borrow))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not release native TT write identity borrow")));
	}
	PG_END_TRY();
	return ok;
}

/* DATA belongs to the same selected UNDO file as TT40 block zero. Validate
 * that complete header on the very FD used for the DATA operation, including
 * cached FDs. The original caller still supplies content/WAL/path authority;
 * this borrow only prevents a file-generation change across physical I/O. */
static bool
undo_native_data_io(ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance,
					uint32 block_no, char *read_out, const char *write_in, bool do_fsync)
{
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	PGAlignedBlock header, data;
	volatile bool ok = false;

	if (owner_instance == 0 || owner_instance > UNDO_OWNER_INSTANCE_MAX || segment_id == 0
		|| segment_id > UINT16_MAX
		|| (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != owner_instance
		|| block_no == 0 || block_no >= UNDO_BLOCKS_PER_SEGMENT
		|| (read_out == NULL) == (write_in == NULL))
		return false;
	key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
	key.origin = owner_instance - 1;
	key.object_id = segment_id;
	if (!cluster_control_native_file_acquire_v2(&key, &borrow))
		return false;
	PG_TRY();
	{
		int fd;

		if (cluster_control_native_file_current_v2(&borrow)
			&& (fd = get_segment_fd(intent, segment_id, owner_instance)) >= 0
			&& cluster_control_native_file_current_v2(&borrow)) {
			ssize_t count = pg_pread(fd, header.data, BLCKSZ, 0);

			cluster_undo_record_note_smgr_pread();
			if (count == BLCKSZ
				&& cluster_undo_segment_header_identity_ok(header.data, segment_id, owner_instance)
				&& cluster_tt_file_reference_matches_v2(header.data, BLCKSZ, &borrow.expected)
				&& cluster_undo_segment_header_integrity(header.data) == CLUSTER_UNDO_HEADER_VALID
				&& cluster_control_native_file_current_v2(&borrow)) {
				off_t offset = (off_t)block_no * BLCKSZ;

				if (write_in != NULL) {
					count = pg_pwrite(fd, write_in, BLCKSZ, offset);
					cluster_undo_record_note_smgr_pwrite();
				} else {
					count = pg_pread(fd, data.data, BLCKSZ, offset);
					cluster_undo_record_note_smgr_pread();
				}
				ok = count == BLCKSZ;
				if (ok && write_in != NULL && do_fsync)
					ok = pg_fsync(fd) == 0;
				if (ok)
					ok = cluster_control_native_file_current_v2(&borrow);
			}
		}
	}
	PG_FINALLY();
	{
		/* The FD belongs to the original cache. Retire only this operation's
		 * borrow, preserving any completed write for caller error handling. */
		if (!cluster_control_native_file_release_v2(&borrow))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not release native UNDO DATA identity borrow")));
	}
	PG_END_TRY();
	if (ok && read_out != NULL)
		memcpy(read_out, data.data, BLCKSZ);
	return ok;
}
#endif

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

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2) {
		if (block_no == 0)
			return undo_native_tt_read(intent, segment_id, owner_instance, buf, false, false);
		return undo_native_data_io(intent, segment_id, owner_instance, block_no, buf, NULL, false);
	}
#endif

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
	uint32 acquired = UINT32_MAX;
	PGAlignedBlock header;

	if (buf == NULL || block_no >= UNDO_BLOCKS_PER_SEGMENT)
		return false;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2) {
		if (block_no == 0)
			return undo_native_tt_write(intent, segment_id, owner_instance, buf, do_fsync, false);
		return undo_native_data_io(intent, segment_id, owner_instance, block_no, NULL, buf, do_fsync);
	}
#endif

	fd = get_segment_fd(intent, segment_id, owner_instance);
	if (fd < 0)
		return false;
	if (block_no == 0) {
		memcpy(header.data, buf, BLCKSZ);
		if (!cluster_undo_segment_header_seal(header.data)
			|| !cluster_undo_block0_header_write_lock(segment_id, owner_instance, &acquired))
			return false;
		buf = header.data;
	}
	offset = (off_t)block_no * BLCKSZ;
	nwritten = pg_pwrite(fd, buf, BLCKSZ, offset);
	cluster_undo_record_note_smgr_pwrite();
	if (nwritten != BLCKSZ)
		ok = false;
	if (ok && do_fsync)
		ok = pg_fsync(fd) == 0;
	if (ok && block_no == 0 && !do_fsync)
		ok = header_write_recorded(fd, intent, segment_id, owner_instance);
	if (block_no == 0)
		cluster_undo_block0_header_write_unlock(acquired);
	return ok;
}


/*
 * cluster_undo_smgr_read_header_bytes / _write_header_bytes (spec-3.11 D2)
 *
 *   Reads preserve the caller's byte-range API. Writes serialize on the
 *   existing block-zero content owner and perform a whole-page RMW, checksum
 *   included. Holding the lock through checkpoint registration/fsync prevents
 *   different slot/prefix writers from losing each other's changes.
 *   A protected corrupt predecessor is never laundered by a partial update.
 *   Zero-checksum legacy input is unverified; a successful write seals the
 *   resulting whole page, without granting recovery qualification.
 */
bool
cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent, uint32 segment_id,
									uint8 owner_instance, uint32 offset, char *buf, uint32 len)
{
	int fd;
	ssize_t nread;

	if (buf == NULL || len == 0 || (uint64)offset + (uint64)len > (uint64)BLCKSZ)
		return false;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2) {
		PGAlignedBlock page;

		if (!undo_native_tt_read(intent, segment_id, owner_instance, page.data, false, false))
			return false;
		memcpy(buf, page.data + offset, len);
		return true;
	}
#endif

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

	if (buf == NULL || len == 0 || (uint64)offset + len > BLCKSZ)
		return false;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2) {
		/* A TT40 checksum covers the complete page. The original content-X
		 * owner supplies its successor; never make a raw slot RMW here. */
		if (offset != 0 || len != BLCKSZ)
			return false;
		return undo_native_tt_write(intent, segment_id, owner_instance, buf, false, true);
	}
#endif

	fd = get_segment_fd(intent, segment_id, owner_instance);
	return fd >= 0 && cluster_undo_smgr_header_writer_write(fd, intent, segment_id,
		owner_instance, offset, buf, len);
}

/* Leave a written header range to the checkpoint's fsync, or fsync it now
 * through the descriptor this process resolved (for a recovery-scoped path,
 * inside the recoverer's scope). */
static bool
header_write_recorded(int fd, ClusterUndoPathIntent intent, uint32 segment_id, uint8 owner_instance)
{
	if (cluster_undo_block0_note_unsynced_header(segment_id, owner_instance, intent))
		return true;
	return pg_fsync(fd) == 0;
}

/* The caller stores only a process-local handle. Never reuse its value: an
 * ERROR cleanup may precede the original stage's later close, while the OS
 * has already reused the descriptor. The original ResourceOwner owns both
 * this private FD and its stable-address identity borrow, not the FD cache. */
typedef struct UndoHeaderWriterResource {
	struct UndoHeaderWriterResource *next;
	ResourceOwner owner;
	int handle;
	int fd;
	ClusterUndoPathIntent intent;
	uint32 segment_id;
	uint8 owner_instance;
#ifdef USE_PGRAC_CLUSTER
	bool native;
	ClusterControlNativeFileBorrowV2 native_file;
#endif
} UndoHeaderWriterResource;

static UndoHeaderWriterResource *header_writers;
static int header_writer_serial;
static bool header_writer_callback_registered;
static bool header_writer_exit_registered;

static UndoHeaderWriterResource *
header_writer_find(int handle)
{
	UndoHeaderWriterResource *resource;

	for (resource = header_writers; resource != NULL; resource = resource->next)
		if (resource->handle == handle)
			return resource;
	return NULL;
}

static void
header_writer_resource_release(ResourceReleasePhase phase, bool isCommit, bool isTopLevel,
							   void *arg)
{
	UndoHeaderWriterResource *resource = header_writers;

	(void)isCommit;
	(void)isTopLevel;
	(void)arg;
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;
	while (resource != NULL) {
		UndoHeaderWriterResource *next = resource->next;

		if (resource->owner == CurrentResourceOwner)
			cluster_undo_smgr_header_writer_close(resource->handle);
		resource = next;
	}
}

static void
header_writer_on_exit(int code, Datum arg)
{
	(void)code;
	(void)arg;
	while (header_writers != NULL)
		cluster_undo_smgr_header_writer_close(header_writers->handle);
}

int
cluster_undo_smgr_header_writer_open(ClusterUndoPathIntent intent, uint32 segment_id,
									 uint8 owner_instance)
{
	UndoHeaderWriterResource *resource;
	volatile bool ok = false;
	int handle;

	if (CurrentResourceOwner == NULL || TopMemoryContext == NULL || header_writer_serial == INT_MAX)
		return -1;
#ifdef USE_PGRAC_CLUSTER
	if (owner_instance == 0 || owner_instance > UNDO_OWNER_INSTANCE_MAX || segment_id == 0
		|| segment_id > UINT16_MAX
		|| (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 != owner_instance)
		return -1;
#endif
	if (!header_writer_callback_registered) {
		RegisterResourceReleaseCallback(header_writer_resource_release, NULL);
		header_writer_callback_registered = true;
	}
	if (!header_writer_exit_registered) {
		before_shmem_exit(header_writer_on_exit, (Datum)0);
		header_writer_exit_registered = true;
	}
	resource = MemoryContextAlloc(TopMemoryContext, sizeof(*resource));
	memset(resource, 0, sizeof(*resource));
	resource->owner = CurrentResourceOwner;
	resource->handle = handle = ++header_writer_serial;
	resource->fd = -1;
	resource->intent = intent;
	resource->segment_id = segment_id;
	resource->owner_instance = owner_instance;
	resource->next = header_writers;
	header_writers = resource;
	PG_TRY();
	{
		int fd;
		bool eligible = true;
		bool ready = false;

#ifdef USE_PGRAC_CLUSTER
		resource->native = cluster_enabled
			|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		if (resource->native) {
			ClusterControlFileKeyV2 key = {0};

			key.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
			key.origin = owner_instance - 1;
			key.object_id = segment_id;
			eligible = cluster_control_native_file_acquire_v2(&key, &resource->native_file)
				&& cluster_control_native_file_current_v2(&resource->native_file);
		}
#endif
		if (eligible && (fd = get_segment_fd(intent, segment_id, owner_instance)) >= 0) {
			resource->fd = dup(fd);
			ready = resource->fd >= 0;
#ifdef USE_PGRAC_CLUSTER
			if (ready && resource->native) {
				PGAlignedBlock disk;

				ready = cluster_control_native_file_current_v2(&resource->native_file);
				if (ready) {
					ssize_t count = pg_pread(resource->fd, disk.data, BLCKSZ, 0);

					cluster_undo_record_note_smgr_pread();
					ready = count == BLCKSZ
						&& cluster_tt_file_reference_matches_v2(disk.data, BLCKSZ, &resource->native_file.expected)
						&& cluster_undo_segment_header_integrity(disk.data) == CLUSTER_UNDO_HEADER_VALID
						&& cluster_control_native_file_current_v2(&resource->native_file);
				}
			}
#endif
		}
		/* Do not publish success before every possible ERROR above completed. */
		ok = ready;
	}
	PG_FINALLY();
	{
		if (!ok)
			cluster_undo_smgr_header_writer_close(handle);
	}
	PG_END_TRY();
	return ok ? handle : -1;
}

bool
cluster_undo_smgr_header_writer_write(int handle, ClusterUndoPathIntent intent, uint32 segment_id,
									  uint8 owner_instance, uint32 offset, const char *buf,
									  uint32 len)
{
	UndoHeaderWriterResource *resource = header_writer_find(handle);
	ssize_t nwritten;
	bool ok;

	if (resource == NULL || resource->fd < 0 || resource->owner != CurrentResourceOwner
		|| resource->intent != intent || resource->segment_id != segment_id
		|| resource->owner_instance != owner_instance || buf == NULL || len == 0
		|| (uint64)offset + (uint64)len > (uint64)BLCKSZ)
		return false;
#ifdef USE_PGRAC_CLUSTER
	if (resource->native) {
		PGAlignedBlock disk;
		ssize_t count;

		if (offset != 0 || len != BLCKSZ
			|| !cluster_control_native_file_current_v2(&resource->native_file)
			|| !cluster_tt_file_reference_matches_v2(buf, BLCKSZ, &resource->native_file.expected)
			|| cluster_undo_segment_header_integrity(buf) != CLUSTER_UNDO_HEADER_VALID)
			return false;
		count = pg_pread(resource->fd, disk.data, BLCKSZ, 0);
		cluster_undo_record_note_smgr_pread();
		if (count != BLCKSZ
			|| !cluster_tt_file_reference_matches_v2(disk.data, BLCKSZ, &resource->native_file.expected)
			|| cluster_undo_segment_header_integrity(disk.data) != CLUSTER_UNDO_HEADER_VALID
			|| !cluster_control_native_file_current_v2(&resource->native_file))
			return false;
	} else if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return false;
#endif
	nwritten = pg_pwrite(resource->fd, buf, len, (off_t)offset);
	cluster_undo_record_note_smgr_pwrite();
	if (nwritten != (ssize_t)len)
		return false;
	ok = header_write_recorded(resource->fd, intent, segment_id, owner_instance);
#ifdef USE_PGRAC_CLUSTER
	if (ok && resource->native)
		ok = cluster_control_native_file_current_v2(&resource->native_file);
#endif
	return ok;
}

void
cluster_undo_smgr_header_writer_close(int handle)
{
	UndoHeaderWriterResource **link = &header_writers;
	UndoHeaderWriterResource *resource;

	while (*link != NULL && (*link)->handle != handle)
		link = &(*link)->next;
	resource = *link;
	if (resource == NULL)
		return;
	if (resource->fd >= 0) {
		if (close(resource->fd) != 0)
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not close staged undo header writer")));
		resource->fd = -1;
	}
#ifdef USE_PGRAC_CLUSTER
	if (resource->native_file.private_self != 0
		&& !cluster_control_native_file_release_v2(&resource->native_file))
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("could not release staged undo header writer identity")));
#endif
	*link = resource->next;
	pfree(resource);
}

/* Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
typedef enum RecoveryHeaderStampKind {
	RECOVERY_HEADER_ABORT_C0 = 1,
	RECOVERY_HEADER_CTRC_D0
} RecoveryHeaderStampKind;

typedef struct RecoveryHeaderStamp {
	int fd;
	ResourceOwner owner;
	uint64 token;
	RecoveryHeaderStampKind kind;
	bool failed;
	bool durable;
	ClusterRecoveryTTAbortV1 record;
	ClusterWalInputsV1 *tx_inputs; /* only original post-EOR C0 */
	struct stat file;
	uint32 slot_offset;
#ifdef USE_PGRAC_CLUSTER
	uint8 ctrc_payload[CLUSTER_RECOVERY_CTRC_BYTES];
#endif
	PGAlignedBlock before;
	PGAlignedBlock after;
} RecoveryHeaderStamp;

/* One serial recovery stamp, with no stack callback or re-openable VFD. */
static RecoveryHeaderStamp recovery_header_stamp = { .fd = -1 };
static uint64 recovery_header_stamp_serial;
static bool recovery_header_stamp_registered;
static bool recovery_header_stamp_exit_registered;

static bool
recovery_header_stamp_release(void)
{
	int fd = recovery_header_stamp.fd;
	bool ok;

	if (fd < 0)
		return false;
	memset(&recovery_header_stamp, 0, sizeof(recovery_header_stamp));
	recovery_header_stamp.fd = -1;
	ok = close(fd) == 0;
	cluster_undo_record_note_smgr_close();
	ReleaseExternalFD();
	return ok;
}

static void
recovery_header_stamp_resource_release(ResourceReleasePhase phase, bool isCommit, bool isTopLevel,
									   void *arg)
{
	if (phase == RESOURCE_RELEASE_AFTER_LOCKS && recovery_header_stamp.fd >= 0
		&& recovery_header_stamp.owner == CurrentResourceOwner)
		(void)recovery_header_stamp_release();
}

static void
recovery_header_stamp_exit(int code, Datum arg)
{
	(void)recovery_header_stamp_release();
}

static bool
recovery_header_stamp_file_matches(const struct stat *observed)
{
	const struct stat *expected = &recovery_header_stamp.file;

	return S_ISREG(observed->st_mode) && observed->st_nlink > 0
		   && observed->st_dev == expected->st_dev && observed->st_ino == expected->st_ino
		   && observed->st_size == expected->st_size;
}

static bool
recovery_header_stamp_open_path(const char *path)
{
	RecoveryHeaderStamp *stage = &recovery_header_stamp;
	int fd;

	if (path == NULL)
		return false;
	/* Registration and descriptor reservation precede opening the target. */
	if (!recovery_header_stamp_registered) {
		RegisterResourceReleaseCallback(recovery_header_stamp_resource_release, NULL);
		recovery_header_stamp_registered = true;
	}
	if (!recovery_header_stamp_exit_registered) {
		before_shmem_exit(recovery_header_stamp_exit, (Datum)0);
		recovery_header_stamp_exit_registered = true;
	}
	if (!AcquireExternalFD())
		return false;
	fd = BasicOpenFile(path, O_RDWR | O_NOFOLLOW | PG_BINARY);
	if (fd < 0) {
		ReleaseExternalFD();
		return false;
	}
	cluster_undo_record_note_smgr_open();
	stage->fd = fd;
	stage->owner = CurrentResourceOwner;
	if (fstat(fd, &stage->file) != 0 || !S_ISREG(stage->file.st_mode) || stage->file.st_nlink == 0
		|| stage->file.st_size != UNDO_SEGMENT_SIZE_BYTES)
		goto refused;
	if (pg_pread(fd, stage->before.data, BLCKSZ, 0) != BLCKSZ) {
		cluster_undo_record_note_smgr_pread();
		goto refused;
	}
	cluster_undo_record_note_smgr_pread();
	return true;

refused:
	(void)recovery_header_stamp_release();
	return false;
}

/* The original C0/D0 recovery path remains distinct from the already
 * authenticated post-EOR TX_CLOSURE path. Never fall back between them. */
static bool
recovery_header_stamp_open(uint32 segment_id, uint8 owner_instance)
{
	char path[MAXPGPATH];
	if (cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, owner_instance,
			segment_id, path, sizeof(path)) != 0)
		return false;
	return recovery_header_stamp_open_path(path);
}

static bool
recovery_abort_prepare_path(const ClusterRecoveryTTAbortV1 *record, const char *path,
	ClusterUndoSmgrRecoveryStampV1 *out)
{
	RecoveryHeaderStamp *stage = &recovery_header_stamp;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)stage->before.data;
	struct stat after;

	if (out == NULL)
		return false;
	out->token = 0;
	if (CurrentResourceOwner == NULL || stage->fd >= 0 || recovery_header_stamp_serial == UINT64_MAX
		|| !cluster_recovery_tt_abort_valid_v1(record)
		|| !(path != NULL ? recovery_header_stamp_open_path(path)
			 : recovery_header_stamp_open(record->exact.segment_id, record->exact.instance)))
		return false;
	if (cluster_undo_segment_header_integrity(stage->before.data) != CLUSTER_UNDO_HEADER_VALID
		|| !UndoSegmentHeader_identity_matches(stage->before.data, record->exact.segment_id,
											record->exact.instance)
		|| cluster_recovery_tt_abort_match_v1(record, header->wrap_count,
											  &header->tt_slots[record->exact.slot_offset])
			   == CLUSTER_RECOVERY_TT_ABORT_REFUSED
		|| fstat(stage->fd, &after) != 0 || !recovery_header_stamp_file_matches(&after))
		goto refused;
	stage->slot_offset = offsetof(UndoSegmentHeaderData, tt_slots)
						 + (uint32)record->exact.slot_offset * sizeof(TTSlot);
	memcpy(stage->after.data, stage->before.data, BLCKSZ);
	((UndoSegmentHeaderData *)stage->after.data)->tt_slots[record->exact.slot_offset].status
		= TT_SLOT_ABORTED;
	if (!cluster_undo_segment_header_seal(stage->after.data))
		goto refused;
	stage->record = *record;
	stage->kind = RECOVERY_HEADER_ABORT_C0;
	stage->token = ++recovery_header_stamp_serial;
	out->token = stage->token;
	return true;

refused:
	(void)recovery_header_stamp_release();
	return false;
}

bool
cluster_undo_smgr_recovery_abort_prepare_v1(const ClusterRecoveryTTAbortV1 *record,
	ClusterUndoSmgrRecoveryStampV1 *out)
{
	return recovery_abort_prepare_path(record, NULL, out);
}

bool
cluster_undo_smgr_recovery_abort_prepare_tx_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *record, ClusterUndoSmgrRecoveryStampV1 *out)
{
	char path[MAXPGPATH];

	if (out == NULL)
		return false;
	out->token = 0;
	if (CurrentResourceOwner == NULL || recovery_header_stamp.fd >= 0
		|| recovery_header_stamp_serial == UINT64_MAX || !cluster_recovery_tt_abort_valid_v1(record)
		|| !cluster_wal_inputs_tx_abort_authorized_v1(inputs, record)
		|| !cluster_wal_inputs_tx_abort_path_v1(inputs, record, path, sizeof(path)))
		return false;
	if (!recovery_abort_prepare_path(record, path, out))
		return false;
	recovery_header_stamp.tx_inputs = inputs;
	if (!cluster_wal_inputs_tx_abort_authorized_v1(inputs, record)) {
		(void)recovery_header_stamp_release();
		out->token = 0;
		return false;
	}
	return true;
}

static bool
recovery_header_stamp_write(ClusterUndoSmgrRecoveryStampV1 stamp, RecoveryHeaderStampKind kind)
{
	RecoveryHeaderStamp *stage = &recovery_header_stamp;
	PGAlignedBlock observed;
	struct stat file;
	ssize_t size;
	uint32 acquired_slot = UINT32_MAX;
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)stage->before.data;

	if (stamp.token == 0 || stage->fd < 0 || stage->token != stamp.token
		|| stage->owner != CurrentResourceOwner || stage->kind != kind || stage->failed)
		return false;
	stage->durable = false;
	if (stage->tx_inputs != NULL
		&& !cluster_wal_inputs_tx_abort_authorized_v1(stage->tx_inputs, &stage->record))
		goto refused;
	if (!cluster_undo_block0_header_write_lock(header->segment_id, header->owner_instance,
												 &acquired_slot))
		goto refused;
	/* No allocation, path/scope resolution or checkpoint delegation here. */
	if (fstat(stage->fd, &file) != 0 || !recovery_header_stamp_file_matches(&file))
		goto refused;
	size = pg_pread(stage->fd, observed.data, BLCKSZ, 0);
	cluster_undo_record_note_smgr_pread();
	if (size != BLCKSZ || memcmp(observed.data, stage->before.data, BLCKSZ) != 0)
		goto refused;
	/* The local header lock and file reads may have waited. Recheck the
	 * original no-I/O guard at the mutation boundary after those waits. */
	if (stage->tx_inputs != NULL
		&& !cluster_wal_inputs_tx_abort_authorized_v1(stage->tx_inputs, &stage->record))
		goto refused;
	if (memcmp(stage->before.data, stage->after.data, BLCKSZ) != 0) {
		size = pg_pwrite(stage->fd, stage->after.data, BLCKSZ, 0);
		cluster_undo_record_note_smgr_pwrite();
		if (size != BLCKSZ)
			goto refused;
	}
	/* An already-result observation must also become durable. */
	if (pg_fsync(stage->fd) != 0) {
		int save_errno = errno;

		stage->failed = true;
		cluster_undo_block0_header_write_unlock(acquired_slot);
		errno = save_errno;
		if (kind == RECOVERY_HEADER_CTRC_D0)
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(), errmsg("could not fsync recovery CTRC target: %m")));
		else
			ereport(data_sync_elevel(ERROR),
				(errcode_for_file_access(),
				 errmsg("could not synchronize cold recovery C0 undo segment %u: %m",
						header->segment_id)));
		return false;
	}
	size = pg_pread(stage->fd, observed.data, BLCKSZ, 0);
	cluster_undo_record_note_smgr_pread();
	if (size != BLCKSZ || memcmp(observed.data, stage->after.data, BLCKSZ) != 0
		|| fstat(stage->fd, &file) != 0 || !recovery_header_stamp_file_matches(&file))
		goto refused;
	if (stage->tx_inputs != NULL
		&& !cluster_wal_inputs_tx_abort_authorized_v1(stage->tx_inputs, &stage->record))
		goto refused;
	memcpy(stage->before.data, stage->after.data, BLCKSZ);
	stage->durable = true;
	cluster_undo_block0_header_write_unlock(acquired_slot);
	return true;

refused:
	cluster_undo_block0_header_write_unlock(acquired_slot);
	stage->failed = true;
	return false;
}

bool
cluster_undo_smgr_recovery_abort_write_v1(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	return recovery_header_stamp_write(stamp, RECOVERY_HEADER_ABORT_C0);
}

bool
cluster_undo_smgr_recovery_abort_durable_v1(ClusterUndoSmgrRecoveryStampV1 stamp,
	const ClusterRecoveryTTAbortV1 *record)
{
	const RecoveryHeaderStamp *stage = &recovery_header_stamp;

	return stamp.token != 0 && stage->fd >= 0 && stage->token == stamp.token
		&& CurrentResourceOwner != NULL && stage->owner == CurrentResourceOwner
		&& stage->kind == RECOVERY_HEADER_ABORT_C0 && !stage->failed && stage->durable && record != NULL
		&& memcmp(record, &stage->record, sizeof(*record)) == 0;
}

static bool
recovery_header_stamp_close(ClusterUndoSmgrRecoveryStampV1 stamp, RecoveryHeaderStampKind kind)
{
	if (stamp.token == 0 || recovery_header_stamp.fd < 0
		|| recovery_header_stamp.token != stamp.token
		|| recovery_header_stamp.owner != CurrentResourceOwner
		|| recovery_header_stamp.kind != kind)
		return false;
	return recovery_header_stamp_release();
}

bool
cluster_undo_smgr_recovery_abort_close_v1(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	return recovery_header_stamp_close(stamp, RECOVERY_HEADER_ABORT_C0);
}

#ifdef USE_PGRAC_CLUSTER
bool
cluster_undo_smgr_recovery_ctrc_prepare_v2(const uint8 *payload, size_t length,
										   uint64 system_identifier,
										   ClusterUndoSmgrRecoveryStampV1 *out)
{
	RecoveryHeaderStamp *stage = &recovery_header_stamp;
	ClusterRecoveryCTRCReleaseV2 record;
	struct stat after;

	if (out == NULL)
		return false;
	out->token = 0;
	/* Decode/hash and reserve the descriptor before entering any window. */
	if (CritSectionCount != 0 || CurrentResourceOwner == NULL || stage->fd >= 0
		|| recovery_header_stamp_serial == UINT64_MAX
		|| !cluster_recovery_ctrc_decode_v2(payload, length, system_identifier, &record)
		|| !recovery_header_stamp_open(record.segment_id, (uint8)record.target_owner))
		return false;
	if (!cluster_recovery_ctrc_target_image_v2(payload, length, system_identifier,
											   stage->before.data, BLCKSZ, stage->after.data,
											   BLCKSZ)
		|| fstat(stage->fd, &after) != 0 || !recovery_header_stamp_file_matches(&after))
		goto refused;
	memcpy(stage->ctrc_payload, payload, CLUSTER_RECOVERY_CTRC_BYTES);
	stage->kind = RECOVERY_HEADER_CTRC_D0;
	stage->token = ++recovery_header_stamp_serial;
	out->token = stage->token;
	return true;

refused:
	(void)recovery_header_stamp_release();
	return false;
}

bool
cluster_undo_smgr_recovery_ctrc_matches_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
	const uint8 *payload, size_t length)
{
	const RecoveryHeaderStamp *stage = &recovery_header_stamp;

	/* Only inspect the caller-owned prepared stage. This does not replace
	 * the post-insert exact-before/write/sync/readback checks or reserve a
	 * new target, and refusal must not poison a still-cancelable operation. */
	return stamp.token != 0 && stage->fd >= 0 && stage->token == stamp.token
		&& CurrentResourceOwner != NULL && stage->owner == CurrentResourceOwner
		&& stage->kind == RECOVERY_HEADER_CTRC_D0 && !stage->failed
		&& payload != NULL && length == CLUSTER_RECOVERY_CTRC_BYTES
		&& memcmp(stage->ctrc_payload, payload, CLUSTER_RECOVERY_CTRC_BYTES) == 0;
}

bool
cluster_undo_smgr_recovery_ctrc_write_v2(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	return recovery_header_stamp_write(stamp, RECOVERY_HEADER_CTRC_D0);
}

bool
cluster_undo_smgr_recovery_ctrc_write_bound_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
											   const uint8 *payload, size_t length)
{
	RecoveryHeaderStamp *stage = &recovery_header_stamp;

	/* A foreign token, owner or kind cannot poison another prepared duty. */
	if (stamp.token == 0 || stage->fd < 0 || stage->token != stamp.token
		|| stage->owner != CurrentResourceOwner || stage->kind != RECOVERY_HEADER_CTRC_D0
		|| stage->failed)
		return false;
	/* The same target image is insufficient: bind the whole canonical D0,
	 * including its graph/input identity, before any lock or target I/O. */
	if (payload == NULL || length != CLUSTER_RECOVERY_CTRC_BYTES
		|| memcmp(stage->ctrc_payload, payload, CLUSTER_RECOVERY_CTRC_BYTES) != 0) {
		stage->failed = true;
		return false;
	}
	return recovery_header_stamp_write(stamp, RECOVERY_HEADER_CTRC_D0);
}

bool
cluster_undo_smgr_recovery_ctrc_readback_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
	const uint8 *payload, size_t length)
{
	RecoveryHeaderStamp *stage = &recovery_header_stamp;
	PGAlignedBlock expected, observed;
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)expected.data;
	uint8 fixed_payload[CLUSTER_RECOVERY_CTRC_BYTES];
	volatile uint32 acquired_slot = UINT32_MAX;
	volatile bool valid = false;
	ResourceOwner owner;
	int fd;

	/* Foreign/preparation-only observations neither enter native I/O nor
	 * poison an otherwise valid, still caller-owned operation. */
	if (CritSectionCount != 0
		|| !cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, payload, length)
		|| !stage->durable)
		return false;
	fd = stage->fd;
	owner = stage->owner;
	memcpy(fixed_payload, payload, sizeof(fixed_payload));
	memcpy(expected.data, stage->after.data, BLCKSZ);
	PG_TRY();
	{
		uint32 acquired = UINT32_MAX;
		struct stat file;

		if (cluster_undo_block0_header_write_lock(header->segment_id,
				header->owner_instance, &acquired)) {
			acquired_slot = acquired;
			/* The lock may have waited. Recheck the complete stage before
			 * using its saved descriptor, never a newly resolved pathname. */
			valid = stage->fd == fd && stage->owner == owner && stage->durable
				&& cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, payload, length)
				&& memcmp(payload, fixed_payload, sizeof(fixed_payload)) == 0
				&& memcmp(stage->after.data, expected.data, BLCKSZ) == 0
				&& fstat(fd, &file) == 0 && recovery_header_stamp_file_matches(&file);
			if (valid) {
				ssize_t size = pg_pread(fd, observed.data, BLCKSZ, 0);

				cluster_undo_record_note_smgr_pread();
				valid = size == BLCKSZ && memcmp(observed.data, expected.data, BLCKSZ) == 0
					&& fstat(fd, &file) == 0 && recovery_header_stamp_file_matches(&file);
			}
		}
	}
	PG_CATCH();
	{
		/* Never invalidate a replacement stage or release a borrowed lock.
		 * The original raw FD and recovery protections remain outstanding. */
		if (stage->fd == fd && stage->owner == owner && stage->token == stamp.token
			&& stage->kind == RECOVERY_HEADER_CTRC_D0)
			stage->failed = true;
		/* ERROR cleared the lock's original holdoff. Restore exactly the
		 * one consumed by our unlock; a borrowed lock is the caller's duty. */
		if (acquired_slot != UINT32_MAX) {
			HOLD_INTERRUPTS();
			cluster_undo_block0_header_write_unlock(acquired_slot);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_undo_block0_header_write_unlock(acquired_slot);
	valid = valid && stage->fd == fd && stage->owner == owner && stage->durable
		&& cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, payload, length)
		&& memcmp(payload, fixed_payload, sizeof(fixed_payload)) == 0
		&& memcmp(stage->after.data, expected.data, BLCKSZ) == 0;
	if (!valid && stage->fd == fd && stage->owner == owner && stage->token == stamp.token
		&& stage->kind == RECOVERY_HEADER_CTRC_D0)
		stage->failed = true;
	return valid;
}

bool
cluster_undo_smgr_recovery_ctrc_close_v2(ClusterUndoSmgrRecoveryStampV1 stamp)
{
	return recovery_header_stamp_close(stamp, RECOVERY_HEADER_CTRC_D0);
}

static bool
recovery_header_stamp_overlap(const void *a, size_t a_length,
	const void *b, size_t b_length)
{
	uintptr_t ap = (uintptr_t)a;
	uintptr_t bp = (uintptr_t)b;

	return a_length != 0 && b_length != 0
		&& (ap >= bp ? ap - bp < b_length : bp - ap < a_length);
}

bool
cluster_undo_smgr_recovery_ctrc_prepare_record_v2(const XLogReaderState *reader,
	uint32 source_index, const uint8 source_identity_digest[32], const uint8 *raw,
	size_t raw_length, ClusterUndoSmgrRecoveryStampV1 *out,
	ClusterRecoveryCTRCReferenceV2 *reference)
{
	ClusterRecoveryCTRCReferenceV2 bound;
	ClusterUndoSmgrRecoveryStampV1 prepared;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
	const void *inputs[6];
	size_t lengths[6];
	size_t i;

	if (CritSectionCount != 0 || CurrentResourceOwner == NULL
		|| reader == NULL || reader->record == NULL
		|| source_identity_digest == NULL
		|| out == NULL || reference == NULL || recovery_header_stamp.fd >= 0
		|| recovery_header_stamp_overlap(out, sizeof(*out), reference, sizeof(*reference)))
		return false;

	/* NULL/0 selects the original current-reader snapshot, never a rebuilt
	 * header or the mutable read buffer. Other pointer/length pairs retain
	 * the explicit immutable-snapshot contract. Resolve before alias checks. */
	if (raw == NULL && raw_length == 0) {
		raw = XLogReaderGetClusterD0RawRecord(reader);
		raw_length = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong
			+ CLUSTER_RECOVERY_CTRC_BYTES;
	}
	if (raw == NULL || raw_length != SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong
			+ CLUSTER_RECOVERY_CTRC_BYTES)
		return false;

	/* Publishing either result must not overwrite the original record, its
	 * provenance, or the held-descriptor state. Do this before hashing or I/O. */
	inputs[0] = reader; lengths[0] = sizeof(*reader);
	inputs[1] = reader->record; lengths[1] = sizeof(*reader->record);
	inputs[2] = reader->record->main_data; lengths[2] = reader->record->main_data_len;
	inputs[3] = raw; lengths[3] = raw_length;
	inputs[4] = source_identity_digest; lengths[4] = 32;
	inputs[5] = &recovery_header_stamp; lengths[5] = sizeof(recovery_header_stamp);
	for (i = 0; i < lengthof(inputs); i++)
		if (recovery_header_stamp_overlap(out, sizeof(*out), inputs[i], lengths[i])
			|| recovery_header_stamp_overlap(reference, sizeof(*reference), inputs[i], lengths[i]))
			return false;

	/* The existing reference reader binds the full native header, payload and
	 * CRC to this completed record and its actual positions, before acquiring
	 * a target descriptor. ROOT/source/cut/graph qualification remains caller's. */
	if (!cluster_recovery_ctrc_reference_v2(reader, source_index,
			source_identity_digest, raw, raw_length, &bound))
		return false;
	memcpy(payload, raw + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong,
		CLUSTER_RECOVERY_CTRC_BYTES);
	if (!cluster_undo_smgr_recovery_ctrc_prepare_v2(payload, sizeof(payload),
			reader->system_identifier, &prepared))
		return false;

	/* Preparation owns the original ResourceOwner cleanup and ERROR behavior.
	 * No target write has occurred; publish both private results only now. */
	memcpy(out, &prepared, sizeof(prepared));
	memcpy(reference, &bound, sizeof(bound));
	return true;
}

bool
cluster_undo_smgr_recovery_ctrc_finish_record_v2(ClusterUndoSmgrRecoveryStampV1 stamp,
	const XLogReaderState *reader, uint32 source_index,
	const uint8 source_identity_digest[32], const uint8 *raw, size_t raw_length,
	const ClusterRecoveryCTRCReferenceV2 *prepared_reference)
{
	ClusterRecoveryCTRCReferenceV2 prepared, observed;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];

	if (CritSectionCount != 0 || CurrentResourceOwner == NULL || prepared_reference == NULL)
		return false;
	/* Preparation may have borrowed the native snapshot. Reacquire only the
	 * current record; the original R comparison below rejects any advance. */
	if (raw == NULL && raw_length == 0) {
		raw = XLogReaderGetClusterD0RawRecord(reader);
		raw_length = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong
			+ CLUSTER_RECOVERY_CTRC_BYTES;
	}
	/* The caller retains this structural reference from preparation. Rebuild
	 * it from the original completed reader, not a window or a guessed end.
	 * No target I/O or ownership change may precede the full-record binding. */
	memcpy(&prepared, prepared_reference, sizeof(prepared));
	if (!cluster_recovery_ctrc_reference_v2(reader, source_index,
			source_identity_digest, raw, raw_length, &observed)
		/* Compare logical members, never ABI-dependent structure padding. */
		|| observed.source_index != prepared.source_index || observed.start != prepared.start
		|| observed.end != prepared.end || observed.raw_length != prepared.raw_length
		|| memcmp(observed.raw_record_sha256, prepared.raw_record_sha256,
			sizeof(observed.raw_record_sha256)) != 0)
		return false;
	memcpy(payload, raw + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong,
		CLUSTER_RECOVERY_CTRC_BYTES);
	if (!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, payload, sizeof(payload)))
		return false;

	/* Leave the caller's exact stage/token available on target refusal or
	 * native ERROR. Only a durable readback permits the original close; false
	 * after close is still not completion. No window or pin is retired here. */
	if (!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)))
		return false;
	return cluster_undo_smgr_recovery_ctrc_close_v2(stamp);
}
#endif

bool
cluster_undo_smgr_header_unchanged_durable(ClusterUndoPathIntent intent, uint32 segment_id,
										   uint8 owner_instance)
{
	int fd;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return undo_native_tt_read(intent, segment_id, owner_instance, NULL, true, true);
#endif
	fd = get_segment_fd(intent, segment_id, owner_instance);

	return fd >= 0 && header_write_recorded(fd, intent, segment_id, owner_instance);
}

bool
cluster_undo_smgr_fsync_header(ClusterUndoPathIntent intent, uint32 segment_id,
							   uint8 owner_instance)
{
	int fd;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return undo_native_tt_read(intent, segment_id, owner_instance, NULL, true, false);
#endif
	fd = get_segment_fd(intent, segment_id, owner_instance);

	return fd >= 0 && pg_fsync(fd) == 0;
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

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return undo_native_tt_probe(intent, segment_id, owner_instance, block0);
#endif
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
		|| cluster_undo_segment_header_integrity(private_block.data) == CLUSTER_UNDO_HEADER_CORRUPT
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
	PGAlignedBlock observed, sealed;
	ClusterUndoSmgrFinalState final_state;
	ssize_t nwritten;
	bool temp_ok = true;
	int fd;
	int save_errno;

	if (block0 == NULL
		|| !cluster_undo_segment_header_identity_ok(block0, segment_id, owner_instance)
		|| !provision_temp_owned(intent, segment_id, owner_instance, temp_path, final_path))
		return CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR;
	memcpy(sealed.data, block0, BLCKSZ);
	if (!cluster_undo_segment_header_seal(sealed.data))
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
	nwritten = pg_pwrite(fd, sealed.data, BLCKSZ, 0);
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

#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled
		|| cluster_control_native_files_mode_v2() != CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return undo_native_tt_read(cluster_undo_recovery_intent_for_owner(owner_instance),
			segment_id, owner_instance, NULL, true, false);
#endif

	fd = get_segment_fd(cluster_undo_recovery_intent_for_owner(owner_instance), segment_id,
						owner_instance);
	if (fd < 0)
		return false;

	return (pg_fsync(fd) == 0);
}
