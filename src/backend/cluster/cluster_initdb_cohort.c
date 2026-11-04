/* Synchronous original-creation owner, outside normal database startup.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#endif

#include "access/xlog_internal.h"
#include "cluster/cluster_initdb_cohort.h"
#include "cluster/cluster_initdb_config.h"
#include "cluster/cluster_wal_thread.h"
#include "common/file_perm.h"
#include "common/cryptohash.h"
#include "common/pgrac_initdb_cohort.h"
#include "common/cluster_recovery_bootstrap.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_recovery_format.h"
#include "catalog/pg_tablespace_d.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "cluster_initdb_origin_private.h"
#include "cluster_initdb_common_private.h"
#include "cluster_initdb_catalog_private.h"
#include "cluster_initdb_tree_private.h"
#include "cluster_control_root_registry_private.h"
#include "cluster_cf_contract_private.h"
#include "../../bin/initdb/pgrac_wal.h"
#include "../../bin/initdb/pgrac_side.h"

typedef struct InitdbDirectory {
	int fd;
	int parent;
	char path[MAXPGPATH];
	char name[MAXPGPATH];
	struct stat identity;
	struct stat parent_identity;
} InitdbDirectory;

typedef struct InitdbOrigin {
	InitdbDirectory data;
	InitdbDirectory thread;
	InitdbDirectory wal;
	ControlFileData control;
	PgracInitdbWalObservation checkpoint;
	ClusterWalHistoryRecord input;
	uint8 control_sha256[32];
	struct stat control_identity;
	ClusterInitdbTree wal_observed;
	ClusterCfContractRecord storage_contract;
	struct stat storage_contract_identity, storage_contract_directory;
	bool side_routed;
	bool managed_relmap;
	bool managed_boot;
	PgracInitdbRecoveryContext relmap_registration;
	ClusterRecoveryCheckpointFileV2 relmap_observations[8];
	ClusterRecoveryBootFileV2 boot_observations[PGRAC_INITDB_BOOT_FILES_V2];
	struct stat boot_file_identity[PGRAC_INITDB_BOOT_FILES_V2];
	struct stat side_archive;
	struct stat side_sources[4], side_targets[4], side_links[4];
	int side_link_fds[4];
	ClusterInitdbTree side_source_trees[4], side_target_trees[4];
} InitdbOrigin;

static const char *const side_families[]
	= { "pg_xact", "pg_subtrans", "pg_multixact", "pg_commit_ts" };
#define INITDB_SIDE_ARCHIVE "pgrac_initdb_native_side"

static volatile sig_atomic_t creation_cancelled;

static void
creation_signal(SIGNAL_ARGS)
{
	int saved_errno = errno;
	creation_cancelled = 1;
	errno = saved_errno;
}

static void
install_creation_signals(void)
{
	sigset_t empty;
	pqsignal(SIGINT, creation_signal);
	pqsignal(SIGTERM, creation_signal);
	sigemptyset(&empty);
	if (sigprocmask(SIG_SETMASK, &empty, NULL) != 0)
		elog(FATAL, "INITDB_COHORT_CREATE: cannot restore creator signal mask");
}

static void
pg_attribute_noreturn() refuse(const char *reason)
{
	ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("INITDB_COHORT_CREATE: %s", reason)));
	pg_unreachable();
}

static pg_cryptohash_ctx *
creation_hash_begin(const char *domain, Size length)
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL || pg_cryptohash_init(ctx) < 0
		|| (length && pg_cryptohash_update(ctx, (const uint8 *)domain, length) < 0))
		refuse("cannot start original creation digest");
	return ctx;
}

static void
creation_hash_feed(pg_cryptohash_ctx *ctx, const void *bytes, Size length)
{
	if (pg_cryptohash_update(ctx, bytes, length) < 0)
		refuse("cannot hash original creation bytes");
}

static void
creation_hash_number(pg_cryptohash_ctx *ctx, uint64 value, unsigned width)
{
	uint8 bytes[8];
	for (unsigned i = 0; i < width; i++)
		bytes[i] = value >> (8 * i);
	creation_hash_feed(ctx, bytes, width);
}

static void
creation_hash_finish(pg_cryptohash_ctx *ctx, uint8 hash[32])
{
	if (pg_cryptohash_final(ctx, hash, 32) < 0)
		refuse("cannot finish original creation digest");
	pg_cryptohash_free(ctx);
}

static bool
owned_directory(const struct stat *st)
{
	return S_ISDIR(st->st_mode) && st->st_uid == geteuid() && (st->st_mode & 0022) == 0;
}

static bool
same_directory(const struct stat *a, const struct stat *b)
{
	return owned_directory(a) && owned_directory(b) && a->st_dev == b->st_dev
		   && a->st_ino == b->st_ino && a->st_mode == b->st_mode;
}

static bool
terminated(const char *value, size_t size)
{
	return value[0] != '\0' && memchr(value, '\0', size) != NULL;
}

static void
read_context(PgracInitdbCohortContext *out)
{
	const char *value = getenv(PGRAC_INITDB_COHORT_ENV);
	char *end, extra;
	long descriptor;
	struct stat st;
	TimestampTz started = GetCurrentTimestamp();
	size_t used = 0;
	bool eof = false;
	int flags;

	if (value == NULL || *value == '\0' || strspn(value, "0123456789") != strlen(value))
		refuse("original initdb pipe is required");
	errno = 0;
	descriptor = strtol(value, &end, 10);
	if (errno != 0 || *end != '\0' || descriptor < 3 || descriptor > INT_MAX
		|| fstat(descriptor, &st) != 0 || !S_ISFIFO(st.st_mode)
		|| (flags = fcntl(descriptor, F_GETFL)) < 0 || (flags & O_ACCMODE) != O_RDONLY
		|| fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0)
		refuse("invalid original initdb pipe");
	unsetenv(PGRAC_INITDB_COHORT_ENV);
	memset(out, 0, sizeof(*out));
	while (!eof) {
		struct pollfd event = { descriptor, POLLIN, 0 };
		ssize_t n;
		if (TimestampDifferenceExceeds(started, GetCurrentTimestamp(), 10000))
			refuse("original initdb request was not closed");
		if (poll(&event, 1, 100) < 0) {
			if (errno == EINTR)
				continue;
			refuse("cannot read original initdb request");
		}
		n = read(descriptor, used < sizeof(*out) ? (char *)out + used : &extra,
				 used < sizeof(*out) ? sizeof(*out) - used : 1);
		if (n < 0 && (errno == EINTR || errno == EAGAIN))
			continue;
		if (n < 0 || (n > 0 && used == sizeof(*out)))
			refuse("invalid original initdb request length");
		if (n == 0)
			eof = true;
		else
			used += n;
	}
	if (close(descriptor) != 0 || used != sizeof(*out) || out->magic != PGRAC_INITDB_COHORT_MAGIC
		|| out->creator_pid != (uint64)getppid() || !IsValidWalSegSize(out->segment_size)
		|| !terminated(out->cache_root, sizeof(out->cache_root))
		|| !terminated(out->config_path, sizeof(out->config_path))
		|| !terminated(out->username, sizeof(out->username))
		|| !terminated(out->encoding, sizeof(out->encoding))
		|| !terminated(out->auth_local, sizeof(out->auth_local))
		|| !terminated(out->auth_host, sizeof(out->auth_host)) || out->config.fd < 3
		|| fcntl(out->config.fd, F_SETFD, FD_CLOEXEC) != 0
		|| getenv(PGRAC_INITDB_WAL_CONTEXT_ENV) != NULL)
		refuse("request is not bound to the original frontend");
}

/* Resolve only a new final component. Every target is checked before the
 * first mkdir; a previous partial creation is never adopted or removed. */
static void
preflight_directory(const char *path, InitdbDirectory *out)
{
	char parent[MAXPGPATH];
	char *resolved, *slash;
	struct stat st;

	memset(out, 0, sizeof(*out));
	out->fd = out->parent = -1;
	if (path[0] != '/' || strlen(path) >= MAXPGPATH
		|| strlcpy(parent, path, sizeof(parent)) >= sizeof(parent))
		refuse("all new target paths must be absolute and bounded");
	slash = strrchr(parent, '/');
	if (slash == parent || slash[1] == '\0' || strcmp(slash + 1, ".") == 0
		|| strcmp(slash + 1, "..") == 0)
		refuse("new target path has no canonical final component");
	strlcpy(out->name, slash + 1, sizeof(out->name));
	*slash = '\0';
	resolved = realpath(parent, NULL);
	if (resolved == NULL || strcmp(resolved, parent) != 0)
		refuse("new target parent must already exist without aliases");
	free(resolved);
	out->parent = open(parent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (out->parent < 0 || fstat(out->parent, &out->parent_identity) != 0
		|| !owned_directory(&out->parent_identity)
		|| fstatat(out->parent, out->name, &st, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
		refuse("target already exists or its parent is not exclusively owned");
	strlcpy(out->path, path, sizeof(out->path));
}

static void
parent_current(const InitdbDirectory *directory)
{
	char parent[MAXPGPATH];
	struct stat held, named;
	strlcpy(parent, directory->path, sizeof(parent));
	get_parent_directory(parent);
	if (fstat(directory->parent, &held) != 0 || lstat(parent, &named) != 0
		|| !same_directory(&held, &named) || !same_directory(&held, &directory->parent_identity))
		refuse("new target parent was replaced");
}

static void
directory_current(const InitdbDirectory *directory)
{
	struct stat held, named;
	parent_current(directory);
	if (fstat(directory->fd, &held) != 0
		|| fstatat(directory->parent, directory->name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_directory(&held, &named) || !same_directory(&held, &directory->identity))
		refuse("original target directory was replaced");
}

static void
create_directory(InitdbDirectory *directory)
{
	if (creation_cancelled)
		refuse("original creation was cancelled");
	parent_current(directory);
	if (mkdirat(directory->parent, directory->name, pg_dir_create_mode) != 0)
		refuse("cannot exclusively create new target directory");
	directory->fd = openat(directory->parent, directory->name,
						   O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (directory->fd < 0 || fstat(directory->fd, &directory->identity) != 0
		|| !owned_directory(&directory->identity))
		refuse("cannot bind original new target directory");
	directory_current(directory);
}

static void
create_child(const InitdbDirectory *parent, const char *name, InitdbDirectory *child)
{
	directory_current(parent);
	memset(child, 0, sizeof(*child));
	child->fd = -1;
	child->parent = parent->fd;
	child->parent_identity = parent->identity;
	strlcpy(child->name, name, sizeof(child->name));
	if (snprintf(child->path, sizeof(child->path), "%s/%s", parent->path, name)
		>= sizeof(child->path))
		refuse("new child path is too long");
	create_directory(child);
}

static void
require_empty_directory(const InitdbDirectory *directory)
{
	struct dirent *entry;
	DIR *scan;
	int fd;
	bool empty = true;

	directory_current(directory);
	fd = openat(directory->fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0 || (scan = fdopendir(fd)) == NULL)
		refuse("cannot read original empty directory");
	errno = 0;
	while ((entry = readdir(scan)) != NULL) {
		if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
			empty = false;
	}
	if (errno != 0 || closedir(scan) != 0 || !empty)
		refuse("original undo directory is not an observed empty set");
	directory_current(directory);
}

/* Shared segments/TT are first published by the admitted live current owner.
 * Preserve the original empty namespace; local bootstrap seg_0 is not a seed. */
static void
create_undo_directories(const InitdbDirectory *shared, const ClusterSharedConfigRef *config)
{
	InitdbDirectory undo, child;
	char name[32];

	create_child(shared, "pg_undo", &undo);
	require_empty_directory(&undo);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		snprintf(name, sizeof(name), "instance_%u", node);
		create_child(&undo, name, &child);
		require_empty_directory(&child);
		if (fsync(child.fd) != 0 || close(child.fd) != 0)
			refuse("cannot persist original empty undo owner directory");
	}
	directory_current(&undo);
	if (fsync(undo.fd) != 0 || close(undo.fd) != 0 || fsync(shared->fd) != 0)
		refuse("cannot persist original empty undo namespace");
	directory_current(shared);
}

static void
request_current(const PgracInitdbCohortContext *request, const ClusterSharedConfigRef *expected)
{
	struct stat named;
	ClusterInitdbConfig *observed;
	char *canonical = realpath(request->config_path, NULL);
	if (creation_cancelled || getppid() != request->creator_pid || canonical == NULL
		|| strcmp(canonical, request->config_path) != 0 || lstat(request->config_path, &named) != 0
		|| !S_ISREG(named.st_mode) || named.st_dev != request->config.device
		|| named.st_ino != request->config.inode)
		refuse("original request or frontend identity changed");
	free(canonical);
	observed = cluster_initdb_config_preflight(&request->config);
	if (memcmp(cluster_initdb_config_reference(observed), expected, sizeof(*expected)) != 0)
		refuse("original configuration changed");
	cluster_initdb_config_free(observed);
}

static void
control_read(InitdbOrigin *origin, uint16 thread, uint64 system_identifier, bool first)
{
	ControlFileData control;
	PgracInitdbWalObservation observed;
	uint8 control_hash[32];
	pg_cryptohash_ctx *hash;
	struct stat file, held, named;
	char bytes[PG_CONTROL_FILE_SIZE + 1];
	char *wal;
	char link[MAXPGPATH];
	int global, fd;
	ssize_t n;

	directory_current(&origin->data);
	directory_current(&origin->thread);
	directory_current(&origin->wal);
	if (snprintf(link, sizeof(link), "%s/pg_wal", origin->data.path) >= sizeof(link)
		|| (wal = realpath(link, NULL)) == NULL || strcmp(wal, origin->wal.path) != 0)
		refuse("native WAL no longer names the original writer directory");
	free(wal);
	global = openat(origin->data.fd, "global", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (global < 0 || fstat(global, &held) != 0 || !owned_directory(&held))
		refuse("native control directory is invalid");
	fd = openat(global, "pg_control", O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &file) != 0 || !S_ISREG(file.st_mode) || file.st_uid != geteuid()
		|| file.st_nlink != 1 || (file.st_mode & 0022) != 0 || file.st_size != PG_CONTROL_FILE_SIZE)
		refuse("native control file is invalid");
	do {
		n = pread(fd, bytes, sizeof(bytes), 0);
	} while (n < 0 && errno == EINTR);
	if (n != PG_CONTROL_FILE_SIZE || fstatat(global, "pg_control", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| file.st_dev != named.st_dev || file.st_ino != named.st_ino
		|| file.st_size != named.st_size || file.st_mode != named.st_mode || named.st_nlink != 1
		|| close(fd) != 0 || fstatat(origin->data.fd, "global", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_directory(&held, &named) || close(global) != 0)
		refuse("native control changed during readback");
	hash = creation_hash_begin(NULL, 0);
	creation_hash_feed(hash, bytes, PG_CONTROL_FILE_SIZE);
	creation_hash_finish(hash, control_hash);
	memcpy(&control, bytes, sizeof(control));
	if (control.system_identifier != system_identifier
		|| !pgrac_initdb_wal_observe(origin->wal.fd, &control, thread, &observed)
		|| (!first
			&& (file.st_dev != origin->control_identity.st_dev
				|| file.st_ino != origin->control_identity.st_ino
				|| file.st_mode != origin->control_identity.st_mode
				|| file.st_uid != origin->control_identity.st_uid
				|| memcmp(control_hash, origin->control_sha256, 32) != 0
				|| memcmp(&control, &origin->control, sizeof(control)) != 0
				|| observed.checkpoint_start != origin->checkpoint.checkpoint_start
				|| observed.checkpoint_end != origin->checkpoint.checkpoint_end
				|| observed.checkpoint_scn != origin->checkpoint.checkpoint_scn
				|| observed.checkpoint_crc != origin->checkpoint.checkpoint_crc)))
		refuse("native shutdown checkpoint differs from its original completed child");
	origin->control_identity = file;
	memcpy(origin->control_sha256, control_hash, 32);
	origin->control = control;
	origin->checkpoint = observed;
	directory_current(&origin->data);
	directory_current(&origin->wal);
}

bool
cluster_initdb_cohort_relmap_prepare_v2(const ClusterSharedConfigRef *config,
	const PgracInitdbConfigContext *request, const uint8 branch[16],
	int data_fd, int shared_fd, PgracInitdbRecoveryContext *out)
{
	ClusterRecoveryFileIdentityV2 first = { 0 }, boot[PGRAC_INITDB_BOOT_FILES_V2];
	uintptr_t output = (uintptr_t)out;
	uintptr_t inputs[3] = {(uintptr_t)config, (uintptr_t)request, (uintptr_t)branch};
	size_t lengths[3] = {sizeof(*config), sizeof(*request), 16};
	for (unsigned i = 0; out != NULL && i < 3; ++i)
		if (output <= inputs[i] ? inputs[i] - output < sizeof(*out)
							   : output - inputs[i] < lengths[i]) return false;
	if (!config || !request || !branch || !out || config->identity.generation != 1
		|| !(config->identity.configured[0] & 1)
		|| memcmp(config->sha256, request->sha256, 32) != 0) return false;
	first.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	first.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	first.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	first.system_identifier = config->identity.system_identifier;
	first.database_incarnation = config->identity.database_incarnation;
	memcpy(first.storage_uuid, config->identity.storage_uuid, 16);
	memcpy(first.redo_branch_uuid, branch, 16);
	first.redo_branch_generation = 1;
	first.redo_timeline = 1; /* original native initdb birth; final control rechecks it */
	first.tablespace_oid = GLOBALTABLESPACE_OID;
	first.creation_generation = 1;
	first.registration_generation = 1;
	/* The original creator assigns distinct births before either child starts.
	 * Four relmaps occupy generations 1..4; CLOG0 and the actual local undo
	 * seed are separate original objects, never inferred from a current file. */
	for (unsigned i = 0; i < PGRAC_INITDB_BOOT_FILES_V2; ++i) {
		boot[i] = first;
		boot[i].kind = i == 0 ? CLUSTER_RECOVERY_FILE_CLOG_V2 : CLUSTER_RECOVERY_FILE_UNDO_V2;
		boot[i].tablespace_oid = 0;
		boot[i].creation_generation = 5 + i;
	}
	return pgrac_initdb_recovery_prepare_files_v2(&first, boot, request, data_fd, shared_fd, out);
}

bool cluster_initdb_cohort_prepare_v2(const ClusterSharedConfigRef *config,
									  const PgracInitdbConfigContext *request,
									  const uint8 branch[16], uint16 origin, int data_fd,
									  int shared_fd, PgracInitdbRecoveryContext *out)
{
	PgracInitdbRecoveryContext founder;
	ClusterRecoveryFileIdentityV2 clog;
	uintptr_t output = (uintptr_t)out;
	const void *inputs[3] = {config, request, branch};
	const Size sizes[3] = {sizeof(*config), sizeof(*request), 16};
	for (unsigned i = 0; out && i < 3; i++)
	{
		uintptr_t input = (uintptr_t)inputs[i];
		if (output <= input ? input - output < sizeof(*out) : output - input < sizes[i])
			return false;
	}
	if (!out || !config || !request || !branch || origin >= 128 ||
		!(config->identity.configured[origin / 64] & (UINT64CONST(1) << (origin % 64))) ||
		!cluster_initdb_cohort_relmap_prepare_v2(config, request, branch, data_fd, shared_fd,
												 &founder))
		return false;
	clog = founder.boot_files[0];
	clog.origin = origin;
	clog.creation_generation = 5 + origin;
	return pgrac_initdb_recovery_prepare_cohort_v2(&founder.first, &clog,
												   config->identity.configured, origin, request,
												   data_fd, shared_fd, out);
}

static void
run_child_registered(char *const *args, uint64 creator_pid,
					 const PgracInitdbRecoveryContext *registration)
{
	int status;
	int recovery_fd = -1;
	pid_t child, waited;
	bool cancelled = false;
	if (creation_cancelled || getppid() != creator_pid)
		refuse("original creation was cancelled");
	if (getenv(PGRAC_INITDB_RECOVERY_ENV) != NULL
		|| (registration != NULL
			&& !pgrac_initdb_recovery_pipe_v2(registration, &recovery_fd)))
		refuse("cannot bind original managed creation request");
	fflush(NULL);
	child = fork();
	if (child < 0) {
		if (recovery_fd >= 0) (void)close(recovery_fd);
		refuse("cannot start an original native initializer");
	}
	if (child == 0) {
		sigset_t empty;
		char descriptor[32];
		if (recovery_fd >= 0) {
			snprintf(descriptor, sizeof(descriptor), "%d", recovery_fd);
			if (fcntl(recovery_fd, F_SETFD, 0) != 0
				|| setenv(PGRAC_INITDB_RECOVERY_ENV, descriptor, 1) != 0) _exit(127);
		}
		if (setpgid(0, 0) != 0)
			_exit(127);
		pqsignal(SIGINT, SIG_DFL);
		pqsignal(SIGTERM, SIG_DFL);
		sigemptyset(&empty);
		sigprocmask(SIG_SETMASK, &empty, NULL);
		execv(args[0], args);
		_exit(127);
	}
	/* The unreaped child owns this new group. No unrelated process may be
	 * addressed through an old PID or a discovered server process list. */
	if (setpgid(child, child) != 0 && errno != EACCES) {
		kill(child, SIGKILL);
		do {
			waited = waitpid(child, &status, 0);
		} while (waited < 0 && errno == EINTR);
		if (recovery_fd >= 0) (void)close(recovery_fd);
		refuse("cannot bind the original native child group");
	}
	if (recovery_fd >= 0 && close(recovery_fd) != 0) {
		(void)kill(-child, SIGKILL);
		do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
		refuse("cannot close original managed creation request");
	}
	for (;;) {
		if (creation_cancelled || getppid() != creator_pid) {
			/* No ROOT exists. Stop every still-owned native writer before
			 * reporting cancellation; leave its partial files unadoptable. */
			cancelled = true;
			(void)kill(-child, SIGKILL);
			do {
				waited = waitpid(child, &status, 0);
			} while (waited < 0 && errno == EINTR);
			break;
		}
		waited = waitpid(child, &status, WNOHANG);
		if (waited == child || (waited < 0 && errno != EINTR))
			break;
		pg_usleep(20000L);
	}
	if (cancelled || creation_cancelled || getppid() != creator_pid)
		refuse("original creation was cancelled");
	if (waited != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
		refuse("an original native initializer did not complete successfully");
}

static void
run_child(char *const *args, uint64 creator_pid)
{
	run_child_registered(args, creator_pid, NULL);
}

/* Read only the two actual original BOOT objects. The CLOG directory moves
 * to the original archive during SIDE routing; never follow its new symlink or
 * infer an expected identity from file contents. All local FDs close on false. */
static bool
creation_boot_read(const InitdbOrigin *origin, unsigned index, bool sync,
				   ClusterRecoveryBootFileV2 *out, struct stat *identity)
{
	ClusterRecoveryFileIdentityV2 expected[PGRAC_INITDB_BOOT_FILES_V2];
	ClusterRecoveryBootFileV2 observed = { 0 };
	struct stat dirs[3], before, after, named;
	const char *parts[3], *name = index == 0 ? "0000" : "seg_0.dat";
	unsigned count = 0;
	int fds[3] = { -1, -1, -1 }, parent = origin->data.fd, fd = -1;
	bool ok = false;
	if (index >= PGRAC_INITDB_BOOT_FILES_V2
		|| !pgrac_initdb_recovery_boot_v2(&origin->relmap_registration, expected))
		return false;
	if (index == 0) {
		if (origin->side_routed)
			parts[count++] = INITDB_SIDE_ARCHIVE;
		parts[count++] = "pg_xact";
	} else {
		parts[count++] = "pg_undo";
		parts[count++] = "instance_0";
	}
	for (unsigned i = 0; i < count; ++i) {
		fds[i] = openat(parent, parts[i], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (fds[i] < 0 || fstat(fds[i], &dirs[i]) || !owned_directory(&dirs[i]))
			goto done;
		parent = fds[i];
	}
	fd = openat(parent, name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &before) || !S_ISREG(before.st_mode) || before.st_uid != geteuid()
		|| before.st_nlink != 1 || (before.st_mode & 0022) || before.st_size <= 0
		|| (sync && fsync(fd)))
		goto done;
	if (!(index == 0 ? cluster_clog_bootstrap_read_v2(fd, &expected[index], &observed)
					 : cluster_undo_bootstrap_read_v2(fd, &expected[index], &observed))
		|| !cluster_recovery_boot_file_matches_v2(&expected[index], &observed)
		|| observed.synced_bytes != (uint64)before.st_size
		|| (index == 1 && observed.synced_bytes != UNDO_SEGMENT_SIZE_BYTES)
		|| (index == 0
			&& (!observed.page.birth_observed
				|| !FullTransactionIdEquals(observed.page.birth_full_xid,
											FirstNormalFullTransactionId)))
		|| fstat(fd, &after) || fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW)
		|| before.st_dev != after.st_dev || before.st_ino != after.st_ino
		|| before.st_size != after.st_size || before.st_mode != after.st_mode
		|| before.st_uid != after.st_uid || after.st_nlink != 1 || after.st_dev != named.st_dev
		|| after.st_ino != named.st_ino || after.st_size != named.st_size
		|| after.st_mode != named.st_mode || named.st_nlink != 1)
		goto done;
	for (unsigned i = 0; i < count; ++i) {
		int p = i == 0 ? origin->data.fd : fds[i - 1];
		if (fstatat(p, parts[i], &named, AT_SYMLINK_NOFOLLOW) || !same_directory(&dirs[i], &named)
			|| (sync && fsync(fds[i])))
			goto done;
	}
	ok = true;
done:
	if (fd >= 0 && close(fd))
		ok = false;
	for (unsigned i = 0; i < count; ++i)
		if (fds[i] >= 0 && close(fds[i]))
			ok = false;
	if (ok) {
		*out = observed;
		*identity = before;
	}
	return ok;
}

static bool
creation_boot_current(InitdbOrigin *origin, bool first)
{
	ClusterRecoveryBootFileV2 observed[PGRAC_INITDB_BOOT_FILES_V2];
	struct stat identities[PGRAC_INITDB_BOOT_FILES_V2];
	PgracInitdbRecoveryContext captured = origin->relmap_registration;
	if (captured.boot_file_count == 0)
		return true; /* legacy explicit relmap-only request */
	directory_current(&origin->data);
	for (unsigned i = 0; i < captured.boot_file_count; ++i)
	{
		const ClusterRecoveryBootFileV2 *old = &origin->boot_observations[i];
		if (!creation_boot_read(origin, i, first, &observed[i], &identities[i]))
			return false;
		if (!first
			&& (identities[i].st_dev != origin->boot_file_identity[i].st_dev
				|| identities[i].st_ino != origin->boot_file_identity[i].st_ino
				|| observed[i].synced_bytes != old->synced_bytes
				|| memcmp(observed[i].image_digest, old->image_digest, 32)
				|| observed[i].page.logical_page != old->page.logical_page
				|| observed[i].page.page_generation != old->page.page_generation
				|| observed[i].page.birth_observed != old->page.birth_observed
				|| !FullTransactionIdEquals(observed[i].page.birth_full_xid,
											old->page.birth_full_xid)
				|| observed[i].page.page_lsn != old->page.page_lsn
				|| observed[i].page.metadata_crc != old->page.metadata_crc
				|| observed[i].page.payload_crc != old->page.payload_crc))
			return false;
	}
	directory_current(&origin->data);
	if (memcmp(&captured, &origin->relmap_registration, sizeof(captured)))
		return false;
	if (first) {
		memcpy(origin->boot_observations, observed, sizeof(observed));
		memcpy(origin->boot_file_identity, identities, sizeof(identities));
	}
	return true;
}

static void
run_origin(const char *initdb, const PgracInitdbCohortContext *request,
					   const ClusterSharedConfigRef *config, const char *shared,
					   InitdbOrigin *origin, int node,
					   const PgracInitdbRecoveryContext *registration) {
	char thread[64], system[96], storage[96], incarnation[96], base[MAXPGPATH + 64];
	char configuration[MAXPGPATH + 64], local[96], host[96], segment[64], uuid[33];
	char *args[24];
	int count = 0;
	static const char hex[] = "0123456789abcdef";

	request_current(request, config);
	for (int i = 0; i < 16; i++) {
		uuid[2 * i] = hex[config->identity.storage_uuid[i] >> 4];
		uuid[2 * i + 1] = hex[config->identity.storage_uuid[i] & 15];
	}
	uuid[32] = '\0';
	snprintf(thread, sizeof(thread), "--pgrac-initdb-thread=%d", node + 1);
	snprintf(system, sizeof(system), "--pgrac-initdb-system-identifier=" UINT64_FORMAT,
			 config->identity.system_identifier);
	snprintf(storage, sizeof(storage), "--pgrac-initdb-storage-uuid=%s", uuid);
	snprintf(incarnation, sizeof(incarnation),
			 "--pgrac-initdb-database-incarnation=" UINT64_FORMAT,
			 config->identity.database_incarnation);
	snprintf(base, sizeof(base), "--pgrac-initdb-shared-base=%s", shared);
	snprintf(configuration, sizeof(configuration), "--pgrac-initdb-shared-config=%s",
			 request->config_path);
	snprintf(local, sizeof(local), "--auth-local=%s", request->auth_local);
	snprintf(host, sizeof(host), "--auth-host=%s", request->auth_host);
	snprintf(segment, sizeof(segment), "--wal-segsize=%u",
			 request->segment_size / (1024 * 1024));
	args[count++] = (char *)initdb;
	args[count++] = "-D";
	args[count++] = origin->data.path;
	args[count++] = "-X";
	args[count++] = origin->wal.path;
	args[count++] = "-U";
	args[count++] = (char *)request->username;
	args[count++] = "-E";
	args[count++] = (char *)request->encoding;
	args[count++] = "--no-locale";
	args[count++] = "-k";
	args[count++] = "--no-instructions";
	args[count++] = "--no-clean";
	args[count++] = local;
	args[count++] = host;
	args[count++] = segment;
	args[count++] = thread;
	args[count++] = system;
	args[count++] = configuration;
	if (node == 0 || registration != NULL)
	{
		args[count++] = base;
		args[count++] = storage;
		args[count++] = incarnation;
	}
	args[count] = NULL;
	Assert(count < lengthof(args));
	if (registration != NULL) {
		ClusterRecoveryFileIdentityV2 identities[4];
		if ((registration->cohort[0] || registration->cohort[1]
				 ? registration->cohort_origin != node ||
					   memcmp(registration->cohort, config->identity.configured, 16)
				 : node != 0) ||
			registration->creator_pid != (uint64)getpid() ||
			registration->first.system_identifier != config->identity.system_identifier ||
			registration->first.database_incarnation != config->identity.database_incarnation ||
			memcmp(registration->first.storage_uuid, config->identity.storage_uuid, 16) != 0 ||
			memcmp(registration->config.sha256, config->sha256, 32) != 0 ||
			!pgrac_initdb_recovery_relmaps_v2(registration, identities))
			refuse("managed founder registration differs from original cohort");
		memcpy(&origin->relmap_registration, registration, sizeof(*registration));
		run_child_registered(args, request->creator_pid, &origin->relmap_registration);
	} else
		run_child(args, request->creator_pid);
	request_current(request, config);
	control_read(origin, node + 1, config->identity.system_identifier, true);
	if (registration != NULL) {
		int fd = open(shared, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		bool ok = fd >= 0 &&
				  memcmp(registration, &origin->relmap_registration, sizeof(*registration)) == 0 &&
				  (node != 0 || pgrac_initdb_recovery_relmap_observe_v2(
									&origin->relmap_registration, origin->data.fd, fd,
									origin->relmap_observations));
		if (fd >= 0 && close(fd))
			ok = false;
		if (!ok)
			refuse("original managed relmap creation cannot be captured completely");
		if (!creation_boot_current(origin, true))
			refuse("original BOOT files cannot be synchronized and captured completely");
		origin->managed_relmap = node == 0;
		origin->managed_boot = true;
	}
}

static void
create_origin_objects(const InitdbDirectory *shared, InitdbOrigin *origins,
					  const ClusterSharedConfigRef *config, uint64 incarnation)
{
	InitdbDirectory global = { 0 }, images, histories;
	char name[64];
	struct stat st;

	/* A preexisting claim cannot be promoted into this creation operation. */
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		if (fstatat(origins[node].wal.fd, CLUSTER_WAL_THREAD_CLAIM_FILENAME, &st,
					AT_SYMLINK_NOFOLLOW)
				== 0
			|| errno != ENOENT)
			refuse("an original writer claim already exists");
	}
	/* global was made by the actual founder child inside our held new DATA. */
	directory_current(shared);
	global.parent = shared->fd;
	global.parent_identity = shared->identity;
	strlcpy(global.name, "global", sizeof(global.name));
	if (snprintf(global.path, sizeof(global.path), "%s/global", shared->path)
		>= sizeof(global.path))
		refuse("original global path is too long");
	global.fd = openat(shared->fd, global.name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (global.fd < 0 || fstat(global.fd, &global.identity) != 0
		|| !owned_directory(&global.identity))
		refuse("original global directory is invalid");
	directory_current(&global);
	create_child(&global, "anchor_images", &images);
	/* INSTALL retains the founder input. Runtime history writers deliberately
	 * do not create directories; the original creator owns this empty namespace
	 * before its complete DATA tree is bound into the creation lineage. */
	create_child(&global, "wal_history", &histories);
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbDirectory thread, generation, staging, history_thread, history_staging;
		InitdbOrigin *origin = &origins[node];
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		snprintf(name, sizeof(name), "thread_%d", node + 1);
		create_child(&images, name, &thread);
		snprintf(name, sizeof(name), "generation_" UINT64_FORMAT, incarnation);
		create_child(&thread, name, &generation);
		create_child(&generation, ".staging", &staging);
		if (!cluster_initdb_origin_create(config, node, incarnation, origin->wal.fd, generation.fd,
										  &origin->control, &origin->input))
			refuse("cannot persist original writer claim and native anchor");
		control_read(origin, node + 1, config->identity.system_identifier, false);
		directory_current(&staging);
		directory_current(&generation);
		directory_current(&thread);
		if (fsync(staging.fd) != 0 || fsync(generation.fd) != 0 || fsync(thread.fd) != 0
			|| close(staging.fd) != 0 || close(generation.fd) != 0 || close(thread.fd) != 0)
			refuse("cannot persist original anchor directories");
		snprintf(name, sizeof(name), "thread_%d", node + 1);
		create_child(&histories, name, &history_thread);
		create_child(&history_thread, ".staging", &history_staging);
		directory_current(&history_staging);
		directory_current(&history_thread);
		if (fsync(history_staging.fd) != 0 || fsync(history_thread.fd) != 0
			|| close(history_staging.fd) != 0 || close(history_thread.fd) != 0)
			refuse("cannot persist original history directories");
	}
	directory_current(&images);
	directory_current(&histories);
	directory_current(&global);
	if (fsync(images.fd) != 0 || fsync(histories.fd) != 0 || fsync(global.fd) != 0
		|| close(images.fd) != 0 || close(histories.fd) != 0 || close(global.fd) != 0)
		refuse("cannot complete original anchor and history namespaces");
}

static void
create_peer_side(const InitdbDirectory *shared, InitdbOrigin *origins,
				 const ClusterSharedConfigRef *config)
{
	struct stat held, named;
	int native;

	directory_current(shared);
	/* Only founder's just-completed native child created this container. */
	native = openat(shared->fd, "native_side", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (native < 0 || fstat(native, &held) != 0 || !owned_directory(&held))
		refuse("original native SIDE namespace is invalid");
	for (int node = 1; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbOrigin *origin = &origins[node];
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(origin, node + 1, config->identity.system_identifier, false);
		if (creation_cancelled || !pgrac_initdb_side_origin_create(origin->data.fd, native, node))
			refuse("cannot persist this origin's original native SIDE");
		control_read(origin, node + 1, config->identity.system_identifier, false);
		if (fstatat(shared->fd, "native_side", &named, AT_SYMLINK_NOFOLLOW) != 0
			|| !same_directory(&held, &named))
			refuse("original native SIDE namespace was replaced");
	}
	directory_current(shared);
	if (fsync(native) != 0 || close(native) != 0 || fsync(shared->fd) != 0)
		refuse("cannot complete original native SIDE namespace");
}

static void
create_common_objects(const InitdbDirectory *shared, InitdbOrigin *origins,
					  const ClusterSharedConfigRef *config, ClusterInitdbCommon *published)
{
	const ControlFileData *sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT] = { 0 };
	ClusterInitdbCommon common;
	InitdbDirectory global = { 0 };
	const char *directories[] = { "control_images", "catalog_checkpoints" };
	char hex[65], name[96];

	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		sources[node] = &origins[node].control;
	}
	if (!cluster_initdb_common_build(config, sources, &common))
		refuse("original cohort common control fields disagree");
	directory_current(shared);
	global.parent = shared->fd;
	global.parent_identity = shared->identity;
	strlcpy(global.name, "global", sizeof(global.name));
	if (snprintf(global.path, sizeof(global.path), "%s/global", shared->path)
		>= sizeof(global.path))
		refuse("original global path is too long");
	global.fd = openat(shared->fd, global.name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (global.fd < 0 || fstat(global.fd, &global.identity) != 0
		|| !owned_directory(&global.identity))
		refuse("original global directory is invalid");
	directory_current(&global);
	for (unsigned i = 0; i < lengthof(directories); i++) {
		InitdbDirectory objects, staging;
		const uint8 *hash = i == 0 ? common.control_sha256 : common.catalog_sha256;
		const uint8 *bytes = i == 0 ? common.control : common.catalog;
		Size length = i == 0 ? sizeof(common.control) : common.catalog_length;
		create_child(&global, directories[i], &objects);
		create_child(&objects, ".staging", &staging);
		for (unsigned j = 0; j < 32; j++)
			snprintf(hex + j * 2, 3, "%02x", hash[j]);
		snprintf(name, sizeof(name), "1-%s.%s", hex, i == 0 ? "bin" : "json");
		if (!cluster_initdb_object_write_new(objects.fd, name, bytes, length))
			refuse("cannot persist original common control/catalog object");
		directory_current(&objects);
		directory_current(&staging);
		if (fsync(staging.fd) != 0 || fsync(objects.fd) != 0 || close(staging.fd) != 0
			|| close(objects.fd) != 0)
			refuse("cannot persist original common object directories");
	}
	directory_current(&global);
	if (fsync(global.fd) != 0 || close(global.fd) != 0)
		refuse("cannot complete original common objects");
	*published = common;
}

static void
open_original_child(const InitdbDirectory *parent, const char *name, InitdbDirectory *child)
{
	directory_current(parent);
	memset(child, 0, sizeof(*child));
	child->parent = parent->fd;
	child->parent_identity = parent->identity;
	strlcpy(child->name, name, sizeof(child->name));
	if (snprintf(child->path, sizeof(child->path), "%s/%s", parent->path, name)
		>= sizeof(child->path))
		refuse("original child path is too long");
	child->fd = openat(parent->fd, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (child->fd < 0 || fstat(child->fd, &child->identity) != 0
		|| !owned_directory(&child->identity))
		refuse("cannot reopen original child directory");
	directory_current(child);
}

static void
creation_tree_recheck(int fd, bool derived, const ClusterInitdbTree *expected)
{
	ClusterInitdbTree observed;
	if (!cluster_initdb_tree_read(fd, derived, &observed)
		|| memcmp(&observed, expected, sizeof(observed)) != 0)
		refuse("original file tree changed before publication");
}

static int
move_original_directory(int from, const char *name, int to)
{
#ifdef __APPLE__
	return renameatx_np(from, name, to, name, RENAME_EXCL);
#elif defined(__linux__) && defined(SYS_renameat2)
	return syscall(SYS_renameat2, from, name, to, name, RENAME_NOREPLACE);
#else
	errno = ENOTSUP;
	return -1;
#endif
}

static void
creation_side_link_current(const InitdbOrigin *origin, unsigned family,
						   const InitdbDirectory *target)
{
	struct stat link, held, routed;
	const struct stat *expected = &origin->side_links[family];
	char path[MAXPGPATH];
	ssize_t length;

	if (fstat(origin->side_link_fds[family], &held) != 0 || !S_ISLNK(held.st_mode)
		|| held.st_nlink != 1 || held.st_dev != expected->st_dev || held.st_ino != expected->st_ino
		|| fstatat(origin->data.fd, side_families[family], &link, AT_SYMLINK_NOFOLLOW) != 0
		|| !S_ISLNK(link.st_mode) || link.st_uid != geteuid() || link.st_nlink != 1
		|| link.st_dev != expected->st_dev || link.st_ino != expected->st_ino)
		refuse("original native SIDE link was replaced");
	length = readlinkat(origin->data.fd, side_families[family], path, sizeof(path));
	if (length < 0 || length != strlen(target->path) || memcmp(path, target->path, length) != 0
		|| fstatat(origin->data.fd, side_families[family], &routed, 0) != 0
		|| !same_directory(&routed, &target->identity))
		refuse("original native SIDE link no longer selects its shared directory");
}

static void
creation_side_current(const InitdbDirectory *shared, const InitdbOrigin *origin, unsigned node)
{
	InitdbDirectory native, target_root, archive;
	char name[32];

	if (!origin->side_routed)
		refuse("original native SIDE routing is incomplete");
	open_original_child(shared, "native_side", &native);
	snprintf(name, sizeof(name), "origin_%u", node);
	open_original_child(&native, name, &target_root);
	open_original_child(&origin->data, INITDB_SIDE_ARCHIVE, &archive);
	if (!same_directory(&archive.identity, &origin->side_archive))
		refuse("original native SIDE archive was replaced");
	for (unsigned i = 0; i < lengthof(side_families); i++) {
		InitdbDirectory source, target;
		open_original_child(&archive, side_families[i], &source);
		open_original_child(&target_root, side_families[i], &target);
		if (!same_directory(&source.identity, &origin->side_sources[i])
			|| !same_directory(&target.identity, &origin->side_targets[i]))
			refuse("original native SIDE directory was replaced");
		creation_tree_recheck(source.fd, false, &origin->side_source_trees[i]);
		creation_tree_recheck(target.fd, false, &origin->side_target_trees[i]);
		creation_side_link_current(origin, i, &target);
		directory_current(&source);
		directory_current(&target);
		if (close(source.fd) != 0 || close(target.fd) != 0)
			refuse("cannot close original native SIDE observation");
	}
	directory_current(&archive);
	directory_current(&target_root);
	directory_current(&native);
	if (close(archive.fd) != 0 || close(target_root.fd) != 0 || close(native.fd) != 0)
		refuse("cannot close original native SIDE namespace");
}

/* Only the original creator calls this, after every native child has exited.
 * Retain the original directories; no startup reader may perform this move. */
static void
route_original_side(const InitdbDirectory *shared, InitdbOrigin *origin, unsigned node)
{
	InitdbDirectory native, target_root, archive, sources[4], targets[4];
	char name[32];

	if (origin->side_routed || node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		refuse("original native SIDE routing cannot be repeated");
	open_original_child(shared, "native_side", &native);
	snprintf(name, sizeof(name), "origin_%u", node);
	open_original_child(&native, name, &target_root);
	/* Inspect all four pairs before the first change to this PGDATA. */
	for (unsigned i = 0; i < lengthof(side_families); i++) {
		open_original_child(&origin->data, side_families[i], &sources[i]);
		open_original_child(&target_root, side_families[i], &targets[i]);
		if ((sources[i].identity.st_dev == targets[i].identity.st_dev
			 && sources[i].identity.st_ino == targets[i].identity.st_ino)
			|| !cluster_initdb_tree_read(sources[i].fd, false, &origin->side_source_trees[i])
			|| !cluster_initdb_tree_read(targets[i].fd, false, &origin->side_target_trees[i])
			|| memcmp(origin->side_source_trees[i].content, origin->side_target_trees[i].content,
					  32)
				   != 0)
			refuse("shared native SIDE differs from its original completed child");
		origin->side_sources[i] = sources[i].identity;
		origin->side_targets[i] = targets[i].identity;
	}
	create_child(&origin->data, INITDB_SIDE_ARCHIVE, &archive);
	origin->side_archive = archive.identity;
	for (unsigned i = 0; i < lengthof(side_families); i++) {
		directory_current(&sources[i]);
		directory_current(&targets[i]);
		directory_current(&archive);
		creation_tree_recheck(sources[i].fd, false, &origin->side_source_trees[i]);
		creation_tree_recheck(targets[i].fd, false, &origin->side_target_trees[i]);
		if (move_original_directory(origin->data.fd, side_families[i], archive.fd) != 0)
			refuse("cannot exclusively retain original native SIDE directory");
		if (symlinkat(targets[i].path, origin->data.fd, side_families[i]) != 0
			|| fstatat(origin->data.fd, side_families[i], &origin->side_links[i],
					   AT_SYMLINK_NOFOLLOW)
				   != 0)
			refuse("cannot install original native SIDE link");
			/* Hold the link itself until this creator exits, including every ROOT
		 * publication recheck. An unlinked original cannot recycle its inode. */
#if defined(__APPLE__)
		origin->side_link_fds[i] = openat(origin->data.fd, side_families[i], O_SYMLINK | O_CLOEXEC);
#elif defined(__linux__) && defined(O_PATH)
		origin->side_link_fds[i]
			= openat(origin->data.fd, side_families[i], O_PATH | O_NOFOLLOW | O_CLOEXEC);
#else
		origin->side_link_fds[i] = -1;
		errno = ENOTSUP;
#endif
		if (origin->side_link_fds[i] < 0)
			refuse("cannot hold original native SIDE link");
		creation_side_link_current(origin, i, &targets[i]);
		if (close(sources[i].fd) != 0 || close(targets[i].fd) != 0)
			refuse("cannot close original native SIDE directories");
	}
	directory_current(&archive);
	directory_current(&target_root);
	directory_current(&native);
	if (fsync(archive.fd) != 0 || fsync(origin->data.fd) != 0 || close(archive.fd) != 0
		|| close(target_root.fd) != 0 || close(native.fd) != 0)
		refuse("cannot persist original native SIDE routing");
	origin->side_routed = true;
	creation_side_current(shared, origin, node);
}

static void
create_catalog_objects(const InitdbDirectory *shared, InitdbOrigin *origins,
					   const ClusterSharedConfigRef *config)
{
	ClusterCatalogInitialInput input = { 0 };
	InitdbDirectory global;
	uint8 *founder = NULL;
	Size founder_length = 0;

	input.identity.system_identifier = config->identity.system_identifier;
	input.identity.database_incarnation = config->identity.database_incarnation;
	memcpy(input.identity.storage_uuid, config->identity.storage_uuid, 16);
	memcpy(input.identity.authority_uuid, config->identity.authority_uuid, 16);
	input.native_control = &origins[0].control;
	input.native_control_length = sizeof(origins[0].control);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbDirectory archive, clog;
		uint8 *bytes = NULL;
		Size length = 0;
		const ClusterRecoveryFileIdentityV2 *expected = NULL;
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		creation_side_current(shared, &origins[node], node);
		if (origins[node].managed_boot) {
			const PgracInitdbRecoveryContext *r = &origins[node].relmap_registration;
			if (r->boot_file_count == 0 || r->boot_file_count > PGRAC_INITDB_BOOT_FILES_V2)
				refuse("invalid original catalog BOOT inventory");
			for (uint64 i = 0; i < r->boot_file_count; ++i)
				if (r->boot_files[i].kind == CLUSTER_RECOVERY_FILE_CLOG_V2) {
					if (expected != NULL || r->boot_files[i].origin != node
						|| r->boot_files[i].system_identifier != config->identity.system_identifier
						|| r->boot_files[i].database_incarnation != config->identity.database_incarnation)
						refuse("original catalog CLOG identity disagrees with its creator");
					expected = &r->boot_files[i];
				}
			if (expected == NULL)
				refuse("original catalog CLOG identity is missing");
		}
		open_original_child(&origins[node].data, INITDB_SIDE_ARCHIVE, &archive);
		open_original_child(&archive, "pg_xact", &clog);
		if (!(origins[node].managed_boot
			? cluster_initdb_catalog_read_managed_clog_v2(clog.fd, expected,
				U64FromFullTransactionId(origins[node].control.checkPointCopy.nextXid), &bytes, &length)
			: cluster_initdb_catalog_read_clog(
				clog.fd, U64FromFullTransactionId(origins[node].control.checkPointCopy.nextXid),
				&bytes, &length)))
			refuse("cannot read original native catalog transaction history");
		if (node == 0) {
			founder = bytes;
			founder_length = length;
		} else {
			if (length != founder_length || memcmp(bytes, founder, length) != 0)
				refuse("original cohort catalog transaction histories disagree");
			free(bytes);
		}
		directory_current(&clog);
		directory_current(&archive);
		if (close(clog.fd) != 0 || close(archive.fd) != 0)
			refuse("cannot close original catalog transaction history");
		creation_side_current(shared, &origins[node], node);
	}
	input.native_clog = founder;
	input.native_clog_length = founder_length;
	open_original_child(shared, "global", &global);
	if (!cluster_initdb_catalog_create(global.fd, &input))
		refuse("cannot persist original catalog allocator inputs");
	directory_current(&global);
	if (close(global.fd) != 0)
		refuse("cannot close original catalog namespace");
	free(founder);
	/* The retained, independent native history remains available at each
	 * origin. ROOT observes both the shared native bytes and these outputs. */
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		creation_side_current(shared, &origins[node], node);
	}
}

static void
create_storage_contracts(InitdbOrigin *origins, const ClusterSharedConfigRef *config)
{
	static const char digits[] = "0123456789abcdef";
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbOrigin *origin = &origins[node];
		InitdbDirectory global;
		ClusterCfContractRecord *record = &origin->storage_contract;
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		memset(record, 0, sizeof(*record));
		record->magic = CLUSTER_CF_CONTRACT_MAGIC;
		record->version = CLUSTER_CF_CONTRACT_VERSION;
		record->authority_system_identifier = config->identity.system_identifier;
		for (unsigned i = 0; i < 16; i++) {
			record->storage_uuid[2 * i] = digits[config->identity.storage_uuid[i] >> 4];
			record->storage_uuid[2 * i + 1] = digits[config->identity.storage_uuid[i] & 15];
		}
		record->state = CLUSTER_CF_CONTRACT_UNVERIFIED;
		INIT_CRC32C(record->crc);
		COMP_CRC32C(record->crc, record, offsetof(ClusterCfContractRecord, crc));
		FIN_CRC32C(record->crc);
		open_original_child(&origin->data, "global", &global);
		origin->storage_contract_directory = global.identity;
		if (!cluster_initdb_object_write_observed(global.fd, "pgrac_cf_contract",
												  (const uint8 *)record, sizeof(*record),
												  &origin->storage_contract_identity))
			refuse("cannot persist original unverified storage identity");
		directory_current(&global);
		if (close(global.fd) != 0)
			refuse("cannot close original local global directory");
	}
}

static void
creation_storage_current(InitdbOrigin *origin)
{
	InitdbDirectory global;
	open_original_child(&origin->data, "global", &global);
	if (global.identity.st_dev != origin->storage_contract_directory.st_dev
		|| global.identity.st_ino != origin->storage_contract_directory.st_ino
		|| !cluster_initdb_object_recheck(
			global.fd, "pgrac_cf_contract", (const uint8 *)&origin->storage_contract,
			sizeof(origin->storage_contract), &origin->storage_contract_identity))
		refuse("original unverified storage identity changed before ROOT publication");
	directory_current(&global);
	if (close(global.fd) != 0)
		refuse("cannot close original local global directory");
}

static void
creation_sources_current(const PgracInitdbCohortContext *request,
									 const ClusterSharedConfigRef *config,
									 InitdbDirectory roots[4], InitdbOrigin *origins,
									 const ClusterInitdbTree *data,
									 const ClusterInitdbTree *undo, bool derived) {
	request_current(request, config);
	for (unsigned i = 0; i < 4; i++)
		directory_current(&roots[i]);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		creation_side_current(&roots[1], &origins[node], node);
		creation_tree_recheck(origins[node].wal.fd, false, &origins[node].wal_observed);
		creation_storage_current(&origins[node]);
		if (origins[node].managed_relmap &&
			!pgrac_initdb_recovery_relmap_current_v2(&origins[node].relmap_registration,
													 origins[node].data.fd, roots[1].fd,
													 origins[node].relmap_observations))
			refuse("original managed relmap creation changed before ROOT publication");
		if ((origins[node].managed_boot || origins[node].managed_relmap) &&
			!creation_boot_current(&origins[node], false))
			refuse("original BOOT file identity or bytes changed before ROOT publication");
	}
	creation_tree_recheck(roots[1].fd, derived, data);
	creation_tree_recheck(roots[3].fd, false, undo);
	request_current(request, config);
}

typedef struct InitdbStartupObject {
	InitdbDirectory directory;
	struct stat identity;
	char name[112];
} InitdbStartupObject;

static void
creation_derived_current(const InitdbDirectory *global, const InitdbDirectory *startups,
						 const InitdbStartupObject objects[128], const ControlRootImage *root,
						 const uint8 inputs[128][CLUSTER_WAL_STARTUP_BYTES],
						 const struct stat *backup)
{
	directory_current(global);
	directory_current(startups);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		if (!root->present[node])
			continue;
		directory_current(&objects[node].directory);
		if (!cluster_initdb_object_recheck(objects[node].directory.fd, objects[node].name,
										   inputs[node], CLUSTER_WAL_STARTUP_BYTES,
										   &objects[node].identity))
			refuse("original selected startup input changed before ROOT publication");
	}
	if (backup != NULL
		&& !cluster_initdb_object_recheck(global->fd, "pgrac_control_root.bak", root->bytes,
										  sizeof(root->bytes), backup))
		refuse("original backup ROOT changed before primary publication");
}

/* The default v3 publisher cannot select even one managed-format child. */
static void
creation_legacy_format_current(const InitdbOrigin *origins)
{
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++node)
		if (origins[node].managed_relmap || origins[node].managed_boot)
			refuse("managed creation requires complete ROOTv4 file registration");
}

/* The managed child retains all four preallocated identities and eight actual
 * file observations. Other file families still require their original owners. */
static bool
creation_relmap_registry_valid(const InitdbOrigin *origins,
										   const ClusterRecoveryRegistryV2 *r) {
	const InitdbOrigin *o = &origins[0];
	ClusterRecoveryFileIdentityV2 identities[4];
	uint8 encoded[128];
	unsigned found = 0;
	for (unsigned node = 1; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++node)
		if (origins[node].managed_relmap)
			return false;
	if (!o->managed_relmap)
		return !(o->relmap_registration.cohort[0] || o->relmap_registration.cohort[1]);
	if (!pgrac_initdb_recovery_relmaps_v2(&o->relmap_registration, identities))
		return false;
	for (unsigned j = 0; j < 8; ++j) {
		const ClusterRecoveryCheckpointFileV2 *captured = &o->relmap_observations[j];
		if (!cluster_recovery_file_identity_encode_v2(&identities[j / 2], encoded,
													  sizeof(encoded)) ||
			!cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded),
													   &captured->identity) ||
			captured->copy_index != j % 2 || captured->copy_count != 2 ||
			captured->synced_bytes != 2304 || captured->first_page != 0 ||
			captured->page_count != 1 || captured->source_count != 0 ||
			captured->sources != NULL)
			return false;
	}
	for (uint64 i = 0; i < r->coverage.file_count; ++i) {
		const ClusterRecoveryCheckpointFileV2 *f = &r->coverage.files[i];
		bool matched = false;
		if (f->identity.kind != CLUSTER_RECOVERY_FILE_RELMAP_V2)
			continue;
		if (!cluster_recovery_file_identity_encode_v2(&f->identity, encoded, sizeof(encoded)))
			return false;
		for (unsigned j = 0; j < 8; ++j) {
			const ClusterRecoveryCheckpointFileV2 *captured = &o->relmap_observations[j];
			if (f->copy_index != captured->copy_index ||
				!cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded),
														   &captured->identity))
				continue;
			if ((found & (1U << j)) || f->copy_count != captured->copy_count ||
				f->synced_bytes != captured->synced_bytes ||
				f->first_page != captured->first_page ||
				f->page_count != captured->page_count ||
				memcmp(f->image_digest, captured->image_digest, 32))
				return false;
			matched = true;
			found |= 1U << j;
			break;
		}
		if (!matched)
			return false;
	}
	return found == 255;
}
/* A relmap-only child cannot become a complete managed creation. Every
 * initial CLOG/TT entry must be the original producer's complete observation. */
static bool
creation_boot_registry_valid(const InitdbOrigin *origins, const ClusterRecoveryRegistryV2 *r)
{
	const InitdbOrigin *o = &origins[0];
	ClusterRecoveryFileIdentityV2 expected[PGRAC_INITDB_BOOT_FILES_V2];
	unsigned found = 0;
	if (o->relmap_registration.cohort[0] || o->relmap_registration.cohort[1])
	{
		unsigned expected_count = 0, found_count = 0;
		bool seen[128] = {false};
		const uint64 *cohort = o->relmap_registration.cohort;
		for (unsigned n = 0; n < 128; n++)
		{
			bool selected = (cohort[n / 64] & (UINT64CONST(1) << (n % 64))) != 0;
			const InitdbOrigin *child = &origins[n];
			ClusterRecoveryFileIdentityV2 ids[2];
			if (!selected)
			{
				if (child->managed_boot)
					return false;
				continue;
			}
			if (!child->managed_boot || child->relmap_registration.cohort_origin != n ||
				memcmp(child->relmap_registration.cohort, cohort, 16) ||
				!pgrac_initdb_recovery_boot_v2(&child->relmap_registration, ids) ||
				!cluster_recovery_boot_file_matches_v2(&ids[0], &child->boot_observations[0]))
				return false;
			expected_count++;
		}
		for (uint64 i = 0; i < r->coverage.file_count; i++)
		{
			const ClusterRecoveryCheckpointFileV2 *f = &r->coverage.files[i];
			const ClusterRecoveryBootFileV2 *b;
			uint8 encoded[128];
			unsigned n = f->identity.origin;
			if (f->identity.kind == CLUSTER_RECOVERY_FILE_RELMAP_V2)
				continue;
			if (f->identity.kind != CLUSTER_RECOVERY_FILE_CLOG_V2 || n >= 128 || seen[n] ||
				!(cohort[n / 64] & (UINT64CONST(1) << (n % 64))))
				return false;
			b = &origins[n].boot_observations[0];
			if (!cluster_recovery_file_identity_encode_v2(&b->identity, encoded, sizeof(encoded)) ||
				!cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded),
														   &f->identity) ||
				f->copy_index || f->copy_count != 1 || f->page_count != 1 || f->first_page ||
				f->synced_bytes != b->synced_bytes || memcmp(f->image_digest, b->image_digest, 32))
				return false;
			seen[n] = true;
			found_count++;
		}
		return found_count == expected_count;
	}

	if (!o->managed_relmap)
		return true;
	if (!pgrac_initdb_recovery_boot_v2(&o->relmap_registration, expected))
		return false;
	for (unsigned i = 0; i < PGRAC_INITDB_BOOT_FILES_V2; ++i)
		if (!cluster_recovery_boot_file_matches_v2(&expected[i], &o->boot_observations[i]))
			return false;
	for (uint64 i = 0; i < r->coverage.file_count; ++i) {
		const ClusterRecoveryCheckpointFileV2 *f = &r->coverage.files[i];
		uint8 encoded[128];
		unsigned j;
		if (f->identity.kind != CLUSTER_RECOVERY_FILE_CLOG_V2
			&& f->identity.kind != CLUSTER_RECOVERY_FILE_UNDO_V2)
			continue;
		for (j = 0; j < PGRAC_INITDB_BOOT_FILES_V2; ++j) {
			const ClusterRecoveryBootFileV2 *b = &o->boot_observations[j];
			if (!cluster_recovery_file_identity_encode_v2(&expected[j], encoded, sizeof(encoded))
				|| !cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded),
															  &f->identity))
				continue;
			if ((found & (1U << j)) || f->copy_index != 0 || f->copy_count != 1
				|| f->synced_bytes != b->synced_bytes || f->first_page != b->page.logical_page
				|| f->page_count != (j == 0 ? 1 : UNDO_SEGMENT_SIZE_BYTES / BLCKSZ)
				|| memcmp(f->image_digest, b->image_digest, 32))
				return false;
			found |= 1U << j;
			break;
		}
		if (j == PGRAC_INITDB_BOOT_FILES_V2)
			return false;
	}
	return found == 3;
}

/* First registration only, from the original complete creation cohort. File
 * observations must already be supplied by that creator's managed producers;
 * neither this predicate nor the registry codec supplies their custody. Bounds
 * unavailable from this native shutdown checkpoint remain explicitly unknown. */
static bool
creation_registry_valid(const ClusterSharedConfigRef *config, const InitdbOrigin *origins,
						const ControlRootImage *root, const ClusterRecoveryRegistryV2 *r,
						Size budget)
{
	Size n;
	uint64 at = 0;
	if (!config || !origins || !root || !r || !cluster_recovery_registry_size_v2(r, budget, &n)
		|| config->identity.generation != 1 || root->header.file_txn_seq != 1
		|| root->header.lineage_kind != PGRAC_CONTROL_LINEAGE_CREATION_V1
		|| root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| r->system_identifier != config->identity.system_identifier
		|| r->system_identifier != root->header.system_identifier
		|| r->database_incarnation != config->identity.database_incarnation
		|| r->database_incarnation != root->header.v2.database_incarnation
		|| memcmp(r->storage_uuid, config->identity.storage_uuid, 16)
		|| memcmp(r->storage_uuid, root->header.storage_uuid, 16)
		|| memcmp(root->header.authority_uuid, config->identity.authority_uuid, 16)
		|| memcmp(root->header.v2.configured, config->identity.configured, 16)
		|| root->header.v2.config_generation != 1
		|| memcmp(root->header.v2.config_sha256, config->sha256, 32)
		|| r->redo_branch_generation != 1 || r->coverage.control_generation != 1
		|| r->coverage.registration_generation != 1 || r->coverage.checkpoint_generation != 1
		|| r->coverage.commit_generation != 1 || !creation_relmap_registry_valid(origins, r)
		|| !creation_boot_registry_valid(origins, r))
		return false;
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++node) {
		const InitdbOrigin *o = &origins[node];
		const ClusterRecoveryCheckpointSourceV2 *source;
		const CheckPoint *actual = &o->control.checkPointCopy, *captured;
		bool selected
			= (config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))) != 0;
		if (root->present[node] != selected)
			return false;
		if (!selected)
			continue;
		if (at >= r->coverage.source_count)
			return false;
		source = &r->coverage.sources[at++];
		captured = &source->checkpoint;
		if (!cluster_control_root_identity_equal(&source->source.claim.identity,
												 &o->input.snapshot.identity)
			|| !cluster_control_root_identity_equal(&root->records[node].identity,
													&o->input.snapshot.identity)
			|| source->source.claim.identity.origin_node_id != node
			|| source->source.claim.database_incarnation != r->database_incarnation
			|| source->source.claim.max_config_generation != 1
			|| memcmp(source->source.claim.claim_sha256, o->input.refs.claim_sha256, 32)
			|| memcmp(root->refs[node].claim_sha256, o->input.refs.claim_sha256, 32)
			|| o->control.system_identifier != r->system_identifier
			|| o->control.state != DB_SHUTDOWNED
			|| source->source.timeline != actual->ThisTimeLineID
			|| source->checkpoint_start != o->control.checkPoint
			|| source->checkpoint_start != o->checkpoint.checkpoint_start
			|| source->checkpoint_end != o->checkpoint.checkpoint_end
			|| source->redo_start != actual->redo
			|| source->replay_end != o->checkpoint.checkpoint_end
			|| U64FromFullTransactionId(source->native_floor)
			|| U64FromFullTransactionId(source->post_flush_issued) || source->local_bounds_observed
			|| U64FromFullTransactionId(source->next_full_xid) || source->oldest_clog_xid)
			return false;
		/* Compare all serialized fields, never native struct padding. */
#define CREATION_CHECKPOINT_SAME(field)                                                            \
	if (captured->field != actual->field)                                                          \
	return false
		CREATION_CHECKPOINT_SAME(redo);
		CREATION_CHECKPOINT_SAME(ThisTimeLineID);
		CREATION_CHECKPOINT_SAME(PrevTimeLineID);
		CREATION_CHECKPOINT_SAME(fullPageWrites);
		if (U64FromFullTransactionId(captured->nextXid)
			!= U64FromFullTransactionId(actual->nextXid))
			return false;
		CREATION_CHECKPOINT_SAME(nextOid);
		CREATION_CHECKPOINT_SAME(nextMulti);
		CREATION_CHECKPOINT_SAME(nextMultiOffset);
		CREATION_CHECKPOINT_SAME(oldestXid);
		CREATION_CHECKPOINT_SAME(oldestXidDB);
		CREATION_CHECKPOINT_SAME(oldestMulti);
		CREATION_CHECKPOINT_SAME(oldestMultiDB);
		CREATION_CHECKPOINT_SAME(time);
		CREATION_CHECKPOINT_SAME(oldestCommitTsXid);
		CREATION_CHECKPOINT_SAME(newestCommitTsXid);
		CREATION_CHECKPOINT_SAME(oldestActiveXid);
#undef CREATION_CHECKPOINT_SAME
	}
	if (at != r->coverage.source_count)
		return false;
	for (uint64 i = 0; i < r->coverage.file_count; ++i) {
		const ClusterRecoveryCheckpointFileV2 *f = &r->coverage.files[i];
		unsigned origin = f->identity.origin;
		if (origin >= 128 || !root->present[origin] || f->identity.registration_generation != 1
			|| f->identity.redo_timeline != origins[origin].control.checkPointCopy.ThisTimeLineID
			|| f->source_count != at)
			return false;
		for (uint64 j = 0; j < f->source_count; ++j) {
			const ClusterRecoveryCheckpointFileSourceV2 *v = &f->sources[j];
			const ClusterRecoveryCheckpointSourceV2 *source = &r->coverage.sources[v->source_index];
			if (v->redo_start != source->redo_start || v->replay_end != source->replay_end)
				return false;
		}
	}
	return true;
}

typedef struct InitdbRegistration
{
	ClusterRecoveryRegistryV2 registry;
	ClusterRecoveryCheckpointSourceV2 sources[128];
	ClusterRecoveryCheckpointFileSourceV2 intervals[128];
	ClusterRecoveryCheckpointFileV2 files[8 + 128];
} InitdbRegistration;

static bool creation_registration_build(const ClusterSharedConfigRef *config,
										const InitdbOrigin *origins, const uint8 branch[16],
										InitdbRegistration *out, Size budget)
{
	InitdbRegistration *result = palloc0(sizeof(*result));
	ClusterRecoveryRegistryV2 *r = &result->registry;
	bool ok = false;
	Size length;
	r->system_identifier = config->identity.system_identifier;
	r->database_incarnation = config->identity.database_incarnation;
	memcpy(r->storage_uuid, config->identity.storage_uuid, 16);
	memcpy(r->redo_branch_uuid, branch, 16);
	r->redo_branch_generation = 1;
	r->coverage_format_version = 4;
	r->coverage.control_generation = r->coverage.registration_generation = 1;
	r->coverage.checkpoint_generation = r->coverage.commit_generation = 1;
	r->coverage.sources = result->sources;
	r->coverage.files = result->files;
	for (unsigned n = 0; n < 128; n++)
	{
		const InitdbOrigin *o = &origins[n];
		ClusterRecoveryCheckpointSourceV2 *v;
		uint64 at = r->coverage.source_count;
		if (!(config->identity.configured[n / 64] & (UINT64CONST(1) << (n % 64))))
			continue;
		if (!o->managed_boot || o->relmap_registration.cohort_origin != n ||
			memcmp(o->relmap_registration.cohort, config->identity.configured, 16) ||
			memcmp(o->relmap_registration.first.redo_branch_uuid, branch, 16))
			goto done;
		v = &result->sources[at];
		v->source.claim.identity = o->input.snapshot.identity;
		v->source.claim.database_incarnation = r->database_incarnation;
		v->source.claim.max_config_generation = 1;
		memcpy(v->source.claim.claim_sha256, o->input.refs.claim_sha256, 32);
		v->source.timeline = o->control.checkPointCopy.ThisTimeLineID;
		v->checkpoint = o->control.checkPointCopy;
		v->checkpoint_start = o->checkpoint.checkpoint_start;
		v->checkpoint_end = o->checkpoint.checkpoint_end;
		v->redo_start = v->checkpoint.redo;
		v->replay_end = v->checkpoint_end;
		result->intervals[at].source_index = at;
		result->intervals[at].redo_start = v->redo_start;
		result->intervals[at].replay_end = v->replay_end;
		r->coverage.source_count++;
	}
	for (unsigned i = 0; i < 8; i++)
		result->files[i] = origins[0].relmap_observations[i];
	r->coverage.file_count = 8;
	for (unsigned n = 0; n < 128; n++)
	{
		const ClusterRecoveryBootFileV2 *b = &origins[n].boot_observations[0];
		ClusterRecoveryCheckpointFileV2 *f;
		if (!(config->identity.configured[n / 64] & (UINT64CONST(1) << (n % 64))))
			continue;
		f = &result->files[r->coverage.file_count++];
		f->identity = b->identity;
		f->synced_bytes = b->synced_bytes;
		f->copy_count = 1;
		f->page_count = 1;
		memcpy(f->image_digest, b->image_digest, 32);
	}
	for (uint64 i = 0; i < r->coverage.file_count; i++)
	{
		result->files[i].source_count = r->coverage.source_count;
		result->files[i].sources = result->intervals;
	}
	if (!creation_relmap_registry_valid(origins, r) || !creation_boot_registry_valid(origins, r) ||
		!cluster_recovery_registry_size_v2(r, budget, &length))
		goto done;
	*out = *result;
	out->registry.coverage.sources = out->sources;
	out->registry.coverage.files = out->files;
	for (uint64 i = 0; i < out->registry.coverage.file_count; i++)
		out->files[i].sources = out->intervals;
	ok = true;
done:
	pfree(result);
	return ok;
}

static void
creation_registry_current(const InitdbDirectory *global, const ClusterSharedConfigRef *config,
						  const InitdbOrigin *origins, const ControlRootImage *root,
						  const ClusterRecoveryRegistryV2 *r, Size budget, const uint8 *captured,
						  Size length, const uint8 reference[128])
{
	if (!creation_registry_valid(config, origins, root, r, budget)
		|| !cluster_recovery_registry_matches_v2(captured, length, r, reference)
		|| !cluster_recovery_registry_read_v2(global->fd, r, reference, budget)
		|| !cluster_recovery_registry_matches_v2(captured, length, r, reference))
		refuse("original complete creation registration changed before ROOT publication");
}

/* Same creator, same source/derived revalidation and exclusive object writes.
 * The optional complete managed registration is dormant until all original
 * family producers supply it. The original Main supplies the complete cohort;
 * an isolated NULL caller retains its v3 refusal for a managed child. No startup/admission/coverage
 * authority escapes. FATAL terminates this dedicated creator; uncertain objects are never erased.
 */
static void
creation_root_publish(const PgracInitdbCohortContext *request, const ClusterSharedConfigRef *config,
					  InitdbDirectory roots[4], InitdbOrigin *origins,
					  const ClusterInitdbTree *data, const ClusterInitdbTree *undo,
					  const InitdbDirectory *global, const InitdbDirectory *startups,
					  const InitdbStartupObject objects[128], const ControlRootImage *root,
					  const uint8 inputs[128][CLUSTER_WAL_STARTUP_BYTES],
					  const ClusterRecoveryRegistryV2 *registration, Size registry_budget)
{
	const uint8 *selected = root->bytes;
	Size selected_length = sizeof(root->bytes), registry_length = 0;
	uint8 *registry_bytes = NULL, *managed_root = NULL;
	uint8 reference[128], persisted[128];
	struct stat backup;

	if (registration) {
		if (!creation_registry_valid(config, origins, root, registration, registry_budget)
			|| !cluster_recovery_registry_size_v2(registration, registry_budget, &registry_length))
			refuse("incomplete original creation registration");
		registry_bytes = palloc(registry_length);
		managed_root = palloc(66560);
		if (!cluster_recovery_registry_encode_v2(registration, registry_bytes, registry_length,
												 reference)
			|| !cluster_control_root_registry_encode_v4(root, registration, reference, managed_root,
														66560))
			refuse("cannot encode original registered creation ROOT");
		selected = managed_root;
		selected_length = 66560;
	} else
		creation_legacy_format_current(origins);

	creation_sources_current(request, config, roots, origins, data, undo, true);
	creation_derived_current(global, startups, objects, root, inputs, NULL);
	if (registration) {
		if (!cluster_recovery_registry_create_v2(global->fd, registration, registry_budget,
												 persisted)
			|| memcmp(reference, persisted, sizeof(reference)))
			refuse("cannot persist original complete creation registration");
		creation_sources_current(request, config, roots, origins, data, undo, true);
		creation_derived_current(global, startups, objects, root, inputs, NULL);
		creation_registry_current(global, config, origins, root, registration, registry_budget,
								  registry_bytes, registry_length, reference);
	}
	if (!cluster_initdb_object_write_observed(global->fd, "pgrac_control_root.bak", selected,
											  selected_length, &backup))
		refuse("cannot persist original backup ROOT");
	creation_sources_current(request, config, roots, origins, data, undo, true);
	creation_derived_current(global, startups, objects, root, inputs, NULL);
	if (registration) {
		creation_registry_current(global, config, origins, root, registration, registry_budget,
								  registry_bytes, registry_length, reference);
		/* Registry I/O cannot substitute for a final native-source check. */
		creation_sources_current(request, config, roots, origins, data, undo, true);
		creation_derived_current(global, startups, objects, root, inputs, NULL);
		if (!creation_registry_valid(config, origins, root, registration, registry_budget)
			|| !cluster_recovery_registry_matches_v2(registry_bytes, registry_length, registration,
													 reference))
			refuse("original creation input changed after registration readback");
	}
	if (!cluster_initdb_object_recheck(global->fd, "pgrac_control_root.bak", selected,
									   selected_length, &backup))
		refuse("original backup ROOT changed before primary publication");
	if (!cluster_initdb_object_write_new(global->fd, "pgrac_control_root", selected,
										 selected_length))
		refuse("cannot persist original primary ROOT");
	if (managed_root)
		pfree(managed_root);
	if (registry_bytes)
		pfree(registry_bytes);
}

static void
create_root_objects(const PgracInitdbCohortContext *request, InitdbDirectory roots[4],
					InitdbOrigin *origins, const ClusterSharedConfigRef *config,
					const ClusterInitdbCommon *common, uint64 incarnation,
					const ClusterRecoveryRegistryV2 *registration, Size registry_budget)
{
	static const char origins_domain[] = "PGRAC-CREATION-ORIGINS-V1";
	static const char cohort_domain[] = "PGRAC-CREATION-COHORT-V1";
	ControlRootImage *sources = palloc0(sizeof(*sources)), *root = palloc0(sizeof(*root));
	uint8(*inputs)[CLUSTER_WAL_STARTUP_BYTES] = palloc0(128 * CLUSTER_WAL_STARTUP_BYTES);
	uint8(*bindings)[PGRAC_CONTROL_BINDING_BYTES] = palloc0(128 * PGRAC_CONTROL_BINDING_BYTES);
	uint8 ids[128][16] = { { 0 } };
	ClusterInitdbTree data, undo;
	ControlRootHeader *header = &sources->header;
	ControlRootCommonV2 *shared = &header->v2;
	InitdbDirectory global, startups;
	InitdbStartupObject *objects = palloc0(128 * sizeof(*objects));
	pg_cryptohash_ctx *hash;
	char name[112], hex[65];

	if (registration == NULL)
		creation_legacy_format_current(origins);
	header->file_txn_seq = 1;
	header->format_version = 3;
	header->system_identifier = config->identity.system_identifier;
	memcpy(header->storage_uuid, config->identity.storage_uuid, 16);
	memcpy(header->authority_uuid, config->identity.authority_uuid, 16);
	header->activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	header->lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
	header->migration_prepare_generation = 1;
	header->created_at_usec = header->published_at_usec = incarnation;
	/* Describes compiled persistent formats, not online activation/admission. */
	header->target_feature_bitmap
		= PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1 & ~PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;
	shared->database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	shared->database_incarnation = config->identity.database_incarnation;
	shared->formation_seq = 1;
	memcpy(shared->configured, config->identity.configured, sizeof(shared->configured));
	shared->config_generation = shared->control_image_generation
		= shared->catalog_manifest_generation = 1;
	memcpy(shared->config_sha256, config->sha256, 32);
	memcpy(shared->control_image_sha256, common->control_sha256, 32);
	memcpy(shared->catalog_manifest_sha256, common->catalog_sha256, 32);
	hash = creation_hash_begin(origins_domain, sizeof(origins_domain));
	creation_hash_number(hash, shared->configured[0], 8);
	creation_hash_number(hash, shared->configured[1], 8);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbOrigin *origin = &origins[node];
		if (!(shared->configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(origin, node + 1, config->identity.system_identifier, false);
		creation_side_current(&roots[1], origin, node);
		if (!cluster_initdb_tree_read(origin->wal.fd, false, &origin->wal_observed)
			|| !pg_strong_random(ids[node], 16))
			refuse("cannot observe original native source");
		ids[node][6] = (ids[node][6] & 0x0f) | 0x40;
		ids[node][8] = (ids[node][8] & 0x3f) | 0x80;
		sources->present[node] = true;
		sources->records[node] = origin->input.snapshot;
		sources->refs[node] = origin->input.refs;
		sources->publisher_node[node] = node;
		sources->publisher_incarnation[node] = origin->input.publisher_incarnation;
		shared->global_scn_high_water
			= Max(shared->global_scn_high_water, origin->checkpoint.checkpoint_scn);
		creation_hash_number(hash, node, 4);
		creation_hash_feed(hash, origin->control_sha256, 32);
		creation_hash_feed(hash, origin->wal_observed.content, 32);
		creation_hash_feed(hash, origin->input.refs.claim_sha256, 32);
		creation_hash_feed(hash, origin->input.refs.anchor_sha256, 32);
		creation_storage_current(origin);
		creation_hash_feed(hash, &origin->storage_contract, sizeof(origin->storage_contract));
	}
	creation_hash_finish(hash, header->source_wal_state_sha256);
	if (!cluster_initdb_tree_read(roots[1].fd, false, &data)
		|| !cluster_initdb_tree_read(roots[3].fd, false, &undo))
		refuse("cannot observe original common trees");
	hash = creation_hash_begin(cohort_domain, sizeof(cohort_domain));
	creation_hash_number(hash, header->system_identifier, 8);
	creation_hash_feed(hash, header->storage_uuid, 16);
	creation_hash_feed(hash, header->authority_uuid, 16);
	creation_hash_number(hash, shared->database_incarnation, 8);
	creation_hash_number(hash, shared->configured[0], 8);
	creation_hash_number(hash, shared->configured[1], 8);
	creation_hash_number(hash, header->format_version, 2);
	creation_hash_number(hash, CLUSTER_CONTROL_ROOT_FORMAT_CREATION_FLAGS_V1, 8);
	creation_hash_number(hash, header->target_feature_bitmap, 8);
	creation_hash_number(hash, shared->config_generation, 8);
	creation_hash_feed(hash, shared->config_sha256, 32);
	creation_hash_number(hash, shared->control_image_generation, 8);
	creation_hash_feed(hash, shared->control_image_sha256, 32);
	creation_hash_number(hash, shared->catalog_manifest_generation, 8);
	creation_hash_feed(hash, shared->catalog_manifest_sha256, 32);
	creation_hash_number(hash, shared->global_scn_high_water, 8);
	creation_hash_feed(hash, header->source_wal_state_sha256, 32);
	creation_hash_feed(hash, data.content, 32);
	creation_hash_feed(hash, undo.content, 32);
	creation_hash_finish(hash, header->migration_round_sha256);
	if (cluster_control_root_v3_initialized_inputs(sources, ids, request->segment_size, root,
												   inputs)
		!= 0)
		refuse("original cohort cannot encode complete initialized inputs");
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		PgracControlBinding binding = { 0 };
		if (!root->present[node])
			continue;
		binding.system_identifier = header->system_identifier;
		binding.database_incarnation = shared->database_incarnation;
		memcpy(binding.storage_uuid, header->storage_uuid, 16);
		memcpy(binding.authority_uuid, header->authority_uuid, 16);
		binding.node_id = node;
		binding.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
		binding.migration_prepare_generation = 1;
		memcpy(binding.migration_round_sha256, header->migration_round_sha256, 32);
		memcpy(binding.source_wal_state_sha256, header->source_wal_state_sha256, 32);
		if (!pgrac_control_binding_encode(&binding, bindings[node], PGRAC_CONTROL_BINDING_BYTES))
			refuse("cannot encode original local binding");
	}
	creation_sources_current(request, config, roots, origins, &data, &undo, false);
	open_original_child(&roots[1], "global", &global);
	create_child(&global, "wal_startup", &startups);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbDirectory staging;
		InitdbStartupObject *object = &objects[node];
		if (!root->present[node])
			continue;
		snprintf(name, sizeof(name), "thread_%u", node + 1);
		create_child(&startups, name, &object->directory);
		create_child(&object->directory, ".staging", &staging);
		for (unsigned j = 0; j < 32; j++)
			snprintf(hex + j * 2, 3, "%02x", root->startup[node].sha256[j]);
		snprintf(object->name, sizeof(object->name), "startup_1-%s.bin", hex);
		if (!cluster_initdb_object_write_observed(object->directory.fd, object->name, inputs[node],
												  CLUSTER_WAL_STARTUP_BYTES, &object->identity)
			|| fsync(staging.fd) != 0 || fsync(object->directory.fd) != 0 || close(staging.fd) != 0)
			refuse("cannot persist original startup input");
	}
	if (fsync(startups.fd) != 0 || fsync(global.fd) != 0)
		refuse("cannot persist original startup namespace");
	creation_root_publish(request, config, roots, origins, &data, &undo, &global, &startups,
						  objects, root, inputs, registration, registry_budget);
	directory_current(&global);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
		if (root->present[node] && close(objects[node].directory.fd) != 0)
			refuse("cannot close original startup directory");
	if (close(startups.fd) != 0 || close(global.fd) != 0)
		refuse("cannot close original ROOT directory");
	request_current(request, config);
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbDirectory local_global;
		if (!root->present[node])
			continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		creation_side_current(&roots[1], &origins[node], node);
		open_original_child(&origins[node].data, "global", &local_global);
		if (!cluster_initdb_object_write_new(local_global.fd, PGRAC_CONTROL_BINDING_NAME,
											 bindings[node], PGRAC_CONTROL_BINDING_BYTES)
			|| close(local_global.fd) != 0)
			refuse("cannot persist original local binding");
	}
	pfree(objects);
	pfree(bindings);
	pfree(inputs);
	pfree(root);
	pfree(sources);
}

void
ClusterInitdbCohortMain(int argc, char **argv)
{
	PgracInitdbCohortContext request;
	ClusterInitdbConfig *config;
	const ClusterSharedConfigRef *ref;
	InitdbDirectory roots[4];
	InitdbOrigin *origins;
	ClusterInitdbCommon common;
	char initdb[MAXPGPATH], generation[64], name[32];
	uint64 incarnation;
	uint8 branch[16];
	InitdbRegistration *registration;
	const Size registry_budget = 1024 * 1024;
	const char *paths[4];

	if (argc != 2 || IsUnderPostmaster)
		refuse("only the original frontend may dispatch creation");
	read_context(&request);
	InitStandaloneProcess(argv[0]);
	install_creation_signals();
	InitializeGUCOptions();
	process_cluster_gucs();
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "original cohort creation");
	if (find_other_exec(argv[0], "initdb", "initdb (PostgreSQL) " PG_VERSION "\n", initdb) < 0)
		refuse("matching native initdb executable is missing");
	config = cluster_initdb_config_preflight(&request.config);
	ref = cluster_initdb_config_reference(config);
	request_current(&request, ref);
	paths[0] = request.cache_root;
	for (int i = 1; i < 4; i++)
		paths[i] = cluster_initdb_config_path(config, i - 1);
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < i; j++) {
			size_t a = strlen(paths[i]), b = strlen(paths[j]);
			if (strcmp(paths[i], paths[j]) == 0
				|| (a > b && strncmp(paths[i], paths[j], b) == 0 && paths[i][b] == '/')
				|| (b > a && strncmp(paths[j], paths[i], a) == 0 && paths[j][a] == '/'))
				refuse("new DATA, WAL, UNDO and local cache roots must not overlap");
		}
		preflight_directory(paths[i], &roots[i]);
	}
	incarnation = (uint64)GetCurrentTimestamp();
	if (incarnation == 0 || incarnation > PG_INT64_MAX)
		refuse("invalid original creation identity");
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT, incarnation);
	umask(pg_mode_mask);
	for (int i = 0; i < 4; i++)
		create_directory(&roots[i]);
	origins = palloc0(sizeof(*origins) * CLUSTER_CONTROL_ROOT_RECORD_COUNT);
	registration = palloc0(sizeof(*registration));
	if (!pg_strong_random(branch, sizeof(branch)))
		refuse("cannot allocate original redo branch");
	branch[6] = (branch[6] & 0x0f) | 0x40;
	branch[8] = (branch[8] & 0x3f) | 0x80;
	/* Create and hold the entire requested namespace before starting a child. */
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbOrigin *origin = &origins[node];
		if (!(ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		snprintf(name, sizeof(name), "node_%d", node);
		create_child(&roots[0], name, &origin->data);
		snprintf(name, sizeof(name), "thread_%d", node + 1);
		create_child(&roots[2], name, &origin->thread);
		create_child(&origin->thread, generation, &origin->wal);
	}
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		PgracInitdbRecoveryContext child;
		if (!(ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		if (!cluster_initdb_cohort_prepare_v2(ref, &request.config, branch, node,
											  origins[node].data.fd, roots[1].fd, &child))
			refuse("cannot bind complete original BOOT inventory");
		run_origin(initdb, &request, ref, roots[1].path, &origins[node], node, &child);
		for (int i = 0; i < 4; i++)
			directory_current(&roots[i]);
	}
	request_current(&request, ref);
	create_peer_side(&roots[1], origins, ref);
	create_origin_objects(&roots[1], origins, ref, incarnation);
	create_storage_contracts(origins, ref);
	create_common_objects(&roots[1], origins, ref, &common);
	create_undo_directories(&roots[1], ref);
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		InitdbOrigin *origin = &origins[node];
		if (!(ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))))
			continue;
		control_read(origin, node + 1, ref->identity.system_identifier, false);
		route_original_side(&roots[1], origin, node);
		if (fsync(origin->wal.fd) != 0 || fsync(origin->thread.fd) != 0
			|| fsync(origin->data.fd) != 0)
			refuse("cannot persist original writer directories");
	}
	create_catalog_objects(&roots[1], origins, ref);
	for (int i = 0; i < 4; i++) {
		directory_current(&roots[i]);
		if (fsync(roots[i].fd) != 0 || fsync(roots[i].parent) != 0)
			refuse("cannot persist original cohort directory entries");
	}
	request_current(&request, ref);
	if (!creation_registration_build(ref, origins, branch, registration, registry_budget))
		refuse("cannot build complete original BOOT registration");
	create_root_objects(&request, roots, origins, ref, &common, incarnation,
						&registration->registry, registry_budget);
	printf("Shared cohort created; shared startup remains closed.\n");
	exit(0);
}
