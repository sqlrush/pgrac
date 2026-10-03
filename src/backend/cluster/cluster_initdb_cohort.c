/* Synchronous original-creation owner, outside normal database startup.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "cluster/cluster_initdb_cohort.h"
#include "cluster/cluster_initdb_config.h"
#include "cluster/cluster_wal_thread.h"
#include "common/file_perm.h"
#include "common/pgrac_initdb_cohort.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "cluster_initdb_origin_private.h"
#include "cluster_initdb_common_private.h"
#include "../../bin/initdb/pgrac_wal.h"
#include "../../bin/initdb/pgrac_side.h"

typedef struct InitdbDirectory
{
	int fd;
	int parent;
	char path[MAXPGPATH];
	char name[MAXPGPATH];
	struct stat identity;
	struct stat parent_identity;
} InitdbDirectory;

typedef struct InitdbOrigin
{
	InitdbDirectory data;
	InitdbDirectory thread;
	InitdbDirectory wal;
	ControlFileData control;
	PgracInitdbWalObservation checkpoint;
	ClusterWalHistoryRecord input;
} InitdbOrigin;

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
		|| (flags = fcntl(descriptor, F_GETFL)) < 0
		|| (flags & O_ACCMODE) != O_RDONLY
		|| fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0)
		refuse("invalid original initdb pipe");
	unsetenv(PGRAC_INITDB_COHORT_ENV);
	memset(out, 0, sizeof(*out));
	while (!eof)
	{
		struct pollfd event = {descriptor, POLLIN, 0};
		ssize_t n;
		if (TimestampDifferenceExceeds(started, GetCurrentTimestamp(), 10000))
			refuse("original initdb request was not closed");
		if (poll(&event, 1, 100) < 0)
		{
			if (errno == EINTR) continue;
			refuse("cannot read original initdb request");
		}
		n = read(descriptor, used < sizeof(*out) ? (char *)out + used : &extra,
				 used < sizeof(*out) ? sizeof(*out) - used : 1);
		if (n < 0 && (errno == EINTR || errno == EAGAIN)) continue;
		if (n < 0 || (n > 0 && used == sizeof(*out)))
			refuse("invalid original initdb request length");
		if (n == 0) eof = true;
		else used += n;
	}
	if (close(descriptor) != 0 || used != sizeof(*out)
		|| out->magic != PGRAC_INITDB_COHORT_MAGIC
		|| out->creator_pid != (uint64)getppid()
		|| !IsValidWalSegSize(out->segment_size)
		|| !terminated(out->cache_root, sizeof(out->cache_root))
		|| !terminated(out->config_path, sizeof(out->config_path))
		|| !terminated(out->username, sizeof(out->username))
		|| !terminated(out->encoding, sizeof(out->encoding))
		|| !terminated(out->auth_local, sizeof(out->auth_local))
		|| !terminated(out->auth_host, sizeof(out->auth_host))
		|| out->config.fd < 3 || fcntl(out->config.fd, F_SETFD, FD_CLOEXEC) != 0
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
	if (path[0] != '/' || strlen(path) >= MAXPGPATH || strlcpy(parent, path, sizeof(parent)) >= sizeof(parent))
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
	if (creation_cancelled) refuse("original creation was cancelled");
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
	if (snprintf(child->path, sizeof(child->path), "%s/%s", parent->path, name) >= sizeof(child->path))
		refuse("new child path is too long");
	create_directory(child);
}

static void
request_current(const PgracInitdbCohortContext *request, const ClusterSharedConfigRef *expected)
{
	struct stat named;
	ClusterInitdbConfig *observed;
	char *canonical = realpath(request->config_path, NULL);
	if (creation_cancelled || getppid() != request->creator_pid || canonical == NULL
		|| strcmp(canonical, request->config_path) != 0
		|| lstat(request->config_path, &named) != 0 || !S_ISREG(named.st_mode)
		|| named.st_dev != request->config.device || named.st_ino != request->config.inode)
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
	if (fd < 0 || fstat(fd, &file) != 0 || !S_ISREG(file.st_mode)
		|| file.st_uid != geteuid() || file.st_nlink != 1 || (file.st_mode & 0022) != 0
		|| file.st_size != PG_CONTROL_FILE_SIZE)
		refuse("native control file is invalid");
	do { n = pread(fd, bytes, sizeof(bytes), 0); } while (n < 0 && errno == EINTR);
	if (n != PG_CONTROL_FILE_SIZE || fstatat(global, "pg_control", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| file.st_dev != named.st_dev || file.st_ino != named.st_ino
		|| file.st_size != named.st_size || file.st_mode != named.st_mode || named.st_nlink != 1
		|| close(fd) != 0 || fstatat(origin->data.fd, "global", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_directory(&held, &named) || close(global) != 0)
		refuse("native control changed during readback");
	memcpy(&control, bytes, sizeof(control));
	if (control.system_identifier != system_identifier
		|| !pgrac_initdb_wal_observe(origin->wal.fd, &control, thread, &observed)
		|| (!first && (memcmp(&control, &origin->control, sizeof(control)) != 0
			|| observed.checkpoint_start != origin->checkpoint.checkpoint_start
			|| observed.checkpoint_end != origin->checkpoint.checkpoint_end
			|| observed.checkpoint_crc != origin->checkpoint.checkpoint_crc)))
		refuse("native shutdown checkpoint differs from its original completed child");
	origin->control = control;
	origin->checkpoint = observed;
	directory_current(&origin->data);
	directory_current(&origin->wal);
}

static void
run_child(char *const *args, uint64 creator_pid)
{
	int status;
	pid_t child, waited;
	bool cancelled = false;
	if (creation_cancelled || getppid() != creator_pid)
		refuse("original creation was cancelled");
	fflush(NULL);
	child = fork();
	if (child < 0) refuse("cannot start an original native initializer");
	if (child == 0)
	{
		sigset_t empty;
		if (setpgid(0, 0) != 0) _exit(127);
		pqsignal(SIGINT, SIG_DFL);
		pqsignal(SIGTERM, SIG_DFL);
		sigemptyset(&empty);
		sigprocmask(SIG_SETMASK, &empty, NULL);
		execv(args[0], args);
		_exit(127);
	}
	/* The unreaped child owns this new group. No unrelated process may be
	 * addressed through an old PID or a discovered server process list. */
	if (setpgid(child, child) != 0 && errno != EACCES)
	{
		kill(child, SIGKILL);
		do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
		refuse("cannot bind the original native child group");
	}
	for (;;)
	{
		if (creation_cancelled || getppid() != creator_pid)
		{
			/* No ROOT exists. Stop every still-owned native writer before
			 * reporting cancellation; leave its partial files unadoptable. */
			cancelled = true;
			(void)kill(-child, SIGKILL);
			do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
			break;
		}
		waited = waitpid(child, &status, WNOHANG);
		if (waited == child || (waited < 0 && errno != EINTR)) break;
		pg_usleep(20000L);
	}
	if (cancelled || creation_cancelled || getppid() != creator_pid)
		refuse("original creation was cancelled");
	if (waited != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0)
		refuse("an original native initializer did not complete successfully");
}

static void
run_origin(const char *initdb, const PgracInitdbCohortContext *request,
		   const ClusterSharedConfigRef *config, const char *shared, InitdbOrigin *origin, int node)
{
	char thread[64], system[96], storage[96], incarnation[96], base[MAXPGPATH + 64];
	char configuration[MAXPGPATH + 64], local[96], host[96], segment[64], uuid[33];
	char *args[24];
	int count = 0;
	static const char hex[] = "0123456789abcdef";

	request_current(request, config);
	for (int i = 0; i < 16; i++)
	{
		uuid[2*i] = hex[config->identity.storage_uuid[i] >> 4];
		uuid[2*i+1] = hex[config->identity.storage_uuid[i] & 15];
	}
	uuid[32] = '\0';
	snprintf(thread, sizeof(thread), "--pgrac-initdb-thread=%d", node + 1);
	snprintf(system, sizeof(system), "--pgrac-initdb-system-identifier=" UINT64_FORMAT, config->identity.system_identifier);
	snprintf(storage, sizeof(storage), "--pgrac-initdb-storage-uuid=%s", uuid);
	snprintf(incarnation, sizeof(incarnation), "--pgrac-initdb-database-incarnation=" UINT64_FORMAT, config->identity.database_incarnation);
	snprintf(base, sizeof(base), "--pgrac-initdb-shared-base=%s", shared);
	snprintf(configuration, sizeof(configuration), "--pgrac-initdb-shared-config=%s", request->config_path);
	snprintf(local, sizeof(local), "--auth-local=%s", request->auth_local);
	snprintf(host, sizeof(host), "--auth-host=%s", request->auth_host);
	snprintf(segment, sizeof(segment), "--wal-segsize=%u", request->segment_size / (1024 * 1024));
	args[count++] = (char *)initdb;
	args[count++] = "-D"; args[count++] = origin->data.path;
	args[count++] = "-X"; args[count++] = origin->wal.path;
	args[count++] = "-U"; args[count++] = (char *)request->username;
	args[count++] = "-E"; args[count++] = (char *)request->encoding;
	args[count++] = "--no-locale"; args[count++] = "-k";
	args[count++] = "--no-instructions"; args[count++] = "--no-clean";
	args[count++] = local; args[count++] = host; args[count++] = segment;
	args[count++] = thread; args[count++] = system;
	if (node == 0)
	{
		args[count++] = base; args[count++] = storage;
		args[count++] = incarnation; args[count++] = configuration;
	}
	args[count] = NULL;
	Assert(count < lengthof(args));
	run_child(args, request->creator_pid);
	request_current(request, config);
	control_read(origin, node + 1, config->identity.system_identifier, true);
}

static void
create_origin_objects(const InitdbDirectory *shared, InitdbOrigin *origins,
					  const ClusterSharedConfigRef *config, uint64 incarnation)
{
	InitdbDirectory global = {0}, images;
	char name[64];
	struct stat st;

	/* A preexisting claim cannot be promoted into this creation operation. */
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		if (fstatat(origins[node].wal.fd, CLUSTER_WAL_THREAD_CLAIM_FILENAME,
					&st, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
			refuse("an original writer claim already exists");
	}
	/* global was made by the actual founder child inside our held new DATA. */
	directory_current(shared);
	global.parent = shared->fd;
	global.parent_identity = shared->identity;
	strlcpy(global.name, "global", sizeof(global.name));
	if (snprintf(global.path, sizeof(global.path), "%s/global", shared->path) >= sizeof(global.path))
		refuse("original global path is too long");
	global.fd = openat(shared->fd, global.name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (global.fd < 0 || fstat(global.fd, &global.identity) != 0
		|| !owned_directory(&global.identity))
		refuse("original global directory is invalid");
	directory_current(&global);
	create_child(&global, "anchor_images", &images);
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		InitdbDirectory thread, generation, staging;
		InitdbOrigin *origin = &origins[node];
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
		snprintf(name, sizeof(name), "thread_%d", node + 1);
		create_child(&images, name, &thread);
		snprintf(name, sizeof(name), "generation_" UINT64_FORMAT, incarnation);
		create_child(&thread, name, &generation);
		create_child(&generation, ".staging", &staging);
		if (!cluster_initdb_origin_create(config, node, incarnation, origin->wal.fd,
										generation.fd, &origin->control, &origin->input))
			refuse("cannot persist original writer claim and native anchor");
		control_read(origin, node + 1, config->identity.system_identifier, false);
		directory_current(&staging);
		directory_current(&generation);
		directory_current(&thread);
		if (fsync(staging.fd) != 0 || fsync(generation.fd) != 0 || fsync(thread.fd) != 0
			|| close(staging.fd) != 0 || close(generation.fd) != 0 || close(thread.fd) != 0)
			refuse("cannot persist original anchor directories");
	}
	directory_current(&images);
	directory_current(&global);
	if (fsync(images.fd) != 0 || fsync(global.fd) != 0 || close(images.fd) != 0 || close(global.fd) != 0)
		refuse("cannot complete original anchor namespace");
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
	for (int node = 1; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		InitdbOrigin *origin = &origins[node];
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
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
					  const ClusterSharedConfigRef *config)
{
	const ControlFileData *sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT] = {0};
	ClusterInitdbCommon common;
	InitdbDirectory global = {0};
	const char *directories[] = {"control_images", "catalog_checkpoints"};
	char hex[65], name[96];

	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		if (!(config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
		control_read(&origins[node], node + 1, config->identity.system_identifier, false);
		sources[node] = &origins[node].control;
	}
	if (!cluster_initdb_common_build(config, sources, &common))
		refuse("original cohort common control fields disagree");
	directory_current(shared);
	global.parent = shared->fd;
	global.parent_identity = shared->identity;
	strlcpy(global.name, "global", sizeof(global.name));
	if (snprintf(global.path, sizeof(global.path), "%s/global", shared->path) >= sizeof(global.path))
		refuse("original global path is too long");
	global.fd = openat(shared->fd, global.name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (global.fd < 0 || fstat(global.fd, &global.identity) != 0 || !owned_directory(&global.identity))
		refuse("original global directory is invalid");
	directory_current(&global);
	for (unsigned i = 0; i < lengthof(directories); i++)
	{
		InitdbDirectory objects, staging;
		const uint8 *hash = i == 0 ? common.control_sha256 : common.catalog_sha256;
		const uint8 *bytes = i == 0 ? common.control : common.catalog;
		Size length = i == 0 ? sizeof(common.control) : common.catalog_length;
		create_child(&global, directories[i], &objects);
		create_child(&objects, ".staging", &staging);
		for (unsigned j = 0; j < 32; j++) snprintf(hex + j * 2, 3, "%02x", hash[j]);
		snprintf(name, sizeof(name), "1-%s.%s", hex, i == 0 ? "bin" : "json");
		if (!cluster_initdb_object_write_new(objects.fd, name, bytes, length))
			refuse("cannot persist original common control/catalog object");
		directory_current(&objects);
		directory_current(&staging);
		if (fsync(staging.fd) != 0 || fsync(objects.fd) != 0
			|| close(staging.fd) != 0 || close(objects.fd) != 0)
			refuse("cannot persist original common object directories");
	}
	directory_current(&global);
	if (fsync(global.fd) != 0 || close(global.fd) != 0)
		refuse("cannot complete original common objects");
}

void
ClusterInitdbCohortMain(int argc, char **argv)
{
	PgracInitdbCohortContext request;
	ClusterInitdbConfig *config;
	const ClusterSharedConfigRef *ref;
	InitdbDirectory roots[4];
	InitdbOrigin *origins;
	char initdb[MAXPGPATH], generation[64], name[32];
	uint64 incarnation;
	const char *paths[4];

	if (argc != 2 || IsUnderPostmaster) refuse("only the original frontend may dispatch creation");
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
	for (int i = 1; i < 4; i++) paths[i] = cluster_initdb_config_path(config, i - 1);
	for (int i = 0; i < 4; i++)
	{
		for (int j = 0; j < i; j++)
		{
			size_t a = strlen(paths[i]), b = strlen(paths[j]);
			if (strcmp(paths[i], paths[j]) == 0
				|| (a > b && strncmp(paths[i], paths[j], b) == 0 && paths[i][b] == '/')
				|| (b > a && strncmp(paths[j], paths[i], a) == 0 && paths[j][a] == '/'))
				refuse("new DATA, WAL, UNDO and local cache roots must not overlap");
		}
		preflight_directory(paths[i], &roots[i]);
	}
	incarnation = (uint64)GetCurrentTimestamp();
	if (incarnation == 0 || incarnation > PG_INT64_MAX) refuse("invalid original creation identity");
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT, incarnation);
	umask(pg_mode_mask);
	for (int i = 0; i < 4; i++) create_directory(&roots[i]);
	origins = palloc0(sizeof(*origins) * CLUSTER_CONTROL_ROOT_RECORD_COUNT);
	/* Create and hold the entire requested namespace before starting a child. */
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		InitdbOrigin *origin = &origins[node];
		if (!(ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
		snprintf(name, sizeof(name), "node_%d", node);
		create_child(&roots[0], name, &origin->data);
		snprintf(name, sizeof(name), "thread_%d", node + 1);
		create_child(&roots[2], name, &origin->thread);
		create_child(&origin->thread, generation, &origin->wal);
	}
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		if (!(ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
		run_origin(initdb, &request, ref, roots[1].path, &origins[node], node);
		for (int i = 0; i < 4; i++) directory_current(&roots[i]);
	}
	request_current(&request, ref);
	create_peer_side(&roots[1], origins, ref);
	create_origin_objects(&roots[1], origins, ref, incarnation);
	create_common_objects(&roots[1], origins, ref);
	for (int node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
	{
		InitdbOrigin *origin = &origins[node];
		if (!(ref->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64)))) continue;
		control_read(origin, node + 1, ref->identity.system_identifier, false);
		if (fsync(origin->wal.fd) != 0 || fsync(origin->thread.fd) != 0 || fsync(origin->data.fd) != 0)
			refuse("cannot persist original writer directories");
	}
	for (int i = 0; i < 4; i++)
	{
		directory_current(&roots[i]);
		if (fsync(roots[i].fd) != 0 || fsync(roots[i].parent) != 0)
			refuse("cannot persist original cohort directory entries");
	}
	request_current(&request, ref);
	/* No re-entry may adopt these prepared directories. ROOT publication is
	 * reserved for this still-live owner after the remaining objects exist. */
	printf("Shared cohort prepared; shared startup authority is not published.\n");
	exit(0);
}
