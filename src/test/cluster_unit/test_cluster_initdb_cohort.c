/* Actual owned native-child lifecycle; no simulated successful creation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <setjmp.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static jmp_buf refused;
static bool expecting;
static char executable[MAXPGPATH];
static bool side_sync_fault, side_link_fault;
static int replaced_link_parent = -1;
static struct stat replaced_link_identity;
static int
side_test_fstatat(int fd, const char *name, struct stat *out, int flags)
{
	int result = fstatat(fd, name, out, flags);
	/* A deleted symlink's inode may immediately be reused. Only the pathname
	 * observation is repeated; fstat on a held original still sees its unlink. */
	if (result == 0 && fd == replaced_link_parent && flags == AT_SYMLINK_NOFOLLOW
		&& strcmp(name, "pg_xact") == 0) {
		out->st_dev = replaced_link_identity.st_dev;
		out->st_ino = replaced_link_identity.st_ino;
	}
	return result;
}
static int
side_test_fsync(int fd)
{
	if (side_sync_fault) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
static int
side_test_symlinkat(const char *path, int fd, const char *name)
{
	if (side_link_fault) {
		errno = EIO;
		return -1;
	}
	return symlinkat(path, fd, name);
}
#define fsync side_test_fsync
#define symlinkat side_test_symlinkat
#define fstatat side_test_fstatat
#include "../../backend/cluster/cluster_initdb_cohort.c"
#undef fsync
#undef symlinkat
#undef fstatat

bool
errstart(int level, const char *domain)
{
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return level >= ERROR;
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *format, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	if (expecting)
		longjmp(refused, 1);
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static void
completed(void)
{
	char *args[] = { executable, "child", "success", NULL };
	run_child(args, getppid());
	UT_ASSERT(waitpid(-1, NULL, WNOHANG) == -1 && errno == ECHILD);
}

static void
unsuccessful(void)
{
	char *args[] = { executable, "child", "failure", NULL };
	expecting = true;
	if (setjmp(refused) == 0) {
		run_child(args, getppid());
		UT_ASSERT(false);
	}
	expecting = false;
	UT_ASSERT(waitpid(-1, NULL, WNOHANG) == -1 && errno == ECHILD);
}

static void
cancel_owns_and_reaps_the_native_group(void)
{
	int pipefd[2];
	char fdtext[24];
	char *args[] = { executable, "child", "cancel", fdtext, NULL };
	sigset_t blocked, current;
	pid_t direct, descendant;
	char extra;
	UT_ASSERT(pipe(pipefd) == 0);
	snprintf(fdtext, sizeof(fdtext), "%d", pipefd[1]);
	sigemptyset(&blocked);
	sigaddset(&blocked, SIGTERM);
	UT_ASSERT(sigprocmask(SIG_BLOCK, &blocked, NULL) == 0);
	install_creation_signals();
	UT_ASSERT(sigprocmask(SIG_SETMASK, NULL, &current) == 0);
	UT_ASSERT(sigismember(&current, SIGTERM) == 0);
	expecting = true;
	if (setjmp(refused) == 0) {
		run_child(args, getppid());
		UT_ASSERT(false);
	}
	expecting = false;
	UT_ASSERT(close(pipefd[1]) == 0);
	UT_ASSERT(read(pipefd[0], &direct, sizeof(direct)) == sizeof(direct));
	UT_ASSERT(read(pipefd[0], &descendant, sizeof(descendant)) == sizeof(descendant));
	UT_ASSERT(direct > 0 && descendant > 0 && direct != descendant);
	/* EOF proves both child writers have gone, including the descendant. */
	UT_ASSERT(read(pipefd[0], &extra, 1) == 0);
	UT_ASSERT(waitpid(direct, NULL, WNOHANG) == -1 && errno == ECHILD);
	UT_ASSERT(close(pipefd[0]) == 0);
}

static void
derived_inputs_must_survive_until_primary_publication(void)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		char temp[] = "/tmp/pgrac-derived-XXXXXX", path[MAXPGPATH];
		char *canonical;
		InitdbDirectory base, global, startups;
		InitdbStartupObject *objects = calloc(128, sizeof(*objects));
		ControlRootImage *root = calloc(1, sizeof(*root));
		uint8(*inputs)[CLUSTER_WAL_STARTUP_BYTES] = calloc(128, CLUSTER_WAL_STARTUP_BYTES);
		struct stat backup;
		int fd;
		UT_ASSERT(mkdtemp(temp) != NULL);
		canonical = realpath(temp, NULL);
		UT_ASSERT(canonical != NULL && objects != NULL && root != NULL && inputs != NULL);
		snprintf(path, sizeof(path), "%s/new", canonical);
		preflight_directory(path, &base);
		create_directory(&base);
		create_child(&base, "global", &global);
		create_child(&global, "wal_startup", &startups);
		create_child(&startups, "thread_1", &objects[0].directory);
		root->present[0] = true;
		memset(inputs[0], 0x6a, CLUSTER_WAL_STARTUP_BYTES);
		strlcpy(objects[0].name, "input", sizeof(objects[0].name));
		fd = openat(objects[0].directory.fd, "input", O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0
				  && write(fd, inputs[0], CLUSTER_WAL_STARTUP_BYTES) == CLUSTER_WAL_STARTUP_BYTES);
		UT_ASSERT(fsync(fd) == 0 && fstat(fd, &objects[0].identity) == 0 && close(fd) == 0);
		fd = openat(global.fd, "pgrac_control_root.bak", O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, root->bytes, sizeof(root->bytes)) == sizeof(root->bytes));
		UT_ASSERT(fsync(fd) == 0 && fstat(fd, &backup) == 0 && close(fd) == 0);
		creation_derived_current(&global, &startups, objects, root, inputs, &backup);
		if (fault == 0 || fault == 1 || fault == 4) {
			fd = openat(fault == 4 ? global.fd : objects[0].directory.fd,
						fault == 4 ? "pgrac_control_root.bak" : "input", O_WRONLY);
			UT_ASSERT(fd >= 0);
			if (fault == 1)
				UT_ASSERT(ftruncate(fd, 31) == 0);
			else
				UT_ASSERT(pwrite(fd, "X", 1, 17) == 1);
			UT_ASSERT(close(fd) == 0);
		} else if (fault == 3) {
			UT_ASSERT(renameat(startups.fd, "thread_1", startups.fd, "old") == 0);
			UT_ASSERT(mkdirat(startups.fd, "thread_1", 0700) == 0);
		} else {
			int dir = fault == 2 ? objects[0].directory.fd : global.fd;
			const char *name = fault == 2 ? "input" : "pgrac_control_root.bak";
			const uint8 *bytes = fault == 2 ? inputs[0] : root->bytes;
			Size length = fault == 2 ? CLUSTER_WAL_STARTUP_BYTES : sizeof(root->bytes);
			UT_ASSERT(renameat(dir, name, dir, "old") == 0);
			fd = openat(dir, name, O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0 && write(fd, bytes, length) == length && close(fd) == 0);
		}
		expecting = true;
		if (setjmp(refused) == 0) {
			creation_derived_current(&global, &startups, objects, root, inputs, &backup);
			UT_ASSERT(false);
		}
		expecting = false;
		UT_ASSERT(faccessat(global.fd, "pgrac_control_root", F_OK, 0) < 0 && errno == ENOENT);
		UT_ASSERT(unlinkat(objects[0].directory.fd, "input", 0) == 0);
		if (fault == 2)
			UT_ASSERT(unlinkat(objects[0].directory.fd, "old", 0) == 0);
		UT_ASSERT(close(objects[0].directory.fd) == 0);
		UT_ASSERT(unlinkat(startups.fd, "thread_1", AT_REMOVEDIR) == 0);
		if (fault == 3)
			UT_ASSERT(unlinkat(startups.fd, "old", AT_REMOVEDIR) == 0);
		UT_ASSERT(close(startups.fd) == 0 && unlinkat(global.fd, "wal_startup", AT_REMOVEDIR) == 0);
		UT_ASSERT(unlinkat(global.fd, "pgrac_control_root.bak", 0) == 0);
		if (fault == 5)
			UT_ASSERT(unlinkat(global.fd, "old", 0) == 0);
		UT_ASSERT(close(global.fd) == 0 && unlinkat(base.fd, "global", AT_REMOVEDIR) == 0);
		UT_ASSERT(close(base.fd) == 0 && unlinkat(base.parent, base.name, AT_REMOVEDIR) == 0);
		UT_ASSERT(close(base.parent) == 0 && rmdir(canonical) == 0);
		free(canonical);
		free(inputs);
		free(root);
		free(objects);
	}
}

static void
side_test_put(int parent, const char *name, char value)
{
	char bytes[8192];
	int fd = openat(parent, name, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	memset(bytes, value, sizeof(bytes));
	UT_ASSERT(fd >= 0 && write(fd, bytes, sizeof(bytes)) == sizeof(bytes) && close(fd) == 0);
}

/* Remove only this fixture's owned tree; never follow a routed symlink. */
static void
side_test_remove(int parent)
{
	DIR *stream = fdopendir(openat(parent, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW));
	struct dirent *entry;
	UT_ASSERT(stream != NULL);
	if (stream == NULL)
		return;
	while ((entry = readdir(stream)) != NULL) {
		struct stat st;
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		UT_ASSERT(fstatat(parent, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0);
		if (S_ISDIR(st.st_mode)) {
			int child = openat(parent, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
			UT_ASSERT(child >= 0);
			side_test_remove(child);
			UT_ASSERT(close(child) == 0 && unlinkat(parent, entry->d_name, AT_REMOVEDIR) == 0);
		} else
			UT_ASSERT(unlinkat(parent, entry->d_name, 0) == 0);
	}
	UT_ASSERT(closedir(stream) == 0);
}

static void
side_route_case(unsigned fault, unsigned node)
{
	char temp[] = "/tmp/pgrac-side-route-XXXXXX", path[MAXPGPATH], name[32];
	char *canonical;
	InitdbDirectory base, shared, native, target;
	InitdbOrigin origin = { 0 };
	pid_t child;
	int status;

	UT_ASSERT(mkdtemp(temp) != NULL);
	canonical = realpath(temp, NULL);
	UT_ASSERT(canonical != NULL);
	snprintf(path, sizeof(path), "%s/new", canonical);
	preflight_directory(path, &base);
	create_directory(&base);
	create_child(&base, "data", &origin.data);
	create_child(&base, "shared", &shared);
	create_child(&shared, "native_side", &native);
	snprintf(name, sizeof(name), "origin_%u", node);
	create_child(&native, name, &target);
	for (unsigned i = 0; i < lengthof(side_families); i++) {
		InitdbDirectory source_dir, target_dir;
		create_child(&origin.data, side_families[i], &source_dir);
		create_child(&target, side_families[i], &target_dir);
		if (i != 3) {
			side_test_put(source_dir.fd, "0000", 'a' + i);
			side_test_put(target_dir.fd, "0000", 'a' + i);
		}
		UT_ASSERT(close(source_dir.fd) == 0 && close(target_dir.fd) == 0);
	}
	/* FATAL deliberately terminates the original owner; the child isolates its
	 * process-owned descriptors exactly as production failure does. */
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		struct stat before, after;
		bool rejected = false;
		UT_ASSERT(fstatat(origin.data.fd, "pg_xact", &before, AT_SYMLINK_NOFOLLOW) == 0);
		if (fault == 1)
			side_test_put(target.fd, "pg_commit_ts/extra", 'x');
		if (fault == 2)
			side_test_put(target.fd, "pg_xact/0000", 'x');
		if (fault == 3) {
			UT_ASSERT(renameat(target.fd, "pg_xact", target.fd, "old") == 0);
			UT_ASSERT(symlinkat("old", target.fd, "pg_xact") == 0);
		}
		if (fault == 4)
			UT_ASSERT(mkdirat(origin.data.fd, INITDB_SIDE_ARCHIVE, 0700) == 0);
		if (fault == 5)
			side_sync_fault = true;
		if (fault == 6)
			side_link_fault = true;
		if ((fault >= 7 && fault <= 12) || fault == 14) {
			route_original_side(&shared, &origin, node);
			if (fault == 7 || fault == 8 || fault == 14) {
				UT_ASSERT(unlinkat(origin.data.fd, "pg_xact", 0) == 0);
				snprintf(path, sizeof(path), "%s/pg_xact", target.path);
				UT_ASSERT(symlinkat(fault == 8 ? "/tmp" : path, origin.data.fd, "pg_xact") == 0);
				if (fault == 14) {
					replaced_link_parent = origin.data.fd;
					replaced_link_identity = origin.side_links[0];
				}
			}
			if (fault == 9)
				side_test_put(origin.data.fd, INITDB_SIDE_ARCHIVE "/pg_xact/0000", 'x');
			if (fault == 10)
				side_test_put(target.fd, "pg_xact/0000", 'x');
			if (fault == 11) {
				UT_ASSERT(renameat(origin.data.fd, INITDB_SIDE_ARCHIVE, origin.data.fd, "old")
						  == 0);
				UT_ASSERT(mkdirat(origin.data.fd, INITDB_SIDE_ARCHIVE, 0700) == 0);
			}
			if (fault == 12) {
				UT_ASSERT(renameat(target.fd, "pg_xact", target.fd, "old") == 0);
				UT_ASSERT(mkdirat(target.fd, "pg_xact", 0700) == 0);
			}
		}
		if (fault == 13) {
			/* Even an empty destination may not be overwritten by the move. */
			UT_ASSERT(unlinkat(target.fd, "pg_xact/0000", 0) == 0);
			UT_ASSERT(fstatat(target.fd, "pg_xact", &after, AT_SYMLINK_NOFOLLOW) == 0);
			UT_ASSERT(move_original_directory(origin.data.fd, "pg_xact", target.fd) < 0);
			UT_ASSERT(fstatat(target.fd, "pg_xact", &before, AT_SYMLINK_NOFOLLOW) == 0);
			UT_ASSERT(same_directory(&before, &after));
		} else {
			expecting = fault != 0;
			if (setjmp(refused) == 0) {
				if (fault >= 7)
					creation_side_current(&shared, &origin, node);
				else
					route_original_side(&shared, &origin, node);
			} else
				rejected = true;
			expecting = false;
			UT_ASSERT(rejected == (fault != 0));
			if (fault == 0) {
				creation_side_current(&shared, &origin, node);
				UT_ASSERT(fstatat(origin.data.fd, INITDB_SIDE_ARCHIVE "/pg_xact", &after,
								  AT_SYMLINK_NOFOLLOW)
						  == 0);
				UT_ASSERT(same_directory(&before, &after));
				UT_ASSERT(origin.side_routed);
			} else if (fault <= 4) {
				UT_ASSERT(fstatat(origin.data.fd, "pg_xact", &after, AT_SYMLINK_NOFOLLOW) == 0);
				UT_ASSERT(same_directory(&before, &after));
			} else if (fault <= 6) {
				UT_ASSERT(fstatat(origin.data.fd, INITDB_SIDE_ARCHIVE "/pg_xact", &after,
								  AT_SYMLINK_NOFOLLOW)
						  == 0);
				UT_ASSERT(same_directory(&before, &after));
				UT_ASSERT(!origin.side_routed);
			}
		}
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	UT_ASSERT(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
	UT_ASSERT(close(target.fd) == 0 && close(native.fd) == 0 && close(shared.fd) == 0
			  && close(origin.data.fd) == 0);
	side_test_remove(base.fd);
	UT_ASSERT(close(base.fd) == 0 && unlinkat(base.parent, base.name, AT_REMOVEDIR) == 0);
	UT_ASSERT(close(base.parent) == 0 && rmdir(canonical) == 0);
	free(canonical);
}

static void
side_routes_retain_actual_original_directories(void)
{
	side_route_case(0, 0);
	side_route_case(0, 3);
}

static void
storage_contract_is_original_unverified_and_rechecked(void)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		char temp[] = "/tmp/pgrac-storage-origin-XXXXXX", path[MAXPGPATH];
		char *canonical;
		InitdbDirectory base, global;
		InitdbOrigin *origins = calloc(128, sizeof(*origins));
		ClusterSharedConfigRef config = { 0 };
		int fd;

		UT_ASSERT(mkdtemp(temp) != NULL);
		canonical = realpath(temp, NULL);
		UT_ASSERT(canonical != NULL && origins != NULL);
		snprintf(path, sizeof(path), "%s/new", canonical);
		preflight_directory(path, &base);
		create_directory(&base);
		create_child(&base, "node0", &origins[0].data);
		create_child(&origins[0].data, "global", &global);
		config.identity.configured[0] = 1;
		config.identity.system_identifier = UINT64CONST(7584383251700000001);
		memset(config.identity.storage_uuid, 0x12, 16);
		create_storage_contracts(origins, &config);
		UT_ASSERT_EQ(origins[0].storage_contract.state, 0);
		UT_ASSERT_EQ(origins[0].storage_contract.authority_system_identifier,
					 config.identity.system_identifier);
		UT_ASSERT_STR_EQ(origins[0].storage_contract.storage_uuid,
						 "12121212121212121212121212121212");
		creation_storage_current(&origins[0]);
		if (fault == 0) {
			struct stat identity = origins[0].storage_contract_identity;
			expecting = true;
			if (setjmp(refused) == 0) {
				create_storage_contracts(origins, &config);
				UT_ASSERT(false);
			}
			expecting = false;
			/* A failed creator exits in production; its output observation is
			 * unspecified. Verify the original file itself was not replaced. */
			UT_ASSERT(cluster_initdb_object_recheck(global.fd, "pgrac_cf_contract",
													(const uint8 *)&origins[0].storage_contract,
													sizeof(ClusterCfContractRecord), &identity));
		} else {
			if (fault == 1) {
				fd = openat(global.fd, "pgrac_cf_contract", O_WRONLY);
				UT_ASSERT(fd >= 0 && pwrite(fd, "X", 1, 16) == 1 && close(fd) == 0);
			} else {
				UT_ASSERT(renameat(global.fd, "pgrac_cf_contract", global.fd, "saved") == 0);
				if (fault == 2)
					UT_ASSERT(cluster_initdb_object_write_new(
						global.fd, "pgrac_cf_contract", (const uint8 *)&origins[0].storage_contract,
						sizeof(ClusterCfContractRecord)));
			}
			expecting = true;
			if (setjmp(refused) == 0) {
				creation_storage_current(&origins[0]);
				UT_ASSERT(false);
			}
			expecting = false;
		}
		UT_ASSERT(faccessat(global.fd, "pgrac_control_root", F_OK, 0) != 0 && errno == ENOENT);
		side_test_remove(base.fd);
		UT_ASSERT(close(global.fd) == 0 && close(origins[0].data.fd) == 0);
		UT_ASSERT(close(base.fd) == 0 && unlinkat(base.parent, base.name, AT_REMOVEDIR) == 0);
		UT_ASSERT(close(base.parent) == 0 && rmdir(canonical) == 0);
		free(canonical);
		free(origins);
	}
}
static void
side_routes_reject_unqualified_inputs_before_moving(void)
{
	for (unsigned fault = 1; fault <= 4; fault++)
		side_route_case(fault, 3);
	side_route_case(13, 0);
}
static void
side_route_io_failure_preserves_originals(void)
{
	side_route_case(5, 0);
	side_route_case(6, 3);
}
static void
side_routes_reject_late_identity_and_byte_changes(void)
{
	for (unsigned fault = 7; fault <= 12; fault++)
		side_route_case(fault, 3);
}
static void
reused_link_inode_does_not_restore_original_link(void)
{
	side_route_case(14, 3);
}

static void
managed_child_cannot_select_the_legacy_root_publisher(void)
{
	InitdbOrigin *origins = calloc(CLUSTER_CONTROL_ROOT_RECORD_COUNT, sizeof(*origins));
	UT_ASSERT(origins != NULL);
	creation_legacy_format_current(origins);
	for (unsigned slot = 0; slot < 128; slot += 127) {
		origins[slot].managed_relmap = true;
		expecting = true;
		if (setjmp(refused) == 0) {
			creation_legacy_format_current(origins);
			UT_ASSERT(false);
		}
		expecting = false;
		origins[slot].managed_relmap = false;
	}
	free(origins);
}

static void
original_registered_child_receives_only_its_creator(void)
{
	char temp[] = "/tmp/pgrac-creation-child-XXXXXX";
	char a[MAXPGPATH], b[MAXPGPATH], ad[24], bd[24];
	char *args[] = {executable, "registered-child", ad, bd, NULL};
	ClusterSharedConfigRef config = {0};
	PgracInitdbConfigContext request = {0};
	PgracInitdbRecoveryContext ctx;
	uint8 branch[16] = {2};
	int data, shared;
	UT_ASSERT(mkdtemp(temp) != NULL);
	snprintf(a, sizeof(a), "%s/a", temp); snprintf(b, sizeof(b), "%s/b", temp);
	UT_ASSERT(mkdir(a, 0700) == 0 && mkdir(b, 0700) == 0);
	data = open(a, O_RDONLY | O_DIRECTORY); shared = open(b, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(data >= 0 && shared >= 0);
	config.identity.system_identifier = 91; config.identity.database_incarnation = 27;
	config.identity.generation = 1; config.identity.storage_uuid[0] = 1;
	config.identity.authority_uuid[0] = 3; config.identity.configured[0] = 1;
	config.sha256[0] = 10;
	request.fd = 3; request.device = 7; request.inode = 8; request.bytes = 9;
	request.sha256[0] = 10;
	UT_ASSERT(cluster_initdb_cohort_relmap_prepare_v2(&config, &request, branch, data, shared, &ctx));
	UT_ASSERT_EQ(ctx.boot_file_count, 2);
	UT_ASSERT_EQ(ctx.boot_files[0].kind, CLUSTER_RECOVERY_FILE_CLOG_V2);
	UT_ASSERT_EQ(ctx.boot_files[1].kind, CLUSTER_RECOVERY_FILE_UNDO_V2);
	UT_ASSERT_EQ(ctx.boot_files[0].creation_generation, 5);
	UT_ASSERT_EQ(ctx.boot_files[1].creation_generation, 6);
	UT_ASSERT_EQ(ctx.boot_files[0].object_id + ctx.boot_files[1].object_id, 0);
	snprintf(ad, sizeof(ad), "%d", data); snprintf(bd, sizeof(bd), "%d", shared);
	run_child_registered(args, getppid(), &ctx);
	UT_ASSERT(getenv(PGRAC_INITDB_RECOVERY_ENV) == NULL);
	UT_ASSERT(waitpid(-1, NULL, WNOHANG) == -1 && errno == ECHILD);
	UT_ASSERT(close(data) == 0 && close(shared) == 0);
	UT_ASSERT(rmdir(a) == 0 && rmdir(b) == 0 && rmdir(temp) == 0);
}

/* Family codecs are controlled here; A's path/FD custody and complete original
 * creator bodies run against real disposable files. F tests the physical codec. */
static unsigned boot_fault, boot_reads;
bool
cluster_clog_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
							   ClusterRecoveryBootFileV2 *out)
{
	struct stat st;
	uint8 byte;
	++boot_reads;
	if (boot_fault == 1 || fstat(fd, &st) || pread(fd, &byte, 1, 0) != 1)
		return false;
	memset(out, 0, sizeof(*out));
	out->identity = *id;
	out->synced_bytes = st.st_size;
	out->image_digest[0] = byte;
	out->page.page_generation = 1;
	out->page.birth_observed = id->kind == CLUSTER_RECOVERY_FILE_CLOG_V2;
	out->page.birth_full_xid
		= out->page.birth_observed ? FirstNormalFullTransactionId : FullTransactionIdFromU64(0);
	if (boot_fault == 2)
		++out->identity.creation_generation;
	if (boot_fault == 3)
		++out->page.page_generation;
	return true;
}
bool
cluster_undo_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
							   ClusterRecoveryBootFileV2 *out)
{
	return cluster_clog_bootstrap_read_v2(fd, id, out);
}
static InitdbOrigin boot_origin;
static char boot_temp[MAXPGPATH];
static void
boot_setup(void)
{
	ClusterSharedConfigRef c = { 0 };
	PgracInitdbConfigContext r = { 0 };
	uint8 branch[16] = { 2 };
	int shared;
	const char *dirs[] = { "pg_xact", "pg_undo", "pg_undo/instance_0" };
	memset(&boot_origin, 0, sizeof(boot_origin));
	strlcpy(boot_temp, "/tmp/pgrac-boot-custody-XXXXXX", sizeof(boot_temp));
	UT_ASSERT(mkdtemp(boot_temp) != NULL);
	boot_origin.data.parent = open(boot_temp, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(!mkdirat(boot_origin.data.parent, "data", 0700));
	snprintf(boot_origin.data.path, sizeof(boot_origin.data.path), "%s/data", boot_temp);
	boot_origin.data.fd = openat(boot_origin.data.parent, "data", O_RDONLY | O_DIRECTORY);
	strlcpy(boot_origin.data.name, "data", sizeof(boot_origin.data.name));
	UT_ASSERT(!fstat(boot_origin.data.fd, &boot_origin.data.identity));
	UT_ASSERT(!fstat(boot_origin.data.parent, &boot_origin.data.parent_identity));
	for (unsigned i = 0; i < lengthof(dirs); ++i)
		UT_ASSERT(!mkdirat(boot_origin.data.fd, dirs[i], 0700));
	for (unsigned i = 0; i < 2; ++i) {
		int fd = openat(boot_origin.data.fd, i ? "pg_undo/instance_0/seg_0.dat" : "pg_xact/0000",
						O_CREAT | O_EXCL | O_RDWR, 0600);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(write(fd, "x", 1), 1);
		UT_ASSERT(!ftruncate(fd, i ? UNDO_SEGMENT_SIZE_BYTES : 16448));
		UT_ASSERT(!close(fd));
	}
	c.identity.system_identifier = 91;
	c.identity.database_incarnation = 27;
	c.identity.generation = 1;
	c.identity.storage_uuid[0] = 1;
	c.identity.configured[0] = 1;
	c.sha256[0] = r.sha256[0] = 10;
	r.fd = 3;
	r.device = 7;
	r.inode = 8;
	r.bytes = 9;
	UT_ASSERT(!mkdirat(boot_origin.data.fd, "shared", 0700));
	shared = openat(boot_origin.data.fd, "shared", O_RDONLY | O_DIRECTORY);
	UT_ASSERT(shared >= 0);
	UT_ASSERT(cluster_initdb_cohort_relmap_prepare_v2(&c, &r, branch, boot_origin.data.fd, shared,
													  &boot_origin.relmap_registration));
	UT_ASSERT(!close(shared));
	boot_origin.managed_relmap = true;
	boot_fault = boot_reads = 0;
	side_sync_fault = false;
}
static void
boot_cleanup(void)
{
	const char *files[] = { "pg_xact/0000", "pg_undo/instance_0/seg_0.dat", "pg_xact/old" };
	for (unsigned i = 0; i < lengthof(files); ++i) {
		int rc = unlinkat(boot_origin.data.fd, files[i], 0);
		UT_ASSERT(!rc || errno == ENOENT);
	}
	UT_ASSERT(!unlinkat(boot_origin.data.fd, "pg_undo/instance_0", AT_REMOVEDIR));
	UT_ASSERT(!unlinkat(boot_origin.data.fd, "pg_undo", AT_REMOVEDIR));
	UT_ASSERT(!unlinkat(boot_origin.data.fd, "pg_xact", AT_REMOVEDIR));
	UT_ASSERT(!unlinkat(boot_origin.data.fd, "shared", AT_REMOVEDIR));
	UT_ASSERT(!close(boot_origin.data.fd));
	UT_ASSERT(!unlinkat(boot_origin.data.parent, "data", AT_REMOVEDIR));
	UT_ASSERT(!close(boot_origin.data.parent));
	UT_ASSERT(!rmdir(boot_temp));
	side_sync_fault = false;
}
static void
complete_boot_capture_and_current_require_both_actual_files(void)
{
	boot_setup();
	UT_ASSERT(creation_boot_current(&boot_origin, true));
	UT_ASSERT_EQ(boot_reads, 2);
	UT_ASSERT(creation_boot_current(&boot_origin, false));
	UT_ASSERT_EQ(boot_reads, 4);
	boot_cleanup();
}
static void
failed_partial_boot_capture_never_installs_half_the_batch(void)
{
	ClusterRecoveryBootFileV2 empty[2] = { 0 };
	boot_setup();
	UT_ASSERT(!unlinkat(boot_origin.data.fd, "pg_undo/instance_0/seg_0.dat", 0));
	UT_ASSERT(!creation_boot_current(&boot_origin, true));
	UT_ASSERT(!memcmp(empty, boot_origin.boot_observations, sizeof(empty)));
	boot_cleanup();
	boot_setup();
	side_sync_fault = true;
	UT_ASSERT(!creation_boot_current(&boot_origin, true));
	UT_ASSERT_EQ(boot_reads, 0);
	UT_ASSERT(!memcmp(empty, boot_origin.boot_observations, sizeof(empty)));
	side_sync_fault = false;
	boot_cleanup();
}
static void
changed_identity_bytes_or_inode_cannot_rebase_boot_observations(void)
{
	for (unsigned f = 1; f <= 5; ++f) {
		boot_setup();
		UT_ASSERT(creation_boot_current(&boot_origin, true));
		if (f <= 3)
			boot_fault = f;
		if (f == 4) {
			int fd = openat(boot_origin.data.fd, "pg_xact/0000", O_WRONLY);
			UT_ASSERT(fd >= 0 && pwrite(fd, "y", 1, 0) == 1 && !close(fd));
		}
		if (f == 5) {
			int fd;
			UT_ASSERT(
				!renameat(boot_origin.data.fd, "pg_xact/0000", boot_origin.data.fd, "pg_xact/old"));
			fd = openat(boot_origin.data.fd, "pg_xact/0000", O_CREAT | O_EXCL | O_RDWR, 0600);
			UT_ASSERT(fd >= 0 && write(fd, "x", 1) == 1 && !ftruncate(fd, 16448) && !close(fd));
		}
		UT_ASSERT(!creation_boot_current(&boot_origin, false));
		boot_cleanup();
	}
}
static void
registry_requires_complete_exact_boot_family_without_duplicates(void)
{
	InitdbOrigin *origins = calloc(128, sizeof(*origins));
	ClusterRecoveryRegistryV2 registry = { 0 };
	ClusterRecoveryCheckpointFileV2 files[3] = { 0 };
	UT_ASSERT(origins != NULL);
	boot_setup();
	UT_ASSERT(creation_boot_current(&boot_origin, true));
	origins[0] = boot_origin;
	registry.coverage.files = files;
	registry.coverage.file_count = 2;
	for (unsigned i = 0; i < 2; ++i) {
		files[i].identity = boot_origin.boot_observations[i].identity;
		files[i].copy_count = 1;
		files[i].synced_bytes = boot_origin.boot_observations[i].synced_bytes;
		files[i].page_count = i ? UNDO_SEGMENT_SIZE_BYTES / BLCKSZ : 1;
		memcpy(files[i].image_digest, boot_origin.boot_observations[i].image_digest, 32);
	}
	UT_ASSERT(creation_boot_registry_valid(origins, &registry));
	registry.coverage.file_count = 1;
	UT_ASSERT(!creation_boot_registry_valid(origins, &registry));
	registry.coverage.file_count = 3;
	files[2] = files[1];
	UT_ASSERT(!creation_boot_registry_valid(origins, &registry));
	registry.coverage.file_count = 2;
	++files[1].identity.creation_generation;
	UT_ASSERT(!creation_boot_registry_valid(origins, &registry));
	--files[1].identity.creation_generation;
	files[0].image_digest[0] ^= 1;
	UT_ASSERT(!creation_boot_registry_valid(origins, &registry));
	files[0].image_digest[0] ^= 1;
	origins[0].relmap_registration.boot_file_count = 0;
	UT_ASSERT(!creation_boot_registry_valid(origins, &registry));
	free(origins);
	boot_cleanup();
}

int
main(int argc, char **argv)
{
	if (argc == 4 && strcmp(argv[1], "registered-child") == 0)
	{
		PgracInitdbConfigContext config = {0};
		PgracInitdbRecoveryContext ctx;
		uint8 storage[16] = {1};
		const char *value = getenv(PGRAC_INITDB_RECOVERY_ENV);
		config.fd = 3; config.device = 7; config.inode = 8; config.bytes = 9;
		config.sha256[0] = 10;
		return value && pgrac_initdb_recovery_read_v2(atoi(value), &config, 91, 27,
			storage, atoi(argv[2]), atoi(argv[3]), &ctx) ? 0 : 17;
	}
	if (argc >= 3 && strcmp(argv[1], "child") == 0) {
		pid_t self = getpid(), descendant;
		int fd;
		if (strcmp(argv[2], "success") == 0)
			return 0;
		if (strcmp(argv[2], "failure") == 0)
			return 7;
		if (argc != 4 || getpgrp() != self)
			return 8;
		fd = atoi(argv[3]);
		{
			sigset_t ready;
			sigemptyset(&ready);
			sigaddset(&ready, SIGUSR1);
			if (sigprocmask(SIG_BLOCK, &ready, NULL) != 0)
				return 15;
		}
		if (write(fd, &self, sizeof(self)) != sizeof(self))
			return 9;
		descendant = fork();
		if (descendant < 0)
			return 10;
		if (descendant == 0) {
			self = getpid();
			if (write(fd, &self, sizeof(self)) != sizeof(self))
				_exit(11);
			/* Notify the owner only after both owned writers are alive. */
			if (kill(getppid(), SIGUSR1) != 0)
				_exit(12);
			for (;;)
				pause();
		}
		/* SIGUSR1 is blocked before fork by the parent fixture below. */
		{
			sigset_t ready;
			int sig;
			sigemptyset(&ready);
			sigaddset(&ready, SIGUSR1);
			if (sigwait(&ready, &sig) != 0)
				return 13;
		}
		if (kill(getppid(), SIGTERM) != 0)
			return 14;
		for (;;)
			pause();
	}
	strlcpy(executable, argv[0], sizeof(executable));
	UT_PLAN(16);
	UT_RUN(complete_boot_capture_and_current_require_both_actual_files);
	UT_RUN(failed_partial_boot_capture_never_installs_half_the_batch);
	UT_RUN(changed_identity_bytes_or_inode_cannot_rebase_boot_observations);
	UT_RUN(registry_requires_complete_exact_boot_family_without_duplicates);
	UT_RUN(managed_child_cannot_select_the_legacy_root_publisher);
	UT_RUN(original_registered_child_receives_only_its_creator);
	UT_RUN(storage_contract_is_original_unverified_and_rechecked);
	UT_RUN(side_routes_retain_actual_original_directories);
	UT_RUN(side_routes_reject_unqualified_inputs_before_moving);
	UT_RUN(side_route_io_failure_preserves_originals);
	UT_RUN(side_routes_reject_late_identity_and_byte_changes);
	UT_RUN(reused_link_inode_does_not_restore_original_link);
	UT_RUN(derived_inputs_must_survive_until_primary_publication);
	UT_RUN(completed);
	UT_RUN(unsuccessful);
	UT_RUN(cancel_owns_and_reaps_the_native_group);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
