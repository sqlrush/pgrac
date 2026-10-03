/* Actual owned native-child lifecycle; no simulated successful creation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <setjmp.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static jmp_buf refused;
static bool expecting;
static char executable[MAXPGPATH];
#include "../../backend/cluster/cluster_initdb_cohort.c"

bool errstart(int level, const char *domain) { return true; }
bool errstart_cold(int level, const char *domain) { return true; }
int errcode(int code) { return 0; }
int errmsg(const char *format, ...) { return 0; }
void errfinish(const char *file, int line, const char *func)
{
	if (expecting) longjmp(refused, 1);
	abort();
}
void ExceptionalCondition(const char *condition, const char *file, int line) { abort(); }

static void
completed(void)
{
	char *args[] = {executable, "child", "success", NULL};
	run_child(args, getppid());
	UT_ASSERT(waitpid(-1, NULL, WNOHANG) == -1 && errno == ECHILD);
}

static void
unsuccessful(void)
{
	char *args[] = {executable, "child", "failure", NULL};
	expecting = true;
	if (setjmp(refused) == 0) { run_child(args, getppid()); UT_ASSERT(false); }
	expecting = false;
	UT_ASSERT(waitpid(-1, NULL, WNOHANG) == -1 && errno == ECHILD);
}

static void
cancel_owns_and_reaps_the_native_group(void)
{
	int pipefd[2];
	char fdtext[24];
	char *args[] = {executable, "child", "cancel", fdtext, NULL};
	sigset_t blocked, current;
	pid_t direct, descendant;
	char extra;
	UT_ASSERT(pipe(pipefd) == 0);
	snprintf(fdtext, sizeof(fdtext), "%d", pipefd[1]);
	sigemptyset(&blocked); sigaddset(&blocked, SIGTERM);
	UT_ASSERT(sigprocmask(SIG_BLOCK, &blocked, NULL) == 0);
	install_creation_signals();
	UT_ASSERT(sigprocmask(SIG_SETMASK, NULL, &current) == 0);
	UT_ASSERT(sigismember(&current, SIGTERM) == 0);
	expecting = true;
	if (setjmp(refused) == 0) { run_child(args, getppid()); UT_ASSERT(false); }
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

static void derived_inputs_must_survive_until_primary_publication(void)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		char temp[] = "/tmp/pgrac-derived-XXXXXX", path[MAXPGPATH];
		char *canonical;
		InitdbDirectory base, global, startups;
		InitdbStartupObject *objects = calloc(128, sizeof(*objects));
		ControlRootImage *root = calloc(1, sizeof(*root));
		uint8 (*inputs)[CLUSTER_WAL_STARTUP_BYTES] = calloc(128, CLUSTER_WAL_STARTUP_BYTES);
		struct stat backup;
		int fd;
		UT_ASSERT(mkdtemp(temp) != NULL);
		canonical = realpath(temp, NULL);
		UT_ASSERT(canonical != NULL && objects != NULL && root != NULL && inputs != NULL);
		snprintf(path, sizeof(path), "%s/new", canonical);
		preflight_directory(path, &base); create_directory(&base);
		create_child(&base, "global", &global); create_child(&global, "wal_startup", &startups);
		create_child(&startups, "thread_1", &objects[0].directory);
		root->present[0] = true;
		memset(inputs[0], 0x6a, CLUSTER_WAL_STARTUP_BYTES);
		strlcpy(objects[0].name, "input", sizeof(objects[0].name));
		fd = openat(objects[0].directory.fd, "input", O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, inputs[0], CLUSTER_WAL_STARTUP_BYTES) == CLUSTER_WAL_STARTUP_BYTES);
		UT_ASSERT(fsync(fd) == 0 && fstat(fd, &objects[0].identity) == 0 && close(fd) == 0);
		fd = openat(global.fd, "pgrac_control_root.bak", O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, root->bytes, sizeof(root->bytes)) == sizeof(root->bytes));
		UT_ASSERT(fsync(fd) == 0 && fstat(fd, &backup) == 0 && close(fd) == 0);
		creation_derived_current(&global, &startups, objects, root, inputs, &backup);
		if (fault == 0 || fault == 1 || fault == 4) {
			fd = openat(fault == 4 ? global.fd : objects[0].directory.fd,
				fault == 4 ? "pgrac_control_root.bak" : "input", O_WRONLY);
			UT_ASSERT(fd >= 0);
			if (fault == 1) UT_ASSERT(ftruncate(fd, 31) == 0);
			else UT_ASSERT(pwrite(fd, "X", 1, 17) == 1);
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
		if (fault == 2) UT_ASSERT(unlinkat(objects[0].directory.fd, "old", 0) == 0);
		UT_ASSERT(close(objects[0].directory.fd) == 0);
		UT_ASSERT(unlinkat(startups.fd, "thread_1", AT_REMOVEDIR) == 0);
		if (fault == 3) UT_ASSERT(unlinkat(startups.fd, "old", AT_REMOVEDIR) == 0);
		UT_ASSERT(close(startups.fd) == 0 && unlinkat(global.fd, "wal_startup", AT_REMOVEDIR) == 0);
		UT_ASSERT(unlinkat(global.fd, "pgrac_control_root.bak", 0) == 0);
		if (fault == 5) UT_ASSERT(unlinkat(global.fd, "old", 0) == 0);
		UT_ASSERT(close(global.fd) == 0 && unlinkat(base.fd, "global", AT_REMOVEDIR) == 0);
		UT_ASSERT(close(base.fd) == 0 && unlinkat(base.parent, base.name, AT_REMOVEDIR) == 0);
		UT_ASSERT(close(base.parent) == 0 && rmdir(canonical) == 0);
		free(canonical); free(inputs); free(root); free(objects);
	}
}

int
main(int argc, char **argv)
{
	if (argc >= 3 && strcmp(argv[1], "child") == 0)
	{
		pid_t self = getpid(), descendant;
		int fd;
		if (strcmp(argv[2], "success") == 0) return 0;
		if (strcmp(argv[2], "failure") == 0) return 7;
		if (argc != 4 || getpgrp() != self) return 8;
		fd = atoi(argv[3]);
		{
			sigset_t ready;
			sigemptyset(&ready); sigaddset(&ready, SIGUSR1);
			if (sigprocmask(SIG_BLOCK, &ready, NULL) != 0) return 15;
		}
		if (write(fd, &self, sizeof(self)) != sizeof(self)) return 9;
		descendant = fork();
		if (descendant < 0) return 10;
		if (descendant == 0)
		{
			self = getpid();
			if (write(fd, &self, sizeof(self)) != sizeof(self)) _exit(11);
			/* Notify the owner only after both owned writers are alive. */
			if (kill(getppid(), SIGUSR1) != 0) _exit(12);
			for (;;) pause();
		}
		/* SIGUSR1 is blocked before fork by the parent fixture below. */
		{
			sigset_t ready; int sig;
			sigemptyset(&ready); sigaddset(&ready, SIGUSR1);
			if (sigwait(&ready, &sig) != 0) return 13;
		}
		if (kill(getppid(), SIGTERM) != 0) return 14;
		for (;;) pause();
	}
	strlcpy(executable, argv[0], sizeof(executable));
	UT_PLAN(4);
	UT_RUN(derived_inputs_must_survive_until_primary_publication);
	UT_RUN(completed);
	UT_RUN(unsuccessful);
	UT_RUN(cancel_owns_and_reaps_the_native_group);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
