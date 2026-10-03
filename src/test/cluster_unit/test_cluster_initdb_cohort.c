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
	UT_PLAN(3);
	UT_RUN(completed);
	UT_RUN(unsuccessful);
	UT_RUN(cancel_owns_and_reaps_the_native_group);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
