/* Actual Startup signal handler and interrupt consumer; only proc_exit and
 * unrelated service boundaries are replaced. Author: SqlRush <sqlrush@gmail.com> */
#include <signal.h>
#include <unistd.h>
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "storage/ipc.h"

bool proc_exit_inprogress;
static volatile sig_atomic_t shutdown_requested, got_SIGHUP;
static bool in_restore_command;
volatile sig_atomic_t ProcSignalBarrierPending, LogMemoryContextPending;
static sigjmp_buf startup_fixture_exit;
static bool startup_fixture_armed;
static int startup_fixture_exit_code;
static void (*startup_fixture_exit_hook)(void);
static void StartupRereadConfig(void) {}
static void WakeupRecovery(void) {}
static bool PostmasterIsAlive(void) { return true; }
static void ProcessProcSignalBarrier(void) {}
void ProcessLogMemoryContextInterrupt(void);
void ProcessLogMemoryContextInterrupt(void) {}
static void startup_fixture_proc_exit(int code) pg_attribute_noreturn();
static void
startup_fixture_proc_exit(int code)
{
	if (!startup_fixture_armed)
		abort();
	startup_fixture_exit_code = code;
	proc_exit_inprogress = true;
	if (startup_fixture_exit_hook != NULL)
		startup_fixture_exit_hook();
	siglongjmp(startup_fixture_exit, 1);
}
static void startup_fixture_stderr(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}
#define proc_exit startup_fixture_proc_exit
#define exit startup_fixture_proc_exit
#define _exit startup_fixture_proc_exit
#define write_stderr_signal_safe startup_fixture_stderr
#include "test_cluster_startup_interrupt.inc"
#undef write_stderr_signal_safe
#undef _exit
#undef exit
#undef proc_exit
