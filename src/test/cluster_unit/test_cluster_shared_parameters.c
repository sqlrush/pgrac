/*-------------------------------------------------------------------------
 *
 * test_cluster_shared_parameters.c
 *	  The parameters pg_control records, against a shared control file.
 *
 *	  Runs the real ClusterRequireRecordedParameters (called by StartupXLOG
 *	  before its first durable write) and XLogReportParameters, extracted
 *	  from xlog.c.  With cluster.shared_config a changed parameter ends the
 *	  start with FATAL 55R07 naming every change, before the parameter-change
 *	  record and without reaching UpdateControlFile; unchanged parameters
 *	  return; without shared configuration the native update is unchanged.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shared_parameters.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <stdarg.h>

#include "access/commit_ts.h"
#include "access/rmgr.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "replication/walsender.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* GUC and control state the extracted code reads. */
int wal_level = WAL_LEVEL_REPLICA;
bool wal_log_hints = false;
int MaxConnections = 100;
int max_worker_processes = 8;
int max_wal_senders = 10;
int max_prepared_xacts = 0;
int max_locks_per_xact = 64;
bool track_commit_timestamp = false;
bool cluster_shared_config = false;
static ControlFileData control;
static ControlFileData *ControlFile = &control;
const struct config_enum_entry wal_level_options[]
	= { { "minimal", WAL_LEVEL_MINIMAL, false }, { "replica", WAL_LEVEL_REPLICA, false },
		{ "archive", WAL_LEVEL_REPLICA, true },	 { "hot_standby", WAL_LEVEL_REPLICA, true },
		{ "logical", WAL_LEVEL_LOGICAL, false }, { NULL, 0, false } };
static LWLockPadded locks[NUM_FIXED_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;

/* Native boundaries the extracted code calls. */
static unsigned inserts, flushes, updates, lock_calls;
static int lock_depth;

void
XLogBeginInsert(void)
{}
void
XLogRegisterData(char *data pg_attribute_unused(), uint32 len pg_attribute_unused())
{}
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(rmid, RM_XLOG_ID);
	UT_ASSERT_EQ(info, XLOG_PARAMETER_CHANGE);
	inserts++;
	return 0x100;
}
void
XLogFlush(XLogRecPtr record pg_attribute_unused())
{
	flushes++;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	UT_ASSERT(lock == ControlFileLock);
	lock_calls++;
	lock_depth++;
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	lock_depth--;
}
static void
UpdateControlFile(void)
{
	UT_ASSERT_EQ(lock_depth, 1);
	updates++;
}

/* A fixed-size StringInfo and psprintf: this file only reports. */
static char string_storage[4096];
void
initStringInfo(StringInfo str)
{
	string_storage[0] = '\0';
	str->data = string_storage;
	str->len = 0;
	str->maxlen = sizeof(string_storage);
	str->cursor = 0;
}
void
appendStringInfo(StringInfo str, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	str->len += vsnprintf(str->data + str->len, str->maxlen - str->len, fmt, ap);
	va_end(ap);
}
void
pfree(void *pointer)
{
	UT_ASSERT(pointer == string_storage);
}
char *
psprintf(const char *fmt, ...)
{
	static char values[32][32];
	static unsigned next;
	char *out = values[next++ % lengthof(values)];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(out, sizeof(values[0]), fmt, ap);
	va_end(ap);
	return out;
}

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

/* The report: FATAL returns to the test, nothing else is expected. */
static sigjmp_buf report_target;
static bool report_armed;
static int report_level, report_code;
static char report_message[512], report_detail[1024], report_hint[512];

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	UT_ASSERT(report_armed && elevel == FATAL);
	report_level = elevel;
	return true;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
int
errcode(int sqlerrcode)
{
	report_code = sqlerrcode;
	return 0;
}
#define REPORT_TEXT(fn, buffer)                                                                    \
	int fn(const char *fmt, ...)                                                                   \
	{                                                                                              \
		va_list ap;                                                                                \
		va_start(ap, fmt);                                                                         \
		vsnprintf(buffer, sizeof(buffer), fmt, ap);                                                \
		va_end(ap);                                                                                \
		return 0;                                                                                  \
	}
REPORT_TEXT(errmsg, report_message)
REPORT_TEXT(errdetail, report_detail)
REPORT_TEXT(errhint, report_hint)
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	siglongjmp(report_target, 1);
}

#ifndef USE_PGRAC_CLUSTER
#define USE_PGRAC_CLUSTER 1
#endif
#include "test_cluster_shared_parameters.inc"
#include "test_cluster_shared_parameters_order.inc"

static void
reset(bool shared)
{
	memset(&control, 0, sizeof(control));
	wal_level = control.wal_level = WAL_LEVEL_REPLICA;
	wal_log_hints = control.wal_log_hints = false;
	MaxConnections = control.MaxConnections = 100;
	max_worker_processes = control.max_worker_processes = 8;
	max_wal_senders = control.max_wal_senders = 10;
	max_prepared_xacts = control.max_prepared_xacts = 0;
	max_locks_per_xact = control.max_locks_per_xact = 64;
	track_commit_timestamp = control.track_commit_timestamp = false;
	cluster_shared_config = shared;
	inserts = flushes = updates = lock_calls = 0;
	lock_depth = 0;
	report_level = report_code = 0;
	report_message[0] = report_detail[0] = report_hint[0] = '\0';
}

/* Returns whether the call ended in FATAL. */
static bool
report_parameters(void)
{
	report_armed = true;
	if (sigsetjmp(report_target, 1) != 0) {
		report_armed = false;
		return true;
	}
	XLogReportParameters();
	report_armed = false;
	return false;
}

UT_TEST(shared_unchanged_parameters_write_nothing)
{
	reset(true);
	UT_ASSERT(!report_parameters());
	UT_ASSERT_EQ(inserts + flushes + updates + lock_calls, 0);
}

UT_TEST(shared_changed_parameter_is_refused_before_any_write)
{
	reset(true);
	MaxConnections = 300;
	UT_ASSERT(report_parameters());
	UT_ASSERT_EQ(report_level, FATAL);
	UT_ASSERT_EQ(report_code, ERRCODE_CLUSTER_SHARED_PARAMETER_FIXED);
	UT_ASSERT(strstr(report_message, "cannot be changed") != NULL);
	UT_ASSERT(strstr(report_detail, "PGRAC_REASON=NATIVE_PARAMETER_CHANGE") != NULL);
	UT_ASSERT(strstr(report_detail, "max_connections=300 (created with 100)") != NULL);
	UT_ASSERT(strstr(report_detail, "max_worker_processes") == NULL);
	UT_ASSERT(strstr(report_hint, "creation values") != NULL);
	/* No parameter-change record, and the control file is untouched. */
	UT_ASSERT_EQ(inserts + flushes + updates + lock_calls, 0);
	UT_ASSERT_EQ(control.MaxConnections, 100);
}

UT_TEST(shared_refusal_names_every_changed_parameter)
{
	reset(true);
	control.wal_level = WAL_LEVEL_MINIMAL;
	wal_log_hints = true;
	max_locks_per_xact = 128;
	track_commit_timestamp = true;
	UT_ASSERT(report_parameters());
	UT_ASSERT(strstr(report_detail, "wal_level=replica (created with minimal)") != NULL);
	UT_ASSERT(strstr(report_detail, "wal_log_hints=on (created with off)") != NULL);
	UT_ASSERT(strstr(report_detail, "max_locks_per_transaction=128 (created with 64)") != NULL);
	UT_ASSERT(strstr(report_detail, "track_commit_timestamp=on (created with off)") != NULL);
	UT_ASSERT(strstr(report_detail, "max_connections") == NULL);
	UT_ASSERT_EQ(inserts + updates, 0);
}

UT_TEST(nonshared_change_keeps_the_native_update)
{
	reset(false);
	MaxConnections = 300;
	UT_ASSERT(!report_parameters());
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(updates, 1);
	UT_ASSERT_EQ(control.MaxConnections, 300);
}

/* Returns whether the startup check ended in FATAL. */
static bool
require_parameters(void)
{
	report_armed = true;
	if (sigsetjmp(report_target, 1) != 0) {
		report_armed = false;
		return true;
	}
	ClusterRequireRecordedParameters();
	report_armed = false;
	return false;
}

UT_TEST(startup_check_returns_when_nothing_changed)
{
	reset(true);
	UT_ASSERT(!require_parameters());
	UT_ASSERT_EQ(report_level, 0);
	UT_ASSERT_EQ(inserts + flushes + updates + lock_calls, 0);
}

UT_TEST(startup_check_refuses_each_parameter_alone)
{
	for (int field = 0; field < 8; field++) {
		static const char *const names[8] = { "wal_level=",
											  "wal_log_hints=",
											  "max_connections=",
											  "max_worker_processes=",
											  "max_wal_senders=",
											  "max_prepared_transactions=",
											  "max_locks_per_transaction=",
											  "track_commit_timestamp=" };

		reset(true);
		switch (field) {
		case 0:
			wal_level = WAL_LEVEL_LOGICAL;
			break;
		case 1:
			wal_log_hints = true;
			break;
		case 2:
			MaxConnections = 101;
			break;
		case 3:
			max_worker_processes = 9;
			break;
		case 4:
			max_wal_senders = 11;
			break;
		case 5:
			max_prepared_xacts = 1;
			break;
		case 6:
			max_locks_per_xact = 65;
			break;
		default:
			track_commit_timestamp = true;
			break;
		}
		UT_ASSERT(require_parameters());
		UT_ASSERT_EQ(report_level, FATAL);
		UT_ASSERT_EQ(report_code, ERRCODE_CLUSTER_SHARED_PARAMETER_FIXED);
		UT_ASSERT(strstr(report_detail, names[field]) != NULL);
		for (int other = 0; other < 8; other++)
			if (other != field)
				UT_ASSERT(strstr(report_detail, names[other]) == NULL);
		UT_ASSERT_EQ(inserts + flushes + updates + lock_calls, 0);
	}
}

/* StartupXLOG refuses before writer selection, crash sealing, WAL recovery
 * and its first WAL record; the late native check is only the backstop. */
UT_TEST(startup_refuses_before_its_first_durable_write)
{
	UT_ASSERT(STARTUP_LINE_REQUIRE > 0);
	UT_ASSERT(STARTUP_LINE_SEAL > STARTUP_LINE_REQUIRE);
	UT_ASSERT(STARTUP_LINE_SELECT > STARTUP_LINE_REQUIRE);
	UT_ASSERT(STARTUP_LINE_RECOVERY > STARTUP_LINE_REQUIRE);
	UT_ASSERT(STARTUP_LINE_FPW > STARTUP_LINE_REQUIRE);
	UT_ASSERT(STARTUP_LINE_REPORT > STARTUP_LINE_FPW);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(shared_unchanged_parameters_write_nothing);
	UT_RUN(shared_changed_parameter_is_refused_before_any_write);
	UT_RUN(shared_refusal_names_every_changed_parameter);
	UT_RUN(nonshared_change_keeps_the_native_update);
	UT_RUN(startup_check_returns_when_nothing_changed);
	UT_RUN(startup_check_refuses_each_parameter_alone);
	UT_RUN(startup_refuses_before_its_first_durable_write);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
