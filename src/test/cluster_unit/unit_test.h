/*-------------------------------------------------------------------------
 *
 * unit_test.h
 *	  Minimal C unit-testing framework for the pgrac cluster subsystem.
 *
 *	  This header provides a tiny zero-dependency unit-testing framework
 *	  that emits TAP (Test Anything Protocol) output, matching the format
 *	  used by PostgreSQL's existing perl/TAP tests.
 *
 *	  Usage:
 *
 *	      #include "unit_test.h"
 *	      #include "cluster/cluster.h"
 *
 *	      UT_DEFINE_GLOBALS();
 *
 *	      UT_TEST(test_version_not_null) {
 *	          UT_ASSERT_NOT_NULL(pgrac_version_string());
 *	      }
 *
 *	      int main(void) {
 *	          UT_PLAN(1);
 *	          UT_RUN(test_version_not_null);
 *	          UT_DONE();
 *	          return ut_failed_count == 0 ? 0 : 1;
 *	      }
 *
 *	  Output (TAP):
 *	      1..1
 *	      ok 1 - test_version_not_null
 *	      # All 1 tests passed.
 *
 *	  Design rationale:
 *	  - Zero external dependencies (no cmocka, gtest, etc.)
 *	  - PG-style minimalism (CLAUDE.md rule 14: dependency control)
 *	  - TAP output for compatibility with CI harnesses
 *	  - Each test_*.c is a standalone executable
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/unit_test.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c source file must call UT_DEFINE_GLOBALS() exactly
 *	  once at file scope to instantiate the bookkeeping variables.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_UNIT_TEST_H
#define PGRAC_UNIT_TEST_H

#include <stdio.h>
#include <string.h>


/*
 * UT_DEFINE_GLOBALS -- Instantiate per-binary bookkeeping state.
 *
 *	Each test_*.c file must call this once at file scope (outside any
 *	function).  These globals track current test number, failure count,
 *	and whether the current test has already failed an assertion.
 */
#define UT_DEFINE_GLOBALS()                                                                        \
	int ut_test_count = 0;                                                                         \
	int ut_failed_count = 0;                                                                       \
	int ut_current_failed = 0

/* Externally visible to UT_RUN / UT_ASSERT_* expansions. */
extern int ut_test_count;
extern int ut_failed_count;
extern int ut_current_failed;


/*
 * UT_TEST(name) -- Declare a test function.
 *
 *	Defines a static void function `name(void)`.  Use UT_RUN(name) to
 *	execute it.
 */
#define UT_TEST(name) static void name(void)


/*
 * UT_PLAN(n) -- Emit TAP plan line "1..n".
 *
 *	Should be called once before any UT_RUN.
 */
#define UT_PLAN(n) printf("1..%d\n", (n))


/*
 * UT_RUN(name) -- Execute a test and emit TAP "ok"/"not ok" line.
 *
 *	Resets ut_current_failed, runs the test, then prints the TAP result
 *	line.  Failures reported by UT_ASSERT_* set ut_current_failed.
 */
#define UT_RUN(name)                                                                               \
	do {                                                                                           \
		ut_test_count++;                                                                           \
		ut_current_failed = 0;                                                                     \
		name();                                                                                    \
		if (ut_current_failed == 0)                                                                \
			printf("ok %d - %s\n", ut_test_count, #name);                                          \
		else {                                                                                     \
			printf("not ok %d - %s\n", ut_test_count, #name);                                      \
			ut_failed_count++;                                                                     \
		}                                                                                          \
	} while (0)


/*
 * UT_DONE() -- Emit final TAP summary comment.
 *
 *	Call after the last UT_RUN.  Prints "# All N tests passed." or
 *	"# X of N tests failed." depending on outcome.
 */
#define UT_DONE()                                                                                  \
	do {                                                                                           \
		if (ut_failed_count == 0)                                                                  \
			printf("# All %d tests passed.\n", ut_test_count);                                     \
		else                                                                                       \
			printf("# %d of %d tests failed.\n", ut_failed_count, ut_test_count);                  \
	} while (0)


/*
 * Assertion macros.
 *
 *	On failure, each macro:
 *	  1. Prints a TAP-compatible "# Failed at file:line: <expr>" comment
 *	  2. Sets ut_current_failed = 1
 *	  3. Continues execution (does NOT abort)
 *
 *	The continuation policy lets a single test report multiple failures,
 *	which is helpful when diagnosing related issues.
 */

#define UT_ASSERT(cond)                                                                            \
	do {                                                                                           \
		if (!(cond)) {                                                                             \
			printf("# Failed at %s:%d: UT_ASSERT(%s)\n", __FILE__, __LINE__, #cond);               \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#define UT_ASSERT_EQ(a, b)                                                                         \
	do {                                                                                           \
		long long _a = (long long)(a);                                                             \
		long long _b = (long long)(b);                                                             \
		if (_a != _b) {                                                                            \
			printf("# Failed at %s:%d: UT_ASSERT_EQ(%s, %s) "                                      \
				   "(%lld != %lld)\n",                                                             \
				   __FILE__, __LINE__, #a, #b, _a, _b);                                            \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#define UT_ASSERT_NE(a, b)                                                                         \
	do {                                                                                           \
		long long _a = (long long)(a);                                                             \
		long long _b = (long long)(b);                                                             \
		if (_a == _b) {                                                                            \
			printf("# Failed at %s:%d: UT_ASSERT_NE(%s, %s) "                                      \
				   "(both %lld)\n",                                                                \
				   __FILE__, __LINE__, #a, #b, _a);                                                \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#define UT_ASSERT_NULL(p)                                                                          \
	do {                                                                                           \
		if ((p) != NULL) {                                                                         \
			printf("# Failed at %s:%d: UT_ASSERT_NULL(%s) "                                        \
				   "(non-null)\n",                                                                 \
				   __FILE__, __LINE__, #p);                                                        \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#define UT_ASSERT_NOT_NULL(p)                                                                      \
	do {                                                                                           \
		if ((p) == NULL) {                                                                         \
			printf("# Failed at %s:%d: UT_ASSERT_NOT_NULL(%s) "                                    \
				   "(was NULL)\n",                                                                 \
				   __FILE__, __LINE__, #p);                                                        \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#define UT_ASSERT_STR_EQ(a, b)                                                                     \
	do {                                                                                           \
		const char *_a = (a);                                                                      \
		const char *_b = (b);                                                                      \
		if (_a == NULL || _b == NULL || strcmp(_a, _b) != 0) {                                     \
			printf("# Failed at %s:%d: UT_ASSERT_STR_EQ(%s, %s) "                                  \
				   "(\"%s\" != \"%s\")\n",                                                         \
				   __FILE__, __LINE__, #a, #b, _a ? _a : "(null)", _b ? _b : "(null)");            \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#define UT_ASSERT_STR_CONTAINS(s, sub)                                                             \
	do {                                                                                           \
		const char *_s = (s);                                                                      \
		const char *_sub = (sub);                                                                  \
		if (_s == NULL || _sub == NULL || strstr(_s, _sub) == NULL) {                              \
			printf("# Failed at %s:%d: UT_ASSERT_STR_CONTAINS(%s, %s) "                            \
				   "(\"%s\" does not contain \"%s\")\n",                                           \
				   __FILE__, __LINE__, #s, #sub, _s ? _s : "(null)", _sub ? _sub : "(null)");      \
			ut_current_failed = 1;                                                                 \
		}                                                                                          \
	} while (0)

#endif /* PGRAC_UNIT_TEST_H */
