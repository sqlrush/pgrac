#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pgrac cluster unit tests
#
# IDENTIFICATION
#    src/test/cluster_unit/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile.  Each test_*.c compiles to a
#    standalone executable that links cluster_version.o (the only
#    cluster object with no PG-internal dependencies).  See
#    spec-0.4-unit-test-framework.md §9.2 for design rationale.
#
#-------------------------------------------------------------------------

subdir = src/test/cluster_unit
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# Test source files (each becomes a standalone executable)
TESTS = test_cluster_basic test_cluster_version test_cluster_backend_types \
        test_cluster_wait_events test_cluster_errcodes test_cluster_guc \
        test_cluster_shmem test_cluster_signal test_cluster_views \
        test_cluster_gviews test_cluster_ic test_cluster_conf \
        test_cluster_ic_mock

# Path to the cluster_version object (no PG deps, safe to link standalone).
CLUSTER_VERSION_O = $(top_builddir)/src/backend/cluster/cluster_version.o

# Path to cluster_guc.o + cluster_elog.o (needed by test_cluster_guc only).
# These reference PG backend symbols (DefineCustomIntVariable, etc.) that
# the test stubs out locally.
CLUSTER_GUC_O = $(top_builddir)/src/backend/cluster/cluster_guc.o
CLUSTER_ELOG_O = $(top_builddir)/src/backend/cluster/cluster_elog.o

# Include path for unit_test.h (this dir) and cluster headers.
override CPPFLAGS := -I. -I$(top_srcdir)/src/include $(CPPFLAGS)


# ----------
# Default target: build all tests
# ----------
all: $(TESTS)

CLUSTER_SHMEM_O = $(top_builddir)/src/backend/cluster/cluster_shmem.o
CLUSTER_SIGNAL_O = $(top_builddir)/src/backend/cluster/cluster_signal.o
CLUSTER_VIEWS_O = $(top_builddir)/src/backend/cluster/cluster_views.o
CLUSTER_IC_O = $(top_builddir)/src/backend/cluster/cluster_ic.o
CLUSTER_CONF_O = $(top_builddir)/src/backend/cluster/cluster_conf.o

# Most tests link only cluster_version.o.  test_cluster_guc /
# test_cluster_shmem / test_cluster_signal / test_cluster_views /
# test_cluster_gviews / test_cluster_ic / test_cluster_conf have
# separate rules because they also link additional cluster_*.o
# objects (the test files stub the PG backend symbols those
# objects reference).
SIMPLE_TESTS = $(filter-out test_cluster_guc test_cluster_shmem test_cluster_signal test_cluster_views test_cluster_gviews test_cluster_ic test_cluster_conf test_cluster_ic_mock,$(TESTS))

$(SIMPLE_TESTS): %: %.c unit_test.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_VERSION_O) -o $@

# test_cluster_guc additionally links cluster_guc.o + cluster_elog.o
# (the test stubs DefineCustomIntVariable so no PG backend is needed).
test_cluster_guc: test_cluster_guc.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_GUC_O) $(CLUSTER_ELOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GUC_O) $(CLUSTER_ELOG_O) -o $@

# test_cluster_shmem additionally links cluster_shmem.o + cluster_guc.o +
# cluster_elog.o (the test stubs RequestAddinShmemSpace, ShmemInitStruct,
# GetCurrentTimestamp, and elog so no PG backend is needed).
test_cluster_shmem: test_cluster_shmem.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_SHMEM_O) $(CLUSTER_GUC_O) $(CLUSTER_ELOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHMEM_O) $(CLUSTER_GUC_O) $(CLUSTER_ELOG_O) -o $@

# test_cluster_signal links cluster_signal.o.  cluster_signal.c calls
# SetLatch(MyLatch) which needs PG backend; the test stubs both.
test_cluster_signal: test_cluster_signal.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_SIGNAL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SIGNAL_O) -o $@

# test_cluster_views links cluster_views.o.  cluster_views.c calls
# fmgr / tuplestore / wait event lookup machinery; the test stubs all
# of them since the unit test only takes the SRF address (never invokes).
test_cluster_views: test_cluster_views.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_VIEWS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VIEWS_O) -o $@

# test_cluster_gviews shares cluster_views.o with test_cluster_views.
# It additionally provides a local cluster_node_id symbol (real backend
# gets it from cluster_guc.o; unit test stubs it as a plain int).
test_cluster_gviews: test_cluster_gviews.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_VIEWS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VIEWS_O) -o $@

# test_cluster_ic links cluster_ic.o standalone.  cluster_ic.c references
# pg_crc32c hardware functions and the ereport machinery (errstart /
# errcode / errmsg / errfinish) plus cluster_node_id; the test stubs all
# of them since the unit test only takes function addresses (never
# invokes a path that triggers ereport or CRC computation).
test_cluster_ic: test_cluster_ic.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_IC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_O) -o $@

# test_cluster_conf links cluster_conf.o standalone.  cluster_conf.c
# references ereport / ShmemInitStruct / AllocateFile / FreeFile /
# cluster_node_id / cluster_config_file / fmgr glue; all stubbed in
# test_cluster_conf.c since the unit test only takes addresses and
# reads constants (never invokes the parser body or SRF body).
test_cluster_conf: test_cluster_conf.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_CONF_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CONF_O) -o $@

# test_cluster_ic_mock links cluster_ic.o standalone (same as
# test_cluster_ic but exercises the mock vtable additions and the four
# cluster_ic_mock_* SRF entry points shipped at stage 0.26).  The unit
# test only takes addresses and reads constants -- the SRF bodies and
# any ereport / palloc / tuplestore paths are not invoked, so the
# stubs in test_cluster_ic_mock.c suffice.
test_cluster_ic_mock: test_cluster_ic_mock.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_IC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_O) -o $@


# ----------
# Run all tests sequentially
# ----------
check: all
	@echo "# pgrac cluster unit tests"
	@total=0; failed=0; \
	for t in $(TESTS); do \
		echo "# Running $$t"; \
		./$$t || failed=$$((failed + 1)); \
		total=$$((total + 1)); \
	done; \
	if [ $$failed -ne 0 ]; then \
		echo "# $$failed of $$total binaries failed"; \
		exit 1; \
	else \
		echo "# All $$total binaries passed"; \
	fi


# ----------
# Standard PG targets
# ----------
clean distclean maintainer-clean:
	rm -f $(TESTS)

install installdirs uninstall installcheck:
	@:

.PHONY: all check clean distclean maintainer-clean install installdirs \
        uninstall installcheck
