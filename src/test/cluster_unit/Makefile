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
        test_cluster_shmem test_cluster_signal

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

# Most tests link only cluster_version.o.  test_cluster_guc /
# test_cluster_shmem / test_cluster_signal have separate rules
# because they also link additional cluster_*.o objects (the test
# files stub the PG backend symbols those objects reference).
SIMPLE_TESTS = $(filter-out test_cluster_guc test_cluster_shmem test_cluster_signal,$(TESTS))

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
