#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pgrac cluster TAP integration tests.
#
# IDENTIFICATION
#    src/test/cluster_tap/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile.  It mirrors the structure of
#    src/test/recovery/Makefile: tests in t/*.pl are driven by perl
#    prove via PG's standard $(prove_check) rule.
#
#    Requires --enable-tap-tests at configure time.  Without it,
#    $(prove_check) is empty and `make check` is a no-op.
#
#-------------------------------------------------------------------------

subdir = src/test/cluster_tap
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

EXTRA_INSTALL=contrib/pageinspect

all:

ifeq ($(PORTNAME), linux)
all: pgrac_direct_io_probe
endif

pgrac_direct_io_probe: bin/pgrac_direct_io_probe.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LDFLAGS) -o $@


# ----------
# PG-standard TAP test entry points
# ----------
check: all
	$(prove_check)

# The PRE2 scope filter uses full filenames: shared numeric prefixes must
# not exclude unrelated fault, consistency or fail-closed coverage.
PRE2_DEFERRED = $(shell awk '!/^\#/ && NF {print $$1}' $(srcdir)/pre2-deferred.list)
PRE2_TESTS = $(filter-out $(PRE2_DEFERRED),$(if $(strip $(PROVE_TESTS)),$(PROVE_TESTS),$(patsubst $(srcdir)/%,%,$(wildcard $(srcdir)/t/*.pl))))

pre2-list:
	@printf '%s\n' $(PRE2_TESTS)

check-pre2:
	@test -n '$(strip $(PRE2_TESTS))' || { echo 'ERROR: empty PRE2 TAP selection'; exit 1; }
	$(MAKE) check PROVE_TESTS='$(PRE2_TESTS)'

installcheck:
	$(prove_installcheck)


# ----------
# Cleanup
# ----------
clean distclean maintainer-clean:
	rm -rf tmp_check pgrac_direct_io_probe

install installdirs uninstall:
	@:

.PHONY: all check check-pre2 pre2-list installcheck clean distclean maintainer-clean \
        install installdirs uninstall
