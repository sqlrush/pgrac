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

EXTRA_INSTALL=


# ----------
# PG-standard TAP test entry points
# ----------
check:
	$(prove_check)

installcheck:
	$(prove_installcheck)


# ----------
# Cleanup
# ----------
clean distclean maintainer-clean:
	rm -rf tmp_check

install installdirs uninstall:
	@:

.PHONY: check installcheck clean distclean maintainer-clean \
        install installdirs uninstall
