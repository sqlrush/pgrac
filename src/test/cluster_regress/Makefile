#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pgrac cluster SQL regression tests.
#
# IDENTIFICATION
#    src/test/cluster_regress/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile.  Stage 0.4 reserved this
#    directory as a placeholder; stage 0.21 fills it with a real
#    pg_regress driver and the cluster_smoke test (column contracts +
#    GUC metadata for spec 0.13/0.16/0.17/0.18/0.19).
#
#    Stage 2+ contributors append tests by:
#      1. Writing a new sql/<name>.sql .
#      2. Adding "test: <name>" to parallel_schedule .
#      3. Running "make check" once -- it will produce
#         results/<name>.out which (after a careful manual review for
#         host / port / OID drift) is copied to expected/<name>.out
#         and committed.
#
#    See spec-0.21-cluster-regress.md and the README in this directory.
#
#-------------------------------------------------------------------------

subdir = src/test/cluster_regress
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global


# REGRESS_OPTS appended to the standard pg_regress_check macro so that
# we drive tests off the schedule file (which lets Stage 2+ contributors
# add tests with a single line per test rather than editing this file).
REGRESS_OPTS = --schedule=$(srcdir)/parallel_schedule


all:
	@:    # nothing to build at this stage; pg_regress is the harness


# pg_regress_check (defined in Makefile.global) launches a temp
# postmaster, runs every test the schedule lists, and diff's the
# output against expected/.
check: submake-pg-regress
	$(pg_regress_check) $(REGRESS_OPTS)

# installcheck reuses an already-installed PG instance instead of
# spinning up a temp one.  Useful in CI matrices but not used by
# the local check target.
installcheck: submake-pg-regress
	$(pg_regress_installcheck) $(REGRESS_OPTS)

# Make sure the pg_regress binary is built before we try to run it.
.PHONY: submake-pg-regress
submake-pg-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress pg_regress$(X)


clean distclean maintainer-clean:
	rm -rf $(srcdir)/tmp_check $(srcdir)/results \
	       $(srcdir)/regression.diffs $(srcdir)/regression.out \
	       $(srcdir)/log $(srcdir)/output_iso


install installdirs uninstall:
	@:


.PHONY: all check installcheck clean distclean maintainer-clean \
        install installdirs uninstall
