#-------------------------------------------------------------------------
#
# Makefile for src/test
#
# Copyright (c) 1994, Regents of the University of California
#
# src/test/Makefile
#
#-------------------------------------------------------------------------
#
# PGRAC MODIFICATIONS by SqlRush <sqlrush@gmail.com>
#
# What changed:
#     Conditionally append cluster_unit and cluster_regress to SUBDIRS
#     when --enable-cluster is set.  cluster_unit hosts the pgrac
#     C unit-test framework (no PG runtime needed).  cluster_regress
#     hosts SQL regression tests (placeholder in stage 0.4).
#
# Why:
#     Wires the two new test directories into PG's build/test system
#     so that "make -C src/test" recurses into them when cluster mode
#     is enabled.  Disabled mode keeps the binary identical to native
#     PG, so cluster tests are skipped.
#
#     See spec-0.4-unit-test-framework.md §5.1.
#
#-------------------------------------------------------------------------

subdir = src/test
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = perl regress isolation modules authentication recovery subscription

# PGRAC: cluster tests included only when --enable-cluster
# (spec-0.4 added cluster_unit + cluster_regress; spec-0.5 added cluster_tap)
ifeq ($(enable_pgrac_cluster), yes)
SUBDIRS += cluster_unit cluster_regress cluster_tap
endif

# PGRAC: convenience targets for cluster test suites
# (spec-0.4 §2.4 introduced cluster-unit / cluster-regress / cluster-check;
#  spec-0.5 §2.3 added cluster-tap and extended cluster-check to include it.)
# Available in both enable and disable modes; disable mode no-ops cleanly.
ifeq ($(enable_pgrac_cluster), yes)
cluster-unit:
	$(MAKE) -C cluster_unit check

cluster-tap:
	$(MAKE) -C cluster_tap check

cluster-regress:
	$(MAKE) -C cluster_regress check

cluster-check: cluster-unit cluster-tap cluster-regress
else
cluster-unit cluster-tap cluster-regress cluster-check:
	@echo "# cluster tests skipped (rebuild with --enable-cluster to run)"
endif

.PHONY: cluster-unit cluster-tap cluster-regress cluster-check

ifeq ($(with_icu),yes)
SUBDIRS += icu
endif
ifeq ($(with_gssapi),yes)
SUBDIRS += kerberos
endif
ifeq ($(with_ldap),yes)
SUBDIRS += ldap
endif
ifeq ($(with_ssl),openssl)
SUBDIRS += ssl
endif

# Test suites that are not safe by default but can be run if selected
# by the user via the whitespace-separated list in variable PG_TEST_EXTRA.
# Export PG_TEST_EXTRA to check it in individual tap tests.
export PG_TEST_EXTRA

# We don't build or execute these by default, but we do want "make
# clean" etc to recurse into them.  (We must filter out those that we
# have conditionally included into SUBDIRS above, else there will be
# make confusion.)
ALWAYS_SUBDIRS = $(filter-out $(SUBDIRS),examples kerberos icu ldap ssl)

# We want to recurse to all subdirs for all standard targets, except that
# installcheck and install should not recurse into the subdirectory "modules".

recurse_alldirs_targets := $(filter-out installcheck install, $(standard_targets))
installable_dirs := $(filter-out modules, $(SUBDIRS))

$(call recurse,$(recurse_alldirs_targets))
$(call recurse,installcheck, $(installable_dirs))
$(call recurse,install, $(installable_dirs))

$(recurse_always)
