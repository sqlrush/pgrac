#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for the pgrac cluster subsystem (stage 0.2 skeleton)
#
# IDENTIFICATION
#    src/backend/cluster/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile (no derivation from PostgreSQL).
#    Stage 0.2 keeps OBJS minimal; subsystem subdirectories (grd/, gcs/,
#    ges/, scn/, ...) will be added in later feature points and listed
#    in SUBDIRS at that time.
#
#-------------------------------------------------------------------------

subdir = src/backend/cluster
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# In --enable-cluster builds we compile every cluster source file.  In
# --disable-cluster builds we still need cluster_views.o so that
# pg_proc.dat's reference to cluster_get_wait_events resolves at link
# time (the SRF body itself is gated by #ifdef USE_PGRAC_CLUSTER and
# returns an empty result set in disable mode).  All other cluster_*.o
# objects are skipped so that the disable-mode postgres binary remains
# symbol-equivalent to vanilla PG -- the spec-0.3 contract verified by
# the CI "Verify postgres binary has NO cluster symbols" step.
ifeq ($(enable_pgrac_cluster), yes)
OBJS = \
	cluster.o \
	cluster_conf.o \
	cluster_elog.o \
	cluster_guc.o \
	cluster_ic.o \
	cluster_shmem.o \
	cluster_signal.o \
	cluster_version.o \
	cluster_views.o
else
# cluster_conf.o, cluster_views.o, cluster_ic.o are always linked
# because pg_proc.dat references their SRF entry points uncondition-
# ally.  In disable-cluster builds the SRF bodies #ifdef out and
# raise ERRCODE_FEATURE_NOT_SUPPORTED, but the function symbols must
# still resolve at link time.
OBJS = cluster_conf.o cluster_ic.o cluster_views.o
endif

include $(top_srcdir)/src/backend/common.mk
