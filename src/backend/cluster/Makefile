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

OBJS = \
	cluster.o \
	cluster_elog.o \
	cluster_version.o

include $(top_srcdir)/src/backend/common.mk
