#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/transam
#
# IDENTIFICATION
#    src/backend/access/transam/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/transam
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	clog.o \
	commit_ts.o \
	generic_xlog.o \
	multixact.o \
	parallel.o \
	rmgr.o \
	slru.o \
	subtrans.o \
	timeline.o \
	transam.o \
	twophase.o \
	twophase_rmgr.o \
	varsup.o \
	xact.o \
	xlog.o \
	xlogarchive.o \
	xlogbackup.o \
	xlogfuncs.o \
	xloginsert.o \
	xlogprefetcher.o \
	xlogreader.o \
	xlogrecovery.o \
	xlogstats.o \
	xlogutils.o

include $(top_srcdir)/src/backend/common.mk

# ensure that version checks in xlog.c get recompiled when catversion.h changes
xlog.o: xlog.c $(top_srcdir)/src/include/catalog/catversion.h

# PGRAC: native cold recovery owns value-sized IR guards. Rebuild on carrier
# changes even when autodepend is disabled. Author: SqlRush <sqlrush@gmail.com>
xlog.o: $(top_srcdir)/src/include/cluster/cluster_ir.h

# PGRAC: keep native predicates and physical origin inspection identical.
# Author: SqlRush <sqlrush@gmail.com>
clog.o subtrans.o multixact.o commit_ts.o: \
	$(top_srcdir)/src/include/cluster/cluster_native_startup.h
