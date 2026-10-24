#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/buffer
#
# IDENTIFICATION
#    src/backend/storage/buffer/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/buffer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	buf_init.o \
	bufmgr_barrier.o \
	buf_table.o \
	bufmgr.o \
	freelist.o \
	localbuf.o

include $(top_srcdir)/src/backend/common.mk

# Native opt-in diagnostic publisher layout.
bufmgr.o: $(top_srcdir)/src/include/cluster/cluster_content_trace.h
