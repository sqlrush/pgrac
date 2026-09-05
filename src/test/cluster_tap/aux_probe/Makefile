# src/test/cluster_tap/aux_probe/Makefile
# Explicitly built test module; not linked into a production server.

MODULE_big = test_pgrac_aux_consumer
OBJS = test_pgrac_aux_consumer.o
PGFILEDESC = "test_pgrac_aux_consumer - auxiliary-page consumer probe"

subdir = src/test/cluster_tap/aux_probe
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
