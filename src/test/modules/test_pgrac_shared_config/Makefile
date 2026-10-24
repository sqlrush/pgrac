# PGRAC: native shared-configuration policy tests (not installed by default).
# Author: SqlRush <sqlrush@gmail.com>
MODULE_big = test_pgrac_shared_config
OBJS = $(WIN32RES) test_pgrac_shared_config.o test_pgrac_wal_history_native.o test_pgrac_catalog_native.o test_pgrac_storage_sync.o test_pgrac_wait_graph.o
PGFILEDESC = "test_pgrac_shared_config - native parameter policy tests"
EXTENSION = test_pgrac_shared_config
DATA = test_pgrac_shared_config--1.0.sql
TAP_TESTS = 1
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pgrac_shared_config
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
test_pgrac_wal_history_native.o: $(top_srcdir)/src/backend/cluster/cluster_wal_history.c

test_pgrac_wait_graph.o: $(top_srcdir)/src/include/cluster/cluster_wait_graph.h \
	$(top_srcdir)/src/include/cluster/cluster_content_trace.h \
	$(top_srcdir)/src/include/cluster/cluster_conf.h \
	$(top_srcdir)/src/include/cluster/cluster_semantic_activation.h \
	$(top_srcdir)/src/include/cluster/cluster_sf_dep.h \
	$(top_srcdir)/src/include/cluster/cluster_grd.h
