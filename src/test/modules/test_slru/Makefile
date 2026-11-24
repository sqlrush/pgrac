# src/test/modules/test_slru/Makefile

MODULE_big = test_slru
OBJS = \
	$(WIN32RES) \
	test_slru.o
PGFILEDESC = "test_slru - test module for SLRUs"

EXTENSION = test_slru
DATA = test_slru--1.0.sql

REGRESS_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_slru/test_slru.conf
REGRESS = test_slru
# Disabled because these tests require "shared_preload_libraries=test_slru",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

# Keep the module's SLRU ABI tied to its selected server headers.
# Author: SqlRush <sqlrush@gmail.com>
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
test_slru.o: $(includedir_server)/access/slru.h
else
subdir = src/test/modules/test_slru
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
test_slru.o: $(top_srcdir)/src/include/access/slru.h
endif
