# Test-only native transaction callbacks, never part of the production binary.
MODULE_big = test_pgrac_abort_owner
OBJS = test_pgrac_abort_owner.o
PGFILEDESC = "test_pgrac_abort_owner - native abort terminal witness"

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/cluster_tap/abort_probe
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
