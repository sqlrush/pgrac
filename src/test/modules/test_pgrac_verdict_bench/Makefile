# src/test/modules/test_pgrac_verdict_bench/Makefile
#
# Opt-in measurement module: deliberately NOT listed in
# src/test/modules/Makefile, so "make check" never builds or runs it.
# Build and install it explicitly:
#   make -C src/test/modules/test_pgrac_verdict_bench install

MODULE_big = test_pgrac_verdict_bench
OBJS = \
	$(WIN32RES) \
	test_pgrac_verdict_bench.o
PGFILEDESC = "test_pgrac_verdict_bench - own-xid verdict resolver micro-benchmark"

EXTENSION = test_pgrac_verdict_bench
DATA = test_pgrac_verdict_bench--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pgrac_verdict_bench
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
