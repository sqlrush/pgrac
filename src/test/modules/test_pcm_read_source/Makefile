MODULE_big = test_pcm_read_source
OBJS = $(WIN32RES) test_pcm_read_source.o
PGFILEDESC = "test_pcm_read_source - native first-read race fixture"
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pcm_read_source
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
