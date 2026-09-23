#-------------------------------------------------------------------------
#
# Makefile for src/bin/pgrac_fenced
#
#-------------------------------------------------------------------------

PGFILEDESC = "pgrac-fenced - privileged external-fence coordinator"

subdir = src/bin/pgrac_fenced
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

FENCED_OBJS = pgrac_fenced.o pgrac_fenced_config.o pgrac_fenced_core.o \
	pgrac_fenced_journal.o pgrac_fenced_provider.o pgrac_fenced_ipmi.o \
	pgrac_fenced_operation.o pgrac_fenced_session.o

FENCED_OBJS += pgrac_fenced_dispatch.o pgrac_fenced_runtime.o \
	pgrac_fenced_schedule.o pgrac_fenced_async.o pgrac_fenced_rejoin.o
FENCED_OBJS += pgrac_fenced_rejoin_async.o pgrac_fenced_coordinator.o \
	pgrac_fenced_rejoin_coordinator.o pgrac_fenced_drain.o pgrac_fenced_pacemaker.o
FENCEDCTL_OBJS = pgrac-fencedctl.o pgrac_fenced_ctl.o \
	pgrac_fenced_core.o pgrac_fenced_journal.o \
	$(top_builddir)/src/common/pgrac_external_fence_protocol.o
PGRAC_FENCED_LIBS = $(top_builddir)/src/common/libpgcommon.a \
	$(top_builddir)/src/port/libpgport.a

all: pgrac-fenced pgrac-fencedctl

pgrac_fenced_pacemaker.o: override CPPFLAGS += $(PACEMAKER_CFLAGS)

pgrac-fenced: $(FENCED_OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(PGRAC_FENCED_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(PACEMAKER_LIBS) $(LIBS) -o $@$(X)

pgrac-fencedctl: $(FENCEDCTL_OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(PGRAC_FENCED_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pgrac-fenced$(X) '$(DESTDIR)$(bindir)/pgrac-fenced$(X)'
	$(INSTALL_PROGRAM) pgrac-fencedctl$(X) '$(DESTDIR)$(bindir)/pgrac-fencedctl$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pgrac-fenced$(X)'
	rm -f '$(DESTDIR)$(bindir)/pgrac-fencedctl$(X)'

clean distclean maintainer-clean:
	rm -f pgrac-fenced$(X) pgrac-fencedctl$(X) \
		$(FENCED_OBJS) pgrac-fencedctl.o pgrac_fenced_ctl.o
