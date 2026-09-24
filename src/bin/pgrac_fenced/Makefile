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
# PGRAC: versioned complete intent remains in the one existing journal chain.
FENCED_OBJS += pgrac_fenced_journal_intent.o
FENCEDCTL_OBJS = pgrac-fencedctl.o pgrac_fenced_ctl.o \
	pgrac_fenced_core.o pgrac_fenced_journal.o pgrac_fenced_journal_intent.o \
	$(top_builddir)/src/common/pgrac_external_fence_protocol.o
PGRAC_FENCED_LIBS = $(top_builddir)/src/common/libpgcommon.a \
	$(top_builddir)/src/port/libpgport.a

# PGRAC: target-side verification reuses the frontend common verifier.
all: pgrac-fenced pgrac-fencedctl pgrac-fenced-map-verify

pgrac_fenced_pacemaker.o: override CPPFLAGS += $(PACEMAKER_CFLAGS)

pgrac-fenced: $(FENCED_OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(PGRAC_FENCED_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(PACEMAKER_LIBS) $(LIBS) -o $@$(X)

pgrac-fencedctl: $(FENCEDCTL_OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(PGRAC_FENCED_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

pgrac-fenced-map-verify: pgrac-fenced-map-verify.o pgrac_fenced_map_filter.o | submake-libpgport
	$(CC) $(CFLAGS) $^ $(PGRAC_FENCED_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pgrac-fenced$(X) '$(DESTDIR)$(bindir)/pgrac-fenced$(X)'
	$(INSTALL_PROGRAM) pgrac-fencedctl$(X) '$(DESTDIR)$(bindir)/pgrac-fencedctl$(X)'
	$(INSTALL_PROGRAM) pgrac-fenced-map-verify$(X) '$(DESTDIR)$(bindir)/pgrac-fenced-map-verify$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pgrac-fenced$(X)'
	rm -f '$(DESTDIR)$(bindir)/pgrac-fencedctl$(X)'
	rm -f '$(DESTDIR)$(bindir)/pgrac-fenced-map-verify$(X)'

clean distclean maintainer-clean:
	rm -f pgrac-fenced$(X) pgrac-fencedctl$(X) pgrac-fenced-map-verify$(X) \
		$(FENCED_OBJS) pgrac-fencedctl.o pgrac_fenced_ctl.o \
		pgrac-fenced-map-verify.o pgrac_fenced_map_filter.o
