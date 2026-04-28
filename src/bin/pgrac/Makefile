#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pgrac client tools (pgrac-init, pgrac-start)
#
# IDENTIFICATION
#    src/bin/pgrac/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile.  Stage 0.20 ships pgrac-init and
#    pgrac-start as bash scripts (no compile step); they wrap the existing
#    PG initdb / pg_ctl tools and write pgrac.conf during cluster
#    bootstrap.  Stage 2+ may add more cluster-aware tooling here (e.g.
#    multi-node SSH bootstrap, cluster_status); when that arrives some
#    scripts may be rewritten in C and this Makefile will gain compile
#    rules at that time.
#
#    See specs/spec-0.20-init-tools.md.
#
#-------------------------------------------------------------------------

subdir = src/bin/pgrac
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# The shell scripts that this directory ships.  They are shipped verbatim
# from the source tree (no preprocessing); $(INSTALL_SCRIPT) handles the
# 0755 permission bits.
SCRIPTS = pgrac-init pgrac-start

all: $(SCRIPTS)

# Sanity-check that the source files exist and are executable in the
# source tree.  Avoids "no such file" surprises during install.
$(SCRIPTS):
	@test -x $(srcdir)/$@ || (chmod +x $(srcdir)/$@ && test -x $(srcdir)/$@)

install: all installdirs
	$(INSTALL_SCRIPT) $(srcdir)/pgrac-init  '$(DESTDIR)$(bindir)/pgrac-init'
	$(INSTALL_SCRIPT) $(srcdir)/pgrac-start '$(DESTDIR)$(bindir)/pgrac-start'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pgrac-init' '$(DESTDIR)$(bindir)/pgrac-start'

clean distclean maintainer-clean:
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

.PHONY: all install installdirs uninstall clean distclean \
        maintainer-clean check installcheck $(SCRIPTS)
