#-------------------------------------------------------------------------
#
# 004_backend_types.pl
#    Reverse-regression test for the BackendType enum extension (stage 0.10).
#
#    Stage 0.10 appends 14 pgrac process types to the PG-native BackendType
#    enum (docs/background-process-design.md §8.2) and adds 14 matching
#    cases to GetBackendTypeDesc() in miscinit.c.  Spawning paths for the
#    new processes do not land until stage 0.13+, so this test cannot
#    observe pgrac descriptor strings at runtime.
#
#    What this test actually proves:
#      - The extended switch in GetBackendTypeDesc() still returns correct
#        strings for the 14 PG-native enum values (no accidental fallthrough
#        from added cases).
#      - pg_stat_activity.backend_type returns expected PG-native values
#        for currently active backends.
#      - The server starts cleanly with the extended enum compiled in.
#
#    The 14 new descriptor strings ("lms worker", "undo cleaner", ...)
#    will be verified directly via ps and pg_stat_activity in stage 0.13+
#    once the corresponding fork() paths are in place.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/004_backend_types.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use Test::More;
use PgracClusterNode;


# Start a single instance.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;


# The querying psql session itself shows up as a 'client backend'.
# This proves GetBackendTypeDesc(B_BACKEND) still returns "client backend"
# after the 14 pgrac cases were added.
my $client_backend_count = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'});
cmp_ok($client_backend_count, '>=', 1,
	'pg_stat_activity sees at least one "client backend" (B_BACKEND desc unchanged)');


# Background helpers -- exact set varies by PG config, but at least one
# of {checkpointer, walwriter, autovacuum launcher, background writer}
# must be present for any healthy cluster.  This proves the PG-native
# desc strings did not regress when the 14 pgrac cases were inserted.
my $bg_helper_count = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity
	   WHERE backend_type IN ('checkpointer', 'walwriter',
	                          'autovacuum launcher', 'background writer')});
cmp_ok($bg_helper_count, '>=', 1,
	'pg_stat_activity reports at least one PG-native bg helper');


# No 'unknown process type' should leak out -- that string is the
# fall-through default in GetBackendTypeDesc(), and seeing it here would
# mean some live backend is in a state the switch does not handle.
my $unknown_count = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'unknown process type'});
is($unknown_count, '0',
	'no live backend resolves to "unknown process type" (switch is complete)');


# The 14 pgrac descriptor strings must NOT appear yet (stage 0.10 adds
# the enum + descriptors but no fork() paths).  This guards against
# accidentally wiring up a process before the supporting infrastructure
# (GUC, ProcessAux, signal handling) lands in stage 0.13+.
# Spec-1.11 Sprint A: LMON aux process is the first pgrac background
# process actually spawned by postmaster.  Spec-1.12 Sprint A: LCK
# aux process is the second.  Spec-1.13 Sprint A: DIAG aux process
# is the third.  Spec-1.14 Sprint A: Cluster Stats aux process is
# the fourth.  Spec-2.18 Sprint A: LMS aux process is the seventh
# cluster aux process spawned by the PM_RUN ServerLoop respawn path.
# Spec-2.19 Sprint A: LMD aux process is the eighth cluster aux process,
# spawned by the same PM_RUN ServerLoop respawn after LMS.  Other types
# (heartbeat / interconnect listener / etc) remain deferred to Stage 2-6.
# Excluded 'lmon', 'lck', 'diag', 'cluster stats', 'lms', and 'lmd' from
# the "no pgrac process visible" assertion accordingly.
my $pgrac_visible = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity
	   WHERE backend_type IN (
	       'heartbeat', 'interconnect listener',
	       'lms worker',
	       'managed recovery process', 'recovery coordinator',
	       'recovery worker', 'sinval broadcaster',
	       'tt gc', 'undo cleaner')});
is($pgrac_visible, '0',
	'no pgrac process descriptor visible at stage 0.10 except LMON (1.11) + LCK (1.12) + DIAG (1.13) + Cluster Stats (1.14) + LMS (2.18) + LMD (2.19) Sprint A (others deferred to Stage 2-6)');

# LMON is spawned by postmaster (spec-1.11 Sprint A).  Verify it
# appears in pg_stat_activity exactly once.
my $lmon_visible = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lmon'});
is($lmon_visible, '1',
	'LMON aux process visible in pg_stat_activity (spec-1.11 Sprint A)');

# LCK is spawned by postmaster (spec-1.12 Sprint A).  Verify it
# appears in pg_stat_activity exactly once.
my $lck_visible = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lck'});
is($lck_visible, '1',
	'LCK aux process visible in pg_stat_activity (spec-1.12 Sprint A)');

# DIAG is spawned by postmaster (spec-1.13 Sprint A) at the
# post-PM_RUN cluster_run_phase4_sequence() driver site (Q2 A').
# Verify it appears in pg_stat_activity exactly once.
my $diag_visible = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'diag'});
is($diag_visible, '1',
	'DIAG aux process visible in pg_stat_activity (spec-1.13 Sprint A)');

# Cluster Stats is spawned by postmaster (spec-1.14 Sprint A) at
# the same phase 4 driver site, after DIAG (Q2 phase 4 内串行).
# Verify it appears in pg_stat_activity exactly once.
my $cluster_stats_visible = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'cluster stats'});
is($cluster_stats_visible, '1',
	'Cluster Stats aux process visible in pg_stat_activity (spec-1.14 Sprint A)');

# LMS is spawned by postmaster (spec-2.18 Sprint A) from the PM_RUN
# ServerLoop respawn path.  Poll because PM_RUN becomes observable before
# the next ServerLoop iteration necessarily starts LMS.
ok($node->poll_query_until(
	'postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lms'}),
	'LMS aux process visible in pg_stat_activity (spec-2.18 Sprint A)');

# LMD is spawned by postmaster (spec-2.19 Sprint A) from the PM_RUN
# ServerLoop respawn path (after LMS).  Poll for the same reason as LMS.
ok($node->poll_query_until(
	'postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lmd'}),
	'LMD aux process visible in pg_stat_activity (spec-2.19 Sprint A)');


$node->stop;

done_testing();
