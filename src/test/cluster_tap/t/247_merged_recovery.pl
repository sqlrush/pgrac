#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 247_merged_recovery.pl
#    spec-4.5 -- k-way SCN merged recovery: the REACHABLE surface.
#
#    A-closure (2026-06-11): there is no real shared-data storage
#    backend yet (cluster_shared_fs LOCAL writes each node's own
#    PGDATA via relpathperm; STUB is pure md.c -- spec-3.18 V-2).  A
#    crashed peer's SHARED-storage page therefore cannot be honestly
#    applied to shared storage, so merged recovery is CAPABILITY-GATED
#    off in production: cluster.merged_recovery=on + crash candidates
#    -> FATAL 53RA3 'not supported without a shared-data storage
#    backend'.  This file verifies exactly the reachable surface and
#    SKIPs the true two-node shared-page apply-through with that
#    documented code-fact reason (not "test not written").
#
#      L1  merged_recovery=off: crash recovery is single-stream, the
#          node's own rows survive (today's behaviour, byte-identical)
#      L2  merged_recovery=on, NO candidate: not engaged, normal
#          single-stream recovery
#      L3  merged_recovery=on + a forged stale-ACTIVE candidate slot:
#          the capability gate FATALs 53RA3 'not supported ... roadmap
#          4.5a' -- never a silent single-stream fallback
#      L4  after the gate FATAL, merged_recovery=off recovers the
#          node's own stream cleanly (the candidate slot is observed,
#          not acted upon)
#      SKIP true two-node shared-page cold-merge apply-through:
#          requires a shared-data backend (roadmap 4.5a)
#
#    NB: this is a Perl TAP file -- never run clang-format on it.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.5-kway-scn-merge-replay.md (FROZEN v1.0, A-closure)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PgracWalState qw(crc32c read_file_raw write_file_raw);
use PostgreSQL::Test::Utils;
use Test::More;

# Forge a stale-ACTIVE candidate slot for thread $tid (node $tid-1).
sub forge_candidate
{
	my ($regfile, $tid) = @_;
	my $img = read_file_raw($regfile);
	my $off = 512 + ($tid - 1) * 512;
	my $slot = "\0" x 512;
	substr($slot, 0, 20) = pack('LSSlLL', 0x50475754, 1, $tid, $tid - 1, 1, 1);
	substr($slot, 24, 8) = pack('q', 1);
	substr($slot, 32, 8) = pack('q', 1000);
	substr($slot, 504, 4) = pack('L', crc32c(substr($slot, 0, 504)));
	substr($img, $off, 512) = $slot;
	write_file_raw($regfile, $img);
}

my $wroot = PostgreSQL::Test::Utils::tempdir();
my $regfile = "$wroot/pgrac_wal_state";

my $node = PgracClusterNode->new('merged_a');
$node->init(extra => [ '-X', "$wroot/thread_4" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 3\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.recovery_stale_active_ms = 1000\n"
	  . "cluster.recovery_workers_max = 0\n"
	  . "autovacuum = off\n");

# L1: merged_recovery=off -> single-stream crash recovery.
$node->append_conf('postgresql.conf', "cluster.merged_recovery = off\n");
$node->start;
$node->safe_psql('postgres',
	'CREATE TABLE s (a int); INSERT INTO s SELECT generate_series(1, 150)');
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
	'150', 'L1 merged_recovery=off: single-stream crash recovery survives');

# L2: merged_recovery=on, no candidate -> not engaged.
$node->append_conf('postgresql.conf', "cluster.merged_recovery = on\n");
$node->restart;
$node->safe_psql('postgres', 'INSERT INTO s SELECT generate_series(151, 300)');
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
	'300', 'L2 merged_recovery=on with no candidate: normal recovery');

# L3: merged_recovery=on + forged candidate -> capability gate 53RA3.
$node->stop('immediate');
forge_candidate($regfile, 5);
my $log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0,
	'L3 start refused when merged_recovery=on meets a crash candidate');
my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/merged k-way recovery is not supported without a shared-data storage backend/,
	'L3 capability gate FATALs 53RA3 (not a silent single-stream fallback)');
like($log, qr/roadmap 4\.5a/, 'L3 the hint points at the shared-data backend prerequisite');

# L4: back to off -> own stream recovers, candidate observed only.
$node->adjust_conf('postgresql.conf', 'cluster.merged_recovery', 'off');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
	'300', 'L4 merged_recovery=off recovers own stream past the forged candidate');
is($node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state "
	  . "WHERE category='recovery' AND key='plan_crashed_candidates'"),
	'5', 'L4 the candidate is observed in the plan, not acted upon');
$node->stop;

# SKIP: true two-node shared-page cold-merge apply-through.
SKIP: {
	skip "true shared-page apply-through needs a shared-data storage backend "
	  . "(cluster_shared_fs local/stub is per-node, not shared; roadmap 4.5a)", 1;
	fail "unreachable: two-node shared-page merged replay not testable yet";
}

done_testing();
