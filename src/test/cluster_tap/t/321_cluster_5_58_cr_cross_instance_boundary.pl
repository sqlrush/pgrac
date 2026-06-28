#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 321_cluster_5_58_cr_cross_instance_boundary.pl
#	  spec-5.58 D3 (HG#3) — cross-instance CR boundary stays fail-closed with the
#	  WHOLE Stage 5.5 CR read-path band live.
#
#	  spec-5.57 proved the cross-instance boundary fail-closes and is observable;
#	  spec-5.58 HG#3 proves NO band optimization (shared pool / admission / tuple
#	  fast path / per-rel generation / resolver memo / L1 cache) opens the
#	  cross-instance data path: a class-③ runtime-warm-remote CR origin ALWAYS
#	  raises 53R9G, regardless of every band GUC AND of the
#	  cluster.cross_instance_cr_coordinator mode (including off).  The coordinator
#	  GUC gates ONLY the observability counters; the fail-closed boundary is 8.A
#	  non-degradable.
#
#	    L1  band-all-on node up; cr_coord category exposes its 4 frozen counters.
#	    L2  boundary mode (default): class-③ inject -> 53R9G AND
#	        cross_instance_cr_refused advances (the boundary fires + is observed,
#	        with the full band live).
#	    L3  off mode (PGC_SIGHUP reload): STILL 53R9G, but the counter does NOT
#	        advance (off gates observability only -- the spec-5.58 HG#3 contract;
#	        the boundary itself is non-degradable).
#	    L4  forward mode: STILL 53R9G (contract placeholder; never opens the data
#	        path) + a single LOG names the Stage 6 data plane.
#	    L5  probe mode (PGC_USERSET, boundary): STILL 53R9G + the boundary-probe
#	        counter advances.
#
#	  Honest scope: a REAL foreign-origin undo chain needs true shared storage
#	  (Stage 6 data plane); the isolated-storage harness drives the SAME
#	  fail-closed path via the cr_cross_instance injection (the boundary is
#	  injection-twinned in cluster_cr.c).  No data-plane result is fabricated.
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/321_cluster_5_58_cr_cross_instance_boundary.pl
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

use Test::More;
use Time::HiRes qw(usleep);
use PgracClusterNode;

my $node = PgracClusterNode->new('cr_xib');
$node->init;
$node->append_conf('postgresql.conf', <<'CONF');
cluster.allow_single_node = on
cluster.node_id = 0
autovacuum = off
cluster.cr_gate_no_peer_fastpath = off
cluster.shared_cr_pool_enabled = on
cluster.shared_cr_pool_size_blocks = 256
cluster.cr_pool_admission_policy = scan_resistant
cluster.cr_tuple_level_fastpath = on
cluster.cr_pool_rel_generation_slots = 64
cluster.resolver_cache_enabled = on
cluster.resolver_cache_measure = on
cluster.resolver_cache_entries = 1024
cluster.cr_cache_max_blocks = 64
CONF
$node->start;

sub coord_counter
{
	my ($key) = @_;
	return ($node->safe_psql('postgres',
		qq{SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state
		   WHERE category='cr_coord' AND key='$key'), 0)}) + 0);
}

# Arm cr_cross_instance + invoke a construct in one fresh backend (picks up the
# current PGC_SIGHUP coordinator mode), return the captured stderr.
sub inject_class3
{
	my $s = $node->background_psql('postgres', on_error_stop => 0);
	$s->query_safe('SELECT 1');	# process any pending SIGHUP first
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','skip',5)");
	$s->query("SELECT cluster_cr_test_construct('t_xib'::regclass,0,0,100)");
	my $err = $s->{stderr};
	$s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','none',0)");
	$s->quit;
	return $err;
}

$node->safe_psql('postgres', 'CREATE TABLE t_xib (id int, v text)');
$node->safe_psql('postgres', "INSERT INTO t_xib VALUES (1,'a')");

# ---- L1: registry --------------------------------------------------------
my $nrows = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='cr_coord'});
is($nrows, '4', 'L1 cr_coord category exposes its 4 frozen counters (band on)');

# ---- L2: boundary mode (default) -- fail-closed + observed ----------------
my $b2 = coord_counter('cross_instance_cr_refused');
my $e2 = inject_class3();
like($e2, qr/(53R9G|cross-instance)/, 'L2 class-③ is fail-closed 53R9G with the full band live');
cmp_ok(coord_counter('cross_instance_cr_refused'), '>', $b2,
	'L2 cross_instance_cr_refused advances (boundary fires + observed)');

# ---- L3: off mode -- STILL 53R9G, counter frozen (HG#3 contract) ----------
$node->append_conf('postgresql.conf', "cluster.cross_instance_cr_coordinator = off\n");
$node->reload;
usleep(300_000);
my $b3 = coord_counter('cross_instance_cr_refused');
my $e3 = inject_class3();
like($e3, qr/(53R9G|cross-instance)/,
	'L3 off mode: boundary STILL fail-closed 53R9G (8.A non-degradable)');
is(coord_counter('cross_instance_cr_refused'), $b3,
	'L3 off mode: counter does NOT advance (off gates observability only -- HG#3)');

# ---- L4: forward mode -- STILL 53R9G + Stage 6 LOG ------------------------
my $log_pos = -s $node->logfile;
$node->append_conf('postgresql.conf', "cluster.cross_instance_cr_coordinator = forward\n");
$node->reload;
usleep(300_000);
my $e4 = inject_class3();
like($e4, qr/(53R9G|cross-instance)/, 'L4 forward mode: STILL fail-closed 53R9G (never opens data path)');
my $tail = '';
if (open(my $lf, '<', $node->logfile)) { seek($lf, $log_pos, 0); local $/; $tail = <$lf>; close($lf); }
like($tail, qr/Stage 6/, 'L4 forward mode logs the Stage 6 data-plane notice');

# ---- L5: probe mode -- STILL 53R9G + probe counter advances ---------------
$node->append_conf('postgresql.conf', "cluster.cross_instance_cr_coordinator = boundary\n");
$node->reload;
usleep(300_000);
{
	my $s = $node->background_psql('postgres', on_error_stop => 0);
	$s->query_safe('SELECT 1');
	$s->query_safe('SET cluster.cross_instance_cr_probe = on');
	my $bp = coord_counter('cross_instance_boundary_probe');
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','skip',5)");
	$s->query("SELECT cluster_cr_test_construct('t_xib'::regclass,0,0,100)");
	my $e5 = $s->{stderr}; $s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','none',0)");
	$s->quit;
	like($e5, qr/(53R9G|cross-instance)/, 'L5 probe mode: STILL fail-closed 53R9G (behavior unchanged)');
	cmp_ok(coord_counter('cross_instance_boundary_probe'), '>', $bp,
		'L5 probe mode: cross_instance_boundary_probe advances');
}

$node->stop;
done_testing();
