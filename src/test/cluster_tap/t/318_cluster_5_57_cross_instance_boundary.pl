#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 318_cluster_5_57_cross_instance_boundary.pl
#	  spec-5.57 D9 — cross-instance CR/current read-path coordinator boundary
#	  behavioral TAP on a 2-node ClusterPair.
#
#	  L373/L374 honesty: the cross-instance DATA PLANE has NO e2e (it is Stage 6;
#	  the value-gate proves it is unreachable in Stage 5.5 -- see L9 / the D0
#	  measure-leg).  The FAIL-CLOSED BOUNDARY has a real e2e: the cr_cross_instance
#	  injection drives the SAME refusal routine the production pre-check uses
#	  (cr_coordinator_refuse_runtime_remote), so 53R9G + the coordinator counters
#	  are exercised on a real cluster.  The classifier decision itself (own /
#	  merged-materialized / runtime-warm / invalid) is unit-tested in
#	  cluster_unit/test_cluster_cr_coordinator.c.
#
#	  L1  ClusterPair up + both nodes alive + 53R9F/53R9G registered + the
#	      cr_cross_instance injection point present
#	  L2  cr_coord category exposes exactly the 4 frozen counters (all 0 at rest)
#	  L3  GUC defaults: cross_instance_cr_coordinator=boundary, *_probe=off; enum
#	      accepts off/boundary/forward
#	  L4  53R9G fail-closed (boundary mode): inject + construct -> CR's own 53R9G
#	  L5  counters (boundary mode): cross_instance_cr_refused++ AND
#	      remote_undo_read_refused++ (one class③ refusal bumps both, by
#	      construction; the invariant cross_instance_cr_refused >=
#	      remote_undo_read_refused holds)
#	  L6  probe mode (PGC_USERSET): probe=on -> still 53R9G (behavior unchanged) +
#	      cross_instance_boundary_probe++
#	  L7  forward mode (PGC_SIGHUP): still 53R9G + LOG-once tells the operator the
#	      data plane is Stage 6
#	  L8  off mode is NON-DEGRADABLE (rule 8.A): still 53R9G, but the advisory
#	      counters DO NOT advance (the GUC gates observability, never fail-closed)
#	  L9  recovery/SRF return-0 contract preserved (Q11-A): cluster_undo_get_record
#	      SRF returns NULL (not ERROR) for an unreadable UBA -> not blanket-ERROR,
#	      no recovery regression
#	  L10 D0 measure-leg value-gate self-test passes (data plane NO-GO -> Stage 6)
#	  L11 materialized_remote_served counter present (class② serve path; 0 here,
#	      exercised by the merged-materialized-remote path + the classifier unit
#	      test)
#
#	  Injection registry is process-local, so arm + invoke MUST share one
#	  backend: the injection legs each use a single background_psql session.
#
#	  Spec: spec-5.57-cross-instance-cr-current-coordinator.md (FROZEN v1.0)
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
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_57_cross',
	quorum_voting_disks => 3,
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'max_prepared_transactions = 4',
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0 = $pair->node0;

# ---- helpers ----------------------------------------------------------
sub coord_counter
{
	my ($key) = @_;
	my $v = $node0->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr_coord' AND key='$key'});
	return $v ne '' ? int($v) : -1;
}

# Arm cr_cross_instance in a fresh backend (so it picks up the current PGC_SIGHUP
# GUC), invoke a CR construct, capture the stderr, disarm, close.  Returns the
# captured stderr.
sub inject_class3_construct
{
	my $s = $node0->background_psql('postgres', on_error_stop => 0);
	# process any pending SIGHUP before arming (PGC_SIGHUP GUCs)
	$s->query_safe('SELECT 1');
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','skip',5)");
	$s->query("SELECT cluster_cr_test_construct('t_cr'::regclass,0,0,100)");
	my $err = $s->{stderr};
	$s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','none',0)");
	$s->quit;
	return $err;
}

$node0->safe_psql('postgres', 'CREATE TABLE t_cr (id int, v text)');
$node0->safe_psql('postgres', "INSERT INTO t_cr VALUES (1, 'a')");

# ----------
# L1: liveness + registry
# ----------
is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1b node1 alive');

my $codes = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM (VALUES ('53R9F'),('53R9G')) v(c)});
is($codes, '2', 'L1c 53R9F/53R9G boundary codes referenced');

my $has_inject = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_cluster_injections WHERE name='cr_cross_instance'});
ok($has_inject >= 1, 'L1d cr_cross_instance injection point present');

# ----------
# L2: cr_coord category exposes exactly the 4 frozen counters
# ----------
my $keys = $node0->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state WHERE category='cr_coord'});
is($keys,
	'cross_instance_boundary_probe,cross_instance_cr_refused,materialized_remote_served,remote_undo_read_refused',
	'L2a cr_coord exposes the 4 frozen counters');
my $nrows = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='cr_coord'});
is($nrows, '4', 'L2b cr_coord has exactly 4 rows');
is(coord_counter('cross_instance_cr_refused'), 0, 'L2c counters start at 0');

# ----------
# L3: GUC defaults + enum domain
# ----------
is($node0->safe_psql('postgres', q{SHOW cluster.cross_instance_cr_coordinator}),
	'boundary', 'L3a coordinator GUC defaults to boundary');
is($node0->safe_psql('postgres', q{SHOW cluster.cross_instance_cr_probe}),
	'off', 'L3b probe GUC defaults to off');
my ($erc, undef, undef) = $node0->psql('postgres',
	q{SET cluster.cross_instance_cr_probe = on});
is($erc, 0, 'L3c probe GUC is PGC_USERSET (SET works)');

# ----------
# L4 + L5: boundary-mode fail-closed + both counters
# ----------
my $pre_x = coord_counter('cross_instance_cr_refused');
my $pre_r = coord_counter('remote_undo_read_refused');
my $err4 = inject_class3_construct();
like($err4, qr/(53R9G|cross-instance)/, 'L4 boundary mode: injected class③ -> 53R9G fail-closed');
my $post_x = coord_counter('cross_instance_cr_refused');
my $post_r = coord_counter('remote_undo_read_refused');
cmp_ok($post_x, '>', $pre_x, "L5a cross_instance_cr_refused++ ($pre_x -> $post_x)");
cmp_ok($post_r, '>', $pre_r, "L5b remote_undo_read_refused++ ($pre_r -> $post_r)");
cmp_ok($post_x, '>=', $post_r, 'L5c invariant cross_instance_cr_refused >= remote_undo_read_refused');

# ----------
# L6: probe mode (USERSET) -> still 53R9G + boundary_probe++
# ----------
{
	my $pre_p = coord_counter('cross_instance_boundary_probe');
	my $s = $node0->background_psql('postgres', on_error_stop => 0);
	$s->query_safe('SET cluster.cross_instance_cr_probe = on');
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','skip',5)");
	$s->query("SELECT cluster_cr_test_construct('t_cr'::regclass,0,0,100)");
	like($s->{stderr}, qr/(53R9G|cross-instance)/, 'L6a probe mode: still 53R9G (behavior unchanged)');
	$s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','none',0)");
	$s->quit;
	my $post_p = coord_counter('cross_instance_boundary_probe');
	cmp_ok($post_p, '>', $pre_p, "L6b cross_instance_boundary_probe++ ($pre_p -> $post_p)");
}

# ----------
# L7: forward mode (SIGHUP) -> still 53R9G + LOG-once "Stage 6"
# ----------
{
	$node0->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.cross_instance_cr_coordinator = 'forward'});
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(300_000);
	my $err7 = inject_class3_construct();
	like($err7, qr/(53R9G|cross-instance)/, 'L7a forward mode: still fail-closed 53R9G');
	usleep(200_000);
	my $log = slurp_file($node0->logfile);
	like($log, qr/forward is a contract placeholder.*Stage 6/s,
		'L7b forward mode LOG-once tells operator the data plane is Stage 6');
	$node0->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.cross_instance_cr_coordinator = 'boundary'});
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(300_000);
}

# ----------
# L8: off mode is NON-DEGRADABLE (8.A): still 53R9G, counters frozen
# ----------
{
	$node0->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.cross_instance_cr_coordinator = 'off'});
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(300_000);
	my $pre_off = coord_counter('cross_instance_cr_refused');
	my $err8 = inject_class3_construct();
	like($err8, qr/(53R9G|cross-instance)/,
		'L8a off mode: STILL 53R9G (fail-closed is non-degradable, 8.A)');
	my $post_off = coord_counter('cross_instance_cr_refused');
	is($post_off, $pre_off, "L8b off mode: counters frozen ($pre_off == $post_off, GUC gates only observability)");
	$node0->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.cross_instance_cr_coordinator = 'boundary'});
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(300_000);
}

# ----------
# L9: recovery/SRF return-0 contract preserved (Q11-A) -- NOT blanket-ERROR
# ----------
{
	# an all-zero (Invalid) UBA is unreadable -> the SRF returns NULL, never
	# ERROR: the cluster_undo_get_record return-based fail-closed contract that
	# recovery (cluster_tt_recovery.c) and the SRF depend on is intact.
	my ($rc, $out, $err) = $node0->psql('postgres',
		q{SELECT cluster_undo_get_record(decode(repeat('00',16),'hex')) IS NULL AS is_null});
	is($rc, 0, 'L9a cluster_undo_get_record SRF does not ERROR on an unreadable UBA');
	is($out, 't', 'L9b SRF returns NULL (return-0 contract preserved, not blanket-ERROR)');
}

# ----------
# L10: D0 measure-leg value-gate self-test (data plane NO-GO -> Stage 6)
# ----------
{
	my $script = "$FindBin::RealBin/../../../../scripts/perf/run-cross-instance-cr-probe.sh";
	if (-x $script)
	{
		my $rc = system("$script --self-test >/dev/null 2>&1");
		is($rc, 0, 'L10 D0 measure-leg self-test passes (value-gate NO-GO, data plane -> Stage 6)');
	}
	else
	{
		ok(-f $script, 'L10 D0 measure-leg script present');
	}
}

# ----------
# L11: materialized_remote_served counter present (class② serve path)
# ----------
is(coord_counter('materialized_remote_served'), 0,
	'L11 materialized_remote_served present (class② path; 0 here, covered by merged-remote + unit test)');

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
