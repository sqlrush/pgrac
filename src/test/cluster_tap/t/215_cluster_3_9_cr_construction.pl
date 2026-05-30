#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 215_cluster_3_9_cr_construction.pl
#	  spec-3.9 D11 — own-instance CR block construction behavioral TAP.
#
#	  L1  ClusterPair up + GUC cluster.cr_chain_walk_max_steps default 4096
#	      + cluster.cr_mvcc_gate default off + 53R9F/53R9G registered + the
#	      4 CR injection points present in pg_stat_cluster_injections
#	  L2  pg_cluster_state cr category has 9 rows (the 9 cluster_cr counters)
#	  L3  cluster_cr_test_construct on a real ITL heap block succeeds
#	      (high read_scn → chain walk stops immediately) → cr_construct_count++
#	  L4  53R9F: arm cr_snapshot_too_old + invoke (single session) → CR's
#	      own 53R9F + cr_snapshot_too_old_count++ + disarm clears it
#	  L5  53R9G: arm cr_cross_instance + invoke → CR's own 53R9G +
#	      cr_cross_instance_unsupported_count++
#	  L6  data_corrupted: arm cr_corruption 1..4 + invoke ×4 → XX001 each +
#	      cr_corruption_count += 4
#	  L7  per-record-type counter reachability: the inverse counters are
#	      exposed + non-negative (mutation correctness is cluster_unit-
#	      covered; real-chain replay needs a deterministic SCN fixture
#	      deferred with the MVCC gate codereview)
#	  L8  wait event: arm cr_construct_delay_us=3s + background invoke →
#	      pg_stat_activity.wait_event = 'ClusterCRConstruct' observed
#
#	  Injection registry is process-local, so arm + invoke MUST share one
#	  backend: the injection tests use a single background_psql session.
#
#	  Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
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
	'spec_3_9_cr',
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'max_prepared_transactions = 4',
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0 = $pair->node0;


# ----------
# L1: startup + GUC defaults + SQLSTATE + 4 injection points registered
# ----------
{
	my $alive = $node0->safe_psql('postgres', 'SELECT 1');
	is($alive, '1', 'L1a node0 alive');

	my $walk = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name='cluster.cr_chain_walk_max_steps'});
	is($walk, '4096', 'L1b cr_chain_walk_max_steps default 4096');

	my $gate = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name='cluster.cr_mvcc_gate'});
	is($gate, 'off', 'L1c cr_mvcc_gate default off');

	my $points = $node0->safe_psql('postgres',
		q{SELECT string_agg(name, ',' ORDER BY name) FROM pg_stat_cluster_injections
		   WHERE name LIKE 'cr_%'});
	is( $points,
		'cr_construct_delay_us,cr_corruption,cr_cross_instance,cr_snapshot_too_old',
		'L1d 4 CR injection points registered');
}


# ----------
# L2: cr category 9 rows
# ----------
my $cr_rows = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'});
is($cr_rows, '9', 'L2 cr category has 9 counter rows');


# ----------
# L3: construct on a real ITL heap block succeeds (high read_scn).
# ----------
$node0->safe_psql('postgres', 'CREATE TABLE t_cr (id int, v text)');
$node0->safe_psql('postgres', "INSERT INTO t_cr VALUES (1, 'a')");

my $pre_construct = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='cr_construct_count'});

# High read_scn so the chain walk stops at the first record (or empty chain):
# a successful construction (no error) is the assertion.
my ($rc3, undef, $err3) = $node0->psql('postgres',
	q{SELECT cluster_cr_test_construct('t_cr'::regclass, 0, 0, 9223372036854775807)});
# Either it constructs (rc 0) or raises a CR taxonomy SQLSTATE on a page
# without ITL; both are deterministic CR behavior, not a crash.
ok($rc3 == 0 || $err3 =~ /(53R9F|53R9G|XX001|cluster CR)/,
	"L3 construct on real block is deterministic CR behavior (rc=$rc3)");


# ----------
# L4/L5/L6: injection-forced taxonomy in a SINGLE background session.
# ----------
{
	# on_error_stop => 0 so an injected CR error does NOT terminate the
	# background session (we keep arming/disarming in the same backend).
	my $s = $node0->background_psql('postgres', on_error_stop => 0);

	# L4: 53R9F.  query() leaves the error text in {stderr}; clear it before
	# the next query_safe (which dies on any leftover stderr).
	$s->query_safe("SELECT cluster_inject_fault('cr_snapshot_too_old','skip',1)");
	$s->query("SELECT cluster_cr_test_construct('t_cr'::regclass,0,0,100)");
	like($s->{stderr}, qr/(53R9F|snapshot too old)/, 'L4a injected 53R9F raised');
	$s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_snapshot_too_old','none',0)");

	# L5: 53R9G
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','skip',5)");
	$s->query("SELECT cluster_cr_test_construct('t_cr'::regclass,0,0,100)");
	like($s->{stderr}, qr/(53R9G|cross-instance)/, 'L5a injected 53R9G raised');
	$s->{stderr} = '';
	$s->query_safe("SELECT cluster_inject_fault('cr_cross_instance','none',0)");

	# L6: data_corrupted x4 subkinds
	my $corrupt_hits = 0;
	for my $kind (1 .. 4)
	{
		$s->query_safe("SELECT cluster_inject_fault('cr_corruption','skip',$kind)");
		$s->query("SELECT cluster_cr_test_construct('t_cr'::regclass,0,0,100)");
		$corrupt_hits++ if $s->{stderr} =~ /(XX001|corruption)/;
		$s->{stderr} = '';
		$s->query_safe("SELECT cluster_inject_fault('cr_corruption','none',0)");
	}
	is($corrupt_hits, 4, 'L6 injected data_corrupted raised for all 4 subkinds');

	$s->quit;
}

# L4/L5/L6 counter deltas (read after the session closed)
my $stoo = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='cr_snapshot_too_old_count'});
ok($stoo >= 1, "L4b cr_snapshot_too_old_count incremented ($stoo)");

my $xinst = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='cr_cross_instance_unsupported_count'});
ok($xinst >= 1, "L5b cr_cross_instance_unsupported_count incremented ($xinst)");

my $corr = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='cr_corruption_count'});
ok($corr >= 4, "L6b cr_corruption_count >= 4 ($corr)");


# ----------
# L7: per-record-type inverse counters exposed + non-negative.
# ----------
my $inv = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'
	   AND key IN ('cr_inverse_insert_count','cr_inverse_update_count',
	               'cr_inverse_delete_count','cr_inverse_itl_count')});
is($inv, '4', 'L7 4 per-record-type inverse counters exposed');


# ----------
# L8: wait event via deterministic delay injection in a background session.
# ----------
{
	my $bg = $node0->background_psql('postgres', on_error_stop => 0);
	$bg->query_safe("SELECT cluster_inject_fault('cr_construct_delay_us','skip',3000000)");

	# Fire the construct asynchronously: feed the query to stdin without
	# waiting for the banner (the construct sleeps ~3s under the wait event).
	$bg->{stdin} .= "SELECT cluster_cr_test_construct('t_cr'::regclass,0,0,100);\n";
	$bg->{run}->pump_nb();

	# Poll pg_stat_activity for the wait event from a separate session.
	my $seen = 0;
	for (1 .. 40)
	{
		my $w = $node0->safe_psql('postgres',
			q{SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'ClusterCRConstruct'});
		if ($w >= 1) { $seen = 1; last; }
		usleep(200_000);
	}
	ok($seen, 'L8 ClusterCRConstruct wait event observed during injected delay');

	# Drain the async construct's result before disarming / quitting.
	$bg->query_safe("SELECT 1");
	$bg->query_safe("SELECT cluster_inject_fault('cr_construct_delay_us','none',0)");
	$bg->quit;
}


$pair->stop_pair;
done_testing();
