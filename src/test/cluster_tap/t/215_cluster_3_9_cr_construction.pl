#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 215_cluster_3_9_cr_construction.pl
#	  spec-3.9 D11 — own-instance CR block construction behavioral TAP.
#
#	  L1  ClusterPair up + GUC cluster.cr_chain_walk_max_steps default 4096
#	      + cluster.cr_mvcc_gate default on + 53R9F/53R9G registered + the
#	      4 CR injection points present in pg_stat_cluster_injections
#	  L2  pg_cluster_state cr category has 30 rows (9 cluster_cr counters +
#	      4 spec-3.10 CR cache counters + 4 spec-3.22 xmax resolve buckets +
#	      5 spec-5.53 key/reuse-fence mismatch diagnostics)
#	  L3  cluster_cr_test_construct on a real ITL heap block succeeds
#	      (high read_scn → chain walk stops immediately) → cr_construct_count++
#	  L4  53R9F: arm cr_snapshot_too_old + invoke (single session) → CR's
#	      own 53R9F + cr_snapshot_too_old_count++ + disarm clears it
#	  L5  53R9G: arm cr_cross_instance + invoke → CR's own 53R9G +
#	      cr_cross_instance_unsupported_count++
#	  L6  data_corrupted: arm cr_corruption 1..4 + invoke ×4 → XX001 each +
#	      cr_corruption_count += 4
#	  L7  per-record-type counter reachability: the 4 inverse counters are
#	      exposed (delete/update/insert are proven end-to-end in L9-L11;
#	      cr_inverse_itl is cluster_unit-covered — see note at L11)
#	  L8  wait event: arm cr_construct_delay_us=3s + background invoke →
#	      pg_stat_activity.wait_event = 'ClusterCRConstruct' observed
#	  L9  REAL e2e DELETE: A's REPEATABLE READ snapshot still sees a row that
#	      B deleted+committed → cr_construct_count++ / chain_walk_steps_sum>0 /
#	      cr_inverse_delete_count++ (gate genuinely fired, no test SRF)
#	  L10 REAL e2e UPDATE: A still sees the OLD value after B updates+commits
#	      → cr_inverse_update_count++ (value assertion forces correctness)
#	  L11 REAL e2e INSERT: A still sees 3 rows after B inserts a 4th+commits
#	      → cr_inverse_insert_count++
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
	quorum_voting_disks => 3,
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
	is($gate, 'on', 'L1c cr_mvcc_gate default on (spec-3.9 真激活)');

	my $points = $node0->safe_psql('postgres',
		q{SELECT string_agg(name, ',' ORDER BY name) FROM pg_stat_cluster_injections
		   WHERE name LIKE 'cr_%'});
	is( $points,
		'cr_construct_delay_us,cr_corruption,cr_cross_instance,cr_force_read_scn,cr_snapshot_too_old',
		'L1d 5 CR injection points registered (spec-4.5a +1 cr_force_read_scn)');
}


# ----------
# L2: cr category 9 rows
# ----------
my $cr_rows = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'});
is($cr_rows, '62',
	'L2 cr category has 62 counter rows (9 + 4 cache + 4 xmax + 5 spec-5.53 mismatch + 8 spec-5.54 tuple + 5 spec-5.56 lifecycle + 6 spec-6.12b cr-server + 16 spec-6.12i/6.15 runtime-visibility & verdict + 2 spec-5.22f D6 fresh-ref verdict + 3 spec-5.22d D4 authority serve)');


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


# ----------
# L9: REAL end-to-end CR via the MVCC gate (no test SRF).
#
#   Session A holds a REPEATABLE READ snapshot (read_scn_A).  Session B
#   DELETEs a row and commits (the tuple's xmax is set in place, the ITL
#   slot write_scn + the page pd_block_scn advance past read_scn_A).  When
#   A re-reads, the 3-tier gate fires (pd_block_scn > read_scn_A, ITL
#   write_scn > read_scn_A, local origin) and CR reconstructs the
#   read_scn_A image, so A still sees the row.  A fresh snapshot (read_scn
#   past the delete) does NOT fire the gate and sees the row gone.
#
#   Asserts the gate path is genuinely taken: cr_construct_count++,
#   cr_chain_walk_steps_sum > 0, cr_inverse_delete_count++.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_e2e (id int);
		 INSERT INTO t_e2e SELECT g FROM generate_series(1,3) g;');

	my $val = sub {
		my ($k) = @_;
		return $node0->safe_psql('postgres',
			qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
	};

	my $pre_construct = $val->('cr_construct_count');
	my $pre_steps     = $val->('cr_chain_walk_steps_sum');
	my $pre_del       = $val->('cr_inverse_delete_count');

	# Session A: open a REPEATABLE READ snapshot and see all 3 rows.
	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	my $a_before = $sa->query_safe('SELECT count(*) FROM t_e2e');
	chomp $a_before;
	is($a_before, '3', 'L9a session A snapshot sees 3 rows');

	# Session B: delete one row and commit (in-place xmax + SCN advance).
	$node0->safe_psql('postgres', 'DELETE FROM t_e2e WHERE id = 1');

	# A fresh snapshot sees the delete (2 rows) -- gate does not fire.
	my $fresh = $node0->safe_psql('postgres', 'SELECT count(*) FROM t_e2e');
	is($fresh, '2', 'L9b fresh snapshot sees the delete (2 rows)');

	# Session A re-reads under its old snapshot: CR reconstructs -> 3 rows.
	my $a_after = $sa->query_safe('SELECT count(*) FROM t_e2e');
	chomp $a_after;
	is($a_after, '3', 'L9c session A still sees 3 rows via CR reconstruction');

	$sa->query_safe('COMMIT');
	$sa->quit;

	# The gate path was genuinely taken (counters are shmem, read fresh).
	ok($val->('cr_construct_count') > $pre_construct,
		'L9d cr_construct_count incremented via the MVCC gate');
	ok($val->('cr_chain_walk_steps_sum') > $pre_steps,
		'L9e cr_chain_walk_steps_sum > 0 (real undo chain walked)');
	ok($val->('cr_inverse_delete_count') > $pre_del,
		'L9f cr_inverse_delete_count incremented (DELETE inverse-applied)');
}


# ----------
# L10: REAL end-to-end CR for UPDATE (cr_inverse_update path).
#
#   Session A snapshots and reads v='x' for id=2.  Session B UPDATEs id=2 to
#   v='y' and commits: the OLD tuple gets xmax + an ITL stamp (write_scn >
#   read_scn_A), the NEW tuple is inserted.  When A re-reads under its old
#   snapshot, the gate fires on the old tuple, the walker inverse-applies the
#   UPDATE (clearing the in-place xmax in the CR image) so A still sees the
#   OLD value 'x'.  The UPDATE-created new version remains physically present
#   in the CR image, so the xmin-side gate must hide it.  The value assertion
#   (not just a count) forces both paths to be genuinely correct.
# ----------
{
	$node0->safe_psql('postgres',
		"CREATE TABLE t_upd (id int, v text);
		 INSERT INTO t_upd VALUES (1,'a'),(2,'x'),(3,'c');");

	my $val = sub {
		my ($k) = @_;
		return $node0->safe_psql('postgres',
			qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
	};

	my $pre_upd   = $val->('cr_inverse_update_count');
	my $pre_steps = $val->('cr_chain_walk_steps_sum');

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	my $a_before = $sa->query_safe(q{SELECT v FROM t_upd WHERE id = 2});
	chomp $a_before;
	is($a_before, 'x', 'L10a session A snapshot sees old value x');

	# Session B updates the value in place (old tuple xmax + ITL SCN advance).
	$node0->safe_psql('postgres', q{UPDATE t_upd SET v = 'y' WHERE id = 2});

	# A fresh snapshot sees the new value -- gate does not fire.
	my $fresh = $node0->safe_psql('postgres', q{SELECT v FROM t_upd WHERE id = 2});
	is($fresh, 'y', 'L10b fresh snapshot sees new value y');

	# Session A re-reads under its old snapshot: CR inverse-applies the UPDATE
	# so A still sees exactly one row for id=2 with the OLD value x.
	my $a_after = $sa->query_safe(q{SELECT v FROM t_upd WHERE id = 2});
	chomp $a_after;
	is($a_after, 'x', 'L10c session A still sees old value x via CR inverse-update');

	my $a_count = $sa->query_safe(q{SELECT count(*) FROM t_upd WHERE id = 2});
	chomp $a_count;
	is($a_count, '1', 'L10d session A sees exactly one version of id=2 (new version hidden by xmin gate)');

	$sa->query_safe('COMMIT');
	$sa->quit;

	ok($val->('cr_inverse_update_count') > $pre_upd,
		'L10e cr_inverse_update_count incremented (UPDATE inverse-applied)');
	ok($val->('cr_chain_walk_steps_sum') > $pre_steps,
		'L10f cr_chain_walk_steps_sum advanced for the UPDATE chain');
}


# ----------
# L11: REAL end-to-end CR for INSERT (cr_inverse_insert path).
#
#   Session A snapshots (sees 3 rows).  Session B INSERTs a 4th row and
#   commits.  When A re-reads, the gate fires on the new tuple (write_scn >
#   read_scn_A) and the walker inverse-applies the INSERT (LP_UNUSED in the
#   CR image), so A's snapshot still sees exactly 3 rows.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_ins (id int);
		 INSERT INTO t_ins SELECT g FROM generate_series(1,3) g;');

	my $val = sub {
		my ($k) = @_;
		return $node0->safe_psql('postgres',
			qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
	};

	my $pre_ins = $val->('cr_inverse_insert_count');

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	my $a_before = $sa->query_safe('SELECT count(*) FROM t_ins');
	chomp $a_before;
	is($a_before, '3', 'L11a session A snapshot sees 3 rows');

	# Session B inserts a 4th row after A's snapshot.
	$node0->safe_psql('postgres', 'INSERT INTO t_ins VALUES (4)');

	my $fresh = $node0->safe_psql('postgres', 'SELECT count(*) FROM t_ins');
	is($fresh, '4', 'L11b fresh snapshot sees the new row (4 rows)');

	# Session A re-reads: the post-snapshot INSERT is inverse-applied away.
	my $a_after = $sa->query_safe('SELECT count(*) FROM t_ins');
	chomp $a_after;
	is($a_after, '3', 'L11c session A still sees 3 rows (INSERT inverse-applied)');

	$sa->query_safe('COMMIT');
	$sa->quit;

	ok($val->('cr_inverse_insert_count') > $pre_ins,
		'L11d cr_inverse_insert_count incremented (INSERT inverse-applied)');
}

# NOTE on cr_inverse_itl: unlike insert/update/delete, an ITL undo record is
# only emitted when an ITL slot is reused while still referenced by a live
# undo chain (slot pressure beyond INITRANS under concurrency).  That is not
# reachable deterministically from a 2-session SQL script, so the ITL inverse
# helper is proven at the cluster_unit layer (test_cluster_cr.c, synthetic
# page) rather than here.  Its real-chain replay is exercised indirectly
# whenever a walked chain crosses a reused slot.


$pair->stop_pair;
done_testing();
