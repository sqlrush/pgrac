#-------------------------------------------------------------------------
#
# 309_cluster_5_54_cr_tuple_verdict.pl
#	  spec-5.54 D8 — tuple-level / verdict-only CR read fast path differential.
#
#	  Proves on a REAL own-instance CR path (not by-construction, L341) that the
#	  fast-path verdict is BIT-EQUIVALENT to the authoritative full-block verdict
#	  (INV-T1), that it actually fires for nchains==1 while AVOIDING the full-block
#	  construct (the value proposition), that nchains>1 falls back to full-block
#	  with the correct verdict (INV-T6), and that GUC off is byte-faithful to
#	  pre-5.54 (pure full-block).  Single cluster-enabled node, two same-node
#	  sessions (mirror t/215/t/300); cross-instance CR = spec-5.57.
#
#	  L1   GUC off: snapshot sees the pre-update value via full-block CR (baseline).
#	  L2   GUC on, nchains==1 UPDATE: fast verdict == the off verdict AND
#	       cr_tuple_verdict_count rises AND cr_construct_count stays FLAT (the fast
#	       path computed the verdict without materializing the whole block).
#	  L3   GUC on, post-snapshot DELETE: the row was live at read_scn -> still
#	       visible; on/off identical (xmax-side verdict equivalence).
#	  L4   nchains>1 (two writer txns touch the same block after the snapshot):
#	       cr_tuple_fallback_multichain_count rises AND the verdict is correct
#	       (== the off/full-block verdict) -- fallback is not a downgrade.
#	  L5   visible-row-set differential: a post-snapshot UPDATE batch yields the
#	       EXACT same snapshot row set with the fast path on vs off (no false-
#	       visible / no false-invisible).
#	  L6   GUC off after on: counters frozen, pure full-block (zero behavior change).
#
#	  Perf tier note: correctness differential (nightly stage5 shard); absolute
#	  timings / the default flip are spec-5.58, not asserted here.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Portions Copyright (c) 2026, pgrac contributors
#
#	  Spec: spec-5.54-tuple-level-cr-verdict-only.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Single cluster-enabled node; cr_gate_no_peer_fastpath=off forces the gate to
# really construct on a single node, autovacuum off keeps the cr counters quiescent.
my $node = PostgreSQL::Test::Cluster->new('spec_5_54_cr_tuple');
$node->init;
$node->append_conf(
	'postgresql.conf', join("\n",
		'cluster.enabled = on',
		'cluster.node_id = 0',
		'cluster.allow_single_node = on',
		'cluster.interconnect_tier = stub',
		'cluster.cr_mvcc_gate = on',
		'cluster.cr_gate_no_peer_fastpath = off',
		'autovacuum = off',
		''));
$node->start;

# Instance-wide cr counter accessor.
my $cr = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};

# Run a self-contained nchains==1 scenario on a FRESH row: an RR reader snapshots,
# a single writer UPDATEs the row after the snapshot, the reader re-reads it (the
# CR verdict).  Returns the value the snapshot sees.  $fastpath toggles the GUC.
my $run_update = sub {
	my ($fastpath, $tbl, $id) = @_;
	my $r = $node->background_psql('postgres', on_error_die => 1);
	$r->query_safe('SET cluster.cr_tuple_level_fastpath = ' . ($fastpath ? 'on' : 'off'));
	$r->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$r->query_safe("SELECT v FROM $tbl WHERE id = $id");    # take snapshot
	$node->safe_psql('postgres', "UPDATE $tbl SET v = v + 100 WHERE id = $id");
	my $seen = $r->query_safe("SELECT v FROM $tbl WHERE id = $id");    # CR verdict
	$r->query_safe('COMMIT');
	$r->quit;
	chomp $seen;
	return $seen;
};

# ----------
# L1: GUC off baseline — full-block CR shows the pre-update value.
# ----------
$node->safe_psql('postgres',
	'CREATE TABLE t_upd (id int primary key, v int); INSERT INTO t_upd VALUES (1, 1), (2, 1);');
is($run_update->(0, 't_upd', 1), '1', 'L1 GUC off: snapshot sees pre-update value (full-block CR)');

# ----------
# L2: GUC on, nchains==1 — fast verdict == off verdict, fast path fires, no construct.
# ----------
my $v0 = $cr->('cr_tuple_verdict_count');
my $c0 = $cr->('cr_construct_count');
is($run_update->(1, 't_upd', 2), '1', 'L2 GUC on nchains==1: fast verdict == full-block (pre-update value)');
ok($cr->('cr_tuple_verdict_count') > $v0, 'L2 fast path fired (cr_tuple_verdict_count rose)');
is($cr->('cr_construct_count'), $c0,
	'L2 fast path avoided the full-block construct (cr_construct_count flat)');

# ----------
# L3: GUC on, post-snapshot DELETE — row was live at read_scn -> still visible; on==off.
# ----------
my $run_delete = sub {
	my ($fastpath) = @_;
	$node->safe_psql('postgres',
		'DROP TABLE IF EXISTS t_del; CREATE TABLE t_del (id int, v int); INSERT INTO t_del VALUES (1, 7);');
	my $r = $node->background_psql('postgres', on_error_die => 1);
	$r->query_safe('SET cluster.cr_tuple_level_fastpath = ' . ($fastpath ? 'on' : 'off'));
	$r->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$r->query_safe('SELECT v FROM t_del WHERE id = 1');
	$node->safe_psql('postgres', 'DELETE FROM t_del WHERE id = 1');
	my $seen = $r->query_safe('SELECT count(*) FROM t_del WHERE id = 1');
	$r->query_safe('COMMIT');
	$r->quit;
	chomp $seen;
	return $seen;
};
is($run_delete->(0), '1', 'L3a GUC off: post-snapshot DELETE still visible to snapshot');
is($run_delete->(1), '1', 'L3b GUC on: post-snapshot DELETE verdict == full-block (still visible)');

# ----------
# L4: nchains>1 — two writer txns modify the same block after the snapshot.  The
#     fast path is not eligible (nchains>1) -> fallback_multichain, and the
#     verdict is the correct full-block value (fallback is not a downgrade).
# ----------
$node->safe_psql('postgres',
	"CREATE TABLE t_mc (id int, v int); INSERT INTO t_mc SELECT g, g FROM generate_series(1, 4) g;");
my $fb0 = $cr->('cr_tuple_fallback_multichain_count');
my $rmc = $node->background_psql('postgres', on_error_die => 1);
$rmc->query_safe('SET cluster.cr_tuple_level_fastpath = on');
$rmc->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
$rmc->query_safe('SELECT count(*) FROM t_mc');    # snapshot
# two SEPARATE committed txns (two ITL slots -> nchains==2) on the same block:
$node->safe_psql('postgres', 'UPDATE t_mc SET v = v + 100 WHERE id = 1');
$node->safe_psql('postgres', 'UPDATE t_mc SET v = v + 100 WHERE id = 2');
my $mc_seen = $rmc->query_safe('SELECT v FROM t_mc WHERE id = 1');    # construct, multi-chain
$rmc->query_safe('COMMIT');
$rmc->quit;
chomp $mc_seen;
is($mc_seen, '1', 'L4a nchains>1: verdict is the correct full-block value (fallback, no downgrade)');
ok($cr->('cr_tuple_fallback_multichain_count') > $fb0,
	'L4b nchains>1: cr_tuple_fallback_multichain_count rose (fell back to full-block)');

# ----------
# L5: visible-row-set differential — the SAME post-snapshot UPDATE batch yields the
#     EXACT same snapshot row set with the fast path on vs off (no false-visible).
# ----------
my $run_rowset = sub {
	my ($fastpath) = @_;
	$node->safe_psql('postgres',
		'DROP TABLE IF EXISTS t_rs; CREATE TABLE t_rs (id int, v int);
		 INSERT INTO t_rs SELECT g, g FROM generate_series(1, 6) g;');
	my $r = $node->background_psql('postgres', on_error_die => 1);
	$r->query_safe('SET cluster.cr_tuple_level_fastpath = ' . ($fastpath ? 'on' : 'off'));
	$r->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$r->query_safe('SELECT count(*) FROM t_rs');    # snapshot
	# one writer txn updates every row (nchains==1 per row's block) after the snapshot:
	$node->safe_psql('postgres', 'UPDATE t_rs SET v = v + 1000');
	my $rows = $r->query_safe(q{SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM t_rs});
	$r->query_safe('COMMIT');
	$r->quit;
	chomp $rows;
	return $rows;
};
my $rs_off = $run_rowset->(0);
my $rs_on = $run_rowset->(1);
is($rs_on, $rs_off, 'L5 visible row set identical fast-path on vs off (no false-visible/invisible)');
is($rs_off, '1:1,2:2,3:3,4:4,5:5,6:6',
	'L5 (sanity) snapshot sees the pre-update row set');

# ----------
# L6: GUC off after on — counters frozen, pure full-block (zero behavior change).
# ----------
my $vfrozen = $cr->('cr_tuple_verdict_count');
is($run_update->(0, 't_upd', 1), '101',
	'L6 GUC off: full-block verdict (id=1 already updated once in L1, second update +100)');
is($cr->('cr_tuple_verdict_count'), $vfrozen, 'L6 GUC off: cr_tuple_verdict_count frozen (fast path inert)');

done_testing();
