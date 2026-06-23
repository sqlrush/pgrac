#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 300_cluster_5_50_cr_profile.pl
#	  spec-5.50 D2 — SELECT-heavy / long-snapshot / parallel-scan CR
#	  read-path profile: REAL own-instance CR path (not by-construction,
#	  L341), producing the value-gate + soundness-gate evidence for the
#	  Stage 5.5 CR Read-Path Performance band.  measure-only: this test reads
#	  the 17 shipped cr counters (pg_cluster_state category='cr') and asserts
#	  real CR work; it changes NO product code, NO catalog (catversion frozen).
#
#	  Own-instance only: a single cluster-enabled node (allow_single_node) with
#	  cr_gate_no_peer_fastpath=off so the gate really constructs.  Writer and
#	  readers are same-node sessions (mirror t/216/t/239); cross-instance CR
#	  fail-closes (ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED) = spec-5.57.
#
#	  Same-read_scn mechanism (spec-5.50 §2.3 / r3 P1):  pg_export_snapshot +
#	  SET TRANSACTION SNAPSHOT FAIL-CLOSES in cluster mode by design (spec-3.3
#	  R5: "invalid cluster snapshot fields"; see t/239 scenario D).  So the
#	  harness does NOT force a snapshot.  Instead it opens N independent
#	  repeatable-read readers in a quiescent window: read_scn == cluster_scn_
#	  current() at snapshot time (procarray.c:2152), so with no commit between
#	  the readers they share the SAME read_scn — and the test PROVES it by
#	  reading each reader's cluster_scn_current() and comparing.  If they ever
#	  differ, distinct_cr_keys==K is unproven and the row is INCONCLUSIVE, never
#	  fed to the value gate.  This is the supported, faithful SELECT-heavy case.
#
#	  L1   fixture up; real own-instance CR; cr_construct_count AND
#	       cr_chain_walk_steps_sum both rise (REAL undo chain, not empty-chain
#	       page copy — resolves F0-11); 17 cr rows.
#	  L2   axis A SELECT-heavy: N independent readers share read_scn (proven),
#	       writer modifies every block; distinct_cr_keys D measured + stability
#	       confirmed; cross-backend redundancy = total_construct/D ~= N.
#	  L3   axis B long-snapshot: deeper undo chain costs more chain-walk steps;
#	       a working set > cache capacity makes cr_cache_evict_count take off.
#	  L4   axis C parallel-scan: one leader RR snapshot, workers inherit its
#	       read_scn (RestoreSnapshot), EXPLAIN ANALYZE asserts launched workers,
#	       redundancy = par_construct/D ~= 1 (seqscan mutual-exclusion: each
#	       block to one worker, no same-key cross-worker duplication).
#	  L5   key over-miss (spec-5.53 input): base_page_lsn churn forces misses.
#	  L6   resolver duplication (spec-5.55 input): N backends resolve the same
#	       deleted-row xmax under their shared read_scn.
#	  L7   FX2: a single node already builds real chains — the heavier 2-node
#	       ClusterPair is not required for the profile.
#	  L8   measure-only guard: no corruption, no cross-instance leak, non-CR
#	       reads byte-faithful (no product change).
#
#	  Perf tier (spec-5.50 Q9 / L91): measurement/characterisation TAP, not a
#	  fast-gate.  Runs in `make check` locally and is wired to the perf workflow;
#	  intentionally NOT in the nightly correctness shards.  Absolute timings are
#	  NOT asserted here (clean Linux release numbers come from the perf workflow,
#	  spec-5.50 M1/M4); this test asserts COUNTER behaviour, which is
#	  release/debug-invariant.
#
#	  Spec: spec-5.50-cr-read-path-profile.md (FROZEN v1.0 + errata 1)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------
# Single cluster-enabled node.  cr_gate_no_peer_fastpath=off forces the gate to
# really construct on a single node (the 3.24 fast path would otherwise short
# the no-peer case).  Parallel knobs (postmaster-level) force real parallel
# plans for axis C.  autovacuum off keeps the instance-wide counters quiescent.
# ----------
my $node = PostgreSQL::Test::Cluster->new('spec_5_50_cr_profile');
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
		'max_worker_processes = 16',
		'max_parallel_workers = 16',
		'min_parallel_table_scan_size = 0',
		'parallel_setup_cost = 0',
		'parallel_tuple_cost = 0',
		'jit = off',
		'synchronize_seqscans = off',
		''));
$node->start;

# Instance-wide cr counter (workers/backends bump the same shmem).
my $cr = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};

# Open an independent repeatable-read reader; return [session, read_scn].
# read_scn == cluster_scn_current() at snapshot time (procarray.c:2152), so the
# value returned by the first in-txn query IS this backend's read_scn.
my $open_rr_reader = sub {
	my (%opt) = @_;
	my $cache = defined $opt{cache} ? $opt{cache} : 4096;
	my $b = $node->background_psql('postgres', on_error_die => 1);
	$b->query_safe("SET cluster.cr_cache_max_blocks = $cache");
	$b->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	my $rscn = $b->query_safe('SELECT cluster_scn_current()');
	chomp $rscn;
	return [ $b, $rscn ];
};


# ----------
# L1: fixture up + REAL own-instance CR + chain-walk proof.
# ----------
{
	is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node alive');

	is( $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'}),
		'17', 'L1b cr category has 17 counters');

	$node->safe_psql('postgres',
		'CREATE TABLE t_l1 (id int, v int); INSERT INTO t_l1 VALUES (1, 100);');

	my $c0 = $cr->('cr_construct_count');
	my $w0 = $cr->('cr_chain_walk_steps_sum');

	my $s = $node->background_psql('postgres', on_error_die => 1);
	$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe('SELECT v FROM t_l1 WHERE id = 1');	# take snapshot
	$node->safe_psql('postgres', 'UPDATE t_l1 SET v = 200 WHERE id = 1');
	my $seen = $s->query_safe('SELECT v FROM t_l1 WHERE id = 1');
	$s->query_safe('COMMIT');
	$s->quit;
	chomp $seen;

	is($seen, '100', 'L1c snapshot sees pre-update value via CR');
	ok($cr->('cr_construct_count') > $c0, 'L1d cr_construct_count rose (real CR ran)');
	ok($cr->('cr_chain_walk_steps_sum') > $w0,
		'L1e cr_chain_walk_steps_sum rose (REAL undo chain, not empty-chain page copy)');
}


# ----------
# L2: axis A SELECT-heavy — cross-backend construction redundancy.
# ----------
{
	$node->safe_psql('postgres',
		"CREATE TABLE t_axa (id int, pad text);
		 INSERT INTO t_axa SELECT g, repeat('x', 120) FROM generate_series(1, 3000) g;");

	# N independent readers snapshot in a quiescent window -> shared read_scn.
	my $N = 4;
	my @rd = map { $open_rr_reader->(cache => 4096) } 1 .. $N;
	my %scns = map { ($_->[1] => 1) } @rd;
	my $shared = (keys %scns) == 1 ? 1 : 0;
	my $rscn = $rd[0][1];

	# Post-snapshot writer touches every block; quiescent afterwards so the live
	# base_page_lsn is stable for all readers.
	$node->safe_psql('postgres', 'UPDATE t_axa SET pad = pad');

	# Settle hint bits with post-write autocommit scans so every reader sees the
	# SAME stable base_page_lsn (same CR key) during measurement; otherwise the
	# first reader to touch a page mutates it and the keys diverge per backend.
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_axa') for 1 .. 2;

	# D = distinct hot blocks via reader[0]'s first-scan construct delta.
	my $pre0 = $cr->('cr_construct_count');
	$rd[0][0]->query_safe('SELECT count(*) FROM t_axa');	# first scan constructs D
	my $D = $cr->('cr_construct_count') - $pre0;
	ok($D >= 5, "L2a distinct hot blocks D = $D (>= 5)");
	is($shared, 1, "L2b all $N readers share read_scn=$rscn (quiescent window)");

	# Construct redundancy: readers[1..N-1] each rebuild all D blocks (backend-
	# local cache is not shared) -> total redundant constructs ~= N*D.
	my $pre = $cr->('cr_construct_count');
	for my $i (1 .. $N - 1) { $rd[$i][0]->query_safe('SELECT count(*) FROM t_axa'); }
	my $total     = $D + ($cr->('cr_construct_count') - $pre);
	my $redundancy = $D > 0 ? $total / $D : 0;
	ok( $redundancy >= $N - 0.75,
		sprintf("L2c construct redundancy ~= N (%.2f vs %d): %d backends each rebuild the same D blocks",
			$redundancy, $N, $N));

	# Steady-state dedup-ability: a shared pool keyed on base_page_lsn can only
	# fold these N*D constructs to D if base_page_lsn is STABLE across backends.
	# wal_log_hints bumps pd_lsn on first-touch, so warm reader[0] until a scan
	# adds zero new misses (keys settled).  Convergence => steady-state dedup-
	# able (key_stable=1); non-convergence is itself a soundness finding fed to
	# spec-5.51/5.53 (base_page_lsn churn defeats a base_page_lsn-keyed pool).
	my @trace;
	my $settled = 0;
	for my $p (1 .. 8) {
		my $mb = $cr->('cr_cache_miss_count');
		$rd[0][0]->query_safe('SELECT count(*) FROM t_axa');
		my $miss = $cr->('cr_cache_miss_count') - $mb;
		push @trace, $miss;
		if ($miss == 0) { $settled = $p; last; }
	}
	for my $r (@rd) { $r->[0]->query_safe('COMMIT'); $r->[0]->quit; }

	my $key_stable = ($shared && $settled) ? 1 : 0;
	note(sprintf("L2 axis A: N=%d D=%d total_construct=%d redundancy=%.2f shared=%d read_scn=%s "
			. "settled_pass=%d miss_trace=[%s] key_stable=%d",
		$N, $D, $total, $redundancy, $shared, $rscn, $settled, join(',', @trace), $key_stable));
	ok($settled > 0,
		"L2d base_page_lsn settles after warm-up (pass $settled, trace [@{[join ',', @trace]}]) "
		. "-> steady-state cross-backend dedup-able");
}


# ----------
# L3: axis B long-snapshot — deep undo chain cost + cache eviction thrash.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_deep (id int, v int); INSERT INTO t_deep VALUES (1, 0);');
	my $s = $node->background_psql('postgres', on_error_die => 1);
	$s->query_safe('SET cluster.cr_cache_max_blocks = 0');	# no cache: measure raw walk
	$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe('SELECT v FROM t_deep WHERE id = 1');	# snapshot

	my $w0 = $cr->('cr_chain_walk_steps_sum');
	$node->safe_psql('postgres', 'UPDATE t_deep SET v = v + 1 WHERE id = 1');	# depth 1
	$s->query_safe('SELECT v FROM t_deep WHERE id = 1');
	my $walk_shallow = $cr->('cr_chain_walk_steps_sum') - $w0;

	$node->safe_psql('postgres', 'UPDATE t_deep SET v = v + 1 WHERE id = 1') for 1 .. 7;	# depth -> 8
	my $w1 = $cr->('cr_chain_walk_steps_sum');
	$s->query_safe('SELECT v FROM t_deep WHERE id = 1');
	my $walk_deep = $cr->('cr_chain_walk_steps_sum') - $w1;
	$s->query_safe('COMMIT');
	$s->quit;

	note("L3 axis B: walk_shallow(d=1)=$walk_shallow walk_deep(d=8)=$walk_deep");
	ok($walk_deep > $walk_shallow,
		"L3a deeper undo chain costs more chain-walk steps ($walk_deep > $walk_shallow)");

	# Eviction thrash: a working set larger than the cache forces evictions.
	for my $t (1 .. 4) {
		$node->safe_psql('postgres',
			"CREATE TABLE t_ws$t (id int, v int); INSERT INTO t_ws$t VALUES ($t, $t);");
	}
	my $ev0 = $cr->('cr_cache_evict_count');
	my $se = $node->background_psql('postgres', on_error_die => 1);
	$se->query_safe('SET cluster.cr_cache_max_blocks = 2');	# working set 4 > cap 2
	$se->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$se->query_safe('SELECT 1');
	$node->safe_psql('postgres', "UPDATE t_ws$_ SET v = v + 100 WHERE id = $_") for 1 .. 4;
	$se->query_safe("SELECT v FROM t_ws$_ WHERE id = $_") for 1 .. 4;
	my $evict_delta = $cr->('cr_cache_evict_count') - $ev0;
	$se->query_safe('COMMIT');
	$se->quit;

	ok($evict_delta > 0,
		"L3b working set > cache cap thrashes (cr_cache_evict_count delta=$evict_delta)");
}


# ----------
# L4: axis C parallel-scan — worker count + same-key cross-worker duplication.
#   Parallel workers inherit the leader's read_scn via RestoreSnapshot (the only
#   same-read_scn multi-executor path in cluster mode; SQL snapshot import fail-
#   closes).  seqscan hands each block to exactly one worker -> redundancy ~= 1.
# ----------
{
	# 3000 rows: enough blocks for forced parallelism, but the single post-
	# snapshot writer transaction stays under cluster.cr_chain_walk_max_steps
	# (4096) — a one-shot UPDATE of 20000 rows builds a 20000-record undo chain
	# that the full-block CR construct cannot walk (fail-closed cost cliff; this
	# is itself a value/soundness finding recorded in perf-baseline §17).
	$node->safe_psql('postgres',
		"CREATE TABLE t_par (id int, pad text);
		 INSERT INTO t_par SELECT g, repeat('y', 120) FROM generate_series(1, 3000) g;
		 ALTER TABLE t_par SET (parallel_workers = 4);");

	my $L = $node->background_psql('postgres', on_error_die => 1);
	$L->query_safe('SET cluster.cr_cache_max_blocks = 4096');
	$L->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$L->query_safe('SELECT count(*) FROM t_par');	# leader snapshot (read_scn)
	$node->safe_psql('postgres', 'UPDATE t_par SET pad = pad');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_par') for 1 .. 2;	# settle pages

	# Serial baseline (leader): D distinct CR blocks.
	$L->query_safe('SET max_parallel_workers_per_gather = 0');
	my $pre_d = $cr->('cr_construct_count');
	$L->query_safe('SELECT count(*) FROM t_par');
	my $D = $cr->('cr_construct_count') - $pre_d;

	# Parallel run: workers have their own empty caches and the leader's read_scn.
	$L->query_safe('SET max_parallel_workers_per_gather = 4');
	$L->query_safe('SET parallel_leader_participation = off');
	my $pre_c = $cr->('cr_construct_count');
	my $plan  = $L->query_safe(
		'EXPLAIN (ANALYZE, FORMAT TEXT, TIMING off, SUMMARY off, COSTS off) SELECT count(*) FROM t_par');
	my $par_construct = $cr->('cr_construct_count') - $pre_c;
	$L->query_safe('COMMIT');
	$L->quit;

	my ($launched) = ($plan =~ /Workers Launched:\s*(\d+)/);
	$launched //= 0;
	my $par_red = $D > 0 ? $par_construct / $D : 0;
	note(sprintf("L4 axis C: D=%d par_construct=%d launched=%d redundancy=%.2f",
		$D, $par_construct, $launched, $par_red));
	ok($launched >= 2, "L4a parallel scan launched workers = $launched (>= 2 real parallel)");
	ok( $par_red <= 1.5,
		sprintf("L4b parallel redundancy ~= 1 (%.2f): seqscan mutual-exclusion, no M-worker dup",
			$par_red));
}


# ----------
# L5: key over-miss (spec-5.53 input) — base_page_lsn churn forces re-misses.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_churn (id int, v int); INSERT INTO t_churn VALUES (1, 1), (2, 2);');
	my $s = $node->background_psql('postgres', on_error_die => 1);
	$s->query_safe('SET cluster.cr_cache_max_blocks = 64');
	$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe('SELECT count(*) FROM t_churn');	# snapshot
	$node->safe_psql('postgres', 'UPDATE t_churn SET v = v + 10 WHERE id = 1');

	my $m0 = $cr->('cr_cache_miss_count');
	$s->query_safe('SELECT v FROM t_churn WHERE id = 1');	# miss #1 (build + cache)
	$node->safe_psql('postgres', 'UPDATE t_churn SET v = v + 100 WHERE id = 2');	# bump page lsn
	$s->query_safe('SELECT v FROM t_churn WHERE id = 1');	# miss #2 (base_page_lsn changed)
	my $miss_delta = $cr->('cr_cache_miss_count') - $m0;
	$s->query_safe('COMMIT');
	$s->quit;

	ok($miss_delta >= 2,
		"L5 base_page_lsn churn forces a re-miss (miss_delta=$miss_delta) -> 5.53 over-miss input");
}


# ----------
# L6: resolver path characterisation (spec-5.55 input).
#   The SCN xmax resolver (cr_xmax_resolved_count) bumps ONLY when the live ITL
#   slot was recycled to a newer writer (slot->xid != cr_xmax, cluster_cr.c:1600);
#   the common RR + concurrent-delete case takes the live-slot shortcut
#   (slot->xid == cr_xmax -> VISIBLE, no resolution, cluster_cr.c:1585).  So the
#   honest intra-instance finding for 5.55: the common path does NOT exercise the
#   resolver, hence a shared resolver cache yields no intra-instance common-case
#   benefit; the resolver (and its duplication) is a slot-recycle / cross-instance
#   concern.  This test PROVES the shortcut deterministically.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_res (id int, v int); INSERT INTO t_res SELECT g, g FROM generate_series(1, 50) g;');

	my $N  = 4;
	my @rd = map { $open_rr_reader->(cache => 0) } 1 .. $N;
	my %scns = map { ($_->[1] => 1) } @rd;
	my $shared = (keys %scns) == 1 ? 1 : 0;

	# Committed delete after the readers' snapshot; the deleter slot is NOT
	# recycled, so reads take the live-slot shortcut (no SCN resolution).
	$node->safe_psql('postgres', 'DELETE FROM t_res WHERE id <= 25');

	my $pre = $cr->('cr_xmax_resolved_count');
	for my $r (@rd) { $r->[0]->query_safe('SELECT count(*) FROM t_res'); }
	my $delta = $cr->('cr_xmax_resolved_count') - $pre;
	for my $r (@rd) { $r->[0]->query_safe('COMMIT'); $r->[0]->quit; }

	note("L6 resolver: N=$N shared_read_scn=$shared cr_xmax_resolved delta=$delta "
		. "(common deleter-not-recycled case)");
	is($delta, 0,
		"L6 common-case deleted-row reads take the live-slot shortcut (no SCN resolve) "
		. "-> shared resolver cache (5.55) is a recycle/cross-instance concern, not intra-instance common-path");
}


# ----------
# L7: FX2 — a single cluster-enabled node already builds real chains.
# ----------
ok($cr->('cr_chain_walk_steps_sum') > 0,
	'L7 FX2: single node builds REAL undo chains (2-node ClusterPair not required for the profile)');


# ----------
# L8: measure-only guard — no product change, no cross-instance leak.
# ----------
{
	is($cr->('cr_corruption_count'), '0', 'L8a no CR corruption across the profile');
	is($cr->('cr_cross_instance_unsupported_count'), '0',
		'L8b no cross-instance fail-closed (own-instance CR only, same-node sessions)');
	$node->safe_psql('postgres',
		'CREATE TABLE t_g (id int, v int); INSERT INTO t_g VALUES (1, 7);');
	is($node->safe_psql('postgres', 'SELECT v FROM t_g WHERE id = 1'), '7',
		'L8c non-CR read is correct (CR path byte-faithful, no product change)');
}


$node->stop;
done_testing();
