#-------------------------------------------------------------------------
#
# 261_thread_orchestrator.pl
#    spec-4.11 D1 increment 3b-2 — the online thread-recovery ORCHESTRATOR.
#
#    Increment 3b-1 drove a DATA-ONLY replay (no visibility pass, no authority,
#    no durability barrier).  3b-2 assembles the COMPLETE online recovery of one
#    dead thread and is the corruption-critical core:
#      * a combined data + VISIBILITY pass (foreign XACT/CLOG outcomes diverted
#        to the per-origin store -- without it a recovered page would be served
#        with an unknown commit state, false-visible, 8.A);
#      * a durability barrier (smgrimmedsync touched rels + flush the outcome
#        store) BEFORE any authority is published;
#      * a 3-way authority publish (registry skip-bound + the node-local reader
#        authority, written LAST) on DONE only;
#      * partial-apply (v0.3 P2): any BLOCKED publishes NO authority, so the
#        thread stays frozen and never serves a stale page (8.A);
#      * R13: a catchable ERROR (injected, or an online unmaterializable record)
#        demotes to a result-returning BLOCKED and the survivor keeps running.
#
#    Single-node stand-in (L239): node_id 0 routes its own WAL into thread_1, so
#    driving thread_1 (origin 0) exercises the orchestrator against real WAL +
#    shared storage on one machine.  The genuine cross-node dead-peer path is
#    forward.
#
#      L1 R13: injected catchable ERROR -> BLOCKED + server survives + NO
#         authority published (origin 0 not materialized).
#      L2 partial-apply: an incomplete window -> BLOCKED + NO authority.
#      L3 fail-closed: bad dead_tid / missing dir / inverted window -> BLOCKED.
#      L4 SUCCESS: a valid window -> done; the visibility pass materialized the
#         dead thread's commits (remote_outcome_committed rose); the node-local
#         reader authority is published (origin 0 materialized) AND durable (the
#         merged.authority marker exists on disk).
#      L5 scope gate (replay_one auto form): single node / no shared backend ->
#         not_applicable.
#
#    The keep_frozen-vs-panic ESCALATION decision is unit-pinned
#    (test_cluster_thread_orchestrator); the e2e panic trigger + 53RA4 + dump
#    keys land with D5 (3b-4).
#
#    Determinism (L247): autovacuum off, full_page_writes on, large
#    wal_keep_size, explicit CHECKPOINTs.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/261_thread_orchestrator.pl
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

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

# Per-thread WAL root; node_id 0 -> thread_1, whose dir is pg_wal (via -X).
my $wroot = PostgreSQL::Test::Utils::tempdir();

my $node = PgracClusterNode->new('threadorch');
$node->init(extra => [ '-X', "$wroot/thread_1" ]);
$node->append_conf('postgresql.conf',
		"cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.online_thread_recovery = on\n"
	  . "cluster.thread_recovery_on_unrecoverable = keep_frozen\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.shared_storage_backend = local\n"
	  . "cluster.smgr_user_relations = on\n"
	  . "autovacuum = off\n"
	  . "full_page_writes = on\n"
	  . "wal_keep_size = 256MB\n");
$node->start;

sub wal_lsn { return $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()'); }

# Parse the orchestrator SRF's ':'-delimited summary
# (<result>:<records>:<applied>:<gated>:<oos>:<recovered_through>).
sub parse_result
{
	my ($s) = @_;
	my @f = split /:/, $s;
	return { result => $f[0], records => $f[1], applied => $f[2] };
}

# Drive the orchestrator (replay_one_window) over an explicit window.
sub replay_one
{
	my ($tid, $lo, $hi) = @_;
	return parse_result($node->safe_psql('postgres',
		"SELECT cluster_thread_replay_one_test($tid, '$lo', '$hi')"));
}

# Drive with the driver injection point armed to raise a catchable ERROR.  The
# arm state is process-local, so arm + drive MUST share one session.
sub replay_one_with_injected_error
{
	my ($tid, $lo, $hi) = @_;
	my $s = $node->safe_psql('postgres', <<"SQL");
SELECT cluster_inject_fault('cluster-thread-recovery-drive', 'error', 0);
SELECT cluster_thread_replay_one_test($tid, '$lo', '$hi');
SQL
	my @lines = grep { length } split /\n/, $s;
	return parse_result($lines[-1]);
}

# Drive the FSM-facing replay_one (scope gate + basic window).
sub replay_one_auto
{
	my ($tid) = @_;
	return parse_result(
		$node->safe_psql('postgres', "SELECT cluster_thread_replay_one_auto_test($tid)"));
}

# Observability: pg_cluster_state recovery rows.
sub recovery_val
{
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='recovery' AND key='$key'");
}
sub materialized { return recovery_val('materialized_remote_instances'); }
sub committed    { return recovery_val('remote_outcome_committed'); }

# The node-local reader authority marker for origin 0 (dead_tid 1 -> origin 0).
my $marker = $node->data_dir . '/pg_undo/instance_0/merged.authority';

# ------------------------------------------------------------------
# Seed: a table, then two PURE INSERT commits inside [lo, hi].
# ------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE t261 (id int, v text) WITH (autovacuum_enabled = off)');
$node->safe_psql('postgres',
	"INSERT INTO t261 SELECT g, 'seed' || g FROM generate_series(1, 8) g");
$node->safe_psql('postgres', 'CHECKPOINT');

my $lo = wal_lsn();
$node->safe_psql('postgres', "INSERT INTO t261 VALUES (100, 'op1')");
$node->safe_psql('postgres', "INSERT INTO t261 VALUES (101, 'op2')");
my $hi = wal_lsn();
$node->safe_psql('postgres', 'CHECKPOINT');    # flush block 0 + leave a straddle record

# Baseline: nothing materialized yet, no marker on disk.
is(materialized(), '-', 'baseline: no origin materialized before any DONE');
ok(!-e $marker, 'baseline: no node-local authority marker on disk');
my $committed0 = committed();

# ============================================================
# L1 R13: injected ERROR -> BLOCKED, server survives, NO authority published.
# ============================================================
{
	my $r = replay_one_with_injected_error(1, $lo, $hi);
	is($r->{result}, 'blocked',
		'L1 R13: injected ERROR demoted to result-returning BLOCKED');
	is($node->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 R13: server healthy after the demoted error (worker survives)');
	is(materialized(), '-', 'L1 partial-apply: a BLOCKED publishes NO authority (8.A)');
	ok(!-e $marker, 'L1 partial-apply: no authority marker written on BLOCKED');
}

# ============================================================
# L2 partial-apply: an incomplete window -> BLOCKED + NO authority.
# ============================================================
{
	my $future =
	  $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 16777216)::pg_lsn');
	my $r = replay_one(1, $hi, $future);
	is($r->{result}, 'blocked',
		'L2 incomplete window (scan_upper beyond WAL) -> BLOCKED (8.A)');
	is(materialized(), '-', 'L2 partial-apply: still no authority published');
}

# ============================================================
# L3 fail-closed: bad dead_tid / missing dir / inverted window -> BLOCKED.
# ============================================================
{
	is(replay_one(0, $lo, $hi)->{result},   'blocked', 'L3 fail-closed: dead_tid 0 -> BLOCKED');
	is(replay_one(200, $lo, $hi)->{result}, 'blocked', 'L3 fail-closed: dead_tid > slots -> BLOCKED');
	is(replay_one(7, $lo, $hi)->{result},   'blocked', 'L3 fail-closed: missing thread dir -> BLOCKED');
	is(replay_one(1, $hi, $lo)->{result},   'blocked', 'L3 fail-closed: inverted window -> BLOCKED');
	is(materialized(), '-', 'L3 fail-closed: no authority published by any failed drive');
}

# ============================================================
# L4 SUCCESS: valid window -> done + visibility materialized + authority durable.
# ============================================================
{
	my $r = replay_one(1, $lo, $hi);
	is($r->{result}, 'done', 'L4 valid window -> orchestrator DONE');
	cmp_ok($r->{records}, '>=', 1, 'L4 the combined engine read records from thread_1 WAL');

	cmp_ok(committed(), '>', $committed0,
		'L4 visibility pass: the dead thread\'s commits were materialized (8.A)');

	like(materialized(), qr/(^|,)0(,|$)/,
		'L4 authority: origin 0 is now materialized (node-local reader authority published)');
	ok(-e $marker, 'L4 durability: the merged.authority marker is on disk');
}

# ============================================================
# L5 scope gate: the FSM-facing replay_one refuses an unsupported config.
# ============================================================
{
	# Single node + shared_storage_backend=local: the scope gate refuses online
	# thread recovery (not_applicable), never runs the mechanism.
	is(replay_one_auto(1)->{result}, 'not_applicable',
		'L5 scope gate: single node / no shared backend -> not_applicable');
}

$node->stop;
done_testing();
