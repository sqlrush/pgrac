#-------------------------------------------------------------------------
#
# 262_thread_unfreeze_gate.pl
#    spec-4.11 D1 increment 3b-3 — the D3 unfreeze gate's authority read.
#
#    3b-2 published the node-local merged materialization authority on a DONE
#    online recovery.  3b-3 adds the unfreeze precondition the reconfig FSM and
#    the per-block serve path consult before letting a survivor serve a dead
#    origin's resources:
#      * cluster_thread_recovery_local_complete(dead_tid, required_lsn) reads the
#        NODE-LOCAL merged authority (Q4 3-way authority, R11) -- materialized at
#        all (required_lsn = 0/0) and, for a real required_lsn, recovered_through
#        >= it.  fail-closed: a bad thread id or any unmet condition -> false
#        (keep frozen), never served as current (8.A).
#      * cluster_thread_recovery_gate_unfreeze(dead_bitmap) is the FSM predicate;
#        OUT of scope (online_thread_recovery off by default / no shared backend /
#        >2-node) it is a NO-OP (false) so the existing spec-4.6/4.7 unfreeze path
#        is unchanged (no regression).  This test pins that no-op property.
#
#    Single-node stand-in (L239, mirrors t/261): node_id 0 routes its own WAL into
#    thread_1, so driving thread_1 (origin 0) publishes real authority on one
#    machine; local_complete then reads it back.  The gate's IN-SCOPE engaged
#    decision (stay frozen until a dead origin is materialized) is unit-pinned
#    purely (cluster_thread_recovery_gate_decide, test_cluster_thread_orchestrator)
#    and its full FSM-driven freeze-until-materialized e2e lands with the 2-node
#    TAP (3b-4): a 2-declared-node cluster_fs scope activates the pre-existing
#    immature 2-node live path, so it is NOT reachable here without flakiness.
#
#      L1 baseline: before any DONE, origin 0 is NOT complete (any required_lsn).
#      L2 fail-closed: a bad dead_tid maps to no origin -> not complete.
#      L3 no regression: gate_unfreeze is a no-op (false) out of scope.
#      L4 SUCCESS: a valid window publishes authority; local_complete now reports
#         complete at the node level and up to (but not beyond) recovered_through.
#      L5 scope dominates: gate_unfreeze stays a no-op out of scope even after the
#         authority is published.
#
#    Determinism (L247): autovacuum off, full_page_writes on, large wal_keep_size,
#    explicit CHECKPOINTs.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/262_thread_unfreeze_gate.pl
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

my $node = PgracClusterNode->new('threadgate');
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

# D3 gate SRFs return bool -> psql prints 't' / 'f'.
sub local_complete
{
	my ($tid, $lsn) = @_;
	return $node->safe_psql('postgres',
		"SELECT cluster_thread_local_complete_test($tid, '$lsn')");
}
sub gate_unfreeze
{
	my ($tid) = @_;
	return $node->safe_psql('postgres', "SELECT cluster_thread_gate_unfreeze_test($tid)");
}

# Publish authority for origin 0 by driving the orchestrator over an explicit
# window (the explicit form bypasses the scope gate, like t/261 L4).
sub publish_origin0
{
	my ($lo, $hi) = @_;
	my $s = $node->safe_psql('postgres',
		"SELECT cluster_thread_replay_one_test(1, '$lo', '$hi')");
	return (split /:/, $s)[0];    # <result>
}

sub materialized
{
	return $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='recovery' "
	  . "AND key='materialized_remote_instances'");
}

# The node-local reader authority marker for origin 0 (dead_tid 1 -> origin 0).
my $marker = $node->data_dir . '/pg_undo/instance_0/merged.authority';

# ------------------------------------------------------------------
# Seed: a table, then two PURE INSERT commits inside [lo, hi].
# ------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE t262 (id int, v text) WITH (autovacuum_enabled = off)');
$node->safe_psql('postgres',
	"INSERT INTO t262 SELECT g, 'seed' || g FROM generate_series(1, 8) g");
$node->safe_psql('postgres', 'CHECKPOINT');

my $lo = wal_lsn();
$node->safe_psql('postgres', "INSERT INTO t262 VALUES (100, 'op1')");
$node->safe_psql('postgres', "INSERT INTO t262 VALUES (101, 'op2')");
my $hi = wal_lsn();
$node->safe_psql('postgres', 'CHECKPOINT');

my $future =
  $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 16777216)::pg_lsn');

# ============================================================
# L1 baseline: origin 0 not yet recovered -> local_complete false everywhere.
# ============================================================
is(materialized(), '-', 'baseline: no origin materialized before any DONE');
ok(!-e $marker, 'baseline: no node-local authority marker on disk');
is(local_complete(1, '0/0'), 'f',
	'L1 baseline: origin 0 not materialized -> local_complete(node-level) false');
is(local_complete(1, $hi), 'f',
	'L1 baseline: origin 0 not materialized -> local_complete(required_lsn) false');

# ============================================================
# L2 fail-closed: a bad dead_tid names no origin -> never complete.
# ============================================================
is(local_complete(0, '0/0'), 'f',
	'L2 fail-closed: dead_tid 0 (legacy) -> no origin -> false');
is(local_complete(200, '0/0'), 'f',
	'L2 fail-closed: dead_tid > max thread -> no origin -> false');

# ============================================================
# L3 no regression: gate_unfreeze is a no-op (false) out of scope.
# Single node + shared_storage_backend=local => decide_scope NOT applicable, so
# the FSM gate never engages -> existing spec-4.6/4.7 unfreeze path unchanged.
# ============================================================
is(gate_unfreeze(1), 'f',
	'L3 no regression: gate_unfreeze(out of scope) is a no-op for a dead origin');
is(gate_unfreeze(2), 'f',
	'L3 no regression: gate_unfreeze(out of scope) is a no-op for another origin');
is(gate_unfreeze(0), 'f',
	'L3 fail-closed: gate_unfreeze(bad dead_tid, out of scope) is a no-op');

# ============================================================
# L4 SUCCESS: publish authority for origin 0, then local_complete reflects it.
# ============================================================
is(publish_origin0($lo, $hi), 'done', 'L4 valid window -> orchestrator DONE (authority published)');
ok(-e $marker, 'L4 durability: the merged.authority marker is on disk');
like(materialized(), qr/(^|,)0(,|$)/, 'L4 authority: origin 0 is now materialized');

is(local_complete(1, '0/0'), 't',
	'L4 node-level: origin 0 materialized -> local_complete true');
is(local_complete(1, $lo), 't',
	'L4 covered: recovered_through >= lo -> local_complete true');
is(local_complete(1, $future), 'f',
	'L4 lost-write guard: recovered_through < a future lsn -> local_complete false (8.A)');

# ============================================================
# L5 scope dominates: gate_unfreeze stays a no-op out of scope even now that the
# authority is published (single-node config is never in scope).
# ============================================================
is(gate_unfreeze(1), 'f',
	'L5 scope dominates: gate_unfreeze stays a no-op out of scope regardless of authority');

$node->stop;
done_testing();
