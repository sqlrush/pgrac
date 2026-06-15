# ============================================================
# 268_thread_recovery_observability.pl -- spec-4.11 D5 (observability) + D7
#    (capability gate) acceptance.
#
#    D5 surfaces:
#      - the ClusterThreadRecovery wait event is registered (recovery class).
#      - the recovery dump category exposes the four online thread-recovery
#        keys (state / threads_recovered / replay_failclosed /
#        recovered_through_lsn).
#      - a BLOCKED drive bumps thread_recovery_replay_failclosed (53RA4); a DONE
#        drive bumps thread_recovery_threads_recovered and advances
#        recovered_through_lsn.
#      - the 53RA4 panic leg: cluster.thread_recovery_on_unrecoverable=panic
#        turns a BLOCKED into a PANIC of the survivor.
#
#    D7 surfaces (capability gate, FEATURE_NOT_SUPPORTED = SQLSTATE 0A000):
#      - NO_SHARED_BACKEND / MULTI_SURVIVOR scopes raise FEATURE_NOT_SUPPORTED.
#      - APPLICABLE / DISABLED / SINGLE_NODE scopes are a no-op.
#      - the live-runtime scope on a single node is SINGLE_NODE (no error).
#
#    Single-node stand-in (L239, mirrors t/261): node_id 0 routes its own WAL
#    into thread_1, so a valid [lo, hi] window over thread_1 drives the
#    orchestrator to a real DONE (counters + authority) on one host.  The panic
#    leg crashes the survivor, so it runs LAST.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Portions Copyright (c) 2026, pgrac contributors
#    Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
# ============================================================
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my $wroot = PostgreSQL::Test::Utils::tempdir();

my $node = PgracClusterNode->new('threadrecobs');
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
	  . "wal_keep_size = 256MB\n"
	  # The L8 panic leg PANICs the survivor; keep it down deterministically
	  # (no restart -> no shmem re-allocation race) so teardown is reliable even
	  # under a shared test run's resource pressure.
	  . "restart_after_crash = off\n");
$node->start;

sub wal_lsn { return $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()'); }

# Recovery-category dump value for a key (the D5 observability surface).
sub rec_val
{
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='recovery' AND key='$key'");
}

# Drive the orchestrator core (replay_one_window) over an explicit window and
# return the result token (done / blocked / not_applicable).
sub replay_result
{
	my ($tid, $lo, $hi) = @_;
	my $s = $node->safe_psql('postgres',
		"SELECT cluster_thread_replay_one_test($tid, '$lo', '$hi')");
	my @f = split /:/, $s;
	return $f[0];
}

# ------------------------------------------------------------------
# Seed a recoverable window: a table + two pure-INSERT commits in [lo, hi].
# ------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE t268 (id int, v text) WITH (autovacuum_enabled = off)');
$node->safe_psql('postgres',
	"INSERT INTO t268 SELECT g, 'seed' || g FROM generate_series(1, 8) g");
$node->safe_psql('postgres', 'CHECKPOINT');

my $lo = wal_lsn();
$node->safe_psql('postgres', "INSERT INTO t268 VALUES (100, 'op1')");
$node->safe_psql('postgres', "INSERT INTO t268 VALUES (101, 'op2')");
my $hi = wal_lsn();
$node->safe_psql('postgres', 'CHECKPOINT');

# ============================================================
# L1 D5 wait event: ClusterThreadRecovery is registered in the recovery class.
# ============================================================
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events WHERE name = 'ClusterThreadRecovery'}),
	'1', 'L1 ClusterThreadRecovery wait event is registered');
is($node->safe_psql('postgres',
		q{SELECT type FROM pg_stat_cluster_wait_events WHERE name = 'ClusterThreadRecovery'}),
	'Cluster: Recovery', 'L1 ClusterThreadRecovery is in the Cluster: Recovery class');

# ============================================================
# L2 D7 capability gate: NO_SHARED_BACKEND (scope 3) -> FEATURE_NOT_SUPPORTED.
# ============================================================
{
	my ($ret, $out, $err) =
	  $node->psql('postgres', 'SELECT cluster_thread_capability_gate_test(3)');
	isnt($ret, 0, 'L2 capability gate (no shared backend) raises an error');
	like($err, qr/0A000|not supported|requires a shared data backend/,
		'L2 capability gate (no shared backend) -> FEATURE_NOT_SUPPORTED');
}

# ============================================================
# L3 D7 capability gate: MULTI_SURVIVOR (scope 4) -> FEATURE_NOT_SUPPORTED.
# ============================================================
{
	my ($ret, $out, $err) =
	  $node->psql('postgres', 'SELECT cluster_thread_capability_gate_test(4)');
	isnt($ret, 0, 'L3 capability gate (>2-node multi-survivor) raises an error');
	like($err, qr/0A000|not supported|more than one survivor/,
		'L3 capability gate (multi-survivor) -> FEATURE_NOT_SUPPORTED');
}

# ============================================================
# L4 D7 capability gate: APPLICABLE / DISABLED / SINGLE_NODE are a no-op; the
#     live-runtime scope on a single node is SINGLE_NODE (2).
# ============================================================
{
	is($node->safe_psql('postgres', 'SELECT cluster_thread_capability_gate_test(0)'),
		'ok:0', 'L4 capability gate APPLICABLE (0) is a no-op');
	is($node->safe_psql('postgres', 'SELECT cluster_thread_capability_gate_test(1)'),
		'ok:1', 'L4 capability gate DISABLED (1) is a no-op');
	is($node->safe_psql('postgres', 'SELECT cluster_thread_capability_gate_test(2)'),
		'ok:2', 'L4 capability gate SINGLE_NODE (2) is a no-op');
	is($node->safe_psql('postgres', 'SELECT cluster_thread_capability_gate_test(-1)'),
		'ok:2', 'L4 live-runtime scope on a single node is SINGLE_NODE (no error)');
}

# ============================================================
# L5 D5 dump keys: the four online thread-recovery keys exist; baseline state.
# ============================================================
{
	is($node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='recovery'
			   AND key LIKE 'thread_recovery_%'}),
		'4', 'L5 recovery dump exposes the 4 thread-recovery keys');
	# No slot is REPLAYING/BLOCKED/DONE before any drive -> idle aggregate.
	is(rec_val('thread_recovery_state'), 'idle', 'L5 baseline thread_recovery_state = idle');
	is(rec_val('thread_recovery_threads_recovered'), '0',
		'L5 baseline threads_recovered = 0');
	is(rec_val('thread_recovery_replay_failclosed'), '0',
		'L5 baseline replay_failclosed = 0');
}

# ============================================================
# L6 D5 counters: a BLOCKED drive (incomplete window) bumps replay_failclosed
#     (the 53RA4 fail-closed counter) and recovers no thread.
# ============================================================
{
	my $future =
	  $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 16777216)::pg_lsn');
	is(replay_result(1, $hi, $future), 'blocked', 'L6 incomplete window -> BLOCKED');
	cmp_ok(rec_val('thread_recovery_replay_failclosed'), '>=', 1,
		'L6 BLOCKED bumped thread_recovery_replay_failclosed (53RA4)');
	is(rec_val('thread_recovery_threads_recovered'), '0',
		'L6 a BLOCKED never counts a recovered thread (8.A)');
}

# ============================================================
# L7 D5 counters: a DONE drive (valid window) bumps threads_recovered and
#     advances recovered_through_lsn past zero.
# ============================================================
{
	is(replay_result(1, $lo, $hi), 'done', 'L7 valid window -> DONE');
	cmp_ok(rec_val('thread_recovery_threads_recovered'), '>=', 1,
		'L7 DONE bumped thread_recovery_threads_recovered');
	isnt(rec_val('thread_recovery_recovered_through_lsn'), '0x0000000000000000',
		'L7 DONE advanced recovered_through_lsn past zero');
}

# ============================================================
# L8 D5 53RA4 panic leg: cluster.thread_recovery_on_unrecoverable=panic turns a
#     BLOCKED into a PANIC of the survivor.  Runs LAST -- it crashes the node.
# ============================================================
{
	$node->safe_psql('postgres',
		'ALTER SYSTEM SET cluster.thread_recovery_on_unrecoverable = panic');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');

	my $future =
	  $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 16777216)::pg_lsn');
	# A fresh connection picks up the reloaded panic policy; the BLOCKED window
	# then PANICs the survivor, so the SRF call fails (connection lost).
	my ($ret, $out, $err) =
	  $node->psql('postgres', "SELECT cluster_thread_replay_one_test(1, '$hi', '$future')");
	isnt($ret, 0, 'L8 panic policy: a BLOCKED drive crashes the SRF call');

	# The server logs a PANIC carrying the 53RA4 errcode / message before crashing.
	$node->wait_for_log(
		qr/PANIC.*online thread recovery for dead thread 1 could not complete/);
	pass('L8 panic policy: survivor PANIC logged with the 53RA4 message');

	# With restart_after_crash=off the PANIC leaves the instance down; the node is
	# now stopped.  A fail-tolerant immediate stop reconciles the framework state.
	$node->stop('immediate', fail_ok => 1);
}

done_testing();
