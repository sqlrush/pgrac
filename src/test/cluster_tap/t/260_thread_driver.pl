#-------------------------------------------------------------------------
#
# 260_thread_driver.pl
#    spec-4.11 D1 increment 3b-1 — the online thread-recovery DATA DRIVER.
#
#    Increment 3a proved the source-agnostic RMW engine
#    (cluster_thread_recovery_replay_stream) is byte-for-byte correct over an
#    explicit window.  3b-1 adds the layer that turns a DEAD THREAD ID into a
#    driven replay: it builds a reader over that thread's per-thread WAL
#    (<cluster.wal_threads_dir>/thread_<tid>) and drives the engine under an
#    R13 error-demotion harness.  It publishes NO authority, does NO visibility
#    pass, and has NO live FSM caller (amend 2) -- a thread it "replays" stays
#    frozen, so a data-only DONE here is consumed by nobody (8.A).
#
#    Single-node stand-in (L239): node_id 0 routes its own WAL into thread_1, so
#    driving thread_1 exercises the foreign-thread reader against real WAL +
#    shared storage on one machine.  The genuine cross-node dead-peer reader is
#    forward.
#
#      L1 DATA DRIVE: drive thread_1 over a valid [lo, hi] window whose records
#         the shared page already reflects -> done, records read, every block
#         LSN-gated (the engine ran end-to-end through the foreign reader).
#
#      L2 R13 (amend 1): arm the cluster-thread-recovery-drive injection point
#         with a catchable ERROR -> the harness demotes it to BLOCKED (not a
#         propagated SQL error); the server stays healthy (worker survives).
#
#      FAIL-CLOSED (amend 4 -- a bad source/window is never a silent success):
#        L3 dead_tid out of range (0, and > slot count) -> BLOCKED.
#        L4 a thread whose per-thread WAL dir does not exist -> BLOCKED.
#        L5 invalid window (scan_lower > scan_upper) -> BLOCKED.
#        L6 scan_upper beyond the durable WAL boundary (incomplete) -> BLOCKED.
#
#    Determinism (L247): autovacuum off, full_page_writes on, large
#    wal_keep_size, explicit CHECKPOINTs to flush the shared page and leave a
#    straddle record past scan_upper.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/260_thread_driver.pl
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

my $node = PgracClusterNode->new('threaddriver');
$node->init(extra => [ '-X', "$wroot/thread_1" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.shared_storage_backend = local\n"
	  . "cluster.smgr_user_relations = on\n"
	  . "autovacuum = off\n"
	  . "full_page_writes = on\n"
	  . "wal_keep_size = 256MB\n");
$node->start;

sub wal_lsn { return $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()'); }

# Parse the driver SRF's ':'-delimited summary
# (<result>:<records>:<applied>:<gated>:<oos>:<recovered_through>).
sub parse_result
{
	my ($s) = @_;
	my @f = split /:/, $s;
	return {
		result  => $f[0],
		records => $f[1],
		applied => $f[2],
		gated   => $f[3],
		oos     => $f[4],
		through => $f[5],
	};
}

# Drive dead thread $tid over [$lo, $hi] with no fault armed.
sub drive
{
	my ($tid, $lo, $hi) = @_;
	return parse_result(
		$node->safe_psql('postgres', "SELECT cluster_thread_drive_test($tid, '$lo', '$hi')"));
}

# Drive with the driver injection point armed to raise a catchable ERROR.  The
# arm state is process-local (spec-0.27 §3.6), so arm + drive MUST share one
# session; psql prints each statement's output, so the drive result is the last
# non-empty line.
sub drive_with_injected_error
{
	my ($tid, $lo, $hi) = @_;
	my $s = $node->safe_psql('postgres', <<"SQL");
SELECT cluster_inject_fault('cluster-thread-recovery-drive', 'error', 0);
SELECT cluster_thread_drive_test($tid, '$lo', '$hi');
SQL
	my @lines = grep { length } split /\n/, $s;
	return parse_result($lines[-1]);
}

sub seed_table
{
	my ($rel) = @_;
	$node->safe_psql('postgres',
		"CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres',
		"INSERT INTO $rel SELECT g, 'seed' || g FROM generate_series(1, 8) g");
	$node->safe_psql('postgres', 'CHECKPOINT');
}

# ============================================================
# L1 DATA DRIVE: the dead-thread reader drives a window to done.
# ============================================================
my ($lo, $hi);
{
	seed_table('t260_drive');
	$lo = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t260_drive VALUES (100, 'op1')");
	$node->safe_psql('postgres', "INSERT INTO t260_drive VALUES (101, 'op2')");
	$hi = wal_lsn();
	$node->safe_psql('postgres', 'CHECKPOINT');    # flush block 0 + leave a straddle record

	my $r = drive(1, $lo, $hi);
	is($r->{result}, 'done', 'L1 foreign-thread reader drives the window to done');
	cmp_ok($r->{records}, '>=', 1, 'L1 driver read records from thread_1 per-thread WAL');
	is($r->{applied}, '0', 'L1 shared page already current -> every record LSN-gated');
}

# ============================================================
# L2 R13: injected catchable ERROR -> demoted to BLOCKED, server survives.
# ============================================================
{
	my $r = drive_with_injected_error(1, $lo, $hi);
	is($r->{result}, 'blocked',
		'L2 R13: injected ERROR demoted to result-returning BLOCKED (not propagated)');
	is($node->safe_psql('postgres', 'SELECT 1'), '1',
		'L2 R13: server healthy after the demoted error (worker survives)');
}

# ============================================================
# L3 FAIL-CLOSED: dead_tid out of range -> BLOCKED.
# ============================================================
{
	is(drive(0, $lo, $hi)->{result}, 'blocked',
		'L3 fail-closed: dead_tid 0 (out of range) -> BLOCKED');
	is(drive(200, $lo, $hi)->{result}, 'blocked',
		'L3 fail-closed: dead_tid beyond slot count (128) -> BLOCKED');
}

# ============================================================
# L4 FAIL-CLOSED: a thread with no per-thread WAL dir -> BLOCKED.
# ============================================================
{
	# thread_7 was never created under $wroot -> the source does not exist.
	is(drive(7, $lo, $hi)->{result}, 'blocked',
		'L4 fail-closed: missing per-thread WAL dir -> BLOCKED');
}

# ============================================================
# L5 FAIL-CLOSED: invalid window (scan_lower > scan_upper) -> BLOCKED.
# ============================================================
{
	is(drive(1, $hi, $lo)->{result}, 'blocked',
		'L5 fail-closed: invalid window (lower > upper) -> BLOCKED');
}

# ============================================================
# L6 FAIL-CLOSED: scan_upper beyond durable WAL (incomplete) -> BLOCKED.
# ============================================================
{
	my $cur = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t260_drive VALUES (300, 'w')");
	$node->safe_psql('postgres', 'CHECKPOINT');
	my $future =
	  $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 16777216)::pg_lsn');
	is(drive(1, $cur, $future)->{result}, 'blocked',
		'L6 fail-closed: scan_upper beyond WAL (incomplete window) -> BLOCKED (8.A)');
}

$node->stop;
done_testing();
