#-------------------------------------------------------------------------
#
# 259_thread_replay_differential.pl
#    spec-4.11 D1 increment 3a — differential for the online thread-recovery
#    RMW replay ENGINE (corruption-critical 8.A).
#
#    Increment 2 (t/258) proved the per-block apply matrix is byte-for-byte
#    equal to PG real redo.  This engine STREAMS that matrix over a dead
#    thread's WAL and read-modify-writes shared storage directly (bypassing the
#    buffer pool).  Its NEW surface is the streaming + the three gates, proven
#    here against a genuinely shared backend (cluster.shared_storage_backend =
#    local makes user relations route to cluster_smgr -- which_for == 1 -- while
#    paths still resolve in PGDATA, so pg_read_binary_file reads the same file
#    the engine writes):
#
#      L1 IDEMPOTENCE: replay a window whose records the shared page already
#         reflects -> every record LSN-gated, blocks_applied = 0, page byte-
#         unchanged.  This is the retry / cold-redo safety the LSN-gate buys.
#
#      L2 APPLY PARITY: seed block 0 with the page at an intermediate LSN (built
#         FPI-first by the t/258-proven redo_test), then replay the full window.
#         The engine must apply the post-seed deltas (blocks_applied >= 1) and
#         leave the shared page byte-for-byte equal to PG real redo.  This is the
#         single-machine stand-in for a 2-node dead peer's un-materialised pages
#         (the genuine cross-node e2e is forward, L239; the apply core itself is
#         t/258's, the write-back is what this proves).
#
#      FAIL-CLOSED (8.A):
#        L3 a relation-extension / init-page record in the window -> BLOCKED
#           (3a has no storage rmgr; extension forwards to Stage 5).
#        L4 an invalid window (scan_lower > scan_upper) -> BLOCKED.
#        L5 scan_upper beyond the durable WAL boundary (incomplete window) ->
#           BLOCKED, never a silent DONE short of the target.
#
#      ROUTING:
#        L6 a window mixing catalog and user-relation block refs -> the catalog
#           (per-node) refs are skipped as out-of-scope, the user refs handled.
#
#    Determinism (L247): autovacuum off, full_page_writes on (op1 bears the
#    FPI), large wal_keep_size, explicit CHECKPOINTs to flush the shared page and
#    to leave a straddle record past scan_upper.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/259_thread_replay_differential.pl
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

my $node = PgracClusterNode->new('threadreplay');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.shared_storage_backend = local\n"
	  . "cluster.smgr_user_relations = on\n"
	  . "autovacuum = off\n"
	  . "full_page_writes = on\n"
	  . "wal_keep_size = 256MB\n");
$node->start;

sub wal_lsn { return $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()'); }

# Hex of block 0 on shared storage (local backend resolves in PGDATA, so the
# path pg_relation_filepath returns is the file the engine writes).
sub read_page
{
	my ($rel) = @_;
	return $node->safe_psql('postgres',
		"SELECT encode(pg_read_binary_file(pg_relation_filepath('$rel'), 0, 8192), 'hex')");
}

# Establish block 0 of a fresh table and checkpoint, so the next touch bears an FPI.
sub seed_table
{
	my ($rel) = @_;
	$node->safe_psql('postgres',
		"CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres',
		"INSERT INTO $rel SELECT g, 'seed' || g FROM generate_series(1, 8) g");
	$node->safe_psql('postgres', 'CHECKPOINT');
}

# Parse the engine SRF's ':'-delimited summary.
sub replay
{
	my ($sql) = @_;
	my $s = $node->safe_psql('postgres', "SELECT $sql");
	my @f = split /:/, $s;
	return {
		result => $f[0],
		records => $f[1],
		applied => $f[2],
		gated => $f[3],
		oos => $f[4],
		through => $f[5],
	};
}

# ============================================================
# L1 IDEMPOTENCE: shared page already current -> every record LSN-gated.
# ============================================================
{
	seed_table('t259_idem');
	my $lo = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t259_idem VALUES (100, 'op1')");
	$node->safe_psql('postgres', "INSERT INTO t259_idem VALUES (101, 'op2')");
	my $op2_end = wal_lsn();
	$node->safe_psql('postgres', 'CHECKPOINT');    # flush block 0 to op2; leave a straddle record

	my $ref = read_page('t259_idem');
	my $r = replay("cluster_thread_replay_test('$lo', '$op2_end', NULL, NULL, NULL, NULL)");

	is($r->{result}, 'done', 'L1 idempotent replay returns done');
	is($r->{applied}, '0', 'L1 idempotent: no block applied (every record LSN-gated)');
	cmp_ok($r->{gated}, '>=', 1, 'L1 idempotent: block-0 records were gated');
	is(read_page('t259_idem'), $ref, 'L1 idempotent: shared page byte-unchanged');
}

# ============================================================
# L2 APPLY PARITY: seed a stale base, replay forward, page == PG real redo.
# ============================================================
{
	seed_table('t259_apply');
	my $lo = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t259_apply VALUES (100, 'op1_fpi')");
	my $mid = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t259_apply VALUES (101, 'op2_delta')");
	my $op2_end = wal_lsn();
	$node->safe_psql('postgres', 'CHECKPOINT');    # block 0 now at op2 = the real-redo reference

	my $ref = read_page('t259_apply');

	# Seed block 0 with the page at $mid (FPI-first via the t/258-proven SRF),
	# then replay the whole window: op1 is gated (<= seed), op2 is applied.
	my $r = replay(qq{
		cluster_thread_replay_test('$lo', '$op2_end', 't259_apply'::regclass, 0, 0,
			cluster_thread_apply_redo_test('t259_apply'::regclass, 0, 0,
				'$lo'::pg_lsn, '$mid'::pg_lsn, NULL))});

	is($r->{result}, 'done', 'L2 apply replay returns done');
	cmp_ok($r->{applied}, '>=', 1, 'L2 apply: engine applied >=1 block from a stale base');
	is(read_page('t259_apply'), $ref,
		'L2 apply: engine RMW result == PG real redo (byte-for-byte, 8.A)');
}

# ============================================================
# L3 FAIL-CLOSED: relation extension / init-page record -> BLOCKED.
# ============================================================
{
	$node->safe_psql('postgres',
		"CREATE TABLE t259_ext (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres', 'CHECKPOINT');    # empty file (0 blocks)
	my $lo = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t259_ext VALUES (1, 'x')");    # inits block 0
	my $hi = wal_lsn();

	# NO checkpoint: the init page stays in the dirty buffer, so shared storage
	# holds only the zero extension page.  The engine reads that new page (never
	# LSN-gated) and the init-page record is off the apply matrix -> BLOCKED.
	# (A checkpoint here would flush the page current and the record would gate
	# DONE -- which is correct, but would not exercise the extension fail-close.)
	my $r = replay("cluster_thread_replay_test('$lo', '$hi', NULL, NULL, NULL, NULL)");
	is($r->{result}, 'blocked',
		'L3 fail-closed: relation-extension / init-page -> BLOCKED (forward Stage 5)');
}

# ============================================================
# L4 FAIL-CLOSED: invalid window (scan_lower > scan_upper) -> BLOCKED.
# ============================================================
{
	my $lo = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t259_idem VALUES (200, 'z')");
	my $hi = wal_lsn();
	my $r = replay("cluster_thread_replay_test('$hi', '$lo', NULL, NULL, NULL, NULL)");
	is($r->{result}, 'blocked', 'L4 fail-closed: invalid window (lower > upper) -> BLOCKED');
}

# ============================================================
# L5 FAIL-CLOSED: scan_upper beyond durable WAL (incomplete) -> BLOCKED.
# ============================================================
{
	my $lo = wal_lsn();
	$node->safe_psql('postgres', "INSERT INTO t259_idem VALUES (300, 'w')");
	$node->safe_psql('postgres', 'CHECKPOINT');
	my $future = $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 16777216)::pg_lsn');
	my $r = replay("cluster_thread_replay_test('$lo', '$future', NULL, NULL, NULL, NULL)");
	is($r->{result}, 'blocked',
		'L5 fail-closed: scan_upper beyond WAL (incomplete window) -> BLOCKED (8.A)');
}

# ============================================================
# L6 ROUTING: catalog block refs skipped as out-of-scope, user refs handled.
# ============================================================
{
	seed_table('t259_route');
	my $lo = wal_lsn();
	$node->safe_psql('postgres', "CREATE TABLE t259_route_dummy (id int)");    # catalog block refs
	$node->safe_psql('postgres', "UPDATE t259_route SET v = v WHERE id = 1");  # in-scope, block 0
	my $hi = wal_lsn();
	$node->safe_psql('postgres', 'CHECKPOINT');    # flush block 0; leave a straddle record

	my $r = replay("cluster_thread_replay_test('$lo', '$hi', NULL, NULL, NULL, NULL)");
	is($r->{result}, 'done', 'L6 routing: mixed catalog/user window replays to done');
	cmp_ok($r->{oos}, '>=', 1,
		'L6 routing: catalog (per-node) block refs skipped as out-of-scope');
}

$node->stop;
done_testing();
