#-------------------------------------------------------------------------
#
# 263_thread_validated_end.pl
#    spec-4.11 D1 increment 3b-4a — the D4 validated torn-tail boundary pass.
#
#    replay_one's 3b-2 window used the registry's OBSERVATIONAL highest_lsn as the
#    replay upper.  D4 replaces it with the VALIDATED boundary: a decode-only pass
#    over the dead thread's per-thread WAL returns the EndRecPtr of the last
#    COMPLETE record, distinguishing
#      * a legitimate torn tail (the live insert point / crash point sits past the
#        last complete record) -> DONE, the boundary is the last complete record;
#      from
#      * corruption BELOW the durable watermark (decode stops short of the
#        registry's highest_lsn, a safe lower bound refreshed AFTER the bytes were
#        written) -> BLOCKED, never a silent truncation of the dead thread's
#        committed WAL (8.A).
#
#    Single-node stand-in (L239, mirrors t/260-262): node_id 0 routes its own WAL
#    into thread_1, so driving thread_1 exercises the real reader + decode over a
#    real WAL stream on one machine.
#
#      L1 healthy: a live WAL tail IS a legitimate torn tail -> DONE, the boundary
#         is >= the flushed records' end (decode reached the committed records).
#      L2 corruption guard: validated_min beyond the data -> the decode stops below
#         it -> BLOCKED (8.A; never reported DONE short of the durable watermark).
#      L3 fail-closed: a bad dead_tid -> BLOCKED.
#      L4 fail-closed: a missing per-thread dir -> BLOCKED.
#      L5 fail-closed: an invalid scan_lower -> BLOCKED.
#
#    Determinism (L247): autovacuum off, full_page_writes on, large wal_keep_size,
#    explicit CHECKPOINTs.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/263_thread_validated_end.pl
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

my $node = PgracClusterNode->new('threadvend');
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

# Drive the D4 pass; returns ($result, $valid_end).
sub validated_end
{
	my ($tid, $lo, $vmin) = @_;
	my $s = $node->safe_psql('postgres',
		"SELECT cluster_thread_validated_end_test($tid, '$lo', '$vmin')");
	my @f = split /:/, $s, 2;
	return ($f[0], $f[1]);
}

# pg_lsn >= comparison via the server.
sub lsn_ge
{
	my ($a, $b) = @_;
	return $node->safe_psql('postgres', "SELECT '$a'::pg_lsn >= '$b'::pg_lsn");
}

# ------------------------------------------------------------------
# Seed: a table, then commits, capture lo/hi, CHECKPOINT (flush).
# ------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE t263 (id int, v text) WITH (autovacuum_enabled = off)');
$node->safe_psql('postgres',
	"INSERT INTO t263 SELECT g, 'seed' || g FROM generate_series(1, 8) g");
$node->safe_psql('postgres', 'CHECKPOINT');

my $lo = wal_lsn();
$node->safe_psql('postgres', "INSERT INTO t263 VALUES (100, 'op1')");
$node->safe_psql('postgres', "INSERT INTO t263 VALUES (101, 'op2')");
my $hi = wal_lsn();
$node->safe_psql('postgres', 'CHECKPOINT');

my $future =
  $node->safe_psql('postgres', 'SELECT (pg_current_wal_lsn() + 1073741824)::pg_lsn');

# ============================================================
# L1 healthy: the live WAL tail is a legitimate torn tail -> DONE, the boundary
# reaches at least the flushed records' end.
# ============================================================
{
	my ($res, $vend) = validated_end(1, $lo, $hi);
	is($res, 'done', 'L1 healthy: validated_end decodes to a complete-record boundary -> DONE');
	is(lsn_ge($vend, $hi), 't',
		'L1 healthy: the validated boundary reaches the flushed committed records');
}

# ============================================================
# L2 corruption guard: validated_min beyond the data -> BLOCKED (8.A).
# ============================================================
{
	my ($res, undef) = validated_end(1, $lo, $future);
	is($res, 'blocked',
		'L2 corruption guard: decode stops below validated_min -> BLOCKED (never silent truncation)');
}

# ============================================================
# L3-L5 fail-closed.
# ============================================================
{
	my ($r0, undef) = validated_end(0, $lo, $lo);
	is($r0, 'blocked', 'L3 fail-closed: dead_tid 0 (legacy) -> BLOCKED');
	my ($r200, undef) = validated_end(200, $lo, $lo);
	is($r200, 'blocked', 'L3 fail-closed: dead_tid > slots -> BLOCKED');

	my ($r7, undef) = validated_end(7, $lo, $lo);
	is($r7, 'blocked', 'L4 fail-closed: missing per-thread dir -> BLOCKED');

	my ($rinv, undef) = validated_end(1, '0/0', $lo);
	is($rinv, 'blocked', 'L5 fail-closed: invalid scan_lower -> BLOCKED');
}

# ============================================================
# L6 (spec-4.11 3b-4c P1 fix): feeding validated_end's own boundary straight into
# replay_one_window must DONE.  validated_end TOLERATES the dead thread's
# legitimate torn tail (the live insert / crash point past the last complete
# record) and returns valid_end = that last complete record's end.  The replay
# engine must therefore STOP at valid_end and not re-read into the torn tail and
# fail closed.  Before the fix the engine read the record past scan_upper, hit the
# torn live-tail (XLogReadRecord NULL+errormsg -> aborted), and returned BLOCKED --
# so a clean validated window failed unless a CHECKPOINT straddle record happened
# to sit just past it (the trick t/261 L4 / t/259 rely on).  This leg uses NO such
# straddle: scan_upper IS the validated boundary.
# ============================================================
{
	my ($res, $vend) = validated_end(1, $lo, $hi);
	is($res, 'done', 'L6 setup: validated_end yields a complete-record boundary');
	my $r = $node->safe_psql('postgres',
		"SELECT cluster_thread_replay_one_test(1, '$lo', '$vend')");
	my $tok = (split /:/, $r)[0];
	is($tok, 'done',
		'L6 torn-tail: replay_one_window reaches DONE at the validated boundary (no straddle record; 3b-4c P1)');
}

$node->stop;
done_testing();
