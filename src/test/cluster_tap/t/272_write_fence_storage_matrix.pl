#-------------------------------------------------------------------------
#
# 272_write_fence_storage_matrix.pl
#    spec-4.12b D7 (M1-M3) -- the shared-FS cooperative write-fence fail-closed
#    boundary matrix.  On the ONE backend that is testable today (spec-4.5a
#    cluster_fs shared data), prove that a node which has been declared dead by a
#    reconfig (so it is in the durable fence marker's dead_bitmap) but is STILL
#    ALIVE fails every cluster_smgr shared-storage write entry closed (53R51), under
#    enforcement default-ON (spec-4.12b D4).
#
#    Mechanism (same proven inject as t/267): cluster_reconfig_inject_dead_node_test
#    drives node0's REAL lmon coordinator path, which under enforcement=ON writes a
#    durable fence marker declaring node1 dead.  node1's own qvotec poll reads it,
#    sets self_fenced, and the D5 hot gate (cluster_write_fence_reject_if_fenced,
#    the ONE helper every entry shares -- L240) fails closed.
#
#    M1 (self-fenced column, deterministic): exercise the user-visible operations
#        that drive the distinct cluster_smgr write entries on the fenced node and
#        assert each fails closed:
#          INSERT into an existing user relation -> extend / write
#          CREATE TABLE (user)                   -> create
#          TRUNCATE                              -> truncate
#          DROP TABLE                            -> unlink
#    M2 (escape hatch): cross-referenced -- enforcement off/dev is a no-op is pinned
#        by the PURE judge unit (test_cluster_write_fence test_enforcement_off_is_
#        escape_hatch) and the single-node surface (t/269 L6/L7); enforcement is
#        PGC_POSTMASTER so it cannot be toggled within one running cluster here.
#    M3 (doc consistency): docs/storage-fencing-matrix.md (pgrac) marks ONLY the
#        shared-FS row as "L1 cooperative ✅ end-to-end verifiable"; this test IS
#        that verification.  The lease-expired / stale-epoch columns of the matrix
#        need a real partition / epoch-skew window and are honest-SKIPped here
#        (L239); they are documented as forward in the matrix doc.
#
#    Anti-false-green: M1 first writes to node1 BEFORE the inject and asserts it
#    SUCCEEDS, so the post-inject 53R51 cannot be a pre-existing failure -- it is the
#    fence firing.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/272_write_fence_storage_matrix.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    pgrac-original file.
#    Spec: spec-4.12b-write-fence-default-on-storage-matrix.md (FROZEN v0.3)
#
#-------------------------------------------------------------------------
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# 2 declared nodes + cluster_fs shared data + quorum voting disks, enforcement
# default-ON (spec-4.12b D4 -- not set here, it is the out-of-the-box default).
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'wfmatrix',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => ['autovacuum = off']);
$pair->start_pair;

usleep(3_000_000); # let the IC tier1 mesh + qvotec lease settle (t/267 pattern)

my $n0 = $pair->node0;
my $n1 = $pair->node1;

is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'M0 node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'M0 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'M0 node0 sees node1 connected');

# enforcement is ON out of the box (D4), and voting disks are configured -> the
# fence is ACTIVELY enforcing on this cluster.
is($n1->safe_psql('postgres', q{SHOW cluster.write_fence_enforcement}),
	'on', 'M0 enforcement is ON by default (spec-4.12b D4)');

# ----------
# M1 baseline (anti-false-green): node1 is NOT yet fenced, so a shared-storage
# write SUCCEEDS.  This proves the post-inject 53R51 is the fence firing, not a
# pre-existing inability to write.
# ----------
$n1->safe_psql('postgres', q{
	CREATE TABLE wf_m (id int primary key, v int);
	INSERT INTO wf_m SELECT g, g FROM generate_series(1, 100) g;
});
is($n1->safe_psql('postgres', q{SELECT count(*) FROM wf_m}),
	'100', 'M1 pre-inject: node1 shared-storage write succeeds (not yet fenced)');

# ----------
# INJECT: node0 (coordinator) declares node1 dead.  Under enforcement=ON this
# writes a durable fence marker (node1 in dead_bitmap) to a quorum-majority of
# voting disks.  node1 stays ALIVE.
# ----------
is($n0->safe_psql('postgres', 'SELECT cluster_reconfig_inject_dead_node_test(1)'),
	't', 'M1 inject: node1 declared dead by node0 coordinator (fence marker written)');

# Helper: retry an operation on node1 until it fails write-fenced (absorbs the
# qvotec poll/refresh latency), returns 1 if it ever fails closed.
sub fenced_until
{
	my ($sql) = @_;
	for (1 .. 40)
	{
		my ($rc, $out, $err) = $n1->psql('postgres', $sql);
		return 1
		  if $rc != 0
		  && $err =~ /write[ -]?fenced|53R51|cluster shared-storage .* rejected/;
		usleep(250_000);
	}
	return 0;
}

# ----------
# M1 self-fenced column: each distinct cluster_smgr write entry fails closed on the
# now-fenced (but still alive) node1.
# ----------
ok(fenced_until(q{INSERT INTO wf_m SELECT g, g FROM generate_series(101, 300) g}),
	'M1 write/extend entry: INSERT on fenced node1 -> 53R51');

# Once fenced the token stays fenced (the dead_bitmap only grows in Stage 4), so the
# remaining entries fail closed immediately -- no extra settle loop needed, but we
# reuse fenced_until for robustness against scheduling jitter.
ok(fenced_until(q{CREATE TABLE wf_m2 (id int)}),
	'M1 create entry: CREATE TABLE on fenced node1 -> 53R51');
ok(fenced_until(q{TRUNCATE wf_m}),
	'M1 truncate entry: TRUNCATE on fenced node1 -> 53R51');
ok(fenced_until(q{DROP TABLE wf_m}),
	'M1 unlink entry: DROP TABLE on fenced node1 -> 53R51');

# ----------
# M1 observability: the fenced node's hot_gate_blocked counter advanced (the gate
# actually fired -- not a different error masquerading as a fence).
# ----------
ok($n1->poll_query_until('postgres',
		"SELECT (SELECT value::bigint FROM cluster_dump_state() "
	  . "WHERE category = 'write_fence' AND key = 'hot_gate_blocked') > 0", 't'),
	'M1 hot_gate_blocked counter advanced on the fenced node (gate fired)');

# ----------
# M3 doc consistency: the survivor (node0, NOT in the dead set) is NOT fenced and
# can still write -- the matrix doc claims shared-FS cooperative fences the dead
# side WITHOUT freezing the live survivor.
# ----------
$n0->safe_psql('postgres', q{
	CREATE TABLE wf_survivor (id int);
	INSERT INTO wf_survivor SELECT g FROM generate_series(1, 50) g;
});
is($n0->safe_psql('postgres', q{SELECT count(*) FROM wf_survivor}),
	'50', 'M3 survivor node0 (not in dead set) still writes (matrix: dead side fenced, survivor free)');

# M2 (escape hatch) + lease-expired / stale-epoch columns: honest SKIP (L239).
# enforcement off/dev no-op is unit-pinned (test_enforcement_off_is_escape_hatch)
# + t/269 L6/L7; lease-expired / stale-epoch need a partition / epoch-skew window
# that this single-host deterministic inject harness cannot reproduce (Q7=A) --
# documented as forward in docs/storage-fencing-matrix.md.
SKIP: {
	skip 'escape-hatch + lease-expired/stale-epoch columns: unit-pinned / need '
	  . 'partition timing (Q7=A honest SKIP, see docs/storage-fencing-matrix.md)', 1;
	ok(1, 'placeholder');
}

$pair->stop_pair;
done_testing();
