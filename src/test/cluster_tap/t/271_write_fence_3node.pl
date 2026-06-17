#-------------------------------------------------------------------------
#
# 271_write_fence_3node.pl
#    spec-4.12b D8 (L7) -- 3-node fence-firing topology: a node declared dead by a
#    reconfig (in the durable fence marker's dead_bitmap) but STILL ALIVE fails its
#    cluster_smgr shared-storage writes closed (53R51), while BOTH survivors (NOT in
#    the dead set) keep writing.  Demonstrates the cooperative fence on a 3-node
#    shared-FS cluster under enforcement default-ON (spec-4.12b D4).
#
#    Mechanism: same proven inject as t/267/t/272 -- cluster_reconfig_inject_dead_
#    node_test drives node0's REAL lmon coordinator path, which under enforcement=ON
#    writes a durable fence marker (the dead node in dead_bitmap) to a quorum-majority
#    of voting disks; the dead-but-alive node's own qvotec reads it and self-fences.
#
#    SCOPE NOTE (honest, L239 + Q7=A).  This drives ONE declared-dead node in a
#    3-node topology.  A SINGLE reconfig declaring TWO nodes dead at once needs a
#    multi-bit inject helper (the current cluster_reconfig_inject_dead_node_test
#    declares one node; two sequential single-node injects are correctly REJECTED by
#    the D5 dead-set-superset guard, which forbids shrinking the dead set -- a node
#    cannot be silently un-fenced).  The "marker carries the COMPLETE multi-dead
#    bitmap / never shrinks" invariant is therefore unit-pinned
#    (test_cluster_write_fence test_authority_advances_dead_superset_required:
#    grown dead set advances, shrunk dead set rejected) rather than driven here.
#
#    Anti-false-green: the soon-to-be-fenced node writes SUCCESSFULLY before the
#    inject, so the post-inject 53R51 is the fence firing, not a pre-existing failure.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/271_write_fence_3node.pl
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

use PostgreSQL::Test::ClusterTriple;
use Test::More;
use Time::HiRes qw(usleep);

# 3 declared nodes + cluster_fs shared data + quorum voting disks; enforcement
# default-ON (spec-4.12b D4, out of the box).
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'wf3node',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => ['autovacuum = off']);
$triple->start_triple;

usleep(3_000_000); # let the IC mesh + qvotec lease settle

my $n0 = $triple->node0;
my $n1 = $triple->node1;
my $n2 = $triple->node2;

is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
is($n2->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node2 alive');
is($n2->safe_psql('postgres', q{SHOW cluster.write_fence_enforcement}),
	'on', 'L0 enforcement ON by default (spec-4.12b D4)');

# ----------
# L1 baseline (anti-false-green): node2 is NOT yet fenced -> shared write succeeds.
# ----------
$n2->safe_psql('postgres', q{
	CREATE TABLE wf3 (id int primary key, v int);
	INSERT INTO wf3 SELECT g, g FROM generate_series(1, 100) g;
});
is($n2->safe_psql('postgres', q{SELECT count(*) FROM wf3}),
	'100', 'L1 pre-inject: node2 shared-storage write succeeds (not yet fenced)');

# ----------
# INJECT: node0 (coordinator) declares node2 dead.  Under enforcement=ON this writes
# a durable fence marker (node2 in dead_bitmap) to a quorum-majority of voting disks.
# node2 stays ALIVE.
# ----------
is($n0->safe_psql('postgres', 'SELECT cluster_reconfig_inject_dead_node_test(2)'),
	't', 'L2 inject: node2 declared dead by node0 coordinator (fence marker written)');

# ----------
# L3: node2 (declared dead, still alive) fails its shared-storage writes closed.
# ----------
my $fenced = 0;
for (1 .. 40)
{
	my ($rc, $out, $err) = $n2->psql('postgres',
		q{INSERT INTO wf3 SELECT g, g FROM generate_series(101, 300) g});
	if ($rc != 0
		&& $err =~ /write[ -]?fenced|53R51|cluster shared-storage .* rejected/)
	{
		$fenced = 1;
		last;
	}
	usleep(250_000);
}
ok($fenced, 'L3 declared-dead-but-alive node2 cluster_smgr write -> 53R51 (self-fenced)');

ok($n2->poll_query_until('postgres',
		"SELECT (SELECT value::bigint FROM cluster_dump_state() "
	  . "WHERE category = 'write_fence' AND key = 'hot_gate_blocked') > 0", 't'),
	'L3 node2 hot_gate_blocked advanced (gate fired)');

# ----------
# L4: BOTH survivors (node0, node1 -- not in the dead set) keep writing.  The
# cooperative fence freezes only the dead side, never the live survivors.
# ----------
$n0->safe_psql('postgres', q{
	CREATE TABLE wf3_s0 (id int);
	INSERT INTO wf3_s0 SELECT g FROM generate_series(1, 40) g;
});
is($n0->safe_psql('postgres', q{SELECT count(*) FROM wf3_s0}),
	'40', 'L4 survivor node0 still writes');

$n1->safe_psql('postgres', q{
	CREATE TABLE wf3_s1 (id int);
	INSERT INTO wf3_s1 SELECT g FROM generate_series(1, 40) g;
});
is($n1->safe_psql('postgres', q{SELECT count(*) FROM wf3_s1}),
	'40', 'L4 survivor node1 still writes');

$triple->stop_triple;
done_testing();
