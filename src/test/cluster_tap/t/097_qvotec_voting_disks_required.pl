#-------------------------------------------------------------------------
#
# 097_qvotec_voting_disks_required.pl
#    spec-2.6 Q7 v0.2 startup validator regression.
#
#    A multi-node cluster (pgrac.conf declares >1 node) running with
#    cluster.allow_single_node=off MUST have cluster.voting_disks
#    configured.  Without voting disks, qvotec has no quorum protocol
#    and backends would silently fail-open under partition (because
#    the xact.c commit-boundary fail-closed gate keys on
#    cluster.voting_disks being non-empty per P1.2).
#
#    Validator runs at end of phase 4 (cluster_finalize_startup_running)
#    after pgrac.conf has been parsed and node count is authoritative.
#
#    Test plan (3 L# scenarios):
#      L1: multi-node + allow=off + voting_disks empty → postmaster
#          refuses to start;log mentions "multi-node cluster requires
#          cluster.voting_disks" + errhint
#      L2: multi-node + allow=off + voting_disks=3 paths → postmaster
#          starts cleanly;in_quorum becomes true within a few cycles
#      L3: multi-node + allow=on + voting_disks empty → postmaster
#          starts (dev / single-node compat path remains open)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/097_qvotec_voting_disks_required.pl
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

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;

# Multi-node pgrac.conf — 2 declared nodes is enough to trigger the
# Q7 validator's "node_count > 1" branch.
my $conf_path = $node->data_dir . '/pgrac.conf';
PostgreSQL::Test::Utils::append_to_file($conf_path, <<'EOC');
[cluster]
name = pgrac-q7-validator-test

[node.0]
interconnect_addr = 10.0.0.0:6432

[node.1]
interconnect_addr = 10.0.0.1:6432
EOC

# Pre-allocate 3 voting disk files (used by L2).
my $disk_dir = PostgreSQL::Test::Utils::tempdir();
for my $i (0 .. 2) {
	# 64KB zero-filled — slot 0..127 each 512B; qvotec will overwrite
	# self slot on first poll cycle.
	open(my $fh, '>', "$disk_dir/disk$i") or die $!;
	binmode $fh;
	print $fh ("\0" x (128 * 512));
	close $fh;
}


# ============================================================
# L1: multi-node + allow=off + voting_disks empty → FATAL
# ============================================================
PostgreSQL::Test::Utils::append_to_file(
	$node->data_dir . '/postgresql.conf',
	"cluster.node_id = 0\n" . "cluster.allow_single_node = off\n"
		# cluster.voting_disks left at default (empty)
);
unlink $node->logfile;
my $start_failed = !$node->start(fail_ok => 1);
ok($start_failed, 'L1 multi-node + allow=off + voting_disks empty → postmaster refuses');

my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log,
	 qr/multi-node cluster requires cluster\.voting_disks/,
	 'L1 FATAL errmsg matches Q7 validator wording');
like($log,
	 qr/cluster\.voting_disks is empty/i,
	 'L1 errdetail mentions voting_disks empty');
like($log,
	 qr/odd majority recommended.*1.*3.*5.*7/i,
	 'L1 errhint suggests 1/3/5/7 disks');


# ============================================================
# L2: multi-node + allow=off + voting_disks=3 paths → OK
#
# Note: cluster.voting_disks was never written to postgresql.conf in
# L1 (defaulted empty), so adjust_conf would no-op (per L59 / 072 L4
# lesson — adjust_conf modifies existing lines only).  Use
# append_to_file for the FIRST write of the GUC.
# ============================================================
PostgreSQL::Test::Utils::append_to_file(
	$node->data_dir . '/postgresql.conf',
	"cluster.voting_disks = '$disk_dir/disk0,$disk_dir/disk1,$disk_dir/disk2'\n");
unlink $node->logfile;
$node->start;
ok(1, 'L2 multi-node + allow=off + voting_disks=3 paths → postmaster started');

# Give qvotec at least one poll cycle (default 2000ms; sleep 3 to be safe)
sleep 3;
my $in_quorum = $node->safe_psql('postgres',
	q{SELECT in_quorum FROM pg_cluster_quorum_state});
is($in_quorum, 't', 'L2 in_quorum=true after first poll cycle');

my $disks_ok = $node->safe_psql('postgres',
	q{SELECT disks_ok FROM pg_cluster_quorum_state});
is($disks_ok, '3', 'L2 disks_ok=3 (all three voting disks reachable)');


# ============================================================
# L3: multi-node + allow=on + voting_disks empty → OK (dev mode)
# ============================================================
$node->stop;
$node->adjust_conf('postgresql.conf', 'cluster.allow_single_node', 'on');
$node->adjust_conf('postgresql.conf', 'cluster.voting_disks',     "''");
unlink $node->logfile;
$node->start;
ok(1, 'L3 multi-node + allow=on + voting_disks empty → postmaster started (dev path)');

# In dev path qvotec stays alive but performs no I/O; in_quorum is
# false (Q4 v0.2 fail-closed default).  Backend fail-closed is also
# disabled because voting_disks is empty (P1.2).  Verify the path
# actually engages — pg_cluster_voting_disks should report 0 rows.
my $disk_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_voting_disks});
is($disk_count, '0',
   'L3 pg_cluster_voting_disks 0 rows when voting_disks empty (dev mode)');

$node->stop;

done_testing();
