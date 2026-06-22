#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 289_cf_bootstrap.pl
#    spec-5.6 -- shared pg_control authority bootstrap + Phase-2 cross-node
#    rename-contract rendezvous, on a TRUE shared-sysid 2-node cluster.
#
#    Unlike the shared-NOTHING ClusterPair harness (each node initdb'd with its
#    own system_identifier), this test builds a real shared-everything pair:
#    node1 is init_from_backup of node0, so both share ONE system_identifier and
#    one shared pg_control authority under cluster.shared_data_dir.
#
#    Lifecycle (Grow-from-single-node; avoids the concurrent-migrate torn-tmp
#    race -- two nodes must never create the authority at the same time):
#      0. node0 init -> backup -> node1 init_from_backup (shared sysid).
#      1. node0 SINGLE-NODE with controlfile_shared_authority=on (cluster off,
#         lms off, t/287 recipe) migrates its local pg_control into the shared
#         authority and symlinks to it; stop.
#      2. reconfigure BOTH for a 2-node cluster (enabled on, tier1, voting,
#         authority on); node1's local pg_control is still a regular file with
#         the shared sysid.
#      3. start both concurrently: each runs Phase-2 (nonce+ack rendezvous over
#         the shared storage) before its role gate -> CROSSNODE_VERIFIED ->
#         both come up; node1's local pg_control becomes a symlink to the shared
#         authority (its sysid matches, so the Da3 gate passes).
#
#    Legs:
#      L1  both nodes start and connect (CSSD) on the shared-sysid cluster.
#      L2  both $PGDATA/global/pg_control are symlinks to the same shared
#          authority (DoD-1: same shared path / inode).
#      L3  pg_controldata on both nodes reports the SAME system_identifier
#          (DoD-3: one control state).
#      L4  Phase-2 verified: both persisted the cross-node rename contract
#          (the cf phase-2 LOG line / the node stays up = role gate passed).
#
#    NO SKIP for the core (L77).  lms is OFF here to isolate Phase-2 + the
#    authority bootstrap from the GES checkpoint path (CF X serialization is
#    t/288).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/289_cf_bootstrap.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (§3.3 / §3.9 T6)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

# ---- shared storage root + voting disks both postmasters can reach ----
my $shared_root = PostgreSQL::Test::Utils::tempdir();
mkdir "$shared_root/global" or die "mkdir shared global: $!";

my $disk_dir = PostgreSQL::Test::Utils::tempdir();
my @disks;
for my $i (0 .. 2)
{
	my $p = "$disk_dir/disk$i";
	open(my $fh, '>', $p) or die "open $p: $!";
	binmode $fh;
	print $fh ("\0" x (128 * 512));
	close $fh;
	push @disks, $p;
}
my $disks_csv = join(',', @disks);

# ---- IC ports ----
my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();

# ----------
# Step 0: node0 init -> backup -> node1 init_from_backup (shared sysid).
# ----------
my $node0 = PostgreSQL::Test::Cluster->new('cf_boot_node0');
$node0->init(allows_streaming => 1);	# enable pg_basebackup for the shared-sysid copy
$node0->start;
$node0->backup('cfb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('cf_boot_node1');
$node1->init_from_backup($node0, 'cfb');

# ----------
# Step 1: node0 single-node builds the shared authority (t/287 recipe).
# ----------
$node0->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
EOC
$node0->start;
# The authority file must now exist under the shared root.
ok(-e "$shared_root/global/pg_control", 'step1: node0 built the shared authority');
$node0->stop;

# ----------
# Step 2: reconfigure BOTH for a 2-node cluster (authority on, lms off).
# ----------
my $common_conf = <<EOC;
shared_buffers = 16MB
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.lms_enabled = off
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
EOC

# node0 already has the single-node authority lines; appending the cluster
# lines (later-wins) turns it into a 2-node member.  node1 starts from the
# clean backup conf.
$node0->append_conf('postgresql.conf', $common_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node1->append_conf('postgresql.conf', $common_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = cf_boot

[node.0]
interconnect_addr = 127.0.0.1:$ic0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
EOC
PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $pgrac_conf);

# ----------
# Step 3: start both concurrently -> Phase-2 -> both up.
#
# node0->start blocks (pg_ctl -w) until node0 is ready, but node0's Phase-2
# rendezvous needs node1 concurrently running its own Phase-2.  So launch
# node1's postmaster in the background (pg_ctl -W, no wait) first, then start
# node0 normally; they rendezvous over the shared storage and both come up.
# ----------
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=cf_boot_node1', 'start');

$node0->start;					# rendezvouses with the background node1
$node1->_update_pid(1);			# sync node1's running state for safe_psql/stop

# Confirm node1 actually came up (it should have rendezvoused with node0).
my $n1_up = 0;
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres', 'SELECT 1');
	if (defined $rc && $rc == 0) { $n1_up = 1; last; }
	usleep(500_000);
}
usleep(3_000_000);

# ----------
# L1: both nodes alive on the shared-sysid cluster.
# ----------
my $a0 = $node0->safe_psql('postgres', 'SELECT 1');
my $a1 = $node1->safe_psql('postgres', 'SELECT 1');
is($a0, '1', 'L1: node0 is up on the shared-sysid 2-node cluster');
is($a1, '1', 'L1: node1 is up on the shared-sysid 2-node cluster');

# ----------
# L2: both local control paths are symlinks to the same shared authority.
# ----------
my $l0 = $node0->data_dir . '/global/pg_control';
my $l1 = $node1->data_dir . '/global/pg_control';
ok(-l $l0, 'L2: node0 global/pg_control is a symlink to the shared authority');
ok(-l $l1, 'L2: node1 global/pg_control is a symlink to the shared authority');
is(readlink($l0), "$shared_root/global/pg_control", 'L2: node0 symlink targets the shared authority');
is(readlink($l1), "$shared_root/global/pg_control", 'L2: node1 symlink targets the shared authority');

# ----------
# L3: both nodes read the same control state / system_identifier (DoD-3).
#     pg_control_system() reads $PGDATA/global/pg_control (through the symlink),
#     so a matching system_identifier proves both see the one shared authority.
# ----------
my $sysid0 = $node0->safe_psql('postgres', 'SELECT system_identifier FROM pg_control_system()');
my $sysid1 = $node1->safe_psql('postgres', 'SELECT system_identifier FROM pg_control_system()');
ok($sysid0 ne '' && $sysid0 eq $sysid1,
	"L3: both nodes read the same shared control state (system_identifier $sysid0)");

# ----------
# L4: BOTH nodes Phase-2 cross-node verified (each rendezvoused with the other
#     and persisted CROSSNODE_VERIFIED; without it a node FATALs at the role
#     gate, so coming up at all already implies it, and the symmetric LOG line
#     confirms BOTH directions -- not just "at least one").
# ----------
my $log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
my $log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
ok($log0 =~ /cross-node storage rename contract verified with node 1/,
	'L4a: node0 logged Phase-2 cross-node verification with node 1');
ok($log1 =~ /cross-node storage rename contract verified with node 0/,
	'L4b: node1 logged Phase-2 cross-node verification with node 0');

# Both nodes persisted the per-node contract record bound to the storage uuid.
ok(-f $node0->data_dir . '/global/pgrac_cf_contract',
	'L4c: node0 persisted its cross-node storage contract record');
ok(-f $node1->data_dir . '/global/pgrac_cf_contract',
	'L4d: node1 persisted its cross-node storage contract record');

$node0->stop;
$node1->stop;

# ----------
# L5: negative -- a MULTI-NODE node started with NO peer cannot cross-node
#     verify (Phase-2 finds no peer probe), so it must FAIL CLOSED rather than
#     take single-node authority over the shared control file (§3.3 B5).  Fresh
#     shared root (no persisted CROSSNODE_VERIFIED) + a short CF timeout so the
#     Phase-2 rendezvous gives up quickly.
# ----------
my $neg_shared = PostgreSQL::Test::Utils::tempdir();
mkdir "$neg_shared/global" or die "mkdir neg global: $!";
my $neg_ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $neg_ic1 = PostgreSQL::Test::Cluster::get_free_port();

my $neg = PostgreSQL::Test::Cluster->new('cf_boot_neg');
$neg->init;
$neg->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.lms_enabled = off
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.shared_data_dir = '$neg_shared'
cluster.controlfile_shared_authority = on
cluster.cf_enqueue_timeout_ms = 3000
cluster.node_id = 0
EOC
# pgrac.conf declares a 2-node cluster (node_count = 2 -> multi_node) but only
# node 0 ever starts, so node 1's Phase-2 probe never appears.
PostgreSQL::Test::Utils::append_to_file($neg->data_dir . '/pgrac.conf', <<EOC);
[cluster]
name = cf_boot_neg

[node.0]
interconnect_addr = 127.0.0.1:$neg_ic0

[node.1]
interconnect_addr = 127.0.0.1:$neg_ic1
EOC

my $nret = $neg->start(fail_ok => 1);
ok(!$nret, 'L5: a multi-node node with no peer fails to start (cannot cross-node verify)');
my $nlog = PostgreSQL::Test::Utils::slurp_file($neg->logfile);
like($nlog, qr/cannot establish bootstrap shared control-file authority on a multi-node cluster/,
	'L5: the failure is the B5 fail-closed gate (unverified storage -> no authority write)');

done_testing();
