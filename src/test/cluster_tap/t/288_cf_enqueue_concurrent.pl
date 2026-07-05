#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 288_cf_enqueue_concurrent.pl
#    spec-5.6 -- CF (control-file) enqueue: concurrent CHECKPOINT across a true
#    shared-sysid 2-node cluster serializes through CF X (the Oracle CF enqueue),
#    so the one shared pg_control authority is never torn (DoD-2 / DoD-4).
#
#    Same shared-sysid lifecycle as t/289 (node1 = init_from_backup of node0;
#    node0 single-node builds the authority; both start a 2-node cluster with
#    Phase-2 verifying cross-node), but lms is ON so steady-state checkpoints
#    take a real cross-node CF X over GES.
#
#    Legs:
#      L1  both nodes up on the shared-sysid cluster, both symlinked to the one
#          shared authority (Phase-2 verified).
#      L5  concurrent CHECKPOINT on both nodes, many interleaved rounds: the
#          shared authority's system_identifier stays stable and pg_control_
#          system() always reads a valid image -- a torn write (two unserialized
#          durable_renames over the same global/pg_control.tmp) would corrupt it.
#      L6  cf_x_acquire grows on at least one node: CF X was actually taken for
#          the checkpoints (direct serialization proof, not just no-torn).
#
#    lms is ON; CF X over GES is the spec-5.3 substrate (proven cross-node by
#    t/283).  NO SKIP for the core (L77).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/288_cf_enqueue_concurrent.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (§3.4 / §3.5)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use POSIX ();
use Test::More;
use Time::HiRes qw(usleep);

sub cf_counter
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres', qq{
		SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
		WHERE category = 'cf' AND key = '$key'});
	return $v // 0;
}

# ---- shared storage root + voting disks ----
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

my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();

# ---- node0 init -> backup -> node1 init_from_backup (shared sysid) ----
my $node0 = PostgreSQL::Test::Cluster->new('cf_cc_node0');
$node0->init(allows_streaming => 1);
$node0->start;
$node0->backup('cfb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('cf_cc_node1');
$node1->init_from_backup($node0, 'cfb');

# ---- node0 single-node builds the shared authority ----
# spec-5.6a grow-from-single contract: the seed sets its node_id before the
# final single-era shutdown so that shutdown writes its recovery anchor.
$node0->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
cluster.node_id = 0
EOC
$node0->start;
$node0->stop;

# ---- reconfigure BOTH for a 2-node cluster (lms ON for cross-node CF X) ----
my $common_conf = <<EOC;
shared_buffers = 16MB
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.lms_enabled = on
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.grd_max_entries = 1024
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
EOC
$node0->append_conf('postgresql.conf', $common_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node1->append_conf('postgresql.conf', $common_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = cf_cc

[node.0]
interconnect_addr = 127.0.0.1:$ic0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
EOC
PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $pgrac_conf);

# ---- start both concurrently (node1 backgrounded so Phase-2 rendezvouses) ----
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=cf_cc_node1', 'start');
$node0->start;
$node1->_update_pid(1);
my $n1_up = 0;
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres', 'SELECT 1');
	if (defined $rc && $rc == 0) { $n1_up = 1; last; }
	usleep(500_000);
}

# ----------
# L1: both up + symlinked to the one shared authority.
# ----------
is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1: node0 up');
is($node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1: node1 up');
ok(-l $node0->data_dir . '/global/pg_control'
	  && readlink($node0->data_dir . '/global/pg_control') eq "$shared_root/global/pg_control",
	'L1: node0 symlinked to shared authority');
ok(-l $node1->data_dir . '/global/pg_control'
	  && readlink($node1->data_dir . '/global/pg_control') eq "$shared_root/global/pg_control",
	'L1: node1 symlinked to shared authority');

# Let LMS/GES settle so checkpoints take a real cross-node CF X.
usleep(5_000_000);

# ----------
# L5/L6: GENUINELY concurrent CHECKPOINT (node0 in the parent, node1 in a fork,
# both firing $rounds rounds at the same time) so CF X actually contends.  The
# CF enqueue must serialize them: every CHECKPOINT on both nodes returns rc=0,
# the shared authority stays a single valid image (no torn durable_rename), each
# node's cf_x_acquire grows by at least $rounds, and cf_failclosed does NOT grow.
# ----------
my $rounds = 12;
my $sysid = $node0->safe_psql('postgres', 'SELECT system_identifier FROM pg_control_system()');

# baseline counters (assert the DELTA over the concurrent burst).
my $x0_b  = cf_counter($node0, 'cf_x_acquire');
my $x1_b  = cf_counter($node1, 'cf_x_acquire');
my $fc0_b = cf_counter($node0, 'cf_failclosed');
my $fc1_b = cf_counter($node1, 'cf_failclosed');

# node1's $rounds CHECKPOINTs run in a forked child concurrently with node0's in
# the parent.  The child exits with its failed-round count via POSIX::_exit so
# it does NOT run the END-block node teardown.
my $child = fork();
if (defined $child && $child == 0)
{
	my $fails = 0;
	for (1 .. $rounds)
	{
		my ($rc) = $node1->psql('postgres', 'CHECKPOINT', timeout => 60);
		$fails++ if !defined $rc || $rc != 0;
	}
	POSIX::_exit($fails > 255 ? 255 : $fails);
}

my $fail0 = 0;
for (1 .. $rounds)
{
	my ($rc) = $node0->psql('postgres', 'CHECKPOINT', timeout => 60);
	$fail0++ if !defined $rc || $rc != 0;
}
waitpid($child, 0);
my $fail1 = $? >> 8;

is($fail0, 0, "L5a: all $rounds node0 CHECKPOINTs returned rc=0 under concurrency");
is($fail1, 0, "L5b: all $rounds node1 CHECKPOINTs returned rc=0 under concurrency");

# no torn: the shared authority still reads one valid image with the same
# system_identifier from BOTH nodes.
my $s0 = $node0->safe_psql('postgres', 'SELECT system_identifier FROM pg_control_system()');
my $s1 = $node1->safe_psql('postgres', 'SELECT system_identifier FROM pg_control_system()');
ok(($s0 // '') eq $sysid && ($s1 // '') eq $sysid,
	'L5c: shared authority stayed valid + stable under concurrent CHECKPOINT (no torn)');

my $x0_d  = cf_counter($node0, 'cf_x_acquire') - $x0_b;
my $x1_d  = cf_counter($node1, 'cf_x_acquire') - $x1_b;
my $fc0_d = cf_counter($node0, 'cf_failclosed') - $fc0_b;
my $fc1_d = cf_counter($node1, 'cf_failclosed') - $fc1_b;
diag("L6 deltas: node0 cf_x=+$x0_d failclosed=+$fc0_d ; node1 cf_x=+$x1_d failclosed=+$fc1_d");

cmp_ok($x0_d, '>=', $rounds, "L6a: node0 cf_x_acquire grew by >= $rounds (every checkpoint took CF X)");
cmp_ok($x1_d, '>=', $rounds, "L6b: node1 cf_x_acquire grew by >= $rounds (every checkpoint took CF X)");
is($fc0_d + $fc1_d, 0, 'L6c: cf_failclosed did NOT grow on either node (CF X granted, never fail-closed)');

$node0->stop;
$node1->stop;
done_testing();
