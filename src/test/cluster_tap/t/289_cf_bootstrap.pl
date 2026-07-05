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
#    spec-5.6a restart legs (per-node recovery anchor; the shared authority's
#    checkpoint fields are deliberately poisoned with the peer's checkpoint
#    before every restart, so each leg fails without the anchor):
#      L-r1 crash-restart with the peer alive: node1 kill9 after a node0
#           checkpoint overwrote the shared authority; restart adopts the
#           anchor and replays its own tail ("redo done at" >= the LSN
#           written before the crash).  This stack runs lms off, so the
#           tail is driven with pg_logical_emit_message (user-table writes
#           would hit the relation-extend gate); data-truth restart
#           coverage on the full stack is t/339's.
#      L-r2 clean-restart order: clean stop -> start must NOT run recovery
#           (anchor state SHUTDOWNED) even though the shared state says the
#           peer is IN_PRODUCTION; immediate-stop -> start must run it.
#      L-r3 anchor missing (with .bak): label-less boot fails closed 53RB3.
#      L-r4 corrupt primary -> .bak adoption WARNING; both corrupt -> 53RB3.
#      L-r5 injection forces the fail-closed branch on a healthy anchor
#           (L408); disarmed reboot moves recovery_anchor_boot_adopt_count.
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
#    Spec: spec-5.6a-per-node-recovery-anchor.md (§4 L-r1 .. L-r5)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use File::Copy qw(copy);
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
# spec-5.6a grow-from-single provisioning contract: the seed node sets its
# cluster.node_id BEFORE its final single-era shutdown, so that shutdown's
# checkpoint writes the per-node recovery anchor it will need on its first
# boot as a cluster member (a label-less multi-node boot without an anchor
# fails closed, 53RB3).
$node0->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared_root'
cluster.controlfile_shared_authority = on
cluster.node_id = 0
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

# ==========================================================================
# spec-5.6a: per-node recovery anchor restart legs (L-r1 .. L-r5).
#
# Every leg first poisons the shared authority's checkpoint fields with the
# PEER's checkpoint (a node0 CHECKPOINT overwrites them), so a restarting
# node1 that consulted the shared fields would look for a foreign checkpoint
# in its own WAL thread and PANIC.  Coming up at all is the anchor working.
# ==========================================================================
my $anchor1     = "$shared_root/global/pgrac_recovery_anchor_n1";
my $anchor1_bak = "$anchor1.bak";

sub cf_counter
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres', qq{
		SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
		WHERE category = 'cf' AND key = '$key'});
	return $v // 0;
}

# Numeric compare of two X/Y LSN strings: -1 / 0 / 1.
sub lsn_cmp
{
	my ($a, $b) = @_;
	my ($ah, $al) = map { hex } split qr{/}, $a;
	my ($bh, $bl) = map { hex } split qr{/}, $b;
	return $ah <=> $bh || $al <=> $bl;
}

# Flip one byte at the given offset of a file (corrupts its CRC).
sub flip_byte
{
	my ($path, $off) = @_;
	open my $fh, '+<', $path or die "flip_byte open $path: $!";
	binmode $fh;
	seek $fh, $off, 0 or die "flip_byte seek: $!";
	defined(read $fh, my $b, 1) or die "flip_byte read: $!";
	seek $fh, $off, 0 or die "flip_byte re-seek: $!";
	print $fh chr(ord($b) ^ 0xFF);
	close $fh;
}

# node0 CHECKPOINT with retry: right after a node1 stop/kill the membership
# reconfigures and the CF X grant can transiently fail; poisoning must land
# before the leg proceeds, so retry until it does.
sub poison_authority_from_node0
{
	my ($why) = @_;
	for (1 .. 60)
	{
		my ($rc) = $node0->psql('postgres', 'CHECKPOINT');
		return if defined $rc && $rc == 0;
		usleep(1_000_000);
	}
	die "node0 CHECKPOINT never succeeded ($why)";
}

ok(-f $anchor1, 'L-r0: node1 has a per-node recovery anchor after its first checkpoint');

# ----------
# L-r1: crash-restart with the peer alive.
# ----------
$node1->safe_psql('postgres', 'CHECKPOINT');
# Un-checkpointed WAL tail on node1's own thread (lms is off here, so no
# user-table writes: they would hit the relation-extend gate).  The pre-tail
# LSN is the replay lower bound: kill9 may legitimately lose the very tail
# of the unflushed stream, but recovery must replay PAST the checkpoint into
# the emitted tail -- impossible from a foreign checkpoint pointer.
my $r1_lsn = $node1->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
$node1->safe_psql('postgres',
	"SELECT pg_logical_emit_message(true, 'ra_r1', 'tail-' || g) FROM generate_series(1,50) g");
poison_authority_from_node0('L-r1 poison');

my $r1_off = -s $node1->logfile;
$node1->kill9;
$node1->start;
my $r1_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r1_off);
like($r1_log, qr/using per-node recovery anchor for node 1/,
	'L-r1: node1 adopted its own recovery anchor on crash-restart');
like($r1_log, qr/database system was not properly shut down; automatic recovery in progress/,
	'L-r1: node1 ran its own crash recovery');
unlike($r1_log, qr/could not locate a valid checkpoint record/,
	'L-r1: no foreign-checkpoint PANIC despite the poisoned shared authority');
my ($redo_done) = ($r1_log =~ /redo done at ([0-9A-F]+\/[0-9A-F]+)/);
ok(defined $redo_done && lsn_cmp($redo_done, $r1_lsn) > 0,
	sprintf('L-r1: own tail replayed past the pre-tail LSN (redo done %s > %s)',
		$redo_done // 'none', $r1_lsn));

# ----------
# L-r2: clean-restart order.  After node1's clean stop the shared authority is
# poisoned again (state DB_IN_PRODUCTION, node0's checkpoint), so only the
# anchor can prove "this node shut down cleanly -> no recovery".
# ----------
$node1->stop;
poison_authority_from_node0('L-r2 poison');

my $r2_off = -s $node1->logfile;
$node1->start;
my $r2_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r2_off);
like($r2_log, qr/database system was shut down at/,
	'L-r2a: clean restart is recognized as clean from the anchor');
unlike($r2_log, qr/automatic recovery in progress/,
	'L-r2a: no crash recovery on a clean restart');

$node1->stop('immediate');
poison_authority_from_node0('L-r2b poison');

$r2_off = -s $node1->logfile;
$node1->start;
$r2_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r2_off);
like($r2_log, qr/database system was not properly shut down; automatic recovery in progress/,
	'L-r2b: immediate-stop restart runs crash recovery (anchor state IN_PRODUCTION)');

# ----------
# L-r3: anchor missing (both primary and .bak) -> label-less boot fails
# closed with the 53RB3 face; the node is NOT allowed to guess from the
# shared authority.
# ----------
$node1->stop;
copy($anchor1, "$anchor1.saved") or die "save anchor: $!";
copy($anchor1_bak, "$anchor1_bak.saved") or die "save anchor bak: $!";
unlink $anchor1, $anchor1_bak;

my $r3_off = -s $node1->logfile;
my $r3 = $node1->start(fail_ok => 1);
ok(!$r3, 'L-r3: label-less boot without an anchor refuses to start');
my $r3_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r3_off);
like($r3_log, qr/per-node recovery anchor for node 1 is missing or invalid/,
	'L-r3: the failure is the 53RB3 anchor fail-closed gate');
like($r3_log, qr/Re-provision this node from a base backup/,
	'L-r3: the errhint names the recovery action');

copy("$anchor1.saved", $anchor1) or die "restore anchor: $!";
copy("$anchor1_bak.saved", $anchor1_bak) or die "restore anchor bak: $!";

# ----------
# L-r4: corrupt primary -> the strictly-validated .bak is adopted (WARNING);
# corrupt both -> fail-closed.
# ----------
flip_byte($anchor1, 24);
my $r4_off = -s $node1->logfile;
$node1->start;
my $r4_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r4_off);
like($r4_log, qr/recovery anchor for node 1 was adopted from its backup copy/,
	'L-r4a: corrupt primary falls back to the .bak with a WARNING');
$node1->stop;

# The shutdown checkpoint just rewrote a fresh primary/.bak pair; save it,
# then corrupt both.
copy($anchor1, "$anchor1.saved") or die "save anchor: $!";
copy($anchor1_bak, "$anchor1_bak.saved") or die "save anchor bak: $!";
flip_byte($anchor1, 24);
flip_byte($anchor1_bak, 24);

$r4_off = -s $node1->logfile;
my $r4 = $node1->start(fail_ok => 1);
ok(!$r4, 'L-r4b: boot with primary AND .bak corrupt refuses to start');
$r4_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r4_off);
like($r4_log, qr/per-node recovery anchor for node 1 is missing or invalid/,
	'L-r4b: the failure is the 53RB3 anchor fail-closed gate');

copy("$anchor1.saved", $anchor1) or die "restore anchor: $!";
copy("$anchor1_bak.saved", $anchor1_bak) or die "restore anchor bak: $!";

# ----------
# L-r5 (L408): the injection point forces the StartupXLOG fail-closed branch
# on a node whose on-disk anchor is healthy; the disarmed reboot proves the
# adoption branch through the counter.
# ----------
$node1->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-recovery-anchor-force-failclosed:skip'\n");
my $r5_off = -s $node1->logfile;
my $r5 = $node1->start(fail_ok => 1);
ok(!$r5, 'L-r5: forced fail-closed branch refuses the boot despite a healthy anchor');
my $r5_log = substr(PostgreSQL::Test::Utils::slurp_file($node1->logfile), $r5_off);
like($r5_log, qr/per-node recovery anchor for node 1 is missing or invalid/,
	'L-r5: the injected failure surfaces the same 53RB3 face');

$node1->append_conf('postgresql.conf', "cluster.injection_points = ''\n");
$node1->start;
cmp_ok(cf_counter($node1, 'recovery_anchor_boot_adopt_count'), '>=', 1,
	'L-r5: disarmed reboot adopted the anchor (recovery_anchor_boot_adopt_count moved)');
cmp_ok(cf_counter($node0, 'recovery_anchor_write_count'), '>=', 1,
	'L-r5: node0 checkpoints moved recovery_anchor_write_count');

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
