#-------------------------------------------------------------------------
#
# 371_gcs_done_mixed_version_2node.pl
#    GCS-race round-2 RC-F mixed-version leg -- a peer that predates the
#    GCS_BLOCK_DONE completion-proof protocol, simulated with the test-only
#    cluster.ic_suppress_gcs_done_cap GUC on node1 from FIRST boot (its
#    HELLO omits GCS_DONE_V1 and its requesters never send DONE -- exactly
#    the two visible behaviors of a pre-protocol binary).
#
#    Runs at the DEFAULT dedup cap (16384) on purpose: legacy registrations
#    pin the protocol-ceiling lifetime (~1h) and are never reclaimed early,
#    so at the t/366 256 floor cap a cold legacy peer wedges the table by
#    design (calibration 2 availability cost -- fail-closed, not wrong).
#    The mixed-version CONTRACT this file locks is the default-cap shape:
#
#      M1  bring-up: a suppressed node1 forms and serves normally.
#      M2  default-cap precondition (SHOW = 16384) -- makes M6 meaningful.
#      M3  masters pin the capability-less peer's requests at the legacy
#          protocol ceiling (legacy_pin_count grows; no hint trusted).
#      M4  the DONE chain is silent in BOTH directions: the old binary
#          never sends (send-side suppression), and node0 withholds DONE
#          from a peer that cannot parse msg 38 (capability gate) --
#          done_sent / done_marked stay 0 file-wide, zero mismatches.
#      M5  legacy registrations are not hint violations (F5 split).
#      M6  under-cap legacy pressure never fails closed (full delta 0)
#          and the data the legacy peer reads is correct.
#      M7  eviction probe: pinned legacy lifetimes survive a
#          post-registration TTL-GUC shrink (the sweep consumes the value
#          pinned at registration, never re-reads live GUCs; unit dedup
#          u14/u16 cover the arithmetic, this locks the e2e face).
#
#    Bring-up mirrors t/366 (shared catalog single authority, t/337
#    recipe) minus the floor-cap pin.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/371_gcs_done_mixed_version_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-7.2a-gcs-block-dedup-capacity-gc.md (round-2 hardening)
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

# Cluster features require --enable-cluster.
if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'GCS mixed-version leg requires --enable-cluster';
}

# ============================================================
# Helpers (t/366 lineage).
# ============================================================

sub gcs_value
{
	my ($node, $key) = @_;

	# Retry: the counter view read itself can transiently fail-close during the
	# node-rejoin window (a catalog block master still recovering).
	for (1 .. 40)
	{
		my ($rc, $out) = $node->psql('postgres',
			qq{SELECT value FROM pg_cluster_state
			   WHERE category='gcs' AND key='$key'});
		return $out if defined $rc && $rc == 0;
		usleep(300_000);
	}
	return '';
}

sub gcs_int
{
	my ($node, $key) = @_;

	my $v = gcs_value($node, $key);
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Sum a dedup counter over both nodes: a cross-node request installs its dedup
# entry on whichever node masters the block, so the master side is not fixed.
sub gcs_int_both
{
	my ($n0, $n1, $key) = @_;
	return gcs_int($n0, $key) + gcs_int($n1, $key);
}

# Retry a read until it succeeds -- first-contact liveness only; every
# semantic assert below stays strict.
sub psql_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 120;
	for (1 .. $tries)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql);
		return (1, $out) if defined $rc && $rc == 0;
		usleep(500_000);
	}
	return (0, undef);
}

# ============================================================
# Bring-up: shared catalog, shared pg_control, one system_identifier.
# ============================================================

my $shared_root = PostgreSQL::Test::Utils::tempdir();
mkdir "$shared_root/global" or die "mkdir shared global: $!";

my $wal_root = PostgreSQL::Test::Utils::tempdir();

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
my $data_port0 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port1 = PostgreSQL::Test::Cluster::get_free_port();

# Step 0: node0 init -> backup -> node1 init_from_backup (one shared sysid).
my $node0 = PostgreSQL::Test::Cluster->new('gcsmv_node0');
$node0->init(allows_streaming => 1, extra => [ '-X', "$wal_root/thread_1" ]);
$node0->start;
$node0->backup('scb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('gcsmv_node1');
$node1->init_from_backup($node0, 'scb');

# Relocate node1's backup-copied WAL into its shared thread dir (t/337 recipe).
{
	my $pgwal = $node1->data_dir . '/pg_wal';
	my $wal2 = "$wal_root/thread_2";
	mkdir $wal2 or die "mkdir $wal2: $!";
	opendir(my $dh, $pgwal) or die "opendir $pgwal: $!";
	for my $e (readdir $dh)
	{
		next if $e eq '.' || $e eq '..';
		rename("$pgwal/$e", "$wal2/$e") or die "rename $pgwal/$e: $!";
	}
	closedir $dh;
	rmdir $pgwal or die "rmdir $pgwal: $!";
	symlink($wal2, $pgwal) or die "symlink $pgwal -> $wal2: $!";
}

my $sc_common = <<EOC;
shared_buffers = 16MB
autovacuum = off
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC

# Step 1: node0 single-node seeds the shared authorities.
$node0->append_conf('postgresql.conf', $sc_common);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC

$node0->start;
ok(-e "$shared_root/global/pgrac_catalog_authority",
	'M1 bring-up: node0 seeded the shared catalog authority marker');
$node0->stop;

# Step 2: reconfigure BOTH for a strict 2-node cluster (DEFAULT dedup cap).
my $cluster_conf = <<EOC;
cluster.enabled = on
cluster.online_join = on
cluster.xid_striping = on
cluster.lms_enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
cluster.wal_threads_dir = '$wal_root'
EOC

$node0->append_conf('postgresql.conf', $cluster_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

$node1->append_conf('postgresql.conf', $sc_common);
$node1->append_conf('postgresql.conf', $cluster_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
# The simulated pre-GCS_DONE binary: suppressed from FIRST cluster boot, so
# every connection it ever dials carries the capability-less HELLO (a real
# old binary is old from boot -- no restart, no cold-cache artifacts).
$node1->append_conf('postgresql.conf', "cluster.ic_suppress_gcs_done_cap = on\n");
# spec-7.3 merge: pin the LMS pool to one worker (t/366 note -- the rig
# reserves ONE data port per node; the default 2-worker pool would
# cross-wire consecutive free ports).
$node0->append_conf('postgresql.conf', "cluster.lms_workers = 1\n");
$node1->append_conf('postgresql.conf', "cluster.lms_workers = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = gcs_mv

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data_port0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data_port1
EOC
PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $pgrac_conf);

# Step 3: start both; node0 Phase-2 rendezvouses with a background node1.
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=gcs_mv_node1', 'start');

$node0->start;
$node1->_update_pid(1);

my ($n1_up) = psql_retry($node1, 'SELECT 1', 120);
ok($n1_up, 'M1 bring-up: suppressed node1 answers on the shared-sysid cluster');

my ($n0_up) = psql_retry($node0, 'SELECT 1', 120);
ok($n0_up, 'M1 bring-up: node0 answers on the shared-sysid cluster');

# M2: default-cap precondition -- the M6 "never fails closed" assert below
# is only meaningful when legacy pins cannot wedge the table.
is($node0->safe_psql('postgres', 'SHOW cluster.gcs_block_dedup_max_entries'),
	'16384', 'M2 rig runs at the default dedup cap');

# ============================================================
# M3-M6: legacy-peer traffic against a fresh multi-block relation.
# ============================================================
my $legacy_pre = gcs_int_both($node0, $node1, 'dedup_legacy_pin_count');
my $viol_pre = gcs_int_both($node0, $node1, 'dedup_hint_violation_count');
my $full_pre = gcs_int_both($node0, $node1, 'dedup_full_count');

my ($mv_ok) = psql_retry($node0, q{
	CREATE TABLE gcs_mv_probe (id int PRIMARY KEY, pad text);
	INSERT INTO gcs_mv_probe
	  SELECT g, repeat('m', 400) FROM generate_series(1, 3000) g;
}, 120);
ok($mv_ok, 'M3 node0 created + filled gcs_mv_probe (3000 rows, never cached on node1)');
my ($mv_seen, $mv_out) = psql_retry($node1,
	'SELECT count(*) FROM gcs_mv_probe', 120);
ok($mv_seen && defined($mv_out) && $mv_out eq '3000',
	'M6 legacy-peer read is correct (3000 rows over cross-node ships)');

my $legacy_post = gcs_int_both($node0, $node1, 'dedup_legacy_pin_count');
cmp_ok($legacy_post, '>', $legacy_pre,
	"M3 masters pinned the capability-less peer's requests at the legacy "
	. "protocol ceiling (legacy_pin $legacy_pre -> $legacy_post)");

# M4: the DONE chain is silent file-wide -- the suppressed side never sends,
# and node0 withholds DONE from a peer that never advertised the capability.
is(gcs_int_both($node0, $node1, 'done_sent_count'), 0,
	'M4 no DONE was ever sent (send-side suppression + capability gate)');
is(gcs_int_both($node0, $node1, 'dedup_done_marked_count'), 0,
	'M4 no DONE was ever consumed');
is(gcs_int_both($node0, $node1, 'dedup_done_mismatch_count'), 0,
	'M4 zero DONE identity mismatches');

is(gcs_int_both($node0, $node1, 'dedup_hint_violation_count') - $viol_pre, 0,
	'M5 legacy registrations are not hint violations (F5 capability split)');
is(gcs_int_both($node0, $node1, 'dedup_full_count') - $full_pre, 0,
	'M6 under-cap legacy pressure never fails closed (full delta 0)');

# ============================================================
# M7 eviction probe: pinned lifetime beats a post-registration GUC shrink.
# ============================================================
# First let short-lived hint-pinned entries (node0's own requests) age out
# so their legitimate evictions cannot pollute the window: wait for
# evict_count to go quiet.
my $ev_prev = gcs_int_both($node0, $node1, 'dedup_evict_count');
for (1 .. 30)
{
	usleep(3_000_000);
	my $ev_now = gcs_int_both($node0, $node1, 'dedup_evict_count');
	last if $ev_now == $ev_prev;
	$ev_prev = $ev_now;
}

# Shrink the TTL inputs on the master: a sweep that (wrongly) re-derived the
# window from live GUCs would age the fresh legacy entries out within a
# second; the pinned protocol-ceiling lifetime must hold instead.
$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.gcs_reply_timeout_ms = 100');
$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.gcs_block_retransmit_max_retries = 0');
$node0->reload;
my $ev_probe_pre = gcs_int_both($node0, $node1, 'dedup_evict_count');
usleep(6_000_000);    # >= 6 LMON sweep ticks at the shrunken window
my $ev_probe_post = gcs_int_both($node0, $node1, 'dedup_evict_count');
is($ev_probe_post - $ev_probe_pre, 0,
	'M7 pinned legacy lifetimes survive a post-registration GUC shrink '
	. '(sweep consumes the pinned value, never re-reads live GUCs)');

$node0->safe_psql('postgres',
	'ALTER SYSTEM RESET cluster.gcs_reply_timeout_ms');
$node0->safe_psql('postgres',
	'ALTER SYSTEM RESET cluster.gcs_block_retransmit_max_retries');
$node0->reload;

# Writer-side integrity (t/366 L4 discipline: assets are read writer-side).
my ($wok, $wout) = psql_retry($node0, 'SELECT count(*) FROM gcs_mv_probe', 30);
ok($wok && defined($wout) && $wout eq '3000',
	'M6 writer-side data intact after the legacy churn');

$node0->stop;
$node1->stop;

done_testing();
