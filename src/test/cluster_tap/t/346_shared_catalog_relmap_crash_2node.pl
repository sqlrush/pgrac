#-------------------------------------------------------------------------
#
# 346_shared_catalog_relmap_crash_2node.pl
#    spec-6.14 D5-activation -- relmap authority crash arbitration, 2 nodes.
#
#    A mapped-catalog rewrite (VACUUM FULL pg_class) commits its new map
#    through the shared relmap authority in two halves: stage PENDING
#    pre-commit, PUBLISH post-commit (INV-14-8).  A writer that dies between
#    the halves leaves a durable pending image that the next sole merger
#    (merge-claim holder) arbitrates by the owner xid's terminal status.
#    The two crash windows are unreachable by natural SQL timing, so two
#    designed-PANIC injection points drive them:
#
#      R1  cluster-relmap-crash-after-stage: die with a staged,
#          UNCOMMITTED pending -> crash arbitration DISCARDS it; the
#          mapped relfilenumber is unchanged on both nodes.
#      R2  cluster-relmap-crash-before-publish: die with a committed,
#          UNPUBLISHED pending -> crash arbitration PUBLISHES it (the
#          deferred post-commit half) and broadcasts the relmap
#          invalidation; both nodes adopt the new relfilenumber.
#      R3  the write path is healthy after both crashes: a plain
#          VACUUM FULL pg_class round-trips cluster-wide.
#      R4  (B-L10) OID lease crash non-reissue: a kill mid-lease loses the
#          lease's unconsumed tail but never reissues -- every OID handed
#          out after the cold restart is strictly above everything issued
#          before it, and the durable authority high-water only advances.
#      R5  (B-L3) authority corruption is fail-closed, not re-seeded: with
#          BOTH the OID authority and its .bak corrupted, boot dies FATAL
#          ("present but corrupt") instead of silently re-seeding from the
#          checkpointed shared pg_control high-water (which can lag leased
#          ranges and reissue them).
#
#    Bring-up mirrors t/339 (shared catalog + per-thread WAL layout so the
#    shared-regime merge claim engages on crash recovery).  Single-writer
#    discipline throughout (only node0 runs DDL): the arbitration facts
#    under test are xid-face independent (own-node CLOG / per-origin
#    remote-xact store).
#
#    KNOWN-BLOCKED (feature #119 recycled-TT-slot, spec-6.12i lane): after
#    the crash rounds node1 cannot serve catalog reads at all -- the crashed
#    transactions' row versions carry node0 xids whose TT slots are gone,
#    the owner cannot hint-stamp them (D8: stamping needs TT-confirmed
#    terminal), so every node1 backend bootstrap fail-closes 53R97 (honest,
#    same shape as the t/339 serving legs).  node1-side POST-CRASH
#    behavioral map adoption is therefore asserted against the shared
#    relmap AUTHORITY's committed image directly (the substrate truth every
#    reload adopts, INV-14-7); the peer's live behavioral adoption is
#    t/337 A3's green fact.  Behavioral post-crash legs land after the
#    6.12/6.15 lane merges.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/346_shared_catalog_relmap_crash_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2; B-L3/L7/L7b/L10)
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
	plan skip_all => 'shared catalog requires --enable-cluster';
}

# ---- shared storage root + voting disks both postmasters can reach ----
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

# ----------
# Step 0: node0 init -> backup -> node1 init_from_backup (one shared sysid),
#         per-thread WAL layout (t/339 recipe).
# ----------
my $node0 = PostgreSQL::Test::Cluster->new('sc_rmc_node0');
$node0->init(allows_streaming => 1, extra => [ '-X', "$wal_root/thread_1" ]);
$node0->start;
$node0->backup('scb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('sc_rmc_node1');
$node1->init_from_backup($node0, 'scb');

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

# ----------
# Step 1: node0 single-node seeds the shared authorities (t/337 recipe;
#         node_id set before the final single-era shutdown = 5.6a anchor).
# ----------
$node0->append_conf('postgresql.conf', $sc_common);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC

$node0->start;
ok(-e "$shared_root/global/pgrac_catalog_authority",
	'step1: node0 seeded the shared catalog authority marker');
ok(-e "$shared_root/global/pgrac_relmap_authority",
	'step1: node0 seeded the shared relmap authority');
$node0->stop;

# ----------
# Step 2: strict 2-node cluster config.
# ----------
my $cluster_conf = <<EOC;
cluster.enabled = on
cluster.lms_enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
cluster.wal_threads_dir = '$wal_root'
cluster.recovery_stale_active_ms = 2000
EOC

$node0->append_conf('postgresql.conf', $cluster_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

$node1->append_conf('postgresql.conf', $sc_common);
$node1->append_conf('postgresql.conf', $cluster_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = sc_rmc

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data_port0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data_port1
EOC
PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $pgrac_conf);

# ----------
# Step 3: start both; node0 Phase-2 rendezvouses with a background node1.
# ----------
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=sc_rmc_node1', 'start');

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
# R0: both alive; one warm cross-node catalog table; baseline relfilenumber.
# ----------
is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'R0: node0 is up');
is($n1_up, 1, 'R0: node1 is up');

$node0->safe_psql('postgres',
	'CREATE TABLE rmc_marker (id int); INSERT INTO rmc_marker VALUES (1)');
my $seen = '';
for (1 .. 100)
{
	(undef, $seen) = $node1->psql('postgres', 'SELECT count(*) FROM rmc_marker');
	last if defined $seen && $seen eq '1';
	usleep(100_000);
}
is($seen, '1', 'R0: node1 sees the marker table (catalog live cross-node)');

my $fn_base = $node0->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class')");
is($node1->safe_psql('postgres', "SELECT pg_relation_filenode('pg_class')"),
	$fn_base, 'R0: both nodes resolve the same pg_class relfilenumber');

# ---- helpers ----------------------------------------------------------

# Owner-settle discipline (t/337 D12 shape): the owner full-scans the
# touched catalogs (stamping hint bits from its own pg_xact -- terminal-
# confirmed for its own xids, including the crashed transactions' aborted
# row versions) and CHECKPOINTs, so post-recovery peer reads never depend
# on fresh foreign-xid resolution (keeps this file 6.15-independent).
sub owner_settle
{
	# Tolerant: right after a cold restart node0 can still be write-fenced
	# (the fail-stop reconfig declared it dead; the fence lifts once
	# membership settles), so the CHECKPOINT half may transiently fail --
	# retry within a budget, and die honestly if the fence never lifts.
	for my $t (1 .. 60)
	{
		my ($rc1) = $node0->psql('postgres',
			'SELECT count(*) FROM pg_class;'
			. 'SELECT count(*) FROM pg_attribute;'
			. 'SELECT count(*) FROM pg_type;'
			. 'SELECT count(*) FROM pg_depend;'
			. 'SELECT count(*) FROM pg_index;'
			. 'SELECT count(*) FROM pg_constraint;');
		if (defined $rc1 && $rc1 == 0)
		{
			my ($rc2) = $node0->psql('postgres', 'CHECKPOINT');
			return if defined $rc2 && $rc2 == 0;
		}
		usleep(500_000);
	}
	die 'owner_settle: node0 never became writable (write fence never lifted?)';
}

owner_settle();

# One designed-crash round trip: node0 is already dead from the injection
# PANIC (postmaster exits; TAP default restart_after_crash=off).  node1
# declares it dead, then crashes too (immediate stop keeps its tail), and
# both cold-restart -- the first claim holder's merged recovery runs the
# relmap arbitration (the second finds nothing pending; idempotent).
sub cold_restart_both
{
	my ($why) = @_;

	# node0's postmaster must be gone before node1 is stopped.
	for (1 .. 120)
	{
		last unless -e $node0->data_dir . '/postmaster.pid';
		usleep(500_000);
	}
	$node0->_update_pid(0);		# it died on its own; drop the stale pid

	# node1 declares node0 dead (real death, fail-stop reconfig).
	my $dead_ok = 0;
	for (1 .. 90)
	{
		my $log = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
		if ($log =~ /cssd: peer 0 transitioned suspected/) { $dead_ok = 1; last; }
		usleep(500_000);
	}
	diag("$why: node1 never declared node0 dead") unless $dead_ok;

	# Simulated crash of the survivor: no shutdown checkpoint.
	$node1->stop('immediate');

	# Let BOTH wal-state slots go stale (cluster.recovery_stale_active_ms
	# = 2000 above): the first claim winner's recovery census then
	# classifies the peer thread CRASHED_CANDIDATE -- deterministic
	# engage + k-way merge of both threads, whichever node wins the
	# claim.  A fresh slot would classify ALIVE, skip the merge window,
	# and fail-close on the loser's own catalog tail (the pre-4.11
	# survivor-online posture).
	sleep 5;

	# Cold restart (boot recipe): node1 background first, node0 rendezvous.
	PostgreSQL::Test::Utils::system_log(
		'pg_ctl', '-W', '-D', $node1->data_dir,
		'-l', $node1->logfile, '-o', '--cluster-name=sc_rmc_node1', 'start');
	$node0->start;
	$node1->_update_pid(1);

	for (1 .. 240)
	{
		my ($rc) = $node0->psql('postgres', 'SELECT count(*) FROM pg_class');
		if (defined $rc && $rc == 0)
		{
			# The crashed transactions left unhinted (aborted/committed)
			# row versions on shared catalog pages; settle them from the
			# owner before the peer is probed (see owner_settle).
			owner_settle();
			return 1;
		}
		usleep(500_000);
	}
	diag("node0 did not come back: $why");
	return 0;
}

# authority_filenode -- the mapped relfilenumber the shared relmap
# AUTHORITY's committed image holds for a per-db mapped rel (db oid 5 =
# postgres).  This is the exact image every reader reload adopts
# (INV-14-7), asserted directly because node1 cannot serve catalog SQL
# after the crash rounds (#119, see header).  The committed slot is the
# first RelMapFile image in the file (magic 0x592717); the pending slot,
# when present, sits after it and is never matched first.
sub authority_filenode
{
	my ($oid) = @_;
	my $path = "$shared_root/base/5/pgrac_relmap_authority";

	open my $fh, '<:raw', $path or return '';
	local $/;
	my $img = <$fh>;
	close $fh;

	my $off = index($img, pack('V', 0x592717));
	return '' if $off < 0;
	my $num = unpack('V', substr($img, $off + 4, 4));
	return '' if !defined $num || $num > 64;
	for my $i (0 .. $num - 1)
	{
		my ($moid, $mfile) = unpack('VV', substr($img, $off + 8 + 8 * $i, 8));
		return $mfile if $moid == $oid;
	}
	return '';
}

# The arbitration runs on whichever node holds the merge claim first; its
# LOG line lands in that node's file.
sub both_logs
{
	return PostgreSQL::Test::Utils::slurp_file($node0->logfile)
		. PostgreSQL::Test::Utils::slurp_file($node1->logfile);
}

# ----------
# R1 (discard shape): die with a staged, UNCOMMITTED pending.
# ----------
$node0->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-relmap-crash-after-stage:skip'");
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

my ($r1rc) = $node0->psql('postgres', 'VACUUM FULL pg_class');
isnt($r1rc, 0, 'R1: VACUUM FULL dies at the designed after-stage PANIC');

ok(cold_restart_both('R1'), 'R1: cluster cold-restarted after the crash');

# Disarm before anything else writes relmap.  NB: an EMPTY list (ALTER
# SYSTEM RESET) does NOT disarm a skip-armed point -- disarming needs an
# explicit ':none' (spec-0.27 lever semantics).
$node0->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-relmap-crash-after-stage:none'");
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

my $logs = both_logs();
like($logs, qr/cluster-relmap-crash-after-stage injection/,
	'R1: the designed PANIC fired');
like($logs, qr/cluster relmap arbitration discarded pending generation/,
	'R1: crash arbitration DISCARDED the uncommitted pending');

is($node0->safe_psql('postgres', "SELECT pg_relation_filenode('pg_class')"),
	$fn_base, 'R1: node0 still resolves the pre-crash relfilenumber');
is(authority_filenode(1259), $fn_base,
	'R1: the authority committed image still maps pg_class to the pre-crash file');

# ----------
# R2 (publish shape): die with a committed, UNPUBLISHED pending.
# ----------
$node0->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-relmap-crash-before-publish:skip'");
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

my ($r2rc) = $node0->psql('postgres', 'VACUUM FULL pg_class');
isnt($r2rc, 0, 'R2: VACUUM FULL dies at the designed before-publish PANIC');

ok(cold_restart_both('R2'), 'R2: cluster cold-restarted after the crash');

$node0->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-relmap-crash-before-publish:none'");
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

$logs = both_logs();
like($logs, qr/cluster-relmap-crash-before-publish injection/,
	'R2: the designed PANIC fired');
like($logs, qr/cluster relmap arbitration published pending generation/,
	'R2: crash arbitration PUBLISHED the committed pending');

my $fn_r2 = $node0->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class')");
isnt($fn_r2, $fn_base, 'R2: node0 resolves the rewritten relfilenumber');
is(authority_filenode(1259), $fn_r2,
	'R2: the authority committed image maps pg_class to the arbitration-published file');

# ----------
# R3: the write path is healthy after both crash shapes.
# ----------
$node0->safe_psql('postgres', 'VACUUM FULL pg_class');
my $fn_r3 = $node0->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class')");
isnt($fn_r3, $fn_r2, 'R3: a plain mapped rewrite works after the crashes');
is(authority_filenode(1259), $fn_r3,
	'R3: the authority committed image maps pg_class to the plain-rewrite file');

# oid_authority_hw -- the durable OID authority high-water (header field
# next_oid), read straight from the shared file like authority_filenode.
sub oid_authority_hw
{
	my $path = "$shared_root/global/pgrac_oid_authority";

	open my $fh, '<:raw', $path or return '';
	local $/;
	my $img = <$fh>;
	close $fh;

	my ($magic, $version, $next) = unpack('VVV', $img);
	return '' if !defined $magic || $magic != 0x0140D617;
	return $next;
}

# ----------
# R4 (B-L10): OID lease crash non-reissue.  The running lease carved
# [hw, hw+lease) and durably bumped the authority to hw+lease BEFORE any
# OID from the block was consumed, so a crash loses the unconsumed tail of
# the lease but can never reissue: the cold restart re-leases at the
# durable high-water, strictly above every pre-crash OID.
# ----------
$node0->safe_psql('postgres',
	'DO $$ BEGIN FOR i IN 1..8 LOOP '
	. "EXECUTE format('CREATE TABLE oid_ls_a_%s (id int)', i); "
	. 'END LOOP; END $$;');
my $max_a = $node0->safe_psql('postgres',
	"SELECT max(oid) FROM pg_class WHERE relname LIKE 'oid_ls_a_%'");
my $hw_a = oid_authority_hw();
cmp_ok($hw_a, '>', $max_a,
	'R4: the durable high-water sits above every issued OID');
owner_settle();

# Crash mid-lease (no shutdown checkpoint on either node).
$node0->stop('immediate');
ok(cold_restart_both('R4'), 'R4: cluster cold-restarted after the mid-lease crash');

$node0->safe_psql('postgres',
	'DO $$ BEGIN FOR i IN 1..8 LOOP '
	. "EXECUTE format('CREATE TABLE oid_ls_b_%s (id int)', i); "
	. 'END LOOP; END $$;');
my $min_b = $node0->safe_psql('postgres',
	"SELECT min(oid) FROM pg_class WHERE relname LIKE 'oid_ls_b_%'");
my $hw_b = oid_authority_hw();
cmp_ok($min_b, '>', $max_a,
	'R4: every post-crash OID is above every pre-crash OID (no reissue)');
cmp_ok($hw_b, '>', $hw_a, 'R4: the authority high-water only advanced');

# ----------
# R5 (B-L3): a present-but-corrupt OID authority is FATAL at boot, never
# silently re-seeded (the checkpointed shared pg_control high-water can lag
# ranges already leased out).  Corrupt BOTH the primary and the .bak, then
# prove the boot refuses AND left the corpses alone.
# ----------
$node1->stop;
$node0->stop;

for my $f ('pgrac_oid_authority', 'pgrac_oid_authority.bak')
{
	open my $fh, '>:raw', "$shared_root/global/$f"
		or die "corrupt $f: $!";
	print $fh "\0" x 16;		# short + bad magic: INVALID on every axis
	close $fh;
}

my $r5ret = $node0->start(fail_ok => 1);
is($r5ret, 0, 'R5: boot with a doubly-corrupt OID authority fails');

my $r5log = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
like($r5log, qr/shared OID authority is present but corrupt/,
	'R5: the failure is the designed fail-closed FATAL, not a re-seed');
is(-s "$shared_root/global/pgrac_oid_authority", 16,
	'R5: the corrupt authority was not rewritten (no silent re-seed)');

done_testing();
