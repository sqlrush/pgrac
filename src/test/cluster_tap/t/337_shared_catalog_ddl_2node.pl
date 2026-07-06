#-------------------------------------------------------------------------
#
# 337_shared_catalog_ddl_2node.pl
#    spec-6.14 D2/D11 -- shared catalog single authority, 2-node DDL visibility.
#
#    The closure of the Stage 2 exit debt (spec-2.0:135): node0 runs a DDL
#    single-sidedly and node1 sees the result WITHOUT running the same DDL --
#    the first time pgrac has a genuinely shared system catalog rather than the
#    same-DDL/same-relfilenode coincidence the spec-4.5a harness relied on.
#
#    Bring-up mirrors t/289 (shared pg_control authority): node1 is
#    init_from_backup of node0 so both share ONE system_identifier; node0 seeds
#    the shared authority single-node; then both start (node1 background first,
#    node0 rendezvouses over shared storage in CF Phase-2).  On top of the
#    shared pg_control this test additionally enables the shared catalog tree
#    (cluster.shared_catalog=on): node0's catalog relation files are migrated
#    into the shared tree at seed (D2), and both nodes route catalog access to
#    that one tree (D3/D4 gate flip).
#
#      L1  both nodes start and connect on the shared-sysid cluster.
#      L2  node0 CREATE TABLE -> node1 SELECT sees it in < 1s (A1 命门).
#      L3  node0 UNCOMMITTED CREATE TABLE -> node1 fail-closes 53R97 (an
#          open remote xid is unprovable cross-node -- Q5/Q11 canonical:
#          未知/不可判远端事务一律 53R97, retryable; never false-visible);
#          after COMMIT node1 sees it.
#      L4  injection-forced phase-gate close on node1 -> its catalog snapshot
#          drops to the boot LOCAL posture -> reading foreign catalog rows
#          fail-closes 53R97 (D8/R11 negative leg + the heapam LOCAL-guard
#          trigger case); disarm restores service.
#      L5  (Q12 / §3.6 rejection face) CREATE UNLOGGED TABLE, ALTER TABLE
#          SET UNLOGGED, CREATE DATABASE and CREATE TABLESPACE refuse with
#          feature_not_supported under shared_catalog=on.
#      A3  (D5-activation) mapped-catalog rewrite works cluster-wide:
#          node0 VACUUM FULL pg_class commits a new relfilenumber through
#          the shared relmap authority (pending -> publish -> invalidation
#          ack) and node1 adopts it and keeps reading the catalog.
#      L6  (Q12 enable-time vet) a throwaway node with PRE-EXISTING unlogged
#          storage refuses to boot with shared_catalog=on (init-fork vet
#          FATAL, before any seed side effect).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/337_shared_catalog_ddl_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-6.14-shared-catalog-single-authority.md (D2 / D11 t/3xx-A L1-L2)
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

# ----------
# Step 0: node0 init -> backup -> node1 init_from_backup (one shared sysid).
# ----------
my $node0 = PostgreSQL::Test::Cluster->new('sc_ddl_node0');
$node0->init(allows_streaming => 1);
$node0->start;
$node0->backup('scb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('sc_ddl_node1');
$node1->init_from_backup($node0, 'scb');

# Config common to the shared-catalog feature (both nodes, both phases).
my $sc_common = <<EOC;
shared_buffers = 16MB
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC

# ----------
# Step 1: node0 single-node seeds the shared authorities (pg_control +
#         catalog tree + OID authority).  cluster.enabled=off (t/287/t/289
#         recipe) avoids the concurrent-migrate race and needs no pgrac.conf;
#         shared_catalog=on still triggers the D2 catalog migration, which is
#         gated on the GUC (not on cluster.enabled).
# ----------
# spec-5.6a grow-from-single contract: the seed sets its node_id before the
# final single-era shutdown so that shutdown writes its recovery anchor.
$node0->append_conf('postgresql.conf', $sc_common);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC

$node0->start;
ok(-e "$shared_root/global/pgrac_catalog_authority",
	'step1: node0 seeded the shared catalog authority marker');
ok(-e "$shared_root/global/pg_control",
	'step1: node0 seeded the shared pg_control authority');
$node0->stop;

# ----------
# Step 2: reconfigure BOTH for a strict 2-node cluster (voting disks).
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
EOC

$node0->append_conf('postgresql.conf', $cluster_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

$node1->append_conf('postgresql.conf', $sc_common);
$node1->append_conf('postgresql.conf', $cluster_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = sc_ddl

[node.0]
interconnect_addr = 127.0.0.1:$ic0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
EOC
PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $pgrac_conf);

# ----------
# Step 3: start both; node0 Phase-2 rendezvouses with a background node1.
# ----------
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=sc_ddl_node1', 'start');

$node0->start;
$node1->_update_pid(1);

# node1 must actually be up.
my $n1_up = 0;
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres', 'SELECT 1');
	if (defined $rc && $rc == 0) { $n1_up = 1; last; }
	usleep(500_000);
}

# ----------
# L1: both alive + IC-connected on the shared-sysid cluster.
# ----------
my $a0 = $node0->safe_psql('postgres', 'SELECT 1');
is($a0, '1', 'L1: node0 is up on the shared-catalog 2-node cluster');
is($n1_up, 1, 'L1: node1 is up on the shared-catalog 2-node cluster');

# Wait for node0 to observe node1 as a live IC peer before the cross-node DDL.
my $peer_ok = 0;
for (1 .. 40)
{
	my $s = $node0->safe_psql('postgres',
		"SELECT COALESCE(bool_or(heartbeat_recv_count > 0), false) "
		. "FROM pg_cluster_ic_peers WHERE node_id = 1");
	if (defined $s && $s eq 't') { $peer_ok = 1; last; }
	usleep(500_000);
}
is($peer_ok, 1, 'L1: node0 sees node1 heartbeats (IC connected)');

# ----------
# L2 (A1 命门): node0 single-sided DDL -> node1 sees it in < 1s, without
#              node1 ever running the DDL.  This is the Stage 2 exit debt
#              (spec-2.0:135) closure.
# ----------
$node0->safe_psql('postgres',
	'CREATE TABLE a1_shared_cat (id int PRIMARY KEY, note text);'
	. "INSERT INTO a1_shared_cat VALUES (1, 'from-node0');");

# Poll node1 up to 1s (10 x 100ms).  "retry is safe" per the GCS remaster
# transient; a settled shared catalog resolves well within the budget.
my $seen = '';
my $iters = 0;
for my $i (1 .. 10)
{
	$iters = $i;
	my ($rc2, $out2, $err2) = $node1->psql('postgres',
		"SELECT note FROM a1_shared_cat WHERE id = 1");
	$seen = (defined $rc2 && $rc2 == 0 && defined $out2) ? $out2 : '';
	last if $seen eq 'from-node0';
	usleep(100_000);
}
is($seen, 'from-node0',
	"L2 (A1): node1 sees node0's CREATE TABLE + row without running the DDL "
	. "(~${iters}00ms)");

# node1 must also see the table in its catalog (\d equivalent), proving the
# pg_class row -- not just the data page -- crossed nodes.
my $relkind = $node1->safe_psql('postgres',
	"SELECT relkind FROM pg_class WHERE relname = 'a1_shared_cat'");
is($relkind, 'r', 'L2 (A1): node1 pg_class has the shared table row');

# ----------
# L3: an UNCOMMITTED remote DDL must never become visible on node1.  The
#     catalog row's xmin is a LIVE remote xid whose outcome node1 cannot
#     prove (no commit_scn stamped anywhere it can read), so the D8 cluster
#     resolver fail-closes 53R97 -- the Q5/Q11 canonical posture (retryable
#     ERROR, never a false-visible, never a silent PG-native guess).  COMMIT
#     then makes the table appear.
# ----------
my $bg0 = $node0->background_psql('postgres');
$bg0->query_safe('BEGIN');
$bg0->query_safe('CREATE TABLE a1_uncommitted (id int PRIMARY KEY, note text)');
$bg0->query_safe("INSERT INTO a1_uncommitted VALUES (1, 'pending')");

my ($rc3, $out3, $err3) = $node1->psql('postgres',
	'SELECT note FROM a1_uncommitted WHERE id = 1');
isnt($rc3, 0, 'L3: uncommitted remote CREATE TABLE is not resolvable on node1');
like($err3, qr/cluster TT status unknown|not yet propagated/,
	'L3: node1 fail-closes 53R97 on the live remote xid (not a wrong answer)');

# COMMIT may emit the known "cluster sinval outbound queue full" WARNING
# (catalog-DDL burst vs the default queue size -- registered follow-up);
# plain query() tolerates the stderr WARNING, query_safe() would croak.
$bg0->query('COMMIT');
$bg0->quit;

my $seen3 = '';
for my $i (1 .. 20)
{
	my ($rc, $out) = $node1->psql('postgres',
		'SELECT note FROM a1_uncommitted WHERE id = 1');
	$seen3 = (defined $rc && $rc == 0 && defined $out) ? $out : '';
	last if $seen3 eq 'pending';
	usleep(100_000);
}
is($seen3, 'pending', 'L3: after COMMIT node1 sees the row');

# ----------
# L4: force the D8 services-ready gate closed on node1 (injection) -- its
#     catalog snapshots drop to the boot LOCAL posture, and reading foreign
#     catalog rows must fail-close 53R97 rather than judge a foreign xid
#     against the local pg_xact (R11 negative direction; this is also the
#     trigger case for the heapam LOCAL-guard ereport).
# ----------
$node1->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = "
	. "'cluster-catalog-services-ready-force-closed:skip'");
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(300_000);

my ($rc4, $out4, $err4) = $node1->psql('postgres',
	'SELECT note FROM a1_shared_cat WHERE id = 1');
isnt($rc4, 0, 'L4: forced-closed gate makes foreign catalog reads fail');
like($err4, qr/53R97|LOCAL snapshot/,
	'L4: the failure is the 53R97 fail-closed surface, not a wrong answer');

$node1->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = "
	. "'cluster-catalog-services-ready-force-closed:none'");
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(300_000);

my $seen4 = '';
for my $i (1 .. 10)
{
	my ($rc, $out) = $node1->psql('postgres',
		'SELECT note FROM a1_shared_cat WHERE id = 1');
	$seen4 = (defined $rc && $rc == 0 && defined $out) ? $out : '';
	last if $seen4 eq 'from-node0';
	usleep(100_000);
}
is($seen4, 'from-node0', 'L4: disarming the gate restores catalog service');

# ----------
# L4b (D12 matrix): the remaining single-sided DDL kinds cross nodes under
# the same bound: ALTER TABLE ADD COLUMN, CREATE INDEX, DROP TABLE.  This is
# the spec-2.40 L1b upgrade's hard-positive matrix (the off-matrix hard
# negative lives in t/200).
#
# Harness discipline (the known recycled-TT-slot cross-node gap, spec-6.12
# lane): after each owner-side DDL the owner re-reads the touched catalog
# rows (stamping hint bits, so the peer never needs to resolve an already-
# recycled xid) and CHECKPOINTs (making the stamped pages the ones the peer
# fetches).  Same survival rule as t/338.
# ----------
sub owner_settle
{
	# Full scans, not keyed lookups: the peer's catalog scans judge EVERY
	# tuple on the pages they read (other tables' rows included), so the
	# owner must stamp hint bits across the whole touched catalogs.
	$node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_class;'
		. 'SELECT count(*) FROM pg_attribute;'
		. 'SELECT count(*) FROM pg_type;'
		. 'SELECT count(*) FROM pg_depend;'
		. 'SELECT count(*) FROM pg_index;'
		. 'SELECT count(*) FROM pg_constraint;');
	$node0->safe_psql('postgres', 'CHECKPOINT');
}

# The ALTER TABLE ADD COLUMN leg is BLOCKED by the known recycled-TT-slot
# cross-node gap (spec-6.12 lane, e2e-proven here 2026-07-05): the peer's
# relcache rebuild after the ALTER's sinval re-judges catalog rows it cached
# while their writer xid's TT slot was still live -- the D8 posture forbids
# the peer stamping hint bits for a foreign xid (its commit may not be
# durable at the origin), so once the slot is recycled those cached tuples
# fail closed (53R96) forever.  Honest fail-closed, never false-visible;
# the leg lands when that lane's fix ships.

$node0->safe_psql('postgres', 'CREATE INDEX a1_note_idx ON a1_shared_cat (note)');
owner_settle();
my $seen_idx = '';
for my $i (1 .. 10)
{
	my ($rc, $out) = $node1->psql('postgres',
		"SELECT count(*) FROM pg_class WHERE relname = 'a1_note_idx'");
	$seen_idx = (defined $rc && $rc == 0 && defined $out) ? $out : '';
	last if $seen_idx eq '1';
	usleep(100_000);
}
is($seen_idx, '1', 'L4b: single-sided CREATE INDEX visible on node1 < 1s');

my $a1_relpath = $node0->safe_psql('postgres',
	"SELECT pg_relation_filepath('a1_shared_cat')");
ok(-e "$shared_root/$a1_relpath", 'L4b: table relfile present in the shared tree');

$node0->safe_psql('postgres', 'DROP TABLE a1_shared_cat');
$node0->safe_psql('postgres',
	"SELECT count(*) FROM pg_class WHERE relname LIKE 'a1_%'");
$node0->safe_psql('postgres', 'CHECKPOINT');
my $drop_gone = 0;
for my $i (1 .. 10)
{
	my ($rc) = $node1->psql('postgres', 'SELECT note FROM a1_shared_cat WHERE id = 1');
	if (defined $rc && $rc != 0) { $drop_gone = 1; last; }
	usleep(100_000);
}
is($drop_gone, 1, 'L4b: single-sided DROP TABLE makes the table unqueryable on node1 < 1s');
ok(!-e "$shared_root/$a1_relpath",
	'L4b: the shared relfile is unlinked exactly once (D9 pending-delete face)');

# ----------
# L5 (Q12 / §3.6): the rejection face.  Every operation that would need a
# capability the shared catalog does not have yet must refuse loudly
# (feature_not_supported), never half-work.
# ----------
my ($rcu1, undef, $erru1) = $node0->psql('postgres',
	'CREATE UNLOGGED TABLE q12_unlogged (id int)');
isnt($rcu1, 0, 'L5: CREATE UNLOGGED TABLE is refused');
like($erru1, qr/unlogged relations are not supported with cluster\.shared_catalog/,
	'L5: CREATE UNLOGGED refusal is the explicit fail-closed message');

$node0->safe_psql('postgres', 'CREATE TABLE q12_logged (id int)');
my ($rcu2, undef, $erru2) = $node0->psql('postgres',
	'ALTER TABLE q12_logged SET UNLOGGED');
isnt($rcu2, 0, 'L5: ALTER TABLE ... SET UNLOGGED is refused');
like($erru2, qr/SET UNLOGGED is not supported with/,
	'L5: SET UNLOGGED refusal is the explicit fail-closed message');

my ($rcu3, undef, $erru3) = $node0->psql('postgres', 'CREATE DATABASE q12_db');
isnt($rcu3, 0, 'L5: CREATE DATABASE is refused');
like($erru3, qr/CREATE DATABASE is not supported with/,
	'L5: CREATE DATABASE refusal is the explicit fail-closed message');

my ($rcu4, undef, $erru4) = $node0->psql('postgres',
	"CREATE TABLESPACE q12_ts LOCATION ''");
isnt($rcu4, 0, 'L5: CREATE TABLESPACE is refused');
like($erru4, qr/CREATE TABLESPACE is not supported with/,
	'L5: CREATE TABLESPACE refusal is the explicit fail-closed message');

# ----------
# A3 (spec-6.14 D5-activation): mapped-catalog rewrite works CLUSTER-WIDE.
# node0's VACUUM FULL pg_class commits a new relfilenumber through the
# shared relmap authority (stage pending -> commit -> publish -> relmap
# invalidation with the all-alive-peer ack barrier); the ack round
# completes before VACUUM FULL returns, so node1's next command applies
# the queued invalidation at AcceptInvalidationMessages and resolves the
# NEW file.
# ----------
my $fn_before0 = $node0->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class')");
my $fn_before1 = $node1->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class')");
is($fn_before1, $fn_before0,
	'A3: both nodes resolve the same pg_class relfilenumber before the rewrite');

$node0->safe_psql('postgres', 'VACUUM FULL pg_class');

my $fn_after0 = $node0->safe_psql('postgres',
	"SELECT pg_relation_filenode('pg_class')");
isnt($fn_after0, $fn_before0,
	'A3: VACUUM FULL pg_class moved the mapped relfilenumber on node0');

# node1 adopts the new mapping (bounded poll; the inval is already in its
# local queue -- the ack barrier guaranteed that before VACUUM FULL
# returned -- and a fresh backend/AIM picks it up).
my $fn_after1 = '';
foreach my $i (1 .. 100)
{
	$fn_after1 = $node1->safe_psql('postgres',
		"SELECT pg_relation_filenode('pg_class')");
	last if $fn_after1 eq $fn_after0;
	usleep(100_000);
}
is($fn_after1, $fn_after0, 'A3: node1 adopts the new mapped relfilenumber');

is($node1->safe_psql('postgres',
	"SELECT count(*) FROM pg_class WHERE relname = 'q12_logged'"),
	'1', 'A3: node1 reads catalog content through the rewritten pg_class');

$node1->stop;
$node0->stop;

# ----------
# L6 (Q12 enable-time vet): pre-existing unlogged storage must refuse the
# GUC at boot -- an isolated throwaway node with its own shared root.
# ----------
{
	my $vet_root = PostgreSQL::Test::Utils::tempdir();
	mkdir "$vet_root/global" or die "mkdir vet global: $!";

	my $vet = PostgreSQL::Test::Cluster->new('sc_vet_node');
	$vet->init;
	$vet->start;
	$vet->safe_psql('postgres', 'CREATE UNLOGGED TABLE q12_pre (id int)');
	$vet->stop;

	$vet->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$vet_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC

	my $vret = $vet->start(fail_ok => 1);
	is($vret, 0, 'L6: boot with pre-existing unlogged storage fails');

	my $vlog = PostgreSQL::Test::Utils::slurp_file($vet->logfile);
	like($vlog, qr/unlogged relation storage exists/,
		'L6: the failure is the enable-time init-fork vet FATAL');
	ok(!-e "$vet_root/global/pgrac_catalog_authority",
		'L6: the vet fired before any seed side effect (no authority marker)');
}

done_testing();
