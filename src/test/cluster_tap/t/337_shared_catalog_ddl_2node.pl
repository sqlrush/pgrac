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
EOC

# ----------
# Step 1: node0 single-node seeds the shared authorities (pg_control +
#         catalog tree + OID authority).  cluster.enabled=off (t/287/t/289
#         recipe) avoids the concurrent-migrate race and needs no pgrac.conf;
#         shared_catalog=on still triggers the D2 catalog migration, which is
#         gated on the GUC (not on cluster.enabled).
# ----------
$node0->append_conf('postgresql.conf', $sc_common);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
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
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
cluster.gcs_block_local_cache = off
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
	($seen) = $node1->psql('postgres',
		"SELECT note FROM a1_shared_cat WHERE id = 1");
	last if defined $seen && $seen eq 'from-node0';
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

$node1->stop;
$node0->stop;

done_testing();
