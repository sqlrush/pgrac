#-------------------------------------------------------------------------
#
# 339_shared_catalog_faults_2node.pl
#    spec-6.14 D9/D11 -- shared catalog fault legs: crash-mid-DDL recovery.
#
#    The D9 contract under test: a foreign TERMINAL record's side effects
#    (relfile drops, invalidations, stats drops) EXECUTE during k-way cold
#    merged recovery instead of fail-closing the whole merge.  The
#    discriminator is deterministic: every DDL commit record carries
#    invalidation messages, so before D9 a foreign thread tail containing ANY
#    DDL commit made the cold merge FATAL on every start (53RA3, "carries an
#    unsupported side effect") -- the cluster could never come back up.  With
#    D9 both nodes cold-restart, each consuming the OTHER's in-flight DDL
#    commits (invals consumed cold, relfile drops executed idempotently), and
#    serve the full post-crash catalog truth.
#
#    Bring-up mirrors t/337 (shared-sysid seed + strict 2-node voting), plus
#    the spec-4.1 shared per-thread WAL layout so each node can read the
#    other's thread during the merge.  cluster.online_thread_recovery keeps
#    its dev default (off): the spec-4.11 online engine's apply matrix is a
#    separate capability gate; D9's recovery surface is the cold merge.
#
#      L0  both nodes up + IC-connected (t/337 recipe).
#      L1  crash windows on BOTH threads: node1 commits its own CREATE (left
#          in its un-checkpointed tail); node0's tail holds a committed
#          CREATE, a committed DROP (relfile drop + invals + stats) and an
#          UNCOMMITTED CREATE.  node0 dies (SIGKILL); node1 declares it dead
#          and the fail-stop reconfig fires (the death is real); node1 is
#          then immediate-stopped (simulated crash, no shutdown checkpoint).
#      L2  (D9 cold) BOTH nodes cold-restart: each node's merged replay
#          diverts the other thread's DDL commit records -- with the pre-D9
#          predicates either record FATALs the merge and the cluster stays
#          down; with D9 both nodes reach ready.
#      L3  truth matrix on BOTH nodes: the settled table, the dead node's
#          in-window committed CREATE and the survivor's in-tail CREATE are
#          all readable; the dropped table is gone (catalog AND shared
#          relfile, exactly once); the crash-uncommitted CREATE never
#          surfaces a row (8.A).
#      L4  neither node's log carries the pre-D9 "unsupported side effect"
#          fail-closed marker.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/339_shared_catalog_faults_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-6.14-shared-catalog-single-authority.md (D9 / D11 t/3xx-B)
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

# spec-4.1 shared per-thread WAL root: the survivor can only recover a dead
# thread it can READ (thread recovery + HW remaster both derive their window
# from the dead node's thread WAL).  node N's own pg_wal is relocated to
# <root>/thread_<N+1> (initdb -X for node0; a post-backup move+symlink for
# node1, which is init_from_backup and never runs initdb).
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

# ----------
# Step 0: node0 init -> backup -> node1 init_from_backup (one shared sysid).
# ----------
my $node0 = PostgreSQL::Test::Cluster->new('sc_flt_node0');
$node0->init(allows_streaming => 1, extra => [ '-X', "$wal_root/thread_1" ]);
$node0->start;
$node0->backup('scb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('sc_flt_node1');
$node1->init_from_backup($node0, 'scb');

# Relocate node1's WAL into its shared thread dir (thread_2 = node_id 1 + 1):
# move the backup's pg_wal contents there, then symlink pg_wal to it -- the
# same layout initdb -X produces for node0.
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
# Step 1: node0 single-node seeds the shared authorities (t/337 recipe).
# ----------
# spec-5.6a grow-from-single provisioning contract: the seed node sets its
# cluster.node_id before its final single-era shutdown so that shutdown's
# checkpoint writes the per-node recovery anchor its first cluster boot
# needs (label-less multi-node boot without an anchor = 53RB3).
$node0->append_conf('postgresql.conf', $sc_common);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC

$node0->start;
ok(-e "$shared_root/global/pgrac_catalog_authority",
	'step1: node0 seeded the shared catalog authority marker');
$node0->stop;

# ----------
# Step 2: reconfigure BOTH for a strict 2-node cluster with online thread
#         recovery enabled (the D9 online path needs the survivor to replay
#         the dead thread; dev default is off).
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
EOC

$node0->append_conf('postgresql.conf', $cluster_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

$node1->append_conf('postgresql.conf', $sc_common);
$node1->append_conf('postgresql.conf', $cluster_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = sc_flt

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
	'-l', $node1->logfile, '-o', '--cluster-name=sc_flt_node1', 'start');

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
# L0: both alive + IC-connected.
# ----------
is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L0: node0 is up');
is($n1_up, 1, 'L0: node1 is up');

my $peer_ok = 0;
for (1 .. 40)
{
	my $s = $node0->safe_psql('postgres',
		"SELECT COALESCE(bool_or(heartbeat_recv_count > 0), false) "
		. "FROM pg_cluster_ic_peers WHERE node_id = 1");
	if (defined $s && $s eq 't') { $peer_ok = 1; last; }
	usleep(500_000);
}
is($peer_ok, 1, 'L0: node0 sees node1 heartbeats (IC connected)');

# ----------
# L1: crash windows on BOTH threads, then a real death + a simulated one.
#
#   node1 tail (un-checkpointed): CREATE t_survivor (+ row, committed)
#     -> node0's cold merge must divert this invals-bearing foreign commit.
#   node0 settled (pre-checkpoint): t_keep + t_dropme exist and are readable.
#   node0 tail:  CREATE t_crash_committed (+ row, committed) -> invals
#                DROP  t_dropme           (committed)  -> nrels+invals+stats
#                CREATE t_crash_uncommitted (+ row, NO commit) -> 8.A leg
#     -> node1's cold merge must divert all of these.
# ----------
$node1->safe_psql('postgres',
	"CREATE TABLE t_survivor (id int PRIMARY KEY, note text);"
	. "INSERT INTO t_survivor VALUES (1, 'alive');");

$node0->safe_psql('postgres',
	"CREATE TABLE t_keep (id int PRIMARY KEY, note text);"
	. "INSERT INTO t_keep VALUES (1, 'keep');"
	. "CREATE TABLE t_dropme (id int PRIMARY KEY, note text);"
	. "INSERT INTO t_dropme VALUES (1, 'doomed');");

# Shared-tree relfile of t_dropme, captured BEFORE the drop.
my $drop_relpath = $node0->safe_psql('postgres',
	"SELECT pg_relation_filepath('t_dropme')");
ok(-e "$shared_root/$drop_relpath",
	'L1: t_dropme relfile exists in the shared tree before the drop');

$node0->safe_psql('postgres', 'CHECKPOINT');

$node0->safe_psql('postgres',
	"CREATE TABLE t_crash_committed (id int PRIMARY KEY, note text);"
	. "INSERT INTO t_crash_committed VALUES (1, 'window');");
$node0->safe_psql('postgres', 'DROP TABLE t_dropme');

my $bg0 = $node0->background_psql('postgres');
$bg0->query_safe('BEGIN');
$bg0->query_safe('CREATE TABLE t_crash_uncommitted (id int PRIMARY KEY, note text)');
$bg0->query_safe("INSERT INTO t_crash_uncommitted VALUES (1, 'phantom')");

$node0->kill9;

# Reap the orphaned background psql (its server died under it).
eval { $bg0->quit; };

# node1 must declare node0 dead and advance the membership epoch (fail-stop
# reconfig): the death is real, not a staged inject.  Assert from the LOG:
# during the outage node1 may not accept NEW connections at all (a backend's
# catalog bootstrap can hit pages whose GCS master died and stays frozen
# until the dead thread is recovered), so SQL polling is not a reliable
# observation channel here.
my $dead_ok = 0;
for (1 .. 90)
{
	my $log = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
	if ($log =~ /cssd: peer 0 transitioned suspected/) { $dead_ok = 1; last; }
	usleep(500_000);
}
is($dead_ok, 1, 'L1: node1 CSSD declared node0 dead (log evidence)');

# The epoch bump itself is deterministic log evidence (spec-6.14 D9 amend
# F5): the coordinator emits an unconditional line at publish.  The old
# traffic-driven patterns (stale-epoch replies / GRD rebuild messages) only
# appear if some backend happens to generate traffic during the outage --
# with zero load a perfectly healthy reconfig printed nothing.
my $reconfig_ok = 0;
for (1 .. 60)
{
	my $log = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
	if ($log =~ /fail-stop epoch bump \d+ -> [1-9]\d* published .* dead node\(s\) \{0\}/)
	{
		$reconfig_ok = 1;
		last;
	}
	usleep(500_000);
}
is($reconfig_ok, 1, 'L1: node1 fail-stop reconfig advanced the epoch (log evidence)');

# Simulated crash of the survivor too: immediate stop writes NO shutdown
# checkpoint, so node1's tail keeps the t_survivor DDL for node0's merge.
$node1->stop('immediate');

# ----------
# L2 (D9 cold): BOTH nodes cold-restart; each one's merged replay diverts the
# OTHER thread's DDL commit records.  With the pre-D9 predicates either
# record FATALs the merge (53RA3 "unsupported side effect") on every start
# and the cluster stays down; with D9 both nodes reach ready.
# ----------
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=sc_flt_node1', 'start');

$node0->start;
$node1->_update_pid(1);

my $n1_up2 = 0;
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres', 'SELECT 1');
	if (defined $rc && $rc == 0) { $n1_up2 = 1; last; }
	usleep(500_000);
}
is($node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L2 (D9 cold): node0 restarted through the merged replay');
is($n1_up2, 1, 'L2 (D9 cold): node1 restarted through the merged replay');

# ----------
# L3: post-recovery truth matrix on BOTH nodes.  The first catalog reads may
# transiently fail-close while the cluster paths settle; poll with a budget.
# ----------
my %expect = (
	t_keep			  => 'keep',
	t_crash_committed => 'window',
	t_survivor		  => 'alive',
);

for my $pair ([ $node0, 'node0' ], [ $node1, 'node1' ])
{
	my ($node, $name) = @$pair;

	for my $tbl (sort keys %expect)
	{
		my $want = $expect{$tbl};
		my $seen = '';
		for my $i (1 .. 60)
		{
			my ($rc, $out) = $node->psql('postgres',
				"SELECT note FROM $tbl WHERE id = 1");
			$seen = (defined $rc && $rc == 0 && defined $out) ? $out : '';
			last if $seen eq $want;
			usleep(500_000);
		}
		is($seen, $want, "L3: $name serves $tbl after the cold merge");
	}

	my ($rcd, $outd, $errd) = $node->psql('postgres',
		'SELECT note FROM t_dropme WHERE id = 1');
	isnt($rcd, 0, "L3: dropped table is not queryable on $name");

	my ($rcu, $outu, $erru) = $node->psql('postgres',
		'SELECT note FROM t_crash_uncommitted WHERE id = 1');
	isnt($rcu, 0, "L3: crash-uncommitted CREATE never surfaces on $name (8.A)");
}

ok(!-e "$shared_root/$drop_relpath",
	'L3: the shared relfile of the dropped table is unlinked (exactly once)');

# ----------
# L4: the pre-D9 fail-closed marker fired on neither node.
# ----------
my $log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
unlike($log0, qr/carries an unsupported\s+side effect/,
	'L4: no "unsupported side effect" fail-closed in node0 log (cold D9)');
my $log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
unlike($log1, qr/carries an unsupported\s+side effect/,
	'L4: no "unsupported side effect" fail-closed in node1 log (cold D9)');

$node1->stop;
$node0->stop;

done_testing();
