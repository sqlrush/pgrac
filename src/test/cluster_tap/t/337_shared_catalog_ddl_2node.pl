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
#      L4c (A-L6) sequence DDL + nextval on both nodes: one line of values.
#      L4d (A-L7 serialized; runs in the END-ZONE after L4h) DDL originated
#          by BOTH nodes commits into disjoint OID leases, proven from the
#          durable authority file + node1 keyed self-reads.  Reverse
#          consumption (node0 judging node1's rows) and the concurrent
#          1000-DDL stress form are KNOWN-BLOCKED on feature #119 /
#          spec-6.15 xid striping (see the end-zone comment).
#      L4e (A-L10) temp isolation: node-qualified temp namespaces, the
#          namespace row crosses, and unqualified name resolution never
#          reaches a foreign temp.  KNOWN-BLOCKED on 6.15: legs that make
#          node1 JUDGE node0's temp-session rows (other-temp classify,
#          explicit-access refusal) and the same-name coexistence arm
#          (node1-originated temp) -- see comments in place.
#      L4f (A-L11) KNOWN-BLOCKED: autovacuum orphan-reaper e2e needs a full
#          catalog pass on node1 -- 6.15 lane (see comment in place).
#      A3  (D5-activation) mapped-catalog rewrite works cluster-wide:
#          node0 VACUUM FULL pg_class commits a new relfilenumber through
#          the shared relmap authority (pending -> publish -> invalidation
#          ack) and node1 adopts it and keeps reading the catalog.
#      L4g (B-L2/A4) KNOWN-BLOCKED: post-restart serving bricks pre-6.15
#          (see comment in place); boot-level zero catchup = t/003 E leg.
#      L4h (B-L5) dropped sinval ack = WARN + convergence, never stale.
#      L6  (Q12 enable-time vet) a throwaway node with PRE-EXISTING unlogged
#          storage refuses to boot with shared_catalog=on (init-fork vet
#          FATAL, before any seed side effect).
#      L7  (B-L4 off-flip vet) shared_catalog=off boot against a seeded
#          shared tree is refused (stale-catalog protection).
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
my $node0 = PostgreSQL::Test::Cluster->new('sc_ddl_node0');
$node0->init(allows_streaming => 1, extra => [ '-X', "$wal_root/thread_1" ]);
$node0->start;
$node0->backup('scb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('sc_ddl_node1');
$node1->init_from_backup($node0, 'scb');

# Relocate node1's backup-copied WAL into its shared thread dir.  shared_catalog
# multi-node formation is fail-fast without cluster.wal_threads_dir, and the WAL
# thread validator requires pg_wal to resolve to the configured thread_N dir.
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
cluster.wal_threads_dir = '$wal_root'
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
# catalog_stamp_sql -- full scans over EVERY pg_catalog heap, not keyed
# lookups and not a hand-kept list: the peer's catalog scans (and a
# restarted peer's backend bootstrap, which re-reads storage cold) judge
# every tuple they meet, so a single unstamped row in ANY catalog a leg
# touched (pg_sequence, pg_shdepend, ... -- holes a fixed list develops)
# strands the peer at 53R96/53R97 once the writer xid's TT slot recycles.
my $catalog_stamp_sql = q{
DO $$
DECLARE r regclass;
BEGIN
	FOR r IN SELECT c.oid::regclass FROM pg_class c
			 JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = 'pg_catalog' AND c.relkind = 'r'
	LOOP
		EXECUTE format('SELECT count(*) FROM %s', r);
	END LOOP;
END $$;
};

sub owner_settle
{
	$node0->safe_psql('postgres', $catalog_stamp_sql);

	# NB: do NOT reach for plain VACUUM here to force peer refetches: a
	# shared catalog accumulates MIXED-ORIGIN xmins once both nodes have
	# originated DDL, and vacuum's single relfrozenxid cannot govern two
	# unstriped xid spaces (proven live 2026-07-06: "found xmin 791 from
	# before relfrozenxid 792" fail-stop).  Catalog vacuum is spec-6.15
	# territory (AD-012 exception 10); the harness discipline for the peer's
	# stale-cache window is a peer restart instead (see L4d).
	$node0->safe_psql('postgres', 'CHECKPOINT');
}


# psql_ok_retry / psql_val_retry -- run $sql on $node until it succeeds;
# cluster cross-node transients (53R96/53R97) are designed retryable
# ("retry the transaction with a fresh snapshot").
sub psql_ok_retry
{
	my ($node, $sql, $tries) = @_;

	$tries //= 20;
	for my $t (1 .. $tries)
	{
		my ($rc) = $node->psql('postgres', $sql);
		return 1 if defined $rc && $rc == 0;
		usleep(500_000);
	}
	return 0;
}

sub psql_val_retry
{
	my ($node, $sql, $tries) = @_;

	$tries //= 20;
	for my $t (1 .. $tries)
	{
		my ($rc, $out) = $node->psql('postgres', $sql);
		return $out if defined $rc && $rc == 0 && defined $out;
		usleep(500_000);
	}
	return '';
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

# ... and node1's planner actually USES it (plancache re-plan face): the
# index arrived through the shared catalog, not through any local DDL.
my $plan_idx = $node1->safe_psql('postgres',
	'SET enable_seqscan = off; '
	. "EXPLAIN (COSTS OFF) SELECT note FROM a1_shared_cat WHERE note = 'x'");
like($plan_idx, qr/a1_note_idx/,
	'L4b: node1 plans with the single-sided index');

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
# L4c (A-L6): sequence DDL + nextval on BOTH nodes.  The sequence's catalog
# rows cross like any DDL; its data page crosses through the SQ enqueue +
# CF page transfer (spec-5.4), so the two nodes draw from ONE line of
# values -- strictly advancing, never colliding.
# ----------
$node0->safe_psql('postgres', 'CREATE SEQUENCE sc_seq');
my $seq_v0 = $node0->safe_psql('postgres', "SELECT nextval('sc_seq')");
owner_settle();
my $seq_v1 = '';
for my $i (1 .. 20)
{
	my ($rc, $out) = $node1->psql('postgres', "SELECT nextval('sc_seq')");
	if (defined $rc && $rc == 0 && defined $out && $out ne '') { $seq_v1 = $out; last; }
	usleep(500_000);
}
cmp_ok($seq_v1, '>', $seq_v0,
	'L4c: node1 draws from the single-sided sequence, after node0');
my $seq_v0b = $node0->safe_psql('postgres', "SELECT nextval('sc_seq')");
cmp_ok($seq_v0b, '>', $seq_v1,
	'L4c: node0 draws again, after node1 (one line of values, no collision)');

# (L4d runs at the very END of the 2-node choreography -- see the section
# after L4h: node1-originated catalog rows are cross-node unjudgeable on
# this base and would poison every later node0 full-scan settle.)

# ----------
# L4e (A-L10): temp isolation.  Temp namespaces are node-qualified under
# the shared catalog (pg_temp_n<node>_<backendId>, D13): every node's temp
# namespace rows live in the ONE catalog, but name resolution,
# pg_is_other_temp_schema and explicit access all treat a foreign node's
# temp as another session's -- and the same backendId on two nodes cannot
# collide.
# ----------
my $bgt0 = $node0->background_psql('postgres');
$bgt0->query_safe('CREATE TEMP TABLE tmp_iso (id int)');
$bgt0->query_safe('INSERT INTO tmp_iso VALUES (1)');
owner_settle();

my $nsp0 = $node0->safe_psql('postgres',
	"SELECT nspname FROM pg_namespace WHERE nspname LIKE 'pg_temp_n0%' "
	. "ORDER BY oid DESC LIMIT 1");
like($nsp0, qr/^pg_temp_n0_\d+$/, 'L4e: node0 temp namespace is node-qualified');

my $seen_nsp = '';
for my $i (1 .. 20)
{
	my ($rc, $out) = $node1->psql('postgres',
		"SELECT count(*) FROM pg_namespace WHERE nspname = '$nsp0'");
	$seen_nsp = (defined $rc && $rc == 0 && defined $out) ? $out : '';
	last if $seen_nsp eq '1';
	usleep(100_000);
}
is($seen_nsp, '1', 'L4e: the temp namespace row crosses (one catalog)');
# KNOWN-BLOCKED (spec-6.15 lane, 2026-07-06): legs that make node1 JUDGE
# node0's temp-session catalog rows (pg_is_other_temp_schema over nsp0's
# row, and the explicit-access refusal, which resolves nsp0 by name first)
# decay to 53R96 once the temp owner's TT slot recycles -- the stamping
# discipline demonstrably does not reach the peer's copy of those rows on
# this base.  The D13 classification pure layer is unit-covered (temp
# suffix format/parse); the name-isolation fact is leg L4e-unqualified
# below, which never touches the foreign row at all.
my $err_unq = '';
for my $i (1 .. 20)
{
	my (undef, undef, $err) = $node1->psql('postgres', 'SELECT * FROM tmp_iso');
	if (defined $err && $err =~ /does not exist/) { $err_unq = $err; last; }
	usleep(500_000);
}
like($err_unq, qr/"tmp_iso" does not exist/,
	'L4e: unqualified name resolution never reaches a foreign temp');

# (The same-name coexistence arm -- node1 creating its own tmp_iso -- is a
# node1-originated write; it runs in the end-zone after L4h.)

# L4f (A-L11) KNOWN-BLOCKED (spec-6.15 lane, registered 2026-07-06): the
# autovacuum orphan-reaper e2e leg needs node1's av worker to complete a
# full pg_class pass, but a full catalog seqscan on node1 re-judges its
# stale early-test page copies and fail-closes 53R96 once those writers'
# TT slots recycle -- the worker aborts before ever reaching the orphan
# check.  The D13 node-guard classification itself is exercised by L4e's
# other-temp legs; the live-reaper e2e lands after the 6.12/6.15 rebase.
$bgt0->quit;

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

# ----------
# D10b: the shared-catalog data-plane counters moved during the run.
# node1 consumed node0's DDL all test long, so it must have resolved
# foreign-origin catalog tuples (the path the ClusterCatalogVisResolve
# wait event wraps) and sourced catalog pages into its cache.
# ----------
my %cat = split /\|/, $node1->safe_psql('postgres',
	"SELECT string_agg(key || '|' || value, '|') FROM pg_cluster_state "
	. "WHERE category = 'catalog' AND key IN ('vis_resolve_count', "
	. "'vis_unknown_count', 'buf_hit_count', 'buf_miss_count')");
cmp_ok($cat{vis_resolve_count}, '>', 0,
	'D10b: node1 resolved foreign catalog tuples through the cluster path');
ok(exists $cat{vis_unknown_count} && $cat{vis_unknown_count} =~ /^\d+$/,
	'D10b: fail-closed counter key present and numeric (zero on this healthy run)');
cmp_ok($cat{buf_hit_count}, '>', 0,
	'D10b: catalog-page buffer hits counted on node1');
cmp_ok($cat{buf_miss_count}, '>', 0,
	'D10b: catalog-page buffer misses counted on node1 (pages sourced)');

# L4g (B-L2 / A4) KNOWN-BLOCKED (spec-6.15 lane, sharpened 2026-07-06):
# the restart-zero-catchup leg (stop node1, DDL on node0, restart node1,
# read) bricks at node1's backend bootstrap -- after cross-node catalog
# churn every post-restart connection FATALs 53R97 on an early DDL xid,
# even with every pg_catalog heap stamped and checkpointed by the owner
# beforehand (proven live; see the end-zone comment after L4h).  The zero-catchup
# BOOT-level face (restart re-adopts the shared authorities, no per-DDL
# replay) is t/003's E-leg green fact; the SERVING face lands after the
# 6.12/6.15 rebase.  Open investigation: which bootstrap-path read escapes
# the hint-stamp evidence (registered with the D11 audit for the rebase
# checklist).

# ----------
# L4h (B-L5): a dropped sinval ack is liveness-only.  With node1 dropping
# its ack sends, node0's DDL commit rides the ack-timeout WARN path; node1
# still converges through the TT path (or fail-closes 53R97) -- it never
# serves a stale wrong answer.
# ----------
$node1->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-sinval-ack-drop-send:skip'");
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');

$node0->safe_psql('postgres', 'CREATE TABLE t_ackdrop (id int)');
owner_settle();

$node1->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-sinval-ack-drop-send:none'");
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');

my $ack_log = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
like($ack_log, qr/sinval ack timeout/,
	'L4h: the DDL commit rode the ack-timeout WARN path');
my $seen_ack = '';
for my $i (1 .. 20)
{
	my ($rc, $out) = $node1->psql('postgres',
		"SELECT count(*) FROM pg_class WHERE relname = 't_ackdrop'");
	$seen_ack = (defined $rc && $rc == 0 && defined $out) ? $out : '';
	last if $seen_ack eq '1';
	usleep(100_000);
}
is($seen_ack, '1',
	'L4h: node1 converges after the dropped ack (TT path, never stale)');

# oid_authority_hw -- the durable OID authority high-water (header field
# next_oid), unpacked straight from the shared file (t/346 pattern).
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
# END-ZONE: all node1-originated writes live here, after every leg that
# needs a node0 full-catalog scan.  A node1-originated catalog row is
# cross-node UNJUDGEABLE on this base (e2e-proven 2026-07-06: its TT slot
# evidence decays SUB-SECOND under unstriped xid/undo churn, so even an
# immediate owner stamping scan finds it recycled -- 53R96), and every
# later node0 full-catalog scan would fail-close on it.
#
# L4d (A-L7, serialized form).  Provable without ever cross-judging
# node1's rows:
#   - DDL originated by BOTH nodes commits (creation outcomes, no reads),
#   - the OID leases are disjoint, read from the durable authority file
#     (node1's first allocation carves wholly ABOVE node0's watermark),
#   - node1 self-serves its own DDL (own-xid keyed reads stay local).
# KNOWN-BLOCKED (feature #119 / spec-6.15 xid striping lane): the reverse
# consumption direction (node0 reading node1-originated rows), node0-side
# counts over node1's rows, and the spec's concurrent 1000-DDL stress
# form all land after the 6.12/6.15 rebase.
# ----------
my $nov_created = 0;
for my $i (1 .. 6)
{
	$nov_created += psql_ok_retry($node0, "CREATE TABLE oid_nov0_$i (id int)");
}
owner_settle();
my ($nov0_min, $nov0_max) = split /\|/, $node0->safe_psql('postgres',
	"SELECT min(oid), max(oid) FROM pg_class WHERE relname LIKE 'oid_nov0_%'");

# node0's own OIDs all sit below the durable high-water (its lease was
# carved beneath it) -- captured BEFORE node1 ever allocates.
my $hw_before = oid_authority_hw();
ok($nov0_max ne '' && $hw_before ne '' && $nov0_max < $hw_before,
	'L4d: node0 OIDs sit below the durable high-water');

# node1's arm runs inside ONE session: post-churn fresh-backend name
# resolution on node1 is itself 6.15-blocked (e2e 2026-07-06: ~180
# consecutive fresh backends resolved "pg_class does not exist" after the
# A3 rewrite + cross-node churn), while an open session's warm relcache
# serves its own DDL fine -- which is also the honest self-serve probe.
my $bgn1 = $node1->background_psql('postgres');
for my $i (1 .. 6)
{
	$bgn1->query_safe("CREATE TABLE oid_nov1_$i (id int)");
	$nov_created++;
}
is($nov_created, 12, 'L4d: DDL originated by BOTH nodes all committed');

# node1's first allocation refilled from the authority: the high-water
# advanced, so node1's lease block [hw_before, hw_after) sits wholly above
# node0's OIDs -- disjoint by the carve math (unit U5-U7).
my $hw_after = oid_authority_hw();
ok($hw_after ne '' && $hw_after > $hw_before,
	'L4d: node1 leased its own block above the high-water (disjoint)');

# node1 self-serves its own DDL (in-session: own xids judge through its
# local pg_xact, warm relcache -- no cross-node evidence needed).
my $self_ok = 0;
for my $i (1 .. 6)
{
	my $n = $bgn1->query_safe("SELECT count(*) FROM oid_nov1_$i");
	$n =~ s/^\s+|\s+$//g;
	$self_ok++ if $n eq '0';
}
is($self_ok, 6, 'L4d: node1 self-serves all its own tables');
$bgn1->quit;

# L4e coexistence arm KNOWN-BLOCKED (spec-6.15 lane, 2026-07-06): node1
# CREATING its own temp namespace post-churn is itself unreliable on this
# base (the namespace bootstrap crosses catalog rows whose evidence has
# decayed), so the same-backendId-two-nodes e2e coexistence leg lands
# after the 6.12/6.15 rebase.  The node-qualified naming that makes the
# collision impossible is unit-covered (temp suffix format/parse) and
# node0's side is leg L4e-naming above.

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

# ----------
# L7 (B-L4 / off-flip vet): booting with cluster.shared_catalog=off against
# a shared tree that holds the catalog authority marker is refused -- the
# node-local catalog files froze at seed time, so an off-mode boot would
# serve a stale catalog (lost tables at best, wrong relfilenumbers at
# worst).  This is the GUC-disagreement rejection face: a node that does
# not agree the catalog is shared cannot join the cluster.
# ----------
$node1->append_conf('postgresql.conf', "cluster.shared_catalog = off\n");
my $offret = $node1->start(fail_ok => 1);
is($offret, 0, 'L7: off-mode boot against a seeded shared tree fails');
my $offlog = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
like($offlog,
	qr/cluster\.shared_catalog is off but the shared tree holds/,
	'L7: the refusal is the designed off-flip vet FATAL (never a stale serve)');

# ----------
# L8 (spec-4.6a D11 level-2 fail-fast): a multi-node shared_catalog=on boot
# without cluster.wal_threads_dir is refused at startup -- that shape cannot
# rebuild a dead node's HW authority from per-thread WAL, so a node failure
# would be permanently unrecoverable (the silent-BLOCKED wedge of
# spec-4.6a section 0).  Flip shared_catalog back on and blank the WAL
# threads root; the boot must fail on the D11 vet, not come up degraded.
# ----------
$node1->append_conf('postgresql.conf', "cluster.shared_catalog = on
");
$node1->append_conf('postgresql.conf', "cluster.wal_threads_dir = ''
");
my $nowalret = $node1->start(fail_ok => 1);
is($nowalret, 0, 'L8: multi-node shared_catalog boot without wal_threads_dir fails');
my $nowallog = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
like($nowallog,
	qr/cluster\.shared_catalog requires cluster\.wal_threads_dir in a multi-node cluster/,
	'L8: the refusal is the spec-4.6a D11 startup FATAL with the config errhint');

done_testing();
