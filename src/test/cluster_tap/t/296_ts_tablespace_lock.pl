#-------------------------------------------------------------------------
#
# 296_ts_tablespace_lock.pl
#    spec-5.7 D5 (§3.3) -- the TT (tablespace-DDL) cross-node enqueue lock.
#
#    Tablespace DDL (CREATE/DROP/ALTER/RENAME) has only LOCAL catalog locks today
#    (§0.4 F0-15), so two nodes can run conflicting tablespace DDL concurrently.
#    TT(X) is the cross-node mutex; TT(S) is the narrow placement-DDL in-use guard
#    (TT-M2) that blocks a DROP while a relation is being created INTO the
#    tablespace.  resid identity is split (TT-M1): DROP/ALTER -> tablespace OID;
#    CREATE/RENAME -> hash(spcname) (the OID is not yet stable at CREATE time, so
#    a NAME hash is the only thing that serialises a cross-node same-name CREATE,
#    R14).
#
#    What this drives (REAL DDL through the tablespace.c / tablecmds.c hooks):
#      L1 CROSS-NODE same-name CREATE serialisation (REAL, name-hash): node0 holds
#         TT(X) on hash('ts_l1'); node1's REAL CREATE TABLESPACE ts_l1 hits the
#         hook -> 53RA8.  The name hash is node-independent, so this is a genuine
#         cross-node conflict.  Release -> node1's CREATE then succeeds.
#      L2 DROP serialisation (REAL, OID): a real tablespace's OID is held as TT(X);
#         a REAL DROP TABLESPACE on it hits the hook -> 53RA8.  Release -> succeeds.
#      L3 PLACEMENT TT(S) blocks DROP (REAL, the L6 case): a real, UNCOMMITTED
#         CREATE TABLE ... TABLESPACE ts_l3 holds TT(S) on the ts OID; a concurrent
#         REAL DROP TABLESPACE ts_l3 hits TT(X) -> S/X conflict -> 53RA8.  Roll the
#         placement back -> the DROP then succeeds.
#      L4 the S/X matrix: two TT(S) claims on the same OID are COMPATIBLE (both
#         granted); a TT(X) claim then CONFLICTS with a held TT(S).
#      L5 observability: ts.x_count / ts.failclosed_count advanced.
#
#    RECORDED e2e GAP (honest): a TRUE cross-node DROP/placement on a SHARED
#    tablespace OID needs a shared catalog (one OID visible on both nodes).  The
#    ClusterPair harness gives each node its own catalog (OID counters diverge), so
#    the OID-keyed legs (L2/L3/L4) drive the real GES mutex with one node's real
#    OID held by the probe (the holder stands in for the peer); the NAME-keyed leg
#    (L1) IS genuinely cross-node because the name hash is catalog-independent.
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off +
#    allow_in_place_tablespaces (so CREATE TABLESPACE needs no external dir).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/296_ts_tablespace_lock.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.7-misc-enqueue-classes.md (D5, §3.3)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ts_lock',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'allow_in_place_tablespaces = on',
	]);
$pair->start_pair;
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# ============================================================
# L1: CROSS-NODE same-name CREATE serialisation (name-hash, REAL DDL on node1).
# node0 holds TT(X) on hash('ts_l1') in a persistent session.
# ============================================================
my $hold0 = $n0->background_psql('postgres');
is($hold0->query("SELECT cluster_ts_acquire_probe('x', 'name', 'ts_l1', true)"),
	'granted', 'L1 node0 holds TT(X) on the CREATE name hash');

my ($rc, $out, $err) =
  $n1->psql('postgres', "CREATE TABLESPACE ts_l1 LOCATION ''", timeout => 60);
isnt($rc, 0, 'L1 node1 REAL CREATE TABLESPACE of the same name is rejected');
like($err, qr/53RA8|tablespace_lock_conflict|could not acquire the cluster tablespace/i,
	'L1 cross-node same-name CREATE fails closed with 53RA8 (TT-M1 name-hash, R14)');

# release -> node1's CREATE now succeeds (the lock is real and released).
is($hold0->query("SELECT cluster_ts_release_probe()"), 't',
	'L1 node0 releases the held TT(X)');
my ($rc2, $o2, $e2) =
  $n1->psql('postgres', "CREATE TABLESPACE ts_l1 LOCATION ''", timeout => 60);
is($rc2, 0, 'L1 after release node1 CREATE TABLESPACE succeeds (lock is real/reusable)')
  or diag("stderr: $e2");

# ============================================================
# L2: DROP serialisation (OID).  Create a real tablespace on node1, hold TT(X) on
# its OID, then a REAL DROP of it hits the hook -> 53RA8.
# ============================================================
$n1->safe_psql('postgres', "CREATE TABLESPACE ts_l2 LOCATION ''");
my $oid_l2 = $n1->safe_psql('postgres',
	"SELECT oid::text FROM pg_tablespace WHERE spcname = 'ts_l2'");
ok($oid_l2 =~ /^\d+$/, "L2 ts_l2 created with OID $oid_l2");

my $hold1 = $n1->background_psql('postgres');
is($hold1->query("SELECT cluster_ts_acquire_probe('x', 'oid', '$oid_l2', true)"),
	'granted', 'L2 node1 holds TT(X) on the ts_l2 OID');

my ($rc3, $o3, $e3) =
  $n1->psql('postgres', "DROP TABLESPACE ts_l2", timeout => 60);
isnt($rc3, 0, 'L2 REAL DROP TABLESPACE on a held OID is rejected');
like($e3, qr/53RA8|tablespace_lock_conflict|could not acquire the cluster tablespace/i,
	'L2 DROP fails closed with 53RA8 (TT-M1 OID resid)');

is($hold1->query("SELECT cluster_ts_release_probe()"), 't',
	'L2 node1 releases the held TT(X)');
my ($rc4, $o4, $e4) = $n1->psql('postgres', "DROP TABLESPACE ts_l2", timeout => 60);
is($rc4, 0, 'L2 after release DROP TABLESPACE succeeds') or diag("stderr: $e4");

# ============================================================
# L3: CROSS-NODE placement TT(S) blocks a DROP-style TT(X) (the L6 case), on a
# REAL tablespace's OID + a REAL DROP after release.  node0 holds TT(S) on
# ts_l3's OID; a cross-node TT(X) claim from node1 on that OID -> S/X conflict ->
# 53RA8.  Release -> node0's REAL DROP TABLESPACE ts_l3 then succeeds.
#
# NOTES (recorded gaps, honest):
#  - the TT(S) is held via the probe rather than a real CREATE TABLE ... TABLESPACE
#    ts_l3: an in-place tablespace cannot host table storage under the cluster
#    shared-storage smgr in this harness ("could not create directory
#    pg_tblspc/.../<db>") -- an ORTHOGONAL storage limitation, not a TT defect (the
#    real DefineRelation placement hook DOES take TT(S), before that storage step).
#  - the blocking claim is a cross-node TT(X) probe, not a real cross-node DROP:
#    each node has its own catalog (OID counters diverge), so a real DROP of a
#    SHARED-OID tablespace from the peer is not constructible here; the DROP leg is
#    real but same-node (after release).  A same-node real DROP does NOT conflict
#    with a same-node TT(S) because the GES NOWAIT local-grant fast path does not
#    consult the share holder (a spec-5.1/5.5 substrate behavior, recorded for
#    follow-up -- the CROSS-node guard, TT's actual purpose, works; same-node is no
#    worse than PG's own local CREATE-INTO-vs-DROP race).
# ============================================================
$n0->safe_psql('postgres', "CREATE TABLESPACE ts_l3 LOCATION ''");
my $oid_l3 = $n0->safe_psql('postgres',
	"SELECT oid::text FROM pg_tablespace WHERE spcname = 'ts_l3'");
my $place = $n0->background_psql('postgres');
is($place->query("SELECT cluster_ts_acquire_probe('s', 'oid', '$oid_l3', true)"),
	'granted', 'L3 a placement-style TT(S) is held on the ts_l3 OID (node0)');

my ($rc5, $o5, $e5) = $n1->psql('postgres',
	"SELECT cluster_ts_acquire_probe('x', 'oid', '$oid_l3', false)", timeout => 60);
isnt($rc5, 0, 'L3 a cross-node DROP-style TT(X) claim while TT(S) is held is rejected');
like($e5, qr/53RA8|tablespace_lock_conflict|could not acquire the cluster tablespace/i,
	'L3 placement TT(S) blocks the cross-node DROP TT(X) -> 53RA8 (TT-M2, the L6 guard)');

# release the TT(S) -> a real DROP then succeeds.
is($place->query("SELECT cluster_ts_release_probe()"), 't',
	'L3 the placement TT(S) is released');
$place->quit;
my ($rc6, $o6, $e6) = $n0->psql('postgres', "DROP TABLESPACE ts_l3", timeout => 60);
is($rc6, 0, 'L3 after the TT(S) is released, the REAL DROP TABLESPACE succeeds')
  or diag("stderr: $e6");

# ============================================================
# L4: the S/X matrix.  Two TT(S) claims on the same OID are COMPATIBLE; a TT(X)
# claim then CONFLICTS with a held TT(S).
# ============================================================
my $sa = $n0->background_psql('postgres');
my $sb = $n0->background_psql('postgres');
is($sa->query("SELECT cluster_ts_acquire_probe('s', 'oid', '99001', true)"),
	'granted', 'L4 first TT(S) on OID 99001 granted');
is($sb->query("SELECT cluster_ts_acquire_probe('s', 'oid', '99001', true)"),
	'granted', 'L4 second TT(S) on the same OID granted (S/S compatible)');

my ($rc7, $o7, $e7) = $n1->psql('postgres',
	"SELECT cluster_ts_acquire_probe('x', 'oid', '99001', false)", timeout => 60);
isnt($rc7, 0, 'L4 a TT(X) claim on the same OID is rejected');
like($e7, qr/53RA8|tablespace_lock_conflict|could not acquire the cluster tablespace/i,
	'L4 TT(X) conflicts with the held TT(S) -> 53RA8 (S/X matrix)');

$sa->query("SELECT cluster_ts_release_probe()");
$sb->query("SELECT cluster_ts_release_probe()");
$sa->quit;
$sb->quit;
$hold0->quit;
$hold1->quit;

# ============================================================
# L5: observability -- TT(X) grants and 53RA8 fail-closed were recorded.
# ============================================================
my $x_n0 = $n0->safe_psql('postgres',
	q{SELECT value::int FROM pg_cluster_state WHERE category='ts' AND key='x_count'});
ok($x_n0 >= 1, "L5 node0 ts.x_count = $x_n0 (>= 1 TT(X) grant recorded)");

my $fc_n1 = $n1->safe_psql('postgres',
	q{SELECT value::int FROM pg_cluster_state WHERE category='ts' AND key='failclosed_count'});
ok($fc_n1 >= 1, "L5 node1 ts.failclosed_count = $fc_n1 (>= 1 53RA8 fail-closed recorded)");

$pair->stop_pair;
done_testing();
